/*-------------------------------------------------------------------------
 *
 * storaged.c
 *		bgworker for storage engine
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *		storage/storage.c
 *
 *-------------------------------------------------------------------------
 */

/* Minimum set of headers */
#include <signal.h>
#include <mcheck.h>
#include "postgres.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/lwlock.h"
#include "storage/shm_toc.h"
#include "storage/shm_mq.h"
#include "storage/threadpool.h"
//#include "fmgr.h"
#include "utils/resowner.h"
#include "access/xact.h"
#include "tdb/storage_processor.h"
#include "tdb/his_transaction/storage_his.h"
#include "tdb/his_transaction/his_generate_key.h"
#include "tdb/session_processor.h"
#include "tdb/range_processor.h"
#include "tdb/rocks_engine.h"
#include "tdb/bootstraprange.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "paxos/paxos_for_c_include.h"
#include "utils/memutils.h"
#include "tdb/storage_param.h"
#include "postmaster/fts.h"
#include "postmaster/postmaster.h"
#include "storage/pmsignal.h"
#include "librw/librm.h"
/*
 * Our standard signal and real-time signal can be received at same time, so it
 * is possible mq_handle has not be created. If it has not be created, we should
 * wait for it.
 *
 * Note that this function and standard signal callback function should not be in
 * the same thread, or it may cause an endless loop.
 */

/*
 * TODO: Hiding bugs may exist. 32768 is just the default pid_max in Linux,
 * according to /proc/sys/kernel/pid_max.
 */
#define MAX_PID 32768
#define PAXOS_EXEC_LOG false

#if (HANDLE_STORAGE == 1)
	List *mq_handles = NULL;
#elif (HANDLE_STORAGE == 2)
	KVEngineMQHandleData mq_handles[MAX_PID + 1];
#endif
__thread KVEngineMQHandle current_handle = NULL;
#define wait_for_attach_finished(pid) while (mq_handles[pid].status == KV_MQ_NOT_ATTACHED)
#define GPPAXOSPORTFILE "paxosportconfigure"

MemoryContext TxnRWSetContext = NULL;

static void storage_sigterm(SIGNAL_ARGS);
static void register_signals(void);
static void storage_attach_session(AttachRequestData attach_req);
static dsm_segment* storage_attach_dsm_with_toc(dsm_handle handle, shm_toc** toc);
static dsm_segment* storage_mapping_dsm_with_toc(dsm_handle handle, shm_toc** toc);
static shm_mq_handle* storage_attach_req_mq(shm_toc* toc, dsm_segment* seg);
static shm_mq_handle* storage_attach_res_mq(shm_toc* toc, dsm_segment* seg);
static void* handle_process_kv_req(void* arg);
static void storage_detach(int pid);
static KVEngineInterface* create_engine(KVEngineType type);
static ResponseHeader* process_kv_request(RequestHeader *req);
static void check_all_req(void);
static void check_kv_req(void);
static void check_attach_req(void);

#if (HANDLE_STORAGE == 1)
static ThreadJob check_and_receive_kv_req(KVEngineMQHandle handle);
#elif (HANDLE_STORAGE == 2)
static ThreadJob check_and_receive_kv_req(int pid);
#endif

static void BootStrapThreadLock(void);
/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;

static ThreadJob
initJob(int pid, KVEngineMQHandle handle, RequestHeader *req, Size len)
{
	ThreadJob job = palloc0(sizeof(*job));
	// RequestHeader* req_new = palloc0(len);
	// memcpy(req_new, req, len);
	job->pid = handle->pid;
	job->handle = handle;
	job->req = req;
	return job;
}

static void
freeJob(ThreadJob job)
{
	// range_free(job->req);
	range_free(job);
}

KVEngineInterface*
create_engine(KVEngineType type)
{
	switch (type)
	{
		case KVENGINE_ROCKSDB: return rocks_create_normal_engine();
        case KVENGINE_TRANSACTIONDB: return rocks_create_optimistic_transactions_engine();
		default: Assert(false);
	}
	return NULL;
}

/*
 * storage_sigterm
 *
 * SIGTERM handler.
 */
void
storage_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;
	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

static void
ReadPaxosPort(void)
{
	FILE	*fd;
	int		idx = 0;
	//int		max_port_size = MAXHOSTNAMELEN;
	int 	*paxos_ports = NULL;
	int 	*paxos_port = NULL;

	char	buf[PortNum * 2 + 32];
	//Assert(!IsTransactionState());

	fd = AllocateFile(GPPAXOSPORTFILE, "r");

	if (!fd)
	{
		elog(WARNING, "cannot read %s, use default port 9996-10006", GPPAXOSPORTFILE);

		for (int i = 0; i < 10; i++)
		{
			StoragePort[i] = 9997 + i;
		}
		PortNum = 10;
		return;
	}

	paxos_ports = palloc0(sizeof (int) * PortNum);

	while (fgets(buf, sizeof(buf), fd))
	{
		paxos_port = &paxos_ports[idx];

		if (sscanf(buf, "%d", (int *)&paxos_port) != 9)
		{
			FreeFile(fd);
			elog(ERROR, "invalid data in paxosportconfigure file: %s:%m", GPPAXOSPORTFILE);
		}
		idx++;
		/*
		 * Expand CdbComponentDatabaseInfo array if we've used up
		 * currently allocated space
		 */
		/*if (idx >= PortNum)
		{
			max_port_size = max_port_size * 2;
			paxos_ports = (int *)
				repalloc(paxos_ports, sizeof(int) * max_port_size);
		}*/
	}

	memcpy(StoragePort, paxos_ports, sizeof(int) * PortNum);
	pfree(paxos_ports);
	FreeFile(fd);

	PortNum = idx;
}

static void
BootStrapThreadLock(void)
{
	am_kv_storage = true;
	enable_thread_lock = true;
	enable_req_failed_retry = false;
	memorycontext_lock = true;
	//CurrentThreadMemoryContext = TopMemoryContext;
    if (CurrentThreadMemoryContext == NULL)
	    CurrentThreadMemoryContext = AllocSetContextCreate(TopMemoryContext,
							  "Storage Work Main Thread Top Context",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);
	if (!enable_paxos)
		DEFAULT_REPLICA_NUM = 1;
	else
		DEFAULT_REPLICA_NUM = 3;
	if (enable_thread_lock)
	{
#ifndef USE_SPIN_LOCK
		pthread_mutex_init (&(StorageThreadLock._Thread_MemoryContext_Lock), NULL);
		pthread_mutex_init (&(StorageThreadLock._Thread_VisionJudge_Lock), NULL);
		pthread_mutex_init (&(StorageThreadLock._Thread_WBHTAB_Lock), NULL);
        // pthread_mutex_init (&(StorageThreadLock._Thread_TxnHTAB_Lock), NULL);
        pthread_mutex_init (&(StorageThreadLock._Thread_LWinte_Lock), NULL);
        pthread_mutex_init (&(StorageThreadLock._Thread_LWarray_Lock), NULL);
        pthread_mutex_init (&(StorageThreadLock._Thread_Update_Lock), NULL);
        pthread_mutex_init (&(StorageThreadLock._Thread_TupleLockHash_Lock), NULL);
#else
        SpinLockInit(&StorageThreadLock._Thread_MemoryContext_Lock);
        SpinLockInit(&StorageThreadLock._Thread_VisionJudge_Lock);
        SpinLockInit(&StorageThreadLock._Thread_WBHTAB_Lock);
        // SpinLockInit(&StorageThreadLock._Thread_TxnHTAB_Lock);
        SpinLockInit(&StorageThreadLock._Thread_LWinte_Lock);
        SpinLockInit(&StorageThreadLock._Thread_LWarray_Lock);
        SpinLockInit(&StorageThreadLock._Thread_Update_Lock);
        SpinLockInit(&StorageThreadLock._Thread_TupleLockHash_Lock);
		SpinLockInit(&StorageThreadLock._Thread_RWSET_Lock);
#endif
		StorageHaveLock.HAVE_MemoryContext_LOCK = false;

		StorageHaveLock.HAVE_VisionJudge_LOCK = false;
		StorageHaveLock.HAVE_WBHTAB_LOCK = false;
        // StorageHaveLock.HAVE_TxnHTAB_LOCK = false;
        StorageHaveLock.HAVE_LWinte_LOCK = false;
        StorageHaveLock.HAVE_LWarray_LOCK = false;
        StorageHaveLock.HAVE_Update_LOCK = false;
        StorageHaveLock.HAVE_TupleLockHash_LOCK = false;
		StorageHaveLock.HAVE_RWSET_LOCK = false;
        StorageInitTupleLockHash();
		InitTxnLock();
	}
	if (enable_dynamic_lock)
	{
		InitThreadLock();
	}
}

static void
BootStrapRangeManage(void)
{
	if (enable_paxos)
	{
		ReadPaxosPort();
		int Port = StoragePort[GpIdentity.segindex + 1];
		char *temp = "127.0.0.1";
		int err = paxos_storage_init(temp, Port);

		if (err != 0)
			ereport(WARNING, (errmsg("PAXOS: paxos start, Port = %d, error code = %d",
							Port, err)));
		else if (PAXOS_EXEC_LOG)
			ereport(LOG, (errmsg("PAXOS: paxos start, Port = %d, error code = %d",
							Port, err)));
	}
	BootstrapInitRange();
	BootstrapInitStatistics();
}

bool
StorageStartRule(Datum main_arg)
{
	/* we start storage on master only when -E is specified */
	if (am_mirror)
		return false;
	return true;
}

/*
 * storage_main
 *
 * Main loop processing.
 */
void
storage_main(Datum main_arg)
{
	mtrace();
	BootStrapThreadLock();

	if (MyBackendId == InvalidBackendId && GpIdentity.segindex == -1 && IsUnderMasterDispatchMode())
	{
		BackgroundWorkerInitializeConnection(DB_FOR_COMMON_ACCESS, NULL);
	}
	if (MyBackendId == InvalidBackendId && GpIdentity.segindex > -1 && Gp_role == GP_ROLE_DISPATCH)
	{
		BackgroundWorkerInitializeConnection(DB_FOR_COMMON_ACCESS, NULL);
	}
    Assert(TopTransactionContext == NULL);
	TopTransactionContext =
		AllocSetContextCreate(TopMemoryContext,
							  "TopTransactionContext",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);
	TxnRWSetContext =
		AllocSetContextCreate(TopMemoryContext,
							  "TxnRWSetContext",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);
	CurTransactionContext = TopTransactionContext;
    
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "KV storage");
	attachQueueControl->storage_pid = MyProcPid;
	register_signals();
	engine = create_engine(transaction_type);
	pool_init(MAX_STORAGE_THREADS);
	
	StorageInitWritebatchHash();
    StorageInitTransactionBatchHash();
	KeyXidCache_init();
	RtsCache_init();
	if (enable_range_distribution)
		BootStrapRangeManage();
#if 1
	while (!got_sigterm)
	{
		WaitLatch(&MyProc->procLatch,
				  WL_LATCH_SET | WL_TIMEOUT,
				  (10L));
		ResetLatch(&MyProc->procLatch);
		check_all_req();		
	}
#else 
	int rc = 0;
	while (true)
	{
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   10L);
		ResetLatch(&MyProc->procLatch);
		check_all_req();
		if (!PostmasterIsAlive())
			break;
		if (rc & WL_POSTMASTER_DEATH)
			break;
	}
#endif
	engine->destroy(engine);
	pool_destroy();
	// ereport(LOG, (errmsg("storage closed")));
	proc_exit(0);
}

void
register_signals()
{
	/* Set up the signals before unblocking them. */
	// pqsignal(SIGTERM, storage_sigterm);
    pqsignal(SIGQUIT, storage_sigterm);
	/* We're now ready to receive signals. */
	BackgroundWorkerUnblockSignals();
}

#if (HANDLE_STORAGE == 1)
static KVEngineMQHandle
findHandleByPid(int pid)
{
	ListCell   *l;
	foreach(l, mq_handles)
	{
		KVEngineMQHandle handle = (KVEngineMQHandle) lfirst(l);
		if (handle->pid == pid)
			return handle;
	}
	return NULL;
}
#endif

void
storage_attach_session(AttachRequestData attach_req)
{
	shm_toc *toc = NULL;
#if (HANDLE_STORAGE == 1 || HANDLE_STORAGE == 3)
	KVEngineMQHandle handle = palloc0(sizeof(*handle));
	handle->pid = attach_req.session_pid;
	handle->node_head.type = T_MQ_HandlePlan;
#elif (HANDLE_STORAGE == 2)
	KVEngineMQHandle handle = &mq_handles[attach_req.session_pid];
#endif
	if (handle->status == KV_MQ_SUCCESS)
		return;
	Assert(handle->status == KV_MQ_NOT_ATTACHED);
	dsm_segment *seg;
	if (handle->seg && dsm_segment_handle(handle->seg) == attach_req.handle)
		seg = storage_mapping_dsm_with_toc(attach_req.handle, &toc);
	else
		seg = storage_attach_dsm_with_toc(attach_req.handle, &toc);
	if (seg)
	{
		handle->req_handle = storage_attach_req_mq(toc, seg);
		handle->res_handle = storage_attach_res_mq(toc, seg);
		handle->seg = seg;
        handle->isruning = false;
		handle->status = KV_MQ_SUCCESS;
#if (HANDLE_STORAGE == 1)
		mq_handles = lappend(mq_handles, handle);
#elif (HANDLE_STORAGE == 3)
		ThreadJob job = palloc0(sizeof(*job));
		job->handle = handle;
		job->pid = handle->pid;
		job->req = NULL;
		pool_add_worker(handle_process_kv_req, job);
#endif
		ResponseHeader* okres = palloc0(sizeof(*okres));
		okres->req_id = -2;
		okres->size = sizeof(*okres);
		okres->type = 0;
		shm_mq_send(handle->res_handle, okres->size, okres, false);
		pfree(okres);
	}
	else
	{
		handle->status = KV_MQ_NOT_ATTACHED;
		pfree(handle);
		//kill(attach_req.session_pid, SIG_DETACH);
	}
}

dsm_segment*
storage_attach_dsm_with_toc(dsm_handle handle, shm_toc** toc)
{
	dsm_segment *seg;
	seg = dsm_attach(handle);
	if (seg)
		*toc = shm_toc_attach(PG_KV_ENGINE_MQ_MAGIC, dsm_segment_address(seg));
	return seg;
}

dsm_segment*
storage_mapping_dsm_with_toc(dsm_handle handle, shm_toc** toc)
{
	dsm_segment *seg;
	seg = dsm_find_mapping(handle);
	if (seg)
		*toc = shm_toc_attach(PG_KV_ENGINE_MQ_MAGIC, dsm_segment_address(seg));
	return seg;
}

shm_mq_handle*
storage_attach_req_mq(shm_toc* toc, dsm_segment* seg)
{
	shm_mq* req_mq = shm_toc_lookup(toc, REQUEST_MQ_KEY);
	shm_mq_set_receiver(req_mq, MyProc);
	return shm_mq_attach(req_mq, seg, NULL);
}

shm_mq_handle*
storage_attach_res_mq(shm_toc* toc, dsm_segment* seg)
{
	shm_mq *res_mq = shm_toc_lookup(toc, RESPONSE_MQ_KEY);
	shm_mq_set_sender(res_mq, MyProc);
	return shm_mq_attach(res_mq, seg, NULL);
}

void
check_attach_req()
{
	if (attachQueueControl->queue_len == 0)
		return;
	SpinLockAcquire(&attachQueueControl->amq_mutex);
	int length = attachQueueControl->queue_len;
	AttachRequestData *attach_reqs = palloc0(sizeof(AttachRequestData) * 
											attachQueueControl->queue_len);
	for (int i = 0; i < attachQueueControl->queue_len; ++i)
	{
		attach_reqs[i] = attachQueueControl->attach_queue[i];
	}
	attachQueueControl->queue_len = 0;
	SpinLockRelease(&attachQueueControl->amq_mutex);
	
	for (int i = 0; i < length; i++)
	{
#if (HANDLE_STORAGE != 3)
		if (attach_reqs[i].handle == 0)
			storage_detach(attach_reqs[i].session_pid);
		else
#endif
			storage_attach_session(attach_reqs[i]);
	}
}

void
check_all_req()
{
	check_attach_req();
#if (HANDLE_STORAGE != 3)
	check_kv_req();
#endif
}

#if (HANDLE_STORAGE == 1)
void
check_kv_req()
{
	ListCell   *l;
	foreach(l, mq_handles)
	{
		KVEngineMQHandle handle = (KVEngineMQHandle) lfirst(l);
        if (handle->isruning == true)
            continue;
		ThreadJob job = check_and_receive_kv_req(handle);
		if (job != NULL)
		{
            handle->isruning = true;
			pool_add_worker(handle_process_kv_req, job);
		}
	}
}


ThreadJob
check_and_receive_kv_req(KVEngineMQHandle handle)
{
	Size len = 0;
	RequestHeader *req = NULL;

	if (handle->status == KV_MQ_NOT_ATTACHED)
		return NULL;
	shm_mq_result result = shm_mq_receive(handle->req_handle, &len, (void**) &req, true);
	if (result != SHM_MQ_SUCCESS)
		return NULL;

	return initJob(handle->pid, handle, req, len);
}

#elif (HANDLE_STORAGE == 2)
void
check_kv_req()
{
	for (int i = 0; i < MAX_PID; i++)
	{
		ThreadJob job = check_and_receive_kv_req(i);
		if (job != NULL)
		{
			pool_add_worker(handle_process_kv_req, job);
		}
	}
}
ThreadJob
check_and_receive_kv_req(int pid)
{
	Size len = 0;
	RequestHeader *req = NULL;
	if (mq_handles[pid].status == KV_MQ_NOT_ATTACHED)
		return NULL;
	shm_mq_result result = shm_mq_receive(mq_handles[pid].req_handle, &len, (void**) &req, true);
	if (result != SHM_MQ_SUCCESS)
		return NULL;
	/*RequestHeader* req_new = palloc0(len);
	memcpy(req_new, req, len);*/
	ThreadJob job = palloc0(sizeof(*job));
	job->pid = pid;
	job->req = req;
	return job;
}
#endif
#if (HANDLE_STORAGE == 1)
void*
handle_process_kv_req(void* arg)
{
	Assert(CurrentThreadMemoryContext != NULL);
	/*
	 * Apply for a new memorycontext, for each request to prevent operational
	 * memory leaks.
	 */
	MemoryContext ThreadWorkMemoryContext =
							AllocSetContextCreate(CurrentThreadMemoryContext,
							  "Storage Work Thread Context",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext oldThreadContext = CurrentThreadMemoryContext;
	CurrentThreadMemoryContext = ThreadWorkMemoryContext;
	if (enable_range_distribution)
		RouteCheckScanDesc = init_kv_scan(true);
	ThreadJob job = (ThreadJob)arg;
	
	/* It's data from shared memory. It can't be released. */
	RequestHeader *req = job->req;
    current_handle = job->handle;
	ResponseHeader* res = process_kv_request((RequestHeader*)req);
	res->req_id = req->req_id;
	shm_mq_result result = SHM_MQ_SUCCESS;
#if (HANDLE_STORAGE == 1)
	KVEngineMQHandle handle = job->handle;
	if (handle->res_handle)
		result = shm_mq_send(handle->res_handle, res->size, res, false);
#elif (HANDLE_STORAGE == 2)
	if (handle->res_handle)
		result = shm_mq_send(mq_handles[pid].res_handle, res->size, res, false);
#endif
	Assert(result == SHM_MQ_SUCCESS);

	range_free(res);

	freeJob(job);
    handle->isruning = false;
    current_handle = NULL;
	if (enable_range_distribution)
		free_kv_desc(RouteCheckScanDesc);
	CurrentThreadMemoryContext = oldThreadContext;
	if (ThreadWorkMemoryContext != NULL)
	{
		MemoryContextDelete(ThreadWorkMemoryContext);
		ThreadWorkMemoryContext = NULL;
	}
	return NULL;
}
#elif (HANDLE_STORAGE == 3)
void*
handle_process_kv_req(void* arg)
{
	ThreadJob job = (ThreadJob)arg;
	int pid = job->pid;
	KVEngineMQHandle handle = job->handle;

	while (true)
	{
		Assert(CurrentThreadMemoryContext != NULL);
		MemoryContext ThreadWorkMemoryContext =
								AllocSetContextCreate(CurrentThreadMemoryContext,
								"Storage Work Thread Context",
								ALLOCSET_DEFAULT_MINSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);
		MemoryContext oldThreadContext = CurrentThreadMemoryContext;
		CurrentThreadMemoryContext = ThreadWorkMemoryContext;

		Size len = 0;
		RequestHeader *req = NULL;
		shm_mq_result result = shm_mq_receive(handle->req_handle, &len, (void**) &req, false);

		if (enable_range_distribution)
			RouteCheckScanDesc = init_kv_scan(true);
		if (req->type == ROCKSDB_DETACH)
		{
			while(handle->status == KV_MQ_NOT_ATTACHED);
			Assert(handle->status != KV_MQ_NOT_ATTACHED);
			if (handle->status == KV_MQ_SUCCESS)
				dsm_detach(handle->seg);
			handle->req_handle = NULL;
			handle->res_handle = NULL;
			handle->status = KV_MQ_NOT_ATTACHED;
			range_free(handle);
			break;
		}
		ResponseHeader* res = process_kv_request((RequestHeader*)req);
		res->req_id = req->req_id;

		result = shm_mq_send(handle->res_handle, res->size, res, false);
		Assert(result == SHM_MQ_SUCCESS);
		range_free(res);
		if (enable_range_distribution)
			free_kv_desc(RouteCheckScanDesc);
		CurrentThreadMemoryContext = oldThreadContext;
		if (ThreadWorkMemoryContext != NULL)
		{
			MemoryContextDelete(ThreadWorkMemoryContext);
			ThreadWorkMemoryContext = NULL;
		}
	}
	freeJob(job);
	return NULL;
}
#endif
/*
 * The storage layer accepts the upper layer data and processes it.
 */
ResponseHeader*
process_kv_request(RequestHeader *req)
{
	StorageUpdateTransactionState(req);
	switch (req->type) {
		case ROCKSDB_GET:  return kvengine_process_get_req(req);
		case ROCKSDB_PUT:  return kvengine_process_put_req(req);
        case ROCKSDB_PUTRTS: return kvengine_process_put_rts_req(req);
		case ROCKSDB_DELETE_DIRECT: return kvengine_process_delete_direct_req(req);
		case ROCKSDB_RANGESCAN:
		case ROCKSDB_SCAN: return kvengine_process_scan_req(req);
		case FIND_MIDDLE_KEY: return kvengine_process_rangescan_req(req);
		case ROCKSDB_CYCLEGET: return kvengine_process_multi_get_req(req);
		case ROCKSDB_DELETE: return kvengine_process_delete_normal_req(req);
		case ROCKSDB_MULTI_PUT: return kvengine_process_multi_put_req(req);
		case ROCKSDB_REFRESH_HISTORY: return kvengine_process_refresh_history_req(req);
		case ROCKSDB_HISTORY_SCAN: return kvengine_process_scan_history(req);
		case ROCKSDB_UPDATE: return kvengine_process_update_req(req);
		case INIT_STATISTICS: return kvengine_process_init_statistics_req(req);
		case ROCKSDB_PREPARE: return kvengine_process_prepare(req);
		case ROCKSDB_COMMIT: return kvengine_process_commit(req);
		case ROCKSDB_ABORT: return kvengine_process_abort(req);
		case ROCKSDB_CLEAR: return kvengine_process_clear(req);
		case CREATEGROUP:
		{
			int result = 0;
			CreateGroupRequest* cgr = (CreateGroupRequest*)req;
			if (enable_paxos)
			{
				result = paxos_storage_process_create_group_req(cgr->groupid,
										cgr->MyIPPort, cgr->IPPortList);
			}
			CreateGroupResponse* cgres = (CreateGroupResponse*)palloc0(sizeof(CreateGroupResponse));
			cgres->header.type = cgr->header.type;
			cgres->header.size = sizeof(CreateGroupResponse);
			cgres->success = result == 0 ? true : false;
			return (ResponseHeader*)cgres;
		}
		case REMOVEGROUP:
		{
			int result = 0;
			RemoveGroupRequest* cgr = (RemoveGroupRequest*)req;
			if (enable_paxos)
			{
				result = paxos_storage_process_remove_group_req(cgr->groupid);
			}
			RemoveGroupResponse* cgres = (RemoveGroupResponse*)palloc0(sizeof(RemoveGroupResponse));
			cgres->header.type = REMOVEGROUP;
			cgres->header.size = sizeof(RemoveGroupResponse);
			cgres->success = result == 0 ? true : false;
			return (ResponseHeader*)cgres;
		}
		case ADDGROUPMEMBER:
		{
			int result = 0;
			AddGroupMemberRequest* cgr = (AddGroupMemberRequest*)req;
			if (enable_paxos)
			{
				result = paxos_storage_process_add_group_member_req(cgr->groupid, cgr->NodeIPPort);
			}
			AddGroupMemberResponse* cgres = (AddGroupMemberResponse*)palloc0(sizeof(AddGroupMemberResponse));
			cgres->header.type = ADDGROUPMEMBER;
			cgres->header.size = sizeof(AddGroupMemberResponse);
			cgres->success = result == 0 ? true : false;

			return (ResponseHeader*)cgres;
		}
		case REMOVEGROUPMEMBER:
		{
			int result = 0;
			RemoveGroupMemberRequest* cgr = (RemoveGroupMemberRequest*)req;
			if (enable_paxos)
			{
				result = paxos_storage_process_remove_group_member_req(cgr->groupid, cgr->NodeIPPort);
			}
			RemoveGroupMemberResponse* cgres = (RemoveGroupMemberResponse*)palloc0(sizeof(RemoveGroupMemberResponse));
			cgres->header.type = REMOVEGROUPMEMBER;
			cgres->header.size = sizeof(RemoveGroupMemberResponse);
			cgres->success = result == 0 ? true : false;
			return (ResponseHeader*)cgres;
		}
		case ROCKSDB_DETACH: return kvengine_process_detach(req);
		default:
		{
			if (!enable_req_failed_retry)
				ereport(ERROR,
					(errmsg("Storage: request type %d error. req_id = %d",
					req->type, req->req_id)));
			ResponseHeader* erres = palloc0(sizeof(*erres));
			erres->req_id = -1;
			erres->size = sizeof(*erres);
			erres->type = 0;
			return erres;
		}
	}
}

#if (HANDLE_STORAGE == 1)
void
storage_detach(int pid)
{
	KVEngineMQHandle handle = findHandleByPid(pid);
	while (handle->status == KV_MQ_NOT_ATTACHED);

	Assert(handle->status != KV_MQ_NOT_ATTACHED);
	if (handle->status == KV_MQ_SUCCESS)
	{
		dsm_detach(handle->seg);
	}
		
	//handle->seg = NULL;
	handle->req_handle = NULL;
	handle->res_handle = NULL;
	handle->status = KV_MQ_NOT_ATTACHED;
	//handle->pid = 0;
	mq_handles = list_delete(mq_handles, handle);
	range_free(handle);
}
#elif (HANDLE_STORAGE == 2)
storage_detach(int pid)
{
	wait_for_attach_finished(pid);
	KVEngineMQHandle handle = &mq_handles[pid];

	Assert(handle->status != KV_MQ_NOT_ATTACHED);
	if (handle->status == KV_MQ_SUCCESS)
		dsm_detach(handle->seg);
	//handle->seg = NULL;
	handle->req_handle = NULL;
	handle->res_handle = NULL;
	handle->status = KV_MQ_NOT_ATTACHED;

}
#endif
