#include "postgres.h"
#include "storage/lwlock.h"
#include "tdb/session_processor.h"
#include "tdb/storage_processor.h"
#include "storage/shmem.h"
#include "storage/shm_toc.h"
#include "storage/dsm.h"
#include "miscadmin.h"
#include <signal.h>
#include "utils/resowner.h"
#include "storage/pmsignal.h"
#include "tdb/storage_param.h"
#include "time.h"
#define MAX_REQUEST_MQ_SIZE (1 << 20)
#define MAX_RESPONSE_MQ_SIZE (1 << 20)

#define DEAD_WAIT_RESPONSE

KVEngineAttachQueueControl *attachQueueControl = NULL;
static KVEngineMQHandleData session_mq_handle = {0};

static bool attach_queue_is_full(void);
static bool attach_queue_append_request(int pid, dsm_handle handle);
static dsm_segment* session_create_and_attach_mq(KVEngineMQHandleData *session_handle);
static void session_keep_try_append_attach_request(int pid, dsm_handle handle);
static dsm_segment* session_create_dsm_with_toc(shm_toc** toc);
static shm_mq_handle* session_attach_req_mq(shm_toc* toc, dsm_segment* seg);
static shm_mq_handle* session_attach_res_mq(shm_toc* toc, dsm_segment* seg);
static shm_mq* create_mq(shm_toc* toc, uint64 key, Size size);

/*
 * Assume AttachMQLock is held.
 */
bool
attach_queue_is_full()
{
	Assert(attachQueueControl->queue_len <= MAX_ATTACH_QUEUE_LEN);
	return (attachQueueControl->queue_len == MAX_ATTACH_QUEUE_LEN);
}

/*
 * Assume AttachMQLock is held and attach queue is not full.
 */
bool
attach_queue_append_request(int pid, dsm_handle handle)
{
	Assert(!attach_queue_is_full());
	AttachRequest req = &(attachQueueControl->attach_queue[attachQueueControl->queue_len]);
	req->session_pid = pid;
	req->handle = handle;
	++(attachQueueControl->queue_len);
	return true;
}

Size
KVEnginePidTableShmemSize(void)
{
	return sizeof(KVEngineAttachQueueControl);
}

void
KVEnginePidTableShmemInit(void)
{
	bool found;
	attachQueueControl = (KVEngineAttachQueueControl*)
		ShmemInitStruct("Attach Queue", sizeof(KVEngineAttachQueueControl), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		attachQueueControl->storage_pid = 0;
		attachQueueControl->queue_len = 0;
		SpinLockInit(&attachQueueControl->amq_mutex);
	}
	else
	{
		Assert(found);
	}
}


ResponseHeader*
SessionProcessKV(RequestHeader* req)
{
	Assert(req);
	ResponseHeader* res = NULL;
	//int reqid = req->req_id;
    if (session_mq_handle.status == KV_MQ_NOT_ATTACHED)
	{
        return res;
	}
	shm_mq_result result = shm_mq_send(session_mq_handle.req_handle, req->size, req, false);

	Assert(result == SHM_MQ_SUCCESS);

	Size len = 0;
#ifdef DEAD_WAIT_RESPONSE
	/* will dead wait the response, so I can have enough time to watch the bug */
	result = shm_mq_receive(session_mq_handle.res_handle, &len, (void**) &res, false);
	Assert(result == SHM_MQ_SUCCESS);
	return res;
#else
	time_t start = time(NULL);
	time_t now = start;
	while (now - start < KV_MAX_RERECEIVE_NUM)
	{
		result = shm_mq_receive(session_mq_handle.res_handle, &len, (void**) &res, true);
		if (res != NULL && result == SHM_MQ_SUCCESS)
		{
			if (res->req_id == -1)
			{
				if (!enable_req_failed_retry)
					ereport(ERROR,
					(errmsg("Storage: request exec failed!!!")));
				else
					ereport(WARNING,
					(errmsg("Storage: request exec failed, we try to send req again")));
				break;
			}
			if (reqid < res->req_id)
				ereport(WARNING,
					(errmsg("Storage: request exec error, return %d response, but we are wait for %d.", res->req_id, reqid)));
			if (reqid == res->req_id)
				return res;
		}
		now = time(NULL);
	}
	ereport(WARNING,
			(errmsg("Session: request %d receive no response.", reqid)));
	return NULL;
#endif
}

void
SessionInitMQ()
{
	if (session_mq_handle.status == KV_MQ_SUCCESS)
		return;
	if (!CurrentResourceOwner)
		CurrentResourceOwner = ResourceOwnerCreate(NULL, "KV storage");
	session_mq_handle.seg = session_create_and_attach_mq(&session_mq_handle);
	session_keep_try_append_attach_request(MyProcPid, dsm_segment_handle(session_mq_handle.seg));
	/* Notify storage there is a request in attach request queue. */
    //kill(attachQueueControl->storage_pid, SIG_ATTACH);
	ResponseHeader *res = NULL;
	Size len;
	shm_mq_result result = shm_mq_receive(session_mq_handle.res_handle, &len, (void**) &res, false);
	Assert(result == SHM_MQ_SUCCESS);
	session_mq_handle.status = KV_MQ_SUCCESS;
}

/*
 * called when session start up
 */
dsm_segment*
session_create_and_attach_mq(KVEngineMQHandleData *session_handle)
{
	shm_toc *toc = NULL;
	dsm_segment *seg = session_create_dsm_with_toc(&toc);
	Assert(!session_handle->req_handle && !session_handle->res_handle);
	session_handle->req_handle = session_attach_req_mq(toc, seg);
	session_handle->res_handle = session_attach_res_mq(toc, seg);
	return seg;
}

void
session_keep_try_append_attach_request(int pid, dsm_handle handle)
{
	while (true)
	{
		SpinLockAcquire(&attachQueueControl->amq_mutex);
		if (attach_queue_is_full())
		{

			SpinLockRelease(&attachQueueControl->amq_mutex);
			continue;
		}
		attach_queue_append_request(pid, handle);
		SpinLockRelease(&attachQueueControl->amq_mutex);
		return;
	}
}

dsm_segment*
session_create_dsm_with_toc(shm_toc** toc)
{
	/* initialize toc estimator */
	shm_toc_estimator e;
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, MAX_REQUEST_MQ_SIZE);
	shm_toc_estimate_chunk(&e, MAX_RESPONSE_MQ_SIZE);
	shm_toc_estimate_keys(&e, 3);

	/* allocate dynamic shared memory */
	Size segsize = shm_toc_estimate(&e);
	dsm_segment *seg = dsm_create(segsize);
	*toc = shm_toc_create(PG_KV_ENGINE_MQ_MAGIC, dsm_segment_address(seg), segsize);

	return seg;
}

shm_mq_handle*
session_attach_req_mq(shm_toc* toc, dsm_segment* seg)
{
	shm_mq* req_mq = create_mq(toc, REQUEST_MQ_KEY, MAX_REQUEST_MQ_SIZE);
	shm_mq_set_sender(req_mq, MyProc);
	return shm_mq_attach(req_mq, seg, NULL);
}

shm_mq_handle*
session_attach_res_mq(shm_toc* toc, dsm_segment* seg)
{
	shm_mq *res_mq = create_mq(toc, RESPONSE_MQ_KEY, MAX_RESPONSE_MQ_SIZE);
	shm_mq_set_receiver(res_mq, MyProc);
	return shm_mq_attach(res_mq, seg, NULL);
}

shm_mq*
create_mq(shm_toc* toc, uint64 key, Size size)
{
	shm_mq *mq = shm_mq_create(shm_toc_allocate(toc, size), size);
	shm_toc_insert(toc, key, mq);
	return mq;
}

void
SessionDestroyMQ()
{
	if ((!session_mq_handle.status) == KV_MQ_SUCCESS)
	{
		session_mq_handle.status = KV_MQ_NOT_ATTACHED;
		return;
	}
#if (HANDLE_STORAGE == 1 || HANDLE_STORAGE == 2)
    session_keep_try_append_attach_request(MyProcPid, 0);
#elif (HANDLE_STORAGE == 3)
	RequestHeader* req = palloc0(sizeof(*req));
	req->type = ROCKSDB_DETACH;
	req->size = sizeof(*req);
	SessionProcessKV(req);
#endif

	session_mq_handle.req_handle = NULL;
	session_mq_handle.res_handle = NULL;
	session_mq_handle.seg = NULL;
	session_mq_handle.status = KV_MQ_NOT_ATTACHED;
}
