
#include "postgres.h"
#include "utils/syscache.h"
#include "access/hio.h"
#include "access/xact.h"
#include "access/multixact.h"
#include "tdb/storage_processor.h"
#include "tdb/kv_universal.h"
#include "tdb/tdbkvam.h"
#include "tdb/kvengine.h"
#include "tdb/range.h"
#include "tdb/rocks_engine.h"
#include "paxos/paxos_for_c_include.h"
#include "tdb/paxos_message.h"
#include "tdb/route.h"
#include "tdb/rangecache.h"
#include "tdb/storage_param.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "access/subtrans.h"
#include "tdb/pg_transaction/storage_pg.h"
#include "tdb/pg_transaction/pgtransaction_generate_key.h"

static const int MultiXactStatusLock[MaxMultiXactStatus + 1] =
{
	LockTupleKeyShare,			/* ForKeyShare */
	LockTupleShare,				/* ForShare */
	LockTupleNoKeyExclusive,	/* ForNoKeyUpdate */
	LockTupleExclusive,			/* ForUpdate */
	LockTupleNoKeyExclusive,	/* NoKeyUpdate */
	LockTupleExclusive			/* Update */
};

/* Get the LockTupleMode for a given MultiXactStatus */
#define TUPLOCK_from_mxstatus(status) \
			(MultiXactStatusLock[(status)])



static bool get_one(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	if (rocksdb_key.data->rel_id != target_key.data->rel_id ||
		rocksdb_key.data->indexOid != target_key.data->indexOid)
		return false;
	Size r_len = 0, t_len = 0;
	void *key_pk_value = get_TupleKeySlice_primarykey_prefix_wxc(rocksdb_key, &r_len);
	void *start_key_pk_value = get_TupleKeySlice_primarykey_prefix_wxc(target_key, &t_len);
	bool result = memcmp(key_pk_value, start_key_pk_value, get_min(r_len, t_len)) == 0;

	return result;
}

static bool get_one_secondary(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	if (rocksdb_key.data->rel_id != target_key.data->rel_id ||
		rocksdb_key.data->indexOid != target_key.data->indexOid)
		return false;
	Size r_len = 0, t_len = 0;
	void *key_pk_value = get_TupleKeySlice_secondarykey_prefix_wxc(rocksdb_key, &r_len);
	void *start_key_pk_value = get_TupleKeySlice_secondarykey_prefix_wxc(target_key, &t_len);
	bool result = memcmp(key_pk_value, start_key_pk_value, get_min(r_len, t_len)) == 0;
	return result;
}

// static bool travers_table(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
// {
// 	return rocksdb_key.data->rel_id == target_key.data->rel_id;
// }

static bool get_maxnum(TupleKeySlice rocksdb_key, TupleKeySlice target_key, bool isforward)
{
	if (rocksdb_key.data->rel_id != target_key.data->rel_id ||
		rocksdb_key.data->indexOid != target_key.data->indexOid)
		return false;

	Size r_len = 0, t_len = 0;
	void *key_pk_value = get_TupleKeySlice_primarykey_prefix_wxc(rocksdb_key, &r_len);
	void *start_key_pk_value = get_TupleKeySlice_primarykey_prefix_wxc(target_key, &t_len);

	int result = memcmp(key_pk_value, start_key_pk_value, get_min(r_len, t_len));
	if ((result > 0 && isforward) || (result < 0 && !isforward))
		return false;
	return true;
}

static bool get_maxnum_second(TupleKeySlice rocksdb_key, TupleKeySlice target_key, bool isforward)
{
	if (rocksdb_key.data->rel_id != target_key.data->rel_id ||
		rocksdb_key.data->indexOid != target_key.data->indexOid)
		return false;

	Size r_len = 0, t_len = 0;
	void *key_pk_value = get_TupleKeySlice_secondarykey_prefix_wxc(rocksdb_key, &r_len);
	void *start_key_pk_value = get_TupleKeySlice_secondarykey_prefix_wxc(target_key, &t_len);

	int result = memcmp(key_pk_value, start_key_pk_value, get_min(r_len, t_len));
	if ((result > 0 && isforward) || (result < 0 && !isforward))
		return false;
	return true;
}

static bool get_maxnum_forward(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	return get_maxnum(rocksdb_key, target_key, true);
}

static bool get_maxnum_backward(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	return get_maxnum(rocksdb_key, target_key, false);
}

static bool get_maxnum_second_forward(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	return get_maxnum_second(rocksdb_key, target_key, true);
}

static bool get_maxnum_second_backward(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	return get_maxnum_second(rocksdb_key, target_key, false);
}


static bool scan_get_next_valid(KVEngineIteratorInterface **engine_it, TupleKeySlice cmp_key,
			Relation rel, SnapshotData *snapshot,
			Dataslice *key, Dataslice *value, Oid *rangeid,
			int session_id,
			int session_pid,
			int combocid_map_count,
			key_cmp_func is_end);
static bool check_pk_value_of_second_index(Dataslice sourceKeySlice, TupleKeySlice destkey);

static bool kvengine_check_unique(TupleKeySlice rocksdb_key, bool isSecondary, SnapshotData snapshot, TransactionId *xmin, TransactionId *xmax, int session_id, int session_pid, int combocid_map_count);
static TupleKeySlice get_checkunique_key(TupleKeySlice rocksdb_key, bool isSecondary);

static HTSU_Result lock_rocksdb_key(KVEngineBatchInterface* batch_engine, CommandId cid, TransactionId currentxid, Relation rel, TupleKeySlice key, RocksUpdateFailureData *rufd, bool *have_tuple_lock);
static HTSU_Result
kv_update_internal(Delete_UpdateRequest* update_req, KVEngineBatchInterface* batch_engine, Relation rel, TupleKeySlice* allKey, TupleValueSlice* allValue, TupleKeySlice newKey, TupleValueSlice newValue, int key_count, RocksUpdateFailureData *rufd, bool *have_tuple_lock);

static HTSU_Result
kv_update_occ_internal(Delete_UpdateRequest* update_req, KVEngineBatchInterface* batch_engine, Relation rel, TupleKeySlice* allKey, TupleValueSlice* allValue, TupleKeySlice newKey, TupleValueSlice newValue, int key_count, RocksUpdateFailureData *rufd, bool *have_tuple_lock);

static bool
scan_get_next_valid_pg_transaction(KVEngineIteratorInterface **engine_it,
					KVEngineBatchInterface* batch_engine,
					TupleKeySlice cmp_key,
                    TupleKeySlice *prev_key,
					Relation rel,
					Snapshot snapshot,
					Dataslice *key,
					Dataslice *value,
					Oid *rangeid,
					int session_id,
					int session_pid,
					int combocid_map_count,
					key_cmp_func is_end);

TupleKeySlice
get_checkunique_key(TupleKeySlice rocksdb_key, bool isSecondary)
{
	char *buffer;
	if (isSecondary)
	{
		Size len = 0;
		buffer = get_TupleKeySlice_secondarykey_prefix_wxc(rocksdb_key, &len);

		TupleKey kvengine_key = palloc0(len + sizeof(pk_len_type) + sizeof(TransactionId)
												 + sizeof(CommandId));
		memcpy((void*)kvengine_key, buffer, len);
		char* temp = (char*)kvengine_key + len;
		pk_len_type pk_len = *(pk_len_type*)temp;
		pk_len = 0;
		temp = (char*)kvengine_key + len + sizeof(pk_len_type);
		TransactionId xmin = *(TransactionId*)temp;
		xmin = 0;
		temp = (char*)kvengine_key + len + sizeof(pk_len_type) + sizeof(TransactionId);
		CommandId cmin = *(CommandId*)temp;
		cmin = 0;

		TupleKeySlice key = {kvengine_key,
			len + sizeof(pk_len_type) + sizeof(TransactionId) + sizeof(CommandId)};
		return key;
	}
	else
	{
		Size len = 0;
		buffer = get_TupleKeySlice_primarykey_prefix_wxc(rocksdb_key, &len);

		TupleKey kvengine_key = palloc0(len + sizeof(pk_len_type) + sizeof(TransactionId)
												+ sizeof(CommandId));
		memcpy((void*)kvengine_key, buffer, len);
		char* temp = (char*)kvengine_key + len;
		temp = (char*)kvengine_key + len;
		TransactionId xmin = *(TransactionId*)temp;
		xmin = 0;
		temp = (char*)kvengine_key + len + sizeof(TransactionId);
		CommandId cmin = *(CommandId*)temp;
		cmin = 0;

		TupleKeySlice key = {kvengine_key,
			len + sizeof(TransactionId) + sizeof(CommandId)};
		return key;
	}

}

bool
kvengine_check_unique(TupleKeySlice rocksdb_key, bool isSecondary, SnapshotData snapshot,
					TransactionId *xmin, TransactionId *xmax, int session_id,
					int session_pid, int combocid_map_count)
{
	KVEngineIteratorInterface *engine_it = engine->create_iterator(engine, true);
	TupleKeySlice start_key = get_checkunique_key(rocksdb_key, isSecondary);

	engine_it->seek(engine_it, start_key);
	Relation rel = kvengine_make_fake_relation();

	bool isunique = true;
	Dataslice key = NULL, value = NULL;
	Oid rangeid = 0;
    TupleKeySlice prevkey = {NULL, 0};
	if (isSecondary)
		isunique = !scan_get_next_valid_pg_transaction(&engine_it, NULL, start_key, &prevkey, rel, 
                    &snapshot, &key, &value,
					&rangeid, session_id, session_pid, combocid_map_count, get_one_secondary);
	else
		isunique = !scan_get_next_valid_pg_transaction(&engine_it, NULL, start_key, &prevkey, rel, 
                    &snapshot, &key, &value,
					&rangeid, session_id, session_pid, combocid_map_count, get_one);
	*xmin = snapshot.xmin;
	*xmax = snapshot.xmax;
	kvengine_free_fake_relation(rel);

	range_free(start_key.data);

	range_free(key);
	range_free(value);

	engine_it->destroy(engine_it);
	return isunique;
}

ResponseHeader*
kvengine_pgprocess_get_req(RequestHeader* req)
{
	GetRequest* get_req = (GetRequest*) req;
	DataSlice *req_key = get_slice_from_buffer(get_req->key);

	TupleKeySlice key = {(TupleKey) req_key->data, req_key->len};
	TupleValueSlice value;

	if (!get_req->header.writebatch)
	{
		value = engine->get(engine, key);
	}
	else
	{
		DistributedTransactionId gxid = get_req->header.gxid;
		KVEngineBatchInterface* batch_engine = engine->create_batch(engine, gxid, false);
		if (batch_engine)
			value = batch_engine->get(engine, batch_engine, key);
		else
			value = engine->get(engine, key);
	}
	DataSlice *resvalue = palloc0(size_of_Keylen(value));
	make_data_slice(*resvalue, value);

	GetResponse* get_res = palloc0(sizeof(GetResponse) + size_of_Keylen(*resvalue));
	get_res->header.type = get_req->header.type;
	get_res->header.size = sizeof(GetResponse) + size_of_Keylen(*resvalue);
	save_slice_into_buffer(/*DataSlice*/ get_res->value, resvalue);
	range_free(req_key);
	range_free(resvalue);
	return (ResponseHeader*) get_res;
}

ResponseHeader*
kvengine_pgprocess_put_req(RequestHeader* req)
{
	PutRequest* put_req = (PutRequest*) req;

	DataSlice *req_key = get_slice_from_buffer(put_req->k_v);
	Size index = size_of_Keylen(*req_key);
	DataSlice *req_value = get_slice_from_buffer(put_req->k_v+index);

	TupleKeySlice key = {(TupleKey) req_key->data, req_key->len};
	TupleValueSlice value = {(TupleValue) req_value->data, req_value->len};

	PutResponse* put_res = palloc0(sizeof(*put_res));
	put_res->header.type = put_req->header.type;
	put_res->header.size = sizeof(*put_res);

	TransactionId xmin = InvalidTransactionId, xmax = InvalidTransactionId;

	if (!put_req->header.writebatch)
	{
		if (!put_req->checkUnique || kvengine_check_unique(key,
			put_req->isSecondary, put_req->snapshot, &xmin, &xmax,
			put_req->header.gp_session_id, put_req->header.pid,
			put_req->header.combocid_map_count))
		{
			if (enable_paxos)
			{
				int length = 0;
				void* req = TransferMsgToPaxos(PAXOS_RUN_PUT, key, value, put_req->rangeid, &length);
				int result = 0;
				if (put_req->rangeid > 0 && put_req->rangeid < MAXRANGECOUNT)
					result = paxos_storage_runpaxos(req, length, put_req->rangeid);
				else
					engine->put(engine, key, value);
				range_free(req);
			}
			else
				engine->put(engine, key, value);
			put_res->checkUnique = true;
		}
		else
			put_res->checkUnique = false;
	}
	else
	{
		DistributedTransactionId gxid = put_req->header.gxid;
		KVEngineBatchInterface* batch_engine = engine->create_batch(engine, gxid, true);
		batch_engine->put(batch_engine, key, value);
		int length = 0;
		if (enable_paxos)
		{
			void* req = TransferMsgToPaxos(PAXOS_RUN_PUT, key, value, put_req->rangeid, &length);
			paxos_storage_save_to_batch((char *)req, length, put_req->rangeid, gxid);
			range_free(req);
		}
		put_res->checkUnique = true;

	}

	put_res->xmin = xmin;
	put_res->xmax = xmax;
	range_free(req_key);
	range_free(req_value);
	return (ResponseHeader*) put_res;
}

static int 
get_all_second_index(Delete_UpdateRequest* delete_req, char* buffer, int count, KVEngineBatchInterface* batch_engine, Relation rel, TupleKeySlice* allkey, TupleValueSlice* allvalue)
{
	char *tmp = buffer;

	pick_slice_from_buffer(tmp, allkey[0]);
	//*allKey[0] = get_tuple_key_from_buffer(tmp);
	tmp += size_of_Keylen(allkey[0]);

	//*allValue[0] = get_tuple_value_from_buffer(tmp);
	pick_slice_from_buffer(tmp, allvalue[0]);
	tmp += size_of_Keylen(allvalue[0]);

	if (count == 2)
		return count - 1;

	KVScanDesc desc = init_kv_scan(false);
	desc->engine_it = batch_engine->create_batch_iterator(desc->engine_it, batch_engine);
	
	Dataslice key = NULL;
	Dataslice value = NULL;
	for (int i = 2; i < count; ++i)
	{
		DataSlice *tkey = get_slice_from_buffer(tmp);
		tmp += size_of_Keylen(*tkey);

		TupleKeySlice pkey = {(TupleKey) tkey->data, tkey->len};
		*(TransactionId*)((char*)pkey.data + pkey.len - sizeof(CommandId) - sizeof(TransactionId)) = (TransactionId)(-1);
		*(CommandId*)((char*)pkey.data + pkey.len - sizeof(CommandId)) = (CommandId)(-1);
		/*set_TupleKeyData_xmin(0xffff, pkey.data, tkey->len - sizeof(Oid) - sizeof(Oid) - sizeof(TransactionId) - sizeof(CommandId));
		set_TupleKeyData_cmin(0xffff, pkey.data, tkey->len - sizeof(Oid) - sizeof(Oid) - sizeof(TransactionId) - sizeof(CommandId));*/
		desc->engine_it->seek(desc->engine_it, pkey);
		scan_get_next_valid(&desc->engine_it, pkey, rel,
									&delete_req->snapshot,
									&key, &value, &delete_req->rangeid,
									delete_req->header.gp_session_id, delete_req->header.pid,
									delete_req->header.combocid_map_count, get_one);

		TupleKeySlice cur_key = {(TupleKey)key->data, key->len};
		TupleValueSlice cur_value = {(TupleValue)value->data, value->len};
		allkey[i - 1] = cur_key;
		allvalue[i - 1] = cur_value;
		range_free(tkey);
	}
	free_kv_desc(desc);
	return count - 1;
}

static inline bool
xmax_infomask_changed(uint16 new_infomask, uint16 old_infomask)
{
	const uint16 interesting =
	HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_LOCK_MASK;

	if ((new_infomask & interesting) != (old_infomask & interesting))
		return true;

	return false;
}

static void
commit_batch(KVEngineBatchInterface* batch_engine)
{
    RocksBatchEngine* batch = (RocksBatchEngine*) batch_engine;
    rocksdb_writeoptions_t *writeopt = rocksdb_writeoptions_create();
    char *err = NULL;
    RocksEngine* rocks_engine = (RocksEngine*) engine;
    rocksdb_write_writebatch_wi(rocks_engine->PRIVATE_db, writeopt, batch->PRIVATE_writebatch_wi, &err);
    rocksdb_writeoptions_destroy(writeopt);
}

static void
destroy_batch(KVEngineBatchInterface* batch_engine)
{
    RocksBatchEngine* batch = (RocksBatchEngine*) batch_engine;
    if (batch->PRIVATE_writebatch_wi == NULL)
        return;
    rocksdb_writebatch_wi_destroy(batch->PRIVATE_writebatch_wi);
    batch->PRIVATE_writebatch_wi = NULL;
}

static void
kv_acquire_tuplock(TupleKeySlice key, bool *have_tuple_lock)
{
    if (*have_tuple_lock)
        return;
    Size size = sizeof(Delete_UpdateResponse);

	size += size_of_Keylen(key);
    DataSlice *reskey = palloc0(size_of_Keylen(key));
	make_data_slice(*reskey, key);

	Delete_UpdateResponse* update_res = palloc0(size);
	update_res->header.type = ROCKSDB_UPDATE;
	update_res->header.size = size;
	update_res->result = 0;
    update_res->result_type = UPDATE_ADD_TUPLE_LOCK;
    save_slice_into_buffer(update_res->rfd, reskey);

    shm_mq_result result = shm_mq_send(current_handle->res_handle, size, update_res, false);

    Size reqlen = 0;
	RequestHeader *req = NULL;
    for (;req == NULL;)
    {
        result = shm_mq_receive(current_handle->req_handle, &reqlen, (void**) &req, true);
    }
    Delete_UpdateRequest* goon = (Delete_UpdateRequest*)req;
    Assert(goon->result_type == GO_ON);
    if (goon->result == 0)
        *have_tuple_lock = true;
    else
        return;
}

static void
kv_release_tuplock(TupleKeySlice key, bool *have_tuple_lock)
{
    if (*have_tuple_lock == false)
        return;
    Size size = sizeof(Delete_UpdateResponse);

	size += size_of_Keylen(key);
    DataSlice *reskey = palloc0(size_of_Keylen(key));
	make_data_slice(*reskey, key);
    Size reqlen = 0;
	RequestHeader *req = NULL;

	Delete_UpdateResponse* update_res = palloc0(size);
	update_res->header.type = ROCKSDB_UPDATE;
	update_res->header.size = size;
	update_res->result = 0;
    update_res->result_type = UPDATE_RELEASE_TUPLE_LOCK;
    save_slice_into_buffer(update_res->rfd, reskey);

    shm_mq_result result = shm_mq_send(current_handle->res_handle, size, update_res, false);
    for (;req == NULL;)
    {
        result = shm_mq_receive(current_handle->req_handle, &reqlen, (void**) &req, true);
    }
    Delete_UpdateRequest* goon = (Delete_UpdateRequest*)req;
    Assert(goon->result_type == GO_ON);
    if (goon->result == 0)
        *have_tuple_lock = false;
    else
        return;
}

static void 
kv_MultiXactIdWait(TupleKeySlice key, TransactionId xwait, MultiXactStatus status, uint16 infomask, XLTW_Oper oper, int* remain)
{
    Size size = sizeof(Delete_UpdateResponse);

	size += size_of_Keylen(key);
    DataSlice *reskey = palloc0(size_of_Keylen(key));
	make_data_slice(*reskey, key);
    Size reqlen = 0;
	RequestHeader *req = NULL;

	Delete_UpdateResponse* update_res = palloc0(size);
	update_res->header.type = ROCKSDB_UPDATE;
	update_res->header.size = size;
	update_res->result = 0;
    update_res->xwait = xwait;
    update_res->status = status;
    update_res->oper = oper;
    update_res->infomask = infomask;
    update_res->result_type = UPDATE_XACTID;
    save_slice_into_buffer(update_res->rfd, reskey);

    shm_mq_result result = shm_mq_send(current_handle->res_handle, size, update_res, false);
    for (;req == NULL;)
    {
        result = shm_mq_receive(current_handle->req_handle, &reqlen, (void**) &req, true);
    }
    Delete_UpdateRequest* goon = (Delete_UpdateRequest*)req;
    Assert(goon->result_type == GO_ON);
    if (goon->result == 0)
    {
        if (remain != NULL)
            *remain = goon->remain;
    }
}

static void 
kv_XactLockTableWait(TupleKeySlice key, TransactionId xwait, XLTW_Oper oper)
{
    Size size = sizeof(Delete_UpdateResponse);

	size += size_of_Keylen(key);
    DataSlice *reskey = palloc0(size_of_Keylen(key));
	make_data_slice(*reskey, key);
    Size reqlen = 0;
	RequestHeader *req = NULL;

	Delete_UpdateResponse* update_res = palloc0(size);
	update_res->header.type = ROCKSDB_UPDATE;
	update_res->header.size = size;
	update_res->result = 0;
    update_res->xwait = xwait;
    update_res->oper = oper;
    update_res->result_type = UPDATE_XACTTABLE;
    save_slice_into_buffer(update_res->rfd, reskey);

    shm_mq_result result = shm_mq_send(current_handle->res_handle, size, update_res, false);
    for (;req == NULL;)
    {
        result = shm_mq_receive(current_handle->req_handle, &reqlen, (void**) &req, true);
    }
    Delete_UpdateRequest* goon = (Delete_UpdateRequest*)req;
    Assert(goon->result_type == GO_ON);
    if (goon->result == 0)
    {
        return;
    }
    else
        return;
}

static HTSU_Result
lock_rocksdb_key(KVEngineBatchInterface* batch_engine, CommandId cid, TransactionId currentxid, Relation rel, TupleKeySlice key, RocksUpdateFailureData *rufd, bool *have_tuple_lock)
{
	HTSU_Result result;
	
	TransactionId xid,
				xmax;
	uint16		old_infomask,
				new_infomask,
				new_infomask2;
	bool		require_sleep;

	// LockTupleWaitType waittype = LockTupleWait;
	LockTupleMode mode = LockTupleExclusive;
	//LockTupleMode mode = LockTupleNoKeyExclusive;
	TupleKeySlice cur_key = key;
	TupleValueSlice cur_value;
	HeapTuple mytup = NULL;
    
    ADD_THREAD_LOCK_EXEC(Update);
    // kv_acquire_tuplock(cur_key, have_tuple_lock);
	/* here to get the tuple */
	cur_value = batch_engine->get(engine, batch_engine, cur_key);
	kvengine_free_fake_heap(mytup);
	mytup = kvengine_make_fake_heap(cur_key, cur_value);
	/* here to get the tuple */
l3:
	/* here to examine the visible */
	ADD_THREAD_LOCK_EXEC(VisionJudge);
	result = HeapTupleSatisfiesUpdate(rel, mytup, cid, InvalidBuffer, NULL);
	REMOVE_THREAD_LOCK_EXEC(VisionJudge);
	if (result == HeapTupleInvisible)
	{
		/* here to release buffer lock */
        //releaseTuplekvLockAndBool(cur_key, have_tuple_lock);
		REMOVE_THREAD_LOCK_EXEC(Update);
        // kv_release_tuplock(cur_key, have_tuple_lock);
		/* here to release buffer lock */
		elog(ERROR, "attempted to lock invisible tuple");
	}
	else if (result == HeapTupleBeingUpdated || result == HeapTupleUpdated)
	{
		TransactionId xwait;
		uint16		infomask;
		uint16		infomask2;
		
		ItemPointerData t_ctid;
		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetRawXmax(mytup->t_data);
		infomask = mytup->t_data->t_infomask;
		infomask2 = mytup->t_data->t_infomask2;
		ItemPointerCopy(&mytup->t_data->t_ctid, &t_ctid);

		REMOVE_THREAD_LOCK_EXEC(Update);
        // kv_release_tuplock(cur_key, have_tuple_lock);
        //releaseTuplekvLockAndBool(cur_key, have_tuple_lock);
		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			int			i;
			int			nmembers;
			MultiXactMember *members;

			/*
			 * We don't need to allow old multixacts here; if that had been
			 * the case, HeapTupleSatisfiesUpdate would have returned
			 * MayBeUpdated and we wouldn't be here.
			 */
			ADD_THREAD_LOCK_EXEC(VisionJudge);
			nmembers = GetMultiXactIdMembers(xwait, &members, false);
			REMOVE_THREAD_LOCK_EXEC(VisionJudge);

			for (i = 0; i < nmembers; i++)
			{
				if (TransactionIdIsCurrentTransactionId(members[i].xid))
				{
					LockTupleMode membermode;

					membermode = TUPLOCK_from_mxstatus(members[i].status);

					if (membermode >= mode)
					{
						/* release tuple lock */
                        /*releaseTuplekvLockAndBool(cur_key, have_tuple_lock);
						if (have_tuple_lock)
							releaseTuplekvLock(cur_key);*/
							//UnlockTupleTuplock(relation, tid, mode);

						pfree(members);
                        //commit_batch(batch_engine);
                        kv_release_tuplock(cur_key, have_tuple_lock);
                        //releaseTuplekvLockAndBool(cur_key, have_tuple_lock);
						return HeapTupleMayBeUpdated;
					}
				}
			}

			pfree(members);
		}

		require_sleep = true;
		if (mode == LockTupleNoKeyExclusive)
		{
			/*
			 * If we're requesting NoKeyExclusive, we might also be able to
			 * avoid sleeping; just ensure that there no conflicting lock
			 * already acquired.
			 */
			if (infomask & HEAP_XMAX_IS_MULTI)
			{
				ADD_THREAD_LOCK_EXEC(VisionJudge);
				if (!DoesMultiXactIdConflict((MultiXactId) xwait, infomask,
											 mode))
				{
					REMOVE_THREAD_LOCK_EXEC(VisionJudge);
					/*
					 * No conflict, but if the xmax changed under us in the
					 * meantime, start over.
					 */
                    // kv_acquire_tuplock(cur_key, have_tuple_lock);
					ADD_THREAD_LOCK_EXEC(Update);
                    
                    // acquireTuplekvLockAndBool(cur_key, have_tuple_lock);
					cur_value = batch_engine->get(engine, batch_engine, cur_key);
					kvengine_free_fake_heap(mytup);
					mytup = kvengine_make_fake_heap(cur_key, cur_value);

					if (xmax_infomask_changed(mytup->t_data->t_infomask, infomask) ||
						!TransactionIdEquals(HeapTupleHeaderGetRawXmax(mytup->t_data),
											 xwait))
						goto l3;

					/* otherwise, we're good */
					require_sleep = false;
				}
				REMOVE_THREAD_LOCK_EXEC(VisionJudge);
			}
			else if (HEAP_XMAX_IS_KEYSHR_LOCKED(infomask))
			{
				ADD_THREAD_LOCK_EXEC(Update);
                // kv_acquire_tuplock(cur_key, have_tuple_lock);

                // acquireTuplekvLockAndBool(cur_key, have_tuple_lock);
				cur_value = batch_engine->get(engine, batch_engine, cur_key);
				kvengine_free_fake_heap(mytup);
				mytup = kvengine_make_fake_heap(cur_key, cur_value);

				/* if the xmax changed in the meantime, start over */
				if (xmax_infomask_changed(mytup->t_data->t_infomask, infomask) ||
					!TransactionIdEquals(
									HeapTupleHeaderGetRawXmax(mytup->t_data),
										 xwait))
					goto l3;
				/* otherwise, we're good */
				require_sleep = false;
			}
		}

		/*
		 * Time to sleep on the other transaction/multixact, if necessary.
		 *
		 * If the other transaction is an update that's already committed,
		 * then sleeping cannot possibly do any good: if we're required to
		 * sleep, get out to raise an error instead.
		 *
		 * By here, we either have already acquired the buffer exclusive lock,
		 * or we must wait for the locking transaction or multixact; so below
		 * we ensure that we grab buffer lock after the sleep.
		 */
		if (require_sleep && result == HeapTupleUpdated)
		{
			ADD_THREAD_LOCK_EXEC(Update);
            // kv_acquire_tuplock(cur_key, have_tuple_lock);
            // acquireTuplekvLockAndBool(cur_key, have_tuple_lock);
			goto failed;
		}
		else if (require_sleep)
		{
			/*
			 * Acquire tuple lock to establish our priority for the tuple.
			 * LockTuple will release us when we are next-in-line for the tuple.
			 * We must do this even if we are share-locking.
			 *
			 * If we are forced to "start over" below, we keep the tuple lock;
			 * this arranges that we stay at the head of the line while rechecking
			 * tuple state.
			 */
			/* acquire tuple lock*/
			/*heap_acquire_tuplock(rel, tid, mode, (waittype != LockTupleWait),
								 &have_tuple_lock);*/
			// acquireTuplekvLockAndBool(cur_key, have_tuple_lock);
			//have_tuple_lock = true;
            kv_acquire_tuplock(cur_key, have_tuple_lock);
			if (infomask & HEAP_XMAX_IS_MULTI)
			{
				ADD_THREAD_LOCK_EXEC(VisionJudge);
				MultiXactStatus status = get_mxact_status_for_lock(mode, false);
				REMOVE_THREAD_LOCK_EXEC(VisionJudge);

				/* We only ever lock tuples, never update them */
				if (status >= MultiXactStatusNoKeyUpdate)
					elog(ERROR, "invalid lock mode in heap_lock_tuple");

				kv_MultiXactIdWait(cur_key, xwait, status, infomask, XLTW_Lock, NULL);
				// MultiXactIdWait((MultiXactId) xwait, status, infomask,
				// 					rel, &mytup->t_self,
				// 					XLTW_Lock, NULL);


				// ADD_THREAD_LOCK_EXEC(Update);
                acquireTuplekvLockAndBool(cur_key, have_tuple_lock);
				cur_value = batch_engine->get(engine, batch_engine, cur_key);
				kvengine_free_fake_heap(mytup);
				mytup = kvengine_make_fake_heap(cur_key, cur_value);

				/*
				 * If xwait had just locked the tuple then some other xact
				 * could update this tuple before we get to this point. Check
				 * for xmax change, and start over if so.
				 */
				if (xmax_infomask_changed(mytup->t_data->t_infomask, infomask) ||
					!TransactionIdEquals(
									HeapTupleHeaderGetRawXmax(mytup->t_data),
										 xwait))
					goto l3;

				/*
				 * Of course, the multixact might not be done here: if we're
				 * requesting a light lock mode, other transactions with light
				 * locks could still be alive, as well as locks owned by our
				 * own xact or other subxacts of this backend.  We need to
				 * preserve the surviving MultiXact members.  Note that it
				 * isn't absolutely necessary in the latter case, but doing so
				 * is simpler.
				 */
			}
			else
			{
				/* use to wait for an special transaction on one rel */
				// XactLockTableStorageWait(xwait, rel, XLTW_Lock);
                kv_XactLockTableWait(cur_key, xwait, XLTW_Lock);
				ADD_THREAD_LOCK_EXEC(Update);
                // acquireTuplekvLockAndBool(cur_key, have_tuple_lock);
				cur_value = batch_engine->get(engine, batch_engine, cur_key);
				kvengine_free_fake_heap(mytup);
				mytup = kvengine_make_fake_heap(cur_key, cur_value);

				/*
				 * xwait is done, but if xwait had just locked the tuple then
				 * some other xact could update this tuple before we get to
				 * this point.  Check for xmax change, and start over if so.
				 */
				if (xmax_infomask_changed(mytup->t_data->t_infomask, infomask) ||
					!TransactionIdEquals(
									HeapTupleHeaderGetRawXmax(mytup->t_data),
										 xwait))
					goto l3;

				/*
				 * Otherwise check if it committed or aborted.  Note we cannot
				 * be here if the tuple was only locked by somebody who didn't
				 * conflict with us; that should have been handled above.  So
				 * that transaction must necessarily be gone by now.
				 */
				ADD_THREAD_LOCK_EXEC(VisionJudge);
				UpdateXmaxHintBits(mytup->t_data, InvalidBuffer, xwait, rel);
				REMOVE_THREAD_LOCK_EXEC(VisionJudge);
			}
		}

		/* By here, we're certain that we hold buffer exclusive lock again */

		/*
		 * We may lock if previous xmax aborted, or if it committed but only
		 * locked the tuple without updating it; or if we didn't have to wait
		 * at all for whatever reason.
		 */
		ADD_THREAD_LOCK_EXEC(VisionJudge);
		if (!require_sleep ||
			(mytup->t_data->t_infomask & HEAP_XMAX_INVALID) ||
			HEAP_XMAX_IS_LOCKED_ONLY(mytup->t_data->t_infomask) ||
			HeapTupleHeaderIsOnlyLocked(mytup->t_data))
			result = HeapTupleMayBeUpdated;
		else
			result = HeapTupleUpdated;
		REMOVE_THREAD_LOCK_EXEC(VisionJudge);
		//ADD_THREAD_LOCK_EXEC(Update);
        // acquireTuplekvLockAndBool(cur_key, have_tuple_lock);
	}

failed:
	if (result != HeapTupleMayBeUpdated)
	{
		ADD_THREAD_LOCK_EXEC(VisionJudge);
		Assert(result == HeapTupleSelfUpdated || result == HeapTupleUpdated);
		Assert(!(mytup->t_data->t_infomask & HEAP_XMAX_INVALID));
		rufd->key = cur_key;
		rufd->xmax = HeapTupleHeaderGetUpdateXid(mytup->t_data);
		if (result == HeapTupleSelfUpdated)
			rufd->cmax = HeapTupleHeaderGetCmax(mytup->t_data, NULL);
		else
			rufd->cmax = 0;		/* for lack of an InvalidCommandId value */
		REMOVE_THREAD_LOCK_EXEC(VisionJudge);
		/*if (have_tuple_lock)
			releaseTuplekvLock(cur_key);*/
            // releaseTuplekvLockAndBool(cur_key, have_tuple_lock);
			//UnlockTupleTuplock(relation, tid, mode);
        //commit_batch(batch_engine);
        REMOVE_THREAD_LOCK_EXEC(Update);
        kv_release_tuplock(cur_key, have_tuple_lock);
        
		return result;
	}

	xmax = HeapTupleHeaderGetRawXmax(mytup->t_data);
	old_infomask = mytup->t_data->t_infomask;

	/*
	 * We might already hold the desired lock (or stronger), possibly under a
	 * different subtransaction of the current top transaction.  If so, there
	 * is no need to change state or issue a WAL record.  We already handled
	 * the case where this is true for xmax being a MultiXactId, so now check
	 * for cases where it is a plain TransactionId.
	 *
	 * Note in particular that this covers the case where we already hold
	 * exclusive lock on the tuple and the caller only wants key share or
	 * share lock. It would certainly not do to give up the exclusive lock.
	 */
	if (!(old_infomask & (HEAP_XMAX_INVALID |
						  HEAP_XMAX_COMMITTED |
						  HEAP_XMAX_IS_MULTI)) &&
		(mode == LockTupleKeyShare ?
		 (HEAP_XMAX_IS_KEYSHR_LOCKED(old_infomask) ||
		  HEAP_XMAX_IS_SHR_LOCKED(old_infomask) ||
		  HEAP_XMAX_IS_EXCL_LOCKED(old_infomask)) :
		 mode == LockTupleShare ?
		 (HEAP_XMAX_IS_SHR_LOCKED(old_infomask) ||
		  HEAP_XMAX_IS_EXCL_LOCKED(old_infomask)) :
		 (HEAP_XMAX_IS_EXCL_LOCKED(old_infomask))) &&
		TransactionIdIsCurrentTransactionId(xmax))
	{

		/* Probably can't hold tuple lock here, but may as well check */
		/*if (have_tuple_lock)
			releaseTuplekvLock(cur_key);*/
			//UnlockTupleTuplock(rel, tid, mode);
            // releaseTuplekvLockAndBool(cur_key, have_tuple_lock);
        //commit_batch(batch_engine);
        REMOVE_THREAD_LOCK_EXEC(Update);
        kv_release_tuplock(cur_key, have_tuple_lock);
        
		return HeapTupleMayBeUpdated;
	}

	/*
	 * If this is the first possibly-multixact-able operation in the current
	 * transaction, set my per-backend OldestMemberMXactId setting. We can be
	 * certain that the transaction will never become a member of any older
	 * MultiXactIds than that.  (We have to do this even if we end up just
	 * using our own TransactionId below, since some other backend could
	 * incorporate our XID into a MultiXact immediately afterwards.)
	 */
	ADD_THREAD_LOCK_EXEC(VisionJudge);
	MultiXactIdSetOldestMember();

	/*
	 * Compute the new xmax and infomask to store into the tuple.  Note we do
	 * not modify the tuple just yet, because that would leave it in the wrong
	 * state if multixact.c elogs.
	 */
	compute_new_xmax_infomask(xmax, old_infomask, mytup->t_data->t_infomask2,
							  currentxid, mode, false,
							  &xid, &new_infomask, &new_infomask2);

	START_CRIT_SECTION();

	/*
	 * Store transaction information of xact locking the tuple.
	 *
	 * Note: Cmax is meaningless in this context, so don't set it; this avoids
	 * possibly generating a useless combo CID.  Moreover, if we're locking a
	 * previously updated tuple, it's important to preserve the Cmax.
	 *
	 * Also reset the HOT UPDATE bit, but only if there's no update; otherwise
	 * we would break the HOT chain.
	 */
	mytup->t_data->t_infomask &= ~HEAP_XMAX_BITS;
	mytup->t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	mytup->t_data->t_infomask |= new_infomask;
	mytup->t_data->t_infomask2 |= new_infomask2;
	if (HEAP_XMAX_IS_LOCKED_ONLY(new_infomask))
		HeapTupleHeaderClearHotUpdated(mytup->t_data);
	HeapTupleHeaderSetXmax(mytup->t_data, xid);

	if (kvengine_judge_dirty(cur_value.data, mytup))
	{
		batch_engine->put(batch_engine, cur_key, cur_value);
	}
	//MarkBufferDirty(*buffer);
    commit_batch(batch_engine);
   
	END_CRIT_SECTION();
	REMOVE_THREAD_LOCK_EXEC(VisionJudge);
    REMOVE_THREAD_LOCK_EXEC(Update);
	/*
	 * Don't update the visibility map here. Locking a tuple doesn't change
	 * visibility info.
	 */

	/*
	 * Now that we have successfully marked the tuple as locked, we can
	 * release the lmgr tuple lock, if we had it.
	 */
	/*if (have_tuple_lock)
		releaseTuplekvLock(cur_key);*/
		//UnlockTupleTuplock(relation, tid, mode);

    kv_release_tuplock(cur_key, have_tuple_lock);
    // releaseTuplekvLockAndBool(cur_key, have_tuple_lock);
	return HeapTupleMayBeUpdated;
}

static void
kv_link_next(TupleKeySlice *cur_key, TupleValueSlice *cur_value, TransactionId *priorXmax)
{
    Size length = 0;
    *priorXmax = cur_value->data->sysattrs.xmax;

    get_TupleKeySlice_primarykey_prefix_wxc(*cur_key, &length);
    set_TupleKeyData_xmin(cur_value->data->sysattrs.xmax, cur_key->data, length - sizeof(Oid) - sizeof(Oid));
    set_TupleKeyData_cmin(cur_value->data->sysattrs.new_cid, cur_key->data, length - sizeof(Oid) - sizeof(Oid));
}

static bool
kv_fetch_tuple(RequestHeader* req, KVEngineBatchInterface* batch_engine, TupleKeySlice *cur_key, TupleValueSlice *cur_value, Relation rel, Snapshot SnapshotDirty)
{
	*cur_value = batch_engine->get(engine, batch_engine, *cur_key);

	HeapTuple oldtup = NULL;
	kvengine_free_fake_heap(oldtup);
	oldtup = kvengine_make_fake_heap(*cur_key, *cur_value);
	
	SessionMessageData sm;
	sm.gp_session_id = req->gp_session_id;
	sm.pid = req->pid;
	sm.combocid_map_count = req->combocid_map_count;

	ADD_THREAD_LOCK_EXEC(VisionJudge);
	bool valid = HeapTupleSatisfiesVisibility(rel, oldtup, SnapshotDirty, InvalidBuffer, &sm);
	REMOVE_THREAD_LOCK_EXEC(VisionJudge);

	if (kvengine_judge_dirty(cur_value->data, oldtup))
	{
		engine->put(engine, *cur_key, *cur_value);
	}
	return valid;
}

static TupleKeySlice
EvalPlanQualFetchKV(Delete_UpdateRequest* update_req, KVEngineBatchInterface* batch_engine, TupleKeySlice *cur_key, TupleValueSlice *cur_value, Relation rel, bool *have_lock)
{
	TransactionId priorXmax = cur_value->data->sysattrs.xmax;
	SnapshotData SnapshotDirty;
	InitDirtySnapshot(SnapshotDirty);
	int time = 0;
    int updated = 0;
	TupleKeySlice nullkey = {NULL, 0};
	TupleKeySlice copykey = {NULL, 0};

	HeapTuple oldtup = NULL;
    /*Size length = 0;
    get_TupleKeySlice_primarykey_prefix_wxc(*cur_key, &length);
    set_TupleKeyData_xmin(cur_value->data->sysattrs.xmax, cur_key->data, length - sizeof(Oid) - sizeof(Oid));
    set_TupleKeyData_cmin(cur_value->data->sysattrs.new_cid, cur_key->data, length - sizeof(Oid) - sizeof(Oid));*/
    kv_link_next(cur_key, cur_value, &priorXmax);
    if (cur_value->data->sysattrs.has_new_cid == false)
	{
        return nullkey;
	}
	while (1)
	{
		if (kv_fetch_tuple((RequestHeader*)update_req, batch_engine, cur_key, cur_value, rel, &SnapshotDirty))
		{
            updated = 0;
			kvengine_free_fake_heap(oldtup);
			oldtup = kvengine_make_fake_heap(*cur_key, *cur_value);
			HTSU_Result lockresult = HeapTupleMayBeUpdated;
			RocksUpdateFailureData fd = initRocksUpdateFailureData();
			if (!TransactionIdEquals(HeapTupleHeaderGetXmin(oldtup->t_data), priorXmax))
			{
				core_dump();
				return nullkey;
			}

			/* otherwise xmin should not be dirty... */
			if (TransactionIdIsValid(SnapshotDirty.xmin))
				elog(ERROR, "t_xmin is uncommitted in tuple to be updated");

			if (TransactionIdIsValid(SnapshotDirty.xmax))
			{
                kv_XactLockTableWait(*cur_key, SnapshotDirty.xmax,  XLTW_FetchUpdated);
				// XactLockTableStorageWait(SnapshotDirty.xmax,
				// 						rel,
				// 						XLTW_FetchUpdated);
				continue;		/* loop back to repeat heap_fetch */
			}

			if (TransactionIdIsCurrentTransactionId(priorXmax) &&
				cur_value->data->sysattrs.cid >= update_req->cid)
			{
				//core_dump();
				return nullkey;
			}
            
			lockresult = lock_rocksdb_key(batch_engine, update_req->cid, update_req->xid, rel, *cur_key, &fd, have_lock);
			switch (lockresult)
			{
				case HeapTupleSelfUpdated:
					/* actually failed */
					return nullkey;

				case HeapTupleMayBeUpdated:
					/* successfully locked */
					break;

				case HeapTupleUpdated:
					{
                        /*get_TupleKeySlice_primarykey_prefix_wxc(*cur_key, &length);
                        set_TupleKeyData_xmin(cur_value->data->sysattrs.xmax, cur_key->data, length - sizeof(Oid) - sizeof(Oid));
                        set_TupleKeyData_cmin(cur_value->data->sysattrs.new_cid, cur_key->data, length - sizeof(Oid) - sizeof(Oid));
	                  	priorXmax = cur_value->data->sysattrs.xmax;*/
                        updated++;
						continue;
					}
					/* tuple was deleted, so give up */
					return nullkey;
				default:
					elog(ERROR, "unrecognized heap_lock_tuple status: %u",
							lockresult);
					return nullkey;
			}
			if (copykey.len != 0)
				range_free(copykey.data);
			copykey = copy_key(*cur_key);
            break;
		}

		if (cur_value == NULL || cur_value->data == NULL)
		{
            core_dump();
			return nullkey;
		}
		kvengine_free_fake_heap(oldtup);
		oldtup = kvengine_make_fake_heap(*cur_key, *cur_value);
        /*if (oldtup->t_data->t_choice.t_heap.t_xmax == oldtup->t_data->t_choice.t_heap.t_xmin)
            core_dump();*/
		if (!TransactionIdEquals(HeapTupleHeaderGetXmin(oldtup->t_data),
								 priorXmax))
		{
            core_dump();
			return nullkey;
		}
        if (cur_value->data->sysattrs.has_new_cid == false)
            break;

        kv_link_next(cur_key, cur_value, &priorXmax);
		/* loop back to fetch next in chain */
        time ++;
	}
	*cur_value = batch_engine->get(engine, batch_engine, copykey);
	return copykey;
}

static HTSU_Result
kv_delete_internal(Delete_UpdateRequest* delete_req, KVEngineBatchInterface* batch_engine, Relation rel, TupleKeySlice* allKey, TupleValueSlice* allValue, int key_count, RocksUpdateFailureData *rufd, bool *have_tuple_lock)
{
    HTSU_Result result;
	TransactionId xid = delete_req->xid;
    CommandId cid = delete_req->cid;
	TransactionId new_xmax;
	uint16		new_infomask,
				new_infomask2;

	bool		iscombo;
	// bool		all_visible_cleared = false;
	// HeapTuple	old_key_tuple = NULL;	/* replica identity of the tuple */
	// bool		old_key_copied = false;

    TupleKeySlice cur_key = allKey[0];
	TupleValueSlice cur_value = allValue[0];
	HeapTuple tp = NULL;

	// LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    ADD_THREAD_LOCK_EXEC(Update);
    cur_value = batch_engine->get(engine, batch_engine, cur_key);
	kvengine_free_fake_heap(tp);
	tp = kvengine_make_fake_heap(cur_key, cur_value);

l1:
    ADD_THREAD_LOCK_EXEC(VisionJudge);
	result = HeapTupleSatisfiesUpdate(rel, tp, cid, InvalidBuffer, NULL);
    REMOVE_THREAD_LOCK_EXEC(VisionJudge);

	if (result == HeapTupleInvisible)
	{
        REMOVE_THREAD_LOCK_EXEC(Update);
		elog(ERROR, "attempted to delete invisible tuple");
	}
	else if (result == HeapTupleBeingUpdated)
	{
		TransactionId xwait;
		uint16		infomask;

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetRawXmax(tp->t_data);
		infomask = tp->t_data->t_infomask;

        REMOVE_THREAD_LOCK_EXEC(Update);
        kv_acquire_tuplock(cur_key, have_tuple_lock);

		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			/* wait for multixact */
            kv_MultiXactIdWait(cur_key, xwait, MultiXactStatusUpdate, infomask, XLTW_Delete, NULL);
			
            ADD_THREAD_LOCK_EXEC(Update);

            cur_value = batch_engine->get(engine, batch_engine, cur_key);
			kvengine_free_fake_heap(tp);
			tp = kvengine_make_fake_heap(cur_key, cur_value);

			if (xmax_infomask_changed(tp->t_data->t_infomask, infomask) ||
				!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp->t_data),
									 xwait))
				goto l1;
		}
		else
		{
			/* wait for regular transaction to end */
            kv_XactLockTableWait(cur_key, xwait, XLTW_Delete);
			// XactLockTableWait(xwait, relation, &(tp.t_self), XLTW_Delete);
			// LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
            ADD_THREAD_LOCK_EXEC(Update);
			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 */
            cur_value = batch_engine->get(engine, batch_engine, cur_key);
            kvengine_free_fake_heap(tp);
            tp = kvengine_make_fake_heap(cur_key, cur_value);
			if (xmax_infomask_changed(tp->t_data->t_infomask, infomask) ||
				!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp->t_data),
									 xwait))
				goto l1;

			/* Otherwise check if it committed or aborted */
            ADD_THREAD_LOCK_EXEC(VisionJudge);
			UpdateXmaxHintBits(tp->t_data, NULL, xwait, rel);
            REMOVE_THREAD_LOCK_EXEC(VisionJudge);
		}

		/*
		 * We may overwrite if previous xmax aborted, or if it committed but
		 * only locked the tuple without updating it.
		 */
        ADD_THREAD_LOCK_EXEC(VisionJudge);
		if ((tp->t_data->t_infomask & HEAP_XMAX_INVALID) ||
			HEAP_XMAX_IS_LOCKED_ONLY(tp->t_data->t_infomask) ||
			HeapTupleHeaderIsOnlyLocked(tp->t_data))
			result = HeapTupleMayBeUpdated;
		else
			result = HeapTupleUpdated;
        REMOVE_THREAD_LOCK_EXEC(VisionJudge);
	}

	if (delete_req->snapshot.satisfies != NULL && result == HeapTupleMayBeUpdated)
	{
		/* Perform additional check for transaction-snapshot mode RI updates */
        ADD_THREAD_LOCK_EXEC(VisionJudge);
		if (!HeapTupleSatisfiesVisibility(rel, tp, &delete_req->snapshot, InvalidBuffer, NULL))
			result = HeapTupleUpdated;
        REMOVE_THREAD_LOCK_EXEC(VisionJudge);	
	}

	if (result != HeapTupleMayBeUpdated)
	{
		Assert(result == HeapTupleSelfUpdated ||
			   result == HeapTupleUpdated ||
			   result == HeapTupleBeingUpdated);
		Assert(!(tp->t_data->t_infomask & HEAP_XMAX_INVALID));
		// UnlockReleaseBuffer(buffer);
        REMOVE_THREAD_LOCK_EXEC(Update);
        kv_release_tuplock(cur_key, have_tuple_lock);
		// if (have_tuple_lock)
		// 	UnlockTupleTuplock(relation, &(tp.t_self), LockTupleExclusive);
		// if (vmbuffer != InvalidBuffer)
		// 	ReleaseBuffer(vmbuffer);
		TupleKeySlice copykey = copy_key(cur_key);
		TupleValueSlice copyvalue = copy_value(cur_value);
		copykey = EvalPlanQualFetchKV(delete_req, batch_engine, &copykey, &copyvalue, rel, have_tuple_lock);

		if (copykey.data == NULL || copyvalue.data == NULL)
		{
			rufd->key.len = 0;
			rufd->value.len = 0;
			rufd->xmax = copyvalue.data->sysattrs.xmax;
			kvengine_free_fake_heap(tp);
			range_free(allKey[0].data);
			range_free(allValue[0].data);
			range_free(allKey);
			range_free(allValue);
		}
        else
        {
            rufd->xmax = copyvalue.data->sysattrs.xmax;
            rufd->key.data = (TupleKey)copykey.data;
            rufd->key.len = copykey.len;
            rufd->value.data = (TupleValue)copyvalue.data;
            rufd->value.len = copyvalue.len;

            kvengine_free_fake_heap(tp);
            range_free(allKey[0].data);
            range_free(allValue[0].data);
            range_free(allKey);
            range_free(allValue);
        }

		return result;
	}

	// CheckForSerializableConflictIn(relation, tp, buffer);

	/* replace cid with a combo cid if necessary */
	HeapTupleHeaderAdjustCmax(tp->t_data, &cid, &iscombo);
	// old_key_tuple = ExtractReplicaIdentity(rel, tp, true, &old_key_copied);

    ADD_THREAD_LOCK_EXEC(VisionJudge);
	MultiXactIdSetOldestMember();

	compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(tp->t_data),
							  tp->t_data->t_infomask, tp->t_data->t_infomask2,
							  xid, LockTupleExclusive, true,
							  &new_xmax, &new_infomask, &new_infomask2);

	START_CRIT_SECTION();
	/* store transaction information of xact deleting the tuple */
	tp->t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	tp->t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	tp->t_data->t_infomask |= new_infomask;
	tp->t_data->t_infomask2 |= new_infomask2;
	HeapTupleHeaderClearHotUpdated(tp->t_data);
	HeapTupleHeaderSetXmax(tp->t_data, new_xmax);
	HeapTupleHeaderSetCmax(tp->t_data, cid, iscombo);
	/* Make sure there is no forward chain link in t_ctid */
    cur_value.data->sysattrs.new_cid = 0;
    cur_value.data->sysattrs.has_new_cid = 0;

	/*
	 * in the range distribution mode, range needs to be judged.  
	 * If the leader of the range where the current key is located is not 
	 * on the current node, deletion is not required.  Because the key will 
	 * be deleted on the leader node and the key on other nodes will be 
	 * deleted synchronously through paxos.
	 */
	RangeDesc route;
	route.rangeID = 0;
	RangeDesc range;
	if (enable_range_distribution && !enable_paxos)
	{
		route = findUpRangeRoute(cur_key);
		range = findUpRangeDescByID(route.rangeID);
		if (range.rangeID != route.rangeID)
		{
			/* here we do not modify the tuple */
			kvengine_free_fake_heap(tp);
			range_free(allKey[0].data);
			range_free(allValue[0].data);
			range_free(allKey);
			range_free(allValue);
            REMOVE_THREAD_LOCK_EXEC(Update);
            kv_release_tuplock(cur_key, have_tuple_lock);
			return HeapTupleMayBeUpdated;
		}
		UpdateStatisticsDelete(range.rangeID, cur_key, cur_value);
	}

	if (kvengine_judge_dirty(cur_value.data, tp))
	{
		if (enable_paxos)
		{
			int length = 0;
			DistributedTransactionId gxid = delete_req->header.gxid;
			void* req = TransferMsgToPaxos(PAXOS_RUN_PUT, cur_key, cur_value, route.rangeID, &length);
			paxos_storage_save_to_batch((char *)req, length, route.rangeID, gxid);
			range_free(req);
		}
		else
			batch_engine->put(batch_engine, cur_key, cur_value);
	}
    commit_batch(batch_engine);
	END_CRIT_SECTION();
    REMOVE_THREAD_LOCK_EXEC(VisionJudge);
	// LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    REMOVE_THREAD_LOCK_EXEC(Update);

	// CacheInvalidateHeapTuple(relation, &tp, NULL);
	/*
	 * Release the lmgr tuple lock, if we had it.
	 */
	// if (have_tuple_lock)
	// 	UnlockTupleTuplock(relation, &(tp.t_self), LockTupleExclusive);
    kv_release_tuplock(cur_key, have_tuple_lock);
	// pgstat_count_heap_delete(relation);

	// if (old_key_tuple != NULL && old_key_copied)
	// 	heap_freetuple(old_key_tuple);
	for (int i = 1; i < key_count; i++)
	{
		cur_key = allKey[i];
		cur_value = allValue[i];
		engine->delete_direct(engine, cur_key);
	}
    kvengine_free_fake_heap(tp);
	range_free(allKey[0].data);
	range_free(allValue[0].data);
	range_free(allKey);
	range_free(allValue);
	return HeapTupleMayBeUpdated;
}

ResponseHeader*
kvengine_pgprocess_delete_normal_req(RequestHeader* req)
{
    HTSU_Result result = HeapTupleMayBeUpdated;
	Delete_UpdateRequest* delete_req = (Delete_UpdateRequest*) req;
	/* 
	 * First, a batch structure is constructed for subsequent secondary index key
	 * acquisition and deletion. 
	 */
	RocksBatchEngine batch;
	KVEngineBatchInterface* batch_engine;
	if (req->writebatch)
	{
		TransactionId gxid = req->gxid;
		batch_engine = engine->create_batch(engine, gxid, true);
	}
	else
	{
		batch.PRIVATE_writebatch_wi = rocksdb_writebatch_wi_create(0,1);
		rocks_engine_init_batch_interface((KVEngineBatchInterface*) &batch);
		batch_engine = &batch.interface;
	}
	/* Second, get all the value through the primary key */
	TupleKeySlice* allkey = (TupleKeySlice*)palloc0(mul_size(delete_req->key_count, sizeof(TupleKeySlice)));
	TupleValueSlice* allvalue = (TupleValueSlice*)palloc0(mul_size(delete_req->key_count, sizeof(TupleValueSlice)));
	Relation rel = kvengine_make_fake_relation();
	int keycount = get_all_second_index(delete_req, delete_req->key, delete_req->key_count, batch_engine, rel, allkey, allvalue);
	/*
	 * Third, uniformly delete all kv that need to be deleted. 
	 */
	RocksUpdateFailureData rufd = initRocksUpdateFailureData();
    bool have_tuple_lock = false;
	result = kv_delete_internal(delete_req, batch_engine, rel, allkey, allvalue, keycount, &rufd, &have_tuple_lock);

	kvengine_free_fake_relation(rel);

    destroy_batch(batch_engine);
		
	Size size = sizeof(Delete_UpdateResponse);
	char *buffer = NULL;

	size += getRocksUpdateFailureDataLens(rufd);
	buffer = encodeRocksUpdateFailureData(rufd);

	Delete_UpdateResponse* delete_res = palloc0(size);
	delete_res->header.type = delete_req->header.type;
	delete_res->header.size = size;
	delete_res->result = result;
	memcpy(delete_res->rfd, buffer, getRocksUpdateFailureDataLens(rufd));
	return (ResponseHeader*) delete_res;
}

static HTSU_Result
kv_update_internal(Delete_UpdateRequest* update_req, KVEngineBatchInterface* batch_engine, Relation rel, TupleKeySlice* allKey, TupleValueSlice* allValue, TupleKeySlice newKey, TupleValueSlice newValue, int key_count, RocksUpdateFailureData *rufd, bool *have_tuple_lock)
{
	bool checked_lockers = false;
	bool locker_remains = false;
	HTSU_Result result = HeapTupleMayBeUpdated;
	LockTupleMode lockmode = LockTupleExclusive;
	
	MultiXactStatus mxact_status = MultiXactStatusUpdate;
	/*
	 * First, the key and value are encapsulated into a heap.
	 */
	int updated = 0;

	TupleKeySlice cur_key = allKey[0];
	TupleValueSlice cur_value = allValue[0];
	HeapTuple oldtup = NULL;

	ADD_THREAD_LOCK_EXEC(Update);

	cur_value = batch_engine->get(engine, batch_engine, cur_key);
	kvengine_free_fake_heap(oldtup);
	oldtup = kvengine_make_fake_heap(cur_key, cur_value);

l2:
	ADD_THREAD_LOCK_EXEC(VisionJudge);
	result = HeapTupleSatisfiesUpdate(rel, oldtup, update_req->cid, InvalidBuffer, NULL);
	REMOVE_THREAD_LOCK_EXEC(VisionJudge);

	if (result == HeapTupleInvisible)
	{
		/* Trace current tuple information before we unlock the buffer */
		REMOVE_THREAD_LOCK_EXEC(Update);
        
		elog(ERROR, "attempted to update invisible tuple");
	}
	else if (result == HeapTupleBeingUpdated)
	{
		TransactionId xwait;
		uint16		infomask;
		bool		can_continue = false;

		checked_lockers = true;

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetRawXmax(oldtup->t_data);
		infomask = oldtup->t_data->t_infomask;

		REMOVE_THREAD_LOCK_EXEC(Update);

		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			TransactionId update_xact;
			int			remain = 0;

			/* acquire tuple lock, if necessary */
			ADD_THREAD_LOCK_EXEC(VisionJudge);
			if (DoesMultiXactIdConflict((MultiXactId) xwait, infomask, lockmode))
			{
				REMOVE_THREAD_LOCK_EXEC(VisionJudge);
                kv_acquire_tuplock(cur_key, have_tuple_lock);

            }
			REMOVE_THREAD_LOCK_EXEC(VisionJudge);

			/* wait for multixact */
            kv_MultiXactIdWait(cur_key, xwait, mxact_status, infomask, XLTW_Update, &remain);

			ADD_THREAD_LOCK_EXEC(Update);

			cur_value = batch_engine->get(engine, batch_engine, cur_key);
			kvengine_free_fake_heap(oldtup);
			oldtup = kvengine_make_fake_heap(cur_key, cur_value);

			/*
			 * If xwait had just locked the tuple then some other xact could
			 * update this tuple before we get to this point.  Check for xmax
			 * change, and start over if so.
			 */
			if (xmax_infomask_changed(oldtup->t_data->t_infomask, infomask) ||
				!TransactionIdEquals(HeapTupleHeaderGetRawXmax(oldtup->t_data),
									 xwait))
				goto l2;

			update_xact = InvalidTransactionId;
			ADD_THREAD_LOCK_EXEC(VisionJudge);
			if (!HEAP_XMAX_IS_LOCKED_ONLY(oldtup->t_data->t_infomask))
				update_xact = HeapTupleGetUpdateXid(oldtup->t_data);
			
			if (!TransactionIdIsValid(update_xact) ||
				TransactionIdDidAbort(update_xact))
				can_continue = true;
			REMOVE_THREAD_LOCK_EXEC(VisionJudge);

			locker_remains = remain != 0;
		}
		else
		{
			if (HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) && false)
			{
				//TODO: buffer lock
				//LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				ADD_THREAD_LOCK_EXEC(Update);
                // kv_acquire_tuplock(cur_key, have_tuple_lock);
                // acquireTuplekvLockAndBool(cur_key, have_tuple_lock);
				cur_value = batch_engine->get(engine, batch_engine, cur_key);
				kvengine_free_fake_heap(oldtup);
				oldtup = kvengine_make_fake_heap(cur_key, cur_value);
				/*
				 * recheck the locker; if someone else changed the tuple while
				 * we weren't looking, start over.
				 */
				if (xmax_infomask_changed(oldtup->t_data->t_infomask, infomask) ||
					!TransactionIdEquals(
									HeapTupleHeaderGetRawXmax(oldtup->t_data),
										 xwait))
					goto l2;

				can_continue = true;
				locker_remains = true;
			}
			else
			{
				/*
				 * Wait for regular transaction to end; but first, acquire
				 * tuple lock.
				 */
                kv_XactLockTableWait(cur_key, xwait, XLTW_Update);

				ADD_THREAD_LOCK_EXEC(Update);

				cur_value = batch_engine->get(engine, batch_engine, cur_key);
				kvengine_free_fake_heap(oldtup);
				oldtup = kvengine_make_fake_heap(cur_key, cur_value);
				/*
				 * xwait is done, but if xwait had just locked the tuple then
				 * some other xact could update this tuple before we get to
				 * this point. Check for xmax change, and start over if so.
				 */
				if (xmax_infomask_changed(oldtup->t_data->t_infomask, infomask) ||
					!TransactionIdEquals(
									HeapTupleHeaderGetRawXmax(oldtup->t_data),
										 xwait))
					goto l2;

				/* Otherwise check if it committed or aborted */
				ADD_THREAD_LOCK_EXEC(VisionJudge);
				UpdateXmaxHintBits(oldtup->t_data, InvalidBuffer, xwait, rel);
				REMOVE_THREAD_LOCK_EXEC(VisionJudge);
				if (oldtup->t_data->t_infomask & HEAP_XMAX_INVALID)
					can_continue = true;
			}
		}
		result = can_continue ? HeapTupleMayBeUpdated : HeapTupleUpdated;
	}

	if (result == HeapTupleMayBeUpdated && update_req->snapshot.satisfies != NULL)
	{
		ADD_THREAD_LOCK_EXEC(VisionJudge);
		if (!HeapTupleSatisfiesVisibility(rel, oldtup, &update_req->snapshot, InvalidBuffer, NULL))
		{
			result = HeapTupleUpdated;
		}
		REMOVE_THREAD_LOCK_EXEC(VisionJudge);	
	}

	if (result != HeapTupleMayBeUpdated)
	{
		updated++;

		REMOVE_THREAD_LOCK_EXEC(Update);
        kv_release_tuplock(cur_key, have_tuple_lock);
        //releaseTuplekvLockAndBool(cur_key, have_tuple_lock);
		//if (have_tuple_lock)
		//	releaseTuplekvLock(cur_key);

		TupleKeySlice copykey = copy_key(cur_key);
		TupleValueSlice copyvalue = copy_value(cur_value);
		copykey = EvalPlanQualFetchKV(update_req, batch_engine, &copykey, &copyvalue, rel, have_tuple_lock);

		if (copykey.data == NULL || copyvalue.data == NULL)
		{
			rufd->key.len = 0;
			rufd->value.len = 0;
			rufd->xmax = copyvalue.data->sysattrs.xmax;
			kvengine_free_fake_heap(oldtup);
			range_free(allKey[0].data);
			range_free(allValue[0].data);
			range_free(allKey);
			range_free(allValue);
		}
        else
        {
            rufd->xmax = copyvalue.data->sysattrs.xmax;
            rufd->key.data = (TupleKey)copykey.data;
            rufd->key.len = copykey.len;
            rufd->value.data = (TupleValue)copyvalue.data;
            rufd->value.len = copyvalue.len;

            kvengine_free_fake_heap(oldtup);
            range_free(allKey[0].data);
            range_free(allValue[0].data);
            range_free(allKey);
            range_free(allValue);
        }

		return result;
	}
	/* Fill in transaction status data */
	/*
	 * If the tuple we're updating is locked, we need to preserve the locking
	 * info in the old tuple's Xmax.  Prepare a new Xmax value for this.
	 */
	TransactionId xmax_new_tuple,
				xmax_old_tuple;
	uint16		infomask_old_tuple,
				infomask2_old_tuple,
				infomask_new_tuple,
				infomask2_new_tuple;
	ADD_THREAD_LOCK_EXEC(VisionJudge);
	compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(oldtup->t_data),
							  oldtup->t_data->t_infomask,
							  oldtup->t_data->t_infomask2,
							  update_req->xid, LockTupleExclusive, true,
							  &xmax_old_tuple, &infomask_old_tuple,
							  &infomask2_old_tuple);
	REMOVE_THREAD_LOCK_EXEC(VisionJudge);
	/*
	 * And also prepare an Xmax value for the new copy of the tuple.  If there
	 * was no xmax previously, or there was one but all lockers are now gone,
	 * then use InvalidXid; otherwise, get the xmax from the old tuple.  (In
	 * rare cases that might also be InvalidXid and yet not have the
	 * HEAP_XMAX_INVALID bit set; that's fine.)
	 */
	if ((oldtup->t_data->t_infomask & HEAP_XMAX_INVALID) ||
		HEAP_LOCKED_UPGRADED(oldtup->t_data->t_infomask) ||
		(checked_lockers && !locker_remains))
		xmax_new_tuple = InvalidTransactionId;
	else
		xmax_new_tuple = HeapTupleHeaderGetRawXmax(oldtup->t_data);

	if (!TransactionIdIsValid(xmax_new_tuple))
	{
		infomask_new_tuple = HEAP_XMAX_INVALID;
		infomask2_new_tuple = 0;
	}
	else
	{
		/*
		 * If we found a valid Xmax for the new tuple, then the infomask bits
		 * to use on the new tuple depend on what was there on the old one.
		 * Note that since we're doing an update, the only possibility is that
		 * the lockers had FOR KEY SHARE lock.
		 */
		if (oldtup->t_data->t_infomask & HEAP_XMAX_IS_MULTI)
		{
			ADD_THREAD_LOCK_EXEC(VisionJudge);
			GetMultiXactIdHintBits(xmax_new_tuple, &infomask_new_tuple,
								   &infomask2_new_tuple);
			REMOVE_THREAD_LOCK_EXEC(VisionJudge);
		}
		else
		{
			infomask_new_tuple = HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY;
			infomask2_new_tuple = 0;
		}
	}

	HeapTuple newtup = NULL;
	newtup = kvengine_make_fake_heap(newKey, newValue);
	/*
	 * Prepare the new tuple with the appropriate initial values of Xmin and
	 * Xmax, as well as initial infomask bits as computed above.
	 */
	newtup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	newtup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	HeapTupleHeaderSetXmin(newtup->t_data, update_req->xid);
	HeapTupleHeaderSetCmin(newtup->t_data, update_req->cid);
	newtup->t_data->t_infomask |= HEAP_UPDATED | infomask_new_tuple;
	newtup->t_data->t_infomask2 |= infomask2_new_tuple;
	HeapTupleHeaderSetXmax(newtup->t_data, xmax_new_tuple);

	/*
	 * Replace cid with a combo cid if necessary.  Note that we already put
	 * the plain cid into the new tuple.
	 */
	bool iscombo;
	ADD_THREAD_LOCK_EXEC(VisionJudge);
	HeapTupleHeaderAdjustCmax(oldtup->t_data, &update_req->cid, &iscombo);
	REMOVE_THREAD_LOCK_EXEC(VisionJudge);
	//!!!TODO: here we need to check the conflict 
	//CheckForSerializableConflictIn(relation, &oldtup, buffer);


	/*
	 * Compute replica identity tuple before entering the critical section so
	 * we don't PANIC upon a memory allocation failure.
	 * ExtractReplicaIdentity() will return NULL if nothing needs to be
	 * logged.
	 */
	//old_key_tuple = ExtractReplicaIdentity(relation, &oldtup, !satisfies_id, &old_key_copied);

	/* NO EREPORT(ERROR) from here till changes are logged */
	ADD_THREAD_LOCK_EXEC(VisionJudge);
	START_CRIT_SECTION();
	//RelationPutHeapTuple(relation, newbuf, heaptup);	/* insert new tuple */

	/* Clear obsolete visibility flags, possibly set by ourselves above... */
	oldtup->t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	oldtup->t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	/* ... and store info about transaction updating this tuple */
	Assert(TransactionIdIsValid(xmax_old_tuple));
	HeapTupleHeaderSetXmax(oldtup->t_data, xmax_old_tuple);
	oldtup->t_data->t_infomask |= infomask_old_tuple;
	oldtup->t_data->t_infomask2 |= infomask2_old_tuple;
	HeapTupleHeaderSetCmax(oldtup->t_data, update_req->cid, iscombo);
	/* record address of new tuple in t_ctid of old one */
	//oldtup->t_data->t_ctid = heaptup->t_self;
	cur_value.data->sysattrs.new_cid = newtup->t_data->t_choice.t_heap.t_field3.t_cid;
    cur_value.data->sysattrs.has_new_cid = true;

	/*
	 * in the range distribution mode, range needs to be judged.  
	 * If the leader of the range where the current key is located is not 
	 * on the current node, deletion is not required.  Because the key will 
	 * be deleted on the leader node and the key on other nodes will be 
	 * deleted synchronously through paxos.
	 */
	RangeDesc route;
	route.rangeID = 0;
	RangeDesc range;
	if (enable_range_distribution && !enable_paxos)
	{
		route = findUpRangeRoute(cur_key);
		range = findUpRangeDescByID(route.rangeID);
		if (range.rangeID != route.rangeID)
		{
			/* here we do not modify the tuple */
			kvengine_free_fake_heap(oldtup);
			kvengine_free_fake_heap(newtup);
			range_free(allKey[0].data);
			range_free(allValue[0].data);
			range_free(allKey);
			range_free(allValue);
            //commit_batch(batch_engine);
            REMOVE_THREAD_LOCK_EXEC(Update);
            // kv_release_tuplock(cur_key, have_tuple_lock);
            // releaseTuplekvLockAndBool(cur_key, have_tuple_lock);
			return HeapTupleMayBeUpdated;
		}
		UpdateStatisticsDelete(range.rangeID, cur_key, cur_value);
	}
	
	if (kvengine_judge_dirty(cur_value.data, oldtup))
	{
		if (enable_paxos)
		{
			int length = 0;
			DistributedTransactionId gxid = update_req->header.gxid;
			void* req = TransferMsgToPaxos(PAXOS_RUN_PUT, cur_key, cur_value, route.rangeID, &length);
			paxos_storage_save_to_batch((char *)req, length, route.rangeID, gxid);
			range_free(req);
		}
		else
			batch_engine->put(batch_engine, cur_key, cur_value);
	}

	if (kvengine_judge_dirty(newValue.data, newtup))
	{
		if (enable_paxos)
		{
			int length = 0;
			DistributedTransactionId gxid = update_req->header.gxid;
			void* req = TransferMsgToPaxos(PAXOS_RUN_PUT, newKey, newValue, route.rangeID, &length);
			paxos_storage_save_to_batch((char *)req, length, route.rangeID, gxid);
			range_free(req);
		}
		else
			batch_engine->put(batch_engine, newKey, newValue);
	}

	for (int i = 1; i < key_count; i++)
	{
		cur_key = allKey[i];
		cur_value = allValue[i];
		engine->delete_direct(engine, cur_key);
	}
    commit_batch(batch_engine);

	END_CRIT_SECTION();
	REMOVE_THREAD_LOCK_EXEC(VisionJudge);

	REMOVE_THREAD_LOCK_EXEC(Update);

	/*
	 * Mark old tuple for invalidation from system caches at next command
	 * boundary, and mark the new tuple for invalidation in case we abort. We
	 * have to do this before releasing the buffer because oldtup is in the
	 * buffer.  (heaptup is all in local memory, but it's necessary to process
	 * both tuple versions in one call to inval.c so we can avoid redundant
	 * sinval messages.)
	 */
	//CacheInvalidateHeapTuple(relation, &oldtup, heaptup);

	/*
	 * Release the lmgr tuple lock, if we had it.
	 */
	kv_release_tuplock(cur_key, have_tuple_lock);

	kvengine_free_fake_heap(oldtup);
	kvengine_free_fake_heap(newtup);
	range_free(allKey[0].data);
	range_free(allValue[0].data);
	range_free(allKey);
	range_free(allValue);

	return HeapTupleMayBeUpdated;
}

static HTSU_Result
kv_update_occ_internal(Delete_UpdateRequest* update_req, KVEngineBatchInterface* batch_engine, Relation rel, TupleKeySlice* allKey, TupleValueSlice* allValue, TupleKeySlice newKey, TupleValueSlice newValue, int key_count, RocksUpdateFailureData *rufd, bool *have_tuple_lock)
{
	bool checked_lockers = false;
	bool locker_remains = false;
	// HTSU_Result result = HeapTupleMayBeUpdated;
	// LockTupleMode lockmode = LockTupleExclusive;

	// MultiXactStatus mxact_status = MultiXactStatusUpdate;
	/*
	 * First, the key and value are encapsulated into a heap.
	 */
	// int updated = 0;
	// int trylock = 0;
	TupleKeySlice cur_key = allKey[0];
	TupleValueSlice cur_value = allValue[0];
	HeapTuple oldtup = NULL;

	// ADD_THREAD_LOCK_EXEC(Update);

	cur_value = batch_engine->get(engine, batch_engine, cur_key);
	kvengine_free_fake_heap(oldtup);
	oldtup = kvengine_make_fake_heap(cur_key, cur_value);

	/* Fill in transaction status data */
	/*
	 * If the tuple we're updating is locked, we need to preserve the locking
	 * info in the old tuple's Xmax.  Prepare a new Xmax value for this.
	 */
	TransactionId xmax_new_tuple,
				xmax_old_tuple;
	uint16		infomask_old_tuple,
				infomask2_old_tuple,
				infomask_new_tuple,
				infomask2_new_tuple;


	/*ADD_THREAD_LOCK_EXEC(VisionJudge);
	compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(oldtup->t_data),
							  oldtup->t_data->t_infomask,
							  oldtup->t_data->t_infomask2,
							  update_req->xid, LockTupleExclusive, true,
							  &xmax_old_tuple, &infomask_old_tuple,
							  &infomask2_old_tuple);
	REMOVE_THREAD_LOCK_EXEC(VisionJudge);*/
	infomask_old_tuple = 0;
	infomask2_old_tuple = 0;

	xmax_old_tuple = update_req->xid;
	infomask2_old_tuple |= HEAP_KEYS_UPDATED;



	/*
	 * And also prepare an Xmax value for the new copy of the tuple.  If there
	 * was no xmax previously, or there was one but all lockers are now gone,
	 * then use InvalidXid; otherwise, get the xmax from the old tuple.  (In
	 * rare cases that might also be InvalidXid and yet not have the
	 * HEAP_XMAX_INVALID bit set; that's fine.)
	 */
	if ((oldtup->t_data->t_infomask & HEAP_XMAX_INVALID) ||
		HEAP_LOCKED_UPGRADED(oldtup->t_data->t_infomask) ||
		(checked_lockers && !locker_remains))
		xmax_new_tuple = InvalidTransactionId;
	else
		xmax_new_tuple = HeapTupleHeaderGetRawXmax(oldtup->t_data);

	if (!TransactionIdIsValid(xmax_new_tuple))
	{
		infomask_new_tuple = HEAP_XMAX_INVALID;
		infomask2_new_tuple = 0;
	}
	else
	{
		/*
		 * If we found a valid Xmax for the new tuple, then the infomask bits
		 * to use on the new tuple depend on what was there on the old one.
		 * Note that since we're doing an update, the only possibility is that
		 * the lockers had FOR KEY SHARE lock.
		 */
		if (oldtup->t_data->t_infomask & HEAP_XMAX_IS_MULTI)
		{
			ADD_THREAD_LOCK_EXEC(VisionJudge);
			GetMultiXactIdHintBits(xmax_new_tuple, &infomask_new_tuple,
								   &infomask2_new_tuple);
			REMOVE_THREAD_LOCK_EXEC(VisionJudge);
		}
		else
		{
			infomask_new_tuple = HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY;
			infomask2_new_tuple = 0;
		}
	}

	HeapTuple newtup = NULL;
	newtup = kvengine_make_fake_heap(newKey, newValue);
	/*
	 * Prepare the new tuple with the appropriate initial values of Xmin and
	 * Xmax, as well as initial infomask bits as computed above.
	 */
	newtup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	newtup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	HeapTupleHeaderSetXmin(newtup->t_data, update_req->xid);
	HeapTupleHeaderSetCmin(newtup->t_data, update_req->cid);
	newtup->t_data->t_infomask |= HEAP_UPDATED | infomask_new_tuple;
	newtup->t_data->t_infomask2 |= infomask2_new_tuple;
	HeapTupleHeaderSetXmax(newtup->t_data, xmax_new_tuple);

	/*
	 * Replace cid with a combo cid if necessary.  Note that we already put
	 * the plain cid into the new tuple.
	 */
	bool iscombo;
	ADD_THREAD_LOCK_EXEC(VisionJudge);
	HeapTupleHeaderAdjustCmax(oldtup->t_data, &update_req->cid, &iscombo);
	REMOVE_THREAD_LOCK_EXEC(VisionJudge);
	//!!!TODO: here we need to check the conflict 
	//CheckForSerializableConflictIn(relation, &oldtup, buffer);


	/*
	 * Compute replica identity tuple before entering the critical section so
	 * we don't PANIC upon a memory allocation failure.
	 * ExtractReplicaIdentity() will return NULL if nothing needs to be
	 * logged.
	 */
	//old_key_tuple = ExtractReplicaIdentity(relation, &oldtup, !satisfies_id, &old_key_copied);

	/* NO EREPORT(ERROR) from here till changes are logged */
	ADD_THREAD_LOCK_EXEC(VisionJudge);
	START_CRIT_SECTION();
	//RelationPutHeapTuple(relation, newbuf, heaptup);	/* insert new tuple */

	/* Clear obsolete visibility flags, possibly set by ourselves above... */
	oldtup->t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	oldtup->t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	/* ... and store info about transaction updating this tuple */
	Assert(TransactionIdIsValid(xmax_old_tuple));
	HeapTupleHeaderSetXmax(oldtup->t_data, xmax_old_tuple);
	oldtup->t_data->t_infomask |= infomask_old_tuple;
	oldtup->t_data->t_infomask2 |= infomask2_old_tuple;
	HeapTupleHeaderSetCmax(oldtup->t_data, update_req->cid, iscombo);
	/* record address of new tuple in t_ctid of old one */
	//oldtup->t_data->t_ctid = heaptup->t_self;
	cur_value.data->sysattrs.new_cid = newtup->t_data->t_choice.t_heap.t_field3.t_cid;
    cur_value.data->sysattrs.has_new_cid = true;

	/*
	 * Third, in the range distribution mode, range needs to be judged.  
	 * If the leader of the range where the current key is located is not 
	 * on the current node, deletion is not required.  Because the key will 
	 * be deleted on the leader node and the key on other nodes will be 
	 * deleted synchronously through paxos.
	 */
	RangeDesc route;
	route.rangeID = 0;
	RangeDesc range;
	if (enable_range_distribution && !enable_paxos)
	{
		route = findUpRangeRoute(cur_key);
		range = findUpRangeDescByID(route.rangeID);
		if (range.rangeID != route.rangeID)
		{
			/* here we do not modify the tuple */
			kvengine_free_fake_heap(oldtup);
			kvengine_free_fake_heap(newtup);
			range_free(allKey[0].data);
			range_free(allValue[0].data);
			range_free(allKey);
			range_free(allValue);
            //commit_batch(batch_engine);
            REMOVE_THREAD_LOCK_EXEC(Update);

			return HeapTupleMayBeUpdated;
		}
		UpdateStatisticsDelete(range.rangeID, cur_key, cur_value);
	}
	
	if (kvengine_judge_dirty(cur_value.data, oldtup))
	{
		if (enable_paxos)
		{
			int length = 0;
			DistributedTransactionId gxid = update_req->header.gxid;
			void* req = TransferMsgToPaxos(PAXOS_RUN_PUT, cur_key, cur_value, route.rangeID, &length);
			paxos_storage_save_to_batch((char *)req, length, route.rangeID, gxid);
			range_free(req);
		}
		else
			batch_engine->put(batch_engine, cur_key, cur_value);
	}

	if (kvengine_judge_dirty(newValue.data, newtup))
	{
		if (enable_paxos)
		{
			int length = 0;
			DistributedTransactionId gxid = update_req->header.gxid;
			void* req = TransferMsgToPaxos(PAXOS_RUN_PUT, newKey, newValue, route.rangeID, &length);
			paxos_storage_save_to_batch((char *)req, length, route.rangeID, gxid);
			range_free(req);
		}
		else
			batch_engine->put(batch_engine, newKey, newValue);
	}

	for (int i = 1; i < key_count; i++)
	{
		cur_key = allKey[i];
		cur_value = allValue[i];
		engine->delete_direct(engine, cur_key);
	}

	END_CRIT_SECTION();
	REMOVE_THREAD_LOCK_EXEC(VisionJudge);

	//REMOVE_THREAD_LOCK_EXEC(Update);

	/*
	 * Mark old tuple for invalidation from system caches at next command
	 * boundary, and mark the new tuple for invalidation in case we abort. We
	 * have to do this before releasing the buffer because oldtup is in the
	 * buffer.  (heaptup is all in local memory, but it's necessary to process
	 * both tuple versions in one call to inval.c so we can avoid redundant
	 * sinval messages.)
	 */
	//CacheInvalidateHeapTuple(relation, &oldtup, heaptup);

	/*
	 * Release the lmgr tuple lock, if we had it.
	 */
	kv_release_tuplock(cur_key, have_tuple_lock);

	kvengine_free_fake_heap(oldtup);
	kvengine_free_fake_heap(newtup);
	range_free(allKey[0].data);
	range_free(allValue[0].data);
	range_free(allKey);
	range_free(allValue);

	return HeapTupleMayBeUpdated;
}

ResponseHeader*
kvengine_pgprocess_update_req(RequestHeader* req)
{
	HTSU_Result result = HeapTupleMayBeUpdated;
	Delete_UpdateRequest* update_req = (Delete_UpdateRequest*) req;
    if (update_req->result_type == GO_ON)
    {
        core_dump();
    }
	/* 
	 * First, a batch structure is constructed for subsequent secondary index key
	 * acquisition and deletion. 
	 */
	RocksBatchEngine batch;
	KVEngineBatchInterface* batch_engine;
	if (req->writebatch)
	{
		TransactionId gxid = req->gxid;
		batch_engine = engine->create_batch(engine, gxid, true);
	}
	else
	{
		batch.PRIVATE_writebatch_wi = rocksdb_writebatch_wi_create(0,1);
		rocks_engine_init_batch_interface((KVEngineBatchInterface*) &batch);
		batch_engine = &batch.interface;
	}
	
	/* Second, get the value through the primary key */
	char *tmp = update_req->key;
	DataSlice *put_key = get_slice_from_buffer(tmp);
	tmp += size_of_Keylen(*put_key);
	DataSlice *put_value = get_slice_from_buffer(tmp);
	tmp += size_of_Keylen(*put_value);
	
	TupleKeySlice InsertKey = {(TupleKey) put_key->data, put_key->len};
	TupleValueSlice InsertValue = {(TupleValue) put_value->data, put_value->len};

	/* Third, get all the value through the primary key */
	TupleKeySlice* allkey = (TupleKeySlice*)palloc0(mul_size(update_req->key_count - 2, sizeof(TupleKeySlice)));
	TupleValueSlice* allvalue = (TupleValueSlice*)palloc0(mul_size(update_req->key_count - 2, sizeof(TupleValueSlice)));
	Relation rel = kvengine_make_fake_relation();

	int keycount = get_all_second_index(update_req, tmp, update_req->key_count - 2, batch_engine, rel, allkey, allvalue);
	/*
	 * fourth, uniformly delete all kv that need to be deleted. 
	 */

	RocksUpdateFailureData rufd = initRocksUpdateFailureData();
    bool have_tuple_lock = false;
	if (req->writebatch)
		result = kv_update_occ_internal((Delete_UpdateRequest*)update_req, batch_engine, rel, allkey, allvalue, InsertKey, InsertValue, keycount, &rufd, &have_tuple_lock);
	else
		result = kv_update_internal((Delete_UpdateRequest*)update_req, batch_engine, rel, allkey, allvalue, InsertKey, InsertValue, keycount, &rufd, &have_tuple_lock);

	kvengine_free_fake_relation(rel);

	if (!req->writebatch)
		destroy_batch(batch_engine);

	range_free(put_key);
	range_free(put_value);
    
	Size size = sizeof(Delete_UpdateResponse);
	char *buffer = NULL;

	size += getRocksUpdateFailureDataLens(rufd);
	buffer = encodeRocksUpdateFailureData(rufd);

	Delete_UpdateResponse* update_res = palloc0(size);
	update_res->header.type = update_req->header.type;
	update_res->header.size = size;
	update_res->result = result;
    update_res->result_type = UPDATE_COMPLETE;
	memcpy(update_res->rfd, buffer, getRocksUpdateFailureDataLens(rufd));
	return (ResponseHeader*) update_res;
}

ResponseHeader*
kvengine_pgprocess_delete_direct_req(RequestHeader* req)
{
	DeleteDirectRequest* delete_req = (DeleteDirectRequest*) req;

	DataSlice *req_key = get_slice_from_buffer(delete_req->key);

	TupleKeySlice key = {(TupleKey) req_key->data, req_key->len};

	DeleteDirectResponse* delete_res = palloc0(sizeof(*delete_res));
	delete_res->header.type = delete_req->header.type;
	delete_res->header.size = sizeof(*delete_res);

	engine->delete_direct(engine, key);

	delete_res->success = true;

	range_free(req_key);
	return (ResponseHeader*) delete_res;
}

ResponseHeader*
kvengine_pgprocess_scan_req(RequestHeader* req)
{
	ScanWithKeyRequest *scan_req = (ScanWithKeyRequest*) req;
	KVScanDesc desc = init_kv_scan(scan_req->isforward);

	// init iter with batch
	TransactionId gxid = req->gxid;
	KVEngineBatchInterface* batch_engine = NULL;
	if (req->writebatch)
	{
		batch_engine = engine->create_batch(engine, gxid, false);
		if (batch_engine)
			desc->engine_it = batch_engine->create_batch_iterator(desc->engine_it, batch_engine);
	}

	TupleKeySlice start_key = {0};
	TupleKeySlice end_key = {0};
	TupleKeySlice os_key = {0};
	TupleKeySlice oe_key = {0};
	get_key_interval_from_scan_req(scan_req, &start_key, &end_key, &os_key, &oe_key);
	desc->engine_it->seek(desc->engine_it, start_key);
	Dataslice key = NULL;
	Dataslice value = NULL;
	Oid rangeid = 0;
	key_cmp_func cmp_func;
	if (scan_req->issecond)
		cmp_func = scan_req->isforward ? get_maxnum_second_forward : get_maxnum_second_backward;
	else
		cmp_func = scan_req->isforward ? get_maxnum_forward : get_maxnum_backward;

	int i = 0;
    TupleKeySlice prevkey = {NULL, 0};
	for (i = 0; i < scan_req->max_num &&
					scan_get_next_valid_pg_transaction(
									&desc->engine_it, batch_engine, end_key, &prevkey, 
                                    desc->fake_rel, &scan_req->snapshot,
									&key, &value, &rangeid,
									scan_req->header.gp_session_id, scan_req->header.pid,
									scan_req->header.combocid_map_count, cmp_func); i++)
	{
		store_kv(desc, key, value, rangeid);
	}

	if (i == scan_req->max_num
		&& (scan_get_next_valid_pg_transaction(
				&desc->engine_it, batch_engine, end_key, &prevkey, desc->fake_rel,
				&scan_req->snapshot, &key, &value, &rangeid,
				scan_req->header.gp_session_id, scan_req->header.pid,
				scan_req->header.combocid_map_count, cmp_func)
			))
	{
		desc->next_key = key;
		range_free(value);
	}
	else
	{
		desc->next_key = palloc0(sizeof(DataSlice));
		desc->next_key->len = 0;
	}
	// range_free(start_key.data);
	// range_free(end_key.data);

	return (ResponseHeader*) finish_kv_scan(desc, scan_req->header.type);
}

ResponseHeader*
kvengine_pgprocess_multi_get_req(RequestHeader* req)
{
	ScanWithKeyRequest *scan_req = (ScanWithKeyRequest*) req;
	KVScanDesc desc = init_kv_scan(true);
	char *buffer = scan_req->start_and_end_key;
	Dataslice key = NULL;
	Dataslice value = NULL;
	Oid rangeid = 0;

	// in occ mode : init iter with batch
	TransactionId gxid = req->gxid;
	KVEngineBatchInterface* batch_engine = NULL;
	if (req->writebatch)
	{
		batch_engine = engine->create_batch(engine, gxid, false);
		if (batch_engine)
			desc->engine_it = batch_engine->create_batch_iterator(desc->engine_it, batch_engine);
	}
	
	for (int i = 0; i < scan_req->max_num; ++i)
	{
		TupleKeySlice pkey = get_tuple_key_from_buffer(buffer);
		buffer += pkey.len + sizeof(Size);
		desc->engine_it->seek(desc->engine_it, pkey);
        TupleKeySlice prevkey = {NULL, 0};
		bool valid = scan_get_next_valid_pg_transaction(
									&desc->engine_it, batch_engine, 
                                    pkey, &prevkey, desc->fake_rel,
									&scan_req->snapshot,
									&key, &value, &rangeid,
									scan_req->header.gp_session_id, scan_req->header.pid,
									scan_req->header.combocid_map_count, get_one);
		Assert(valid);
		store_kv(desc, key, value, rangeid);
		range_free(pkey.data);
	}
	return (ResponseHeader*) finish_kv_scan(desc, scan_req->header.type);
}
/*
ResponseHeader*
kvengine_pgprocess_delete_one_tuple_all_index(RequestHeader* req)
{
	KVScanDesc desc = init_kv_scan(true);
	ScanWithPkeyRequest *scan_req = (ScanWithPkeyRequest*) req;
	char* key_buf = scan_req->start_and_end_key;
	TupleKeySlice start_key = get_tuple_key_from_buffer(key_buf);
	desc->engine_it->seek(desc->engine_it, start_key);
	Dataslice key = NULL;
	Dataslice value = NULL;
	Oid rangeid = 0;
    TupleKeySlice prevkey = {NULL, 0};
	while (scan_get_next_valid_pg_transaction(&desc->engine_it, start_key, &prevkey, desc->fake_rel,
									&scan_req->snapshot, &key, &value, &rangeid,
									scan_req->header.gp_session_id, scan_req->header.pid,
									scan_req->header.combocid_map_count, travers_table))
	{
		if (check_pk_value_of_second_index(key, start_key))
		{
			store_kv(desc, key, value, rangeid);
		}
	}
	range_free(start_key.data);
	return (ResponseHeader*) finish_kv_scan(desc, scan_req->header.type);
}
*/
static bool
kv_normal_fetch_tuple(KVEngineBatchInterface* batch_engine,
						TupleKeySlice *cur_key, 
                        TupleValueSlice *cur_value, 
                        Relation rel, 
                        Snapshot Snapshot,
                        int session_id,
                        int session_pid,
                        int combocid_map_count)
{
	if (batch_engine)
		*cur_value = batch_engine->get(engine, batch_engine, *cur_key);
	else
		*cur_value = engine->get(engine, *cur_key);

	HeapTuple htup = NULL;
	kvengine_free_fake_heap(htup);
	htup = kvengine_make_fake_heap(*cur_key, *cur_value);
    if (htup == NULL)
	{
        return false;
	}
	SessionMessageData sm;
	sm.gp_session_id = session_id;
	sm.pid = session_pid;
	sm.combocid_map_count = combocid_map_count;
    bool valid = true;
	ADD_THREAD_LOCK_EXEC(VisionJudge);
    if (Snapshot->satisfies != NULL)
	{
	    valid = HeapTupleSatisfiesVisibility(rel, htup, Snapshot, InvalidBuffer, &sm);
	}
	REMOVE_THREAD_LOCK_EXEC(VisionJudge);

	if (kvengine_judge_dirty(cur_value->data, htup))
	{
		engine->put(engine, *cur_key, *cur_value);
	}
	return valid;
}


bool
scan_get_next_valid_pg_transaction(KVEngineIteratorInterface **engine_it,
					KVEngineBatchInterface* batch_engine,
					TupleKeySlice cmp_key,
                    TupleKeySlice *prev_key,
					Relation rel,
					Snapshot snapshot,
					Dataslice *key,
					Dataslice *value,
					Oid *rangeid,
					int session_id,
					int session_pid,
					int combocid_map_count,
					key_cmp_func is_end)
{
	for (; (*engine_it)->is_valid(*engine_it); (*engine_it)->next(*engine_it))
	{
		bool valid = false;
		TupleKeySlice tempkey = {NULL, 0};
		TupleValueSlice tempvalue = {NULL, 0};
		/* Read KV from RocksDB. */
		(*engine_it)->get(*engine_it, &tempkey, &tempvalue);

		bool noend = (*is_end)(tempkey, cmp_key);
		if (!noend)
		{
			return false;
		}
        if (prev_key->data != NULL)
        {
            Size prevK_len = prev_key->len - sizeof(TransactionId) - sizeof(CommandId);
            Size tempK_len = tempkey.len  - sizeof(TransactionId) - sizeof(CommandId);
            if (prevK_len == tempK_len && memcmp(prev_key->data, tempkey.data, prevK_len) == 0)
                continue;
        }
		if (enable_range_distribution && enable_paxos && !checkRouteVisible(tempkey))
			continue;
        
        TupleKeySlice copykey = copy_key(tempkey);
        TupleValueSlice copyvalue = copy_value(tempvalue);
        TupleKeySlice last_valid_key = {NULL, 0};
        TupleValueSlice last_valid_value = {NULL, 0};
        TransactionId priorXmax = 0;
        if (copykey.data == NULL || copykey.len == 0 || copyvalue.data == NULL || copyvalue.len == 0)
            continue;
        for (;;)
        {
            if (kv_normal_fetch_tuple(batch_engine, &copykey, &tempvalue, rel, snapshot, session_id, session_pid, combocid_map_count))
            {
                if (last_valid_key.data)
                {
                    range_free(last_valid_key.data);
                }
                if (last_valid_value.data)
                {
                    range_free(last_valid_value.data);
                }
                last_valid_key = copy_key(copykey);
                last_valid_value = copy_value(tempvalue);
            }
            if (tempvalue.data && tempvalue.data->sysattrs.has_new_cid)
            {
                kv_link_next(&copykey, &tempvalue, &priorXmax);
            }
            else
                break;
        }
        if (last_valid_key.data && last_valid_value.data)
            valid = true;
        else
            valid = false;
		/* If tuple can be seen, store mem tuple to slot. */
		if (valid)
		{
            *prev_key = copy_key(last_valid_key);
			if (enable_range_distribution)
			{
				RangeDesc *route = FindRangeRouteByKey(tempkey);
				if (route != NULL)
					*rangeid = route->rangeID;
				else
					*rangeid = 0;
				freeRangeDescPoints(route);
			}
			*key = palloc0(size_of_Keylen(last_valid_key));
			*value = palloc0(size_of_Keylen(last_valid_value));
			make_data_slice(**key, last_valid_key);
			make_data_slice(**value, last_valid_value);

			(*engine_it)->next(*engine_it);
			return true;
		}
	}
	return false;
}


static bool
check_pk_value_of_second_index(Dataslice sourceKeySlice, TupleKeySlice destkey)
{
	char *s_value = NULL, *d_value = NULL;
	TupleKeySlice sourcekey = {0};
	save_data_slice(sourcekey, *sourceKeySlice);

	Size s_len = 0, d_len = 0;
	if (sourcekey.data->indexOid == destkey.data->indexOid)
		return false;
	get_second_key_pk_value(sourcekey, s_value, &s_len);
	get_primary_key_pk_value(destkey, d_value, &d_len);

	int result = memcmp(s_value, d_value, get_min(s_len, d_len));
	return result == 0;
}

ResponseHeader*
kvengine_pgprocess_commit(RequestHeader* req)
{
	ResponseHeader* res = palloc0(sizeof(*res));
	res->type = req->type;
	res->size = sizeof(*res);

	DistributedTransactionId gxid = req->gxid;
	KVEngineBatchInterface* batch_engine = engine->create_batch(engine, gxid, false);
	if (batch_engine)
	{
		batch_engine->commit_and_destroy(engine, batch_engine, gxid);
		if (enable_paxos)
			paxos_storage_commit_batch(gxid);
	}
    // todo: check if batch_engine create fail and add the return value of commit operation.
	return res;
}

ResponseHeader*
kvengine_pgprocess_abort(RequestHeader* req)
{
	ResponseHeader* res = palloc0(sizeof(*res));
	res->type = req->type;
	res->size = sizeof(*res);

	DistributedTransactionId gxid = req->gxid;
	KVEngineBatchInterface* batch_engine = engine->create_batch(engine, gxid, false);
	if (batch_engine)
	{
		batch_engine->abort_and_destroy(engine, batch_engine, gxid);
		if (enable_paxos)
			paxos_storage_commit_batch(gxid);
	}
    // todo: check if batch_engine create fail and add the return value of commit operation.
	return res;
}

static bool
scan_get_next_valid(KVEngineIteratorInterface **engine_it,
					TupleKeySlice cmp_key,
					Relation rel,
					Snapshot snapshot,
					Dataslice *key,
					Dataslice *value,
					Oid *rangeid,
					int session_id,
					int session_pid,
					int combocid_map_count,
					key_cmp_func is_end)
{
	for (; (*engine_it)->is_valid(*engine_it); (*engine_it)->next(*engine_it))
	{
		bool valid = false;
		TupleKeySlice tempkey = {NULL, 0};
		TupleValueSlice tempvalue = {NULL, 0};
		/* Read KV from RocksDB. */
		(*engine_it)->get(*engine_it, &tempkey, &tempvalue);

		bool noend = (*is_end)(tempkey, cmp_key);
		if (!noend)
		{
			return false;
		}
		if (enable_range_distribution && enable_paxos && !checkRouteVisible(tempkey))
			continue;

		valid = true;

		if (snapshot->satisfies != NULL)
		{
			HeapTuple htup = kvengine_make_fake_heap(tempkey, tempvalue);
			SessionMessageData sm;

			sm.gp_session_id = session_id;
			sm.pid = session_pid;
			sm.combocid_map_count = combocid_map_count;

			ADD_THREAD_LOCK_EXEC(VisionJudge);
			valid = HeapTupleSatisfiesVisibility(rel, htup, snapshot, InvalidBuffer, &sm);
			REMOVE_THREAD_LOCK_EXEC(VisionJudge);

			if (kvengine_judge_dirty(tempvalue.data, htup))
			{
				engine->put(engine, tempkey, tempvalue);
			}
			
			kvengine_free_fake_heap(htup);
		}

		/* If tuple can be seen, store mem tuple to slot. */
		if (valid)
		{
			if (enable_range_distribution)
			{
				RangeDesc *route = FindRangeRouteByKey(tempkey);
				if (route != NULL)
					*rangeid = route->rangeID;
				else
					*rangeid = 0;
				freeRangeDescPoints(route);
			}
			*key = palloc0(size_of_Keylen(tempkey));
			*value = palloc0(size_of_Keylen(tempvalue));
			make_data_slice(**key, tempkey);
			make_data_slice(**value, tempvalue);

			(*engine_it)->next(*engine_it);
			return true;
		}
	}
	return false;
}