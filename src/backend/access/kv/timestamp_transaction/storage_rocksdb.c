
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
#include "tdb/timestamp_transaction/storage_rocksdb.h"
#include "tdb/timestamp_transaction/timestamp_generate_key.h"


#include "librw/librm.h"


static bool get_one(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	if (rocksdb_key.data->rel_id != target_key.data->rel_id ||
		rocksdb_key.data->indexOid != target_key.data->indexOid)
		return false;
	Size r_len = 0, t_len = 0;
	void *key_pk_value = get_TupleKeySlice_primarykey_prefix_raw(rocksdb_key, &r_len);
	void *start_key_pk_value = get_TupleKeySlice_primarykey_prefix_raw(target_key, &t_len);
	bool result = memcmp(key_pk_value, start_key_pk_value, get_min(r_len, t_len)) == 0;

	return result;
}

static bool get_one_secondary(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	if (rocksdb_key.data->rel_id != target_key.data->rel_id ||
		rocksdb_key.data->indexOid != target_key.data->indexOid)
		return false;
	Size r_len = 0, t_len = 0;
	void *key_pk_value = get_TupleKeySlice_secondarykey_prefix_raw(rocksdb_key, &r_len);
	void *start_key_pk_value = get_TupleKeySlice_secondarykey_prefix_raw(target_key, &t_len);
	bool result = memcmp(key_pk_value, start_key_pk_value, get_min(r_len, t_len)) == 0;
	return result;
}

static bool travers_table(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	return rocksdb_key.data->rel_id == target_key.data->rel_id;
}

static bool get_maxnum(TupleKeySlice rocksdb_key, TupleKeySlice target_key, bool isforward)
{
	if (rocksdb_key.data->rel_id != target_key.data->rel_id ||
		rocksdb_key.data->indexOid != target_key.data->indexOid)
		return false;

	Size r_len = 0, t_len = 0;
	void *key_pk_value = get_TupleKeySlice_primarykey_prefix_raw(rocksdb_key, &r_len);
	void *start_key_pk_value = get_TupleKeySlice_primarykey_prefix_raw(target_key, &t_len);

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
	void *key_pk_value = get_TupleKeySlice_secondarykey_prefix_raw(rocksdb_key, &r_len);
	void *start_key_pk_value = get_TupleKeySlice_secondarykey_prefix_raw(target_key, &t_len);

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


typedef bool (*key_cmp_func)(TupleKeySlice, TupleKeySlice);

static bool opt_scan_get_next_valid(KVEngineIteratorInterface **engine_it,
									KVEngineTransactionInterface *txn,
									TupleKeySlice cmp_key,
									Dataslice *key,
									Dataslice *value,
									Oid *rangeid,
									key_cmp_func is_end,
									CmdType Scantype,
									bool writebatch, 
									uint64_t *cts, 
									uint64_t *nts);
static bool kv_optengine_check_unique(KVEngineTransactionInterface *txn,
									  TupleKeySlice rocksdb_key, uint64_t *cts, uint64_t *nts);

KVScanDesc
init_kv_optprocess_scan(KVEngineTransactionInterface *txn, bool isforward)
{
	KVScanDesc desc = palloc0(sizeof(*desc));
	desc->engine_it = rocks_transaction_create_iterator(txn, isforward, ROCKS_DEFAULT_CF_I);
	if (desc->engine_it == NULL)
	{
		pfree(desc);
		return NULL;
	}
	desc->fake_rel = kvengine_make_fake_relation();
	desc->kv_count = 0;
	desc->res_size = sizeof(ScanResponse);
	desc->next_key = NULL;
	desc->cts = 0;
	desc->nts = kMaxSequenceNumber;
	return desc;
}

ResponseHeader *
kvengine_optprocess_get_req(RequestHeader *req)
{
	GetRequest *get_req = (GetRequest *)req;
	// DataSlice *req_key = get_slice_from_buffer(get_req->key);

	TupleKeySlice key = pick_tuple_key_from_buffer(get_req->key);
	TupleValueSlice value;

	KVEngineTransactionInterface *txn =
		engine->create_txn(engine, req->gxid, true, req->start_ts);
	// acquire_txnengine_mutex(txn);
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, req->lower_lts, req->upper_lts);
	KeyXidCache_addReadXidWithMutex(key.data, key.len, key.data, 0, (uint64_t)req->gxid);
	insert_readset(txn, key, key, true, false);
	// TODO:maintain the DTA message 
	uint64_t cts = 0, nts = kMaxSequenceNumber;
	value = txn->get(txn, key, ROCKS_DEFAULT_CF_I, &cts, &nts);

	GetResponse *get_res = palloc0(sizeof(GetResponse) + size_of_Keylen(value));
	get_res->header.type = get_req->header.type;
	get_res->header.size = sizeof(GetResponse) + size_of_Keylen(value);
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, cts, nts);
	get_txnengine_ts(txn, &cts, &nts);
	// TODO:maintain the DTA message 
	get_res->header.cts = cts;
	get_res->header.nts = nts;
	save_tuple_value_into_buffer(/*TupleValueSlice*/ get_res->value, value);
	// release_txnengine_mutex(txn);
	return (ResponseHeader *)get_res;
}

static bool
kv_optengine_check_unique(KVEngineTransactionInterface *txn,
						  TupleKeySlice rocksdb_key, uint64_t *cts, uint64_t *nts)
{
	TupleValueSlice value = txn->get(txn, rocksdb_key, ROCKS_DEFAULT_CF_I, cts, nts);
	if (value.data && value.len != 0)
		return false;
	else
		return true;
}

ResponseHeader *
kvengine_optprocess_put_req(RequestHeader *req)
{
	PutRequest *put_req = (PutRequest *)req;

	TupleKeySlice key = pick_tuple_key_from_buffer(put_req->k_v);
	Size index = size_of_Keylen(key);
	TupleValueSlice value = pick_tuple_value_from_buffer(put_req->k_v + index);

	PutResponse *put_res = palloc0(sizeof(*put_res));
	put_res->header.type = put_req->header.type;
	put_res->header.size = sizeof(*put_res);
	Assert(key.len < MAX_SLICE_LEN);
	Assert(value.len < MAX_SLICE_LEN);
	KVEngineTransactionInterface *txn =
		engine->create_txn(engine, req->gxid, true, req->start_ts);
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, req->lower_lts, req->upper_lts);
	KeyXidCache_addReadXidWithMutex(key.data, key.len, key.data, 0, (uint64_t)req->gxid);
	insert_readset(txn, key, key, true, false);
	insert_writeset(txn, key, key, true, false);
	// TODO:maintain the DTA message 
	uint64_t cts = 0, nts = kMaxSequenceNumber;
	if (!kv_optengine_check_unique(txn, key, &cts, &nts))
	{
		put_res->checkUnique = false;
	}
	else
	{
		if (enable_paxos)
		{
			int length = 0;
			void *req = TransferMsgToPaxos(PAXOS_RUN_PUT, key, value,
										   put_req->rangeid, &length);
			int result = 0;
			if (put_req->rangeid > 0 && put_req->rangeid < MAXRANGECOUNT)
				result = paxos_storage_runpaxos(req, length, put_req->rangeid);
			else
				txn->put(txn, key, value, ROCKS_DEFAULT_CF_I);
			range_free(req);
		}
		else
			txn->put(txn, key, value, ROCKS_DEFAULT_CF_I);
		put_res->checkUnique = true;
	}
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, cts, nts);
	get_txnengine_ts(txn, &cts, &nts);
	// TODO:maintain the DTA message 
	put_res->header.cts = cts;
	put_res->header.nts = nts;
	// release_txnengine_mutex(txn);
	return (ResponseHeader *)put_res;
}

static bool 
optprocess_maintain_all_second_index(char *buffer,
									 int count,
									 uint32 gxid,
									 KVEngineTransactionInterface *txn,
									 bool isdelete)
{
	char *tmp = buffer;
	TupleKeySlice key;
	pick_slice_from_buffer(tmp, key);
	tmp += size_of_Keylen(key);
	TupleKeySlice value;
	pick_slice_from_buffer(tmp, value);
	tmp += size_of_Keylen(value);

	if (count == 2)
		return count - 1;

	for (int i = 2; i < count; ++i)
	{
		TupleKeySlice pkey = pick_tuple_key_from_buffer(tmp);
		tmp += size_of_Keylen(pkey);

		KeyXidCache_addReadXidWithMutex(pkey.data, pkey.len, pkey.data, 0, (uint64_t)gxid);
		insert_readset(txn, pkey, pkey, true, isdelete);
		insert_writeset(txn, pkey, pkey, true, isdelete);
	}
	return true;
}
static int
optprocess_get_all_second_index(Delete_UpdateRequest *delete_req,
								char *buffer,
								int count,
								KVEngineTransactionInterface *txn,
								TupleKeySlice *allkey,
								TupleValueSlice *allvalue,
								bool writebatch,
								uint64_t *cts, uint64_t *nts)
{
	char *tmp = buffer;

	pick_slice_from_buffer(tmp, allkey[0]);
	tmp += size_of_Keylen(allkey[0]);

	pick_slice_from_buffer(tmp, allvalue[0]);
	tmp += size_of_Keylen(allvalue[0]);

	if (count == 2)
		return count - 1;

	// if ((scan_req->Scantype == CMD_UPDATE || scan_req->Scantype == CMD_DELETE) &&
	// 	req->transaction_op_type == TRANSACTION_TYPE_P)
	// {
	// 	if (judge_delete_update_write_lock(delete_req, tmp);)
	// 		//TODO:there we need abort
	// }
	
	KVScanDesc desc = init_kv_optprocess_scan(txn, false);
	if (desc == NULL)
	{
		return 0;
	}
	Dataslice key = NULL;
	Dataslice value = NULL;
	int index = 1;
	uint64_t ccts = 0, nnts = kMaxSequenceNumber;
	for (int i = 2; i < count; ++i)
	{
		TupleKeySlice pkey = pick_tuple_key_from_buffer(tmp);
		tmp += size_of_Keylen(pkey);

		desc->engine_it->seek(desc->engine_it, pkey);
		bool valid = opt_scan_get_next_valid(&desc->engine_it,
											 txn, pkey,
											 &key, &value,
											 &delete_req->rangeid,
											 get_one, CMD_UPDATE,
											 writebatch, &ccts, &nnts);
		if (valid)
		{
			TupleKeySlice cur_key = {(TupleKey)key->data, key->len};
			TupleValueSlice cur_value = {(TupleValue)value->data, value->len};
			allkey[index] = cur_key;
			allvalue[index] = cur_value;
			*cts = *cts > ccts ? *cts : ccts;
			*nts = *nts < nnts ? *nts : nnts;
			index++;
		}
	}
	free_kv_desc(desc);
	return index;
}

ResponseHeader *
kvengine_optprocess_delete_normal_req(RequestHeader *req)
{
	HTSU_Result result = HeapTupleMayBeUpdated;
	Delete_UpdateRequest *delete_req = (Delete_UpdateRequest *)req;

	/* Second, get all the value through the primary key */
	TupleKeySlice *allkey = (TupleKeySlice *)palloc0(
		mul_size(delete_req->key_count, sizeof(TupleKeySlice)));
	TupleValueSlice *allvalue = (TupleValueSlice *)palloc0(
		mul_size(delete_req->key_count, sizeof(TupleValueSlice)));
	KVEngineTransactionInterface *txn = engine->create_txn(engine, req->gxid, true, req->start_ts);
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, req->lower_lts, req->upper_lts);
	optprocess_maintain_all_second_index(delete_req->key, delete_req->key_count, delete_req->header.gxid, txn, true);
	// TODO:maintain the DTA message 
	uint64_t cts = 0, nts = kMaxSequenceNumber;
	int keycount = optprocess_get_all_second_index(delete_req,
												   delete_req->key,
												   delete_req->key_count,
												   txn, allkey, allvalue,
												   req->writebatch,
												   &cts, &nts);

	/*
	 * Third, uniformly delete all kv that need to be deleted. 
	 */
	RocksUpdateFailureData rufd = initRocksUpdateFailureData();

	TupleKeySlice cur_key;
	TupleValueSlice cur_value;
	for (int i = 0; i < keycount; i++)
	{
		cur_key = allkey[i];
		cur_value = allvalue[i];
		Assert(cur_key.len < MAX_SLICE_LEN);
		Assert(cur_value.len < MAX_SLICE_LEN);
		txn->delete (txn, cur_key, ROCKS_DEFAULT_CF_I);
	}
	range_free(allkey[0].data);
	range_free(allvalue[0].data);
	range_free(allkey);
	range_free(allvalue);

	Size size = sizeof(Delete_UpdateResponse);
	char *buffer = NULL;
	size += getRocksUpdateFailureDataLens(rufd);
	buffer = encodeRocksUpdateFailureData(rufd);

	Delete_UpdateResponse *delete_res = palloc0(size);
	delete_res->header.type = delete_req->header.type;
	delete_res->header.size = size;
	delete_res->result = result;
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, cts, nts);
	get_txnengine_ts(txn, &cts, &nts);
	// TODO:maintain the DTA message 
	delete_res->header.cts = cts;
	delete_res->header.nts = nts;
	memcpy(delete_res->rfd, buffer, getRocksUpdateFailureDataLens(rufd));
	// release_txnengine_mutex(txn);
	return (ResponseHeader *)delete_res;
}

ResponseHeader *
kvengine_optprocess_update_req(RequestHeader *req)
{
	HTSU_Result result = HeapTupleMayBeUpdated;
	Delete_UpdateRequest *update_req = (Delete_UpdateRequest *)req;

	/* Second, get the value through the primary key */
	char *tmp = update_req->key;

	TupleKeySlice InsertKey = pick_tuple_key_from_buffer(tmp);
	tmp += size_of_Keylen(InsertKey);
	TupleValueSlice InsertValue = pick_tuple_value_from_buffer(tmp);
	tmp += size_of_Keylen(InsertValue);
	Assert(InsertKey.len < MAX_SLICE_LEN);
	Assert(InsertValue.len < MAX_SLICE_LEN);

	/* Third, get all the value through the primary key */
	TupleKeySlice *allkey = (TupleKeySlice *)palloc0(
		mul_size(update_req->key_count - 2, sizeof(TupleKeySlice)));
	TupleValueSlice *allvalue = (TupleValueSlice *)palloc0(
		mul_size(update_req->key_count - 2, sizeof(TupleValueSlice)));
	KVEngineTransactionInterface *txn =
		engine->create_txn(engine, req->gxid, true, req->start_ts);
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, req->lower_lts, req->upper_lts);
	// insert_writeset(txn, InsertKey, InsertKey, true, false);
	optprocess_maintain_all_second_index(update_req->key, update_req->key_count, update_req->header.gxid, txn, false);
	// TODO:maintain the DTA message 
	uint64_t cts = 0, nts = kMaxSequenceNumber;
	int keycount = optprocess_get_all_second_index(update_req, tmp,
												   update_req->key_count - 2,
												   txn, allkey, allvalue,
												   req->writebatch,
												   &cts, &nts);

	/*
	 * fourth, uniformly delete all kv that need to be deleted. 
	 */
	RocksUpdateFailureData rufd = initRocksUpdateFailureData();
	TupleKeySlice cur_key;
	TupleValueSlice cur_value;
	for (int i = 1; i < keycount; i++)
	{
		cur_key = allkey[i];
		cur_value = allvalue[i];
		Assert(cur_key.len < MAX_SLICE_LEN);
		Assert(cur_value.len < MAX_SLICE_LEN);
		txn->delete (txn, cur_key, ROCKS_DEFAULT_CF_I);
	}
	txn->put(txn, InsertKey, InsertValue, ROCKS_DEFAULT_CF_I);
	range_free(allkey[0].data);
	range_free(allvalue[0].data);
	range_free(allkey);
	range_free(allvalue);

	Size size = sizeof(Delete_UpdateResponse);
	char *buffer = NULL;

	size += getRocksUpdateFailureDataLens(rufd);
	buffer = encodeRocksUpdateFailureData(rufd);

	Delete_UpdateResponse *update_res = palloc0(size);
	update_res->header.type = update_req->header.type;
	update_res->header.size = size;
	update_res->result = result;
	update_res->result_type = UPDATE_COMPLETE;
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, cts, nts);
	get_txnengine_ts(txn, &cts, &nts);
	// TODO:maintain the DTA message 
	update_res->header.cts = cts;
	update_res->header.nts = nts;
	memcpy(update_res->rfd, buffer, getRocksUpdateFailureDataLens(rufd));
	// release_txnengine_mutex(txn);
	return (ResponseHeader *)update_res;
}

ResponseHeader *
kvengine_optprocess_delete_direct_req(RequestHeader *req)
{
	DeleteDirectRequest *delete_req = (DeleteDirectRequest *)req;

	TupleKeySlice key = pick_tuple_key_from_buffer(delete_req->key);

	DeleteDirectResponse *delete_res = palloc0(sizeof(*delete_res));
	delete_res->header.type = delete_req->header.type;
	delete_res->header.size = sizeof(*delete_res);
	Assert(key.len < MAX_SLICE_LEN);

	engine->delete_direct(engine, key);

	delete_res->success = true;

	return (ResponseHeader *)delete_res;
}

ResponseHeader *
kvengine_optprocess_scan_req(RequestHeader *req)
{
	ScanWithKeyRequest *scan_req = (ScanWithKeyRequest *)req;
	uint64_t startts = req->start_ts;
	TupleKeySlice start_key = {0};
	TupleKeySlice end_key = {0};
	TupleKeySlice os_key = {0};
	TupleKeySlice oe_key = {0};
	get_key_interval_from_scan_req(scan_req, &start_key, &end_key, &os_key, &oe_key);

	KVEngineTransactionInterface *txn =
		engine->create_txn(engine, req->gxid, true, startts);
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, req->lower_lts, req->upper_lts);
	// TODO:here we need to judge the os_key == oe_key
	bool isSingle = equal_key(os_key, oe_key);
	insert_readset(txn, os_key, oe_key, isSingle, false);
	KeyXidCache_addReadXidWithMutex(os_key.data, os_key.len, oe_key.data, 
									isSingle ? 0 : oe_key.len, req->gxid);
	// TODO:maintain the DTA message 
	KVScanDesc desc = init_kv_optprocess_scan(txn, scan_req->isforward);
	if (desc == NULL)
	{
		ScanResponse *scan_res = palloc0(sizeof(ScanResponse));
		scan_res->header.type = scan_req->header.type;
		scan_res->header.size = sizeof(ScanResponse);
		scan_res->num = 0;
		return (ResponseHeader *)scan_res;
	}

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
	uint64_t cts = 0, nts = kMaxSequenceNumber;
	uint64_t ccts = 0, nnts = kMaxSequenceNumber;
	for (i = 0; i < scan_req->max_num &&
				opt_scan_get_next_valid(&desc->engine_it,
										txn, end_key,
										&key, &value,
										&rangeid, cmp_func,
										scan_req->Scantype,
										req->writebatch,
										&ccts, &nnts);
		 i++)
	{
		cts = cts > ccts ? cts : ccts;
		nts = nts < nnts ? nts : nnts;
		store_kv(desc, key, value, rangeid);
	}

	if (i == scan_req->max_num &&
		opt_scan_get_next_valid(&desc->engine_it,
								txn, end_key,
								&key, &value,
								&rangeid, cmp_func,
								scan_req->Scantype,
								req->writebatch,
								&ccts, &nnts))
	{
		desc->next_key = key;
		range_free(value);
	}
	else
	{
		desc->next_key = palloc0(sizeof(DataSlice));
		desc->next_key->len = 0;
	}
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, cts, nts);
	get_txnengine_ts(txn, &cts, &nts);
	desc->cts = cts;
	desc->nts = nts;
	// TODO:maintain the DTA message 
	// release_txnengine_mutex(txn);
	return (ResponseHeader *)finish_kv_scan(desc, scan_req->header.type);
}

static bool maintain_multi_get_req(char *buffer, int count, uint32 gxid,
								   KVEngineTransactionInterface *txn)
{
	bool success = false;
	for (int i = 0; i < count; ++i)
	{
		TupleKeySlice pkey = get_tuple_key_from_buffer(buffer);
		buffer += pkey.len + sizeof(Size);
		KeyXidCache_addReadXidWithMutex(pkey.data, pkey.len, pkey.data, 0, (uint64_t)gxid);
		insert_readset(txn, pkey, pkey, true, false);
		range_free(pkey.data);
	}

	return success;
}

ResponseHeader *
kvengine_optprocess_multi_get_req(RequestHeader *req)
{
	ScanWithKeyRequest *scan_req = (ScanWithKeyRequest *)req;
	char *buffer = scan_req->start_and_end_key;

	uint64_t startts = req->start_ts;

	KVEngineTransactionInterface *txn = 
					engine->create_txn(engine, req->gxid, true, startts);
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, req->lower_lts, req->upper_lts);
	maintain_multi_get_req(scan_req->start_and_end_key, scan_req->max_num, scan_req->header.gxid, txn);
	// TODO:maintain the DTA message 
	KVScanDesc desc = init_kv_optprocess_scan(txn, true);
	if (desc == NULL)
	{
		ScanResponse *scan_res = palloc0(sizeof(ScanResponse));
		scan_res->header.type = scan_req->header.type;
		scan_res->header.size = sizeof(ScanResponse);
		scan_res->num = 0;
		return (ResponseHeader *)scan_res;
	}
	buffer = scan_req->start_and_end_key;
	Dataslice key = NULL;
	Dataslice value = NULL;
	Oid rangeid = 0;
	uint64_t cts = 0, nts = kMaxSequenceNumber;
	uint64_t ccts = 0, nnts = kMaxSequenceNumber;
	for (int i = 0; i < scan_req->max_num; ++i)
	{
		TupleKeySlice pkey = get_tuple_key_from_buffer(buffer);
		buffer += pkey.len + sizeof(Size);

		desc->engine_it->seek(desc->engine_it, pkey);
		bool valid = opt_scan_get_next_valid(&desc->engine_it, txn, pkey,
											 &key, &value, &rangeid, get_one,
											 scan_req->Scantype, req->writebatch,
											 &ccts, &nnts);
		if (valid)
		{
			cts = cts > ccts ? cts : ccts;
			nts = nts < nnts ? nts : nnts;
			store_kv(desc, key, value, rangeid);
		}
		range_free(pkey.data);
	}
	// TODO:maintain the DTA message 
	set_txnengine_ts(txn, cts, nts);
	get_txnengine_ts(txn, &cts, &nts);
	desc->cts = cts;
	desc->nts = nts;
	// TODO:maintain the DTA message 
	// release_txnengine_mutex(txn);
	return (ResponseHeader *)finish_kv_scan(desc, scan_req->header.type);
}

static bool
opt_scan_get_next_valid(KVEngineIteratorInterface **engine_it,
						KVEngineTransactionInterface *txn,
						TupleKeySlice cmp_key,
						Dataslice *key,
						Dataslice *value,
						Oid *rangeid,
						key_cmp_func is_end,
						CmdType Scantype,
						bool writebatch,
						uint64_t *cts,
						uint64_t *nts)
{
	for (; (*engine_it)->is_valid(*engine_it); (*engine_it)->next(*engine_it))
	{
		TupleKeySlice tempkey = {NULL, 0};
		TupleValueSlice tempvalue = {NULL, 0};
		/* Read KV from RocksDB. */
		uint64_t ccts = 0, nnts = kMaxSequenceNumber;
		(*engine_it)->get(*engine_it, &tempkey, &tempvalue);

		bool noend = (*is_end)(tempkey, cmp_key);
		if (!noend)
		{
			return false;
		}
		if (enable_range_distribution && enable_paxos && !checkRouteVisible(tempkey))
			continue;
		if (enable_range_distribution)
		{
			RangeDesc *route = FindRangeRouteByKey(tempkey);
			if (route != NULL)
				*rangeid = route->rangeID;
			else
				*rangeid = 0;
			freeRangeDescPoints(route);
		}

		if (!writebatch && (Scantype == CMD_UPDATE || Scantype == CMD_DELETE))
		{
			tempvalue = txn->get_for_update(txn, tempkey, &ccts, &nnts);
			if (cts != NULL && nts != NULL)
			{
				*cts = ccts;
				*nts = nnts;
			}
		}
		if (tempvalue.data == NULL || tempvalue.len == 0)
			continue;
		(*engine_it)->ts(*engine_it, &ccts, &nnts);
		if (cts != NULL && nts != NULL)
		{
			*cts = ccts;
			*nts = nnts;
		}
		*key = palloc0(size_of_Keylen(tempkey));
		*value = palloc0(size_of_Keylen(tempvalue));
		make_data_slice(**key, tempkey);
		make_data_slice(**value, tempvalue);

		(*engine_it)->next(*engine_it);
		return true;
	}
	return false;
}

ResponseHeader *
kvengine_optprocess_prepare(RequestHeader *req)
{
	CommitResponse *res = palloc0(sizeof(*res));
	res->header.type = req->type;
	res->header.size = sizeof(*res);
	res->success = true;

	KVEngineTransactionInterface *txn =
		engine->create_txn(engine, req->gxid, false, req->start_ts);
	if (txn)
	{
		set_txnengine_ts(txn, req->lower_lts, req->upper_lts);
		res->success = txn->prepare(txn, engine, req->gxid, req->start_ts);
		uint64 cts, nts;
		get_txnengine_ts(txn, &cts, &nts);
		res->header.cts = cts;
		res->header.nts = nts;		
	}
	return res;
}

ResponseHeader *
kvengine_optprocess_commit(RequestHeader *req)
{
	CommitResponse *res = palloc0(sizeof(*res));
	res->header.type = req->type;
	res->header.size = sizeof(*res);
	res->success = false;

	KVEngineTransactionInterface *txn =
		engine->create_txn(engine, req->gxid, false, req->start_ts);
	if (txn)
	{
		// set_txnengine_ts(txn, req->commit_ts, req->commit_ts + 1);
		acquire_txnengine_mutex(txn);
		bool result = txn->commit_and_destroy(txn, req->gxid, req->commit_ts);
		res->success = result;
		if (enable_paxos)
			paxos_storage_commit_batch(req->gxid);
	}
	// TODO: check if batch_engine create fail and add the return value of commit operation.
	return res;
}

ResponseHeader *
kvengine_optprocess_abort(RequestHeader *req)
{
	ResponseHeader *res = palloc0(sizeof(*res));
	res->type = req->type;
	res->size = sizeof(*res);

	KVEngineTransactionInterface *txn =
		engine->create_txn(engine, req->gxid, false, req->start_ts);
	if (txn)
	{
		set_txnengine_ts(txn, req->lower_lts, req->upper_lts);
		acquire_txnengine_mutex(txn);
		txn->abort_and_destroy(txn, req->gxid);
		if (enable_paxos)
			paxos_storage_commit_batch(req->gxid);
	}
	// TODO: check if batch_engine create fail and add the return value of commit operation.
	return res;
}

ResponseHeader *
kvengine_optprocess_clear(RequestHeader *req)
{
	ClearResponse *res = palloc0(sizeof(*res));
	res->header.type = req->type;
	res->header.size = sizeof(*res);
	res->success = true;

	KVEngineTransactionInterface *txn =
		engine->create_txn(engine, req->gxid, false, req->start_ts);
	if (txn)
	{
		acquire_txnengine_mutex(txn);	
		txn->destroy(txn, req->gxid);
	}
	return res;
}
