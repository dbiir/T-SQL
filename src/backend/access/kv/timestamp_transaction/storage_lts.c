
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
#include "tdb/timestamp_transaction/lts_generate_key.h"
#include "tdb/timestamp_transaction/storage_lts.h"
#include "tdb/timestamp_transaction/timestamp_generate_key.h"

typedef bool (*key_cmp_func)(TupleKeySlice, TupleKeySlice);

static bool get_one(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	if (rocksdb_key.data->rel_id != target_key.data->rel_id ||
		rocksdb_key.data->indexOid != target_key.data->indexOid)
		return false;
	Size r_len = 0, t_len = 0;
	void *key_pk_value = get_TupleKeySlice_primarykey_prefix_lts(rocksdb_key, &r_len);
	void *start_key_pk_value = get_TupleKeySlice_primarykey_prefix_raw(target_key, &t_len);
	bool result = memcmp(key_pk_value, start_key_pk_value, get_min(r_len, t_len)) == 0;

	return result;
}

static bool get_maxnum(TupleKeySlice rocksdb_key, TupleKeySlice target_key, bool isforward)
{
	if (rocksdb_key.data->rel_id != target_key.data->rel_id ||
		rocksdb_key.data->indexOid != target_key.data->indexOid)
		return false;

	Size r_len = 0, t_len = 0;
	void *key_pk_value = get_TupleKeySlice_primarykey_prefix_lts(rocksdb_key, &r_len);
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
	void *key_pk_value = get_TupleKeySlice_secondarykey_prefix_lts(rocksdb_key, &r_len);
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

static TupleKeySlice
add_ltx_label(TupleKeySlice oldkey)
{
	TupleKeySlice newkey = {NULL, 0};
	newkey.len = oldkey.len + 1;
	newkey.data = palloc0(1 + oldkey.len);
	memcpy(newkey.data, oldkey.data, oldkey.len);
	newkey.data->type = LTS_KEY;
	char *label = (char *)newkey.data + oldkey.len;
	*label = 'l';
	return newkey;
}

static void
modify_ltx_label(TupleKeySlice *oldkey)
{
	oldkey->data->type = LTS_KEY;
}

static TupleKeySlice
add_fake_ltx(TupleKeySlice oldkey, uint32 lts)
{
	TupleKeySlice newkey = {NULL, 0};
	newkey.len = oldkey.len + 6;
	newkey.data = palloc0(6 + oldkey.len);
	memcpy(newkey.data, oldkey.data, oldkey.len);
	newkey.data->type = LTS_KEY;
	char *label = (char *)newkey.data + oldkey.len;
	*label = 'l';
	label ++;
	uint32* flts = (uint32*)label;
	*flts = htonl((uint32)lts);
	label += 4;
	*label = 'l';
	return newkey;
}

static bool
get_all_lts(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	if (rocksdb_key.data->rel_id != target_key.data->rel_id ||
		rocksdb_key.data->indexOid != target_key.data->indexOid)
		return false;
	Size r_len = 0, t_len = 0;
	void *key_pk_value = get_TupleKeySlice_primarykey_prefix_lts(rocksdb_key, &r_len);
	void *start_key_pk_value = get_TupleKeySlice_primarykey_prefix_raw(target_key, &t_len);
	bool result = memcmp(key_pk_value, start_key_pk_value, get_min(r_len, t_len)) == 0;

	return result;
}

static bool lts_scan_get_next_valid(KVEngineIteratorInterface **engine_it,
									KVEngineTransactionInterface *txn,
									TupleKeySlice cmp_key,
									Dataslice *key,
									Dataslice *value,
									Oid *rangeid,
									key_cmp_func is_end,
									uint32 lts,
									bool isforward);
static bool kv_ltsengine_check_unique(KVEngineTransactionInterface *txn,
									  TupleKeySlice rocksdb_key);

KVScanDesc
init_kv_ltsprocess_scan(KVEngineTransactionInterface *txn, bool isforward)
{
	KVScanDesc desc = palloc0(sizeof(*desc));
	desc->engine_it = rocks_transaction_create_iterator(txn, isforward, ROCKS_LTS_CF_I);
	if (desc->engine_it == NULL)
	{
		pfree(desc);
		return NULL;
	}
	desc->fake_rel = kvengine_make_fake_relation();
	desc->kv_count = 0;
	desc->res_size = sizeof(ScanResponse);
	desc->next_key = NULL;
	return desc;
}

ResponseHeader *
kvengine_ltsprocess_get_req(RequestHeader *req)
{
	GetRequest *get_req = (GetRequest *)req;
	DataSlice *req_key = get_slice_from_buffer(get_req->key);

	TupleKeySlice key = {(TupleKey)req_key->data, req_key->len};
	TupleValueSlice value;
	modify_ltx_label(&key);
	KVEngineTransactionInterface *txn = engine->create_txn(engine, req->gxid, true, req->start_ts);
	acquire_txnengine_mutex(txn);
	//TODO: here we need get the timestamp and send it into the rocksdb
	uint64_t cts, nts;
	value = txn->get(txn, key, ROCKS_LTS_CF_I, &cts, &nts);

	DataSlice *resvalue = palloc0(size_of_Keylen(value));
	make_data_slice(*resvalue, value);

	GetResponse *get_res = palloc0(sizeof(GetResponse) + size_of_Keylen(*resvalue));
	get_res->header.type = get_req->header.type;
	get_res->header.size = sizeof(GetResponse) + size_of_Keylen(*resvalue);
	save_slice_into_buffer(/*DataSlice*/ get_res->value, resvalue);
	range_free(req_key);
	range_free(resvalue);
	release_txnengine_mutex(txn);
	return (ResponseHeader *)get_res;
}

static bool
kv_ltsengine_check_unique(KVEngineTransactionInterface *txn, TupleKeySlice rocksdb_key)
{
	uint64_t cts, nts;
	TupleValueSlice value = txn->get(txn, rocksdb_key, ROCKS_LTS_CF_I, &cts, &nts);
	if (value.data && value.len != 0)
		return false;
	else
		return true;
}

ResponseHeader *
kvengine_ltsprocess_put_req(RequestHeader *req)
{
	PutRequest *put_req = (PutRequest *)req;

	DataSlice *req_key = get_slice_from_buffer(put_req->k_v);
	Size index = size_of_Keylen(*req_key);
	DataSlice *req_value = get_slice_from_buffer(put_req->k_v + index);

	TupleKeySlice key = {(TupleKey)req_key->data, req_key->len};
	TupleValueSlice value = {(TupleValue)req_value->data, req_value->len};

	PutResponse *put_res = palloc0(sizeof(*put_res));
	put_res->header.type = put_req->header.type;
	put_res->header.size = sizeof(*put_res);

	KVEngineTransactionInterface *txn = engine->create_txn(engine, req->gxid, true, req->start_ts);
	acquire_txnengine_mutex(txn);
	if (!kv_ltsengine_check_unique(txn, key))
	{
		put_res->checkUnique = false;
	}
	else
	{
		if (enable_paxos)
		{
			TupleKeySlice ltskey = add_ltx_label(key);
			int length = 0;
			void *req = TransferMsgToPaxos(PAXOS_RUN_PUT, key, value, put_req->rangeid, &length);
			int result = 0;
			if (put_req->rangeid > 0 && put_req->rangeid < MAXRANGECOUNT)
				result = paxos_storage_runpaxos(req, length, put_req->rangeid);
			else
				txn->put(txn, ltskey, value, ROCKS_LTS_CF_I);
			range_free(req);
			range_free(ltskey.data);
		}
		else
		{
			TupleKeySlice ltskey = add_ltx_label(key);
			txn->put(txn, ltskey, value, ROCKS_LTS_CF_I);
			range_free(ltskey.data);
		}
		put_res->checkUnique = true;
	}
	range_free(req_key);
	range_free(req_value);
	release_txnengine_mutex(txn);
	return (ResponseHeader *)put_res;
}

ResponseHeader *
kvengine_ltsprocess_put_rts_req(RequestHeader *req)
{
	PutRequest *put_req = (PutRequest *)req;

	DataSlice *req_key = get_slice_from_buffer(put_req->k_v);
	Size index = size_of_Keylen(*req_key);
	DataSlice *req_value = get_slice_from_buffer(put_req->k_v + index);

	TupleKeySlice key = {(TupleKey)req_key->data, req_key->len};
	TupleValueSlice value = {(TupleValue)req_value->data, req_value->len};

	PutResponse *put_res = palloc0(sizeof(*put_res));
	put_res->header.type = put_req->header.type;
	put_res->header.size = sizeof(*put_res);

	KVEngineTransactionInterface *txn = engine->create_txn(engine, req->gxid, true, req->start_ts);
	acquire_txnengine_mutex(txn);
	txn->put(txn, key, value, ROCKS_LTS_CF_I);

	range_free(req_key);
	range_free(req_value);
	release_txnengine_mutex(txn);
	return (ResponseHeader *)put_res;
}

static void
ltsprocess_delete_one_key(Delete_UpdateRequest *delete_req, KVEngineTransactionInterface *txn, KVScanDesc desc, TupleKeySlice pkey)
{
	Dataslice key = NULL;
	Dataslice value = NULL;
	desc->engine_it->seek(desc->engine_it, pkey);
	while (lts_scan_get_next_valid(&desc->engine_it, txn, pkey,
								   &key, &value, &delete_req->rangeid, get_all_lts, delete_req->header.upper_lts, true))
	{
		TupleKeySlice cur_key = {(TupleKey)key->data, key->len};
		txn->delete (txn, cur_key, ROCKS_LTS_CF_I);
	}
}
static int
ltsprocess_find_all_delete_key(Delete_UpdateRequest *delete_req, char *buffer, int count, KVEngineTransactionInterface *txn, TupleKeySlice *allkey, TupleValueSlice *allvalue)
{
	char *tmp = buffer;

	pick_slice_from_buffer(tmp, allkey[0]);
	modify_ltx_label(&allkey[0]);
	//*allKey[0] = get_tuple_key_from_buffer(tmp);
	tmp += size_of_Keylen(allkey[0]);

	//*allValue[0] = get_tuple_value_from_buffer(tmp);
	pick_slice_from_buffer(tmp, allvalue[0]);
	tmp += size_of_Keylen(allvalue[0]);

	if (count == 2)
		return count - 1;

	KVScanDesc desc = init_kv_ltsprocess_scan(txn, false);
	if (desc == NULL)
	{
		return 0;
	}
	Dataslice key = NULL;
	Dataslice value = NULL;
	int index = 1;
	for (int i = 2; i < count; ++i)
	{
		DataSlice *tkey = get_slice_from_buffer(tmp);
		tmp += size_of_Keylen(*tkey);

		TupleKeySlice pkey = {(TupleKey)tkey->data, tkey->len};
		TupleKeySlice ltskey = add_ltx_label(pkey);
		desc->engine_it->seek(desc->engine_it, ltskey);
		bool valid = lts_scan_get_next_valid(&desc->engine_it,
											 txn, ltskey,
											 &key, &value,
											 &delete_req->rangeid,
											 get_one,
											 delete_req->header.upper_lts,
											 false);
		if (valid)
		{
			TupleKeySlice cur_key = {(TupleKey)key->data, key->len};
			TupleValueSlice cur_value = {(TupleValue)value->data, value->len};
			allkey[index] = cur_key;
			allvalue[index] = cur_value;
			index++;
		}
		range_free(tkey);
		range_free(ltskey.data);
	}
	free_kv_desc(desc);
	return index;
}

ResponseHeader *
kvengine_ltsprocess_delete_normal_req(RequestHeader *req)
{
	HTSU_Result result = HeapTupleMayBeUpdated;
	Delete_UpdateRequest *delete_req = (Delete_UpdateRequest *)req;

	/* Second, get all the value through the primary key */
	TupleKeySlice *allkey = (TupleKeySlice *)palloc0(mul_size(delete_req->key_count, sizeof(TupleKeySlice)));
	TupleValueSlice *allvalue = (TupleValueSlice *)palloc0(mul_size(delete_req->key_count, sizeof(TupleValueSlice)));
	KVEngineTransactionInterface *txn = engine->create_txn(engine, req->gxid, true, req->start_ts);
	acquire_txnengine_mutex(txn);
	int keycount = ltsprocess_find_all_delete_key(delete_req, delete_req->key, delete_req->key_count, txn, allkey, allvalue);

	/*
	 * Third, uniformly delete all kv that need to be deleted. 
	 */
	RocksUpdateFailureData rufd = initRocksUpdateFailureData();

	TupleKeySlice cur_key;
	TupleValueSlice cur_value;
	KVScanDesc desc = init_kv_ltsprocess_scan(txn, true);
	for (int i = 0; i < keycount; i++)
	{
		cur_key = allkey[i];
		cur_value = allvalue[i];
		ltsprocess_delete_one_key(delete_req, txn, desc, cur_key);
	}
	free_kv_desc(desc);
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
	memcpy(delete_res->rfd, buffer, getRocksUpdateFailureDataLens(rufd));
	release_txnengine_mutex(txn);
	return (ResponseHeader *)delete_res;
}

ResponseHeader *
kvengine_ltsprocess_update_req(RequestHeader *req)
{
	HTSU_Result result = HeapTupleMayBeUpdated;
	Delete_UpdateRequest *update_req = (Delete_UpdateRequest *)req;

	/* Second, get the value through the primary key */
	char *tmp = update_req->key;
	DataSlice *put_key = get_slice_from_buffer(tmp);
	tmp += size_of_Keylen(*put_key);
	DataSlice *put_value = get_slice_from_buffer(tmp);
	tmp += size_of_Keylen(*put_value);

	TupleKeySlice InsertKey = {(TupleKey)put_key->data, put_key->len};
	TupleKeySlice ltskey = add_ltx_label(InsertKey);
	TupleValueSlice InsertValue = {(TupleValue)put_value->data, put_value->len};

	/* Third, get all the value through the primary key */
	TupleKeySlice *allkey = (TupleKeySlice *)palloc0(mul_size(update_req->key_count - 2, sizeof(TupleKeySlice)));
	TupleValueSlice *allvalue = (TupleValueSlice *)palloc0(mul_size(update_req->key_count - 2, sizeof(TupleValueSlice)));
	KVEngineTransactionInterface *txn = engine->create_txn(engine, req->gxid, true, req->start_ts);
	acquire_txnengine_mutex(txn);
	int keycount = ltsprocess_find_all_delete_key(update_req, tmp, update_req->key_count - 2, txn, allkey, allvalue);

	/*
	 * fourth, uniformly delete all kv that need to be deleted. 
	 */
	RocksUpdateFailureData rufd = initRocksUpdateFailureData();
	TupleKeySlice cur_key;
	TupleValueSlice cur_value;
	KVScanDesc desc = init_kv_ltsprocess_scan(txn, true);
	for (int i = 1; i < keycount; i++)
	{
		cur_key = allkey[i];
		cur_value = allvalue[i];
		ltsprocess_delete_one_key(update_req, txn, desc, cur_key);
	}
	free_kv_desc(desc);
	txn->put(txn, ltskey, InsertValue, ROCKS_LTS_CF_I);
	range_free(put_key);
	range_free(ltskey.data);
	range_free(put_value);
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
	memcpy(update_res->rfd, buffer, getRocksUpdateFailureDataLens(rufd));
	release_txnengine_mutex(txn);
	return (ResponseHeader *)update_res;
}

ResponseHeader *
kvengine_ltsprocess_delete_direct_req(RequestHeader *req)
{
	DeleteDirectRequest *delete_req = (DeleteDirectRequest *)req;

	DataSlice *req_key = get_slice_from_buffer(delete_req->key);

	TupleKeySlice key = {(TupleKey)req_key->data, req_key->len};
	modify_ltx_label(&key);
	DeleteDirectResponse *delete_res = palloc0(sizeof(*delete_res));
	delete_res->header.type = delete_req->header.type;
	delete_res->header.size = sizeof(*delete_res);

	engine->delete_direct(engine, key);

	delete_res->success = true;

	range_free(req_key);
	return (ResponseHeader *)delete_res;
}

ResponseHeader *
kvengine_ltsprocess_scan_req(RequestHeader *req)
{
	ScanWithKeyRequest *scan_req = (ScanWithKeyRequest *)req;
	KVEngineTransactionInterface *txn = engine->create_txn(engine, req->gxid, true, req->start_ts);
	acquire_txnengine_mutex(txn);
	KVScanDesc desc = init_kv_ltsprocess_scan(txn, scan_req->isforward);
	if (desc == NULL)
	{
		ScanResponse *scan_res = palloc0(sizeof(ScanResponse));
		scan_res->header.type = scan_req->header.type;
		scan_res->header.size = sizeof(ScanResponse);
		scan_res->num = 0;
		return (ResponseHeader *)scan_res;
	}
	TupleKeySlice start_key = {0};
	TupleKeySlice end_key = {0};
	TupleKeySlice os_key = {0};
	TupleKeySlice oe_key = {0};
	get_key_interval_from_scan_req(scan_req, &start_key, &end_key, &os_key, &oe_key);
	// modify_ltx_label(&start_key);
	// modify_ltx_label(&end_key);
	uint32 startlts = scan_req->isforward ? 0 : (uint32)-1;
	uint32 endlts = scan_req->isforward ? (uint32)-1 : 0;
	TupleKeySlice lts_startkey = add_fake_ltx(start_key, startlts);
	TupleKeySlice lts_end_key = add_fake_ltx(end_key, endlts);
	desc->engine_it->seek(desc->engine_it, lts_startkey);
	Dataslice key = NULL;
	Dataslice value = NULL;
	Oid rangeid = 0;
	key_cmp_func cmp_func;
	if (scan_req->issecond)
		cmp_func = scan_req->isforward ? get_maxnum_second_forward : get_maxnum_second_backward;
	else
		cmp_func = scan_req->isforward ? get_maxnum_forward : get_maxnum_backward;

	int i = 0;
	for (i = 0; i < scan_req->max_num &&
				lts_scan_get_next_valid(&desc->engine_it, txn, lts_end_key,
										&key, &value, &rangeid, cmp_func, 
										scan_req->header.upper_lts, scan_req->isforward);
		 i++)
	{
		store_kv(desc, key, value, rangeid);
	}

	if (i == scan_req->max_num && 
				(lts_scan_get_next_valid(&desc->engine_it, txn, lts_end_key, 
										 &key, &value, &rangeid, cmp_func, 
										 scan_req->header.upper_lts, scan_req->isforward)))
	{
		TupleKeySlice ltskey = {(TupleKey)key->data, key->len};
		if (IsPartLTSKey(ltskey))
		{
			key->len = key->len - 1;
		}
		else if (IsLTSKey(ltskey))
		{
			key->len = key->len - 2 - sizeof(uint32);
		}
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
	range_free(lts_startkey.data);
	range_free(lts_end_key.data);
	release_txnengine_mutex(txn);
	return (ResponseHeader *)finish_kv_scan(desc, scan_req->header.type);
}

ResponseHeader *
kvengine_ltsprocess_multi_get_req(RequestHeader *req)
{
	ScanWithKeyRequest *scan_req = (ScanWithKeyRequest *)req;
	KVEngineTransactionInterface *txn = engine->create_txn(engine, req->gxid, true, req->start_ts);
	acquire_txnengine_mutex(txn);
	KVScanDesc desc = init_kv_ltsprocess_scan(txn, true);
	if (desc == NULL)
	{
		ScanResponse *scan_res = palloc0(sizeof(ScanResponse));
		scan_res->header.type = scan_req->header.type;
		scan_res->header.size = sizeof(ScanResponse);
		scan_res->num = 0;
		return (ResponseHeader *)scan_res;
	}
	char *buffer = scan_req->start_and_end_key;
	Dataslice key = NULL;
	Dataslice value = NULL;
	Oid rangeid = 0;
	for (int i = 0; i < scan_req->max_num; ++i)
	{
		TupleKeySlice pkey = get_tuple_key_from_buffer(buffer);
		buffer += pkey.len + sizeof(Size);
		desc->engine_it->seek(desc->engine_it, pkey);
		bool valid = lts_scan_get_next_valid(&desc->engine_it, txn, pkey,
											 &key, &value, &rangeid, get_one,
											 scan_req->header.upper_lts, true);
		if (valid)
		{
			store_kv(desc, key, value, rangeid);
		}
		range_free(pkey.data);
	}
	release_txnengine_mutex(txn);
	return (ResponseHeader *)finish_kv_scan(desc, scan_req->header.type);
}

static bool
ltsVisionJudge(TupleKeySlice tempkey, uint32 lts)
{
	uint32 klts = get_lts_suffix(tempkey);
	return lts > klts;
}

static bool
ltsKeycompare(TupleKeySlice tempkey, TupleKeySlice last_valid_key)
{
	char *tbuf = (char*)tempkey.data;
	char *lbuf = (char*)last_valid_key.data;
	int tlen = tempkey.len - sizeof(uint32) - 2;
	int llen = last_valid_key.len - sizeof(uint32) - 2;
	if (tlen != llen)
		return false;

	int result = memcmp(tbuf, lbuf, tlen);
	return result == 0;
}

static bool 
ltsKeycompareLts(TupleKeySlice tempkey, TupleKeySlice last_valid_key)
{
	uint32 tlts = get_lts_suffix(tempkey);
	uint32 llts = get_lts_suffix(last_valid_key);
	return tlts > llts;
}

static bool
partltsKeycompare(TupleKeySlice tempkey, TupleKeySlice last_valid_key)
{
	char *tbuf = (char*)tempkey.data;
	char *lbuf = (char*)last_valid_key.data;
	int tlen = tempkey.len - 1;
	int llen = last_valid_key.len - sizeof(uint32) - 2;
	if (tlen != llen)
		return false;

	int result = memcmp(tbuf, lbuf, tlen);
	return result == 0;
}

static bool
lts_scan_get_next_valid(KVEngineIteratorInterface **engine_it,
						KVEngineTransactionInterface *txn,
						TupleKeySlice cmp_key,
						Dataslice *key,
						Dataslice *value,
						Oid *rangeid,
						key_cmp_func is_end,
						uint32 lts,
						bool isforward)
{
	int next_time = 0;
	bool self_update = false;
	bool use_seek = false;
	TupleKeySlice last_valid_key = {NULL, 0};
	last_valid_key.data = palloc0(MAX_KEY_SIZE);
	TupleValueSlice last_valid_value = {NULL, 0};
	last_valid_value.data = palloc0(MAX_VALUE_SIZE);
	TupleKeySlice seek_key = {NULL, 0};
	seek_key.data = palloc0(MAX_KEY_SIZE);
	for (; (*engine_it)->is_valid(*engine_it);)
	{
		if (next_time > 10)
			use_seek = true;
		TupleKeySlice tempkey = {NULL, 0};
		TupleValueSlice tempvalue = {NULL, 0};
		/* Read KV from RocksDB. */
		(*engine_it)->get(*engine_it, &tempkey, &tempvalue);

		bool noend = (*is_end)(tempkey, cmp_key);
		if (!noend)
		{
			break;
		}
		if (enable_range_distribution && enable_paxos && !checkRouteVisible(tempkey))
		{
			(*engine_it)->next(*engine_it);
			next_time++;
			continue;
		}
		if (enable_range_distribution)
		{
			RangeDesc *route = FindRangeRouteByKey(tempkey);
			if (route != NULL)
				*rangeid = route->rangeID;
			else
				*rangeid = 0;
			freeRangeDescPoints(route);
		}

		/*
		 * The version is controlled here. 
		 * The following action is to find the largest version that can be read by 
		 * the current timestamp.
		 */
		if (!IsLTSKey(tempkey))
		{
			(*engine_it)->next(*engine_it);
			next_time++;
			continue;
		}
		/* in this case, the key must be current txn write, so the key will not have lts */
		else if (IsPartLTSKey(tempkey))
		{
			if (last_valid_key.len == 0 || last_valid_value.len == 0 ||
				partltsKeycompare(tempkey, last_valid_key))
			{
				memset(last_valid_key.data, 0, MAX_KEY_SIZE);
				memset(last_valid_value.data, 0, MAX_VALUE_SIZE);
				Assert(tempkey.len < MAX_KEY_SIZE);
				Assert(tempvalue.len < MAX_VALUE_SIZE);
				memcpy(last_valid_key.data, tempkey.data, tempkey.len);
				last_valid_key.len = tempkey.len;
				memcpy(last_valid_value.data, tempvalue.data, tempvalue.len);
				last_valid_value.len = tempvalue.len;
				// only in forward case, we need here to seek
				if (use_seek && isforward)
				{
					memset(seek_key.data, 0, MAX_KEY_SIZE);
					seek_key.len = last_valid_key.len + 5;
					memcpy(seek_key.data, last_valid_key.data, last_valid_key.len);
					char *tmp = (char*)seek_key.data + last_valid_key.len;
					uint32 *lts = (uint32*)tmp;
					*lts = htonl((uint32)-1);
					tmp += 4;
					*tmp = 'l';
					(*engine_it)->seek(*engine_it, seek_key);
				}
				else
				{
					(*engine_it)->next(*engine_it);
					self_update = true;
					next_time++;
				}
				continue;
			}
		}
		/* in this case, the key have lts*/
		else if (ltsVisionJudge(tempkey, lts))
		{
			if (last_valid_key.len == 0 || last_valid_value.len == 0)
			{
				memset(last_valid_key.data, 0, MAX_KEY_SIZE);
				memset(last_valid_value.data, 0, MAX_VALUE_SIZE);
				Assert(tempkey.len < MAX_KEY_SIZE);
				Assert(tempvalue.len < MAX_VALUE_SIZE);
				memcpy(last_valid_key.data, tempkey.data, tempkey.len);
				last_valid_key.len = tempkey.len;
				memcpy(last_valid_value.data, tempvalue.data, tempvalue.len);
				last_valid_value.len = tempvalue.len;

				(*engine_it)->next(*engine_it);
				next_time++;
				continue;
			}
			else if (ltsKeycompare(tempkey, last_valid_key) && !self_update)
			{
				if (ltsKeycompareLts(tempkey, last_valid_key))
				{
					memset(last_valid_key.data, 0, MAX_KEY_SIZE);
					memset(last_valid_value.data, 0, MAX_VALUE_SIZE);
					Assert(tempkey.len < MAX_KEY_SIZE);
					Assert(tempvalue.len < MAX_VALUE_SIZE);
					memcpy(last_valid_key.data, tempkey.data, tempkey.len);
					last_valid_key.len = tempkey.len;
					memcpy(last_valid_value.data, tempvalue.data, tempvalue.len);
					last_valid_value.len = tempvalue.len;
				}
				else if (use_seek && !isforward)
				{
					memset(seek_key.data, 0, MAX_KEY_SIZE);
					seek_key.len = tempkey.len;
					memcpy(seek_key.data, tempkey.data, tempkey.len);
					char *tmp = (char*)seek_key.data + tempkey.len - 5;
					uint32 *lts = (uint32*)tmp;
					*lts = htonl((uint32)0);
					(*engine_it)->seek(*engine_it, seek_key);
					range_free(seek_key.data);
				}
				(*engine_it)->next(*engine_it);
				next_time++;
				continue;
			}
		}
		else
		{
			if (last_valid_key.len == 0 || last_valid_value.len == 0)
			{
				(*engine_it)->next(*engine_it);
				next_time++;
				continue;
			}
			else if (ltsKeycompare(tempkey, last_valid_key))
			{
				if (use_seek && isforward)
				{
					memset(seek_key.data, 0, MAX_KEY_SIZE);
					seek_key.len = tempkey.len;
					memcpy(seek_key.data, tempkey.data, tempkey.len);
					char *tmp = (char*)seek_key.data + tempkey.len - 5;
					uint32 *lts = (uint32*)tmp;
					*lts = htonl((uint32)-1);
					(*engine_it)->seek(*engine_it, seek_key);
					range_free(seek_key.data);
					continue;
				}
				else
				{
					(*engine_it)->next(*engine_it);
					next_time++;
					continue;
				}
			}
		}
		break;
	}
	if (last_valid_key.len == 0 || last_valid_value.len == 0)
	{
		return false;
	}
	*key = palloc0(size_of_Keylen(last_valid_key));
	*value = palloc0(size_of_Keylen(last_valid_value));
	make_data_slice(**key, last_valid_key);
	make_data_slice(**value, last_valid_value);
	range_free(last_valid_key.data);
	range_free(last_valid_value.data);
	return true;
}

ResponseHeader *
kvengine_ltsprocess_commit(RequestHeader *req)
{
	CommitResponse *res = palloc0(sizeof(*res));
	res->header.type = req->type;
	res->header.size = sizeof(*res);
	res->success = false;

	KVEngineTransactionInterface *txn = engine->create_txn(engine, req->gxid, false, req->start_ts);
	acquire_txnengine_mutex(txn);
	if (txn)
	{
		bool result = txn->commit_with_lts_and_destroy(txn, req->gxid, req->commit_ts, req->lower_lts);
		res->success = result;
		if (enable_paxos)
			paxos_storage_commit_batch(req->gxid);
	}
	// release_txnengine_mutex(txn);
	// TODO: check if batch_engine create fail and add the return value of commit operation.
	return res;
}

ResponseHeader *
kvengine_ltsprocess_abort(RequestHeader *req)
{
	ResponseHeader *res = palloc0(sizeof(*res));
	res->type = req->type;
	res->size = sizeof(*res);

	KVEngineTransactionInterface *txn = engine->create_txn(engine, req->gxid, false, req->start_ts);
	acquire_txnengine_mutex(txn);
	if (txn)
	{
		txn->abort_and_destroy(txn, req->gxid);
		if (enable_paxos)
			paxos_storage_commit_batch(req->gxid);
	}
	// release_txnengine_mutex(txn);
	// TODO: check if batch_engine create fail and add the return value of commit operation.
	return res;
}
