
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
#include "tdb/his_transaction/storage_his.h"
#include "tdb/his_transaction/his_generate_key.h"
#include "tdb/historical_transfer.h"
#include "cdb/cdbvars.h"

// #define PRINT_VOLUME

ResponseHeader*
kvengine_process_multi_put_req(RequestHeader* req)
{
	MultiPutRequest *multi_put_req = (MultiPutRequest *) req;
	uint64_t startts = req->start_ts;
	// KVEngineTransactionInterface *txn =
	// 	engine->create_txn(engine, req->gxid, true, startts);
	Size buffer_size = req->size - sizeof(MultiPutRequest);
	void *buf = multi_put_req->buffer;
	HistoricalKV kv = {{0}};
	while ((char *) buf - multi_put_req->buffer < buffer_size)
	{
		pick_kv_from_buffer_and_move_buf(buf, kv);
		// txn->put(txn, * (TupleKeySlice *) &kv.key, * (TupleValueSlice *) &kv.value, ROCKS_DEFAULT_CF_I);
		engine->put(engine, * (TupleKeySlice *) &kv.key, * (TupleValueSlice *) &kv.value);
	}
	Assert((char *) buf - multi_put_req->buffer == buffer_size);

	MultiPutResponse *multi_put_res = palloc0(sizeof(*multi_put_res));
	multi_put_res->header.size = sizeof(*multi_put_res);
	multi_put_res->header.type = ROCKSDB_MULTI_PUT;
	return (ResponseHeader*) multi_put_res;
}

void
put_as_comp_kv(HistoricalKV kv, KVEngineTransactionInterface* txn)
{
	Assert(CheckHisSign((TupleKey)kv.key.data, HIS_COMP_KEY) || CheckHisSign((TupleKey)kv.key.data, HIS_TEMP_KEY));
	SetHisSign((TupleKey)kv.key.data, HIS_COMP_KEY);
	// txn->put(txn, * (TupleKeySlice *) &kv.key, * (TupleValueSlice *) &kv.value, ROCKS_DEFAULT_CF_I);
	__sync_fetch_and_add(&(temporal_data_full_key_storage_size), kv.key.len);
	__sync_fetch_and_add(&(temporal_data_full_value_storage_size), kv.value.len); //use atom add to keep correctness
	// temporal_data_storage_size += kv.key.len;
	// temporal_data_storage_size += kv.value.len;
	engine->put(engine, * (TupleKeySlice *) &kv.key, * (TupleValueSlice *) &kv.value);
}

void
put_as_diff_kv(HistoricalKV cur_kv, HistoricalKV pre_kv, KVEngineTransactionInterface* txn)
{
	HistoricalKV diff_kv = generic_different_historical_kv(cur_kv, pre_kv);
	// txn->put(txn, * (TupleKeySlice *) &diff_kv.key, * (TupleValueSlice *) &diff_kv.value, ROCKS_DEFAULT_CF_I);
	__sync_fetch_and_add(&(temporal_data_delta_key_storage_size), diff_kv.key.len);
	__sync_fetch_and_add(&(temporal_data_delta_value_storage_size), diff_kv.value.len); //use atom add to keep correctness
	// temporal_data_storage_size += diff_kv.key.len;
	// temporal_data_storage_size += diff_kv.value.len;
	engine->put(engine, * (TupleKeySlice *) &diff_kv.key, * (TupleValueSlice *) &diff_kv.value);
	clean_slice(diff_kv.key);
	clean_slice(diff_kv.value);
}

/* Return false if we found no comp kvs for the logical record. */
bool
get_last_comp_kv(HistoricalKV temp_kv, HistoricalKV* last_comp_kv, KVEngineTransactionInterface* txn)
{
	GenericSlice comp_key = {0};
	copy_slice(comp_key, temp_kv.key);
	SetHisSign((TupleKey)comp_key.data, HIS_COMP_KEY);
	//KVEngineIteratorInterface *iter = txn->create_iterator(txn, false, ROCKS_DEFAULT_CF_I);
	KVEngineIteratorInterface *iter = engine->create_iterator(engine, false /* isforward */);
	iter->seek(iter, * (TupleKeySlice *) &comp_key);
	GenericSlice last_comp_key = {0}, last_comp_value = {0};
	if (!iter->is_valid(iter))
	{
		iter->destroy(iter);
		return false;
	}
	iter->get(iter, (TupleKeySlice *) &last_comp_key, (TupleValueSlice *) &last_comp_value);
	copy_slice(last_comp_kv->key, last_comp_key);
	copy_slice(last_comp_kv->value, last_comp_value);
	if (is_same_record_version(temp_kv, *last_comp_kv))
	{
		iter->next(iter);
		if (!iter->is_valid(iter))
		{
			iter->destroy(iter);
			return false;
		}
		iter->get(iter, (TupleKeySlice *) &last_comp_key, (TupleValueSlice *) &last_comp_value);
		copy_slice(last_comp_kv->key, last_comp_key);
		copy_slice(last_comp_kv->value, last_comp_value);
	}
	iter->destroy(iter);
	return is_same_record(temp_kv, *last_comp_kv);
}

ResponseHeader *
kvengine_process_refresh_history_req(RequestHeader* req)
{
	KVEngineTransactionInterface * txn = engine->create_txn(engine, req->gxid, true, req->start_ts);
	// KVEngineIteratorInterface * iter = txn->create_iterator(txn, true, ROCKS_DEFAULT_CF_I);
	KVEngineIteratorInterface *iter = engine->create_iterator(engine, true /* isforward */);
	seek_first_temp_his_kv(iter);
	if (iter->is_valid(iter))
		kvengine_refresh_historical_kvs_internal(txn, iter, req->comp_his_interval);
	iter->destroy(iter);

#ifdef PRINT_VOLUME
	if (GpIdentity.segindex != -1)
	{
		FILE *fptr;
		fptr = fopen("/home/gpadmin/wkdb/gpdata/gpdatap/gpseg0/volume.txt", "a");
		time_t now = time(0);
		fprintf(fptr,"%s", ctime(&now));
		fprintf(fptr,"full_key = %llu, full_value = %llu, delta_key = %llu, delta_value = %llu)", 
			temporal_data_full_key_storage_size, temporal_data_full_value_storage_size,
			temporal_data_delta_key_storage_size, temporal_data_delta_value_storage_size);
		fprintf(fptr,"\n");
		fclose(fptr);
	}
#endif

	RefreshHistoryResponse* refresh_his_res = palloc0(sizeof(*refresh_his_res));
	refresh_his_res->header.size = sizeof(*refresh_his_res);
	refresh_his_res->header.type = ROCKSDB_REFRESH_HISTORY;
	return (ResponseHeader*) refresh_his_res;
}

void
seek_first_temp_his_kv(KVEngineIteratorInterface* iter)
{
	TupleKeySlice prefix;
	alloc_slice(prefix, 1);
	prefix.data->type = HIS_TEMP_KEY;
	iter->seek(iter, prefix);
	clean_slice(prefix);
}

/* Either relation id or pk changes indicates our iterator has moved to another record. */
void
kvengine_refresh_historical_kvs_internal(KVEngineTransactionInterface * txn, KVEngineIteratorInterface* iter, int interval)
{
	HistoricalKV pre_kv = {{0}}, cur_kv = {{0}};
	Assert(iter->is_valid(iter));
	iter->get(iter, (TupleKeySlice *) &cur_kv.key, (TupleValueSlice *) &cur_kv.value);
	if (!CheckHisSign((TupleKey)cur_kv.key.data, HIS_TEMP_KEY))
		return;

	bool refresh_over = false;

	while (!refresh_over) /* each cycle handles a logical record */
	{
		Assert(CheckHisSign((TupleKey)cur_kv.key.data, HIS_TEMP_KEY));
		GenericSlice diff_count_key = make_diff_count_key(cur_kv);
		/*
		 * If it is the first version of the logical record, we set diff_count to
		 * COMP_HIS_INTERVAL to move.
		 */
		int diff_count = interval;
		if (get_last_comp_kv(cur_kv, &pre_kv, txn))
		{
			Assert(CheckHisSign((TupleKey)pre_kv.key.data, HIS_COMP_KEY));
			diff_count = get_diff_kv_count(diff_count_key, txn);
			HistoricalKV temp_kv = {{0}};
			if (diff_count > 0 && get_last_comp_kv(pre_kv, &temp_kv, txn)) 
			{
				__sync_fetch_and_sub(&(temporal_data_full_key_storage_size), pre_kv.key.len);
				__sync_fetch_and_sub(&(temporal_data_full_value_storage_size), pre_kv.value.len); //use atom add to keep correctness
				engine->delete_direct(engine, * (TupleKeySlice *) &pre_kv.key);
				// txn->delete(engine, * (TupleKeySlice *) &pre_kv.key, ROCKS_DEFAULT_CF_I);
			}
		}
		do /* each cycle handles a physical historical version (kv) */
		{
			if (diff_count >= interval)
			{
				put_as_comp_kv(cur_kv, txn);
				diff_count = 0;
			}
			else
			{
				put_as_diff_kv(cur_kv, pre_kv, txn);
				++ diff_count;
			}
			refresh_over = !refresh_load_next_historical_kv(txn, iter, &cur_kv, &pre_kv);
		} while (!refresh_over && is_same_record(cur_kv, pre_kv));
		
		if (diff_count > 0)
			put_as_comp_kv(pre_kv, txn);
		put_diff_kv_count(diff_count_key, diff_count, txn);
		clean_slice(diff_count_key);
	}
	/* We need not clean cur_kv cause it reference to iter's buffer. */
	clean_slice(pre_kv.key);
	clean_slice(pre_kv.value);
	// engine->compaction(engine);
}

/* 
 * Get next temp historical kv and convert pre_kv to comp kv. 
 * Return false if there are no more temp historical kvs. 
 */

bool
refresh_load_next_historical_kv(KVEngineTransactionInterface * txn, KVEngineIteratorInterface* iter, HistoricalKV* cur_kv, HistoricalKV* pre_kv)
{
	copy_slice(pre_kv->key, cur_kv->key);
	copy_slice(pre_kv->value, cur_kv->value);
	SetHisSign((TupleKey)pre_kv->key.data, HIS_TEMP_KEY);
	engine->delete_direct(engine,  * (TupleKeySlice *) &pre_kv->key);
	// txn->delete(txn, * (TupleKeySlice *) &pre_kv->key, ROCKS_DEFAULT_CF_I);
	SetHisSign((TupleKey)pre_kv->key.data, HIS_COMP_KEY);
	iter -> next(iter);
	if (!iter->is_valid(iter))
		return false;
	iter->get(iter, (TupleKeySlice *) &cur_kv->key, (TupleValueSlice *) &cur_kv->value);
	return CheckHisSign((TupleKey)cur_kv->key.data, HIS_TEMP_KEY);
}

/* Assume diff count kv has already stored in RocksDB. */
int
get_diff_kv_count(GenericSlice diff_count_key, KVEngineTransactionInterface* txn)
{
	Assert(CheckHisSign((TupleKey)diff_count_key.data, HIS_DIFFCOUNT_KEY));
	// KVEngineIteratorInterface *iter = txn->create_iterator(txn, true, ROCKS_DEFAULT_CF_I);
	KVEngineIteratorInterface *iter = engine->create_iterator(engine, true /* isforward */);
	iter->seek(iter, * (TupleKeySlice *) &diff_count_key);
	HistoricalKV diff_count_found_kv;
	iter->get(iter, (TupleKeySlice *) &diff_count_found_kv.key, (TupleValueSlice *) &diff_count_found_kv.value);
	Assert(equal_slice(diff_count_found_kv.key, diff_count_key));
	int diff_count = * (int *) diff_count_found_kv.value.data;
	iter->destroy(iter);
	return diff_count;
}

void
put_diff_kv_count(GenericSlice diff_count_key, int diff_count, KVEngineTransactionInterface* txn) 
{
	Assert(CheckHisSign((TupleKey)diff_count_key.data, HIS_DIFFCOUNT_KEY));
	GenericSlice diff_count_value = { &diff_count, sizeof(diff_count) };
	// txn->put(txn, * (TupleKeySlice *) &diff_count_key, * (TupleValueSlice *) &diff_count_value, ROCKS_DEFAULT_CF_I);
	engine->put(engine, * (TupleKeySlice *) &diff_count_key, * (TupleValueSlice *) &diff_count_value);
}
/* [hzy9819] temporal function implementation */

KVScanHistoryDesc
init_history_kv_scan(bool isforward, KVEngineTransactionInterface *txn)
{
	KVScanHistoryDesc desc = palloc0(sizeof(*desc));
	desc->engine_it_comp = rocks_transaction_create_iterator(txn, isforward, ROCKS_DEFAULT_CF_I);
	desc->scan_header.engine_it = rocks_transaction_create_iterator(txn, true, ROCKS_DEFAULT_CF_I);
	desc->scan_header.fake_rel = kvengine_make_fake_relation();
	desc->scan_header.kv_count = 0;
	desc->scan_header.res_size = sizeof(ScanResponse);
	desc->scan_header.next_key = NULL;
	return desc;
}

bool
cmp_time(GenericSlice rocksdb_key, GenericSlice target_key)
{
	his_timestamps targettime = decode_timestamps_internal(target_key);
	his_timestamps rockstime = decode_timestamps_internal(rocksdb_key);
	if (targettime.xmin_ts >= rockstime.xmax_ts)
		return true;
	else
		return false;	
}

bool
cmp_range_time(GenericSlice rocksdb_key, GenericSlice target_key)
{
	his_timestamps targettime = decode_timestamps_internal(target_key);
	his_timestamps rockstime = decode_timestamps_internal(rocksdb_key);
	if (targettime.xmax_ts >= rockstime.xmin_ts && targettime.xmin_ts <= rockstime.xmax_ts)
		return true;
	else
		return false;	
}

/**
 * check whether current key still satisfiy target_key
 **/
bool
cmp_history_range(GenericSlice rocksdb_key, GenericSlice target_key, bool isforward)
{
	// GenericSlice target_diff = copy_generic_key(target_key);
	char type = ((TupleKey)(target_key.data))->type;
	SetHisSign((TupleKey)target_key.data, HIS_COMP_KEY);
	int result = memcmp(rocksdb_key.data, target_key.data, get_min(rocksdb_key.len, target_key.len)) < 0;
	SetHisSign((TupleKey)target_key.data, type);
	// pfree(target_diff.data);
	if ((result > 0 && isforward) || (result < 0 && !isforward))
		return false;
	return true;	
}

bool
cmp_diff_range(GenericSlice rocksdb_key, GenericSlice target_key, bool isforward)
{
	// GenericSlice target_diff = copy_generic_key(target_key);
	char type = ((TupleKey)(target_key.data))->type;
	SetHisSign((TupleKey)target_key.data, HIS_DIFF_KEY);
	int result = memcmp(rocksdb_key.data, target_key.data, get_min(rocksdb_key.len, target_key.len)) < 0;	
	SetHisSign((TupleKey)target_key.data, type);
	// pfree(target_diff.data);
	if ((result > 0 && isforward) || (result < 0 && !isforward))
		return false;
	return true;	
}

void
get_key_interval_from_hisscan_req(ScanWithTimeRequest* scan_req, GenericSlice* start_key, GenericSlice* end_key)
{
	char *key_buf = scan_req->start_and_end_key;
	TupleKeySlice left_key = get_tuple_key_from_buffer(key_buf);
	key_buf += left_key.len + sizeof(Size);
	TupleKeySlice right_key = get_tuple_key_from_buffer(key_buf);

	GenericSlice startcompkey = *(GenericSlice*) &left_key;
	GenericSlice endcompkey = *(GenericSlice*) &right_key;
	if (scan_req->isforward)
	{
		*start_key = startcompkey;
		*end_key = endcompkey;
	}
	else
	{
		*start_key = endcompkey;
		*end_key = startcompkey;
	}

}

ResponseHeader*
kvengine_process_scan_history(RequestHeader* req)
{
	ScanWithTimeRequest *scan_req = (ScanWithTimeRequest*) req;
	uint64_t startts = req->start_ts;

	int key_num = 0;
	int max_num = scan_req->max_num;
	bool has_next = false;

	KVEngineTransactionInterface *txn = engine->create_txn(engine, req->gxid, true, startts);
	KVScanHistoryDesc desc = init_history_kv_scan(scan_req->isforward, txn);

	GenericSlice startcompkey = {0};
	GenericSlice endcompkey = {0};
	get_key_interval_from_hisscan_req(scan_req, &startcompkey, &endcompkey);

	/* Get the key of the first compelte version */
	KVEngineIteratorInterface *engine_it_comp = desc->engine_it_comp;
	engine_it_comp->seek(engine_it_comp, *(TupleKeySlice*) &startcompkey);

	for (;(engine_it_comp)->is_valid(engine_it_comp);(engine_it_comp)->next(engine_it_comp))
	{
		bool cmp_is_visible = false;
		TupleKeySlice tempkey = {NULL, 0};
		TupleValueSlice tempvalue = {NULL, 0};

		/* Read a compete version. */
		(engine_it_comp)->get(engine_it_comp, &tempkey, &tempvalue);
		HistoricalKV compkv = {*(GenericSlice*) &tempkey, *(GenericSlice*) &tempvalue};

		/* Check whether this version satisfies the scan condition. */
		if (cmp_history_range(compkv.key, endcompkey, scan_req->isforward))
			break;
		/* Check whether this verison is visible for given timestamp interval. */
		if (cmp_range_time(compkv.key, endcompkey))
			cmp_is_visible = true;

		/* Copy the compete version into a buff named copy_kv. */
		HistoricalKV copy_kv = copy_his_kv(compkv);
		Dataslice key = palloc0(size_of_Keylen(copy_kv.key));
		Dataslice value = palloc0(size_of_Keylen(copy_kv.value));
		make_data_slice(*key, copy_kv.key);
		make_data_slice(*value, copy_kv.value);

		/* We have to omit the last complete version since it is duplicated. */
		if (cmp_is_visible && (!desc->scan_header.kv_count || desc->scan_header.valueslice[desc->scan_header.kv_count - 1]->len != value->len 
							|| memcmp(desc->scan_header.valueslice[desc->scan_header.kv_count - 1]->data, value->data, value->len))) 
		{
			/* Batch is full!! return this batch. */
			if (key_num > max_num)
			{
				desc->scan_header.next_key = key;
				has_next = true;
				range_free(value);
				break;
			}
			else
			{
				store_kv((KVScanDesc) desc, key, value, 0);
				key_num ++;
			}
		}

		/* Fetch delta versions. */
		if (req->comp_his_interval)
		{
			/* Generate the search key for delta version. */
			// GenericSlice diff_end_key = copy_generic_key(compkv.key);
			char type = ((TupleKey)(copy_kv.key.data))->type;
			SetHisSign((TupleKey)copy_kv.key.data, HIS_DIFF_KEY);

			desc->scan_header.engine_it->seek(desc->scan_header.engine_it, *(TupleKeySlice *) &copy_kv.key);
			SetHisSign((TupleKey)copy_kv.key.data, type);

			int diff_record_num = scan_get_next_diff(&desc, &desc->scan_header.engine_it, &copy_kv, endcompkey, cmp_range_time, req->comp_his_interval);

			key_num += diff_record_num;

			// free_generic_key(diff_end_key);
		}
		free_his_kv(copy_kv);
	}

	if (!has_next)
	{
		desc->scan_header.next_key = palloc0(sizeof(DataSlice));
		desc->scan_header.next_key->len = 0;
	}

	return (ResponseHeader*) finish_kv_scan((KVScanDesc) desc, ROCKSDB_HISTORY_SCAN);
}

int
scan_get_next_diff(KVScanDesc *desc, KVEngineIteratorInterface **engine_it_diff, 
					HistoricalKV *comp_kv, GenericSlice endcompkey,
					key_his_cmp_func is_end, int interval)
{
	int attnum = 0;
	int diff_count = 0;
	int diff_record_num = 0;

	for (; (*engine_it_diff)->is_valid(*engine_it_diff); (*engine_it_diff)->next(*engine_it_diff))
	{
		bool cmp_is_visible = false;
		TupleKeySlice tempkey = {NULL, 0};
		TupleValueSlice tempvalue = {NULL, 0};
		/* Read KV from RocksDB. */
		(*engine_it_diff)->get(*engine_it_diff, &tempkey, &tempvalue);
		HistoricalKV diffkv = { *(GenericSlice*)&tempkey, *(GenericSlice*)&tempvalue};
		if (!is_same_record(diffkv, *comp_kv))
			break;
		if (diff_count >= interval)
			break;
		if (!check_order(diffkv, *comp_kv))
			continue;
		if (cmp_range_time(diffkv.key, endcompkey))
			cmp_is_visible = true;

		if (cmp_is_visible)
		{
			HistoricalKV diff = {*(GenericSlice*)&tempkey, *(GenericSlice*)&tempvalue};
			combine_diff_to_comp_kv(comp_kv, diff);

			Dataslice key = palloc0(size_of_Keylen(comp_kv->key));
			Dataslice value = palloc0(size_of_Keylen(comp_kv->value));
			make_data_slice(*key, comp_kv->key);
			make_data_slice(*value, comp_kv->value);
			store_kv((KVScanDesc) *desc, key, value, 0);
			diff_record_num ++;
		}
		diff_count++;
	}

	return diff_record_num;
}
