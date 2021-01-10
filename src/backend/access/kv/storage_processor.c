
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
#include "tdb/timestamp_transaction/storage_rocksdb.h"
#include "tdb/timestamp_transaction/storage_lts.h"
#include "tdb/pg_transaction/pgtransaction_generate_key.h"
#include "tdb/timestamp_transaction/timestamp_generate_key.h"
#include "tdb/timestamp_transaction/lts_generate_key.h"
#include "tdb/his_transaction/storage_his.h"
#include "tdb/his_transaction/his_generate_key.h"

#include "utils/guc.h"
#include "tdb/historical_transfer.h"
KVEngineInterface *engine = NULL;

bool am_kv_storage = false;
__thread KVScanDesc RouteCheckScanDesc = NULL;

typedef bool (*key_cmp_func)(TupleKeySlice, TupleKeySlice);

KVScanDesc
init_kv_scan(bool isforward)
{
	KVScanDesc desc = palloc0(sizeof(*desc));
	desc->engine_it = engine->create_iterator(engine, isforward);
	desc->fake_rel = kvengine_make_fake_relation();
	desc->kv_count = 0;
	desc->res_size = sizeof(ScanResponse);
	desc->next_key = NULL;
	desc->cts = 0;
	desc->nts = kMaxSequenceNumber;
	return desc;
}

void store_kv(KVScanDesc desc, Dataslice key, Dataslice value, Oid rangeid)
{
	desc->keyslice[desc->kv_count] = key;
	desc->valueslice[desc->kv_count] = value;
	desc->rangeid[desc->kv_count] = rangeid;
	desc->res_size += key->len + value->len + sizeof(Size) * 2 + sizeof(Oid);
	desc->kv_count++;
}


ScanResponse *
make_scan_res(KVScanDesc desc, int type)
{
	if (type == ROCKSDB_SCAN || type == ROCKSDB_HISTORY_SCAN)
	{
		desc->res_size += desc->next_key->len + sizeof(Size);
	}
	ScanResponse *scan_res = palloc0(desc->res_size);
	scan_res->header.type = type;
	scan_res->header.size = desc->res_size;
	scan_res->num = desc->kv_count;
	scan_res->header.cts = desc->cts;
	scan_res->header.nts = desc->nts;
	Size offset = 0;
	if (type == ROCKSDB_SCAN || type == ROCKSDB_HISTORY_SCAN)
	{
		offset = size_of_Keylen(*desc->next_key);
		save_slice_into_buffer(scan_res->buffer, desc->next_key);
		range_free(desc->next_key);
	}
	for (int i = 0; i < desc->kv_count; ++i)
	{
		Size temp = offset;
		offset += size_of_Keylen(*desc->keyslice[i]);
		save_slice_into_buffer((char *)scan_res->buffer + temp, desc->keyslice[i]);
		range_free(desc->keyslice[i]);
		temp = offset;
		offset += size_of_Keylen(*desc->valueslice[i]);
		save_slice_into_buffer((char *)scan_res->buffer + temp, desc->valueslice[i]);
		range_free(desc->valueslice[i]);
		temp = offset;
		offset += sizeof(Oid);
		Oid *rangeid = (Oid *)(scan_res->buffer + temp);
		*rangeid = desc->rangeid[i];
	}
	return scan_res;
}

void free_kv_desc(KVScanDesc desc)
{
	range_free(desc->fake_rel->rd_rel);
	range_free(desc->fake_rel);
	desc->engine_it->destroy(desc->engine_it);
	range_free(desc);
}

ScanResponse *
finish_kv_scan(KVScanDesc desc, int type)
{
	ScanResponse *scan_res = make_scan_res(desc, type);
	free_kv_desc(desc);
	return scan_res;
}

bool checkRouteVisible(TupleKeySlice key)
{
	bool getkey = false;
	TupleKeySlice routekey = makeRouteKey(key, &getkey);
	if (!getkey)
		return true;
	KVScanDesc desc = RouteCheckScanDesc;
	desc->engine_it->seek(desc->engine_it, routekey);
	int repeat = 0;
	bool success = true;
	for (repeat = 0; repeat < 3 && (desc->engine_it)->is_valid(desc->engine_it);
		 (desc->engine_it)->next(desc->engine_it))
	{
		TupleKeySlice tempkey = {NULL, 0};
		TupleValueSlice tempvalue = {NULL, 0};
		/* Read KV from RocksDB. */
		(desc->engine_it)->get(desc->engine_it, &tempkey, &tempvalue);

		RangeDesc rangeroute = TransferBufferToRangeDesc((char *)tempvalue.data);
		bool result = checkKeyInRange(key, rangeroute);
		bool isleader = false;
		findUpReplicaOnThisSeg(rangeroute, &isleader);
		freeRangeDesc(rangeroute);
		if (result)
		{
			if (isleader)
			{
				success = true;
				break;
			}
			else
			{
				success = false;
				break;
			}
		}
		repeat++;
	}
	range_free(routekey.data);
	return success;
}

void get_key_interval_from_scan_req(ScanWithKeyRequest *scan_req, TupleKeySlice *start_key, TupleKeySlice *end_key, TupleKeySlice *os_key, TupleKeySlice *oe_key)
{
	char *key_buf = scan_req->start_and_end_key;
	TupleKeySlice left_key = pick_tuple_key_from_buffer(key_buf);
	key_buf += left_key.len + sizeof(Size);
	TupleKeySlice right_key = pick_tuple_key_from_buffer(key_buf);
	key_buf += right_key.len + sizeof(Size);
	TupleKeySlice oss_key = pick_tuple_key_from_buffer(key_buf);
	key_buf += oss_key.len + sizeof(Size);
	TupleKeySlice oee_key = pick_tuple_key_from_buffer(key_buf);
	if (scan_req->isforward)
	{
		*start_key = left_key;
		*end_key = right_key;
	}
	else
	{
		*start_key = right_key;
		*end_key = left_key;
	}
	*os_key = oss_key;
	*oe_key = oee_key;
}

ResponseHeader *
kvengine_process_get_req(RequestHeader *req)
{
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		return (ResponseHeader *)kvengine_pgprocess_get_req(req);
		break;
	case KVENGINE_TRANSACTIONDB:
		return (ResponseHeader *)kvengine_optprocess_get_req(req);
		break;
	default:
		break;
	}
	return NULL;
}

ResponseHeader *
kvengine_process_put_req(RequestHeader *req)
{
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		return (ResponseHeader *)kvengine_pgprocess_put_req(req);
		break;
	case KVENGINE_TRANSACTIONDB:
		return (ResponseHeader *)kvengine_optprocess_put_req(req);
		break;
	default:
		break;
	}
	return NULL;
}

ResponseHeader *
kvengine_process_put_rts_req(RequestHeader *req)
{
	return (ResponseHeader *)kvengine_ltsprocess_put_rts_req(req);
}

ResponseHeader *
kvengine_process_delete_normal_req(RequestHeader *req)
{
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		return (ResponseHeader *)kvengine_pgprocess_delete_normal_req(req);
		break;
	case KVENGINE_TRANSACTIONDB:
		return (ResponseHeader *)kvengine_optprocess_delete_normal_req(req);
		break;
	default:
		break;
	}
	return NULL;
}

ResponseHeader *
kvengine_process_update_req(RequestHeader *req)
{
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		return (ResponseHeader *)kvengine_pgprocess_update_req(req);
		break;
	case KVENGINE_TRANSACTIONDB:
		return (ResponseHeader *)kvengine_optprocess_update_req(req);
		break;
	default:
		break;
	}
	return NULL;
}

ResponseHeader *
kvengine_process_delete_direct_req(RequestHeader *req)
{
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		return (ResponseHeader *)kvengine_pgprocess_delete_direct_req(req);
		break;
	case KVENGINE_TRANSACTIONDB:
	
		return (ResponseHeader *)kvengine_optprocess_delete_direct_req(req);
		break;
	default:
		break;
	}
	return NULL;
}

ResponseHeader *
kvengine_process_scan_req(RequestHeader *req)
{
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		return (ResponseHeader *)kvengine_pgprocess_scan_req(req);
		break;
	case KVENGINE_TRANSACTIONDB:
		return (ResponseHeader *)kvengine_optprocess_scan_req(req);
		break;
	default:
		break;
	}
	return NULL;
}

ResponseHeader *
kvengine_process_multi_get_req(RequestHeader *req)
{
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		return (ResponseHeader *)kvengine_pgprocess_multi_get_req(req);
		break;
	case KVENGINE_TRANSACTIONDB:
		return (ResponseHeader *)kvengine_optprocess_multi_get_req(req);
		break;
	default:
		break;
	}
	return NULL;
}

ResponseHeader *
kvengine_process_prepare(RequestHeader *req)
{
	switch (transaction_type)
	{
	case KVENGINE_TRANSACTIONDB:
		return (ResponseHeader *)kvengine_optprocess_prepare(req);
		break;
	default:
		break;
	}
	return NULL;
}

ResponseHeader *
kvengine_process_commit(RequestHeader *req)
{
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		return (ResponseHeader *)kvengine_pgprocess_commit(req);
		break;
	case KVENGINE_TRANSACTIONDB:
	{
		switch (req->txn_mode)
		{
		case TRANSAM_MODE_NEW_OCC:
			return (ResponseHeader *)kvengine_optprocess_commit(req);
		default:
			return (ResponseHeader *)kvengine_optprocess_commit(req);
		}
	}
		return (ResponseHeader *)kvengine_optprocess_commit(req);
		break;
	default:
		break;
	}
	return NULL;
}

ResponseHeader *
kvengine_process_abort(RequestHeader *req)
{
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		return (ResponseHeader *)kvengine_pgprocess_abort(req);
		break;
	case KVENGINE_TRANSACTIONDB:
	{
		switch (req->txn_mode)
		{
		case TRANSAM_MODE_NEW_OCC:
			return (ResponseHeader *)kvengine_optprocess_abort(req);
		default:
			return (ResponseHeader *)kvengine_optprocess_abort(req);
		}
	}
		return (ResponseHeader *)kvengine_optprocess_abort(req);
		break;
	default:
		break;
	}
	return NULL;
}

ResponseHeader *
kvengine_process_clear(RequestHeader *req)
{
	switch (transaction_type)
	{
	case KVENGINE_TRANSACTIONDB:
		return (ResponseHeader *)kvengine_optprocess_clear(req);
		break;
	default:
		break;
	}
	return NULL;
}

ResponseHeader *
kvengine_process_detach(RequestHeader *req)
{
	ResponseHeader *res = palloc0(sizeof(*res));
	res->type = req->type;
	res->size = sizeof(*res);
	return res;
}

/* end of implementation */