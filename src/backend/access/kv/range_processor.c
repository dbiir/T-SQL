#include "postgres.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shm_toc.h"
#include "storage/shm_mq.h"
#include "storage/threadpool.h"
#include "fmgr.h"
#include "utils/resowner.h"
#include "tdb/range_processor.h"
#include "tdb/range.h"
#include "tdb/storage_processor.h"
#include "tdb/storage_param.h"
#include "tdb/rangecache.h"
#include "paxos/paxos_for_c_include.h"
#include "cdb/cdbvars.h"

#define MAX_STATISTICS_THREADS 128

static bool get_range(TupleKeySlice rocksdb_key, TupleKeySlice target_key);
static bool key_cmp(TupleKeySlice rocksdb_key, TupleKeySlice target_key);

static RangeDesc* kvrange_scan_get_next(KVEngineIteratorInterface **engine_it,
					TupleKeySlice cmp_key);
static bool rangekey_scan_get_next(KVEngineIteratorInterface **engine_it,
					TupleKeySlice cmp_key, RangeSatistics* statistics);
static void get_key_interval_from_rangescan_req(SplitRequest* range_req, TupleKeySlice* start_key, TupleKeySlice* end_key);
static RangeResponse* finish_range_scan(KVScanDesc desc, int type);
static bool scan_get_middle_key(KVEngineIteratorInterface **engine_it,
					TupleKeySlice cmp_key,
					Dataslice *key,
					Dataslice *value,
					Size 	  all_size);
bool
key_cmp(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	int result = memcmp(rocksdb_key.data, target_key.data, get_min(rocksdb_key.len, target_key.len));
	if (result > 0)
		return false;
	return true;
}

bool
get_range(TupleKeySlice rocksdb_key, TupleKeySlice target_key)
{
	if (rocksdb_key.data->rel_id != target_key.data->rel_id)
	{
		return false;
	}
	if (rocksdb_key.data->indexOid >= target_key.data->indexOid)
		return false;
	else
		return true;
}

/*
 * Since the interval of a range is left open and right closed,
 * when calculating the range size, if you can seek to the beginning
 * of the key, you need to omit the key.
 */
static bool
check_start_key_is_exist(KVEngineIteratorInterface **engine_it, TupleKeySlice cmp_key)
{
	TupleKeySlice tempkey = {NULL, 0};
	TupleValueSlice tempvalue = {NULL, 0};
	/* Read KV from RocksDB. */
	(*engine_it)->get(*engine_it, &tempkey, &tempvalue);
	int result = memcmp(tempkey.data, cmp_key.data, get_min(tempkey.len, cmp_key.len));
	if (result == 0)
	{
		(*engine_it)->next(*engine_it);
		return true;
	}
	return false;
}

List*
kvengine_process_scan_all_range(void)
{
	List *rangelist = NIL;
	RangeDesc *temp;

	KVScanDesc desc = init_kv_scan(true);
	TupleKeySlice start_key = makeRangeDescKey(UNVALID_RANGE_ID);
	TupleKeySlice end_key = makeRangeDescKey(MAX_RANGE_ID);

	desc->engine_it->seek(desc->engine_it, start_key);
	do
	{
		temp = kvrange_scan_get_next(&desc->engine_it, end_key);
		if (temp == NULL)
			break;
		rangelist = lcons(temp, rangelist);
		AddRangeListCache(*temp);
	} while (true);
	return rangelist;
}

RangeDesc*
kvrange_scan_get_next(KVEngineIteratorInterface **engine_it,
					TupleKeySlice cmp_key)
{
	while ((*engine_it)->is_valid(*engine_it))
	{
		TupleKeySlice tempkey = {NULL, 0};
		TupleValueSlice tempvalue = {NULL, 0};
		/* Read KV from RocksDB. */
		(*engine_it)->get(*engine_it, &tempkey, &tempvalue);

		bool noend = get_range(tempkey, cmp_key);
		if (!noend)
			return NULL;

		RangeDesc range = *(RangeDesc*)tempvalue.data;
		if (tempkey.data->indexOid != range.rangeID)
			return NULL;

		RangeDesc *rangeroute = palloc0(sizeof(RangeDesc));
		*rangeroute = TransferBufferToRangeDesc((char*)tempvalue.data);
		(*engine_it)->next(*engine_it);
		return rangeroute;
	}
	return NULL;
}

void
kvengine_process_scan_one_range_all_key(TupleKeySlice startkey, TupleKeySlice endkey,
						RangeID rangeid, RangeSatistics* Rangestatis)
{
	KVScanDesc desc = init_kv_scan(true);
	Rangestatis->rangeID = rangeid;
	Rangestatis->readSize = 0;
	Rangestatis->writeSize = 0;
	Rangestatis->keybytes = 0;
	Rangestatis->keycount = 0;
	Rangestatis->valuebytes = 0;
	Rangestatis->valuecount = 0;
	desc->engine_it->seek(desc->engine_it, startkey);
	check_start_key_is_exist(&desc->engine_it, startkey);
	while(rangekey_scan_get_next(&desc->engine_it, endkey, Rangestatis));
}

ResponseHeader*
kvengine_process_init_statistics_req(RequestHeader* req)
{
	InitStatisticsRequest *range_req = (InitStatisticsRequest*) req;

	char *key_buf = range_req->start_and_end_key;
	TupleKeySlice start_key = get_tuple_key_from_buffer(key_buf);
	key_buf += start_key.len + sizeof(Size);
	TupleKeySlice end_key = get_tuple_key_from_buffer(key_buf);

	RangeSatistics *rangeSatistics = &ssm_statistics->range_statistics[range_req->new_stat_id];
	range_req->new_stat_id++;

	kvengine_process_scan_one_range_all_key(start_key, end_key, range_req->rangeid, rangeSatistics);
	range_free(start_key.data);
	range_free(end_key.data);
	InitStatisticsResponse *res = (InitStatisticsResponse*)palloc0(sizeof(InitStatisticsResponse));
	res->success = true;
	res->new_stat_id = range_req->new_stat_id;
	res->header.type = INIT_STATISTICS;
	res->header.size = sizeof(InitStatisticsResponse);
	return (ResponseHeader*) res;
}

bool
rangekey_scan_get_next(KVEngineIteratorInterface **engine_it,
					TupleKeySlice cmp_key, RangeSatistics* statistics)
{
	while ((*engine_it)->is_valid(*engine_it))
	{
		TupleKeySlice tempkey = {NULL, 0};
		TupleValueSlice tempvalue = {NULL, 0};
		/* Read KV from RocksDB. */
		(*engine_it)->get(*engine_it, &tempkey, &tempvalue);

		bool noend = key_cmp(tempkey, cmp_key);
		if (!noend)
			return false;

		statistics->keycount++;
		statistics->keybytes += tempkey.len + sizeof(Size);
		statistics->valuecount++;
		statistics->valuebytes += tempvalue.len + sizeof(Size);
		(*engine_it)->next(*engine_it);
		return true;
	}
	return false;
}

ResponseHeader*
kvengine_process_rangescan_req(RequestHeader* req)
{
	SplitRequest *range_req = (SplitRequest*) req;
	KVScanDesc desc = init_kv_scan(true);
	TupleKeySlice start_key = {0};
	TupleKeySlice end_key = {0};
	get_key_interval_from_rangescan_req(range_req, &start_key, &end_key);
	desc->engine_it->seek(desc->engine_it, start_key);

	Dataslice key = NULL;
	Dataslice value = NULL;
	check_start_key_is_exist(&desc->engine_it, start_key);
	bool result = scan_get_middle_key(&desc->engine_it, end_key, &key, &value, range_req->all_size);
	if (result)
	{
		store_kv(desc, key, value, 0);
	}
	range_free(start_key.data);
	range_free(end_key.data);
	return (ResponseHeader*) finish_range_scan(desc, range_req->header.type);
}

void
get_key_interval_from_rangescan_req(SplitRequest* range_req, TupleKeySlice* start_key, TupleKeySlice* end_key)
{
	char *key_buf = range_req->start_and_end_key;
	TupleKeySlice left_key = get_tuple_key_from_buffer(key_buf);
	key_buf += left_key.len + sizeof(Size);
	TupleKeySlice right_key = get_tuple_key_from_buffer(key_buf);

	*start_key = left_key;
	*end_key = right_key;
}

RangeResponse*
finish_range_scan(KVScanDesc desc, int type)
{
	RangeResponse *range_res = palloc0(desc->res_size);
	range_res->header.type = type;
	range_res->header.size = desc->res_size;
	range_res->num = desc->kv_count;
	Size offset = 0;
	for (int i = 0; i < desc->kv_count; ++i)
	{
		Size temp = offset;
		offset += size_of_Keylen(*desc->keyslice[i]);
		save_slice_into_buffer((char*)range_res->buffer + temp, desc->keyslice[i]);
		temp = offset;
		offset += size_of_Keylen(*desc->valueslice[i]);
		save_slice_into_buffer((char*)range_res->buffer + temp, desc->valueslice[i]);
	}
	free_kv_desc(desc);
	return range_res;
}

bool
scan_get_middle_key(KVEngineIteratorInterface **engine_it,
					TupleKeySlice cmp_key,
					Dataslice *key,
					Dataslice *value,
					Size 	  all_size)
{
	Size totalsize = 0;
	for (; (*engine_it)->is_valid(*engine_it); (*engine_it)->next(*engine_it))
	{
		TupleKeySlice tempkey = {NULL, 0};
		TupleValueSlice tempvalue = {NULL, 0};
		/* Read KV from RocksDB. */
		(*engine_it)->get(*engine_it, &tempkey, &tempvalue);

		bool noend = key_cmp(tempkey, cmp_key);
		if (!noend)
			return false;

		totalsize += tempkey.len + tempvalue.len + sizeof(Size) * 2;
		if (totalsize * 2 >= all_size)
		{
			*key = palloc0(size_of_Keylen(tempkey));
			*value = palloc0(size_of_Keylen(tempvalue));
			make_data_slice(**key, tempkey);
			make_data_slice(**value, tempvalue);
			return true;
		}
	}
	return false;
}

bool
Rangeengineprocessor_create_paxos(RangeID rangeid, SegmentID* seglist, int segcount)
{
    int result = 0;
    if (enable_paxos)
    {
        int length = 0;
        char* segchar = TransferSegIDToIPPortList(seglist, segcount, &length);
        SegmentID* currentseg = (SegmentID*)palloc0(sizeof(SegmentID));
        currentseg[0] = GpIdentity.segindex;
        int current_length = 1;
        char* current = TransferSegIDToIPPortList(currentseg, 1, &current_length);
        ereport(LOG,(errmsg("PAXOS: begin range %d paxos start myid %s, all list %s", rangeid, current, segchar)));
        result = paxos_storage_process_create_group_req(rangeid, current, segchar);
        ereport(LOG,
            (errmsg("PAXOS: range %d, currentip %s, target ip %s paxos restart thread result = %d",
                            rangeid, current, segchar, result)));
        pfree(current);
        pfree(currentseg);
    }
	return result == 0;
}
