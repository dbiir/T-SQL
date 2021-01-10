/*-------------------------------------------------------------------------
 *
 * range.c
 *	  Storage and reading of range meta-information in the case of range
 * distribution.
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/kv/range.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/relpath.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/xact.h"
#include "tdb/tdbkvam.h"
#include "tdb/rangestatistics.h"
#include "tdb/range.h"
#include "tdb/range_struct.h"
#include "cdb/cdbvars.h"
#include "tdb/rangecache.h"

int RootRangeLevel = 1;
RangeID Rangeidvalue = 0;
int DEFAULT_REPLICA_NUM = 3;

RangeID rootRouteRangeID = 0;


static RangeResponse* kvengine_send_range_scan_req(TupleKeySlice startkey, TupleKeySlice endkey,
							 Size rangesize, int req_type);

void
freeRangeDescPoints(RangeDesc* range)
{
	if (range == NULL)
		return;
	freeRangeDesc(*range);
	pfree(range);
}

void
freeRangeDesc(RangeDesc range)
{
	if (range.replica != NULL)
	{
		for (int i = 0; i < range.replica_num; i++)
		{
			if (range.replica[i] != NULL)
				pfree(range.replica[i]);
		}
		pfree(range.replica);
	}
	if (range.startkey.data != NULL)
		pfree(range.startkey.data);
	if (range.endkey.data != NULL)
		pfree(range.endkey.data);
}

bool
compareReplica(RangeDesc a, RangeDesc b)
{
	if (a.replica == NULL || b.replica == NULL)
		return false;
	if (a.replica_num != b.replica_num)
		return false;
	int equalcount = 0;
	for (int i = 0; i < a.replica_num; i++)
	{
		for (int j = 0; j < b.replica_num; j++)
		{
			if (a.replica[i]->segmentID == b.replica[j]->segmentID)
			{
				equalcount++;
				break;
			}
		}
	}
	if (equalcount == a.replica_num)
		return true;
	else
		return false;
}

TupleKeySlice
makeRangeDescKey(RangeID rangeID)
{
	TupleKey rangekey = (TupleKey)palloc(sizeof(TupleKeyData));
	rangekey->rel_id = RANGEDESC;
	rangekey->indexOid = rangeID;
	MemSet(rangekey->other_data, 0, 4);
	TupleKeySlice key = {rangekey, sizeof(TupleKeyData)};
	return key;
}

RangeDesc
CreateNewRangeDesc(RangeID rangeID, TupleKeySlice startkey, TupleKeySlice endkey, Replica *replica, int replica_num)
{
	RangeDesc range = initVoidRangeDesc();
	range.rangeID = rangeID;
	range.startkey = startkey;
	range.endkey = endkey;
	range.version = 0;
	range.replica = replica;
	range.replica_num = replica_num;
	for (int i = 0; i < range.replica_num; i++)
	{
		/* Since there are no more copies at the moment, pretend to have held an election here */
		range.replica[i]->LeaderReplicaID = 0;
		if (range.replica[i]->LeaderReplicaID == range.replica[i]->replicaID)
			range.replica[i]->replica_state = leader_working;
		else
			range.replica[i]->replica_state = follower_working;
	}
	return range;
}

Replica
CreateNewReplica(ReplicaID replicaID, SegmentID segmentID, ReplicaID leader)
{
	Replica replica = (Replica)palloc0(sizeof(ReplicaDesc));
	replica->LeaderReplicaID = leader;
	replica->replicaID = replicaID;
	replica->segmentID = segmentID;
	replica->replica_state = replica_init;
	return replica;
}

void
AddReplicaToRange(RangeDesc *range, Replica replica)
{
	Replica* replicas = palloc0(sizeof(Replica) * (range->replica_num + 1));

	for (int i = 0; i < range->replica_num; i++)
	{
		replicas[i] = palloc0(sizeof(ReplicaDesc));
		*replicas[i] = *range->replica[i];
		pfree(range->replica[i]);
	}
	pfree(range->replica);
	replicas[range->replica_num] = replica;
	range->replica_num++;
	range->replica = replicas;
	return;
}

void
RemoveReplicaToRange(RangeDesc *range, SegmentID targetseg)
{
	Replica* replicas = palloc0(sizeof(Replica) * (range->replica_num - 1));
	int index = 0;
	for (int i = 0; i < range->replica_num; i++)
	{
		if (range->replica[i]->segmentID == targetseg)
		{
			pfree(range->replica[i]);
			continue;
		}
		replicas[index] = palloc0(sizeof(ReplicaDesc));
		*replicas[index] = *range->replica[i];
		index++;
		pfree(range->replica[i]);
	}
	pfree(range->replica);
	range->replica_num--;
	range->replica = replicas;
	return;
}

void
TransferLeader(RangeDesc *range, ReplicaID targetreplica)
{
	for (int i = 0; i < range->replica_num; i++)
	{
		range->replica[i]->LeaderReplicaID = targetreplica;
		if (range->replica[i]->replicaID == targetreplica)
		{
			range->replica[i]->replica_state = leader_working;
		}
		else
		{
			range->replica[i]->replica_state = follower_working;
		}

	}
}

RangeDesc
initNewTableRangeDesc(Relation relation, RangeID rangeid, SegmentID *replica_segid, int replicanum)
{
	int RN = 0;
	SegmentID *segid;
	if (replicanum != 0)
	{
		segid = replica_segid;
		RN = replicanum;
	}
	else
	{

		segid = GetBestTargetSegmentList(DEFAULT_REPLICA_NUM);
		RN = DEFAULT_REPLICA_NUM;
	}
	/* there we need to know all of the segments in this cluster */
	Replica *replicas = (Replica*)palloc0(sizeof(Replica) * RN);
	RangeID rangeID;
	if (rangeid == UNVALID_RANGE_ID)
		rangeID = getMaxRangeID();
	else
		rangeID = rangeid;
	for (int i = 0; i < RN; i++)
	{
		Replica replica = CreateNewReplica(i, segid[i], UNVALID_REPLICA_ID);
		replicas[i] = replica;
	}
	InitKeyDesc initkey = init_basis_in_keydesc(RAW_KEY);
	initkey.rel_id = relation->rd_id;
	initkey.init_type = INDEX_NULL_KEY;
	initkey.isend = false;
	TupleKeySlice startKey = build_key(initkey);
	initkey.isend = true;
	TupleKeySlice endKey = build_key(initkey);

	RangeDesc range = CreateNewRangeDesc(rangeID, startKey, endKey, replicas, RN);
	return range;
}

SegmentID*
getRangeSegID(RangeDesc range)
{
	int seg_count = range.replica_num;
	SegmentID *use_segid = palloc0(sizeof(SegmentID) * MAXSEGCOUNT);

	for (int i = 0; i < seg_count; i++)
	{
		use_segid[i] = range.replica[i]->segmentID;
	}
	return use_segid;
}

void
storeNewRangeDesc(RangeDesc range)
{
	Size length = 0;
	char *rangebuffer = TransferRangeDescToBuffer(range, &length);
	TupleValueSlice rangevalue = {(TupleValue)rangebuffer, length};

	TupleKeySlice rangekey = makeRangeDescKey(range.rangeID);
	//kvengine_check_unique_and_insert(NULL, rangekey, rangevalue, -1, false);
	kvengine_send_put_req(rangekey, rangevalue, -1, false, false, NULL);

	/* gp_role need to add a m-s role */
	if (GpIdentity.segindex >= 0)
	{
		/* TODO: send request to postmaster, master add statistics */
		if (!checkRangeStatistics(range))
		{
			UpdateStatisticsInsert(rootRouteRangeID, rangekey, rangevalue);
		}
		AddRangeListCache(range);
	}
	pfree(rangevalue.data);
	pfree(rangekey.data);
}

void
removeRangeDesc(RangeDesc range)
{
	Size length = 0;
	char *rangebuffer = TransferRangeDescToBuffer(range, &length);
	TupleValueSlice rangevalue = {(TupleValue)rangebuffer, length};

	TupleKeySlice rangekey = makeRangeDescKey(range.rangeID);
	kvengine_delete_direct(rangekey);

	/* gp_role need to add a m-s role */
	if (GpIdentity.segindex >= 0)
	{
		/* TODO: send request to postmaster, master add statistics */
		if (!removeRangeStatistics(range))
		{
			UpdateStatisticsDelete(rootRouteRangeID, rangekey, rangevalue);
		}
		RemoveRangeListCache(range);
	}
	pfree(rangevalue.data);
	pfree(rangekey.data);
}

void
mergeRange(RangeDesc *a, RangeDesc *b)
{
	b->startkey.len = a->startkey.len;
	memcpy(b->startkey.data, a->startkey.data, b->startkey.len);
}

RangeDesc
copyRangeDesc(RangeDesc range)
{
	RangeDesc r = range;
	r.replica = palloc0(sizeof(Replica) * range.replica_num);
	for (int i = 0; i < range.replica_num; i++)
	{
		r.replica[i] = (Replica)palloc0(sizeof(ReplicaDesc));
		r.replica[i]->segmentID = range.replica[i]->segmentID;
		r.replica[i]->replicaID = range.replica[i]->replicaID;
		r.replica[i]->replica_state = range.replica[i]->replica_state;
		r.replica[i]->LeaderReplicaID = range.replica[i]->LeaderReplicaID;
	}
	r.startkey.data = palloc0(range.startkey.len);
	memcpy(r.startkey.data, range.startkey.data, range.startkey.len);

	r.endkey.data = palloc0(range.endkey.len);
	memcpy(r.endkey.data, range.endkey.data, range.endkey.len);

	return r;
}

RangeDesc
findUpRangeDescByID(RangeID id)
{
	TupleKeySlice rangekey = makeRangeDescKey(id);
	GetResponse *res = kvengine_send_get_req(rangekey);
	TupleValueSlice rangevalue = get_tuple_value_from_buffer(res->value);
	RangeDesc nullrange;
	if (rangevalue.len != 0)
		nullrange = TransferBufferToRangeDesc((char*)rangevalue.data);
	range_free(rangevalue.data);
	return nullrange;
}

Replica
findUpLeader(RangeDesc range)
{
	return range.replica[range.replica[0]->LeaderReplicaID];
}

Replica
findUpReplicaOnOneSeg(RangeDesc range, SegmentID segid, bool *isleader)
{
	if (range.replica_num <= 0)
	{
		return NULL;
	}
	*isleader = false;
	Replica leader = range.replica[range.replica[0]->LeaderReplicaID];
	for (int i = 0; i < range.replica_num; i++)
	{
		if (range.replica[i]->segmentID == segid)
		{
			if (leader->segmentID == segid)
				*isleader = true;
			return range.replica[i];
		}
	}
	return NULL;
}

Replica
findUpReplicaOnThisSeg(RangeDesc range, bool *isleader)
{
	return findUpReplicaOnOneSeg(range, GpIdentity.segindex, isleader);
}

Replica
findUpReplicaByReplicaID(RangeDesc range, ReplicaID id)
{
	for (int i = 0; i < range.replica_num; i++)
	{
		if (range.replica[i]->replicaID == id)
			return range.replica[i];
	}
	return NULL;
}

SegmentID
GetBestTargetSegment(SegmentID *have_used_seg, int have_used_seg_num, bool *find, RangeDesc range,
						SegmentSatistics* tempstat)
{
	/* TODO: there we need to know the distribution state of segments. the second param is the be chocied seg */
	if (GpIdentity.segindex != -1)
	{
		ereport(ERROR,(errmsg("segment cannot find the best segment! segmentid: %d", GpIdentity.segindex)));
	}
	int seg_num = ssm_statistics->seg_count;
	SegmentID seg = UNVALID_SEG_ID;
	double min_use_percent = 1;
	for (int i = 0; i < seg_num; i++)
	{
		if (tempstat[i].location.location_level == 0)
		{
			ereport(ERROR,
			(errmsg("The heartbeat has not been received. MS does not know all the seg storage conditions. Please try again later. unknow seg: %d",
				i)));
		}
	}
	for (int i = 0; i < seg_num; i++)
	{
		bool skip_the_seg = false;
		for (int j = 0; j < have_used_seg_num; j++)
		{
			if (have_used_seg[j] == i)
				skip_the_seg = true;
		}
		bool isleader = false;
		Replica replica = findUpReplicaOnOneSeg(range, i, &isleader);
		if (replica != NULL)
			continue;
		if (skip_the_seg)
			continue;
		double temp = (double)tempstat[i].userSize / (double)tempstat[i].totalSize;
		if (temp < min_use_percent)
		{
			min_use_percent = temp;
			seg = tempstat[i].segmentID;
			*find = true;
		}
	}
	return seg;
}

SegmentID
GetBestSourceSegment(SegmentID *have_used_seg, int have_used_seg_num, bool *find, RangeDesc range,
						 SegmentSatistics* tempstat)
{
	/* TODO: there we need to know the distribution state of segments. the second param is the be chocied seg */

	if (GpIdentity.segindex != -1)
	{
		ereport(ERROR,(errmsg("segment cannot find the best segment! segmentid: %d", GpIdentity.segindex)));
	}
	int seg_num = range.replica_num;
	SegmentID seg = UNVALID_SEG_ID;
	double max_use_percent = 0;
	for (int i = 0; i < seg_num; i++)
	{
		bool skip_the_seg = false;
		Replica replica = range.replica[i];
		SegmentID rsegid = replica->segmentID;
		for (int j = 0; j < have_used_seg_num; j++)
		{
			if (have_used_seg[j] == rsegid)
				skip_the_seg = true;
		}
		if (skip_the_seg)
			continue;
		double temp = (double)tempstat[rsegid].userSize / (double)tempstat[rsegid].totalSize;
		if (temp > max_use_percent)
		{
			max_use_percent = temp;
			seg = tempstat[rsegid].segmentID;
			*find = true;
		}
	}
	return seg;
}

SegmentID*
GetBestTargetSegmentList(int replica_num)
{
	SegmentID *seglist = palloc0(sizeof(SegmentID) * replica_num);
	MemSet(seglist, UNVALID_SEG_ID, sizeof(SegmentID) * replica_num);
	SegmentSatistics* segstat = copyTempSegStat(NULL);
	RangeDesc voidrange;
	voidrange.replica = NULL;
	voidrange.replica_num = 0;
	for (int i = 0; i < replica_num; i++)
	{
		bool find = false;
		seglist[i] = GetBestTargetSegment(seglist, i, &find, voidrange, segstat);
	}
	return seglist;
}

List*
ScanAllRange(void)
{
	TupleKeySlice minkey = makeRangeDescKey(UNVALID_RANGE_ID);
    TupleKeySlice *minkeyp = palloc0(sizeof(TupleKeySlice));
    minkeyp->data = minkey.data;
    minkeyp->len = minkey.len;
	TupleKeySlice maxkey = makeRangeDescKey(MAX_RANGE_ID);
    TupleKeySlice *maxkeyp = palloc0(sizeof(TupleKeySlice));
    maxkeyp->data = maxkey.data;
    maxkeyp->len = maxkey.len;
	KVEngineScanDesc rangescan = initKVEngineScanDesc(NULL, 0, NULL, minkeyp, maxkeyp, true);
	List *rangelist = NIL;
	RangeDesc *temp;
	do
	{
		temp = range_scan_get_next(rangescan);
		AddRangeListCache(*temp);
		if (temp == NULL)
			break;
		rangelist = lcons(temp, rangelist);
	} while (true);
	return rangelist;
}

RangeDesc*
range_scan_get_next(KVEngineScanDesc scan)
{
	if (scan->scan_index == scan->scan_num)
	{
		scan->scan_index = 0;
		if (scan->isforward)
		{
			if (scan->next_key.len > 0)
			{
				kvengine_supplement_tuples(scan, ROCKSDB_RANGESCAN);
			}
			else
			{
				Assert(!scan->next_key.data);
				scan->scan_num = 0;
			}
		}
		else
		{
			if (scan->end_key.len > 0)
			{
				kvengine_supplement_tuples(scan, ROCKSDB_RANGESCAN);
			}
			else
			{
				Assert(!scan->end_key.data);
				scan->scan_num = 0;
			}
		}
	}
	if (scan->scan_num == 0)
		return NULL;
	else
	{
		TupleValueSlice value = scan->cur_value[scan->scan_index];
		TupleKeySlice key = scan->cur_key[scan->scan_index];
		RangeDesc range = *(RangeDesc*)value.data;
		if (key.data->indexOid != range.rangeID)
			return NULL;
		RangeDesc *rangeroute = palloc0(sizeof(RangeDesc) + sizeof(ReplicaDesc) * DEFAULT_REPLICA_NUM);
		*rangeroute =TransferBufferToRangeDesc((char*)value.data);
		++(scan->scan_index);
		return rangeroute;
	}
}

TupleKeySlice
getRangeMiddleKey(RangeDesc range, Size range_size)
{
	TupleKeySlice key = {NULL, 0};
	RangeResponse *res = kvengine_send_range_scan_req(range.startkey, range.endkey,
									range_size, FIND_MIDDLE_KEY);
	DataSlice *middle_key_data = get_slice_from_buffer(res->buffer);
	save_data_slice(key, *middle_key_data);
	range_free(middle_key_data);
	return key;
}

RangeResponse*
kvengine_send_range_scan_req(TupleKeySlice startkey, TupleKeySlice endkey,
							 Size rangesize, int req_type)
{
	Dataslice start_key = palloc0(startkey.len + sizeof(Size));
	make_data_slice(*start_key, startkey);
	Dataslice end_key = palloc0(endkey.len + sizeof(Size));
	make_data_slice(*end_key, endkey);

	Size size = sizeof(SplitRequest) + startkey.len + sizeof(Size) + endkey.len + sizeof(Size) ;
	SplitRequest *req = palloc0(size);
	SessionSaveTransactionState((RequestHeader*) req);
	req->header.type = req_type;
	req->header.size = size;
	req->all_size = rangesize;
	char* temp = req->start_and_end_key;
	save_slice_into_buffer(temp, start_key);
	pfree(start_key);
	save_slice_into_buffer(temp + startkey.len + sizeof(Size), end_key);
	pfree(end_key);

	ResponseHeader* res = send_kv_request((RequestHeader*) req);
	Assert(res->type == req_type);
	return (RangeResponse*) res;
}

Replica
replica_state_machine(Replica replica, RangePlanDesc plan)
{
	switch (replica->replica_state)
	{
	case replica_init:
		replica->replica_state = follow_online;
		break;
	case follow_online:
		if (replica->LeaderReplicaID == replica->replicaID)
			replica->replica_state = leader_working;
		else
			replica->replica_state = follower_working;
		break;
	case leader_working:
		/* code */
		if (plan.ms_plan_type == MS_SPLIT_PLAN)
		{
			replica->replica_state = split_barrier;
		}
		if (plan.ms_plan_type == MS_MERGE_PLAN)
		{
			replica->replica_state = merge_barrier;
		}
		return replica;
	case split_barrier:
		if (replica->LeaderReplicaID == replica->replicaID)
			replica->replica_state = leader_working;
		else
			replica->replica_state = follower_working;
		return replica;
	case merge_barrier:
		if (replica->LeaderReplicaID == replica->replicaID)
			replica->replica_state = leader_working;
		else
			replica->replica_state = follower_working;
		return replica;
	default:
		break;
	}
	return replica;
}
