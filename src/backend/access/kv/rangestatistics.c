/*-------------------------------------------------------------------------
 *
 * rangestatistics.c
 *	  For updating and reading statistics in the case of range distribution
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/kv/rangestatistics.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "tdb/tdbkvam.h"
#include "tdb/range.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "utils/memutils.h"
#include "tdb/range_processor.h"
#include "tdb/rangestatistics.h"
MemoryContext StatisticsContext = NULL;

static char* test_one[3] = {"beijing", "shanghai", "guangzhou"};
static char* test_two[3] = {"haidian00", "huangpu01", "fanyu02"};
static char* test_three[3] = {"D:", "E:", "F:"};

static void InitSegmentSatistics(SegmentSatistics* segmentSatistcs,
					 List *rangelist,
					 SegmentID segmentID,
					 SegmentSize total,
					 char** location,
					 Size*  location_size,
					 Size   location_level);
static bool InitStatisticReq(RangeDesc scan, int stat_id);
static bool UpdateRangeStatisticsInsert(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value);
static void UpdateSegmentStatisticsInsert(TupleKeySlice key, TupleValueSlice value);
static bool UpdateRangeStatisticsDelete(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value);
static void UpdateSegmentStatisticsDelete(TupleKeySlice key, TupleValueSlice value);
static bool UpdateRangeStatisticsRead(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value);
static void UpdateSegmentStatisticsRead(TupleKeySlice key, TupleValueSlice value);

int
FindRangeStatisticsByRangeID(RangeID rangeid)
{
	for (int i = 0; i < ssm_statistics->range_count; i++)
	{
		RangeID range = ssm_statistics->range_statistics[i].rangeID;
		if (range == rangeid)
		{
			return i;
		}
	}
	return -1;
}

void
Init_Seg_Stat(void)
{
	if (ssm_statistics == NULL)
	{
		return;
	}
	if (engine == NULL)
	{
		ereport(WARNING,(errmsg("engine is null!!!!")));
		return;
	}
	List *rangelist = kvengine_process_scan_all_range();
	Size range_length = 0;
	if (rangelist != NIL)
	{
		range_length = list_length(rangelist);
		ssm_statistics->range_count = range_length;

		for (int i = 0; i < range_length; i++)
		{
			Assert(rangelist != NIL);
			RangeDesc* range = (RangeDesc*)list_nth(rangelist, i);
			InitRangeStatistics(*range, i);
			if (IsRangeIDValid(range->rangeID))
			{
				/* Restart all paxos groups when rebooting */
				SegmentID* seglist = getRangeSegID(*range);
				Rangeengineprocessor_create_paxos(range->rangeID, seglist, range->replica_num);
			}
		}
	}
	else
	{
		//ereport(LOG,(errmsg("current node don't have range.")));
	}

	Size test_total_size = (GpIdentity.segindex + 1) * 1024 * 1024;
	ssm_statistics->seg_count = 1;
	char* location[3] = {test_one[GpIdentity.segindex], test_two[GpIdentity.segindex],
							test_three[GpIdentity.segindex]};
	Size location_len[3] = {strlen(test_one[GpIdentity.segindex]),
						  strlen(test_two[GpIdentity.segindex]),
						  strlen(test_three[GpIdentity.segindex])};
	InitSegmentSatistics(&ssm_statistics->seg_statistics[0], rangelist,
							GpIdentity.segindex, test_total_size, location, location_len, 3);
	ssm_statistics->statistics_init = true;
}

void
InitRangeStatistics(RangeDesc Range, int stat_id)
{
	if (am_kv_storage)
	{
		RangeSatistics *Rangestatis = &ssm_statistics->range_statistics[stat_id];
		kvengine_process_scan_one_range_all_key(Range.startkey, Range.endkey, Range.rangeID, Rangestatis);
	}
	else
		InitStatisticReq(Range, stat_id);
}

void
InitSegmentSatistics(SegmentSatistics* segmentSatistcs,
					 List *rangelist,
					 SegmentID segmentID,
					 SegmentSize total,
					 char** location,
					 Size*  location_size,
					 Size   location_level)
{
	segmentSatistcs->segmentID = segmentID;
	/* location message */
	segmentSatistcs->location.location_level = location_level;
	for(int i = 0; i < location_level; i++)
	{
		memcpy(segmentSatistcs->location.address[i], location[i], location_size[i]);
		segmentSatistcs->location.location_len[i] = location_size[i];
	}

	Size range_length = 0;
	SegmentSize all_range_size = 0;
	range_length = ssm_statistics->range_count;
	if (rangelist != NIL && range_length > 0)
	{
		for (int i = 0; i < range_length; i++)
		{
			/* Range user data size */

			all_range_size += ssm_statistics->range_statistics[i].keybytes;
			all_range_size += ssm_statistics->range_statistics[i].valuebytes;
			/* Range meta data size */
			Assert(rangelist != NIL);
			RangeDesc *range = (RangeDesc*)list_nth(rangelist, i);
			all_range_size += sizeof(*range) + sizeof(ReplicaDesc) * DEFAULT_REPLICA_NUM;
			TupleKeySlice rangekey= makeRangeDescKey(range->rangeID);
			all_range_size += sizeof(Size) + rangekey.len;
		}
	}
	/* range message */
	segmentSatistcs->rangeCount = range_length;
	/* store message */
	segmentSatistcs->totalSize = total;
	segmentSatistcs->userSize = all_range_size;
	segmentSatistcs->availableSize = total - all_range_size;
	/* I/O message */
	segmentSatistcs->readSize = 0;
	segmentSatistcs->writeSize = 0;
	//ereport(LOG,(errmsg("segmentStatistics initial complete.")));
}

RangeSatistics*
get_rangesatistics_from_list(RangeID targetRangeID)
{
	Size range_length = ssm_statistics->range_count;
	for (int i = 0; i < range_length; i++)
	{
		RangeSatistics range = ssm_statistics->range_statistics[i];
		if (range.rangeID == targetRangeID)
			return &ssm_statistics->range_statistics[i];
	}
	return NULL;
}

bool
UpdateStatisticsInsert(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value)
{
	bool result = UpdateRangeStatisticsInsert(targetRangeID, key, value);
	UpdateSegmentStatisticsInsert(key, value);
	return result;
}

bool
UpdateRangeStatisticsInsert(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value)
{
	RangeSatistics* rangestat = get_rangesatistics_from_list(targetRangeID);
	if (rangestat == NULL)
		return false;
	if (key.data != NULL)
	{
		rangestat->keybytes += key.len;
		rangestat->keycount += 1;
	}
	if(value.data != NULL)
	{
		rangestat->valuebytes += value.len;
		rangestat->valuecount += 1;
	}
	rangestat->writeSize += key.len + value.len;
	return true;
}

void
UpdateSegmentStatisticsInsert(TupleKeySlice key, TupleValueSlice value)
{
	/*TODO: update the statistics*/
	if (key.data != NULL)
	{
		ssm_statistics->seg_statistics[0].userSize += key.len;
		ssm_statistics->seg_statistics[0].availableSize -= key.len;
	}
	if(value.data != NULL)
	{
		ssm_statistics->seg_statistics[0].userSize += key.len;
		ssm_statistics->seg_statistics[0].availableSize -= key.len;
	}
	ssm_statistics->seg_statistics[0].writeSize += key.len + value.len;
}

bool
UpdateStatisticsDelete(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value)
{
	bool result = UpdateRangeStatisticsDelete(targetRangeID, key, value);
	UpdateSegmentStatisticsDelete(key, value);
	return result;
}


bool
UpdateRangeStatisticsDelete(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value)
{
	RangeSatistics* rangestat = get_rangesatistics_from_list(targetRangeID);
	if (rangestat == NULL)
		return false;
	if (key.data != NULL)
	{
		rangestat->keybytes -= key.len;
		rangestat->keycount -= 1;
	}
	if(value.data != NULL)
	{
		rangestat->valuebytes -= value.len;
		rangestat->valuecount -= 1;
	}
	rangestat->writeSize += key.len + value.len;
	return true;
}

void
UpdateSegmentStatisticsDelete(TupleKeySlice key, TupleValueSlice value)
{
	/*TODO: update the statistics*/
	if (key.data != NULL)
	{
		ssm_statistics->seg_statistics[0].userSize -= key.len;
		ssm_statistics->seg_statistics[0].availableSize += key.len;
	}
	if(value.data != NULL)
	{
		ssm_statistics->seg_statistics[0].userSize -= key.len;
		ssm_statistics->seg_statistics[0].availableSize += key.len;
	}
	ssm_statistics->seg_statistics[0].writeSize += key.len + value.len;
}

bool
UpdateStatisticsRead(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value)
{
	bool result = UpdateRangeStatisticsRead(targetRangeID, key, value);
	UpdateSegmentStatisticsRead(key, value);
	return result;
}

bool
UpdateRangeStatisticsRead(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value)
{
	RangeSatistics* rangestat = get_rangesatistics_from_list(targetRangeID);
	if (rangestat == NULL)
		return false;
	rangestat->readSize += key.len + value.len;
	return true;
}

void
UpdateSegmentStatisticsRead(TupleKeySlice key, TupleValueSlice value)
{
	ssm_statistics->seg_statistics[0].readSize += key.len + value.len;
}

bool
InitStatisticReq(RangeDesc range, int stat_id)
{
	TupleKeySlice startkey = range.startkey;
	TupleKeySlice endkey = range.endkey;
	Dataslice start_key = palloc0(startkey.len + sizeof(Size));
	make_data_slice(*start_key, startkey);
	Dataslice end_key = palloc0(endkey.len + sizeof(Size));
	make_data_slice(*end_key, endkey);

	Size size = sizeof(InitStatisticsRequest) + startkey.len + sizeof(Size) + endkey.len + sizeof(Size);
	InitStatisticsRequest *req = palloc0(size);
	req->header.size = size;
	req ->header.type = INIT_STATISTICS;
	req->new_stat_id = stat_id;
	req->rangeid = range.rangeID;
	char* temp = req->start_and_end_key;
	save_slice_into_buffer(temp, start_key);
	pfree(start_key);
	save_slice_into_buffer(temp + startkey.len + sizeof(Size), end_key);
	pfree(end_key);

	ResponseHeader* res = send_kv_request((RequestHeader*) req);
	InitStatisticsResponse *ir = (InitStatisticsResponse*)res;
	if (ir->success && ir->new_stat_id == stat_id)
		return true;
	else
		return false;
}


/* Request a piece of space in static shared memory to hold all the statistics of the current node. */
void
StatisticsQueueShmemInit(void)
{
	bool found;
	ssm_statistics = (SSM_Statistics*)
		ShmemInitStruct("Statistics Queue", sizeof(SSM_Statistics), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		ssm_statistics->range_count = 0;
		ssm_statistics->seg_count = 0;
	}
	else
	{
		Assert(found);
	}
}

Size
StatisticsQueueShmemSize(void)
{
	return sizeof(SSM_Statistics);
}

bool
checkRangeStatistics(RangeDesc Range)
{
	if (FindRangeStatisticsByRangeID(Range.rangeID) != -1)
	{
		return false;
	}
	else
	{
		InitRangeStatistics(Range, ssm_statistics->range_count++);
		return true;
	}
}

bool
removeRangeStatistics(RangeDesc Range)
{
	int id = FindRangeStatisticsByRangeID(Range.rangeID);
	for (int i = id; i < ssm_statistics->range_count; i++)
	{
		ssm_statistics->range_statistics[i] = ssm_statistics->range_statistics[i + 1];
	}
	ssm_statistics->range_count--;
	return true;
}

bool
removeMSRangeStatistics(RangeDesc Range)
{
	int id = FindRangeStatisticsByRangeID(Range.rangeID);
	ssm_statistics->range_statistics[id].rangeID = -1;
	ssm_statistics->range_statistics[id].keybytes = 0;
	ssm_statistics->range_statistics[id].keycount = 0;
	ssm_statistics->range_statistics[id].valuebytes = 0;
	ssm_statistics->range_statistics[id].valuecount = 0;
	ssm_statistics->range_statistics[id].writeSize = 0;
	ssm_statistics->range_statistics[id].readSize = 0;
	return true;
}
