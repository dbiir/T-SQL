/*-------------------------------------------------------------------------
 *
 * rangecache.c
 *	  For updating and reading statistics in the case of range distribution
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/kv/rangecache.c
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
#include "tdb/rangecache.h"
RangeList *ssm_rangelist = NULL;

static int FindRangeIndexByRangeID(RangeID rangeid);
/* Request a piece of space in static shared memory to hold all the statistics of the current node. */
void
RangeListQueueShmemInit(void)
{
	bool found;
	ssm_rangelist = (RangeList*)
		ShmemInitStruct("Range List Queue", sizeof(RangeList), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		ssm_rangelist->range_count = 0;
		MemSet(ssm_rangelist->buffer, 0, MAXRANGECOUNT * MAX_RANGEDESC_SIZE);
		MemSet(ssm_rangelist->rangeBufferSize, 0, MAXRANGECOUNT);
	}
	else
	{
		Assert(found);
	}
}

Size
RangeListQueueShmemSize(void)
{
	return sizeof(RangeList);
}

int
FindRangeIndexByRangeID(RangeID rangeid)
{
	for (int i = 0; i < ssm_rangelist->range_count; i++)
	{
		RangeDesc range = TransferBufferToRangeDesc(ssm_rangelist->buffer[i]);
		if (range.rangeID == rangeid)
		{
			freeRangeDesc(range);
			return i;
		}
	}
	return -1;
}

RangeDesc*
FindRangeDescByRangeID(RangeID rangeid)
{
	int index = FindRangeIndexByRangeID(rangeid);
	RangeDesc* range = palloc0(sizeof(RangeDesc));
	*range = TransferBufferToRangeDesc(ssm_rangelist->buffer[index]);
	return range;
}

void
AddRangeListCache(RangeDesc temp)
{
	/* First of all, determine if the range has been added. */
	int index = FindRangeIndexByRangeID(temp.rangeID);
	if (index != -1)
	{
		/* If the range, has been added, check if the range is the latest data. */
		//RangeDesc range = TransferBufferToRangeDesc(ssm_rangelist->buffer[index]);
		/*
		 * TODO:The parameter passed in is the data fetched from the rocksdb.
		 * Should I update the cache or update the rocksdb?
		 */
		Size length = 0;
		char* buffer = TransferRangeDescToBuffer(temp, &length);
		memcpy(ssm_rangelist->buffer[index], buffer, length);
		ssm_rangelist->rangeBufferSize[index] = length;
		pfree(buffer);
	}
	else
	{
		/* Otherwise, the range is added to the cache. */
		Size length = 0;
		char* buffer = TransferRangeDescToBuffer(temp, &length);
		ssm_rangelist->rangeBufferSize[ssm_rangelist->range_count] = length;
		memcpy(ssm_rangelist->buffer[ssm_rangelist->range_count++], buffer, length);
		pfree(buffer);
	}
}

void
RemoveRangeListCache(RangeDesc temp)
{
	/* First of all, determine if the range has been added. */
	int index = FindRangeIndexByRangeID(temp.rangeID);
	for (int i = index; i < ssm_rangelist->range_count; i++)
	{
		memcpy(ssm_rangelist->buffer[i], ssm_rangelist->buffer[i + 1], ssm_rangelist->rangeBufferSize[i + 1]);
		ssm_rangelist->rangeBufferSize[i] = ssm_rangelist->rangeBufferSize[i + 1];
	}
	ssm_rangelist->range_count--;
}

RangeDesc*
FindRangeDescByIndex(int index)
{
	if (index >= ssm_rangelist->range_count)
		return NULL;
	RangeDesc *range = palloc0(sizeof(RangeDesc));
  	*range = TransferBufferToRangeDesc(ssm_rangelist->buffer[index]);
	return range;
}

RangeDesc*
FindRangeDescByKey(TupleKeySlice key, bool is_startkey)
{
	RangeDesc *range = NULL;
	for (int i = 0; i < ssm_rangelist->range_count; i++)
	{
		range = FindRangeDescByIndex(i);
		if (range == NULL)
			continue;
		if (is_startkey)
		{
			if (key.len != range->startkey.len)
			{
				range = NULL;
				continue;
			}
			int result = memcmp(key.data, range->startkey.data, key.len);
			if (result == 0)
				break;
			else
				range = NULL;
		}
		else
		{
			if (key.len != range->endkey.len)
			{
				range = NULL;
				continue;
			}
			int result = memcmp(key.data, range->endkey.data, key.len);
			if (result == 0)
				break;
			else
				range = NULL;
		}
	}
	return range;
}

RangeDesc*
FindRangeRouteByKey(TupleKeySlice key)
{
	RangeDesc *range = NULL;
	for (int i = 0; i < ssm_rangelist->range_count; i++)
	{
		range = FindRangeDescByIndex(i);
		if (range == NULL)
			continue;
		if (range->rangeID == 0)
		{
			range = NULL;
			continue;
		}

		int startresult = memcmp(key.data, range->startkey.data, get_min(key.len, range->startkey.len));
		int endresult = memcmp(key.data, range->endkey.data, get_min(key.len, range->endkey.len));
		if (startresult > 0 && endresult <= 0)
			break;
		else
			range = NULL;
	}

	return range;
}
