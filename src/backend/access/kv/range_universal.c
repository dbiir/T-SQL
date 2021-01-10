/*-------------------------------------------------------------------------
 *
 * range_universal.c
 *	  Some basic functions for range Distribution
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/kv/range_universal.c
 *
 *-------------------------------------------------------------------------
 */
#include "tdb/range_universal.h"
#include "tdb/tdbkvam.h"

RangeDesc
initVoidRangeDesc(void)
{
    RangeDesc rangedesc;
    rangedesc.version = -1;
    rangedesc.endkey.len = 0;
    rangedesc.endkey.data = NULL;
    rangedesc.startkey.len = 0;
    rangedesc.startkey.data = NULL;
    rangedesc.replica = NULL;
    rangedesc.replica_num = 0;
    rangedesc.rangeID = -1;
    return rangedesc;
}

int
getRootRangeLevel()
{
    TupleKey rrlkey = (TupleKey)palloc(sizeof(TupleKeyData));
    rrlkey->rel_id = ROOTRANGELEVEL;
    rrlkey->indexOid = ROOTRANGELEVEL;
    memset(rrlkey->other_data, 0, 3);

    TupleKeySlice key = {rrlkey, sizeof(TupleKeyData)};

    GetResponse* res = kvengine_send_get_req(key);

    TupleValueSlice value = get_tuple_value_from_buffer(res->value);
    if (value.len == 0)
        return -1;
	/*
     * Because the type of natts is the same as the type of RootRangeLevel here,
     * natts is used here to store RootRangeLevel
     */
    int *rrlv = (int*)value.data->memtuple.PRIVATE_mt_bits;
	int result = -1;
    if (*rrlv == value.data->sysattrs.natts)
	{
        result = value.data->sysattrs.natts;
	}
	range_free(value.data);
	return result;
}

void
putRootRangeLevel(int RootRangeLevel)
{
    TupleKey rrlkey = (TupleKey)palloc0(sizeof(TupleKeyData));
    rrlkey->rel_id = ROOTRANGELEVEL;
    rrlkey->indexOid = ROOTRANGELEVEL;
    memset(rrlkey->other_data, 0, 3);
    TupleKeySlice key = {rrlkey, sizeof(TupleKeyData)};

    char *rrlbuffer = (char*)palloc0(sizeof(int));
    int *rrlv = (int*)rrlbuffer;
    *rrlv = RootRangeLevel;
    TupleValue rrlvalue = (TupleValue)palloc0(sizeof(TupleValueData) + sizeof(int));

    /*
     * Here, in order to distinguish whether it is RRL, or not,
     * you need to assign some special values to other data as a judgment.
     */
    rrlvalue->sysattrs.natts = RootRangeLevel;
    rrlvalue->sysattrs.xmax = InvalidTransactionId;
	rrlvalue->sysattrs.infomask = 0;
	rrlvalue->sysattrs.infomask2 = 0;
	rrlvalue->sysattrs.cid = 0;
    rrlvalue->memtuple.PRIVATE_mt_len = sizeof(int);
    memcpy(rrlvalue->memtuple.PRIVATE_mt_bits, rrlbuffer, sizeof(int));

    TupleValueSlice value = {rrlvalue, sizeof(TupleValueData) + sizeof(int)};

    kvengine_send_put_req(key, value, -1, false, false, NULL);
    pfree(rrlkey);
    pfree(rrlvalue);
}

char*
TransferRangeDescToBuffer(RangeDesc range, Size *length)
{
    Size rangeStartAndEndKeyLength = range.startkey.len + range.endkey.len + sizeof(Size);
    Size rangeSize = sizeof(RangeDesc) + sizeof(ReplicaDesc) * range.replica_num + rangeStartAndEndKeyLength;
    char *rangebuffer = (char*)palloc0(rangeSize);

    memcpy(rangebuffer, (char*)&range, sizeof(RangeDesc));
    char *temp = rangebuffer + sizeof(RangeDesc);
    for (int i = 0; i < range.replica_num; i++)
    {
        memcpy(temp, (char*)range.replica[i], sizeof(ReplicaDesc));
        temp += sizeof(ReplicaDesc);
    }

    //temp = temp + sizeof(ReplicaDesc) * range.replica_num;
    memcpy(temp, (char*)range.startkey.data, range.startkey.len);

    temp = temp + range.startkey.len;
    memcpy(temp, (char*)range.endkey.data, range.endkey.len);

    *length = rangeSize;
    return rangebuffer;
}

RangeDesc
TransferBufferToRangeDesc(char* rangebuffer)
{
    RangeDesc range;
    range = *(RangeDesc*)rangebuffer;
    range.replica = (Replica*)palloc0(sizeof(Replica) * range.replica_num);
    char *temp = rangebuffer + sizeof(RangeDesc) ;
    for (int i = 0; i < range.replica_num; i++)
    {
        ReplicaDesc rpad = *(ReplicaDesc*)(temp + sizeof(ReplicaDesc) * i);
        range.replica[i] = (Replica)palloc0(sizeof(ReplicaDesc));
        *range.replica[i] = rpad;
    }
    temp = temp + sizeof(ReplicaDesc) * range.replica_num;
    TupleKey startkey = palloc0(range.startkey.len);
    memcpy((char*)startkey, temp, range.startkey.len);

    temp = temp + range.startkey.len;
    TupleKey endkey = palloc0(range.endkey.len);
    memcpy((char*)endkey, temp, range.endkey.len);

    range.startkey.data = startkey;
    range.endkey.data = endkey;

    return range;
}
