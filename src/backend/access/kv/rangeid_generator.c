/*-------------------------------------------------------------------------
 *
 * rangeid_generator.c
 *	  Used to generate a unique rangeid
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/kv/rangeid_generator.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "tdb/tdbkvam.h"
#include "tdb/rangeid_generator.h"

RangeID
getMaxRangeID(void)
{
    getRangeID();
    IncRangeID();
    return Rangeidvalue - 1;
}


void
IncRangeID(void)
{
    PutRangeID(Rangeidvalue + 1);
    Rangeidvalue++;
}

void
PutRangeID(RangeID rangeid)
{
    TupleKey rangeidkey = (TupleKey)palloc0(sizeof(TupleKeyData));
    rangeidkey->rel_id = RANGEID;
    rangeidkey->indexOid = RANGEID;
    memset(rangeidkey->other_data, 0, 4);
    TupleKeySlice key = {rangeidkey, sizeof(TupleKeyData)};

    char *rangeidbuffer = (char*)palloc0(sizeof(RangeID));
    RangeID *rangeidv = (RangeID*)rangeidbuffer;
    *rangeidv = rangeid;
    TupleValue rangeidvalue = (TupleValue)palloc0(sizeof(TupleValueData) + sizeof(RangeID));

    /*
	 * Because the cid type here is the same as the rangeidvalue type,
	 * cid is used here to store rangeidvalue.
	 */
    rangeidvalue->sysattrs.natts = 0;
    rangeidvalue->sysattrs.xmax = InvalidTransactionId;
	rangeidvalue->sysattrs.infomask = 0;
	rangeidvalue->sysattrs.infomask2 = 0;
	rangeidvalue->sysattrs.cid = rangeid;
    rangeidvalue->memtuple.PRIVATE_mt_len = sizeof(int);
    memcpy(rangeidvalue->memtuple.PRIVATE_mt_bits, rangeidbuffer, sizeof(RangeID));

    TupleValueSlice value = {rangeidvalue, sizeof(TupleValueData) + sizeof(RangeID)};

    kvengine_send_put_req(key, value, -1, false, false, NULL);
    pfree(rangeidkey);
    pfree(rangeidvalue);
}

void
getRangeID(void)
{
    Rangeidvalue = 0;
    TupleKey rrlkey = (TupleKey)palloc(sizeof(TupleKeyData));
    rrlkey->rel_id = RANGEID;
    rrlkey->indexOid = RANGEID;
    memset(rrlkey->other_data, 0, 4);

    TupleKeySlice key = {rrlkey, sizeof(TupleKeyData)};

    GetResponse* res = kvengine_send_get_req(key);
    TupleValueSlice value = get_tuple_value_from_buffer(res->value);
    if (value.len == 0)
        return;
    int *rrlv = (int*)value.data->memtuple.PRIVATE_mt_bits;
    if (*rrlv == value.data->sysattrs.cid)
        Rangeidvalue =  value.data->sysattrs.cid;
	range_free(value.data);
}
