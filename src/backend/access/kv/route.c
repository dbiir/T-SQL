/*-------------------------------------------------------------------------
 *
 * route.c
 *	  Routing operations for range distribution
 *
 * Portions Copyright (c) 2019-Present, TDSQL
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/kv/route.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "tdb/tdbkvam.h"
#include "tdb/route.h"
#include "tdb/rangestatistics.h"
#include "cdb/cdbvars.h"

static RangeDesc route_scan_get_next(KVEngineScanDesc scan);

/*
 * key: metalevel(Represents the current level of routing) + sourcekey
 */
TupleKeySlice
makeRouteKey(TupleKeySlice sourcekey, bool *getkey)
{
	int metalevel = META;
	/* when the sourcekey's table id is larger than user_Data_start, we can think the key is a sourcekey */
	if (sourcekey.data->rel_id > USER_DATA_START)
	{
		metalevel = META - 1;
	}
	else if (sourcekey.data->rel_id <= META - RootRangeLevel)
	{
		/* the current key is root key, we cannot find the route key of root key */
		*getkey = false;
		return sourcekey;
	}
	else
	{
		/* Route key level */
		metalevel = sourcekey.data->rel_id - 1;
	}
	TupleKey source = (TupleKey)palloc0(sizeof(TupleKeyData) + sourcekey.len + sizeof(Size));
	source->rel_id = metalevel;
	source->indexOid = metalevel;
	set_TupleKeyData_all_value(source, sourcekey.data, sourcekey.len);
	set_TupleKeyData_pk_value_len(source, sourcekey.len, sourcekey.len);

	TupleKeySlice routekey = {source, sizeof(TupleKeyData) + sourcekey.len + sizeof(Size)};
	*getkey = true;
	return routekey;
}

/*
 * store the route kv
 */
void
storeNewRangeRoute(RangeDesc range)
{
	Size length = 0;
	char *rangebuffer = TransferRangeDescToBuffer(range, &length);
	TupleValueSlice rangevalue = {(TupleValue)rangebuffer, length};
	bool getkey;
	TupleKeySlice rangekey = makeRouteKey(range.endkey, &getkey);
	if (getkey)
	{
		//kvengine_check_unique_and_insert(NULL, rangekey, rangevalue, -1, false);
		kvengine_send_put_req(rangekey, rangevalue, -1, false, false, NULL);
		if (GpIdentity.segindex >= 0)
		{
			UpdateStatisticsInsert(rootRouteRangeID, rangekey, rangevalue);
		}
	}
	pfree(rangevalue.data);
	pfree(rangekey.data);
}

void
removeRangeRoute(RangeDesc range)
{
	Size length = 0;
	char *rangebuffer = TransferRangeDescToBuffer(range, &length);
	TupleValueSlice rangevalue = {(TupleValue)rangebuffer, length};
	bool getkey;
	TupleKeySlice rangekey = makeRouteKey(range.endkey, &getkey);
	kvengine_delete_direct(rangekey);

	/* gp_role need to add a m-s role */
	if (getkey)
	{
		if (GpIdentity.segindex >= 0)
		{
			UpdateStatisticsDelete(rootRouteRangeID, rangekey, rangevalue);
		}
		else
		{
			removeMSRangeStatistics(range);
		}
	}
	pfree(rangevalue.data);
	pfree(rangekey.data);
}

RangeDesc*
findUpSomeRangeRoute(TupleKeySlice sourcekey, int num, int *length)
{
	RangeDesc *rangeroutelist = palloc0(sizeof(RangeDesc) * num);
	bool getRoute = false;
	TupleKeySlice routekey = makeRouteKey(sourcekey, &getRoute);
    TupleKeySlice *startkeyp = palloc0(sizeof(TupleKeySlice));
    startkeyp->data = routekey.data;
    startkeyp->len = routekey.len;
	if (getRoute == false)
	{
		return NULL;
	}
	InitKeyDesc initkey = init_basis_in_keydesc(RAW_KEY);
	initkey.init_type = ALL_NULL_KEY;
	initkey.isend = true;
	TupleKeySlice endsourcekey = build_key(initkey);
	TupleKeySlice endroutekey = makeRouteKey(endsourcekey, &getRoute);
    TupleKeySlice *endkeyp = palloc0(sizeof(TupleKeySlice));
    endkeyp->data = endroutekey.data;
    endkeyp->len = endroutekey.len;
	KVEngineScanDesc routescan = initKVEngineScanDesc(NULL, 0, NULL, startkeyp, endkeyp, true);

	for (int i = 0; i < num; i++)
	{
		RangeDesc rangeroute = route_scan_get_next(routescan);

		int endresult = memcmp((char*)sourcekey.data, (char*)rangeroute.endkey.data,
							sourcekey.len < rangeroute.endkey.len ? sourcekey.len : rangeroute.endkey.len);

		if (rangeroute.rangeID != UNVALID_RANGE_ID)
		{
			*length = i;
			return rangeroutelist;
		}
		if (endresult <= 0)
		{
			rangeroutelist[i] = rangeroute;
		}
	}
	*length = num;
	kvengine_endscan(routescan);
	return rangeroutelist;
}

bool
checkKeyInRange(TupleKeySlice sourcekey, RangeDesc rangeroute)
{
	int endresult = memcmp((char*)sourcekey.data, (char*)rangeroute.endkey.data, sourcekey.len);
	int startresult = memcmp((char*)sourcekey.data, (char*)rangeroute.startkey.data, sourcekey.len);
	if (startresult > 0 && endresult <= 0)
		return true;
	else
		return false;
}

RangeDesc
findUpRangeRoute(TupleKeySlice sourcekey)
{
	RangeDesc rangeroute = initVoidRangeDesc();
	bool getRoute = false;
	TupleKeySlice routekey = makeRouteKey(sourcekey, &getRoute);
    TupleKeySlice *startkeyp = palloc0(sizeof(TupleKeySlice));
    startkeyp->data = routekey.data;
    startkeyp->len = routekey.len;
	if (getRoute == false)
	{
		return rangeroute;
	}
	InitKeyDesc initkey = init_basis_in_keydesc(RAW_KEY);
	initkey.init_type = ALL_NULL_KEY;
	initkey.isend = true;
	TupleKeySlice endsourcekey = build_key(initkey);
	TupleKeySlice endroutekey = makeRouteKey(endsourcekey, &getRoute);
    TupleKeySlice *endkeyp = palloc0(sizeof(TupleKeySlice));
    endkeyp->data = endroutekey.data;
    endkeyp->len = endroutekey.len;
	KVEngineScanDesc routescan = initKVEngineScanDesc(NULL, 0, NULL, startkeyp, endkeyp, true);

	rangeroute = route_scan_get_next(routescan);

	if (sourcekey.len == rangeroute.endkey.len)
	{
		//int endresult = memcmp((char*)sourcekey.data, (char*)rangeroute.endkey.data, sourcekey.len);
		//int startresult = memcmp((char*)sourcekey.data, (char*)rangeroute.startkey.data, sourcekey.len);
		if (checkKeyInRange(sourcekey, rangeroute))
		{
			kvengine_endscan(routescan);
			return rangeroute;
		}
	}

	kvengine_endscan(routescan);
	return rangeroute;
}

RangeDesc
route_scan_get_next(KVEngineScanDesc scan)
{
	Assert(scan->scan_index <= scan->scan_num);
	Assert(scan->isforward);
	RangeDesc rangeroute = initVoidRangeDesc();
	if (scan->scan_index == scan->scan_num && scan->next_key.len > 0)
	{
		scan->scan_index = 0;
		kvengine_supplement_tuples(scan, ROCKSDB_SCAN);
	}
	if (scan->scan_index == scan->scan_num && scan->next_key.len == 0)
	{
		Assert(!scan->next_key.data);
		scan->scan_num = 0;
		rangeroute.rangeID = UNVALID_RANGE_ID;
		return rangeroute;
	}

	TupleValueSlice value = scan->cur_value[scan->scan_index];

	rangeroute = TransferBufferToRangeDesc((char*)value.data);
	++(scan->scan_index);

	return rangeroute;

}
