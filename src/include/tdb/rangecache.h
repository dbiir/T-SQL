/*----------------------------------
 *
 * rangecache.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/rangecache.h
 *----------------------------------
 */
#ifndef RANGECACHE
#define RANGECACHE
#include "tdb/range_universal.h"

extern Size RangeListQueueShmemSize(void);
extern void RangeListQueueShmemInit(void);
extern void AddRangeListCache(RangeDesc temp);
extern RangeDesc* FindRangeDescByRangeID(RangeID rangeid);
extern RangeDesc* FindRangeDescByIndex(int index);
extern RangeDesc* FindRangeDescByKey(TupleKeySlice key, bool is_startkey);
extern RangeDesc* FindRangeRouteByKey(TupleKeySlice key);
extern void RemoveRangeListCache(RangeDesc temp);
#endif
