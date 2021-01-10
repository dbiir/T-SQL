/*----------------------------------
 *
 * route.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/route.h
 *
 *----------------------------------
 */

#include "tdb/range_universal.h"
#include "tdb/rangestatistics.h"
#include "tdb/rangeid_generator.h"

extern TupleKeySlice makeRouteKey(TupleKeySlice sourcekey, bool *getkey);
extern void storeNewRangeRoute(RangeDesc range);
extern RangeDesc findUpRangeRoute(TupleKeySlice sourcekey);
extern RangeDesc* findUpSomeRangeRoute(TupleKeySlice sourcekey, int num, int *length);
extern void removeRangeRoute(RangeDesc range);
extern bool checkKeyInRange(TupleKeySlice sourcekey, RangeDesc rangeroute);
