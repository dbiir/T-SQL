/*----------------------------------
 *
 * seg_plan.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/seg_plan.h
 *----------------------------------
 */
#include "tdb/range_plan.h"
#include "tdb/range_universal.h"

extern SplitPreparePlan seg_check_one_range_split(RangeSatistics rangestat);
