
/*----------------------------------
 * ms_plan.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/ms_plan.h
 *----------------------------------
 */
#include "tdb/range_plan.h"
#include "tdb/range_universal.h"
#define INVALID_PLANID 0

typedef struct RangeAvg
{
    double avg_read_size;
    double avg_write_size;
}RangeAvg;

extern double rebalance_threshold_percent;
//extern void freeRangePlanByPlan(List *rangeplanlist, RangePlan rp);
//extern RangePlan FindPlanByID(List *rangeplanlist, int planid);
//extern void freePlanByID(List *rangeplanlist, int planid);
extern SegmentSatistics* copyTempSegStat(List* context);
extern void freeRangePlan(RangePlan rp);
extern void update_stat_from_heart_beat(SSM_Statistics *temp_stat, RangeList *rangelist, List* context, int *length);
