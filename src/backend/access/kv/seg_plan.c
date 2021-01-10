/*----------------------------------
 *
 * ms_plan.c
 *
 * Portions Copyright (c) 2019-Present, TDSQL
 *
 * src/backend/access/kv/ms_plan.c
 *----------------------------------
 */

#include "tdb/range.h"
#include "tdb/route.h"
#include "tdb/seg_plan.h"
#include "cdb/cdbvars.h"
#include "tdb/rangecache.h"

SplitPreparePlan
seg_check_one_range_split(RangeSatistics rangestat)
{
    SplitPreparePlan sp = palloc0(sizeof(SplitPreparePlanDesc));
    Size rangesize = rangestat.keybytes + rangestat.valuebytes;
    RangeDesc *range = FindRangeDescByRangeID(rangestat.rangeID);
    bool isleader = false;
    findUpReplicaOnThisSeg(*range, &isleader);
    if (!isleader)
    {
        return NULL;
    }
    if (rangesize > MAX_RANGE_SIZE)
    {
        sp->split_range = palloc0(sizeof(RangeDesc));
        *sp->split_range = findUpRangeDescByID(rangestat.rangeID);
        sp->split_key = getRangeMiddleKey(*sp->split_range, rangesize);
        sp->header.ms_plan_type = MS_SPLIT_PREPARE;
        sp->header.plan_id = 0;
        sp->new_range_id = UNVALID_RANGE_ID;
        sp->targetSegID = GpIdentity.segindex;
        return sp;
    }
    else
    {
        return NULL;
    }
}
