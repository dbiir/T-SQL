/*-------------------------------------------------------------------------
 *
 * range_allocator_score.c
 *	  Function used to calculate the best storage segment for range copies
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *	    src/backend/access/kv/range_allocator_score.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"
#include "tdb/range_universal.h"
#include "tdb/range_plan.h"
#include "tdb/range.h"
#include "tdb/route.h"
#include "tdb/range_allocator_score.h"

#define MAX_DIVERSITY_SCORE ((double)1.0)

static double
DiversityScore(seg_location l1, seg_location l2)
{
    Size compare_level = l1.location_level < l2.location_level ? l1.location_level : l2.location_level;

    for (int i = 0; i < compare_level; i++)
    {
        Size compare_length = l1.location_len[i] < l2.location_len[i] ? l1.location_len[i] : l2.location_len[i];
        int result = memcmp(l1.address[i], l2.address[i], compare_length);
        if (result != 0)
            return (double)(compare_level - i) / (double)(compare_level);
    }

    if (l1.location_level != l2.location_level)
    {
        return MAX_DIVERSITY_SCORE / (double)(compare_level + 1);
    }

    return 0;
}

double
rangeDiversityScore(SegmentID *replica_located_seg, int replica_num)
{
    double diversity_score = 0;
    int compare_pair = 0;
    for (int i = 0; i < replica_num; i++)
    {
        SegmentSatistics s1 = ssm_statistics->seg_statistics[replica_located_seg[i]];
        seg_location l1 = s1.location;
        for (int j = 0; j < i; j++)
        {
            SegmentSatistics s2 = ssm_statistics->seg_statistics[replica_located_seg[i]];
            seg_location l2 = s2.location;
            diversity_score += DiversityScore(l1, l2);
            compare_pair++;
        }
    }
    if (compare_pair == 0)
        return MAX_DIVERSITY_SCORE;

    return diversity_score / compare_pair;
}
