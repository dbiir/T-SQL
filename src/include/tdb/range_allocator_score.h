
/*----------------------------------
 * range_allocator_score.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/range_allocator_score.h
 *----------------------------------
 */
#ifndef RANGE_ALLOCATOR_SCORE
#define RANGE_ALLOCATOR_SCORE

#include "tdb/range_plan.h"
#include "tdb/range_universal.h"

typedef enum BalanceScore
{
	OVERFULL,
	BALANCE,
	UNDERFULL,

}BalanceScore;

typedef struct CandidateDesc
{
	SegmentID segid;
	double diversityScore;
	BalanceScore balanceScore;
	int rangeCount;
	int convergeScore;
	bool isFull;
}CandidateDesc;


extern double rangeDiversityScore(SegmentID *replica_located_seg, int replica_num);

#endif
