/*-------------------------------------------------------------------------
 *
 * ms_plan.c
 *	  The main function used by the MS process to verify that range needs to be scheduled
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *	    src/backend/access/kv/ms_plan.c
 *
 *-------------------------------------------------------------------------
 */
#include "tdb/range.h"
#include "tdb/route.h"
#include "tdb/ms_plan.h"
#include "tdb/rangecache.h"
#include "tdb/range_allocator_score.h"
#include "tdb/rangestatistics.h"
#include "tdb/storage_param.h"
double RebalanceThresholdPercent = 0.3;
double MaxFractionUsedThreshold = 0.70;
double leaderRebalanceThresholdPercent = 0.1;
int RebalanceRangeCountMinDiffer = 3;
int RebalanceRangeCountMinStart = 10;
int TransferLeaderCountMinDiffer = 10;
int TransferLeaderCountMinStart = 50;
int MAX_RETRY_COUNT = 10;

static void
freeSplitPreparePlan(SplitPreparePlan sp)
{
	if (sp == NULL)
		return;
	freeRangeDescPoints(sp->split_range);
	if (sp->split_key.data != NULL)
		pfree(sp->split_key.data);
	pfree(sp);
}

static void
freeSplitPlan(SplitPlan sp)
{
	if (sp == NULL)
		return;
	freeRangeDescPoints(sp->old_range);
	freeRangeDescPoints(sp->new_range);
	pfree(sp);
}
static void
freeMergePlan(MergePlan mp)
{
	if (mp == NULL)
		return;
	freeRangeDescPoints(mp->first_range);
	freeRangeDescPoints(mp->second_range);

	pfree(mp);
}

static void
freeAddReplicaPlan(AddReplicaPlan rp)
{
	if (rp == NULL)
		return;
	freeRangeDescPoints(rp->range);
	pfree(rp);
}

static void
freeRemoveReplicaPlan(RemoveReplicaPlan rp)
{
	if (rp == NULL)
		return;
	freeRangeDescPoints(rp->range);
	pfree(rp);
}

static void
freeRebalanceReplicaPlan(RebalancePlan rp)
{
	if (rp == NULL)
		return;
	freeRangeDescPoints(rp->rebalance_range);
	pfree(rp);
}

void
freeRangePlan(RangePlan rp)
{
	if (rp == NULL)
		return;
	switch (rp->ms_plan_type)
	{
		case MS_HEART_BEAT:
			pfree(rp);
			break;
		case MS_SPLIT_PREPARE:
		case MS_SPLIT_PLAN:
			freeSplitPreparePlan((SplitPreparePlan)rp);
			break;
		case MS_SPLIT_COMPLETE:
			freeSplitPlan((SplitPlan)rp);
			break;
		case MS_MERGE_PLAN:
			freeMergePlan((MergePlan)rp);
			break;
		case MS_ADDREPLICA_PLAN:
			freeAddReplicaPlan((AddReplicaPlan)rp);
			break;
		case MS_REMOVEREPLICA_PLAN:
			freeRemoveReplicaPlan((RemoveReplicaPlan)rp);
			break;
		case MS_REBALANCE_PLAN:
			freeRebalanceReplicaPlan((RebalancePlan)rp);
			break;
		default:
			break;
	}
}

static bool
checkRangePlanByRangeID(RangeID rangeid, List* tempplan)
{
	if (tempplan == NULL)
		return false;
	int length = list_length(tempplan);
	if (length < 0 || length > 20)
		return false;
	for (int i = 0; i < length; i++)
	{
		RangePlan rp = list_nth(tempplan, i);
		switch (rp->ms_plan_type)
		{
			case MS_SPLIT_PREPARE:
				if (((SplitPreparePlan)rp)->split_range->rangeID == rangeid)
					return true;
				break;
			case MS_SPLIT_PLAN:
				if (((SplitPlan)rp)->new_range->rangeID == rangeid ||
					((SplitPlan)rp)->old_range->rangeID == rangeid)
					return true;
				break;
			case MS_MERGE_PLAN:
				if (((MergePlan)rp)->first_range->rangeID == rangeid ||
					((MergePlan)rp)->second_range->rangeID == rangeid)
					return true;
				break;
			case MS_ADDREPLICA_PLAN:
				if (((AddReplicaPlan)rp)->range->rangeID == rangeid)
					return true;
				break;
			case MS_REBALANCE_PLAN:
				if (((RebalancePlan)rp)->rebalance_range->rangeID == rangeid)
					return true;
				break;
			case MS_REMOVEREPLICA_PLAN:
				if (((RemoveReplicaPlan)rp)->range->rangeID == rangeid)
					return true;
				break;
			default:
				break;
		}
	}
	return false;
}

static bool
checkSegmentFullDisk(SegmentSatistics seg)
{
	double seg_use_size_percent = (double)seg.userSize / (double)seg.totalSize;
	return seg_use_size_percent > MaxFractionUsedThreshold;
}

static SplitPreparePlan
checkSplit(RangeSatistics rangestat, RangeDesc *range, SegmentID segid, SegmentSatistics* tempstat)
{
	if (rangestat.keybytes + rangestat.valuebytes > MAX_RANGE_SIZE)
	{
		SplitPreparePlan sp = palloc0(sizeof(SplitPreparePlanDesc));
		initRangePlanHead((RangePlan)sp, MS_SPLIT_PREPARE);
		sp->split_range = palloc0(sizeof(RangeDesc));
		sp->split_range = range;

		sp->targetSegID = segid;
		for (int i = 0; i < MAXSEGCOUNT; i++)
		{
			if (i != segid)
			{
				sp->header.send_type[i] = true;
				sp->header.complete_type[i] = true;
			}
		}
		return sp;
	}
	return NULL;
}

#define TEST_MERGE_RANGE
static KeySize
checkTwoRangeMerge(RangeDesc a, RangeDesc b)
{
	bool samereplica = compareReplica(a, b);
	if (!samereplica)
		return 0;
	RangeSatistics a_stat = ssm_statistics->range_statistics[a.rangeID];
	RangeSatistics b_stat = ssm_statistics->range_statistics[b.rangeID];
	KeySize size = a_stat.keybytes + a_stat.valuebytes + b_stat.keybytes + b_stat.valuebytes;
#ifndef TEST_MERGE_RANGE
	if (size > MAX_RANGE_SIZE)
		return 0;
#endif
	return size;
}

static MergePlan
checkMerge(RangeSatistics rangestat, RangeDesc *range, List *context, SegmentSatistics* tempstat)
{
	KeySize rangesize = rangestat.keybytes + rangestat.valuebytes;
	if (rangesize < MIN_RANGE_SIZE)
	{
		RangeDesc *next = FindRangeDescByKey(range->endkey, true);
		RangeDesc *prev = FindRangeDescByKey(range->startkey, false);
		KeySize nextsize = 0;
		KeySize prevsize = 0;
		if (next != NULL)
		{
			nextsize = checkTwoRangeMerge(*next, *range);
		}
		if (prev != NULL)
		{
			prevsize = checkTwoRangeMerge(*prev, *range);
		}
		if (nextsize == 0 && prevsize == 0)
		{
			return NULL;
		}
		MergePlan mp = palloc0(sizeof(MergePlanDesc));
		initRangePlanHead((RangePlan)mp, MS_MERGE_PLAN);
		if (nextsize >= 0 && prevsize >= 0)
		{
			if (nextsize > prevsize)
			{
				mp->first_range = range;
				mp->second_range = next;
			}
			else
			{
				mp->first_range = prev;
				mp->second_range = range;
			}
		}
		else if (nextsize >= 0)
		{
			mp->first_range = range;
			mp->second_range = next;
		}
		else if (prevsize >= 0)
		{
			mp->first_range = prev;
			mp->second_range = range;
		}
		/* Check to see if the range involved in the merge operation has the appropriate operation. */
 		if (checkRangePlanByRangeID(mp->first_range->rangeID, context) ||
		 	checkRangePlanByRangeID(mp->second_range->rangeID, context))
		{
			mp = NULL;
		}
		return mp;
	}
	else
	{
		return NULL;
	}

}

typedef struct ComparableStoreList
{
	CandidateDesc rebalance_source_candidate;
	CandidateDesc *rebalance_target_candidate;  /* CandidateDesc */
	int target_num;
}ComparableStoreList;

static double
compareTwoCandidate(CandidateDesc a, CandidateDesc b)
{
	if (b.isFull)
		return 5;
	if (a.isFull)
		return -5;
	if (a.diversityScore != b.diversityScore)
	{
		if (a.diversityScore > b.diversityScore)
			return 3;
		else
		{
			return -3;
		}
	}
	if (a.convergeScore != b.convergeScore)
	{
		if (a.convergeScore > b.convergeScore) {
			return 2 + (double)(a.convergeScore-b.convergeScore)/10.0;
		}
		return -(2 + (double)(b.convergeScore-a.convergeScore)/10.0);
	}
	if (a.rangeCount == 0 && b.rangeCount == 0)
	{
		return 0;
	}
	/*
	 * Load balancing is not allowed when the number of range is less than a
	 * certain value or the gap between the two target range numbers is too small.
	 */
	if (Abs(a.rangeCount - b.rangeCount) <= RebalanceRangeCountMinDiffer)
		return 0;
	if (a.rangeCount < RebalanceRangeCountMinStart &&
		b.rangeCount < RebalanceRangeCountMinStart)
		return 0;
	if (a.rangeCount < b.rangeCount)
	{
		return (double)(b.rangeCount - a.rangeCount) / (double)b.rangeCount;
	}
	return -(double)(a.rangeCount - b.rangeCount) / (double)a.rangeCount;
}

static int
sortCandidate(const void *a, const void *b)
{
	Assert(a);
	Assert(b);
	CandidateDesc candidate1 = *(CandidateDesc*)a;
	CandidateDesc candidate2 = *(CandidateDesc*)b;
	double result = compareTwoCandidate(candidate1, candidate2);
	if (result > 0)
		return -1;
	else if (result < 0)
		return 1;
	else
    {
		if (candidate1.segid > candidate2.segid)
            return 1;
        else if (candidate1.segid < candidate2.segid)
            return -1;
        else
            return 0;
    }
}

static CandidateDesc*
getBetterEnoughCandidate(CandidateDesc* descendingCandidateList, int *list_count)
{
	/* As long as the distribution is good enough, we can choose it. */
	CandidateDesc* best = palloc0(sizeof(CandidateDesc) * MAXSEGCOUNT);
	CandidateDesc first = descendingCandidateList[0];
	best[0] = first;
	int i = 1;
	for (; i < *list_count; i++)
	{
		CandidateDesc temp = descendingCandidateList[i];
		if ((temp.diversityScore == first.diversityScore))
		{
			best[i] = temp;
		}
		else
		{
			*list_count = i;
			return best;
		}
	}
	*list_count = i;
	return best;
}

static CandidateDesc*
getBestCandidate(CandidateDesc* descendingCandidateList, int *list_count)
{
	/* As long as the distribution is good enough, we can choose it. */
	CandidateDesc* best = palloc0(sizeof(CandidateDesc) * MAXSEGCOUNT);
	CandidateDesc first = descendingCandidateList[0];
	best[0] = first;
	int i = 1;
	for (; i < *list_count; i++)
	{
		CandidateDesc temp = descendingCandidateList[i];
		if (compareTwoCandidate(temp, first) == 0)
		{
			best[i] = temp;
		}
		else
		{
			//repalloc((void*)best, sizeof(CandidateDesc) * i);
			*list_count = i;
			return best;
		}
	}
	//repalloc((void*)best, sizeof(CandidateDesc) * i);
	*list_count = i;
	return best;
}

static BalanceScore
computeBalanceScore(SegmentID source, SegmentID* targetList, int list_count, SegmentSatistics* tempstat)
{
	/*
	 * Simulate the migration of one range to another node to calculate
	 * whether the overall storage score is increased
	 */
	double avgReadSize, avgWriteSize, TotalUseSize, TotalSize;
	double avgRangeCount = 0;
	for (int i = 0; i < list_count; i++)
	{
		SegmentID id = targetList[i];
		SegmentSatistics segstatus = tempstat[id];
		avgReadSize += (double)(segstatus.readSize);
		avgWriteSize += (double)(segstatus.writeSize);
		TotalUseSize += (double)(segstatus.userSize);
		TotalSize += (double)(segstatus.totalSize);
		avgRangeCount += (double)(segstatus.rangeCount);
	}
	avgReadSize /= list_count;
	avgWriteSize /= list_count;
	avgRangeCount /= list_count;

	SegmentID sourceid = source;
	SegmentSatistics sourceStat = tempstat[sourceid];
	if (avgRangeCount - (double)sourceStat.rangeCount < 1 && avgRangeCount - (double)sourceStat.rangeCount > -1)
		return BALANCE;
	if ((double)sourceStat.rangeCount > avgRangeCount * (1 + RebalanceThresholdPercent))
		return OVERFULL;
	else if ((double)sourceStat.rangeCount < avgRangeCount * (1 - RebalanceThresholdPercent))
		return UNDERFULL;
	else
		return BALANCE;
}

static bool
computeConvergeScore(SegmentID source, SegmentID* targetList, int list_count, bool isSource, SegmentSatistics* tempstat)
{
	double avgReadSize, avgWriteSize, TotalUseSize, TotalSize;
	double avgRangeCount = 0;
	for (int i = 0; i < list_count; i++)
	{
		SegmentID id = targetList[i];
		SegmentSatistics segstatus = tempstat[id];
		avgReadSize += (double)(segstatus.readSize);
		avgWriteSize += (double)(segstatus.writeSize);
		TotalUseSize += (double)(segstatus.userSize);
		TotalSize += (double)(segstatus.totalSize);
		avgRangeCount += (double)(segstatus.rangeCount);
	}
	avgReadSize /= list_count;
	avgWriteSize /= list_count;
	avgRangeCount /= list_count;

	SegmentID sourceid = source;
	SegmentSatistics sourceStat = tempstat[sourceid];

	double newCount = sourceStat.rangeCount;
	if (isSource)
		newCount--;
	else
		newCount++;

	return (Abs(newCount - avgRangeCount) < Abs(sourceStat.rangeCount - avgRangeCount));
}

typedef struct RebalanceOptions
{
	CandidateDesc existingCandidates;
	CandidateDesc* candidates;
	int target_num;
}RebalanceOptions;

static CandidateDesc
newEmptyCandidate()
{
	CandidateDesc ca;
	ca.convergeScore = 0;
	ca.diversityScore = 0;
	ca.balanceScore = BALANCE;
	ca.isFull = false;
	ca.rangeCount = 0;
	ca.segid = -1;
	return ca;
}

static SegmentID*
getCandidateSegID(CandidateDesc* target, int target_num)
{
	SegmentID *target_segid = palloc0(sizeof(SegmentID) * target_num);
	for (int j = 0; j < target_num; j++)
	{
		target_segid[j] = target[j].segid;
	}
	return target_segid;
}

static SegmentID*
getAllValidSegID()
{
	SegmentID *all_segid = palloc0(sizeof(SegmentID) * ssm_statistics->seg_count);
	for (int i = 0; i < ssm_statistics->seg_count; i++)
	{
		all_segid[i] = ssm_statistics->seg_statistics[i].segmentID;
	}
	return all_segid;
}

static void
freeComparableStoreList(ComparableStoreList* list, int length)
{
	if (list == NULL)
		return;
	for (int i = 0; i < length; i++)
	{
		if (list[i].rebalance_target_candidate == NULL)
			continue;
		pfree(list[i].rebalance_target_candidate);
	}
	pfree(list);
	return;
}

static RebalanceOptions*
findRebalanceCandidates(RangeDesc range, int *length, SegmentSatistics* tempstat)
{
	/*
	 * First of all, count the storage nodes of all the copies at present.
	 */
	int total_seg_count = ssm_statistics->seg_count;

	CandidateDesc *rebalance_source_candidate = palloc0(sizeof(CandidateDesc) * total_seg_count);
	int rsc = 0;

	SegmentID *use_segid = getRangeSegID(range);
	int seg_count = range.replica_num;

	double curDiversityScore = rangeDiversityScore(use_segid, seg_count);
	bool needRebalanceFrom = false;
	/*
	 * First: first record the list of seg that require process load balancing.
	 */
	for (int i = 0; i < seg_count; i++)
	{
		SegmentID id = use_segid[i];
		SegmentSatistics segStat = tempstat[id];
		bool isFull = checkSegmentFullDisk(segStat);
		/* TODO: After that, we need to consider the rebalance after the seg node crashes. */
		if (isFull && !needRebalanceFrom)
			needRebalanceFrom = true;
		CandidateDesc source = newEmptyCandidate();
		source.segid = id;
		source.diversityScore = curDiversityScore;
		source.rangeCount = segStat.rangeCount;
		source.isFull = isFull;
		source.balanceScore = 0;

		rebalance_source_candidate[rsc] = source;
		rsc++;
	}
	/*
	 * Second: here to see if there are any suitable candidate nodes
	 */
	bool needRebalanceTo = false;

	ComparableStoreList* comparableSegs = palloc0(sizeof(ComparableStoreList) * total_seg_count);
	int cs = 0;
	for (int i = 0; i < rsc; i++)
	{
		CandidateDesc source = rebalance_source_candidate[i];

		bool needSkip = false;
		/* Here is to determine if there is an original node with exactly the same storage location. */
		for (int j = 0; j < cs; j++)
		{
			ComparableStoreList comSegs = comparableSegs[j];
			if (source.segid == comSegs.rebalance_source_candidate.segid)
			{
				needSkip = true;
				break;
			}
		}
		if (needSkip)
			continue;
		/* This is where the official selection of the target node begins. */

		CandidateDesc *rebalance_target_candidate = palloc0(sizeof(CandidateDesc) * total_seg_count);
		int rtc = 0;
		for (int j = 0; j < total_seg_count; j++)
		{
			bool isleader = false;
			Replica replica = findUpReplicaOnOneSeg(range, j, &isleader);
			if (replica != NULL)
				continue;
			SegmentSatistics segStat = tempstat[j];
			bool isFull = checkSegmentFullDisk(segStat);
			SegmentID *candidate_use_segid = palloc0(sizeof(SegmentID) * total_seg_count);

			int current_seg_count = range.replica_num;
			for (int k = 0; k < current_seg_count; k++)
			{
				if (source.segid == use_segid[k])
					candidate_use_segid[k] = segStat.segmentID;
				else
					candidate_use_segid[k] = use_segid[k];
			}
			double DiversityScore = rangeDiversityScore(candidate_use_segid, seg_count);
			pfree(candidate_use_segid);

			CandidateDesc cur_target = newEmptyCandidate();
			cur_target.segid = segStat.segmentID;
			cur_target.rangeCount = segStat.rangeCount;
			cur_target.isFull = isFull;
			cur_target.diversityScore = DiversityScore;
			cur_target.balanceScore = 0;

			/* here we need this seg is better than old */
			if (compareTwoCandidate(cur_target, source) >= 0)
			{
				rebalance_target_candidate[rtc] = cur_target;
				rtc++;
				if (!needRebalanceFrom && !needRebalanceTo &&
					compareTwoCandidate(cur_target, source) > (double)0)
				{
					needRebalanceTo = true;
				}
			}
		}
		qsort((void*)rebalance_target_candidate, rtc, sizeof(CandidateDesc), sortCandidate);
		CandidateDesc *bestCandidate = getBetterEnoughCandidate(rebalance_target_candidate, &rtc);

		ComparableStoreList curCsL;
		curCsL.rebalance_source_candidate = source;
		curCsL.rebalance_target_candidate = bestCandidate;
		curCsL.target_num = rtc;

		comparableSegs[cs] = curCsL;
		cs++;
		pfree(rebalance_target_candidate);
		rebalance_target_candidate = NULL;
	}
	/*
	 * Third: Decide whether we should try to rebalance
	 */
	bool needRebalance = needRebalanceFrom || needRebalanceTo;
	if (!needRebalance)
	{
		return NULL;
	}
	/*
	 * Fourth: Refer to CRDB, Create sets of rebalance options
	 */
	RebalanceOptions *rebalance_option = palloc0(sizeof(RebalanceOptions) * cs);
	int ro = 0;
	for (int i = 0; i < cs; i++)
	{
		ComparableStoreList comparableStoreList = comparableSegs[i];
		CandidateDesc source_desc = comparableStoreList.rebalance_source_candidate;
		int target_num = comparableStoreList.target_num;
		CandidateDesc* target_list = comparableStoreList.rebalance_target_candidate;

		CandidateDesc* option_target_list = palloc0(sizeof(CandidateDesc) * comparableStoreList.target_num);
		int option_target_list_count = 0;

		SegmentID *targetSegID = getCandidateSegID(target_list, target_num);
		/* TODO:Here we need to check whether the source candidate seg can be migrated */
		{
			source_desc.balanceScore = computeBalanceScore(source_desc.segid,
									targetSegID, target_num, tempstat);
			if (!computeConvergeScore(source_desc.segid, targetSegID, target_num, true, tempstat))
				source_desc.convergeScore = 1;
		}

		for (int j = 0; j < target_num; j++)
		{
			CandidateDesc target = comparableStoreList.rebalance_target_candidate[j];
			bool skip = false;
			/* Verify that the target seg has stored a backup */
			for (int k = 0; k < rsc; k++)
			{
				if (rebalance_source_candidate[k].segid == target.segid)
				{
					skip = true;
					break;
				}
			}
			if (skip)
				continue;

			target.balanceScore = computeBalanceScore(target.segid, targetSegID, target_num, tempstat);
			if (computeConvergeScore(source_desc.segid, targetSegID, target_num, false, tempstat))
				target.convergeScore = 1;
			else if (!needRebalance)
				continue;

			option_target_list[option_target_list_count++] = target;
		}
		if (option_target_list_count == 0)
			continue;
		qsort((void*)option_target_list, option_target_list_count, sizeof(CandidateDesc), sortCandidate);
		CandidateDesc *final_target = getBestCandidate(option_target_list, &option_target_list_count);

		RebalanceOptions result;
		result.candidates = final_target;
		result.target_num = option_target_list_count;
		result.existingCandidates = source_desc;

		rebalance_option[ro++] = result;

		pfree(option_target_list);
		option_target_list = NULL;
		pfree(targetSegID);
		targetSegID = NULL;
	}
	*length = ro;

	freeComparableStoreList(comparableSegs, cs);
	comparableSegs = NULL;
	pfree(rebalance_source_candidate);
	pfree(use_segid);
	return rebalance_option;
}

static CandidateDesc*
findRemoveCandidates(RangeDesc range, int *length, SegmentSatistics* tempstat)
{
	SegmentID* replica_segid = getRangeSegID(range);
	int replica_num = range.replica_num;
	Assert(range.replica_num > 0);
	CandidateDesc *remove_candidate = palloc0(sizeof(CandidateDesc) * MAXSEGCOUNT);
	int remove_candidate_count = 0;
	for (int i = 0; i < replica_num; i++)
	{
		Replica current = range.replica[i];
		double diversityScore = rangeDiversityScore(replica_segid, replica_num);
		BalanceScore balanceScore = computeBalanceScore(current->segmentID,
									replica_segid, replica_num, tempstat);
		int convergesScore = 0;
		if (!computeConvergeScore(current->segmentID, replica_segid, replica_num, true, tempstat))
		{
			convergesScore = 1;
		}

		CandidateDesc ca = newEmptyCandidate();
		ca.segid = current->segmentID;
		ca.rangeCount = tempstat[current->segmentID].rangeCount;
		ca.isFull = checkSegmentFullDisk(tempstat[current->segmentID]);
		ca.diversityScore = diversityScore;
		ca.convergeScore = convergesScore;
		ca.balanceScore = balanceScore;

		remove_candidate[remove_candidate_count++] = ca;
	}
	//repalloc(remove_candidate, sizeof(CandidateDesc) * remove_candidate_count);
	qsort((void*)remove_candidate, remove_candidate_count, sizeof(CandidateDesc), sortCandidate);
	*length = remove_candidate_count;
	pfree(replica_segid);
	return remove_candidate;
}

static ReplicaID
checkRemoveReplica(RangeDesc range, SegmentSatistics* tempstat)
{
	int length = 0;
	CandidateDesc *result = findRemoveCandidates(range, &length, tempstat);
	ReplicaID id = -1;
	if (length == 0)
		return -1;
	for (int i = length - 1; i >= 0; i++)
	{
		CandidateDesc target = result[i];
		bool find = false;
		for (int j = 0; j < range.replica_num; j++)
		{
			if (target.segid == range.replica[j]->segmentID)
			{
				id = range.replica[j]->replicaID;
				find = true;
				break;
			}
		}
		if (find)
			break;
	}
	if (result == NULL)
		pfree(result);
	return id;
}

static ReplicaID
simulateRemoveReplica(RangeDesc range, SegmentSatistics* tempstat, Replica newReplica)
{
	SegmentID segid = newReplica->segmentID;
	tempstat[segid].rangeCount++;
	ReplicaID replicaid = checkRemoveReplica(range, tempstat);
	tempstat[segid].rangeCount--;
	return replicaid;
}

static double*
computeLeaderTargetScore(SegmentID source, SegmentSatistics* tempstat)
{
	double avg_leader_count = 0;
	for (int i = 0; i < ssm_statistics->seg_count; i++)
	{
		avg_leader_count += tempstat[i].leaderCount;
	}
	avg_leader_count /= ssm_statistics->seg_count;
	double Max_threshold = (1 + leaderRebalanceThresholdPercent) * avg_leader_count;
	double Min_threshold = (1 - leaderRebalanceThresholdPercent) * avg_leader_count;
	double *all_seg_score = palloc0(sizeof(double) * ssm_statistics->seg_count);
	for (int i = 0; i < ssm_statistics->seg_count; i++)
	{
		double overfullscore = tempstat[source].leaderCount - Max_threshold;
		double underfullscore = Min_threshold - tempstat[i].leaderCount;
		all_seg_score[i] = overfullscore + underfullscore;
		if (tempstat[i].leaderCount < TransferLeaderCountMinStart &&
			tempstat[source].leaderCount < TransferLeaderCountMinStart)
			all_seg_score[i] = 0;
		if (Abs(tempstat[i].leaderCount - tempstat[source].leaderCount) < TransferLeaderCountMinDiffer)
			all_seg_score[i] = 0;
	}

	return all_seg_score;
}

static Replica
findTransferLeaderReplica(RangeDesc range, SegmentSatistics* tempstat)
{
	if (range.replica == NULL)
		return NULL;
	Replica sourceleader = findUpLeader(range);
	SegmentID sourceseg = sourceleader->segmentID;

	/*
	 * TODO: We need to record the location of the request and calculate
	 * the dispersion of each copy with the request. Because we want the
	 * leader to be as close to the requested location as possible to
	 * reduce network latency.
	 *
	 * However, there is currently no place to record where the request
	 * came from. Therefore, at present, only the read and write situation
	 * of each node and the number of leader are considered.
	 */

	double *all_seg_score = computeLeaderTargetScore(sourceseg, tempstat);
	double max_score = 0;
	SegmentID target = -1;
	for (int i = 0; i < ssm_statistics->seg_count; i++)
	{
		bool isleader = false;
		Replica replica = findUpReplicaOnOneSeg(range, i, &isleader);
		if (isleader || replica == NULL)
			continue;
		if (all_seg_score[i] > max_score)
		{
			max_score = all_seg_score[i];
			target = i;
		}
	}
	Replica replica = NULL;
	if (target != -1 && max_score > 0)
	{
		bool isleader = false;
		replica = findUpReplicaOnOneSeg(range, target, &isleader);
	}
	pfree(all_seg_score);
	return replica;
}

static AddReplicaPlan
checkRebalance(RangeDesc *range, SegmentSatistics* tempstat)
{
	int length = 0;
	Replica leader = findUpLeader(*range);
	RebalanceOptions* results = findRebalanceCandidates(*range, &length, tempstat);
	/* Simulation when a copy is added to the target seg, the removed copy is normal. */
	CandidateDesc target = newEmptyCandidate();
	SegmentID sourceID = -1;
	for (int i = 0; i < length; i++)
	{
		//for (int j = 0; j < results[i].target_num; j++)
		//{
		int j = 0;
		target = results[i].candidates[j];
		RangeDesc simulateRange = copyRangeDesc(*range);
		Replica *simulateReplica = palloc0(sizeof(Replica) * (simulateRange.replica_num + 1));
		for (int k = 0; k < simulateRange.replica_num; k++)
		{
			simulateReplica[k] = palloc0(sizeof(ReplicaDesc));
			*simulateReplica[k] = *simulateRange.replica[k];
			pfree(simulateRange.replica[k]);
		}
		pfree(simulateRange.replica);
		simulateRange.replica = simulateReplica;

		Replica newReplica = palloc0(sizeof(ReplicaDesc));
		newReplica->segmentID = target.segid;
		newReplica->LeaderReplicaID = simulateRange.replica[0]->LeaderReplicaID;
		newReplica->replica_state = follower_working;
		newReplica->replicaID = simulateRange.replica_num;
		simulateRange.replica[simulateRange.replica_num++] = newReplica;
		ReplicaID id = -1;
		id = simulateRemoveReplica(simulateRange, tempstat, newReplica);
		if (id == -1)
		{
			continue;
		}
		if (id != newReplica->replicaID && id != leader->replicaID)
		{
			sourceID = simulateRange.replica[id]->segmentID;
			freeRangeDesc(simulateRange);
			break;
		}
		//}
	}
	if (sourceID == -1)
	{
		return NULL;
	}
	AddReplicaPlan addplan = palloc0(sizeof(AddReplicaPlanDesc));
	initRangePlanHead((RangePlan)addplan, MS_ADDREPLICA_PLAN);

	addplan->range = range;
	addplan->targetSegID = target.segid;

	/*RebalancePlan rebalance = palloc0(sizeof(RebalancePlanDesc));
	initRangePlanHead((RangePlan)rebalance, MS_REBALANCE_PLAN);

	rebalance->rebalance_range = range;
	rebalance->sourceSegID = sourceID;
	rebalance->targetSegID = target.segid;*/

	pfree(results);
	return addplan;
}

static RangePlan*
checkReplicaNum(RangeDesc *range, int *length, SegmentSatistics* tempstat)
{
	RangePlan *rangeplan = palloc0(sizeof(RangePlan) * PLAN_NUM);
	int rangeplan_count = 0;
	int default_replica_num = DEFAULT_REPLICA_NUM;
	if (range->replica_num < default_replica_num)
	{
		int needAddReplicaCount = default_replica_num - range->replica_num;
		SegmentID *seglist = palloc0(sizeof(SegmentID) * (default_replica_num));
		MemSet(seglist, -1, sizeof(SegmentID) * (default_replica_num));

		bool find = false;
		seglist[0] = GetBestTargetSegment(seglist, 0, &find, *range, tempstat);
		if (!find)
		{
			ereport(WARNING,
				(errmsg("MS: ADD REPLICA: cannot find a valid segment to add replica. rangeid: %d, need add count: %d",
					range->rangeID, needAddReplicaCount)));
			return NULL;
		}
		AddReplicaPlan addplan = palloc0(sizeof(AddReplicaPlanDesc));
		initRangePlanHead((RangePlan)addplan, MS_ADDREPLICA_PLAN);

		addplan->range = range;
		addplan->targetSegID = seglist[0];

		rangeplan[rangeplan_count] = (RangePlan)addplan;
		rangeplan_count++;

		pfree(seglist);
	}
	else if (range->replica_num > default_replica_num)
	{
		int needRemoveReplicaCount = range->replica_num - default_replica_num;
		//SegmentID *seglist = palloc0(sizeof(SegmentID) * default_replica_num);
		//MemSet(seglist, -1, sizeof(SegmentID) * default_replica_num);

		// bool find = false;
		//seglist[i] = GetBestSourceSegment(seglist, i, &find, *range, tempstat);
		ReplicaID replicaid = checkRemoveReplica(*range, tempstat);
		Replica replica = findUpReplicaByReplicaID(*range, replicaid);
		if (replica == NULL)
		{
			ereport(WARNING,
				(errmsg("MS: REMOVE REPLICA: cannot find a valid segment to remove replica. rangeid: %d, need add count: %d",
					range->rangeID, needRemoveReplicaCount)));
			return NULL;
		}
		bool isleader = replica->LeaderReplicaID == replica->replicaID;
		if (isleader)
		{
			Replica targetreplica = findTransferLeaderReplica(*range, tempstat);
			if (targetreplica != NULL)
			{
				TransferLeaderPlan transplan = palloc0(sizeof(TransferLeaderPlanDesc));
				initRangePlanHead((RangePlan)transplan, MS_TRANSFERLEADER_PLAN);

				transplan->range = range;
				transplan->targetID = targetreplica->replicaID;

				rangeplan[rangeplan_count] = (RangePlan)transplan;
				rangeplan_count++;
			}
			else
			{
				return NULL;
			}

		}
		else
		{
			RemoveReplicaPlan removeplan = palloc0(sizeof(RemoveReplicaPlanDesc));
			initRangePlanHead((RangePlan)removeplan, MS_REMOVEREPLICA_PLAN);

			removeplan->range = range;
			removeplan->targetSegID = replica->segmentID;

			rangeplan[rangeplan_count] = (RangePlan)removeplan;
			rangeplan_count++;
		}
	}
	else
	{
		Replica targetreplica = findTransferLeaderReplica(*range, tempstat);
		if (targetreplica == NULL)
		{
			/* Now start thinking about load balancing. */
			AddReplicaPlan rebalanceplan = checkRebalance(range, tempstat);
			if (rebalanceplan != NULL)
			{
				rangeplan[rangeplan_count] = (RangePlan)rebalanceplan;
				rangeplan_count++;
			}
		}
		else
		{
			TransferLeaderPlan transplan = palloc0(sizeof(TransferLeaderPlanDesc));
			initRangePlanHead((RangePlan)transplan, MS_TRANSFERLEADER_PLAN);
			transplan->range = range;
			transplan->targetID = targetreplica->replicaID;

			rangeplan[rangeplan_count] = (RangePlan)transplan;
			rangeplan_count++;
		}
	}
	//repalloc(rangeplan, sizeof(RangePlan) * rangeplan_count);
	*length = rangeplan_count;
	return rangeplan;
}

static void
updateTempSegStat(SegmentSatistics* segstat, RangePlan rp)
{
	switch (rp->ms_plan_type)
	{
	case MS_SPLIT_PREPARE:
		{
			SplitPreparePlan sp = (SplitPreparePlan)rp;
			for (int i = 0; i < sp->split_range->replica_num; i++)
			{
				SegmentID segid = sp->split_range->replica[i]->segmentID;
				segstat[segid].rangeCount++;
				segstat[segid].leaderCount++;
			}
		}
		break;
	case MS_SPLIT_PLAN:
		{
			SplitPlan sp = (SplitPlan)rp;
			for (int i = 0; i < sp->old_range->replica_num; i++)
			{
				SegmentID segid = sp->old_range->replica[i]->segmentID;
				segstat[segid].rangeCount++;
				segstat[segid].leaderCount++;
			}
		}
		break;
	case MS_MERGE_PLAN:
		{
			MergePlan mp = (MergePlan)rp;
			for (int i = 0; i < mp->first_range->replica_num; i++)
			{
				Replica replica = mp->first_range->replica[i];
				segstat[replica->segmentID].rangeCount--;
				if (replica->LeaderReplicaID == replica->replicaID)
					segstat[replica->segmentID].leaderCount--;
			}
		}
		break;
	case MS_ADDREPLICA_PLAN:
		{
			AddReplicaPlan ap = (AddReplicaPlan)rp;
			int rangeindex = FindRangeStatisticsByRangeID(ap->range->rangeID);
			RangeSatistics rangestat = ssm_statistics->range_statistics[rangeindex];
			SegmentID id = ap->targetSegID;
			segstat[id].rangeCount++;
			segstat[id].userSize += (rangestat.keybytes + rangestat.valuebytes);
			segstat[id].availableSize -= (rangestat.keybytes + rangestat.valuebytes);
		}
		break;
	case MS_REMOVEREPLICA_PLAN:
		{
			RemoveReplicaPlan ap = (RemoveReplicaPlan)rp;
			int rangeindex = FindRangeStatisticsByRangeID(ap->range->rangeID);
			RangeSatistics rangestat = ssm_statistics->range_statistics[rangeindex];
			SegmentID id = ap->targetSegID;
			segstat[id].rangeCount--;
			segstat[id].userSize -= (rangestat.keybytes + rangestat.valuebytes);
			segstat[id].availableSize += (rangestat.keybytes + rangestat.valuebytes);
		}
		break;
	case MS_REBALANCE_PLAN:
		{
			RebalancePlan ap = (RebalancePlan)rp;
			SegmentID sid = ap->sourceSegID;
			SegmentID tid = ap->targetSegID;
			int rangeindex = FindRangeStatisticsByRangeID(ap->rebalance_range->rangeID);
			RangeSatistics rangestat = ssm_statistics->range_statistics[rangeindex];
			segstat[sid].rangeCount--;
			segstat[tid].rangeCount++;

			segstat[sid].userSize -= (rangestat.keybytes + rangestat.valuebytes);
			segstat[sid].availableSize += (rangestat.keybytes + rangestat.valuebytes);

			segstat[tid].userSize += (rangestat.keybytes + rangestat.valuebytes);
			segstat[tid].availableSize -= (rangestat.keybytes + rangestat.valuebytes);
		}
		break;
	case MS_TRANSFERLEADER_PLAN:
		{
			TransferLeaderPlan ap = (TransferLeaderPlan)rp;
			Replica replica = findUpReplicaByReplicaID(*ap->range, ap->targetID);
			Replica leader = findUpLeader(*ap->range);
			segstat[leader->segmentID].leaderCount--;
			segstat[replica->segmentID].leaderCount++;
		}
		break;
	default:
		break;
	}
}

SegmentSatistics*
copyTempSegStat(List* context)
{
	SegmentSatistics* tempstat = palloc0(sizeof(SegmentSatistics) * ssm_statistics->seg_count);
	for (int i = 0; i < ssm_statistics->seg_count; i++)
	{
		tempstat[i] = ssm_statistics->seg_statistics[i];
	}
	if (context == NULL)
		return tempstat;
	int length = list_length(context);
	for (int i = 0; i < length; i++)
	{
		RangePlan rp = list_nth(context, i);
		updateTempSegStat(tempstat, rp);
	}
	return tempstat;
}

void
update_stat_from_heart_beat(SSM_Statistics *temp_stat, RangeList *rangelist, List* context, int *length)
{
	/* Update range information from each node */
	for (int i = 0; i < rangelist->range_count; i++)
	{
		RangeDesc range = TransferBufferToRangeDesc(rangelist->buffer[i]);
		AddRangeListCache(range);
		freeRangeDesc(range);
	}
	/* Update seg statistics from each node */
	SegmentSatistics segstat = temp_stat->seg_statistics[0];
	ssm_statistics->seg_statistics[segstat.segmentID] = segstat;
	if (segstat.segmentID > ssm_statistics->seg_count + 1)
		ssm_statistics->seg_count = segstat.segmentID + 1;

	/* Update range statistics from each node. */
	for (int i = 0; i < temp_stat->range_count; i++)
	{
		RangeSatistics rangestat = temp_stat->range_statistics[i];
		RangeDesc *range = FindRangeDescByRangeID(rangestat.rangeID);
		Replica replica = findUpLeader(*range);
		if (segstat.segmentID != replica->segmentID)
			continue;
		ssm_statistics->range_statistics[rangestat.rangeID] = rangestat;
		if (rangestat.rangeID + 1 > ssm_statistics->range_count)
		{
			ssm_statistics->range_count = rangestat.rangeID + 1;
		}
	}

	/*
	 * Traverse all the range, to see if any range needs to add or
	 * delete copies or switch leader.
	 */
	//RangePlan* tempplanlist = palloc0(sizeof(RangePlan) * temp_stat->range_count);
	int pl_index = 0;
	SegmentSatistics* tempstat = copyTempSegStat(context);
	for (int i = 0; i < temp_stat->range_count; i++)
	{
		RangeSatistics rangestat = temp_stat->range_statistics[i];
		RangeDesc *range = FindRangeDescByRangeID(rangestat.rangeID);
		if (range->rangeID == rootRouteRangeID || range->rangeID == -1)
		{
			continue;
		}
		bool isleader = false;
		findUpReplicaOnOneSeg(*range, segstat.segmentID, &isleader);
		if (!isleader)
			continue;
		/* Here we check the operation of a single range, such as split,
		 * add and delete copies, and so on. Make sure this range has no other plans.
		 */
		if (checkRangePlanByRangeID(range->rangeID, context))
		{
			continue;
		}
		SplitPreparePlan sp = checkSplit(rangestat, range, segstat.segmentID, tempstat);
		MergePlan mp = checkMerge(rangestat, range, context, tempstat);

		int length = 0;
        RangePlan* replicaPlan = NULL;
        if (enable_paxos)
		    replicaPlan = checkReplicaNum(range, &length, tempstat);
		if (sp == NULL && mp == NULL)
		{
			for (int j = 0; j < length; j++)
			{
				context = lcons((RangePlan)replicaPlan[j], context);
				updateTempSegStat(tempstat, (RangePlan)replicaPlan[j]);
			}
			if (replicaPlan != NULL)
				pfree(replicaPlan);
		}
		if (sp != NULL)
		{
			context = lcons((RangePlan)sp, context);
			updateTempSegStat(tempstat, (RangePlan)sp);
		}
		if (sp == NULL && mp != NULL)
		{
			context = lcons((RangePlan)mp, context);
			updateTempSegStat(tempstat, (RangePlan)mp);
		}
	}
	*length = pl_index;
	return ;
}
