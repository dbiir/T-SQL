/*-------------------------------------------------------------------------
 *
 * ms_reciever.c
 *	  Implementation of handling of MS messages
 *
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/ms/ms_reciever.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "postmaster/fts.h"
#include "postmaster/postmaster.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "replication/gp_replication.h"
#include "storage/fd.h"
#include "tdb/rangestatistics.h"
#include "tdb/seg_plan.h"
#include "tdb/ms_plan.h"
#include "tdb/range.h"
#include "tdb/route.h"
#include "tdb/ms.h"
#include "cdb/cdbvars.h"
#include "tdb/rangecache.h"
#include "tdb/tdbkvam.h"

static void SendMSHeartBeat(const char *messagetype, HeartBeatPlan hp, int planid);
static void HandleCheckOneSplit(const char *messagetype, SplitPreparePlan spp, int planid);
static void SendMSSplitReq(StringInfoData *buf, SplitPreparePlan splitplan, int planid);
static void HandleMSSplit(const char *messagetype, SplitPlan sp, int planid);

static void
setRangeDescState(RangeDesc *range, SplitPlan sp)
{
	for (int i = 0; i < range->replica_num; i++)
	{
		replica_state_machine(range->replica[i], *((RangePlan)sp));
	}
}

static void
SendMSHeartBeat(const char *messagetype, HeartBeatPlan hp, int planid)
{
	for (int i = 0; i < hp->segnum; i++)
	{
		memcpy(ssm_statistics->ip[i], hp->ip[i], strlen(hp->ip[i]));
	}
	int leadercount = 0;
	StringInfoData buf;
	initStringInfo(&buf);

	BeginCommand(messagetype, DestRemote);
	/* Send a M-S message */
	pq_beginmessage(&buf, 'M');
	pq_sendint(&buf, MS_HEART_BEAT, 4);
	pq_sendint(&buf, planid, 4);

	Assert(ssm_statistics != NULL);
	/* Statistics section */
	pq_sendint(&buf, ssm_statistics->range_count, 4);		/* # range statistics len */
	ssm_statistics->seg_statistics[0].rangeCount = ssm_statistics->range_count;

	for (int i = 0; i < ssm_statistics->range_count; i++)
	{
		RangeSatistics rangestat = ssm_statistics->range_statistics[i];

		RangeDesc *range = FindRangeDescByRangeID(rangestat.rangeID);
		bool isleader;
		findUpReplicaOnThisSeg(*range, &isleader);
		if (isleader)
			leadercount++;
		pq_sendint(&buf, rangestat.rangeID, 4);

		pq_sendint(&buf, rangestat.keybytes, 4);
		pq_sendint(&buf, rangestat.keycount, 4);

		pq_sendint(&buf, rangestat.valuebytes, 4);
		pq_sendint(&buf, rangestat.valuecount, 4);

		pq_sendint(&buf, rangestat.readSize, 4);
		pq_sendint(&buf, rangestat.writeSize, 4);
		ssm_statistics->range_statistics[i].writeSize = 0;
		ssm_statistics->range_statistics[i].readSize = 0;
	}
	SegmentSatistics segstat = ssm_statistics->seg_statistics[0];
	ssm_statistics->seg_statistics[0].leaderCount = segstat.leaderCount = leadercount;
	pq_sendint(&buf, segstat.segmentID, 4);
	pq_sendint(&buf, segstat.rangeCount, 4);
	pq_sendint(&buf, segstat.leaderCount, 4);
	pq_sendint64(&buf, segstat.totalSize);
	pq_sendint64(&buf, segstat.userSize);
	pq_sendint64(&buf, segstat.availableSize);

	pq_sendint(&buf, segstat.readSize, 4);
	pq_sendint(&buf, segstat.writeSize, 4);

	ssm_statistics->seg_statistics[0].readSize = 0;
	ssm_statistics->seg_statistics[0].writeSize = 0;

	pq_sendint(&buf, segstat.location.location_level, 4);
	for (int i = 0; i < segstat.location.location_level; i++)
	{
		pq_sendint(&buf, segstat.location.location_len[i], 4);
		pq_sendtext(&buf, segstat.location.address[i], segstat.location.location_len[i]);
	}

	Assert(ssm_rangelist != NULL);
	/* rangedesc section */
	pq_sendint(&buf, ssm_rangelist->range_count, 4);

	for (int i = 0; i < ssm_rangelist->range_count; i++)
	{
		pq_sendint(&buf, ssm_rangelist->rangeBufferSize[i], 4);
		pq_sendtext(&buf, ssm_rangelist->buffer[i], ssm_rangelist->rangeBufferSize[i]);
 	}

	pq_endmessage(&buf);
	EndCommand(messagetype, DestRemote);

	if (MSPLAN_EXEC_LOG)
		ereport(LOG,
				(errmsg("send MS response type: %d", MS_HEART_BEAT)));

	pq_flush();
	/* Refresh seg read and write times */
	ssm_statistics->seg_statistics[0].readSize = 0;
	ssm_statistics->seg_statistics[0].writeSize = 0;

}

static void
SendMSSplitReq(StringInfoData *buf, SplitPreparePlan splitplan, int planid)
{
	Assert(ssm_statistics != NULL);
	/* Send a M-S message */
	pq_beginmessage(buf, 'M');
	Assert(splitplan->header.ms_plan_type == MS_SPLIT_PREPARE);
	pq_sendint(buf, splitplan->header.ms_plan_type, 4);
	pq_sendint(buf, planid, 4);
	/*  */
	pq_sendint(buf, splitplan->split_range->rangeID, 4);
	pq_sendint(buf, splitplan->split_range->version, 4);
	pq_sendint(buf, splitplan->split_range->replica_num, 4);		/* # range statistics len */

	pq_sendint(buf, splitplan->split_range->startkey.len, 4);
	pq_sendtext(buf, (char*)splitplan->split_range->startkey.data, splitplan->split_range->startkey.len);

	pq_sendint(buf, splitplan->split_range->endkey.len, 4);
	pq_sendtext(buf, (char*)splitplan->split_range->endkey.data, splitplan->split_range->endkey.len);

	for (int i = 0; i < splitplan->split_range->replica_num; i++)
	{
		Replica rep = splitplan->split_range->replica[i];

		pq_sendint(buf, rep->replicaID, 4);
		pq_sendint(buf, rep->LeaderReplicaID, 4);
		pq_sendint(buf, rep->segmentID, 4);
		pq_sendint(buf, rep->replica_state, 4);
	}
	pq_sendint(buf, splitplan->split_key.len, 4);
	pq_sendtext(buf, (char*)splitplan->split_key.data, splitplan->split_key.len);

	pq_endmessage(buf);
	if (MSPLAN_EXEC_LOG)
		ereport(LOG,
				(errmsg("send MS response type: %d", splitplan->header.ms_plan_type)));
}

static void
HandleCheckOneSplit(const char *messagetype, SplitPreparePlan spp, int planid)
{
	BeginCommand(messagetype, DestRemote);
	StringInfoData buf;
	initStringInfo(&buf);

	RangeID rangeid = spp->split_range->rangeID;
	RangeSatistics *rangestat = get_rangesatistics_from_list(rangeid);
	SplitPreparePlan splitplan = seg_check_one_range_split(*rangestat);
	if (splitplan != NULL)
	{
		SendMSSplitReq(&buf, splitplan, planid);
		pfree(splitplan);
	}
	else
	{
		BeginCommand(messagetype, DestRemote);
		pq_beginmessage(&buf, 'M');
		pq_sendint(&buf, MS_SPLIT_FAILD, 4);
		pq_sendint(&buf, planid, 4);
		pq_endmessage(&buf);
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("send MS response type: %d", MS_SPLIT_FAILD)));
	}
	EndCommand(messagetype, DestRemote);
	pq_flush();
}

static void
HandleMSSplit(const char *messagetype, SplitPlan sp, int planid)
{
	/* TODO: we need to control the We also need to deal with concurrency conflicts. */
	RangeDesc oldRange = findUpRangeDescByID(sp->old_range->rangeID);
	setRangeDescState(sp->new_range, sp);
	setRangeDescState(sp->old_range, sp);
	if (oldRange.rangeID == sp->old_range->rangeID)
	{
		storeNewRangeDesc(*sp->new_range);
		storeNewRangeDesc(*sp->old_range);
		SegmentID* seglist = getRangeSegID(*sp->new_range);
		bool result = Rangeengine_create_paxos(sp->new_range->rangeID, seglist, sp->new_range->replica_num);
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,(errmsg("PAXOS: range %d paxos start result = %d",
						sp->new_range->rangeID, result)));
	}
	storeNewRangeRoute(*sp->new_range);
	storeNewRangeRoute(*sp->old_range);

	StringInfoData buf;

	initStringInfo(&buf);

	BeginCommand(messagetype, DestRemote);

	pq_beginmessage(&buf, 'M');
	pq_sendint(&buf, MS_SPLIT_SUCCESS, 4);
	pq_sendint(&buf, planid, 4);
	pq_endmessage(&buf);

	EndCommand(messagetype, DestRemote);

	pq_flush();
	if (MSPLAN_EXEC_LOG)
		ereport(LOG,
				(errmsg("send MS response type: %d", MS_SPLIT_COMPLETE)));
}

static void
HandleMSSplitComplete(const char *messagetype, SplitPlan sp, int planid)
{
	/* TODO: we need to control the We also need to deal with concurrency conflicts. */
	RangeDesc oldRange = findUpRangeRoute(sp->old_range->endkey);
	RangeDesc newRange = findUpRangeRoute(sp->new_range->endkey);
	Assert(oldRange.rangeID == sp->old_range->rangeID);
	Assert(newRange.rangeID == sp->new_range->rangeID);
	setRangeDescState(&oldRange, sp);
	setRangeDescState(&newRange, sp);
	RangeDesc old_range_desc = findUpRangeDescByID(sp->old_range->rangeID);
	if (old_range_desc.rangeID == sp->old_range->rangeID)
	{
		storeNewRangeDesc(oldRange);
		storeNewRangeDesc(newRange);
		int old_id = FindRangeStatisticsByRangeID(oldRange.rangeID);
		int new_id = FindRangeStatisticsByRangeID(newRange.rangeID);
		InitRangeStatistics(newRange, new_id);
		InitRangeStatistics(oldRange, old_id);
	}
	storeNewRangeRoute(oldRange);
	storeNewRangeRoute(newRange);

	StringInfoData buf;

	initStringInfo(&buf);

	BeginCommand(messagetype, DestRemote);

	pq_beginmessage(&buf, 'M');
	pq_sendint(&buf, MS_SPLIT_COMPLETE, 4);
	pq_sendint(&buf, planid, 4);
	pq_endmessage(&buf);

	EndCommand(messagetype, DestRemote);

	pq_flush();
	if (MSPLAN_EXEC_LOG)
		ereport(LOG,
				(errmsg("send MS response type: %d", MS_SPLIT_COMPLETE)));
}

bool
ExecRangeMerge(MergePlan sp)
{
	/* TODO: we need to control the We also need to deal with concurrency conflicts. */
	RangeDesc firstRange = findUpRangeRoute(sp->first_range->endkey);
	RangeDesc secondRange = findUpRangeRoute(sp->second_range->endkey);
	/*
	 * If the query is found by the routing result of the merged range, the next range,
	 * indicates that the merge operation has been completed.
	 */
	if (firstRange.rangeID != sp->second_range->rangeID)
	{
		/*
		 * If the rangeid found is different from the two id used for merge,
		 * it is not clear what is going on. Report a mistake first
		 */
		if (firstRange.rangeID != sp->first_range->rangeID)
		{
			ereport(WARNING,
				(errmsg("route find the range id %d is different from two range %d and %d",
					firstRange.rangeID, sp->first_range->rangeID, sp->second_range->rangeID)));
			return false;
		}
		Assert(firstRange.rangeID == sp->first_range->rangeID);
		Assert(secondRange.rangeID == sp->second_range->rangeID);

		mergeRange(&firstRange, &secondRange);

		RangeDesc first_range_desc = findUpRangeDescByID(sp->first_range->rangeID);
		if (first_range_desc.rangeID == sp->first_range->rangeID)
		{
			removeRangeDesc(firstRange);
			storeNewRangeDesc(secondRange);

			Rangeengine_remove_paxos(firstRange.rangeID);

			int new_id = FindRangeStatisticsByRangeID(secondRange.rangeID);
			InitRangeStatistics(secondRange, new_id);
		}
		removeRangeRoute(firstRange);
		storeNewRangeRoute(secondRange);
	}
	return true;
}

static void
HandleMSMerge(const char *messagetype, MergePlan sp, int planid)
{
	bool result = ExecRangeMerge(sp);
	if (!result)
	{
		StringInfoData buf;
		initStringInfo(&buf);
		BeginCommand(messagetype, DestRemote);

		/* TEST: In cases where you have not yet connected to part of the paxos code, it is first assumed that the increase fails */
		pq_beginmessage(&buf, 'M');
		pq_sendint(&buf, MS_MERGE_FAILED, 4);
		pq_sendint(&buf, planid, 4);
		pq_endmessage(&buf);

		EndCommand(messagetype, DestRemote);
		pq_flush();
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("send MS response type: %d", MS_MERGE_FAILED)));
	}
	else
	{
		StringInfoData buf;
		initStringInfo(&buf);
		BeginCommand(messagetype, DestRemote);

		/* TEST: In cases where you have not yet connected to part of the paxos code, it is first assumed that the increase fails */
		pq_beginmessage(&buf, 'M');
		pq_sendint(&buf, MS_MERGE_SUCCESS, 4);
		pq_sendint(&buf, planid, 4);
		pq_endmessage(&buf);

		EndCommand(messagetype, DestRemote);

		pq_flush();
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("send MS response type: %d", MS_MERGE_SUCCESS)));
	}
}

bool
ExecAddReplica(AddReplicaPlan sp)
{
	/* TODO: we need to control the We also need to deal with concurrency conflicts. */
	RangeDesc range = findUpRangeRoute(sp->range->endkey);
	/*
	 * If the rangeid found through the route is not the same as planned. This is a mistake.
	 */
	if (range.rangeID != sp->range->rangeID)
	{
		ereport(WARNING,
			(errmsg("route find the range id %d is different from range %d",
				range.rangeID, sp->range->rangeID)));
		return false;
	}
	bool isleader = false;
	Replica newReplica = findUpReplicaOnOneSeg(range, sp->targetSegID, &isleader);

	if (newReplica != NULL)
		return true;
	RangeDesc range_desc = findUpRangeDescByID(sp->range->rangeID);
	Replica leader = findUpLeader(range);
	Replica replica = CreateNewReplica(range.replica_num, sp->targetSegID, leader->replicaID);
	replica->replica_state = follower_working;
	AddReplicaToRange(&range, replica);
	if (range_desc.rangeID == sp->range->rangeID || GpIdentity.segindex == sp->targetSegID)
	{
		storeNewRangeDesc(range);

		int new_id = FindRangeStatisticsByRangeID(range.rangeID);
		InitRangeStatistics(range, new_id);
	}
	findUpReplicaOnThisSeg(range, &isleader);
	if (isleader)
	{
		Rangeengine_add_replica(range.rangeID, sp->targetSegID);
	}
	if (GpIdentity.segindex == sp->targetSegID)
	{
		SegmentID* seglist = getRangeSegID(range);
		bool result = Rangeengine_create_paxos(range.rangeID, seglist,
								range.replica_num);
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("PAXOS: add range %d replica on seg %d result = %d",
					range.rangeID, sp->targetSegID, result)));
	}
	storeNewRangeRoute(range);
	return true;
}

static void
HanldeMSAddReplica(const char *messagetype, AddReplicaPlan sp, int planid)
{
	bool result = ExecAddReplica(sp);
	if (!result)
	{
		StringInfoData buf;
		initStringInfo(&buf);
		BeginCommand(messagetype, DestRemote);

		pq_beginmessage(&buf, 'M');
		pq_sendint(&buf, MS_ADDREPLICA_FAILED, 4);
		pq_sendint(&buf, planid, 4);
		pq_endmessage(&buf);

		EndCommand(messagetype, DestRemote);
		pq_flush();
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
			(errmsg("send MS response type: %d", MS_ADDREPLICA_FAILED)));
	}
	else
	{
		StringInfoData buf;

		initStringInfo(&buf);
		BeginCommand(messagetype, DestRemote);

		pq_beginmessage(&buf, 'M');
		pq_sendint(&buf, MS_ADDREPLICA_SUCCESS, 4);
		pq_sendint(&buf, planid, 4);
		pq_endmessage(&buf);

		EndCommand(messagetype, DestRemote);

		pq_flush();
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("send MS response type: %d", MS_ADDREPLICA_SUCCESS)));
	}
}

bool
ExecRemoveReplica(RemoveReplicaPlan sp)
{
	/* TODO: we need to control the We also need to deal with concurrency conflicts. */
	RangeDesc range = findUpRangeRoute(sp->range->endkey);
	/*
	 * If the rangeid found through the route is not the same as planned. This is a mistake.
	 */
	if (range.rangeID != sp->range->rangeID)
	{
		ereport(WARNING,
			(errmsg("route find the range id %d is different from range %d",
				range.rangeID, sp->range->rangeID)));
		return false;
	}
	bool isleader = false;
	Replica newReplica = findUpReplicaOnOneSeg(range, sp->targetSegID, &isleader);
	if (newReplica == NULL)
		return true;

	RangeDesc range_desc = findUpRangeDescByID(sp->range->rangeID);

	RemoveReplicaToRange(&range, sp->targetSegID);
	if (range_desc.rangeID == sp->range->rangeID && GpIdentity.segindex != sp->targetSegID)
	{
		storeNewRangeDesc(range);

		int new_id = FindRangeStatisticsByRangeID(range.rangeID);
		InitRangeStatistics(range, new_id);
	}

	findUpReplicaOnThisSeg(range, &isleader);
	if (isleader)
	{
		Rangeengine_remove_replica(range.rangeID, sp->targetSegID);
	}
	if (GpIdentity.segindex == sp->targetSegID)
	{
		removeRangeDesc(range);
		bool result = Rangeengine_remove_paxos(range.rangeID);
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("PAXOS: remove range %d replica on %d result = %d",
					range.rangeID, sp->targetSegID, result)));
	}
	storeNewRangeRoute(range);
	return true;
}

static void
HanldeMSRemoveReplica(const char *messagetype, RemoveReplicaPlan sp, int planid)
{
	bool result = ExecRemoveReplica(sp);
	if (!result)
	{
		StringInfoData buf;
		initStringInfo(&buf);
		BeginCommand(messagetype, DestRemote);

		pq_beginmessage(&buf, 'M');
		pq_sendint(&buf, MS_REMOVEREPLICA_FAILED, 4);
		pq_sendint(&buf, planid, 4);
		pq_endmessage(&buf);

		EndCommand(messagetype, DestRemote);
		pq_flush();
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
			(errmsg("send MS response type: %d", MS_REMOVEREPLICA_FAILED)));
	}
	else
	{
		StringInfoData buf;

		initStringInfo(&buf);
		BeginCommand(messagetype, DestRemote);

		pq_beginmessage(&buf, 'M');
		pq_sendint(&buf, MS_REMOVEREPLICA_SUCCESS, 4);
		pq_sendint(&buf, planid, 4);
		pq_endmessage(&buf);

		EndCommand(messagetype, DestRemote);

		pq_flush();
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("send MS response type: %d", MS_REMOVEREPLICA_SUCCESS)));
	}
}

bool
ExecRebalance(RebalancePlan sp)
{
	/* TODO: we need to control the We also need to deal with concurrency conflicts. */
	RangeDesc range = findUpRangeRoute(sp->rebalance_range->endkey);
	/*
	 * If the rangeid found through the route is not the same as planned. This is a mistake.
	 */
	if (range.rangeID != sp->rebalance_range->rangeID)
	{
		ereport(WARNING,
			(errmsg("route find the range id %d is different from range %d",
				range.rangeID, sp->rebalance_range->rangeID)));
		return false;
	}
	RangeDesc range_desc = findUpRangeDescByID(sp->rebalance_range->rangeID);

	bool isleader = false;
	Replica newReplica = findUpReplicaOnOneSeg(range, sp->targetSegID, &isleader);
	Replica oldReplica = findUpReplicaOnOneSeg(range, sp->sourceSegID, &isleader);

	Replica leader = findUpLeader(range);
	Replica replica = CreateNewReplica(range.replica_num, sp->targetSegID, leader->replicaID);
	replica->replica_state = follower_working;
	if (newReplica == NULL)
		AddReplicaToRange(&range, replica);
	if (oldReplica != NULL)
		RemoveReplicaToRange(&range, sp->sourceSegID);

	if ((range_desc.rangeID == sp->rebalance_range->rangeID || GpIdentity.segindex == sp->targetSegID) &&
			GpIdentity.segindex != sp->sourceSegID)
	{
		storeNewRangeDesc(range);
		int new_id = FindRangeStatisticsByRangeID(range.rangeID);
		InitRangeStatistics(range, new_id);
	}

	findUpReplicaOnThisSeg(range, &isleader);
	if (isleader)
	{
		bool result = Rangeengine_add_replica(range.rangeID, sp->targetSegID);
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("PAXOS: leader add range %d replica on seg %d result = %d",
					range.rangeID, sp->targetSegID, result)));
		result = Rangeengine_remove_replica(range.rangeID, sp->targetSegID);
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("PAXOS: remove range %d replica on %d result = %d",
					range.rangeID, sp->sourceSegID, result)));
	}
	if (GpIdentity.segindex == sp->targetSegID)
	{
		SegmentID* seglist = getRangeSegID(range);
		bool result = Rangeengine_create_paxos(range.rangeID, seglist,
								range.replica_num);
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("PAXOS: add range %d replica on seg %d result = %d",
					range.rangeID, sp->targetSegID, result)));
	}
	if (GpIdentity.segindex == sp->sourceSegID)
	{
		removeRangeDesc(range);
		bool result = Rangeengine_remove_paxos(range.rangeID);
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("PAXOS: remove range %d replica on %d result = %d",
					range.rangeID, sp->sourceSegID, result)));
	}
	storeNewRangeRoute(range);
	return true;
}

static void
HanldeMSRebalance(const char *messagetype, RebalancePlan sp, int planid)
{
	bool result = ExecRebalance(sp);
	if (!result)
	{
		StringInfoData buf;
		initStringInfo(&buf);
		BeginCommand(messagetype, DestRemote);

		/* TEST: In cases where you have not yet connected to part of the paxos code, it is first assumed that the increase fails */
		pq_beginmessage(&buf, 'M');
		pq_sendint(&buf, MS_REMOVEREPLICA_FAILED, 4);
		pq_sendint(&buf, planid, 4);
		pq_endmessage(&buf);

		EndCommand(messagetype, DestRemote);
		pq_flush();
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
			(errmsg("send MS response type: %d", MS_REMOVEREPLICA_FAILED)));
		return;
	}
	else
	{
		StringInfoData buf;

		initStringInfo(&buf);
		BeginCommand(messagetype, DestRemote);

		/* TEST: In cases where you have not yet connected to part of the paxos code, it is first assumed that the increase fails */
		pq_beginmessage(&buf, 'M');
		pq_sendint(&buf, MS_REBALANCE_SUCCESS, 4);
		pq_sendint(&buf, planid, 4);
		pq_endmessage(&buf);

		EndCommand(messagetype, DestRemote);

		pq_flush();
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("send MS response type: %d", MS_REBALANCE_SUCCESS)));
	}
}

bool
ExecTransferLeader(TransferLeaderPlan sp)
{
   	/* TODO: we need to control the We also need to deal with concurrency conflicts. */
	RangeDesc range = findUpRangeRoute(sp->range->endkey);
	/*
	 * If the rangeid found through the route is not the same as planned. This is a mistake.
	 */
	if (range.rangeID != sp->range->rangeID)
	{
		ereport(WARNING,
			(errmsg("route find the range id %d is different from range %d",
				range.rangeID, sp->range->rangeID)));
		return false;
	}
	RangeDesc range_desc = findUpRangeDescByID(sp->range->rangeID);

	Replica leader = findUpLeader(range);
	Replica newLeader = findUpReplicaByReplicaID(range, sp->targetID);
	if (newLeader == NULL)
	{
		ereport(WARNING,
			(errmsg("The current replicaid %d is invalid.",
				sp->targetID)));
		return false;
	}
	if (leader->replicaID == sp->targetID)
	{
		ereport(WARNING,
			(errmsg("The current replica %d is already leader and does not need to be modified.",
				newLeader->replicaID)));
		return true;
	}
	TransferLeader(&range, sp->targetID);

	if (range_desc.rangeID == sp->range->rangeID)
	{
		storeNewRangeDesc(range);
	}

	storeNewRangeRoute(range);
	return true;
}

static void
HanldeMSTransferLeader(const char *messagetype, TransferLeaderPlan sp, int planid)
{
	bool result = ExecTransferLeader(sp);
	if (!result)
	{
		StringInfoData buf;

		initStringInfo(&buf);
		BeginCommand(messagetype, DestRemote);

		/* TEST: In cases where you have not yet connected to part of the paxos code, it is first assumed that the increase fails */
		pq_beginmessage(&buf, 'M');
		pq_sendint(&buf, MS_TRANSFERLEADER_FAILED, 4);
		pq_sendint(&buf, planid, 4);
		pq_endmessage(&buf);

		EndCommand(messagetype, DestRemote);

		pq_flush();
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("send MS response type: %d", MS_TRANSFERLEADER_FAILED)));
	}
	else
	{
		StringInfoData buf;

		initStringInfo(&buf);
		BeginCommand(messagetype, DestRemote);

		/* TEST: In cases where you have not yet connected to part of the paxos code, it is first assumed that the increase fails */
		pq_beginmessage(&buf, 'M');
		pq_sendint(&buf, MS_TRANSFERLEADER_SUCCESS, 4);
		pq_sendint(&buf, planid, 4);
		pq_endmessage(&buf);

		EndCommand(messagetype, DestRemote);

		pq_flush();
		if (MSPLAN_EXEC_LOG)
			ereport(LOG,
				(errmsg("send MS response type: %d", MS_TRANSFERLEADER_SUCCESS)));
	}
}

void
HandleMSMessage(char* query_string)
{
	Assert(strncmp(query_string, M_S_MSG_PLAN,
				strlen(M_S_MSG_PLAN)) == 0);
    char* query = palloc0(strlen(query_string));
    memcpy(query, query_string, strlen(query_string));
	char* temp = query + strlen(M_S_MSG_PLAN);

	RangePlan rp = (RangePlan)temp;
	if (MSPLAN_EXEC_LOG)
		ereport(LOG,(errmsg("received MS query type: %d", rp->ms_plan_type)));
	switch (rp->ms_plan_type)
	{
		case MS_HEART_BEAT:
		{
			HeartBeatPlan hp = (HeartBeatPlan)temp;
			SendMSHeartBeat(M_S_MSG_PLAN, hp, rp->plan_id);
			break;
		}
		case MS_SPLIT_PREPARE:
		{
			SplitPreparePlan spp = TransferBufferToSplitPrepare(temp);
			HandleCheckOneSplit(M_S_MSG_PLAN, spp, rp->plan_id);
			break;
		}
		case MS_SPLIT_PLAN:
		{
			SplitPlan sp = TransferBufferToSplitPlan(temp);
			HandleMSSplit(M_S_MSG_PLAN, sp, rp->plan_id);
			break;
		}
		case MS_SPLIT_COMPLETE:
		{
			SplitPlan sp = TransferBufferToSplitPlan(temp);
			HandleMSSplitComplete(M_S_MSG_PLAN, sp, rp->plan_id);
			break;
		}
		case MS_MERGE_PLAN:
		{
 			MergePlan sp = TransferBufferToMergePlan(temp);
			HandleMSMerge(M_S_MSG_PLAN, sp, rp->plan_id);
			break;
		}
		case MS_ADDREPLICA_PLAN:
		{
			AddReplicaPlan ap = TransferBufferToAddReplicaPlanPlan(temp);
			HanldeMSAddReplica(M_S_MSG_PLAN, ap, rp->plan_id);
			break;
		}
		case MS_REBALANCE_PLAN:
		{
			RebalancePlan ap = TransferBufferToRebalancePlan(temp);
			HanldeMSRebalance(M_S_MSG_PLAN, ap, rp->plan_id);
			break;
		}
		case MS_REMOVEREPLICA_PLAN:
		{
			RemoveReplicaPlan ap = TransferBufferToRemoveReplica(temp);
			HanldeMSRemoveReplica(M_S_MSG_PLAN, ap, rp->plan_id);
			break;
		}
		case MS_TRANSFERLEADER_PLAN:
		{
			TransferLeaderPlan ap = TransferBufferToTransferLeaderPlan(temp);
			HanldeMSTransferLeader(M_S_MSG_PLAN, ap, rp->plan_id);
			break;
		}
		default:
			ereport(WARNING,
					(errmsg("received unknown MS query: %s", query_string)));
			break;
	}
}
