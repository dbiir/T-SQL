/*-------------------------------------------------------------------------
 *
 * mssender.c
 *	  Implementation of segment probing interface
 *
 * Portions Copyright (c) 2006-2011, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2019-present Tencent [hongyaozhao], Inc.
 *
 * IDENTIFICATION
 *	    src/backend/ms/mssender.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "libpq-fe.h"
#include "libpq-int.h"
#include "access/xact.h"

#include "cdb/cdbvars.h"

#include "postmaster/postmaster.h"
#include "utils/snapmgr.h"
#include "tdb/range_universal.h"
#include "tdb/ms_plan.h"

#include "cdb/cdbfts.h"
#include "postmaster/fts.h"
#include "tdb/mssender.h"
#include "tdb/ms.h"
#include "tdb/session_processor.h"
#include "tdb/route.h"
#include "tdb/range.h"

static struct pollfd *PollFds;

static int DEFAULT_RETRY_RECEIVE_TIME = 5;
static int DEFAULT_RETRY_SEND_TIME = 30;
#define CHECK_LENGTH_VARIABLE

static bool
isLengthNotNull(ms_context *context)
{
	int length = list_length(context->rangeplanlist);
	if (length > 0)
	{
#ifdef CHECK_LENGTH_VARIABLE
		if (length < 20)
#endif
			return true;
#ifdef CHECK_LENGTH_VARIABLE
		else
			return false;
#endif
	}
	else
		return false;
}

static MSMessageState
nextFailedState(MSMessageState state)
{
	MSMessageState result = MS_FAILED; /* to shut up compiler */
	switch(state)
	{
		case MS_FAILED:
		case MS_HANDLING:
			result = MS_FAILED;
			break;
		case MS_SUCCESS:
			elog(ERROR, "cannot determine next failed state for %d", state);
	}
	return result;
}

static bool
allDone(ms_context *context)
{
	bool result = true;
	for (int i = 0; i < context->num; i++)
	{
		ms_node_info msInfo = context->perNodeInfos[i];
		result = result && (msInfo.state == MS_SUCCESS || msInfo.state == MS_FAILED);
	}
	bool result2 = list_length(context->rangeplanlist) == 0;
	return result || result2;
}

/*
 * Establish async libpq connection to a segment
 */
static bool
msConnectStart(ms_node_info *msInfo)
{
	char conninfo[1024];

	/*
	 * No events should be pending on the connection that hasn't started
	 * yet.
	 */
	Assert(msInfo->poll_revents == 0);

	snprintf(conninfo, 1024, "host=%s port=%d gpconntype=%s",
			 msInfo->cdbinfo->config->hostip, msInfo->cdbinfo->config->port,
			 GPCONN_TYPE_MS);
	msInfo->conn = PQconnectStart(conninfo);

	if (msInfo->conn == NULL)
	{
		elog(ERROR, "MS: cannot create libpq connection object, possibly out"
			 " of memory (content=%d, dbid=%d)",
			 msInfo->cdbinfo->config->segindex,
			 msInfo->cdbinfo->config->dbid);
	}
	if (msInfo->conn->status == CONNECTION_BAD)
	{
		elog(LOG, "MS: cannot establish libpq connection to "
			 "(content=%d, dbid=%d): %s",
			 msInfo->cdbinfo->config->segindex, msInfo->cdbinfo->config->dbid,
			 PQerrorMessage(msInfo->conn));
		return false;
	}

	/*
	 * Connection started, we must wait until the socket becomes ready for
	 * writing before anything can be written on this socket.  Therefore, mark
	 * the connection to be considered for subsequent poll step.
	 */
	msInfo->poll_events |= POLLOUT;
	/*
	 * Start the timer.
	 */
	msInfo->startTime = (pg_time_t) time(NULL);

	return true;
}

static void
checkIfFailedDueToRecoveryInProgress(ms_node_info *msInfo)
{
	if (strstr(PQerrorMessage(msInfo->conn), _(POSTMASTER_IN_RECOVERY_MSG)))
	{
		XLogRecPtr tmpptr;
		char *ptr = strstr(PQerrorMessage(msInfo->conn),
				_(POSTMASTER_IN_RECOVERY_DETAIL_MSG));
		uint32		tmp_xlogid;
		uint32		tmp_xrecoff;

		if ((ptr == NULL) ||
			sscanf(ptr, POSTMASTER_IN_RECOVERY_DETAIL_MSG " %X/%X\n",
			&tmp_xlogid, &tmp_xrecoff) != 2)
		{
#ifdef USE_ASSERT_CHECKING
			elog(ERROR,
#else
			elog(LOG,
#endif
				"invalid in-recovery message %s "
				"(content=%d, dbid=%d) state=%d",
				PQerrorMessage(msInfo->conn),
				msInfo->cdbinfo->config->segindex,
				msInfo->cdbinfo->config->dbid,
				msInfo->state);
			return;
		}
		tmpptr = ((uint64) tmp_xlogid) << 32 | (uint64) tmp_xrecoff;

		/*
		 * If the xlog record returned from the primary is less than or
		 * equal to the xlog record we had saved from the last probe
		 * then we assume that recovery is not making progress. In the
		 * case of rolling panics on the primary the returned xlog
		 * location can be less than the recorded xlog location. In
		 * these cases of rolling panic or recovery hung we want to
		 * mark the primary as down.
		 */
		if (tmpptr <= msInfo->xlogrecptr)
		{
			elog(LOG, "MS: detected segment is in recovery mode and not making progress (content=%d) "
				 "dbid=%d",
				 msInfo->cdbinfo->config->segindex,
				 msInfo->cdbinfo->config->dbid);
		}
		else
		{
			msInfo->recovery_making_progress = true;
			msInfo->xlogrecptr = tmpptr;
			elogif(gp_log_fts >= GPVARS_VERBOSITY_VERBOSE, LOG,
				 "MS: detected segment is in recovery mode replayed (%X/%X) (content=%d) "
				 "dbid=%d",
				 (uint32) (tmpptr >> 32),
				 (uint32) tmpptr,
				 msInfo->cdbinfo->config->segindex,
				 msInfo->cdbinfo->config->dbid);
		}
	}
}

/*
 * Start a libpq connection for each "per segment" object in context.  If the
 * connection is already started for an object, advance libpq state machine for
 * that object by calling PQconnectPoll().  An established libpq connection
 * (authentication complete and ready-for-query received) is identified by: (1)
 * state of the "per segment" object is any of FTS_PROBE_SEGMENT,
 * FTS_SYNCREP_OFF_SEGMENT, FTS_PROMOTE_SEGMENT and (2) PQconnectPoll() returns
 * PGRES_POLLING_OK for the connection.
 *
 * Upon failure, transition that object to a failed state.
 */
static void
msConnect(ms_context *context)
{
	int i;
	for (i = 0; i < context->num; i++)
	{
		ms_node_info *msInfo = &context->perNodeInfos[i];
		elogif(gp_log_fts == GPVARS_VERBOSITY_DEBUG, LOG,
			   "MS: MSConnect (content=%d, dbid=%d) state=%d, "
			   "retry_count=%d, conn->status=%d",
			   msInfo->cdbinfo->config->segindex,
			   msInfo->cdbinfo->config->dbid,
			   msInfo->state, msInfo->retry_count,
			   msInfo->conn ? msInfo->conn->status : -1);
		if (msInfo->conn && PQstatus(msInfo->conn) == CONNECTION_OK)
			continue;
		switch(msInfo->state)
		{
			case MS_HANDLING:
				/*
				 * We always default to false.  If connect fails due to recovery in progress
				 * this variable will be set based on LSN value in error message.
				 */
				msInfo->recovery_making_progress = false;
				if (msInfo->conn == NULL)
				{
					AssertImply(msInfo->retry_count > 0,
								msInfo->retry_count <= gp_fts_probe_retries);
					if (!msConnectStart(msInfo))
						msInfo->state = nextFailedState(msInfo->state);
				}
				else if (msInfo->poll_revents & (POLLOUT | POLLIN))
				{
					switch(PQconnectPoll(msInfo->conn))
					{
						case PGRES_POLLING_OK:
							/*
							 * Response-state is already set and now the
							 * connection is also ready with authentication
							 * completed.  Next step should now be able to send
							 * the appropriate FTS message.
							 */
							elogif(gp_log_fts == GPVARS_VERBOSITY_DEBUG, LOG,
								   "MS: established libpq connection "
								   "(content=%d, dbid=%d) state=%d, "
								   "retry_count=%d, conn->status=%d",
								   msInfo->cdbinfo->config->segindex,
								   msInfo->cdbinfo->config->dbid,
								   msInfo->state, msInfo->retry_count,
								   msInfo->conn->status);
							msInfo->poll_events = POLLOUT;
							break;

						case PGRES_POLLING_READING:
							/*
							 * The connection can now be polled for reading and
							 * if the poll() returns POLLIN in revents, data
							 * has arrived.
							 */
							msInfo->poll_events |= POLLIN;
							break;

						case PGRES_POLLING_WRITING:
							/*
							 * The connection can now be polled for writing and
							 * may be written to, if ready.
							 */
							msInfo->poll_events |= POLLOUT;
							break;

						case PGRES_POLLING_FAILED:
							msInfo->state = nextFailedState(msInfo->state);
							checkIfFailedDueToRecoveryInProgress(msInfo);
							elog(LOG, "MS: cannot establish libpq connection "
								 "(content=%d, dbid=%d): %s, retry_count=%d",
								 msInfo->cdbinfo->config->segindex,
								 msInfo->cdbinfo->config->dbid,
								 PQerrorMessage(msInfo->conn),
								 msInfo->retry_count);
							break;

						default:
							elog(ERROR, "MS: invalid response to PQconnectPoll"
								 " (content=%d, dbid=%d): %s",
								 msInfo->cdbinfo->config->segindex,
								 msInfo->cdbinfo->config->dbid,
								 PQerrorMessage(msInfo->conn));
							break;
					}
				}
				else
					elogif(gp_log_fts >= GPVARS_VERBOSITY_VERBOSE, LOG,
						   "MS: msConnect (content=%d, dbid=%d) state=%d, "
						   "retry_count=%d, conn->status=%d pollfd.revents unset",
						   msInfo->cdbinfo->config->segindex,
						   msInfo->cdbinfo->config->dbid,
						   msInfo->state, msInfo->retry_count,
						   msInfo->conn ? msInfo->conn->status : -1);
				break;
			default:
				break;
		}
	}
}

/*
 * Timeout is said to have occurred if greater than gp_fts_probe_timeout
 * seconds have elapsed since connection start and a response is not received.
 * Segments for which a response is received already are exempted from timeout
 * evaluation.
 */
#if 0
static void
MSCheckTimeout(ms_node_info *msInfo, pg_time_t now)
{
	if (!IsMSMessageStateSuccess(msInfo->state) &&
		(int) (now - msInfo->startTime) > gp_fts_probe_timeout)
	{
		elog(LOG,
			 "MS timeout detected for (content=%d, dbid=%d) "
			 "state=%d, retry_count=%d,",
			 msInfo->cdbinfo->config->segindex,
			 msInfo->cdbinfo->config->dbid, msInfo->state,
			 msInfo->retry_count);
		msInfo->state = nextFailedState(msInfo->state);
	}
}
#endif
static void
msPoll(ms_context *context)
{
	int i;
	int nfds=0;
	int nready;
	for (i = 0; i < context->num; i++)
	{
		ms_node_info *nodeInfo = &context->perNodeInfos[i];
		if (nodeInfo->poll_events & (POLLIN|POLLOUT))
		{
			PollFds[nfds].fd = PQsocket(nodeInfo->conn);
			PollFds[nfds].events = nodeInfo->poll_events;
			PollFds[nfds].revents = 0;
			nodeInfo->fd_index = nfds;
			nfds++;
		}
		else
			nodeInfo->fd_index = -1; /*
									 * This socket is not considered for
									 * polling.
									 */
	}
	if (nfds == 0)
		return;

	nready = poll(PollFds, nfds, 50);
	if (nready < 0)
	{
		if (errno == EINTR)
		{
			elogif(gp_log_fts == GPVARS_VERBOSITY_DEBUG, LOG,
				   "MS: msPoll() interrupted, nfds %d", nfds);
		}
		else
			elog(ERROR, "MS: msPoll() failed: nfds %d, %m", nfds);
	}
	else if (nready == 0)
	{
		elogif(gp_log_fts == GPVARS_VERBOSITY_DEBUG, LOG,
			   "MS: msPoll() timed out, nfds %d", nfds);
	}

	elogif(gp_log_fts == GPVARS_VERBOSITY_DEBUG, LOG,
		   "MS: msPoll() found %d out of %d sockets ready",
		   nready, nfds);

	// pg_time_t now = (pg_time_t) time(NULL);

	/* Record poll() response poll_revents for each "per_segment" object. */
	for (i = 0; i < context->num; i++)
	{
		ms_node_info *nodeInfo = &context->perNodeInfos[i];

		if (nodeInfo->poll_events & (POLLIN|POLLOUT))
		{
			Assert(PollFds[nodeInfo->fd_index].fd == PQsocket(nodeInfo->conn));
			nodeInfo->poll_revents = PollFds[nodeInfo->fd_index].revents;
			/*
			 * Reset poll_events for fds that were found ready.  Assume
			 * that at the most one bit is set in poll_events (POLLIN
			 * or POLLOUT).
			 */
			if (nodeInfo->poll_revents & nodeInfo->poll_events)
			{
				nodeInfo->poll_events = 0;
			}
			else if (nodeInfo->poll_revents & (POLLHUP | POLLERR))
			{
				nodeInfo->state = nextFailedState(nodeInfo->state);
				elog(LOG,
					 "MS poll failed (revents=%d, events=%d) for "
					 "(content=%d, dbid=%d) state=%d, retry_count=%d, "
					 "libpq status=%d, asyncStatus=%d",
					 nodeInfo->poll_revents, nodeInfo->poll_events,
					 nodeInfo->cdbinfo->config->segindex,
					 nodeInfo->cdbinfo->config->dbid, nodeInfo->state,
					 nodeInfo->retry_count, nodeInfo->conn->status,
					 nodeInfo->conn->asyncStatus);
			}
			else if (nodeInfo->poll_revents)
			{
				nodeInfo->state = nextFailedState(nodeInfo->state);
				elog(LOG,
					 "MS unexpected events (revents=%d, events=%d) for "
					 "(content=%d, dbid=%d) state=%d, retry_count=%d, "
					 "libpq status=%d, asyncStatus=%d",
					 nodeInfo->poll_revents, nodeInfo->poll_events,
					 nodeInfo->cdbinfo->config->segindex,
					 nodeInfo->cdbinfo->config->dbid, nodeInfo->state,
					 nodeInfo->retry_count, nodeInfo->conn->status,
					 nodeInfo->conn->asyncStatus);
			}
			/* If poll timed-out above, check timeout */
			//MSCheckTimeout(nodeInfo, now);
		}
	}
}

static void
send_to_one_seg(char* message, size_t length, ms_node_info *msInfo)
{
	if (PQsendNCharQuery(msInfo->conn, message, length))
	{
		/*
		 * Message sent successfully, mark the socket to be polled
		 * for reading so we will be ready to read response when it
		 * arrives.
		 */
		msInfo->poll_events = POLLIN;
		elogif(gp_log_fts >= GPVARS_VERBOSITY_VERBOSE, LOG,
			   "MS sent %s to (content=%d, dbid=%d), retry_count=%d",
			   message, msInfo->cdbinfo->config->segindex,
			   msInfo->cdbinfo->config->dbid, msInfo->retry_count);
	}
	else
	{
		elog(LOG,
			 "MS: failed to send %s to segment (content=%d, "
			 "dbid=%d) state=%d, retry_count=%d, "
			 "conn->asyncStatus=%d %s", message,
			 msInfo->cdbinfo->config->segindex,
			 msInfo->cdbinfo->config->dbid,
			 msInfo->state, msInfo->retry_count,
			 msInfo->conn->asyncStatus,
			 PQerrorMessage(msInfo->conn));
		msInfo->state = nextFailedState(msInfo->state);
	}
}

static void
SendMSPlanMessage(ms_node_info *msInfo, ms_context* context)
{
	int length = list_length(context->rangeplanlist);
	if (!isLengthNotNull(context))
		return;
	for (int i = 0; i < length; i++)
	{
		RangePlan endPlan = (RangePlan)list_nth(context->rangeplanlist, i);
		if (endPlan->send_type[msInfo->cdbinfo->config->segindex] == true)
			continue;
		switch (endPlan->ms_plan_type)
		{
			case MS_HEART_BEAT:
				{
					char *message = palloc0(sizeof(HeartBeatPlanDesc) + strlen(M_S_MSG_PLAN));
					memcpy(message, M_S_MSG_PLAN, strlen(M_S_MSG_PLAN));
					memcpy((message + strlen(M_S_MSG_PLAN)), (HeartBeatPlan)endPlan, sizeof(HeartBeatPlanDesc));
					send_to_one_seg(message, sizeof(HeartBeatPlanDesc) + strlen(M_S_MSG_PLAN), msInfo);
					pfree(message);
					break;
				}
			case MS_SPLIT_PREPARE:
				{
					Size length = 0;
					char *buffer = TransferSplitPrepareToBuffer((SplitPreparePlan)endPlan, &length);
					char *message = palloc0(length + strlen(M_S_MSG_PLAN));
					memcpy(message, M_S_MSG_PLAN, strlen(M_S_MSG_PLAN));
					memcpy((message + strlen(M_S_MSG_PLAN)), buffer, length);

					int segid = (int)((SplitPreparePlan)endPlan)->targetSegID;
					if (segid == msInfo->cdbinfo->config->segindex)
					{
						send_to_one_seg(message, length + strlen(M_S_MSG_PLAN), msInfo);
					}
					else
					{
						ereport(WARNING,
							(errmsg("MS: error seg want to send the split prepare plan, seg id :%d", segid)));
					}
					pfree(buffer);
					pfree(message);
					break;
				}
			case MS_SPLIT_PLAN:
				{
					Size length = 0;
					char *buffer = TransferSplitPlanToBuffer((SplitPlan)endPlan, &length);

					char *message = palloc0(length + strlen(M_S_MSG_PLAN));
					memcpy(message, M_S_MSG_PLAN, strlen(M_S_MSG_PLAN));
					memcpy((message + strlen(M_S_MSG_PLAN)), buffer, length);
					send_to_one_seg(message, length + strlen(M_S_MSG_PLAN), msInfo);
					pfree(buffer);
					pfree(message);
					break;
				}
			case MS_SPLIT_COMPLETE:
				{
					Size length = 0;
					char *buffer = TransferSplitPlanToBuffer((SplitPlan)endPlan, &length);
					char *message = palloc0(length + strlen(M_S_MSG_PLAN));
					memcpy(message, M_S_MSG_PLAN, strlen(M_S_MSG_PLAN));
					memcpy((message + strlen(M_S_MSG_PLAN)), buffer, length);
					send_to_one_seg(message, length + strlen(M_S_MSG_PLAN), msInfo);
					pfree(buffer);
					pfree(message);
					break;
				}
			case MS_MERGE_PLAN:
				{
					Size length = 0;
					char *buffer = TransferMergePlanToBuffer((MergePlan)endPlan, &length);
					char *message = palloc0(length + strlen(M_S_MSG_PLAN));
					memcpy(message, M_S_MSG_PLAN, strlen(M_S_MSG_PLAN));
					memcpy((message + strlen(M_S_MSG_PLAN)), buffer, length);
					send_to_one_seg(message, length + strlen(M_S_MSG_PLAN), msInfo);
					pfree(buffer);
					pfree(message);
					break;
				}
			case MS_ADDREPLICA_PLAN:
				{
					Size length = 0;
					char *buffer = TransferAddReplicaPlanToBuffer((AddReplicaPlan)endPlan, &length);
					char *message = palloc0(length + strlen(M_S_MSG_PLAN));
					memcpy(message, M_S_MSG_PLAN, strlen(M_S_MSG_PLAN));
					memcpy((message + strlen(M_S_MSG_PLAN)), buffer, length);

					send_to_one_seg(message, length + strlen(M_S_MSG_PLAN), msInfo);

					pfree(buffer);
					pfree(message);
					break;
				}
			case MS_REMOVEREPLICA_PLAN:
				{
					Size length = 0;
					char *buffer = TransferRemoveReplicaToBuffer((RemoveReplicaPlan)endPlan, &length);
					char *message = palloc0(length + strlen(M_S_MSG_PLAN));
					memcpy(message, M_S_MSG_PLAN, strlen(M_S_MSG_PLAN));
					memcpy((message + strlen(M_S_MSG_PLAN)), buffer, length);

					send_to_one_seg(message, length + strlen(M_S_MSG_PLAN), msInfo);

					pfree(buffer);
					pfree(message);
					break;
				}
			case MS_REBALANCE_PLAN:
				{
					Size length = 0;
					char *buffer = TransferRebalancePlanToBuffer((RebalancePlan)endPlan, &length);
					char *message = palloc0(length + strlen(M_S_MSG_PLAN));
					memcpy(message, M_S_MSG_PLAN, strlen(M_S_MSG_PLAN));
					memcpy((message + strlen(M_S_MSG_PLAN)), buffer, length);

					send_to_one_seg(message, length + strlen(M_S_MSG_PLAN), msInfo);

					pfree(buffer);
					pfree(message);
					break;
				}
			case MS_TRANSFERLEADER_PLAN:
				{
					Size length = 0;
					char *buffer = TransferTransferLeaderPlanToBuffer((TransferLeaderPlan)endPlan, &length);
					char *message = palloc0(length + strlen(M_S_MSG_PLAN));
					memcpy(message, M_S_MSG_PLAN, strlen(M_S_MSG_PLAN));
					memcpy((message + strlen(M_S_MSG_PLAN)), buffer, length);

					send_to_one_seg(message, length + strlen(M_S_MSG_PLAN), msInfo);

					pfree(buffer);
					pfree(message);
					break;
				}
			default:
				break;
		}
		endPlan->send_type[msInfo->cdbinfo->config->segindex] = true;
		return ;
	}
}
/*
 * Send FTS query
 */
static void
msSend(ms_context *context)
{
	ms_node_info *msInfo;
	int i;
	for (i = 0; i < context->num; i++)
	{
		msInfo = &context->perNodeInfos[i];
		elogif(gp_log_fts == GPVARS_VERBOSITY_DEBUG, LOG,
			   "MS: msSend (content=%d, dbid=%d) state=%d, "
			   "retry_count=%d, conn->asyncStatus=%d",
			   msInfo->cdbinfo->config->segindex,
			   msInfo->cdbinfo->config->dbid,
			   msInfo->state, msInfo->retry_count,
			   msInfo->conn ? msInfo->conn->asyncStatus : -1);
		/*
		* The libpq connection must be ready for accepting query and
		* the socket must be writable.
		*/
		if (PQstatus(msInfo->conn) != CONNECTION_OK ||
			msInfo->conn->asyncStatus != PGASYNC_IDLE ||
			!(msInfo->poll_revents & POLLOUT))
			continue;
		SendMSPlanMessage(msInfo, context);
	}
}


static void
freeRangePlanByPlan(ms_context *context, RangePlan rp)
{
	if (rp != NULL)
	{
		int length = list_length(context->rangeplanlist);
		list_delete(context->rangeplanlist, rp);
		ereport(LOG, (errmsg("ms delete one rangeplan, plan type = %d, id = %d",
						rp->ms_plan_type, rp->plan_id)));
		if (length == 1)
		{
			ereport(LOG, (errmsg("ms rangeplanlist become NIL")));
			context->rangeplanlist = NIL;
		}
	}
	freeRangePlan(rp);
}


static RangePlan
FindPlanByID(ms_context *context, int planid)
{
	int length = list_length(context->rangeplanlist);
	if (isLengthNotNull(context))
		for (int i = 0; i < length; i++)
		{
			RangePlan rp = (RangePlan)list_nth(context->rangeplanlist, i);
			if (rp->plan_id == planid)
				return rp;
		}
	else
	{
		ereport(WARNING,
					(errmsg("ms range find error, rangeplanlist length invalid, %d", length)));
	}
	return NULL;
}

static void
freePlanByID(ms_context *context, int planid)
{
	if (context->rangeplanlist == NIL)
	{
		ereport(WARNING,
					(errmsg("ms rangeplanlist is NIL")));
		return;
	}
	int length = list_length(context->rangeplanlist);
	if (length > 0)
	{
		RangePlan oldrp = FindPlanByID(context, planid);
		freeRangePlanByPlan(context, oldrp);
	}
	if (length == 1)
		context->rangeplanlist = NIL;
}

static bool
checkRangePlanComplete(ms_context *context, RangePlan rp)
{
	bool result = true;
	for (int i = 0; i < context->num; i++)
	{
		result = result && rp->complete_type[i];
	}
	return result && rp->complete_count >= context->num;
}

static void
HandleSingleSegResponse(MsResultNode ms_result_node, ms_node_info *msInfo, ms_context *context)
{
	switch (ms_result_node->ms_type)
	{
		case MS_HEART_BEAT:
			{
				SSM_Statistics *ssm_statistics = (SSM_Statistics*)(ms_result_node->ms_result);
				RangeList *rangelist = (RangeList*)(ms_result_node->rangelist);
				int length;
				msInfo->poll_events = msInfo->poll_revents = 0;
				msInfo->conn->asyncStatus = PGASYNC_IDLE;
				update_stat_from_heart_beat(ssm_statistics, rangelist, context->rangeplanlist, &length);
				RangePlan rp = FindPlanByID(context, ms_result_node->plan_id);
				if (rp != NULL)
				{
					rp->complete_count++;
					rp->complete_type[msInfo->cdbinfo->config->segindex] = true;
				}
				else
				{
					ereport(WARNING,
						(errmsg("ms: cannot find range plan, plan id = %d", ms_result_node->plan_id)));
				}
			}
			break;
		case MS_SPLIT_PREPARE:
			{
				freePlanByID(context, ms_result_node->plan_id);

				SplitPreparePlan ssp = (SplitPreparePlan)(ms_result_node->ms_result);
				SplitPlan sp = TransferSplitPrepareToSplitPlan(ssp);

				context->rangeplanlist = lcons((void*)sp, context->rangeplanlist);
			}
			break;
		case MS_SPLIT_FAILD:
			{
				SplitPlan oldrp = (SplitPlan)FindPlanByID(context, ms_result_node->plan_id);
				freeRangePlanByPlan(context, (RangePlan)oldrp);
			}
			break;
		case MS_SPLIT_SUCCESS:
			{
				SplitPlan oldrp = (SplitPlan)FindPlanByID(context, ms_result_node->plan_id);
				if (oldrp != NULL)
				{
					oldrp->header.complete_count++;
					oldrp->header.complete_type[msInfo->cdbinfo->config->segindex] = true;
				}
				else
				{
					ereport(WARNING,
						(errmsg("ms: cannot find range plan, plan id = %d", ms_result_node->plan_id)));
				}
			}
			break;
		case MS_SPLIT_COMPLETE:
			{
				SplitPlan oldrp = (SplitPlan)FindPlanByID(context, ms_result_node->plan_id);
				if (oldrp != NULL)
				{
					oldrp->header.complete_count++;
					oldrp->header.complete_type[msInfo->cdbinfo->config->segindex] = true;

					msInfo->poll_events = msInfo->poll_revents = 0;
					msInfo->conn->asyncStatus = PGASYNC_IDLE;
				}
				else
				{
					ereport(WARNING,
						(errmsg("ms: cannot find range plan, plan id = %d", ms_result_node->plan_id)));
				}
			}
			break;
		case MS_MERGE_SUCCESS:
		case MS_MERGE_FAILED:
		case MS_ADDREPLICA_SUCCESS:
		case MS_ADDREPLICA_FAILED:
		case MS_REMOVEREPLICA_SUCCESS:
		case MS_REMOVEREPLICA_FAILED:
		case MS_REBALANCE_FAILED:
		case MS_REBALANCE_SUCCESS:
			{
				RangePlan oldrp = FindPlanByID(context, ms_result_node->plan_id);
				if (oldrp != NULL)
				{
					//oldrp->ms_plan_type = MS_COMPLETE;
					oldrp->complete_count++;
					oldrp->complete_type[msInfo->cdbinfo->config->segindex] = true;

					msInfo->poll_events = msInfo->poll_revents = 0;
					msInfo->conn->asyncStatus = PGASYNC_IDLE;
				}
				else
				{
					ereport(WARNING,
						(errmsg("ms: cannot find range plan, plan id = %d", ms_result_node->plan_id)));
				}
			}
			break;
		default:
			break;
	}
}

/*
 * Record FTS handler's response from libpq result into fts_result
 */
static void
HandleSegResponse(ms_node_info *msInfo, ms_context *context, PGresult *result)
{
	if (PQresultStatus(result) == PGRES_COMMAND_OK)
	{
		int length = result->result_num;
		if (length > 0 && result->ms_result_list != NULL)
		{
			for (int i = 0; i < length; i++)
			{
				MsResultNode ms_result_node = result->ms_result_list[i];
				HandleSingleSegResponse(ms_result_node, msInfo, context);
				free(ms_result_node);
			}
		}
	}
	else
	{
		msInfo->state = nextFailedState(msInfo->state);
	}
	result->ms_result_list = NULL;
}

/*
 * Receive segment response
 */
static void
msReceive(ms_context *context)
{
	ms_node_info *msInfo;
	PGresult *result = NULL;
	int i;

	for (i = 0; i < context->num; i++)
	{
		msInfo = &context->perNodeInfos[i];
		elogif(gp_log_fts == GPVARS_VERBOSITY_DEBUG, LOG,
			   "MS: MSReceive (content=%d, dbid=%d) state=%d, "
			   "retry_count=%d, conn->asyncStatus=%d",
			   msInfo->cdbinfo->config->segindex,
			   msInfo->cdbinfo->config->dbid,
			   msInfo->state, msInfo->retry_count,
			   msInfo->conn ? msInfo->conn->asyncStatus : -1);

		/*
		 * The libpq connection must be established and a message must
		 * have arrived on the socket.
		 */
		if (PQstatus(msInfo->conn) != CONNECTION_OK ||
			!(msInfo->poll_revents & POLLIN))
			continue;
		/* Read the response that has arrived. */
		if (!PQconsumeInput(msInfo->conn))
		{
			elog(LOG, "MS: failed to read from (content=%d, dbid=%d)"
				 " state=%d, retry_count=%d, conn->asyncStatus=%d %s",
				 msInfo->cdbinfo->config->segindex,
				 msInfo->cdbinfo->config->dbid,
				 msInfo->state, msInfo->retry_count,
				 msInfo->conn->asyncStatus,
				 PQerrorMessage(msInfo->conn));
			msInfo->state = nextFailedState(msInfo->state);
			break;
		}
		/*
		 * Response parsed, PQgetResult() should not block for I/O now.
		 */

		result = PQgetResult(msInfo->conn);

		if (!result || PQstatus(msInfo->conn) == CONNECTION_BAD)
		{
			elog(LOG, "MS: error getting results from (content=%d, "
				 "dbid=%d) state=%d, retry_count=%d, "
				 "conn->asyncStatus=%d conn->status=%d %s",
				 msInfo->cdbinfo->config->segindex,
				 msInfo->cdbinfo->config->dbid,
				 msInfo->state, msInfo->retry_count,
				 msInfo->conn->asyncStatus,
				 msInfo->conn->status,
				 PQerrorMessage(msInfo->conn));
			msInfo->state = nextFailedState(msInfo->state);
			break;
		}
		/*
		 * Result received and parsed successfully.  Record it so that
		 * subsequent step processes it and transitions to next state.
		 */
		HandleSegResponse(msInfo, context, result);

		PQclear(result);
		result = NULL;
	}
}

/*
 * Process resonses from primary segments:
 * (a) Transition internal state so that segments can be messaged subsequently
 * (e.g. promotion and turning off syncrep).
 * (b) Update gp_segment_configuration catalog table, if needed.
 */
static bool
msprocessResponse(ms_context *context)
{
	bool is_updated = false;
	int length = list_length(context->rangeplanlist);
	if (isLengthNotNull(context))
	{
		for (int i = 0; i < length; i++)
		{
			RangePlan rp = (RangePlan)list_nth(context->rangeplanlist, i);
			switch (rp->ms_plan_type)
			{
				case MS_HEART_BEAT:
					{
						if (checkRangePlanComplete(context, rp))
						{
							freeRangePlanByPlan(context, rp);
							length = list_length(context->rangeplanlist);
							i = -1;
						}
					}
					break;
				case MS_SPLIT_PREPARE:
					break;
				case MS_SPLIT_PLAN:
				case MS_SPLIT_SUCCESS:
					{
						if (checkRangePlanComplete(context, rp))
						{
							MemSet(rp->complete_type, 0, MAXSEGCOUNT);
							MemSet(rp->send_type, 0, MAXSEGCOUNT);
							rp->complete_count = 0;
							rp->ms_plan_type = MS_SPLIT_COMPLETE;
						}
					}
					break;
				case MS_SPLIT_COMPLETE:
					{
						SplitPlan sp = (SplitPlan)rp;
						if (checkRangePlanComplete(context, rp))
						{
							storeNewRangeRoute(*sp->new_range);
							storeNewRangeRoute(*sp->old_range);
							freeRangePlanByPlan(context, rp);
							ereport(WARNING,
								(errmsg("ms range split complete!")));
							length = list_length(context->rangeplanlist);
							i = -1;
						}
					}
					break;
				case MS_MERGE_PLAN:
					{
						if (checkRangePlanComplete(context, rp))
						{
							MergePlan sp = (MergePlan)rp;
							ExecRangeMerge(sp);
							rp->ms_plan_type = MS_COMPLETE;
							goto ms_complete_check;
						}
						break;
					}
				case MS_ADDREPLICA_SUCCESS:
					{
						if (checkRangePlanComplete(context, rp))
						{
							AddReplicaPlan sp = (AddReplicaPlan)rp;
							ExecAddReplica(sp);
							rp->ms_plan_type = MS_COMPLETE;
							goto ms_complete_check;
						}
					}
					break;
				case MS_REBALANCE_SUCCESS:
					{
						if (checkRangePlanComplete(context, rp))
						{
							RebalancePlan sp = (RebalancePlan)rp;
							ExecRebalance(sp);
							rp->ms_plan_type = MS_COMPLETE;
							goto ms_complete_check;
						}
					}
					break;
				case MS_REMOVEREPLICA_SUCCESS:
					{
						if (checkRangePlanComplete(context, rp))
						{
							RemoveReplicaPlan sp = (RemoveReplicaPlan)rp;
							ExecRemoveReplica(sp);
							rp->ms_plan_type = MS_COMPLETE;
							goto ms_complete_check;
						}
					}
					break;
				case MS_TRANSFERLEADER_SUCCESS:
					{
						if (checkRangePlanComplete(context, rp))
						{
							TransferLeaderPlan sp = (TransferLeaderPlan)rp;
							ExecTransferLeader(sp);
							rp->ms_plan_type = MS_COMPLETE;
							goto ms_complete_check;
						}
					}
					break;
				case MS_ADDREPLICA_FAILED:
				case MS_REMOVEREPLICA_FAILED:
				case MS_MERGE_FAILED:
				case MS_TRANSFERLEADER_FAILED:
				case MS_REBALANCE_FAILED:
					{
						if (checkRangePlanComplete(context, rp))
						{
							rp->ms_plan_type = MS_COMPLETE;
						}
					}
ms_complete_check:
				case MS_COMPLETE:
					{
						if (checkRangePlanComplete(context, rp))
						{
							freeRangePlanByPlan(context, rp);
							ereport(WARNING,
								(errmsg("ms range complete!")));
							length = list_length(context->rangeplanlist);
							i = -1;
						}
					}
					break;
				default:
					break;
			}
		}
	}
	length = list_length(context->rangeplanlist);
	for (int response_index = 0;
		 response_index < context->num && FtsIsActive();
		 response_index ++)
	{
		ms_node_info *msInfo = &(context->perNodeInfos[response_index]);
		if (msInfo->conn && PQstatus(msInfo->conn) == CONNECTION_BAD)
		{
			PQfinish(msInfo->conn);
			msInfo->conn = NULL;
			msInfo->poll_events = msInfo->poll_revents = 0;
			msInfo->retry_count = 0;
			msInfo->state = MS_FAILED;
			continue;
		}
		else if (PQstatus(msInfo->conn) != CONNECTION_OK)
		{
			continue;
		}

		bool result = true, all_receive = true, all_send = true;
		int max_retry_time = 0;
		for (int i = 0; i < length; i++)
		{
			RangePlan rp = (RangePlan)list_nth(context->rangeplanlist, i);
			max_retry_time = max_retry_time > rp->retry_time[msInfo->cdbinfo->config->segindex] ?
                        max_retry_time : rp->retry_time[msInfo->cdbinfo->config->segindex];
			all_send = all_send && rp->send_type[msInfo->cdbinfo->config->segindex];
			all_receive = all_receive && rp->complete_type[msInfo->cdbinfo->config->segindex];
			result = result && (rp->ms_plan_type == MS_HEART_BEAT || rp->ms_plan_type == MS_SPLIT_COMPLETE || MS_COMPLETE);
		}
		if (result && all_receive)
		{
			//PQfinish(msInfo->conn);
			//msInfo->conn = NULL;
			msInfo->poll_events = msInfo->poll_revents = 0;
			msInfo->retry_count = 0;
			msInfo->state = MS_SUCCESS;
		}
		else if (all_receive)
		{
			msInfo->poll_events = msInfo->poll_revents = 0;
			msInfo->retry_count = 0;
		}
		else if (!all_send)
		{
			msInfo->poll_events |= POLLOUT;
			if (msInfo->conn != NULL)
				msInfo->conn->asyncStatus = PGASYNC_IDLE;
			msInfo->state = MS_HANDLING;
		}
		else if(all_send && !all_receive)
		{
			if (max_retry_time % DEFAULT_RETRY_RECEIVE_TIME != (DEFAULT_RETRY_RECEIVE_TIME - 1))
			{
				msInfo->poll_events = POLLIN;
				msInfo->state = MS_HANDLING;
			}
			else if(max_retry_time % DEFAULT_RETRY_RECEIVE_TIME == (DEFAULT_RETRY_RECEIVE_TIME - 1))
			{
				msInfo->poll_events |= POLLOUT;
				for (int i = 0; i < length; i++)
				{
					RangePlan rp = (RangePlan)list_nth(context->rangeplanlist, i);
					if (rp->retry_time[msInfo->cdbinfo->config->segindex] % DEFAULT_RETRY_RECEIVE_TIME == (DEFAULT_RETRY_RECEIVE_TIME - 1))
						rp->send_type[msInfo->cdbinfo->config->segindex] = false;
				}
				if (msInfo->conn != NULL)
					msInfo->conn->asyncStatus = PGASYNC_IDLE;
				msInfo->state = MS_HANDLING;
			}
			if (max_retry_time > DEFAULT_RETRY_SEND_TIME)
			{
				/*
				* TODO: Here we can assume that the current node is offline.
				* We should start the recovery process.
				*
				* But now the output fails first.
				*/
				PQfinish(msInfo->conn);
				msInfo->conn = NULL;
				msInfo->poll_events = msInfo->poll_revents = 0;
				msInfo->retry_count = 0;
				msInfo->state = MS_FAILED;
			}
			for (int i = 0; i < length; i++)
			{
				RangePlan rp = (RangePlan)list_nth(context->rangeplanlist, i);
				rp->retry_time[msInfo->cdbinfo->config->segindex] ++;
			}
		}
	}
	if (!isLengthNotNull(context))
	{
		context->rangeplanlist = NIL;
		for (int response_index = 0;
			response_index < context->num && FtsIsActive();
			response_index ++)
		{
			ms_node_info *msInfo = &(context->perNodeInfos[response_index]);
			PQfinish(msInfo->conn);
			msInfo->conn = NULL;
			msInfo->poll_events = msInfo->poll_revents = 0;
			msInfo->retry_count = 0;
			msInfo->state = MS_SUCCESS;
		}
		ereport(LOG,
				(errmsg("ms range plan handle over.")));
	}
	return is_updated;
}

#ifdef USE_ASSERT_CHECKING
static bool
MSIsSegmentAlive(CdbComponentDatabaseInfo *segInfo)
{
	if (SEGMENT_IS_ACTIVE_MIRROR(segInfo) && SEGMENT_IS_ALIVE(segInfo))
		return true;

	if (SEGMENT_IS_ACTIVE_PRIMARY(segInfo))
		return true;

	return false;
}
#endif

/*
 * Initialize context before a probe cycle based on cluster configuration in
 * cdbs.
 */
static void
MSWalRepInitProbeContext(CdbComponentDatabases *cdbs, ms_context *context)
{
	context->num = cdbs->total_segments;
	context->perNodeInfos = (ms_node_info *) palloc0(
		context->num * sizeof(ms_node_info));

	int ms_index = 0;
	int cdb_index = 0;
	HeartBeatPlan rp = palloc0(sizeof(HeartBeatPlanDesc));
	initRangePlanHead((RangePlan)rp, MS_HEART_BEAT);
	rp->segnum = cdbs->total_segments;
    int index = 0;
	for(; cdb_index < cdbs->total_segment_dbs; cdb_index++)
	{
		CdbComponentDatabaseInfo *primary = &(cdbs->segment_db_info[cdb_index]);
		if (!SEGMENT_IS_ACTIVE_PRIMARY(primary))
			continue;

		/* primary in catalog will NEVER be marked down. */
		Assert(MSIsSegmentAlive(primary));
		memcpy(ssm_statistics->ip[index], primary->config->hostip,
											strlen(primary->config->hostip));
		memcpy(rp->ip[index++], primary->config->hostip,
											strlen(primary->config->hostip));
		ms_node_info *msInfo = &(context->perNodeInfos[ms_index]);
		/*
		 * Initialize the response object.  Response from a segment will be
		 * processed only if ftsInfo->state is one of SUCCESS states.  If a
		 * failure is encountered in messaging a segment, its response will not
		 * be processed.
		 */
		msInfo->result.isPrimaryAlive = false;
		msInfo->result.isMirrorAlive = false;
		msInfo->result.isInSync = false;
		msInfo->result.isSyncRepEnabled = false;
		msInfo->result.retryRequested = false;
		msInfo->result.isRoleMirror = false;
		msInfo->result.dbid = primary->config->dbid;
		msInfo->state = MS_HANDLING;
		msInfo->recovery_making_progress = false;
		msInfo->xlogrecptr = InvalidXLogRecPtr;

		msInfo->cdbinfo = primary;
		Assert(ms_index < context->num);
		ms_index ++;
	}

	context->rangeplanlist = NIL;
	context->rangeplanlist = lcons((void*)rp, context->rangeplanlist);
}

static void
InitPollFds(size_t size)
{
	PollFds = (struct pollfd *) palloc0(size * sizeof(struct pollfd));
}

bool
MSWalRepMessageSegments(CdbComponentDatabases *cdbs)
{
	bool is_updated = false;
	ms_context context;

	MSWalRepInitProbeContext(cdbs, &context);
	InitPollFds(cdbs->total_segments);

	while (!allDone(&context) && FtsIsActive())
	{
		msConnect(&context);
		msPoll(&context);
		msSend(&context);
		msReceive(&context);
		is_updated |= msprocessResponse(&context);
	}
	int i;

	for (i = 0; i < context.num; i++)
	{
		if (context.perNodeInfos[i].conn)
		{
			PQfinish(context.perNodeInfos[i].conn);
			context.perNodeInfos[i].conn = NULL;
		}
	}

	pfree(context.perNodeInfos);
	pfree(PollFds);
	return is_updated;
}

/* EOF */
