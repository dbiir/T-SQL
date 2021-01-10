/*-------------------------------------------------------------------------
 *
 * ms.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 *
 * IDENTIFICATION
 *		src/include/tdb/ms.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MS_H
#define MS_H

#include "utils/guc.h"
#include "cdb/cdbutil.h"
#include "tdb/range_plan.h"

/* Queries for MS messages */
#define	M_S_MSG_PLAN "MSPLAN"

#define MSPLAN_EXEC_LOG false

#define Natts_fts_message_response 5
#define Anum_fts_message_response_is_mirror_up 0
#define Anum_fts_message_response_is_in_sync 1
#define Anum_fts_message_response_is_syncrep_enabled 2
#define Anum_fts_message_response_is_role_mirror 3
#define Anum_fts_message_response_request_retry 4

#define FTS_MESSAGE_RESPONSE_NTUPLES 1

typedef struct MSResponse
{
	bool IsMirrorUp;
	bool IsInSync;
	bool IsSyncRepEnabled;
	bool IsRoleMirror;
	bool RequestRetry;
} MSResponse;

extern bool am_mshandler;
extern bool am_msprobe;

/*
 * MS process interface
 */
extern void MSMain (Datum main_arg);
extern bool MSStartRule(Datum main_arg);
extern void MSProbeShmemInit(void);
extern pid_t MSProbePID(void);
/*
 * Interface for WALREP specific checking
 */
extern void HandleMSMessage(char* query_string);
extern bool ExecAddReplica(AddReplicaPlan sp);
extern bool ExecRemoveReplica(RemoveReplicaPlan sp);
extern bool ExecRebalance(RebalancePlan sp);
extern bool ExecRangeMerge(MergePlan sp);
extern bool ExecTransferLeader(TransferLeaderPlan sp);
#endif   /* FTS_H */
