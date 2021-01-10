/*-------------------------------------------------------------------------
 *
 * fts.h
 *	  Interface for fault tolerance service (FTS).
 *
 * Portions Copyright (c) 2005-2010, Greenplum Inc.
 * Portions Copyright (c) 2011, EMC Corp.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *		src/include/postmaster/fts.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FTS_H
#define FTS_H

#include "utils/guc.h"
#include "cdb/cdbutil.h"


/* Queries for FTS messages */
#define	FTS_MSG_PROBE "PROBE"
#define FTS_MSG_SYNCREP_OFF "SYNCREP_OFF"
#define FTS_MSG_PROMOTE "PROMOTE"

/*
 * This is used for constructing string to store the full fts message request
 * string from QD to QE. Format for which is defined using FTS_MSG_FORMAT and
 * first part of it string to define type of fts message like FTS_MSG_PROBE,
 * FTS_MSG_SYNCREP_OFF or FTS_MSG_PROMOTE.
 */
#define FTS_MSG_MAX_LEN 100

/*
 * If altering the fts message format, consider if FTS_MSG_MAX_LEN is enough
 * to store the modified format.
 */
#define FTS_MSG_FORMAT "%s dbid=%d contid=%d"

#define Natts_fts_message_response 5
#define Anum_fts_message_response_is_mirror_up 0
#define Anum_fts_message_response_is_in_sync 1
#define Anum_fts_message_response_is_syncrep_enabled 2
#define Anum_fts_message_response_is_role_mirror 3
#define Anum_fts_message_response_request_retry 4

#define FTS_MESSAGE_RESPONSE_NTUPLES 1

typedef struct FtsResponse
{
	bool IsMirrorUp;
	bool IsInSync;
	bool IsSyncRepEnabled;
	bool IsRoleMirror;
	bool RequestRetry;
} FtsResponse;

extern bool am_ftsprobe;
extern bool am_ftshandler;
extern bool am_mirror;

/*
 * ENUMS
 */

enum probe_result_e
{
	PROBE_DEAD            = 0x00,
	PROBE_ALIVE           = 0x01,
	PROBE_SEGMENT         = 0x02,
	PROBE_FAULT_CRASH     = 0x08,
	PROBE_FAULT_MIRROR    = 0x10,
	PROBE_FAULT_NET       = 0x20,
};

#define PROBE_CHECK_FLAG(result, flag) (((result) & (flag)) == (flag))

#define PROBE_IS_ALIVE(dbInfo) \
	PROBE_CHECK_FLAG(probe_results[(dbInfo)->dbid], PROBE_ALIVE)
#define PROBE_HAS_FAULT_CRASH(dbInfo) \
	PROBE_CHECK_FLAG(probe_results[(dbInfo)->dbid], PROBE_FAULT_CRASH)
#define PROBE_HAS_FAULT_MIRROR(dbInfo) \
	PROBE_CHECK_FLAG(probe_results[(dbInfo)->dbid], PROBE_FAULT_MIRROR)
#define PROBE_HAS_FAULT_NET(dbInfo) \
	PROBE_CHECK_FLAG(probe_results[(dbInfo)->dbid], PROBE_FAULT_NET)

/*
 * primary/mirror state after probing;
 * this is used to transition the segment pair to next state;
 * we ignore the case where both segments are down, as no transition is performed;
 * each segment can be:
 *    1) (U)p or (D)own
 */
enum probe_transition_e
{
	TRANS_D_D = 0x01,	/* not used for state transitions */
	TRANS_D_U = 0x02,
	TRANS_U_D = 0x04,
	TRANS_U_U = 0x08,

	TRANS_SENTINEL
};

#define IS_VALID_TRANSITION(trans) \
	(trans == TRANS_D_D || trans == TRANS_D_U || trans == TRANS_U_D || trans == TRANS_U_U)

/* buffer size for SQL command */
#define SQL_CMD_BUF_SIZE     1024

/*
 * STRUCTURES
 */

/* prototype */
struct CdbComponentDatabaseInfo;

typedef struct
{
	int	dbid;
	int16 segindex;
	uint8 oldStatus;
	uint8 newStatus;
} FtsSegmentStatusChange;

typedef struct
{
	CdbComponentDatabaseInfo *primary;
	CdbComponentDatabaseInfo *mirror;
	uint32 stateNew;
	uint32 statePrimary;
	uint32 stateMirror;
} FtsSegmentPairState;

/*
 * Interface for checking if FTS is active
 */
extern bool FtsIsActive(void);

/*
 * Interface for WALREP specific checking
 */
extern void HandleFtsMessage(const char* query_string);
extern void probeWalRepUpdateConfig(int16 dbid, int16 segindex, char role,
									bool IsSegmentAlive, bool IsInSync);

extern bool FtsProbeStartRule(Datum main_arg);
extern void FtsProbeMain (Datum main_arg);
extern void FtsProbeShmemInit(void);
extern pid_t FtsProbePID(void);

#endif   /* FTS_H */
