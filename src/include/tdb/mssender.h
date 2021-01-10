/*-------------------------------------------------------------------------
 *
 * mssender.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 *
 * IDENTIFICATION
 *	    src/include/tdb/mssender.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MSSENDER_H
#define MSSENDER_H
#include "access/xlogdefs.h"
#include "tdb/range_plan.h"

typedef struct
{
	int16 dbid;
	bool isPrimaryAlive;
	bool isMirrorAlive;
	bool isInSync;
	bool isSyncRepEnabled;
	bool isRoleMirror;
	bool retryRequested;
	bool isMSHeart;
} ms_result;

/* States used by FTS main loop for probing segments. */
typedef enum
{
    MS_HANDLING,
    MS_SUCCESS,
    MS_FAILED,
} MSMessageState;

#define IsMSMessageStateSuccess(state) (state == MS_SUCCESS)
#define IsMSMessageStateFailed(state) (state == MS_FAILED)

typedef struct
{
	/*
	 * The primary_cdbinfo and mirror_cdbinfo are references to primary and
	 * mirror configuration at the beginning of a probe cycle.  They are used
	 * to start libpq connection to send a FTS message.  Their state/role/mode
	 * is not used and does remain unchanged even when configuration is updated
	 * in the middle of a probe cycle (e.g. mirror marked down in configuration
	 * before sending SYNCREP_OFF message).
	 */
	CdbComponentDatabaseInfo *cdbinfo;
	ms_result result;
	MSMessageState state;
	short poll_events;
	short poll_revents;
	int16 fd_index;               /* index into PollFds array */
	pg_time_t startTime;          /* probe start timestamp */
	pg_time_t retryStartTime;     /* time at which next retry attempt can start */
	int16 probe_errno;            /* saved errno from the latest system call */
	struct pg_conn *conn;         /* libpq connection object */
	int retry_count;
	XLogRecPtr xlogrecptr;
	bool recovery_making_progress;

} ms_node_info;

typedef struct
{
	int num; /* number of primary-mirror pairs FTS wants to probe */
	ms_node_info *perNodeInfos;
	List *rangeplanlist;
} ms_context;

extern bool MSWalRepMessageSegments(CdbComponentDatabases *context);

#endif
