/*-------------------------------------------------------------------------
 *
 * cdbsreh.h
 *	  routines for single row error handling
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbsreh.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBSREH_H
#define CDBSREH_H

#include "fmgr.h"
#include "cdb/cdbcopy.h"
#include "utils/memutils.h"


/*
 * The error table is ALWAYS of the following format
 * cmdtime     timestamptz,
 * relname     text,
 * filename    text,
 * linenum     int,
 * bytenum     int,
 * errmsg      text,
 * rawdata     text,
 * rawbytes    bytea
 */
#define NUM_ERRORTABLE_ATTR 8
#define errtable_cmdtime 1
#define errtable_relname 2
#define errtable_filename 3
#define errtable_linenum 4
#define errtable_bytenum 5
#define errtable_errmsg 6
#define errtable_rawdata 7
#define errtable_rawbytes 8

/*
 * All the Single Row Error Handling state is kept here.
 * When an error happens and we are in single row error handling
 * mode this struct is updated and handed to the single row
 * error handling manager (cdbsreh.c).
 */
typedef struct CdbSreh
{
	/* bad row information */
	char	*errmsg;		/* the error message for this bad data row */
	char	*rawdata;		/* the bad data row */
	char	*relname;		/* target relation */
	int64		linenumber;		/* line number of error in original file */
	uint64  processed;      /* num logical input rows processed so far */
	bool	is_server_enc;	/* was bad row converted to server encoding? */

	/* reject limit state */
	int		rejectlimit;	/* SEGMENT REJECT LIMIT value */
	int		rejectcount;	/* how many were rejected so far */
	bool	is_limit_in_rows; /* ROWS = true, PERCENT = false */

	MemoryContext badrowcontext;	/* per-badrow evaluation context */
	char	   filename[MAXPGPATH];		/* "uri [filename]" */

	bool	log_to_file;		/* or log into file? */
	Oid		relid;				/* parent relation id */
} CdbSreh;

extern int gp_initial_bad_row_limit;

extern CdbSreh *makeCdbSreh(int rejectlimit, bool is_limit_in_rows,
							char *filename, char *relname, bool log_to_file);
extern void destroyCdbSreh(CdbSreh *cdbsreh);
extern void HandleSingleRowError(CdbSreh *cdbsreh);
extern void ReportSrehResults(CdbSreh *cdbsreh, uint64 total_rejected);
extern void SendNumRows(int numrejected, int64 numcompleted);
extern void SendNumRowsRejected(int numrejected);
extern bool IsErrorTable(Relation rel);
extern void ErrorIfRejectLimitReached(CdbSreh *cdbsreh);
extern bool ExceedSegmentRejectHardLimit(CdbSreh *cdbsreh);
extern bool IsRejectLimitReached(CdbSreh *cdbsreh);
extern void VerifyRejectLimit(char rejectlimittype, int rejectlimit);

extern bool ErrorLogDelete(Oid databaseId, Oid relationId);
extern Datum gp_read_error_log(PG_FUNCTION_ARGS);
extern Datum gp_truncate_error_log(PG_FUNCTION_ARGS);


#endif /* CDBSREH_H */
