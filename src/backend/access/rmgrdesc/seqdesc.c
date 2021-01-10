/*-------------------------------------------------------------------------
 *
 * seqdesc.c
 *	  rmgr descriptor routines for commands/sequence.c
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/seqdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/sequence.h"


void
seq_desc(StringInfo buf, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	char		*rec = XLogRecGetData(record);
	xl_seq_rec *xlrec = (xl_seq_rec *) rec;

	if (info == XLOG_SEQ_LOG)
		appendStringInfoString(buf, "log: ");
	else
	{
		appendStringInfoString(buf, "UNKNOWN");
		return;
	}

	appendStringInfo(buf, "rel %u/%u/%u",
			   xlrec->node.spcNode, xlrec->node.dbNode, xlrec->node.relNode);
}
