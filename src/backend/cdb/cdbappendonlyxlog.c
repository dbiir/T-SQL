/*-------------------------------------------------------------------------
 *
 * cdbappendonlyxlog.c
 *
 * Portions Copyright (c) 2009-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbappendonlyxlog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/file.h>

#include "cdb/cdbappendonlyxlog.h"
#include "storage/fd.h"
#include "catalog/catalog.h"
#include "utils/faultinjector.h"
#include "utils/faultinjector_lists.h"
#include "access/xlogutils.h"

/*
 * Insert an AO XLOG/AOCO record.
 *
 * This is also used with 0 length, to mark creation of a new segfile.
 */
void
xlog_ao_insert(RelFileNode relFileNode, int32 segmentFileNum,
			   int64 offset, void *buffer, int32 bufferLen)
{
	xl_ao_insert	xlaoinsert;
	XLogRecData		rdata[2];

	xlaoinsert.target.node = relFileNode;
	xlaoinsert.target.segment_filenum = segmentFileNum;
	xlaoinsert.target.offset = offset;

	rdata[0].data = (char*) &xlaoinsert;
	rdata[0].len = SizeOfAOInsert;
	rdata[0].buffer = InvalidBuffer;

	if (bufferLen == 0)
		rdata[0].next = NULL;
	else
	{
		rdata[0].next = &(rdata[1]);

		rdata[1].data = (char*) buffer;
		rdata[1].len = bufferLen;
		rdata[1].buffer = InvalidBuffer;
		rdata[1].next = NULL;
	}

	SIMPLE_FAULT_INJECTOR("xlog_ao_insert");
	XLogInsert(RM_APPEND_ONLY_ID, XLOG_APPENDONLY_INSERT, rdata);
}

static void
ao_insert_replay(XLogRecord *record)
{
	char	   *dbPath;
	char		path[MAXPGPATH];
	int			written_len;
	int64		seek_offset;
	File		file;
	int			fileFlags;
	xl_ao_insert *xlrec = (xl_ao_insert *) XLogRecGetData(record);
	char	   *buffer = (char *) xlrec + SizeOfAOInsert;
	uint32		len = record->xl_len - SizeOfAOInsert;

	dbPath = GetDatabasePath(xlrec->target.node.dbNode,
							 xlrec->target.node.spcNode);

	if (xlrec->target.segment_filenum == 0)
		snprintf(path, MAXPGPATH, "%s/%u", dbPath, xlrec->target.node.relNode);
	else
		snprintf(path, MAXPGPATH, "%s/%u.%u", dbPath, xlrec->target.node.relNode, xlrec->target.segment_filenum);
	pfree(dbPath);

	fileFlags = O_RDWR | PG_BINARY;

	/* When writing from the beginning of the file, it might not exist yet. Create it. */
	if (xlrec->target.offset == 0)
		fileFlags |= O_CREAT;
	file = PathNameOpenFile(path, fileFlags, 0600);
	if (file < 0)
	{
		XLogAOSegmentFile(xlrec->target.node, xlrec->target.segment_filenum);
		return;
	}

	seek_offset = FileSeek(file, xlrec->target.offset, SEEK_SET);
	if (seek_offset != xlrec->target.offset)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("seeked to position " INT64_FORMAT " but expected to seek to position " INT64_FORMAT " in file \"%s\": %m",
						seek_offset,
						xlrec->target.offset,
						path)));
	}

	written_len = FileWrite(file, buffer, len);
	if (written_len < 0 || written_len != len)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("failed to write %d bytes in file \"%s\": %m",
						len,
						path)));
	}

	if (FileSync(file) != 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("failed to flush file \"%s\": %m",
						path)));
	}

	FileClose(file);
}

/*
 * AO/CO truncate xlog record insertion.
 */
void xlog_ao_truncate(RelFileNode relFileNode, int32 segmentFileNum, int64 offset)
{
	xl_ao_truncate	xlaotruncate;
	XLogRecData		rdata[1];

	xlaotruncate.target.node = relFileNode;
	xlaotruncate.target.segment_filenum = segmentFileNum;
	xlaotruncate.target.offset = offset;

	rdata[0].data = (char*) &xlaotruncate;
	rdata[0].len = sizeof(xl_ao_truncate);
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = NULL;

	XLogInsert(RM_APPEND_ONLY_ID, XLOG_APPENDONLY_TRUNCATE, rdata);
}

static void
ao_truncate_replay(XLogRecord *record)
{
	char	   *dbPath;
	char		path[MAXPGPATH];
	File		file;

	xl_ao_truncate *xlrec = (xl_ao_truncate*) XLogRecGetData(record);

	dbPath = GetDatabasePath(xlrec->target.node.dbNode,
							 xlrec->target.node.spcNode);

	if (xlrec->target.segment_filenum == 0)
		snprintf(path, MAXPGPATH, "%s/%u", dbPath, xlrec->target.node.relNode);
	else
		snprintf(path, MAXPGPATH, "%s/%u.%u", dbPath, xlrec->target.node.relNode, xlrec->target.segment_filenum);

	file = PathNameOpenFile(path, O_RDWR | PG_BINARY, 0600);
	if (file < 0)
	{
		/*
		 * Primary creates the file first and then writes the xlog record for
		 * the creation for AO tables similar to heap.  Hence, file can get
		 * created on primary without writing xlog record if failure happens
		 * on primary just after creating the file. This creates situation
		 * where VACUUM can generate truncate record based on aoseg entry with
		 * eof 0 and file present on primary. Then during replay mirror may
		 * not have the file, as was never created on mirror. So, avoid adding
		 * the entry to invalid hash table for truncate at offset zero
		 * (EOF=0).  This avoids mirror PANIC, as anyways truncate to zero is
		 * same as file not present.
		 */
		if (xlrec->target.offset != 0)
			XLogAOSegmentFile(xlrec->target.node, xlrec->target.segment_filenum);
		return;
	}

	if (FileTruncate(file, xlrec->target.offset) != 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("failed to truncate file \"%s\" to offset:" INT64_FORMAT " : %m",
						path, xlrec->target.offset)));
	}

	FileClose(file);
}

void
appendonly_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *record)
{
	uint8         xl_info = record->xl_info;
	uint8         info = xl_info & ~XLR_INFO_MASK;

	/*
	 * Perform redo of AO XLOG records only for standby mode. We do
	 * not need to replay AO XLOG records in normal mode because fsync
	 * is performed on file close.
	 */
	if (!IsStandbyMode())
		return;

	switch (info)
	{
		case XLOG_APPENDONLY_INSERT:
			ao_insert_replay(record);
			break;
		case XLOG_APPENDONLY_TRUNCATE:
			ao_truncate_replay(record);
			break;
		default:
			elog(PANIC, "appendonly_redo: unknown code %u", info);
	}
}
