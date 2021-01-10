/*-------------------------------------------------------------------------
 *
 * aomd.c
 *	  This code manages append only relations that reside on magnetic disk.
 *	  It serves the same general purpose as smgr/md.c however we introduce
 *    AO specific file access functions mainly because would like to bypass 
 *	  md.c's and bgwriter's fsyncing. AO relations also use a non constant
 *	  block number to file segment mapping unlike heap relations.
 *
 *	  As of now we still let md.c create and unlink AO relations for us. This
 *	  may need to change if inconsistencies arise.
 *
 * Portions Copyright (c) 2008, Greenplum Inc.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/appendonly/aomd.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <access/aomd.h>

#include "access/aomd.h"
#include "access/appendonlytid.h"
#include "access/appendonlywriter.h"
#include "catalog/catalog.h"
#include "cdb/cdbappendonlystorage.h"
#include "cdb/cdbappendonlyxlog.h"
#include "common/relpath.h"
#include "utils/guc.h"

static bool mdunlink_ao_perFile(const int segno, void *ctx);
static bool copy_append_only_data_perFile(const int segno, void *ctx);

int
AOSegmentFilePathNameLen(Relation rel)
{
	char		*basepath;
	int 		len;
		
	/* Get base path for this relation file */
	basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);

	/*
	 * The basepath will be the RelFileNode number.  Optional part is dot "." plus 
	 * 6 digit segment file number.
	 */
	len = strlen(basepath) + 8;	// Generous.
	
	pfree(basepath);

	return len;
}

/*
 * Formats an Append Only relation file segment file name.
 *
 * The filepathname parameter assume sufficient space.
 */
void
FormatAOSegmentFileName(char *basepath,
						int segno,
						int col,
						int32 *fileSegNo,
						char *filepathname)
{
	int	pseudoSegNo;
	
	Assert(segno >= 0);
	Assert(segno <= AOTupleId_MaxSegmentFileNum);

	if (col < 0)
	{
		/*
		 * Row oriented Append-Only.
		 */
		pseudoSegNo = segno;		
	}
	else
	{
		/*
		 * Column oriented Append-only.
		 */
		pseudoSegNo = (col*AOTupleId_MultiplierSegmentFileNum) + segno;
	}
	
	*fileSegNo = pseudoSegNo;

	if (pseudoSegNo > 0)
	{
		sprintf(filepathname, "%s.%u", basepath, pseudoSegNo);
	}
	else
		strcpy(filepathname, basepath);
}

/*
 * Make an Append Only relation file segment file name.
 *
 * The filepathname parameter assume sufficient space.
 */
void
MakeAOSegmentFileName(Relation rel,
					  int segno,
					  int col,
					  int32 *fileSegNo,
					  char *filepathname)
{
	char	*basepath;
	int32   fileSegNoLocal;
	
	/* Get base path for this relation file */
	basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);

	FormatAOSegmentFileName(basepath, segno, col, &fileSegNoLocal, filepathname);
	
	*fileSegNo = fileSegNoLocal;
	
	pfree(basepath);
}

/*
 * Open an Append Only relation file segment
 *
 * The fd module's PathNameOpenFile() is used to open the file, so the
 * the File* routines can be used to read, write, close, etc, the file.
 */
File
OpenAOSegmentFile(Relation rel, 
				  char *filepathname, 
				  int32	segmentFileNum,
				  int64	logicalEof)
{	
	char	   *dbPath;
	char		path[MAXPGPATH];
	int			fileFlags = O_RDWR | PG_BINARY;
	File		fd;

	dbPath = GetDatabasePath(rel->rd_node.dbNode, rel->rd_node.spcNode);

	if (segmentFileNum == 0)
		snprintf(path, MAXPGPATH, "%s/%u", dbPath, rel->rd_node.relNode);
	else
		snprintf(path, MAXPGPATH, "%s/%u.%u", dbPath, rel->rd_node.relNode, segmentFileNum);

	errno = 0;

	fd = PathNameOpenFile(path, fileFlags, 0600);
	if (fd < 0)
	{
		if (logicalEof == 0 && errno == ENOENT)
			return -1;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open Append-Only segment file \"%s\": %m",
						filepathname)));
	}
	pfree(dbPath);

	return fd;
}


/*
 * Close an Append Only relation file segment
 */
void
CloseAOSegmentFile(File fd)
{
	FileClose(fd);
}

/*
 * Truncate all bytes from offset to end of file.
 */
void
TruncateAOSegmentFile(File fd, Relation rel, int32 segFileNum, int64 offset)
{
	char *relname = RelationGetRelationName(rel);

	Assert(fd > 0);
	Assert(offset >= 0);

	/*
	 * Call the 'fd' module with a 64-bit length since AO segment files
	 * can be multi-gigabyte to the terabytes...
	 */
	if (FileTruncate(fd, offset) != 0)
		ereport(ERROR,
				(errmsg("\"%s\": failed to truncate data after eof: %m",
					    relname)));
	if (RelationNeedsWAL(rel))
		xlog_ao_truncate(rel->rd_node, segFileNum, offset);

	if (file_truncate_hook)
	{
		RelFileNodeBackend rnode;
		rnode.node = rel->rd_node;
		rnode.backend = rel->rd_backend;
		(*file_truncate_hook)(rnode);
	}
}

struct mdunlink_ao_callback_ctx {
	char *segPath;
	char *segpathSuffixPosition;
};

void
mdunlink_ao(const char *path, ForkNumber forkNumber)
{
	/*
	 * Unlogged AO tables have INIT_FORK, in addition to MAIN_FORK.  This
	 * function is called for each fork type.  For INIT_FORK, the "_init"
	 * file is unlinked generically by mdunlinkfork.
	 */
	if (forkNumber == INIT_FORKNUM)
		return;

	Assert(forkNumber == MAIN_FORKNUM);

	int pathSize = strlen(path);
	char *segPath = (char *) palloc(pathSize + 12);
	char *segPathSuffixPosition = segPath + pathSize;
	struct mdunlink_ao_callback_ctx unlinkFiles = { 0 };

	strncpy(segPath, path, pathSize);

	unlinkFiles.segPath = segPath;
	unlinkFiles.segpathSuffixPosition = segPathSuffixPosition;

    ao_foreach_extent_file(mdunlink_ao_perFile, &unlinkFiles);

	pfree(segPath);
}

static bool
mdunlink_ao_perFile(const int segno, void *ctx)
{
	const struct mdunlink_ao_callback_ctx *unlinkFiles = ctx;

	char *segPath = unlinkFiles->segPath;
	char *segPathSuffixPosition = unlinkFiles->segpathSuffixPosition;

	sprintf(segPathSuffixPosition, ".%u", segno);
	if (unlink(segPath) != 0)
	{
		/* ENOENT is expected after the end of the extensions */
		if (errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
							errmsg("could not remove file \"%s\": %m", segPath)));
		else
			return false;
	}

	return true;
}

static void
copy_file(char *srcsegpath, char *dstsegpath,
		  RelFileNode dst, int segfilenum, bool use_wal)
{
	File		srcFile;
	File		dstFile;
	int64		left;
	off_t		offset;
	char       *buffer = palloc(BLCKSZ);
	int dstflags;

	srcFile = PathNameOpenFile(srcsegpath, O_RDONLY | PG_BINARY, 0600);
	if (srcFile < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not open file %s: %m", srcsegpath))));

	dstflags = O_WRONLY | O_EXCL | PG_BINARY;
	/*
	 * .0 relfilenode is expected to exist before calling this
	 * function. Caller calls RelationCreateStorage() which creates the base
	 * file for the relation. Hence use different flag for the same.
	 */
	if (segfilenum)
		dstflags |= O_CREAT;

	dstFile = PathNameOpenFile(dstsegpath, dstflags, 0600);
	if (dstFile < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not create destination file %s: %m", dstsegpath))));

	left = FileSeek(srcFile, 0, SEEK_END);
	if (left < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not seek to end of file %s: %m", srcsegpath))));

	if (FileSeek(srcFile, 0, SEEK_SET) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not seek to beginning of file %s: %m", srcsegpath))));

	offset = 0;
	while(left > 0)
	{
		int			len;

		CHECK_FOR_INTERRUPTS();

		len = Min(left, BLCKSZ);
		if (FileRead(srcFile, buffer, len) != len)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read %d bytes from file \"%s\": %m",
							len, srcsegpath)));

		if (FileWrite(dstFile, buffer, len) != len)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write %d bytes to file \"%s\": %m",
							len, dstsegpath)));

		if (use_wal)
			xlog_ao_insert(dst, segfilenum, offset, buffer, len);

		offset += len;
		left -= len;
	}

	if (FileSync(dstFile) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m",
						dstsegpath)));
	FileClose(srcFile);
	FileClose(dstFile);
	pfree(buffer);
}

struct copy_append_only_data_callback_ctx {
	char *srcPath;
	char *dstPath;
	RelFileNode dst;
	bool useWal;
};

/*
 * Like copy_relation_data(), but for AO tables.
 *
 */
void
copy_append_only_data(RelFileNode src, RelFileNode dst,
        BackendId backendid, char relpersistence)
{
	char *srcPath;
	char *dstPath;
	bool useWal;
	struct copy_append_only_data_callback_ctx copyFiles = { 0 };
	/*
	 * We need to log the copied data in WAL iff WAL archiving/streaming is
	 * enabled AND it's a permanent relation.
	 */
	useWal = XLogIsNeeded() && relpersistence == RELPERSISTENCE_PERMANENT;

	srcPath = relpathbackend(src, backendid, MAIN_FORKNUM);
	dstPath = relpathbackend(dst, backendid, MAIN_FORKNUM);

	copy_file(srcPath, dstPath, dst, 0, useWal);

	copyFiles.srcPath = srcPath;
	copyFiles.dstPath = dstPath;
	copyFiles.dst = dst;
	copyFiles.useWal = useWal;

    ao_foreach_extent_file(copy_append_only_data_perFile, &copyFiles);

	if (file_extend_hook)
	{
		RelFileNodeBackend rnode;
		rnode.node = dst;
		rnode.backend = backendid;
		(*file_extend_hook)(rnode);
	}
}

static bool
copy_append_only_data_perFile(const int segno, void *ctx)
{
	const struct copy_append_only_data_callback_ctx *copyFiles = ctx;

	char srcSegPath[MAXPGPATH + 12];
	char dstSegPath[MAXPGPATH + 12];

	sprintf(srcSegPath, "%s.%u", copyFiles->srcPath, segno);
	if (access(srcSegPath, F_OK) != 0)
	{
		/* ENOENT is expected after the end of the extensions */
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
							errmsg("access failed for file \"%s\": %m", srcSegPath)));
		return false;
	}
	sprintf(dstSegPath, "%s.%u", copyFiles->dstPath, segno);
	copy_file(srcSegPath, dstSegPath, copyFiles->dst, segno, copyFiles->useWal);

	return true;
}

