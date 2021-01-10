/*
 * dbsize.c
 *		Database object size functions, and related inquiries
 *
 * Copyright (c) 2002-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/dbsize.c
 *
 */

#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <glob.h>

#include "access/heapam.h"
#include "access/appendonlywriter.h"
#include "access/aocssegfiles.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_appendonly_fn.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "common/relpath.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/relfilenodemap.h"
#include "utils/relmapper.h"
#include "utils/syscache.h"

#include "libpq-fe.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "utils/snapmgr.h"

static int64 calculate_total_relation_size(Relation rel);

/*
 * Helper function to dispatch a size-returning command.
 *
 * Dispatches the given SQL query to segments, and sums up the results.
 * The query is expected to return one int8 value.
 */
int64
get_size_from_segDBs(const char *cmd)
{
	int64		result;
	CdbPgResults cdb_pgresults = {NULL, 0};
	int			i;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	CdbDispatchCommand(cmd, DF_WITH_SNAPSHOT, &cdb_pgresults);

	result = 0;
	for (i = 0; i < cdb_pgresults.numResults; i++)
	{
		Datum		value;
		struct pg_result *pgresult = cdb_pgresults.pg_results[i];

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected result from segment: %d",
							PQresultStatus(pgresult))));
		}
		if (PQntuples(pgresult) != 1 || PQnfields(pgresult) != 1)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected shape of result from segment (%d rows, %d cols)",
							PQntuples(pgresult), PQnfields(pgresult))));
		}
		if (PQgetisnull(pgresult, 0, 0))
			value = 0;
		else
			value = DirectFunctionCall1(int8in,
										CStringGetDatum(PQgetvalue(pgresult, 0, 0)));
		result += value;
	}

	cdbdisp_clearCdbPgResults(&cdb_pgresults);

	return result;
}

/* Return physical size of directory contents, or 0 if dir doesn't exist */
static int64
db_dir_size(const char *path)
{
	int64		dirsize = 0;
	struct dirent *direntry;
	DIR		   *dirdesc;
	char		filename[MAXPGPATH * 2];

	dirdesc = AllocateDir(path);

	if (!dirdesc)
		return 0;

	while ((direntry = ReadDir(dirdesc, path)) != NULL)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(filename, sizeof(filename), "%s/%s", path, direntry->d_name);

		if (stat(filename, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", filename)));
		}
		dirsize += fst.st_size;
	}

	FreeDir(dirdesc);
	return dirsize;
}

/*
 * calculate size of database in all tablespaces
 */
static int64
calculate_database_size(Oid dbOid)
{
	int64		totalsize;
	DIR		   *dirdesc;
	struct dirent *direntry;
	char		dirpath[MAXPGPATH];
	char		pathname[MAXPGPATH + 13 + get_dbid_string_length() + 1 + sizeof(GP_TABLESPACE_VERSION_DIRECTORY)];
	AclResult	aclresult;

	/* User must have connect privilege for target database */
	aclresult = pg_database_aclcheck(dbOid, GetUserId(), ACL_CONNECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_DATABASE,
					   get_database_name(dbOid));

	/* Shared storage in pg_global is not counted */

	/* Include pg_default storage */
	snprintf(pathname, sizeof(pathname), "base/%u", dbOid);
	totalsize = db_dir_size(pathname);

	/* Scan the non-default tablespaces */
	snprintf(dirpath, MAXPGPATH, "pg_tblspc");
	dirdesc = AllocateDir(dirpath);
	if (!dirdesc)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open tablespace directory \"%s\": %m",
						dirpath)));

	while ((direntry = ReadDir(dirdesc, dirpath)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(pathname, sizeof(pathname), "pg_tblspc/%s/%s/%u",
				 direntry->d_name, GP_TABLESPACE_VERSION_DIRECTORY, dbOid);
		totalsize += db_dir_size(pathname);
	}

	FreeDir(dirdesc);

	return totalsize;
}

Datum
pg_database_size_oid(PG_FUNCTION_ARGS)
{
	Oid			dbOid = PG_GETARG_OID(0);
	int64		size;

	size = calculate_database_size(dbOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_database_size(%u)", dbOid);

		size += get_size_from_segDBs(sql);
	}

	if (size == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}

Datum
pg_database_size_name(PG_FUNCTION_ARGS)
{
	Name		dbName = PG_GETARG_NAME(0);
	Oid			dbOid = get_database_oid(NameStr(*dbName), false);
	int64		size;

	size = calculate_database_size(dbOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_database_size(%s)",
					   quote_literal_cstr(NameStr(*dbName)));

		size += get_size_from_segDBs(sql);
	}

	if (size == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}


/*
 * Calculate total size of tablespace. Returns -1 if the tablespace directory
 * cannot be found.
 */
static int64
calculate_tablespace_size(Oid tblspcOid)
{
	char		tblspcPath[MAXPGPATH];
	char		pathname[MAXPGPATH * 2];
	int64		totalsize = 0;
	DIR		   *dirdesc;
	struct dirent *direntry;
	AclResult	aclresult;

	/*
	 * User must have CREATE privilege for target tablespace, either
	 * explicitly granted or implicitly because it is default for current
	 * database.
	 */
	if (tblspcOid != MyDatabaseTableSpace)
	{
		aclresult = pg_tablespace_aclcheck(tblspcOid, GetUserId(), ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
						   get_tablespace_name(tblspcOid));
	}

	if (tblspcOid == DEFAULTTABLESPACE_OID)
		snprintf(tblspcPath, MAXPGPATH, "base");
	else if (tblspcOid == GLOBALTABLESPACE_OID)
		snprintf(tblspcPath, MAXPGPATH, "global");
	else
		snprintf(tblspcPath, MAXPGPATH, "pg_tblspc/%u/%s", tblspcOid,
				 GP_TABLESPACE_VERSION_DIRECTORY);

	dirdesc = AllocateDir(tblspcPath);

	if (!dirdesc)
		return -1;

	while ((direntry = ReadDir(dirdesc, tblspcPath)) != NULL)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(pathname, sizeof(pathname), "%s/%s", tblspcPath, direntry->d_name);

		if (stat(pathname, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", pathname)));
		}

		if (S_ISDIR(fst.st_mode))
			totalsize += db_dir_size(pathname);

		totalsize += fst.st_size;
	}

	FreeDir(dirdesc);

	return totalsize;
}

Datum
pg_tablespace_size_oid(PG_FUNCTION_ARGS)
{
	Oid			tblspcOid = PG_GETARG_OID(0);
	int64		size;

	size = calculate_tablespace_size(tblspcOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_tablespace_size(%u)", tblspcOid);

		size += get_size_from_segDBs(sql);
	}

	if (size < 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}

Datum
pg_tablespace_size_name(PG_FUNCTION_ARGS)
{
	Name		tblspcName = PG_GETARG_NAME(0);
	Oid			tblspcOid = get_tablespace_oid(NameStr(*tblspcName), false);
	int64		size;

	size = calculate_tablespace_size(tblspcOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_tablespace_size(%s)",
					   quote_literal_cstr(NameStr(*tblspcName)));

		size += get_size_from_segDBs(sql);
	}

	if (size < 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}


/*
 * calculate size of (one fork of) a relation
 *
 * Iterator over all files belong to the relation and do stat.
 * The obviously better way is to use glob.  For whatever reason,
 * glob is extremely slow if there are lots of relations in the
 * database.  So we handle all cases, instead. 
 *
 * Note: we can safely apply this to temp tables of other sessions, so there
 * is no check here or at the call sites for that.
 */
static int64
calculate_relation_size(Relation rel, ForkNumber forknum)
{
	int64		totalsize = 0;
	char	   *relationpath;
	char		pathname[MAXPGPATH];
	unsigned int segcount = 0;

	relationpath = relpathbackend(rel->rd_node, rel->rd_backend, forknum);

if (RelationIsHeap(rel))
{
	/* Ordinary relation, including heap and index.
	 * They take form of relationpath, or relationpath.%d
	 * There will be no holes, therefore, we can stop when
	 * we reach the first non-existing file.
	 */
	for (segcount = 0;; segcount++)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (segcount == 0)
			snprintf(pathname, MAXPGPATH, "%s",
					 relationpath);
		else
			snprintf(pathname, MAXPGPATH, "%s.%u",
					 relationpath, segcount);

		if (stat(pathname, &fst) < 0)
		{
			if (errno == ENOENT)
				break;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file %s: %m", pathname)));
		}
		totalsize += fst.st_size;
	}
}
/* AO tables don't have any extra forks. */
else if (forknum == MAIN_FORKNUM)
{
	if (RelationIsAoRows(rel))
	{
		totalsize = GetAOTotalBytes(rel, GetActiveSnapshot());
	}
	else if (RelationIsAoCols(rel))
	{
		totalsize = GetAOCSTotalBytes(rel, GetActiveSnapshot(), true);
	}
}

	/* RELSTORAGE_VIRTUAL has no space usage */
	return totalsize;
}

Datum
pg_relation_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	text	   *forkName = PG_GETARG_TEXT_P(1);
	ForkNumber	forkNumber;
	Relation	rel;
	int64		size = 0;

	/**
	 * This function is peculiar in that it does its own dispatching.
	 * It does not work on entry db since we do not support dispatching
	 * from entry-db currently.
	 */
	if (Gp_role == GP_ROLE_EXECUTE && IS_QUERY_DISPATCHER())
		elog(ERROR, "This query is not currently supported by GPDB.");

	rel = try_relation_open(relOid, AccessShareLock, false);

	/*
	 * Before 9.2, we used to throw an error if the relation didn't exist, but
	 * that makes queries like "SELECT pg_relation_size(oid) FROM pg_class"
	 * less robust, because while we scan pg_class with an MVCC snapshot,
	 * someone else might drop the table. It's better to return NULL for
	 * already-dropped tables than throw an error and abort the whole query.
	 */
	if (rel == NULL)
		PG_RETURN_NULL();

	forkNumber = forkname_to_number(text_to_cstring(forkName));

	if (relOid == 0 || rel->rd_node.relNode == 0)
		size = 0;
	else
		size = calculate_relation_size(rel, forkNumber);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_relation_size(%u, '%s')", relOid,
					   forkNames[forkNumber]);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

/*
 * Calculate total on-disk size of a TOAST relation, including its indexes.
 * Must not be applied to non-TOAST relations.
 */
static int64
calculate_toast_table_size(Oid toastrelid)
{
	int64		size = 0;
	Relation	toastRel;
	ForkNumber	forkNum;
	ListCell   *lc;
	List	   *indexlist;

	toastRel = relation_open(toastrelid, AccessShareLock);

	/* toast heap size, including FSM and VM size */
	for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
		size += calculate_relation_size(toastRel, forkNum);

	/* toast index size, including FSM and VM size */
	indexlist = RelationGetIndexList(toastRel);

	/* Size is calculated using all the indexes available */
	foreach(lc, indexlist)
	{
		Relation	toastIdxRel;

		toastIdxRel = relation_open(lfirst_oid(lc),
									AccessShareLock);
		for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
			size += calculate_relation_size(toastIdxRel, forkNum);

		relation_close(toastIdxRel, AccessShareLock);
	}
	list_free(indexlist);
	relation_close(toastRel, AccessShareLock);

	return size;
}

/*
 * Calculate total on-disk size of a given table,
 * including FSM and VM, plus TOAST table if any.
 * Indexes other than the TOAST table's index are not included.
 * GPDB: Also includes aoseg, aoblkdir, and aovisimap tables
 *
 * Note that this also behaves sanely if applied to an index or toast table;
 * those won't have attached toast tables, but they can have multiple forks.
 */
static int64
calculate_table_size(Relation rel)
{
	int64		size = 0;
	ForkNumber	forkNum;

	if (!RelationIsValid(rel))
		return 0;

	/*
	 * heap size, including FSM and VM
	 */
	if (rel->rd_node.relNode != 0)
	{
		for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
			size += calculate_relation_size(rel, forkNum);
	}

	/*
	 * Size of toast relation
	 */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
		size += calculate_toast_table_size(rel->rd_rel->reltoastrelid);

	if (RelationIsAppendOptimized(rel))
	{
		Relation ao_rel;

		Assert(OidIsValid(rel->rd_appendonly->segrelid));
		ao_rel = try_relation_open(rel->rd_appendonly->segrelid, AccessShareLock, false);
		size += calculate_total_relation_size(ao_rel);
		relation_close(ao_rel, AccessShareLock);

        /* block directory may not exist, post upgrade or new table that never has indexes */
   		if (OidIsValid(rel->rd_appendonly->blkdirrelid))
        {
			ao_rel = try_relation_open(rel->rd_appendonly->blkdirrelid, AccessShareLock, false);
     		size += calculate_total_relation_size(ao_rel);
			relation_close(ao_rel, AccessShareLock);
        }
		if (OidIsValid(rel->rd_appendonly->visimaprelid))
		{
			ao_rel = try_relation_open(rel->rd_appendonly->visimaprelid, AccessShareLock, false);
			size += calculate_total_relation_size(ao_rel);
			relation_close(ao_rel, AccessShareLock);
		}
	}

	return size;
}

/*
 * Calculate total on-disk size of all indexes attached to the given table.
 *
 * Can be applied safely to an index, but you'll just get zero.
 */
static int64
calculate_indexes_size(Relation rel)
{
	int64		size = 0;

	/*
	 * Aggregate all indexes on the given relation
	 */
	if (rel->rd_rel->relhasindex)
	{
		List	   *index_oids = RelationGetIndexList(rel);
		ListCell   *cell;

		foreach(cell, index_oids)
		{
			Oid			idxOid = lfirst_oid(cell);
			Relation	idxRel;
			ForkNumber	forkNum;

			idxRel = try_relation_open(idxOid, AccessShareLock, false);

			if (RelationIsValid(idxRel))
			{
				for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
					size += calculate_relation_size(idxRel, forkNum);

				relation_close(idxRel, AccessShareLock);
			}
		}

		list_free(index_oids);
	}

	return size;
}

Datum
pg_table_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	Relation	rel;
	int64		size;

	rel = try_relation_open(relOid, AccessShareLock, false);

	if (rel == NULL)
		PG_RETURN_NULL();

	size = calculate_table_size(rel);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_table_size(%u)", relOid);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

Datum
pg_indexes_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	Relation	rel;
	int64		size;

	rel = try_relation_open(relOid, AccessShareLock, false);

	if (rel == NULL)
		PG_RETURN_NULL();

	size = calculate_indexes_size(rel);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_indexes_size(%u)", relOid);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

/*
 *	Compute the on-disk size of all files for the relation,
 *	including heap data, index data, toast data, FSM, VM.
 */
static int64
calculate_total_relation_size(Relation rel)
{
	int64		size;

	/*
	 * Aggregate the table size, this includes size of the heap, toast and
	 * toast index with free space and visibility map
	 */
	size = calculate_table_size(rel);

	/*
	 * Add size of all attached indexes as well
	 */
	size += calculate_indexes_size(rel);

	return size;
}

Datum
pg_total_relation_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	Relation	rel;
	int64		size;

	/*
	 * While we scan pg_class with an MVCC snapshot,
	 * someone else might drop the table. It's better to return NULL for
	 * already-dropped tables than throw an error and abort the whole query.
	 */
	if (get_rel_name(relOid) == NULL)
		PG_RETURN_NULL();

	rel = try_relation_open(relOid, AccessShareLock, false);

	if (rel == NULL)
		PG_RETURN_NULL();

	size = calculate_total_relation_size(rel);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_total_relation_size(%u)",
					   relOid);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

/*
 * formatting with size units
 */
Datum
pg_size_pretty(PG_FUNCTION_ARGS)
{
	int64		size = PG_GETARG_INT64(0);
	char		buf[64];
	int64		limit = 10 * 1024;
	int64		limit2 = limit * 2 - 1;

	if (size < limit)
		snprintf(buf, sizeof(buf), INT64_FORMAT " bytes", size);
	else
	{
		size >>= 9;				/* keep one extra bit for rounding */
		if (size < limit2)
			snprintf(buf, sizeof(buf), INT64_FORMAT " kB",
					 (size + 1) / 2);
		else
		{
			size >>= 10;
			if (size < limit2)
				snprintf(buf, sizeof(buf), INT64_FORMAT " MB",
						 (size + 1) / 2);
			else
			{
				size >>= 10;
				if (size < limit2)
					snprintf(buf, sizeof(buf), INT64_FORMAT " GB",
							 (size + 1) / 2);
				else
				{
					size >>= 10;
					snprintf(buf, sizeof(buf), INT64_FORMAT " TB",
							 (size + 1) / 2);
				}
			}
		}
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf));
}

static char *
numeric_to_cstring(Numeric n)
{
	Datum		d = NumericGetDatum(n);

	return DatumGetCString(DirectFunctionCall1(numeric_out, d));
}

static Numeric
int64_to_numeric(int64 v)
{
	Datum		d = Int64GetDatum(v);

	return DatumGetNumeric(DirectFunctionCall1(int8_numeric, d));
}

static bool
numeric_is_less(Numeric a, Numeric b)
{
	Datum		da = NumericGetDatum(a);
	Datum		db = NumericGetDatum(b);

	return DatumGetBool(DirectFunctionCall2(numeric_lt, da, db));
}

static Numeric
numeric_plus_one_over_two(Numeric n)
{
	Datum		d = NumericGetDatum(n);
	Datum		one;
	Datum		two;
	Datum		result;

	one = DirectFunctionCall1(int8_numeric, Int64GetDatum(1));
	two = DirectFunctionCall1(int8_numeric, Int64GetDatum(2));
	result = DirectFunctionCall2(numeric_add, d, one);
	result = DirectFunctionCall2(numeric_div_trunc, result, two);
	return DatumGetNumeric(result);
}

static Numeric
numeric_shift_right(Numeric n, unsigned count)
{
	Datum		d = NumericGetDatum(n);
	Datum		divisor_int64;
	Datum		divisor_numeric;
	Datum		result;

	divisor_int64 = Int64GetDatum((int64) (1LL << count));
	divisor_numeric = DirectFunctionCall1(int8_numeric, divisor_int64);
	result = DirectFunctionCall2(numeric_div_trunc, d, divisor_numeric);
	return DatumGetNumeric(result);
}

Datum
pg_size_pretty_numeric(PG_FUNCTION_ARGS)
{
	Numeric		size = PG_GETARG_NUMERIC(0);
	Numeric		limit,
				limit2;
	char	   *result;

	limit = int64_to_numeric(10 * 1024);
	limit2 = int64_to_numeric(10 * 1024 * 2 - 1);

	if (numeric_is_less(size, limit))
	{
		result = psprintf("%s bytes", numeric_to_cstring(size));
	}
	else
	{
		/* keep one extra bit for rounding */
		/* size >>= 9 */
		size = numeric_shift_right(size, 9);

		if (numeric_is_less(size, limit2))
		{
			/* size = (size + 1) / 2 */
			size = numeric_plus_one_over_two(size);
			result = psprintf("%s kB", numeric_to_cstring(size));
		}
		else
		{
			/* size >>= 10 */
			size = numeric_shift_right(size, 10);
			if (numeric_is_less(size, limit2))
			{
				/* size = (size + 1) / 2 */
				size = numeric_plus_one_over_two(size);
				result = psprintf("%s MB", numeric_to_cstring(size));
			}
			else
			{
				/* size >>= 10 */
				size = numeric_shift_right(size, 10);

				if (numeric_is_less(size, limit2))
				{
					/* size = (size + 1) / 2 */
					size = numeric_plus_one_over_two(size);
					result = psprintf("%s GB", numeric_to_cstring(size));
				}
				else
				{
					/* size >>= 10 */
					size = numeric_shift_right(size, 10);
					/* size = (size + 1) / 2 */
					size = numeric_plus_one_over_two(size);
					result = psprintf("%s TB", numeric_to_cstring(size));
				}
			}
		}
	}

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * Get the filenode of a relation
 *
 * This is expected to be used in queries like
 *		SELECT pg_relation_filenode(oid) FROM pg_class;
 * That leads to a couple of choices.  We work from the pg_class row alone
 * rather than actually opening each relation, for efficiency.  We don't
 * fail if we can't find the relation --- some rows might be visible in
 * the query's MVCC snapshot even though the relations have been dropped.
 * (Note: we could avoid using the catcache, but there's little point
 * because the relation mapper also works "in the now".)  We also don't
 * fail if the relation doesn't have storage.  In all these cases it
 * seems better to quietly return NULL.
 */
Datum
pg_relation_filenode(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Oid			result;
	HeapTuple	tuple;
	Form_pg_class relform;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		PG_RETURN_NULL();
	relform = (Form_pg_class) GETSTRUCT(tuple);

	switch (relform->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
		case RELKIND_INDEX:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
			/* okay, these have storage */
			if (relform->relfilenode)
				result = relform->relfilenode;
			else	/* Consult the relation mapper */
				result = RelationMapOidToFilenode(relid,
												  relform->relisshared);
			break;

		default:
			/* no storage, return NULL */
			result = InvalidOid;
			break;
	}

	ReleaseSysCache(tuple);

	if (!OidIsValid(result))
		PG_RETURN_NULL();

	PG_RETURN_OID(result);
}

/*
 * Get the relation via (reltablespace, relfilenode)
 *
 * This is expected to be used when somebody wants to match an individual file
 * on the filesystem back to its table. That's not trivially possible via
 * pg_class, because that doesn't contain the relfilenodes of shared and nailed
 * tables.
 *
 * We don't fail but return NULL if we cannot find a mapping.
 *
 * InvalidOid can be passed instead of the current database's default
 * tablespace.
 */
Datum
pg_filenode_relation(PG_FUNCTION_ARGS)
{
	Oid			reltablespace = PG_GETARG_OID(0);
	Oid			relfilenode = PG_GETARG_OID(1);
	Oid			heaprel = InvalidOid;

	heaprel = RelidByRelfilenode(reltablespace, relfilenode);

	if (!OidIsValid(heaprel))
		PG_RETURN_NULL();
	else
		PG_RETURN_OID(heaprel);
}

/*
 * Get the pathname (relative to $PGDATA) of a relation
 *
 * See comments for pg_relation_filenode.
 */
Datum
pg_relation_filepath(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	HeapTuple	tuple;
	Form_pg_class relform;
	RelFileNode rnode;
	BackendId	backend;
	char	   *path;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		PG_RETURN_NULL();
	relform = (Form_pg_class) GETSTRUCT(tuple);

	switch (relform->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
		case RELKIND_INDEX:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOVISIMAP:
		case RELKIND_AOBLOCKDIR:
			/* okay, these have storage */

			/* This logic should match RelationInitPhysicalAddr */
			if (relform->reltablespace)
				rnode.spcNode = relform->reltablespace;
			else
				rnode.spcNode = MyDatabaseTableSpace;
			if (rnode.spcNode == GLOBALTABLESPACE_OID)
				rnode.dbNode = InvalidOid;
			else
				rnode.dbNode = MyDatabaseId;
			if (relform->relfilenode)
				rnode.relNode = relform->relfilenode;
			else	/* Consult the relation mapper */
				rnode.relNode = RelationMapOidToFilenode(relid,
													   relform->relisshared);
			break;

		default:
			/* no storage, return NULL */
			rnode.relNode = InvalidOid;
			/* some compilers generate warnings without these next two lines */
			rnode.dbNode = InvalidOid;
			rnode.spcNode = InvalidOid;
			break;
	}

	if (!OidIsValid(rnode.relNode))
	{
		ReleaseSysCache(tuple);
		PG_RETURN_NULL();
	}

	/* Determine owning backend. */
	switch (relform->relpersistence)
	{
		case RELPERSISTENCE_UNLOGGED:
		case RELPERSISTENCE_PERMANENT:
			backend = InvalidBackendId;
			break;
		case RELPERSISTENCE_TEMP:
			if (isTempOrToastNamespace(relform->relnamespace))
				backend = MyBackendId;
			else
			{
				/* Do it the hard way. */
				backend = GetTempNamespaceBackendId(relform->relnamespace);
				Assert(backend != InvalidBackendId);
			}
			break;
		default:
			elog(ERROR, "invalid relpersistence: %c", relform->relpersistence);
			backend = InvalidBackendId; /* placate compiler */
			break;
	}

	ReleaseSysCache(tuple);

	path = relpathbackend(rnode, backend, MAIN_FORKNUM);

	PG_RETURN_TEXT_P(cstring_to_text(path));
}
