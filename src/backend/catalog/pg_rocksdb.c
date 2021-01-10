/*-------------------------------------------------------------------------
 *
 * pg_rocksdb.c
 *	  routines to support manipulation of the pg_rocksdb relation
 *
 * Portions Copyright (c) 2018-Present TDSQL.
 *
 *
 * IDENTIFICATION
 *	    src/backend/catalog/pg_rocksdb.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_rocksdb.h"
#include "catalog/pg_rocksdb_fn.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/gp_fastsequence.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "tdb/rocks_engine.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"

/*
 * Adds an entry into the pg_rocksdb catalog table. The entry
 * includes the new relfilenode of the rocksdb relation that
 * was just created and an initial eof and reltuples values of 0
 */
void
InsertRocksDBEntry(Oid relid,
					  const char* relname,
					  const char* cfoptions,
					  const List* identKeys)
{
	Relation	pg_rocksdb_rel;
	HeapTuple	pg_rocksdb_tuple = NULL;
	NameData	cf_name;
	char		cf_options[CFOPTIONSLEN];
	bool	   *nulls;
	Datum	   *values;
	int			natts = 0;

	Assert(relname != NULL);

    /*
     * Open and lock the pg_rocksdb catalog.
     */
	pg_rocksdb_rel = heap_open(RocksDBRelationId, RowExclusiveLock);

	natts = Natts_pg_rocksdb;
	values = palloc0(sizeof(Datum) * natts);
	nulls = palloc0(sizeof(bool) * natts);

	fill_cf_name(relid, relname, &cf_name);

	if (cfoptions)
		strlcpy(cf_options, cfoptions, CFOPTIONSLEN);
	else
		strcpy(cf_options, "");
	values[Anum_pg_rocksdb_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_rocksdb_cfname - 1] = NameGetDatum(&cf_name);
	values[Anum_pg_rocksdb_cfoptions - 1] = CStringGetTextDatum(cf_options);

	/*
	 * form the tuple and insert it
	 */
	pg_rocksdb_tuple = heap_form_tuple(RelationGetDescr(pg_rocksdb_rel), values, nulls);

	/* insert a new tuple */
	simple_heap_insert(pg_rocksdb_rel, pg_rocksdb_tuple);
	CatalogUpdateIndexes(pg_rocksdb_rel, pg_rocksdb_tuple);

	/* TODO: insert the entry into rocksdb system column family */

	/*
     * Close the pg_rocksdb_rel relcache entry without unlocking.
     * We have updated the catalog: consequently the lock must be
	 * held until end of transaction.
     */
    heap_close(pg_rocksdb_rel, NoLock);

	pfree(values);
	pfree(nulls);
}

/*
 * Remove all pg_rocksdb entries that the table we are DROPing
 * refers to.
 */
void
RemoveRocksDBEntry(Oid relid)
{
	Relation	pg_rocksdb_rel;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tuple;

	/*
	 * now remove the pg_rocksdb entry
	 */
	pg_rocksdb_rel = heap_open(RocksDBRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_rocksdb_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pg_rocksdb_rel, RocksDBRelidIndexId, true,
							  NULL, 1, key);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("rocksdb table relid \"%d\" does not exist in "
						"pg_rocksdb", relid)));

	/*
	 * Delete the rocksdb table entry from the catalog (pg_rocksdb).
	 */
	simple_heap_delete(pg_rocksdb_rel, &tuple->t_self);

	/* TODO: delete the entry from rocksdb system column family */

	/* Finish up scan and close rocksdb catalog. */
	systable_endscan(scan);
	heap_close(pg_rocksdb_rel, NoLock);
}
