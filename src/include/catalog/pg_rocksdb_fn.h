/*-------------------------------------------------------------------------
 *
 * pg_rocksdb_fn.h
 *	  Functions related to RocksDB related system catalogs.
 *
 * Portions Copyright (c) 2018-Present TDSQL.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/pg_rocksdb_fn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ROCKSDB_FN_H
#define PG_ROCKSDB_FN_H

#include "catalog/genbki.h"
#include "utils/relcache.h"
#include "utils/tqual.h"

extern void
InsertRocksDBEntry(Oid relid,
					  const char* relname,
					  const char* cfoptions,
					  const List* identKeys);

extern void
RemoveRocksDBEntry(Oid relid);

#endif   /* PG_RocksDB_FN_H */
