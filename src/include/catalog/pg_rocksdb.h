/*-------------------------------------------------------------------------
 *
 * pg_rocksdb.h
 *	  internal specifications of the rocksdb relation storage.
 *
 * Portions Copyright (c) 2018-Present TDSQL.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/pg_rocksdb.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ROCKSDB_H
#define PG_ROCKSDB_H

#include "catalog/genbki.h"

#define RocksDBRelationId  6666

CATALOG(pg_rocksdb,6666) BKI_WITHOUT_OIDS
{
	Oid		    relid;              /* refer to pg_class.oid */
	NameData	cfname;             /* the name of the column family in which this rocksdb relation is stored */
	text        cfoptions;          /* the options string used to create the column family options for this relation */
} FormData_pg_rocksdb;

FOREIGN_KEY(relid REFERENCES pg_class(oid));

typedef FormData_pg_rocksdb *Form_pg_rocksdb;

#define Natts_pg_rocksdb		3

#define Anum_pg_rocksdb_relid               1
#define Anum_pg_rocksdb_cfname 	            2
#define Anum_pg_rocksdb_cfoptions   	    3

#endif   /* PG_ROCKSDB_H */

