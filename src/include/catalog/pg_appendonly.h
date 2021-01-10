/*-------------------------------------------------------------------------
 *
 * pg_appendonly.h
 *	  internal specifications of the appendonly relation storage.
 *
 * Portions Copyright (c) 2008-2010, Greenplum Inc.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/pg_appendonly.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_APPENDONLY_H
#define PG_APPENDONLY_H

#include "catalog/genbki.h"

/*
 * pg_appendonly definition.
 */

#define AppendOnlyRelationId  6105

CATALOG(pg_appendonly,6105) BKI_WITHOUT_OIDS
{
	Oid				relid;				/* relation id */
	int32			blocksize;			/* the max block size of this relation */
	int32			safefswritesize;	/* min write size in bytes to prevent torn-write */
	int16			compresslevel;		/* the (per seg) total number of varblocks */
	bool			checksum;			/* true if checksum is stored with data and checked */
	NameData		compresstype;		/* the compressor used (e.g. zlib) */
    bool            columnstore;        /* true if orientation is column */ 
    Oid             segrelid;           /* OID of aoseg table; 0 if none */
    Oid             blkdirrelid;        /* OID of aoblkdir table; 0 if none */
    Oid             blkdiridxid;        /* if aoblkdir table, OID of aoblkdir index */
	Oid             visimaprelid;		/* OID of the aovisimap table */
	Oid             visimapidxid;		/* OID of aovisimap index */
} FormData_pg_appendonly;

/* GPDB added foreign key definitions for gpcheckcat. */
FOREIGN_KEY(relid REFERENCES pg_class(oid));

/*
 * Size of fixed part of pg_appendonly tuples, not counting var-length fields
 * (there are no var-length fields currentl.)
*/
#define APPENDONLY_TUPLE_SIZE \
	 (offsetof(FormData_pg_appendonly,visimapidxid) + sizeof(Oid))

/* ----------------
*		Form_pg_appendonly corresponds to a pointer to a tuple with
*		the format of pg_appendonly relation.
* ----------------
*/
typedef FormData_pg_appendonly *Form_pg_appendonly;

#define Natts_pg_appendonly					12
#define Anum_pg_appendonly_relid			1
#define Anum_pg_appendonly_blocksize		2
#define Anum_pg_appendonly_safefswritesize	3
#define Anum_pg_appendonly_compresslevel	4
#define Anum_pg_appendonly_checksum			5
#define Anum_pg_appendonly_compresstype		6
#define Anum_pg_appendonly_columnstore      7
#define Anum_pg_appendonly_segrelid         8
#define Anum_pg_appendonly_blkdirrelid      9
#define Anum_pg_appendonly_blkdiridxid      10
#define Anum_pg_appendonly_visimaprelid     11
#define Anum_pg_appendonly_visimapidxid     12


/* No initial contents. */

/*
 * AORelationVersion defines valid values for the version of AppendOnlyEntry.
 * NOTE: When this is updated, AoRelationVersion_GetLatest() must be updated accordingly.
 */
typedef enum AORelationVersion
{
	AORelationVersion_None =  0,
	AORelationVersion_Original =  1,		/* first valid version */
	AORelationVersion_Aligned64bit = 2,		/* version where the fixes for AOBlock and MemTuple
											 * were introduced, see MPP-7251 and MPP-7372. */
	AORelationVersion_PG83 = 3,				/* Same as Aligned64bit, but numerics are stored
											 * in the PostgreSQL 8.3 format. */
	MaxAORelationVersion                    /* must always be last */
} AORelationVersion;

#define AORelationVersion_GetLatest() AORelationVersion_PG83

#define AORelationVersion_IsValid(version) \
	(version > AORelationVersion_None && version < MaxAORelationVersion)

extern bool Debug_appendonly_print_verify_write_block;

static inline void AORelationVersion_CheckValid(int version)
{
	if (!AORelationVersion_IsValid(version))
	{
		ereport(Debug_appendonly_print_verify_write_block?PANIC:ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("append-only table version %d is invalid", version),
				 errprintstack(true)));
	}
}

/*
 * Versions higher than AORelationVersion_Original include the fixes for AOBlock and
 * MemTuple alignment.
 */
#define IsAOBlockAndMemtupleAlignmentFixed(version) \
( \
	AORelationVersion_CheckValid(version), \
	(version > AORelationVersion_Original) \
)

/*
 * Are numerics stored in old, pre-PostgreSQL 8.3 format, and need converting?
 */
#define PG82NumericConversionNeeded(version) \
( \
	AORelationVersion_CheckValid(version), \
	(version < AORelationVersion_PG83) \
)

#endif   /* PG_APPENDONLY_H */
