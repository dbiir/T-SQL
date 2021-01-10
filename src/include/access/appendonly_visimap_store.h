/*------------------------------------------------------------------------------
 *
 * appendonly_visimap_store
 *   responsible for storing and finding visimap entries.
 *
 * Copyright (c) 2013-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/access/appendonly_visimap_store.h
 *
 *------------------------------------------------------------------------------
*/
#ifndef APPENDONLY_VISIMAP_STORE_H
#define APPENDONLY_VISIMAP_STORE_H

#include "access/skey.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/appendonly_visimap_entry.h"
#include "storage/lock.h"
#include "utils/rel.h"
#include "utils/tqual.h"

/*
 * The visimap store is responsible for all
 * storage and scanning related activities of the visibility map.
 */
typedef struct AppendOnlyVisimapStore
{
	ScanKey		scanKeys;

	/*
	 * Visibility map relation for the ao table.
	 */
	Relation	visimapRelation;

	/*
	 * Index on the visibility map relation
	 */
	Relation	visimapIndex;

	/*
	 * Snapshot to use for meta data operations. No ownership
	 */
	Snapshot	snapshot;

	/*
	 * Memory context to use for allocations. No ownership of memory context.
	 */
	MemoryContext memoryContext;

} AppendOnlyVisimapStore;

void AppendOnlyVisimapStore_Init(
							AppendOnlyVisimapStore *visiMapStore,
							Oid visimapRelId,
							Oid visimapIdxId,
							LOCKMODE lockmode,
							Snapshot snapshot,
							MemoryContext memoryContext);

void AppendOnlyVisimapStore_Finish(
							  AppendOnlyVisimapStore *visiMapStore,
							  LOCKMODE lockmode);

bool AppendOnlyVisimapStore_Find(
							AppendOnlyVisimapStore *visiMapStore,
							int32 segmentFileNum,
							int64 firstRowNum,
							AppendOnlyVisimapEntry *visiMapEntry);

void AppendOnlyVisimapStore_Store(
							 AppendOnlyVisimapStore *visiMapStore,
							 AppendOnlyVisimapEntry *visiMapEntry);

void AppendOnlyVisimapStore_DeleteSegmentFile(
										 AppendOnlyVisimapStore *visiMapStore,
										 int segno);

int64 AppendOnlyVisimapStore_GetSegmentFileHiddenTupleCount(
													  AppendOnlyVisimapStore *visiMapStore,
													  AppendOnlyVisimapEntry *visiMapEntry,
													  int segno);

int64 AppendOnlyVisimapStore_GetRelationHiddenTupleCount(
												   AppendOnlyVisimapStore *visiMapStore,
												   AppendOnlyVisimapEntry *visiMapEntry);

/* Scan related functions */

IndexScanDesc AppendOnlyVisimapStore_BeginScan(
								 AppendOnlyVisimapStore *visiMapStore,
								 int nkeys,
								 ScanKey keys);

void AppendOnlyVisimapStore_EndScan(
							   AppendOnlyVisimapStore *visiMapStore,
							   IndexScanDesc indexScan);

bool AppendOnlyVisimapStore_GetNext(
							   AppendOnlyVisimapStore *visiMapStore,
							   IndexScanDesc indexScan,
							   ScanDirection scanDirection,
							   AppendOnlyVisimapEntry *visiMapEntry,
							   ItemPointerData *tupleTid);

#endif
