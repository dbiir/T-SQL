/*-------------------------------------------------------------------------
 *
 * tqual.h
 *	  POSTGRES "time qualification" definitions, ie, tuple visibility rules.
 *
 *	  Should be moved/renamed...    - vadim 07/28/98
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/tqual.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TQUAL_H
#define TQUAL_H

#include "utils/rel.h"	/* Relation */
#include "utils/snapshot.h"


/* Static variables representing various special snapshot semantics */
extern PGDLLIMPORT SnapshotData SnapshotSelfData;
extern PGDLLIMPORT SnapshotData SnapshotAnyData;
extern PGDLLIMPORT SnapshotData SnapshotToastData;
extern PGDLLIMPORT SnapshotData CatalogSnapshotData;

#define SnapshotSelf		(&SnapshotSelfData)
#define SnapshotAny			(&SnapshotAnyData)
#define SnapshotToast		(&SnapshotToastData)

/*
 * We don't provide a static SnapshotDirty variable because it would be
 * non-reentrant.  Instead, users of that snapshot type should declare a
 * local variable of type SnapshotData, and initialize it with this macro.
 */
#define InitDirtySnapshot(snapshotdata)  \
	((snapshotdata).satisfies = HeapTupleSatisfiesDirty)

/* This macro encodes the knowledge of which snapshots are MVCC-safe */
#define IsMVCCSnapshot(snapshot)  \
	((snapshot)->satisfies == HeapTupleSatisfiesMVCC || \
	 (snapshot)->satisfies == HeapTupleSatisfiesHistoricMVCC)

/*
 * HeapTupleSatisfiesVisibility
 *		True iff heap tuple satisfies a time qual.
 *
 * Notes:
 *	Assumes heap tuple is valid.
 *	Beware of multiple evaluations of snapshot argument.
 *	Hint bits in the HeapTuple's t_infomask may be updated as a side effect;
 *	if so, the indicated buffer is marked dirty.
 *
 *   GP: The added relation parameter helps us decide if we are going to set tuple hint
 *   bits.  If it is null, we ignore the gp_disable_tuple_hints GUC.
 */
#define HeapTupleSatisfiesVisibility(rel, tuple, snapshot, buffer, sm)	\
	((*(snapshot)->satisfies) (rel, tuple, snapshot, buffer, sm))

/* Result codes for HeapTupleSatisfiesVacuum */
typedef enum
{
	HEAPTUPLE_DEAD,				/* tuple is dead and deletable */
	HEAPTUPLE_LIVE,				/* tuple is live (committed, no deleter) */
	HEAPTUPLE_RECENTLY_DEAD,	/* tuple is dead, but not deletable yet */
	HEAPTUPLE_INSERT_IN_PROGRESS,		/* inserting xact is still in progress */
	HEAPTUPLE_DELETE_IN_PROGRESS	/* deleting xact is still in progress */
} HTSV_Result;

/* These are the "satisfies" test routines for the various snapshot types */
extern bool HeapTupleSatisfiesMVCC(Relation relation, HeapTuple htup,
					   Snapshot snapshot, Buffer buffer, SessionMessage sm);
extern bool HeapTupleSatisfiesSelf(Relation relation, HeapTuple htup,
					   Snapshot snapshot, Buffer buffer, SessionMessage sm);
extern bool HeapTupleSatisfiesAny(Relation relation, HeapTuple htup,
					  Snapshot snapshot, Buffer buffer, SessionMessage sm);
extern bool HeapTupleSatisfiesToast(Relation relation, HeapTuple htup,
						Snapshot snapshot, Buffer buffer, SessionMessage sm);
extern bool HeapTupleSatisfiesDirty(Relation relation, HeapTuple htup,
						Snapshot snapshot, Buffer buffer, SessionMessage sm);
extern bool HeapTupleSatisfiesHistoricMVCC(Relation relation, HeapTuple htup,
							   Snapshot snapshot, Buffer buffer, SessionMessage sm);

/* Special "satisfies" routines with different APIs */
extern HTSU_Result HeapTupleSatisfiesUpdate(Relation relation, HeapTuple htup,
						 CommandId curcid, Buffer buffer, SessionMessage sm);
extern HTSV_Result HeapTupleSatisfiesVacuum(Relation relation, HeapTuple htup,
						 TransactionId OldestXmin, Buffer buffer, SessionMessage sm);
extern bool HeapTupleIsSurelyDead(HeapTuple htup,
					  TransactionId OldestXmin);
extern bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot,
							  bool distributedSnapshotIgnore, bool *setDistributedSnapshotIgnore);
extern bool XidInMVCCSnapshot_Local(TransactionId xid, Snapshot snapshot);

extern void HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer, Relation rel,
					 uint16 infomask, TransactionId xid);
extern bool HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple);

/*
 * To avoid leaking too much knowledge about reorderbuffer implementation
 * details this is implemented in reorderbuffer.c not tqual.c.
 */
struct HTAB;
extern bool ResolveCminCmaxDuringDecoding(struct HTAB *tuplecid_data,
							  Snapshot snapshot,
							  HeapTuple htup,
							  Buffer buffer,
							  CommandId *cmin, CommandId *cmax);
#endif   /* TQUAL_H */
