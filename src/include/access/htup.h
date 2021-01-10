/*-------------------------------------------------------------------------
 *
 * htup.h
 *	  POSTGRES heap tuple definitions.
 *
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/htup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HTUP_H
#define HTUP_H

#include "storage/itemptr.h"
#include "storage/relfilenode.h"
#include "access/sysattr.h"

//#include "access/memtup.h"

/* typedefs and forward declarations for structs defined in htup_details.h */

typedef struct HeapTupleHeaderData HeapTupleHeaderData;

typedef HeapTupleHeaderData *HeapTupleHeader;

typedef struct MinimalTupleData MinimalTupleData;

typedef MinimalTupleData *MinimalTuple;


/*
 * HeapTupleData is an in-memory data structure that points to a tuple.
 *
 * There are several ways in which this data structure is used:
 *
 * * Pointer to a tuple in a disk buffer: t_data points directly into the
 *	 buffer (which the code had better be holding a pin on, but this is not
 *	 reflected in HeapTupleData itself).
 *
 * * Pointer to nothing: t_data is NULL.  This is used as a failure indication
 *	 in some functions.
 *
 * * Part of a palloc'd tuple: the HeapTupleData itself and the tuple
 *	 form a single palloc'd chunk.  t_data points to the memory location
 *	 immediately following the HeapTupleData struct (at offset HEAPTUPLESIZE).
 *	 This is the output format of heap_form_tuple and related routines.
 *
 * * Separately allocated tuple: t_data points to a palloc'd chunk that
 *	 is not adjacent to the HeapTupleData.  (This case is deprecated since
 *	 it's difficult to tell apart from case #1.  It should be used only in
 *	 limited contexts where the code knows that case #1 will never apply.)
 *
 * * Separately allocated minimal tuple: t_data points MINIMAL_TUPLE_OFFSET
 *	 bytes before the start of a MinimalTuple.  As with the previous case,
 *	 this can't be told apart from case #1 by inspection; code setting up
 *	 or destroying this representation has to know what it's doing.
 *
 * t_len should always be valid, except in the pointer-to-nothing case.
 * t_self and t_tableOid should be valid if the HeapTupleData points to
 * a disk buffer, or if it represents a copy of a tuple on disk.  They
 * should be explicitly set invalid in manufactured tuples.
 *
 * CDB: t_tableOid deleted.  Instead, use tts_tableOid in TupleTableSlot.
 */
typedef struct HeapTupleData
{
	uint32		t_len;			/* length of *t_data */
	bool		t_iskey;		/* [williamcliu] should always be true in HeapTupleData */
	ItemPointerData t_self;		/* SelfItemPointer */
	HeapTupleHeader t_data;		/* -> tuple header and data */
} HeapTupleData;

/*
 * [williamcliu] 
 */
typedef struct KeyTupleData
{
	uint32		t_len;			/* length of t_key */
	bool		t_iskey;		/* should always be false in KeyTupleData */
	unsigned char	t_key[1];		/* key stored in kv storage */
} KeyTupleData;

typedef HeapTupleData *HeapTuple;

typedef struct SessionMessageData
{
	bool isused;
	int gp_session_id;
	int pid;
	int combocid_map_count;
}SessionMessageData;

typedef SessionMessageData* SessionMessage;

#define HEAPTUPLESIZE	MAXALIGN(sizeof(HeapTupleData))

/*
 * GenericTuple is a pointer that can point to either a HeapTuple or a
 * MemTuple. Use is_memtuple() to check which one it is.
 *
 * GenericTupleData has no definition; this is a fake "supertype".
 */
struct GenericTupleData;
typedef struct GenericTupleData *GenericTuple;


#define TUP_KEY_LEN_MASK 0x7FFFFFFF


/* XXX Hack Hack Hack 
 * heaptuple, or memtuple, cannot be more than 2G, so, if
 * the first bit is ever set, it is really a memtuple
 */
static inline bool is_memtuple(GenericTuple tup)
{
	return ((((HeapTuple) tup)->t_len & 0x80000000) != 0);
}

static inline bool is_heaptuple_splitter(HeapTuple htup)
{
	return ((char *) htup->t_data) != ((char *) htup + HEAPTUPLESIZE);
}
static inline uint32 heaptuple_get_size(HeapTuple htup)
{
	return htup->t_len + HEAPTUPLESIZE;
}

/*
 * Accessor macros to be used with HeapTuple pointers.
 */
#define HeapTupleIsValid(tuple) PointerIsValid(tuple)

/* HeapTupleHeader functions implemented in utils/time/combocid.c */
extern CommandId HeapTupleHeaderGetCmin(HeapTupleHeader tup, SessionMessage sm);
extern CommandId HeapTupleHeaderGetCmax(HeapTupleHeader tup, SessionMessage sm);
extern void HeapTupleHeaderAdjustCmax(HeapTupleHeader tup,
						  CommandId *cmax, bool *iscombo);

/* Prototype for HeapTupleHeader accessors in heapam.c */
extern TransactionId HeapTupleGetUpdateXid(HeapTupleHeader tuple);

#endif   /* HTUP_H */
