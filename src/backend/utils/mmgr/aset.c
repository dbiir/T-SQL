/*-------------------------------------------------------------------------
 *
 * aset.c
 *	  Allocation set definitions.
 *
 * AllocSet is our standard implementation of the abstract MemoryContext
 * type.
 *
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/aset.c
 *
 * NOTE:
 *	This is a new (Feb. 05, 1999) implementation of the allocation set
 *	routines. AllocSet...() does not use OrderedSet...() any more.
 *	Instead it manages allocations in a block pool by itself, combining
 *	many small allocations in a few bigger blocks. AllocSetFree() normally
 *	doesn't free() memory really. It just add's the free'd area to some
 *	list for later reuse by AllocSetAlloc(). All memory blocks are free()'d
 *	at once on AllocSetReset(), which happens when the memory context gets
 *	destroyed.
 *				Jan Wieck
 *
 *	Performance improvement from Tom Lane, 8/99: for extremely large request
 *	sizes, we do want to be able to give the memory back to free() as soon
 *	as it is pfree()'d.  Otherwise we risk tying up a lot of memory in
 *	freelist entries that might never be usable.  This is specially needed
 *	when the caller is repeatedly repalloc()'ing a block bigger and bigger;
 *	the previous instances of the block were guaranteed to be wasted until
 *	AllocSetReset() under the old way.
 *
 *	Further improvement 12/00: as the code stood, request sizes in the
 *	midrange between "small" and "large" were handled very inefficiently,
 *	because any sufficiently large free chunk would be used to satisfy a
 *	request, even if it was much larger than necessary.  This led to more
 *	and more wasted space in allocated chunks over time.  To fix, get rid
 *	of the midrange behavior: we now handle only "small" power-of-2-size
 *	chunks as chunks.  Anything "large" is passed off to malloc().  Change
 *	the number of freelists to change the small/large boundary.
 *
 *
 *	About CLOBBER_FREED_MEMORY:
 *
 *	If this symbol is defined, all freed memory is overwritten with 0x7F's.
 *	This is useful for catching places that reference already-freed memory.
 *
 *	About MEMORY_CONTEXT_CHECKING:
 *
 *	Since we usually round request sizes up to the next power of 2, there
 *	is often some unused space immediately after a requested data area.
 *	Thus, if someone makes the common error of writing past what they've
 *	requested, the problem is likely to go unnoticed ... until the day when
 *	there *isn't* any wasted space, perhaps because of different memory
 *	alignment on a new platform, or some other effect.  To catch this sort
 *	of problem, the MEMORY_CONTEXT_CHECKING option stores 0x7E just beyond
 *	the requested space whenever the request is less than the actual chunk
 *	size, and verifies that the byte is undamaged when the chunk is freed.
 *
 *
 *	About USE_VALGRIND and Valgrind client requests:
 *
 *	Valgrind provides "client request" macros that exchange information with
 *	the host Valgrind (if any).  Under !USE_VALGRIND, memdebug.h stubs out
 *	currently-used macros.
 *
 *	When running under Valgrind, we want a NOACCESS memory region both before
 *	and after the allocation.  The chunk header is tempting as the preceding
 *	region, but mcxt.c expects to able to examine the standard chunk header
 *	fields.  Therefore, we use, when available, the requested_size field and
 *	any subsequent padding.  requested_size is made NOACCESS before returning
 *	a chunk pointer to a caller.  However, to reduce client request traffic,
 *	it is kept DEFINED in chunks on the free list.
 *
 *	The rounded-up capacity of the chunk usually acts as a post-allocation
 *	NOACCESS region.  If the request consumes precisely the entire chunk,
 *	there is no such region; another chunk header may immediately follow.  In
 *	that case, Valgrind will not detect access beyond the end of the chunk.
 *
 *	See also the cooperating Valgrind client requests in mcxt.c.
 *
 *-------------------------------------------------------------------------
 */

#include <inttypes.h>
#include "postgres.h"

#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/memaccounting.h"
#include "utils/gp_alloc.h"

#include "miscadmin.h"

#include "utils/memaccounting_private.h"
#include "tdb/storage_processor.h"
#include "tdb/storage_param.h"
/* Define this to detail debug alloc information */
/* #define HAVE_ALLOCINFO */

#ifdef CDB_PALLOC_CALLER_ID
#define CDB_MCXT_WHERE(context) (context)->callerFile, (context)->callerLine
#else
#define CDB_MCXT_WHERE(context) __FILE__, __LINE__
#endif

#if defined(CDB_PALLOC_TAGS) && !defined(CDB_PALLOC_CALLER_ID)
#error "If CDB_PALLOC_TAGS is defined, CDB_PALLOC_CALLER_ID must be defined too"
#endif

/*--------------------
 * Chunk freelist k holds chunks of size 1 << (k + ALLOC_MINBITS),
 * for k = 0 .. ALLOCSET_NUM_FREELISTS-1.
 *
 * Note that all chunks in the freelists have power-of-2 sizes.  This
 * improves recyclability: we may waste some space, but the wasted space
 * should stay pretty constant as requests are made and released.
 *
 * A request too large for the last freelist is handled by allocating a
 * dedicated block from malloc().  The block still has a block header and
 * chunk header, but when the chunk is freed we'll return the whole block
 * to malloc(), not put it on our freelists.
 *
 * CAUTION: ALLOC_MINBITS must be large enough so that
 * 1<<ALLOC_MINBITS is at least MAXALIGN,
 * or we may fail to align the smallest chunks adequately.
 * 8-byte alignment is enough on all currently known machines.
 *
 * With the current parameters, request sizes up to 8K are treated as chunks,
 * larger requests go into dedicated blocks.  Change ALLOCSET_NUM_FREELISTS
 * to adjust the boundary point; and adjust ALLOCSET_SEPARATE_THRESHOLD in
 * memutils.h to agree.  (Note: in contexts with small maxBlockSize, we may
 * set the allocChunkLimit to less than 8K, so as to avoid space wastage.)
 *--------------------
 */

#define ALLOC_MINBITS		3	/* smallest chunk size is 8 bytes */
#define ALLOCSET_NUM_FREELISTS	11
#define ALLOC_CHUNK_LIMIT	(1 << (ALLOCSET_NUM_FREELISTS-1+ALLOC_MINBITS))
/* Size of largest chunk that we use a fixed size for */
#define ALLOC_CHUNK_FRACTION	4
/* We allow chunks to be at most 1/4 of maxBlockSize (less overhead) */

/*--------------------
 * The first block allocated for an allocset has size initBlockSize.
 * Each time we have to allocate another block, we double the block size
 * (if possible, and without exceeding maxBlockSize), so as to reduce
 * the bookkeeping load on malloc().
 *
 * Blocks allocated to hold oversize chunks do not follow this rule, however;
 * they are just however big they need to be to hold that single chunk.
 *--------------------
 */

#define ALLOC_BLOCKHDRSZ	MAXALIGN(sizeof(AllocBlockData))
#define ALLOC_CHUNKHDRSZ	MAXALIGN(sizeof(AllocChunkData))

/* Portion of ALLOC_CHUNKHDRSZ examined outside aset.c. */
#define ALLOC_CHUNK_PUBLIC	\
	(offsetof(AllocChunkData, size) + sizeof(Size))

/* Portion of ALLOC_CHUNKHDRSZ excluding trailing padding. */
#ifdef MEMORY_CONTEXT_CHECKING
#define ALLOC_CHUNK_USED	\
	(offsetof(AllocChunkData, requested_size) + sizeof(Size))
#else
#define ALLOC_CHUNK_USED	\
	(offsetof(AllocChunkData, size) + sizeof(Size))
#endif

/*
 * AllocPointer
 *		Aligned pointer which may be a member of an allocation set.
 */
typedef void *AllocPointer;

/*
 * AllocBlock
 *		An AllocBlock is the unit of memory that is obtained by aset.c
 *		from malloc().  It contains one or more AllocChunks, which are
 *		the units requested by palloc() and freed by pfree().  AllocChunks
 *		cannot be returned to malloc() individually, instead they are put
 *		on freelists by pfree() and re-used by the next palloc() that has
 *		a matching request size.
 *
 *		AllocBlockData is the header data for a block --- the usable space
 *		within the block begins at the next alignment boundary.
 */
typedef struct AllocBlockData
{
	AllocSet	aset;			/* aset that owns this block */
	AllocBlock	prev;			/* prev block in aset's blocks list, if any */
	AllocBlock	next;			/* next block in aset's blocks list, if any */
	char	   *freeptr;		/* start of free space in this block */
}	AllocBlockData;

/*
 * AllocChunk
 *		The prefix of each piece of memory in an AllocBlock
 *
 * NB: this MUST match StandardChunkHeader as defined by utils/memutils.h.
 */
typedef struct AllocChunkData
{
	 /*
	  * SharedChunkHeader stores all the "shared" details
	  * among multiple chunks, such as memoryAccount to charge,
	  * generation of memory account, memory context that owns this
	  * chunk etc. However, in case of a free chunk, this pointer
	  * actually refers to the next chunk in the free list.
	  */
	struct SharedChunkHeader* sharedHeader;

	Size		size;			/* size of data space allocated in chunk */

	/*
	 * The "requested size" of the chunk. This is the intended allocation
	 * size of the client. Though we may end up allocating a larger block
	 * because of AllocSet overhead, optimistic reuse of chunks
	 * and alignment of chunk size at the power of 2
	 */
#ifdef MEMORY_CONTEXT_CHECKING
	/* when debugging memory usage, also store actual requested size */
	/* this is zero in a free chunk */
	Size		requested_size;
#endif
#ifdef CDB_PALLOC_TAGS
	const char  *alloc_tag;
	int 		alloc_n;
	void *prev_chunk;
	void *next_chunk;
#endif
}	AllocChunkData;

/*
 * AllocPointerIsValid
 *		True iff pointer is valid allocation pointer.
 */
#define AllocPointerIsValid(pointer) PointerIsValid(pointer)

/*
 * AllocSetIsValid
 *		True iff set is valid allocation set.
 */
#define AllocSetIsValid(set) PointerIsValid(set)

#define AllocPointerGetChunk(ptr)	\
					((AllocChunk)(((char *)(ptr)) - ALLOC_CHUNKHDRSZ))
#define AllocChunkGetPointer(chk)	\
					((AllocPointer)(((char *)(chk)) + ALLOC_CHUNKHDRSZ))

/*
 * These functions implement the MemoryContext API for AllocSet contexts.
 */
static void *AllocSetAlloc(MemoryContext context, Size size);
static void *AllocSetAllocHeader(MemoryContext context, Size size);
static void AllocSetFree(MemoryContext context, void *pointer);
static void AllocSetFreeHeader(MemoryContext context, void *pointer);
static void *AllocSetRealloc(MemoryContext context, void *pointer, Size size);
static void AllocSetInit(MemoryContext context);
static void AllocSetReset(MemoryContext context);
static void AllocSetDelete(MemoryContext context);
static Size AllocSetGetChunkSpace(MemoryContext context, void *pointer);
static bool AllocSetIsEmpty(MemoryContext context);
static void AllocSet_GetStats(MemoryContext context, uint64 *nBlocks, uint64 *nChunks,
		uint64 *currentAvailable, uint64 *allAllocated, uint64 *allFreed, uint64 *maxHeld);
static void AllocSetReleaseAccountingForAllAllocatedChunks(MemoryContext context);

static void dump_allocset_block(FILE *file, AllocBlock block);
static void dump_allocset_blocks(FILE *file, AllocBlock blocks);
static void dump_allocset_freelist(FILE *file, AllocSet set);
static void dump_mc_for(FILE *file, MemoryContext mc);
void dump_mc(const char *fname, MemoryContext mc);

#ifdef MEMORY_CONTEXT_CHECKING
static void AllocSetCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for AllocSet contexts.
 */
static MemoryContextMethods AllocSetMethods = {
	AllocSetAlloc,
	AllocSetFree,
	AllocSetRealloc,
	AllocSetInit,
	AllocSetReset,
	AllocSetDelete,
	AllocSetGetChunkSpace,
	AllocSetIsEmpty,
	AllocSet_GetStats,
	AllocSetReleaseAccountingForAllAllocatedChunks
#ifdef MEMORY_CONTEXT_CHECKING
	,AllocSetCheck
#endif
};

/*
 * Table for AllocSetFreeIndex
 */
#define LT16(n) n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n

static const unsigned char LogTable256[256] =
{
	0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4,
	LT16(5), LT16(6), LT16(6), LT16(7), LT16(7), LT16(7), LT16(7),
	LT16(8), LT16(8), LT16(8), LT16(8), LT16(8), LT16(8), LT16(8), LT16(8)
};


static void dump_allocset_block(FILE *file, AllocBlock block)
{
	// block start/free/end pointer
	fprintf(file, "\t%p|%p|%p\n", block, block->freeptr, UserPtr_GetEndPtr(block));

	AllocChunk chunk = (AllocChunk) (((char *)block) + ALLOC_BLOCKHDRSZ);
	while ((char *) chunk < (char *) block->freeptr)
	{
		fprintf(file, "\t\tchunk|%p|%ld", chunk, chunk->size);

#ifdef MEMORY_CONTEXT_CHECKING
		fprintf(file, "%ld", chunk->requested_size);
#endif

		fprintf(file, "\n");

		chunk = (AllocChunk) ((char *) chunk + chunk->size + ALLOC_CHUNKHDRSZ);
	}
}

static void dump_allocset_blocks(FILE *file, AllocBlock blocks)
{
	AllocBlock block = blocks;
	while (block != NULL)
	{
		dump_allocset_block(file, block);
		block = block->next;
	}
}

static void dump_allocset_freelist(FILE *file, AllocSet set)
{
	if (set == NULL)
		return;

	int size = 8;

	for (int i = 0; i < ALLOCSET_NUM_FREELISTS; i++)
	{
		AllocChunk chunk = set->freelist[i];

		while (chunk != NULL)
		{
			fprintf(file, "\t\tfreelist_%d|%p|%ld", size << i, chunk, chunk->size);

#ifdef MEMORY_CONTEXT_CHECKING
			fprintf(file, "%ld", chunk->requested_size);
#endif

			fprintf(file, "\n");

			chunk = (AllocChunk) chunk->sharedHeader;

		}
	}
}

static void dump_mc_for(FILE *file, MemoryContext mc)
{
	if (mc == NULL)
		return;

	AllocSet set = (AllocSet) mc;
	fprintf(file, "%p|%p|%d|%s|"UINT64_FORMAT"|"UINT64_FORMAT"|%zu|%zu|%zu|%zu", mc, mc->parent, mc->type, mc->name,
			mc->allBytesAlloc, mc->allBytesFreed, mc->maxBytesHeld,
			set->initBlockSize, set->maxBlockSize, set->nextBlockSize);

#ifdef CDB_PALLOC_CALLER_ID
	fprintf(file, "|%s|%d", (mc->callerFile == NULL ? "NA" : mc->callerFile), mc->callerLine);
#endif

	fprintf(file, "\n");

	dump_allocset_blocks(file, set->blocks);
	dump_allocset_freelist(file, set);

	dump_mc_for(file, mc->nextchild);
	dump_mc_for(file, mc->firstchild);
}

void dump_mc(const char *fname, MemoryContext mc)
{
	FILE *ofile = fopen(fname, "w+");
	dump_mc_for(ofile, mc);
	fclose(ofile);
}


/* ----------
 * Debug macros
 * ----------
 */
#ifdef CDB_PALLOC_TAGS

void dump_memory_allocation(const char* fname)
{
	FILE *ofile = fopen(fname, "w+");
	dump_memory_allocation_ctxt(ofile, TopMemoryContext);
	fclose(ofile);
}

void dump_memory_allocation_ctxt(FILE *ofile, void *ctxt)
{
	AllocSet set = (AllocSet) ctxt;
	AllocSet next;
	AllocChunk chunk = set->allocList;

	while(chunk)
	{
#ifdef MEMORY_CONTEXT_CHECKING
		fprintf(ofile, "%ld|%s|%d|%d|%d\n", (long) ctxt, chunk->alloc_tag, chunk->alloc_n, (int) chunk->size, (int) chunk->requested_size);
#else
		fprintf(ofile, "%ld|%s|%d|%d\n", (long) ctxt, chunk->alloc_tag, chunk->alloc_n, (int) chunk->size);
#endif
		chunk = chunk->next_chunk;
	}

	next = (AllocSet) set->header.firstchild;
	while(next)
	{
		dump_memory_allocation_ctxt(ofile, next);
		next = (AllocSet) next->header.nextchild;
	}
}
#endif

inline void
AllocFreeInfo(AllocSet set, AllocChunk chunk, bool isHeader) __attribute__((always_inline));

inline void
AllocAllocInfo(AllocSet set, AllocChunk chunk, bool isHeader) __attribute__((always_inline));

/*
 * AllocFreeInfo
 *		Internal function to remove a chunk from the "used" chunks list.
 *		Also, updates the memory accounting of the chunk.
 *
 * set: allocation set that the chunk is part of
 * chunk: the chunk that is being freed
 * isHeader: whether the chunk was hosting a shared header for some chunks
 */
void
AllocFreeInfo(AllocSet set, AllocChunk chunk, bool isHeader)
{
#ifdef MEMORY_CONTEXT_CHECKING
	Assert(chunk->requested_size != 0xFFFFFFFF); /* This chunk must be in-use. */
#endif

	/* A header chunk should never have any sharedHeader */
	Assert((isHeader && chunk->sharedHeader == NULL) || (!isHeader && chunk->sharedHeader != NULL));

	if (!isHeader)
	{
		chunk->sharedHeader->balance -= (chunk->size + ALLOC_CHUNKHDRSZ);

		Assert(chunk->sharedHeader->balance >= 0);

		/*
		 * Some chunks don't use memory accounting. E.g., any chunks allocated before
		 * memory accounting is setup will get undefined owner. Chunks without memory
		 * account do not need any accounting adjustment.
		 */
		if (chunk->sharedHeader->memoryAccountId != MEMORY_OWNER_TYPE_Undefined)
		{
			MemoryAccounting_Free(chunk->sharedHeader->memoryAccountId, chunk->size + ALLOC_CHUNKHDRSZ);

			if (chunk->sharedHeader->balance == 0)
			{
				/* No chunk is sharing this header, so remove it from the sharedHeaderList */
				Assert(set->sharedHeaderList != NULL &&
						(set->sharedHeaderList->next != NULL || set->sharedHeaderList == chunk->sharedHeader));
				SharedChunkHeader *prevSharedHeader = chunk->sharedHeader->prev;
				SharedChunkHeader *nextSharedHeader = chunk->sharedHeader->next;

				if (prevSharedHeader != NULL)
				{
					prevSharedHeader->next = chunk->sharedHeader->next;
				}
				else
				{
					Assert(set->sharedHeaderList == chunk->sharedHeader);
					set->sharedHeaderList = nextSharedHeader;
				}

				if (nextSharedHeader != NULL)
				{
					nextSharedHeader->prev = prevSharedHeader;
				}

				/* Free the memory held by the header */
				AllocSetFreeHeader((MemoryContext) set, chunk->sharedHeader);
			}
		}
		else
		{
			/*
			 * nullAccountHeader assertion. Note: we have already released the shared header balance.
			 * Also note: we don't try to free nullAccountHeader, even if the balance reaches 0 (MPP-22566).
			 */

			Assert(chunk->sharedHeader == set->nullAccountHeader);
		}
	}
	else
	{
		/*
		 * At this point, we have already freed the chunks that were using this
		 * SharedChunkHeader and the chunk's shared header had a memory account
		 * (otherwise we don't call AllocSetFreeHeader()). So, the header should
		 * reduce the balance of SharedChunkHeadersMemoryAccount. Note, if we
		 * decide to fix MPP-22566 (the nullAccountHeader releasing), we need
		 * to check if we are releasing nullAccountHeader, as that header is not
		 * charged against SharedChunkMemoryAccount, and that result in a
		 * negative balance for SharedChunkMemoryAccount.
		 */
		MemoryAccounting_Free(MEMORY_OWNER_TYPE_SharedChunkHeader, chunk->size + ALLOC_CHUNKHDRSZ);
	}

#ifdef CDB_PALLOC_TAGS
	AllocChunk prev = chunk->prev_chunk;
	AllocChunk next = chunk->next_chunk;

	if(prev != NULL)
	{
		prev->next_chunk = next;
	}
	else
	{
		Assert(set->allocList == chunk);
		set->allocList = next;
	}

	if(next != NULL)
	{
		next->prev_chunk = prev;
	}
#endif
}

/*
 * AllocAllocInfo
 *		Internal function to add a "newly used" (may be already allocated) chunk
 *		into the "used" chunks list.
 *		Also, updates the memory accounting of the chunk.
 *
 * set: allocation set that the chunk is part of
 * chunk: the chunk that is being freed
 * isHeader: whether the chunk will be used to host a shared header for another chunk
 */
void
AllocAllocInfo(AllocSet set, AllocChunk chunk, bool isHeader)
{
	if (!isHeader)
	{
		/*
		 * We only start tallying memory after the initial setup is done.
		 * We may not keep accounting for some chunk's memory: e.g., TopMemoryContext
		 * or MemoryAccountMemoryContext gets allocated even before we start assigning
		 * accounts to any chunks.
		 */
		if (ActiveMemoryAccountId != MEMORY_OWNER_TYPE_Undefined)
		{
			SharedChunkHeader *desiredHeader = set->sharedHeaderList;

			/* Try to look-ahead in the sharedHeaderList to find the desiredHeader */
			if (set->sharedHeaderList != NULL && set->sharedHeaderList->memoryAccountId == ActiveMemoryAccountId)
			{
				/* Do nothing, we already assigned sharedHeaderList to desiredHeader */
			}
			else if (set->sharedHeaderList != NULL && set->sharedHeaderList->next != NULL &&
					set->sharedHeaderList->next->memoryAccountId == ActiveMemoryAccountId)
			{
				desiredHeader = set->sharedHeaderList->next;
			}
			else if (set->sharedHeaderList != NULL && set->sharedHeaderList->next != NULL &&
					set->sharedHeaderList->next->next != NULL &&
					set->sharedHeaderList->next->next->memoryAccountId == ActiveMemoryAccountId)
			{
				desiredHeader = set->sharedHeaderList->next->next;
			}
			else
			{
				/* The last 3 headers are not suitable for next chunk, so we need a new shared header */

				desiredHeader = AllocSetAllocHeader((MemoryContext) set, sizeof(SharedChunkHeader));

				desiredHeader->context = (MemoryContext) set;
				desiredHeader->memoryAccountId = ActiveMemoryAccountId;
				desiredHeader->balance = 0;

				desiredHeader->next = set->sharedHeaderList;
				if (desiredHeader->next != NULL)
				{
					desiredHeader->next->prev = desiredHeader;
				}
				desiredHeader->prev = NULL;

				set->sharedHeaderList = desiredHeader;
			}

			desiredHeader->balance += (chunk->size + ALLOC_CHUNKHDRSZ);
			chunk->sharedHeader = desiredHeader;

			MemoryAccounting_Allocate(ActiveMemoryAccountId, chunk->size + ALLOC_CHUNKHDRSZ);
		}
		else
		{
			/* We have NULL ActiveMemoryAccount, so use nullAccountHeader */

			if (set->nullAccountHeader == NULL)
			{
				/*
				 * SharedChunkHeadersMemoryAccount comes to life first. So, if
				 * ActiveMemoryAccount is NULL, so should be the SharedChunkHeadersMemoryAccount
				 */
				Assert(ActiveMemoryAccountId == MEMORY_OWNER_TYPE_Undefined && NULL == SharedChunkHeadersMemoryAccount);

				/* We initialize nullAccountHeader only if necessary */
				SharedChunkHeader *desiredHeader = AllocSetAllocHeader((MemoryContext) set, sizeof(SharedChunkHeader));
				desiredHeader->context = (MemoryContext) set;
				desiredHeader->memoryAccountId = MEMORY_OWNER_TYPE_Undefined;
				desiredHeader->balance = 0;

				set->nullAccountHeader = desiredHeader;

				/*
				 * No need to charge SharedChunkHeadersMemoryAccount for
				 * the nullAccountHeader as a null ActiveMemoryAccount
				 * automatically implies a null SharedChunkHeadersMemoryAccount
				 */
			}

			chunk->sharedHeader = set->nullAccountHeader;

			set->nullAccountHeader->balance += (chunk->size + ALLOC_CHUNKHDRSZ);
		}
	}
	else
	{
		/*
		 * At this point we still may have NULL SharedChunksHeadersMemoryAccount.
		 * Note: this is only possible if the ActiveMemoryAccount and
		 * SharedChunksHeadersMemoryAccount both are null, and we are
		 * trying to create a nullAccountHeader
		 */
		if (SharedChunkHeadersMemoryAccount != NULL)
		{
			MemoryAccounting_Allocate(MEMORY_OWNER_TYPE_SharedChunkHeader, chunk->size + ALLOC_CHUNKHDRSZ);
		}

		/*
		 * The only reason a sharedChunkHeader can be NULL is the chunk
		 * is allocated (not part of the freelist), and it is being used
		 * to store shared header for other chunks
		 */
		chunk->sharedHeader = NULL;
	}

#ifdef CDB_PALLOC_TAGS
	chunk->prev_chunk = NULL;

	/*
	 * We are not double-calling AllocAllocInfo where chunk is the only chunk in the allocList.
	 * Note: the double call is only detectable if allocList is currently pointing to this chunk
	 * (i.e., this chunk is the head). To detect generic case, we need another flag, which we
	 * are avoiding here. Also note, if chunk is the head, then it will create a circular linked
	 * list, otherwise we might just corrupt the linked list
	 */
	Assert(!chunk->next_chunk || chunk != set->allocList);

	chunk->next_chunk = set->allocList;

	if(set->allocList)
	{
		set->allocList->prev_chunk = chunk;
	}

	set->allocList = chunk;

	chunk->alloc_tag = set->header.callerFile;
	chunk->alloc_n = set->header.callerLine;
#endif
}

/* ----------
 * AllocSetFreeIndex -
 *
 *		Depending on the size of an allocation compute which freechunk
 *		list of the alloc set it belongs to.  Caller must have verified
 *		that size <= ALLOC_CHUNK_LIMIT.
 * ----------
 */
static inline int
AllocSetFreeIndex(Size size)
{
	int			idx;
	unsigned int t,
				tsize;

	if (size > (1 << ALLOC_MINBITS))
	{
		tsize = (size - 1) >> ALLOC_MINBITS;

		/*
		 * At this point we need to obtain log2(tsize)+1, ie, the number of
		 * not-all-zero bits at the right.  We used to do this with a
		 * shift-and-count loop, but this function is enough of a hotspot to
		 * justify micro-optimization effort.  The best approach seems to be
		 * to use a lookup table.  Note that this code assumes that
		 * ALLOCSET_NUM_FREELISTS <= 17, since we only cope with two bytes of
		 * the tsize value.
		 */
		t = tsize >> 8;
		idx = t ? LogTable256[t] + 8 : LogTable256[tsize];

		Assert(idx < ALLOCSET_NUM_FREELISTS);
	}
	else
		idx = 0;

	return idx;
}

#ifdef CLOBBER_FREED_MEMORY

/* Wipe freed memory for debugging purposes */
static void
wipe_mem(void *ptr, size_t size)
{
	VALGRIND_MAKE_MEM_UNDEFINED(ptr, size);
	memset(ptr, 0x7F, size);
	VALGRIND_MAKE_MEM_NOACCESS(ptr, size);
}
#endif

#ifdef MEMORY_CONTEXT_CHECKING
static void
set_sentinel(void *base, Size offset)
{
	char	   *ptr = (char *) base + offset;

	VALGRIND_MAKE_MEM_UNDEFINED(ptr, 1);
	*ptr = 0x7E;
	VALGRIND_MAKE_MEM_NOACCESS(ptr, 1);
}

static bool
sentinel_ok(const void *base, Size offset)
{
	const char *ptr = (const char *) base + offset;
	bool		ret;

	VALGRIND_MAKE_MEM_DEFINED(ptr, 1);
	ret = *ptr == 0x7E;
	VALGRIND_MAKE_MEM_NOACCESS(ptr, 1);

	return ret;
}
#endif

#ifdef RANDOMIZE_ALLOCATED_MEMORY

/*
 * Fill a just-allocated piece of memory with "random" data.  It's not really
 * very random, just a repeating sequence with a length that's prime.  What
 * we mainly want out of it is to have a good probability that two palloc's
 * of the same number of bytes start out containing different data.
 *
 * The region may be NOACCESS, so make it UNDEFINED first to avoid errors as
 * we fill it.  Filling the region makes it DEFINED, so make it UNDEFINED
 * again afterward.  Whether to finally make it UNDEFINED or NOACCESS is
 * fairly arbitrary.  UNDEFINED is more convenient for AllocSetRealloc(), and
 * other callers have no preference.
 */
static void
randomize_mem(char *ptr, size_t size)
{
	static int	save_ctr = 1;
	size_t		remaining = size;
	int			ctr;

	ctr = save_ctr;
	VALGRIND_MAKE_MEM_UNDEFINED(ptr, size);
	while (remaining-- > 0)
	{
		*ptr++ = ctr;
		if (++ctr > 251)
			ctr = 1;
	}
	VALGRIND_MAKE_MEM_UNDEFINED(ptr - size, size);
	save_ctr = ctr;
}
#endif   /* RANDOMIZE_ALLOCATED_MEMORY */


/*
 * Public routines
 */


/*
 * AllocSetContextCreate
 *		Create a new AllocSet context.
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (for debugging --- string will be copied)
 * minContextSize: minimum context size
 * initBlockSize: initial allocation block size
 * maxBlockSize: maximum allocation block size
 */
MemoryContext
AllocSetContextCreate(MemoryContext parent,
					  const char *name,
					  Size minContextSize,
					  Size initBlockSize,
					  Size maxBlockSize)
{
	if (am_kv_storage && memorycontext_lock)
	{
		ADD_THREAD_LOCK_EXEC(MemoryContext);
	}
	AllocSet	context;

	/* Do the type-independent part of context creation */
	context = (AllocSet) MemoryContextCreate(T_AllocSetContext,
											 sizeof(AllocSetContext),
											 &AllocSetMethods,
											 parent,
											 name);

	/*
	 * Make sure alloc parameters are reasonable, and save them.
	 *
	 * We somewhat arbitrarily enforce a minimum 1K block size.
	 */
	initBlockSize = MAXALIGN(initBlockSize);
	if (initBlockSize < 1024)
		initBlockSize = 1024;
	maxBlockSize = MAXALIGN(maxBlockSize);
	if (maxBlockSize < initBlockSize)
		maxBlockSize = initBlockSize;
	Assert(AllocHugeSizeIsValid(maxBlockSize)); /* must be safe to double */
	context->initBlockSize = initBlockSize;
	context->maxBlockSize = maxBlockSize;
	context->nextBlockSize = initBlockSize;

	context->sharedHeaderList = NULL;

#ifdef CDB_PALLOC_TAGS
	context->allocList = NULL;
#endif

	/*
	 * Compute the allocation chunk size limit for this context.  It can't be
	 * more than ALLOC_CHUNK_LIMIT because of the fixed number of freelists.
	 * If maxBlockSize is small then requests exceeding the maxBlockSize, or
	 * even a significant fraction of it, should be treated as large chunks
	 * too.  For the typical case of maxBlockSize a power of 2, the chunk size
	 * limit will be at most 1/8th maxBlockSize, so that given a stream of
	 * requests that are all the maximum chunk size we will waste at most
	 * 1/8th of the allocated space.
	 *
	 * We have to have allocChunkLimit a power of two, because the requested
	 * and actually-allocated sizes of any chunk must be on the same side of
	 * the limit, else we get confused about whether the chunk is "big".
	 *
	 * Also, allocChunkLimit must not exceed ALLOCSET_SEPARATE_THRESHOLD.
	 */
	StaticAssertStmt(ALLOC_CHUNK_LIMIT == ALLOCSET_SEPARATE_THRESHOLD,
					 "ALLOC_CHUNK_LIMIT != ALLOCSET_SEPARATE_THRESHOLD");

	context->allocChunkLimit = ALLOC_CHUNK_LIMIT;
	while ((Size) (context->allocChunkLimit + ALLOC_CHUNKHDRSZ) >
		   (Size) ((maxBlockSize - ALLOC_BLOCKHDRSZ) / ALLOC_CHUNK_FRACTION))
		context->allocChunkLimit >>= 1;

	/*
	 * Grab always-allocated space, if requested
	 */
	if (minContextSize > ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ)
	{
		Size		blksize = MAXALIGN(minContextSize);
		AllocBlock	block;

		block = (AllocBlock) gp_malloc(blksize);
		if (block == NULL)
			MemoryContextError(ERRCODE_OUT_OF_MEMORY,
							   &context->header, CDB_MCXT_WHERE(&context->header),
							   "Out of memory.  Unable to allocate %lu bytes.",
							   (unsigned long)blksize);
		block->aset = context;
		block->freeptr = ((char *) block) + ALLOC_BLOCKHDRSZ;
		block->prev = NULL;
		block->next = context->blocks;
		if (block->next)
			block->next->prev = block;
		context->blocks = block;
		/* Mark block as not to be released at reset time */
		context->keeper = block;

		MemoryContextNoteAlloc(&context->header, blksize);              /*CDB*/
		/*
		 * We are allocating new memory in this block, but we are not accounting
		 * for this. The concept of memory accounting is to track the actual
		 * allocation/deallocation by the memory user. This block is preemptively
		 * allocating memory, which is "unused" by actual consumers. Therefore,
		 * memory accounting currently wouldn't track this
		 */

		/* Mark unallocated space NOACCESS; leave the block header alone. */
		VALGRIND_MAKE_MEM_NOACCESS(block->freeptr,
								   blksize - ALLOC_BLOCKHDRSZ);
	}

	context->nullAccountHeader = NULL;

	if (am_kv_storage && memorycontext_lock)
	{
		REMOVE_THREAD_LOCK_EXEC(MemoryContext);
	}
	return (MemoryContext) context;
}

/*
 * AllocSetInit
 *		Context-type-specific initialization routine.
 *
 * This is called by MemoryContextCreate() after setting up the
 * generic MemoryContext fields and before linking the new context
 * into the context tree.  We must do whatever is needed to make the
 * new context minimally valid for deletion.  We must *not* risk
 * failure --- thus, for example, allocating more memory is not cool.
 * (AllocSetContextCreate can allocate memory when it gets control
 * back, however.)
 */
static void
AllocSetInit(MemoryContext context)
{
	/*
	 * Since MemoryContextCreate already zeroed the context node, we don't
	 * have to do anything here: it's already OK.
	 */
}


/*
 * AllocSetReleaseAccountingForAllAllocatedChunks
 * 		Iterates through all the shared headers in the sharedHeaderList
 * 		and release their accounting information using the correct MemoryAccount.
 *
 * This is called by AllocSetReset() or AllocSetDelete(). In other words, any time we
 * bulk release all the chunks that are in-use, we want to update the corresponding
 * accounting information.
 *
 * This is also part of the function pointers of MemoryContextMethods. During the
 * memory accounting reset, this is called to release all the chunk accounting
 * in MemoryAccountMemoryContext without actually deletion the chunks.
 *
 * This method can be called multiple times during a memory context reset process
 * without any harm. It correctly removes all the shared headers from the sharedHeaderList
 * on the first use. So, on subsequent use we do not "double" free the memory accounts.
 */
static void AllocSetReleaseAccountingForAllAllocatedChunks(MemoryContext context)
{
	AllocSet set = (AllocSet) context;

	/* The memory consumed by the shared headers themselves */
	uint64 sharedHeaderMemoryOverhead = 0;

	for (SharedChunkHeader* curHeader = set->sharedHeaderList; curHeader != NULL;
			curHeader = curHeader->next)
	{
		Assert(curHeader->balance > 0);
		MemoryAccounting_Free(curHeader->memoryAccountId, curHeader->balance);

		AllocChunk chunk = AllocPointerGetChunk(curHeader);

		sharedHeaderMemoryOverhead += (chunk->size + ALLOC_CHUNKHDRSZ);
	}

	/*
	 * In addition to releasing accounting for the chunks, we also need
	 * to release accounting for the shared headers
	 */
	MemoryAccounting_Free(MEMORY_OWNER_TYPE_SharedChunkHeader, sharedHeaderMemoryOverhead);

	/*
	 * Wipe off the sharedHeaderList. We don't free any memory here,
	 * as this method is only supposed to be called during reset
	 */
	set->sharedHeaderList = NULL;

#ifdef CDB_PALLOC_TAGS
	set->allocList = NULL;
#endif
}

/*
 * AllocSetReset
 *		Frees all memory which is allocated in the given set.
 *
 * Actually, this routine has some discretion about what to do.
 * It should mark all allocated chunks freed, but it need not necessarily
 * give back all the resources the set owns.  Our actual implementation is
 * that we hang onto any "keeper" block specified for the set.  In this way,
 * we don't thrash malloc() when a context is repeatedly reset after small
 * allocations, which is typical behavior for per-tuple contexts.
 */
static void
AllocSetReset(MemoryContext context)
{
	AllocSet	set = (AllocSet) context;
	AllocBlock	block;

	AssertArg(AllocSetIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
	/* Check for corruption and leaks before freeing */
	AllocSetCheck(context);
#endif

	/* Before we wipe off the allocList, we must ensure that the MemoryAccounts
	 * who holds the allocation accounting for the chunks now release these
	 * allocation accounting.
	 */
	AllocSetReleaseAccountingForAllAllocatedChunks(context);

	/* Clear chunk freelists */
	MemSetAligned(set->freelist, 0, sizeof(set->freelist));

	block = set->blocks;

	/* New blocks list is either empty or just the keeper block */
	set->blocks = set->keeper;

	while (block != NULL)
	{
		AllocBlock	next = block->next;

		if (block == set->keeper)
		{
			/* Reset the block, but don't return it to malloc */
			char	   *datastart = ((char *) block) + ALLOC_BLOCKHDRSZ;

#ifdef CLOBBER_FREED_MEMORY
			wipe_mem(datastart, block->freeptr - datastart);
#else
			/* wipe_mem() would have done this */
			VALGRIND_MAKE_MEM_NOACCESS(datastart, block->freeptr - datastart);
#endif
			block->freeptr = datastart;
			block->prev = NULL;
			block->next = NULL;
		}
		else
		{
			size_t freesz = UserPtr_GetUserPtrSize(block);

			/* Normal case, release the block */
			MemoryContextNoteFree(&set->header, freesz);

#ifdef CLOBBER_FREED_MEMORY
			wipe_mem(block, block->freeptr - ((char *) block));
#endif
			gp_free(block);
		}
		block = next;
	}

	/* Reset block size allocation sequence, too */
	set->nextBlockSize = set->initBlockSize;

	set->nullAccountHeader = NULL;
}

/*
 * AllocSetDelete
 *		Frees all memory which is allocated in the given set,
 *		in preparation for deletion of the set.
 *
 * Unlike AllocSetReset, this *must* free all resources of the set.
 * But note we are not responsible for deleting the context node itself.
 */
static void
AllocSetDelete(MemoryContext context)
{
	AllocSet	set = (AllocSet) context;
	AllocBlock	block = set->blocks;

	AssertArg(AllocSetIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
	/* Check for corruption and leaks before freeing */
	AllocSetCheck(context);
#endif

	AllocSetReleaseAccountingForAllAllocatedChunks(context);

	/* Make it look empty, just in case... */
	MemSetAligned(set->freelist, 0, sizeof(set->freelist));
	set->blocks = NULL;
	set->keeper = NULL;

	while (block != NULL)
	{
		AllocBlock	next = block->next;
		size_t freesz = UserPtr_GetUserPtrSize(block);
		MemoryContextNoteFree(&set->header, freesz);

#ifdef CLOBBER_FREED_MEMORY
		wipe_mem(block, block->freeptr - ((char *) block));
#endif
		gp_free(block);
		block = next;
	}

	set->sharedHeaderList = NULL;
	set->nullAccountHeader = NULL;
}

/*
 * AllocSetAllocImpl
 *		Returns pointer to allocated memory of given size; memory is added
 *		to the set.
 *
 * Parameters:
 *		context: the context under which the memory was allocated
 *		size: size of the memory to allocate
 *		isHeader: whether the memory will be hosting a shared header
 *
 * Returns the pointer to the memory region.
 *
 * No request may exceed:
 *		MAXALIGN_DOWN(SIZE_MAX) - ALLOC_BLOCKHDRSZ - ALLOC_CHUNKHDRSZ
 * All callers use a much-lower limit.
 */
static void *
AllocSetAllocImpl(MemoryContext context, Size size, bool isHeader)
{
	AllocSet	set = (AllocSet) context;
	AllocBlock	block;
	AllocChunk	chunk;
	int			fidx;
	Size		chunk_size;
	Size		blksize;

	AssertArg(AllocSetIsValid(set));
#ifdef USE_ASSERT_CHECKING
	if (IsUnderPostmaster && context != ErrorContext && mainthread() != 0 && !pthread_equal(main_tid, pthread_self()))
	{
#if defined(__darwin__)
		elog(ERROR,"palloc called from thread (OS-X pthread_sigmask is broken: MPP-4923)");
#else
		elog(ERROR,"palloc called from thread");
#endif
	}
#endif

	/*
	 * If requested size exceeds maximum for chunks, allocate an entire block
	 * for this request.
	 */
	if (size > set->allocChunkLimit)
	{
		chunk_size = MAXALIGN(size);
		blksize = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
		block = (AllocBlock) gp_malloc(blksize);
		if (block == NULL)
			MemoryContextError(ERRCODE_OUT_OF_MEMORY,
							   &set->header, CDB_MCXT_WHERE(&set->header),
							   "Out of memory.  Failed on request of size %zu bytes.", size);
		block->aset = set;
		block->freeptr = UserPtr_GetEndPtr(block);

		chunk = (AllocChunk) (((char *) block) + ALLOC_BLOCKHDRSZ);
		chunk->size = chunk_size;
		Assert(chunk->size >= 8);
		/* We use malloc internally, which may not 0 out the memory. */
		chunk->sharedHeader = NULL;
		/* set mark to catch clobber of "unused" space */
#ifdef MEMORY_CONTEXT_CHECKING
		/* Valgrind: Will be made NOACCESS below. */
		chunk->requested_size = size;
		if (size < chunk_size)
			set_sentinel(AllocChunkGetPointer(chunk), size);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* fill the allocated space with junk */
		randomize_mem((char *) AllocChunkGetPointer(chunk), size);
#endif

		/*
		 * Stick the new block underneath the active allocation block, if any,
		 * so that we don't lose the use of the space remaining therein.
		 */
		if (set->blocks != NULL)
		{
			block->prev = set->blocks;
			block->next = set->blocks->next;
			if (block->next)
				block->next->prev = block;
			set->blocks->next = block;
		}
		else
		{
			block->prev = NULL;
			block->next = NULL;
			set->blocks = block;
		}

		MemoryContextNoteAlloc(&set->header, blksize);              /*CDB*/


		AllocAllocInfo(set, chunk, isHeader);
		/*
		 * Chunk header public fields remain DEFINED.  The requested
		 * allocation itself can be NOACCESS or UNDEFINED; our caller will
		 * soon make it UNDEFINED.  Make extra space at the end of the chunk,
		 * if any, NOACCESS.
		 */
		VALGRIND_MAKE_MEM_NOACCESS((char *) chunk + ALLOC_CHUNK_PUBLIC,
						 chunk_size + ALLOC_CHUNKHDRSZ - ALLOC_CHUNK_PUBLIC);

		return AllocChunkGetPointer(chunk);
	}

	/*
	 * Request is small enough to be treated as a chunk.  Look in the
	 * corresponding free list to see if there is a free chunk we could reuse.
	 * If one is found, remove it from the free list, make it again a member
	 * of the alloc set and return its data address.
	 */
	fidx = AllocSetFreeIndex(size);
	chunk = set->freelist[fidx];

	if (chunk != NULL)
	{
		Assert(chunk->size >= size);
		set->freelist[fidx] = (AllocChunk) chunk->sharedHeader;
		// if (set->freelist[fidx] != NULL)
		// 	Assert(set->freelist[fidx] >= 8);
		/*
		 * The sharedHeader pointer until now was pointing to
		 * the next free chunk in this freelist. As this chunk
		 * is just removed from freelist, it no longer points
		 * to the next free chunk in this freelist
		 */
		chunk->sharedHeader = NULL;

#ifdef MEMORY_CONTEXT_CHECKING
		/* Valgrind: Free list requested_size should be DEFINED. */
		chunk->requested_size = size;
		VALGRIND_MAKE_MEM_NOACCESS(&chunk->requested_size,
								   sizeof(chunk->requested_size));
		/* set mark to catch clobber of "unused" space */
		if (size < chunk->size)
			set_sentinel(AllocChunkGetPointer(chunk), size);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* fill the allocated space with junk */
		randomize_mem((char *) AllocChunkGetPointer(chunk), size);
#endif

		AllocAllocInfo(set, chunk, isHeader);
		return AllocChunkGetPointer(chunk);
	}

	/*
	 * Choose the actual chunk size to allocate.
	 */
	chunk_size = (1 << ALLOC_MINBITS) << fidx;
	Assert(chunk_size >= size);

	/*
	 * If there is enough room in the active allocation block, we will put the
	 * chunk into that block.  Else must start a new one.
	 */
	if ((block = set->blocks) != NULL)
	{
		Size		availspace = (char*)UserPtr_GetEndPtr(block) - block->freeptr;

		if (availspace < (chunk_size + ALLOC_CHUNKHDRSZ))
		{
			/*
			 * The existing active (top) block does not have enough room for
			 * the requested allocation, but it might still have a useful
			 * amount of space in it.  Once we push it down in the block list,
			 * we'll never try to allocate more space from it. So, before we
			 * do that, carve up its free space into chunks that we can put on
			 * the set's freelists.
			 *
			 * Because we can only get here when there's less than
			 * ALLOC_CHUNK_LIMIT left in the block, this loop cannot iterate
			 * more than ALLOCSET_NUM_FREELISTS-1 times.
			 */
			while (availspace >= ((1 << ALLOC_MINBITS) + ALLOC_CHUNKHDRSZ))
			{
				Size		availchunk = availspace - ALLOC_CHUNKHDRSZ;
				int			a_fidx = AllocSetFreeIndex(availchunk);

				/*
				 * In most cases, we'll get back the index of the next larger
				 * freelist than the one we need to put this chunk on.  The
				 * exception is when availchunk is exactly a power of 2.
				 */
				if (availchunk != ((Size) 1 << (a_fidx + ALLOC_MINBITS)))
				{
					a_fidx--;
					Assert(a_fidx >= 0);
					availchunk = ((Size) 1 << (a_fidx + ALLOC_MINBITS));
				}

				chunk = (AllocChunk) (block->freeptr);

				/* Prepare to initialize the chunk header. */
				VALGRIND_MAKE_MEM_UNDEFINED(chunk, ALLOC_CHUNK_USED);

				block->freeptr += (availchunk + ALLOC_CHUNKHDRSZ);
				availspace -= (availchunk + ALLOC_CHUNKHDRSZ);

				chunk->size = availchunk;
				Assert(chunk->size >= 8);
#ifdef MEMORY_CONTEXT_CHECKING
				chunk->requested_size = 0xFFFFFFFF;	/* mark it free */
#endif
				chunk->sharedHeader = (void *) set->freelist[a_fidx];

				set->freelist[a_fidx] = chunk;
			}

			/* Mark that we need to create a new block */
			block = NULL;
		}
	}

	/*
	 * Time to create a new regular (multi-chunk) block?
	 */
	if (block == NULL)
	{
		Size		required_size;

		/*
		 * The first such block has size initBlockSize, and we double the
		 * space in each succeeding block, but not more than maxBlockSize.
		 */
		blksize = set->nextBlockSize;
		set->nextBlockSize <<= 1;
		if (set->nextBlockSize > set->maxBlockSize)
			set->nextBlockSize = set->maxBlockSize;

		/*
		 * If initBlockSize is less than ALLOC_CHUNK_LIMIT, we could need more
		 * space... but try to keep it a power of 2.
		 */
		required_size = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
		while (blksize < required_size)
			blksize <<= 1;

		/* Try to allocate it */
		block = (AllocBlock) gp_malloc(blksize);

		/*
		 * We could be asking for pretty big blocks here, so cope if malloc
		 * fails.  But give up if there's less than a meg or so available...
		 */
		while (block == NULL && blksize > 1024 * 1024)
		{
			blksize >>= 1;
			if (blksize < required_size)
				break;
			block = (AllocBlock) gp_malloc(blksize);
		}

		if (block == NULL)
			MemoryContextError(ERRCODE_OUT_OF_MEMORY,
							   &set->header, CDB_MCXT_WHERE(&set->header),
							   "Out of memory.  Failed on request of size %zu bytes.",
							   size);

		block->aset = set;
		block->freeptr = ((char *) block) + ALLOC_BLOCKHDRSZ;

		/*
		 * If this is the first block of the set, make it the "keeper" block.
		 * Formerly, a keeper block could only be created during context
		 * creation, but allowing it to happen here lets us have fast reset
		 * cycling even for contexts created with minContextSize = 0; that way
		 * we don't have to force space to be allocated in contexts that might
		 * never need any space.  Don't mark an oversize block as a keeper,
		 * however.
		 */
		if (set->keeper == NULL && blksize == set->initBlockSize)
			set->keeper = block;

		/* Mark unallocated space NOACCESS. */
		VALGRIND_MAKE_MEM_NOACCESS(block->freeptr,
								   blksize - ALLOC_BLOCKHDRSZ);

		block->prev = NULL;
		block->next = set->blocks;
		if (block->next)
			block->next->prev = block;
		set->blocks = block;
		MemoryContextNoteAlloc(&set->header, blksize);              /*CDB*/
	}

	/*
	 * OK, do the allocation
	 */
	chunk = (AllocChunk) (block->freeptr);

	/* Prepare to initialize the chunk header. */
	VALGRIND_MAKE_MEM_UNDEFINED(chunk, ALLOC_CHUNK_USED);

	block->freeptr += (chunk_size + ALLOC_CHUNKHDRSZ);
	Assert(block->freeptr <= ((char *)UserPtr_GetEndPtr(block)));

	chunk->sharedHeader = NULL;
	chunk->size = chunk_size;
	Assert(chunk->size >= 8);
#ifdef MEMORY_CONTEXT_CHECKING
	chunk->requested_size = size;
	VALGRIND_MAKE_MEM_NOACCESS(&chunk->requested_size,
							   sizeof(chunk->requested_size));
	/* set mark to catch clobber of "unused" space */
	if (size < chunk->size)
		set_sentinel(AllocChunkGetPointer(chunk), size);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
	/* fill the allocated space with junk */
	randomize_mem((char *) AllocChunkGetPointer(chunk), size);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
	/* fill the allocated space with junk */
	randomize_mem((char *) AllocChunkGetPointer(chunk), size);
#endif

	AllocAllocInfo(set, chunk, isHeader);

	return AllocChunkGetPointer(chunk);
}


/*
 * AllocSetAlloc
 *		Returns pointer to an allocated memory of given size; memory is added
 *		to the set.
 */
static void *
AllocSetAlloc(MemoryContext context, Size size)
{
	return AllocSetAllocImpl(context, size, false);
}

/*
 * AllocSetAllocHeader
 *		Returns pointer to an allocated memory of a given size
 *		that will be used to host a shared header for other chunks
 */
static void *
AllocSetAllocHeader(MemoryContext context, Size size)
{
	return AllocSetAllocImpl(context, size, true);
}

/*
 * AllocSetFreeImpl
 *		Frees allocated memory; memory is removed from the set.
 *
 * Parameters:
 *		context: the context under which the memory was allocated
 *		pointer: the pointer to free
 *		isHeader: whether the memory was hosting a shared header
 */
static void
AllocSetFreeImpl(MemoryContext context, void *pointer, bool isHeader)
{
	AllocSet	set = (AllocSet) context;
	AllocChunk	chunk = AllocPointerGetChunk(pointer);

	Assert(chunk->size > 0);
	AllocFreeInfo(set, chunk, isHeader);

#ifdef USE_ASSERT_CHECKING
	/*
	 * This check doesnt work because pfree is called during error handling from inside
	 * AtAbort_Portals.  That can only happen if the error was from a thread (say a SEGV)
	 */
	/*
	if (IsUnderPostmaster && context != ErrorContext && mainthread() != 0 && !pthread_equal(main_tid, pthread_self()))
	{
		elog(ERROR,"pfree called from thread");
	}
	*/
#endif

#ifdef MEMORY_CONTEXT_CHECKING
	VALGRIND_MAKE_MEM_DEFINED(&chunk->requested_size,
							  sizeof(chunk->requested_size));
	Assert(chunk->requested_size != 0xFFFFFFFF);
	/* Test for someone scribbling on unused space in chunk */
	if (chunk->requested_size < chunk->size)
	{
		if (!sentinel_ok(pointer, chunk->requested_size))
			elog(ERROR, "detected write past chunk end in %s %p (%s:%d)",
					set->header.name, chunk, CDB_MCXT_WHERE(&set->header));
	}
#endif

	if (chunk->size > set->allocChunkLimit)
	{
		/*
		 * Big chunks are certain to have been allocated as single-chunk
		 * blocks.  Just unlink that block and return it to malloc().
		 */
		AllocBlock	block = (AllocBlock) (((char *) chunk) - ALLOC_BLOCKHDRSZ);

		size_t freesz;

		/*
		 * Try to verify that we have a sane block pointer: it should
		 * reference the correct aset, and freeptr and endptr should point
		 * just past the chunk.
		 */
		if (block->aset != set ||
			block->freeptr != UserPtr_GetEndPtr(block) ||
			block->freeptr != ((char *) block) +
			(chunk->size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ))
			elog(ERROR, "could not find block containing chunk %p", chunk);

		/* OK, remove block from aset's list and free it */
		if (block->prev)
			block->prev->next = block->next;
		else
			set->blocks = block->next;
		if (block->next)
			block->next->prev = block->prev;
		freesz = UserPtr_GetUserPtrSize(block);

#ifdef CLOBBER_FREED_MEMORY
		wipe_mem(block, block->freeptr - ((char *) block));
#endif
		MemoryContextNoteFree(&set->header, freesz);
		gp_free(block);
	}
	else
	{
		/* Normal case, put the chunk into appropriate freelist */
		int			fidx = AllocSetFreeIndex(chunk->size);

		chunk->sharedHeader = (void *) set->freelist[fidx];

#ifdef CLOBBER_FREED_MEMORY
		wipe_mem(pointer, chunk->size);
#endif

#ifdef MEMORY_CONTEXT_CHECKING
		/* Reset requested_size to 0 in chunks that are on freelist */
		chunk->requested_size = 0xFFFFFFFF;
#endif

		set->freelist[fidx] = chunk;
		Assert(chunk->size >= 8);
	}
}

/*
 * AllocSetFree
 *		Frees allocated memory; memory is removed from the set.
 */
static void
AllocSetFree(MemoryContext context, void *pointer)
{
	AllocSetFreeImpl(context, pointer, false);
}

/*
 * AllocSetFreeHeader
 *		Frees an allocated memory that was hosting a shared header
 *		for other chunks
 */
static void
AllocSetFreeHeader(MemoryContext context, void *pointer)
{
	AllocSetFreeImpl(context, pointer, true);
}
/*
 * AllocSetRealloc
 *		Returns new pointer to allocated memory of given size; this memory
 *		is added to the set.  Memory associated with given pointer is copied
 *		into the new memory, and the old memory is freed.
 *
 * Without MEMORY_CONTEXT_CHECKING, we don't know the old request size.  This
 * makes our Valgrind client requests less-precise, hazarding false negatives.
 * (In principle, we could use VALGRIND_GET_VBITS() to rediscover the old
 * request size.)
 */
static void *
AllocSetRealloc(MemoryContext context, void *pointer, Size size)
{
	AllocSet	set = (AllocSet) context;
	AllocChunk	chunk = AllocPointerGetChunk(pointer);
	Size		oldsize = chunk->size;
	Assert(chunk->size >= 8);
#ifdef USE_ASSERT_CHECKING
	if (IsUnderPostmaster  && context != ErrorContext && mainthread() != 0 && !pthread_equal(main_tid, pthread_self()))
	{
#if defined(__darwin__)
		elog(ERROR,"prealloc called from thread (OS-X pthread_sigmask is broken: MPP-4923)");
#else
		elog(ERROR,"prealloc called from thread");
#endif
	}
#endif
#ifdef MEMORY_CONTEXT_CHECKING
	VALGRIND_MAKE_MEM_DEFINED(&chunk->requested_size,
							  sizeof(chunk->requested_size));
	/* Test for someone scribbling on unused space in chunk */
	if (chunk->requested_size < oldsize)
	{
		if (!sentinel_ok(pointer, chunk->requested_size))
			elog(WARNING, "detected write past chunk end in %s %p (%s:%d)",
				 set->header.name, chunk, CDB_MCXT_WHERE(&set->header));
	}
#endif

	/*
	 * Chunk sizes are aligned to power of 2 in AllocSetAlloc(). Maybe the
	 * allocated area already is >= the new size.  (In particular, we always
	 * fall out here if the requested size is a decrease.)
	 */
	if (oldsize >= size)
	{
		/* isHeader is set to false as we should never require realloc for shared header */
		AllocFreeInfo(set, chunk, false);

#ifdef MEMORY_CONTEXT_CHECKING
		Size		oldrequest = chunk->requested_size;

#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* We can only fill the extra space if we know the prior request */
		if (size > oldrequest)
			randomize_mem((char *) pointer + oldrequest,
						  size - oldrequest);
#endif

		chunk->requested_size = size;
		VALGRIND_MAKE_MEM_NOACCESS(&chunk->requested_size,
								   sizeof(chunk->requested_size));

		/*
		 * If this is an increase, mark any newly-available part UNDEFINED.
		 * Otherwise, mark the obsolete part NOACCESS.
		 */
		if (size > oldrequest)
			VALGRIND_MAKE_MEM_UNDEFINED((char *) pointer + oldrequest,
										size - oldrequest);
		else
			VALGRIND_MAKE_MEM_NOACCESS((char *) pointer + size,
									   oldsize - size);

		/* set mark to catch clobber of "unused" space */
		if (size < oldsize)
			set_sentinel(pointer, size);

#else							/* !MEMORY_CONTEXT_CHECKING */

		/*
		 * We don't have the information to determine whether we're growing
		 * the old request or shrinking it, so we conservatively mark the
		 * entire new allocation DEFINED.
		 */
		VALGRIND_MAKE_MEM_NOACCESS(pointer, oldsize);
		VALGRIND_MAKE_MEM_DEFINED(pointer, size);
#endif

		/* isHeader is set to false as we should never require realloc for shared header */
		AllocAllocInfo(set, chunk, false);

		return pointer;
	}

	if (oldsize > set->allocChunkLimit)
	{
		/*
		 * The chunk must have been allocated as a single-chunk block.  Use
		 * realloc() to make the containing block bigger with minimum space
		 * wastage.
		 */
		AllocBlock	block = (AllocBlock) (((char *) chunk) - ALLOC_BLOCKHDRSZ);
		Size		chksize;
		Size		blksize;
		Size        oldblksize;

		/*
		 * Try to verify that we have a sane block pointer: it should
		 * reference the correct aset, and freeptr and endptr should point
		 * just past the chunk.
		 */
		if (block->aset != set ||
			block->freeptr != UserPtr_GetEndPtr(block) ||
			block->freeptr != ((char *) block) +
			(chunk->size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ))
			elog(ERROR, "could not find block containing chunk %p", chunk);

		/* isHeader is set to false as we should never require realloc for shared header */
		AllocFreeInfo(set, chunk, false);

		/* Do the realloc */
		oldblksize = UserPtr_GetUserPtrSize(block);
		chksize = MAXALIGN(size);
		blksize = chksize + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
		block = (AllocBlock) gp_realloc(block, blksize);
		if (block == NULL)
		{
			/* we need to set chunk info back*/
			AllocAllocInfo(set, chunk, false);
			MemoryContextError(ERRCODE_OUT_OF_MEMORY,
							   &set->header, CDB_MCXT_WHERE(&set->header),
							   "Out of memory.  Failed on request of size %zu bytes.",
							   size);
		}
		block->freeptr = UserPtr_GetEndPtr(block);

		/* Update pointers since block has likely been moved */
		chunk = (AllocChunk) (((char *) block) + ALLOC_BLOCKHDRSZ);
		pointer = AllocChunkGetPointer(chunk);
		if (block->prev)
			block->prev->next = block;
		else
			set->blocks = block;
		if (block->next)
			block->next->prev = block;
		chunk->size = chksize;
		Assert(chunk->size >= 8);
#ifdef MEMORY_CONTEXT_CHECKING
#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* We can only fill the extra space if we know the prior request */
		randomize_mem((char *) pointer + chunk->requested_size,
					  size - chunk->requested_size);
#endif

		/*
		 * realloc() (or randomize_mem()) will have left the newly-allocated
		 * part UNDEFINED, but we may need to adjust trailing bytes from the
		 * old allocation.
		 */
		VALGRIND_MAKE_MEM_UNDEFINED((char *) pointer + chunk->requested_size,
									oldsize - chunk->requested_size);

		chunk->requested_size = size;
		VALGRIND_MAKE_MEM_NOACCESS(&chunk->requested_size,
								   sizeof(chunk->requested_size));

		/* set mark to catch clobber of "unused" space */
		if (size < chunk->size)
			set_sentinel(pointer, size);
#else							/* !MEMORY_CONTEXT_CHECKING */

		/*
		 * We don't know how much of the old chunk size was the actual
		 * allocation; it could have been as small as one byte.  We have to be
		 * conservative and just mark the entire old portion DEFINED.
		 */
		VALGRIND_MAKE_MEM_DEFINED(pointer, oldsize);
#endif

		/* Make any trailing alignment padding NOACCESS. */
		VALGRIND_MAKE_MEM_NOACCESS((char *) pointer + size, chksize - size);

		AllocAllocInfo(set, chunk, false /* We should never require realloc for shared header */);

		MemoryContextNoteAlloc(&set->header, blksize - oldblksize); /*CDB*/
		return pointer;
	}
	else
	{
		/*
		 * Small-chunk case.  We just do this by brute force, ie, allocate a
		 * new chunk and copy the data.  Since we know the existing data isn't
		 * huge, this won't involve any great memcpy expense, so it's not
		 * worth being smarter.  (At one time we tried to avoid memcpy when it
		 * was possible to enlarge the chunk in-place, but that turns out to
		 * misbehave unpleasantly for repeated cycles of
		 * palloc/repalloc/pfree: the eventually freed chunks go into the
		 * wrong freelist for the next initial palloc request, and so we leak
		 * memory indefinitely.  See pgsql-hackers archives for 2007-08-11.)
		 */
		AllocPointer newPointer;

		/*
		 * We do not call AllocAllocInfo() or AllocFreeInfo() in this case.
		 * The corresponding AllocSetAlloc() and AllocSetFree() take care
		 * of updating the memory accounting.
		 */

		/* allocate new chunk */
		newPointer = AllocSetAlloc((MemoryContext) set, size);

		/*
		 * AllocSetAlloc() just made the region NOACCESS.  Change it to
		 * UNDEFINED for the moment; memcpy() will then transfer definedness
		 * from the old allocation to the new.  If we know the old allocation,
		 * copy just that much.  Otherwise, make the entire old chunk defined
		 * to avoid errors as we copy the currently-NOACCESS trailing bytes.
		 */
		VALGRIND_MAKE_MEM_UNDEFINED(newPointer, size);
#ifdef MEMORY_CONTEXT_CHECKING
		oldsize = chunk->requested_size;
#else
		VALGRIND_MAKE_MEM_DEFINED(pointer, oldsize);
#endif

		/* transfer existing data (certain to fit) */
		memcpy(newPointer, pointer, oldsize);

		/* free old chunk */
		AllocSetFree((MemoryContext) set, pointer);

		return newPointer;
	}
}

/*
 * AllocSetGetChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 */
static Size
AllocSetGetChunkSpace(MemoryContext context, void *pointer)
{
	AllocChunk	chunk = AllocPointerGetChunk(pointer);

	return chunk->size + ALLOC_CHUNKHDRSZ;
}

/*
 * AllocSetIsEmpty
 *		Is an allocset empty of any allocated space?
 */
static bool
AllocSetIsEmpty(MemoryContext context)
{
	/*
	 * For now, we say "empty" only if the context is new or just reset. We
	 * could examine the freelists to determine if all space has been freed,
	 * but it's not really worth the trouble for present uses of this
	 * functionality.
	 */
	if (context->isReset)
		return true;
	return false;
}

/*
 * AllocSet_GetStats
 *		Returns stats about memory consumption of an AllocSet.
 *
 *	Input parameters:
 *		context: the context of interest
 *
 *	Output parameters:
 *		nBlocks: number of blocks in the context
 *		nChunks: number of chunks in the context
 *
 *		currentAvailable: free space across all blocks
 *
 *		allAllocated: total bytes allocated during lifetime (including
 *		blocks that was dropped later on, e.g., freeing a large chunk
 *		in an exclusive block would drop the block)
 *
 *		allFreed: total bytes that was freed during lifetime
 *		maxHeld: maximum bytes held during lifetime
 */
static void
AllocSet_GetStats(MemoryContext context, uint64 *nBlocks, uint64 *nChunks,
		uint64 *currentAvailable, uint64 *allAllocated, uint64 *allFreed, uint64 *maxHeld)
{
	AllocSet	set = (AllocSet) context;
	AllocBlock	block;
	AllocChunk	chunk;
	int			fidx;
	uint64 currentAllocated = 0;

	*nBlocks = 0;
	*nChunks = 0;
	*currentAvailable = 0;
	*allAllocated = set->header.allBytesAlloc;
	*allFreed = set->header.allBytesFreed;
	*maxHeld = set->header.maxBytesHeld;

	/* Total space obtained from host's memory manager */
	for (block = set->blocks; block != NULL; block = block->next)
	{
		*nBlocks = *nBlocks + 1;
		currentAllocated += UserPtr_GetUserPtrSize(block);
	}

	/* Space at end of first block is available for use. */
	if (set->blocks)
	{
		*nChunks = *nChunks + 1;
		*currentAvailable += (char*)UserPtr_GetEndPtr(set->blocks) - set->blocks->freeptr;
	}

	/* Freelists.  Count usable space only, not chunk headers. */
	for (fidx = 0; fidx < ALLOCSET_NUM_FREELISTS; fidx++)
	{
		for (chunk = set->freelist[fidx]; chunk != NULL;
			 chunk = (AllocChunk) chunk->sharedHeader)
		{
			*nChunks = *nChunks + 1;
			*currentAvailable += chunk->size;
		}
	}
}

#ifdef MEMORY_CONTEXT_CHECKING

/*
 * AllocSetCheck
 *		Walk through chunks and check consistency of memory.
 *
 * NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
 * find yourself in an infinite loop when trouble occurs, because this
 * routine will be entered again when elog cleanup tries to release memory!
 */
static void
AllocSetCheck(MemoryContext context)
{
	AllocSet	set = (AllocSet) context;
	char	   *name = set->header.name;
	AllocBlock	prevblock;
	AllocBlock	block;

	for (prevblock = NULL, block = set->blocks;
		 block != NULL;
		 prevblock = block, block = block->next)
	{
		char	   *bpoz = ((char *) block) + ALLOC_BLOCKHDRSZ;
		Size		blk_used = block->freeptr - bpoz;
		Size		blk_data = 0;
		long		nchunks = 0;

		/*
		 * Empty block - empty can be keeper-block only
		 */
		if (!blk_used)
		{
			if (set->keeper != block)
			{
				Assert(!"Memory Context problem");
				elog(WARNING, "problem in alloc set %s: empty block %p (%s:%d)",
					 name, block, CDB_MCXT_WHERE(&set->header));
			}
		}

		/*
		 * Check block header fields
		 */
		if (block->aset != set ||
			block->prev != prevblock ||
			block->freeptr < bpoz ||
			block->freeptr > (char *) UserPtr_GetEndPtr(block))
			elog(WARNING, "problem in alloc set %s: corrupt header in block %p",
				 name, block);

		/*
		 * Chunk walker
		 */
		while (bpoz < block->freeptr)
		{
			AllocChunk	chunk = (AllocChunk) bpoz;
			Size		chsize,
						dsize;

			chsize = chunk->size;		/* aligned chunk size */
			VALGRIND_MAKE_MEM_DEFINED(&chunk->requested_size,
									  sizeof(chunk->requested_size));
			dsize = chunk->requested_size;		/* real data */
			if (dsize > 0)		/* not on a free list */
				VALGRIND_MAKE_MEM_NOACCESS(&chunk->requested_size,
										   sizeof(chunk->requested_size));

			/*
			 * Check chunk size
			 */
			if (dsize != 0xFFFFFFFF && dsize > chsize)
			{
				Assert(!"Memory Context error");
				elog(WARNING, "problem in alloc set %s: req size > alloc size for chunk %p in block %p (%s:%d)",
					 name, chunk, block, CDB_MCXT_WHERE(&set->header));
			}

			if (chsize < (1 << ALLOC_MINBITS))
			{
				Assert(!"Memory Context Error");
				elog(WARNING, "problem in alloc set %s: bad size %zu for chunk %p in block %p (%s:%d)",
					 name, chsize, chunk, block, CDB_MCXT_WHERE(&set->header));
			}

			/* single-chunk block? */
			if (chsize > set->allocChunkLimit &&
				chsize + ALLOC_CHUNKHDRSZ != blk_used)
			{
				Assert(!"Memory context error");
				elog(WARNING, "problem in alloc set %s: bad single-chunk %p in block %p",
					 name, chunk, block);
			}

			/*
			 * If chunk is allocated, check for correct aset pointer. (If it's
			 * free, the aset is the freelist pointer, which we can't check as
			 * easily...)
			 */
			if (dsize != 0xFFFFFFFF && chunk->sharedHeader != NULL && chunk->sharedHeader->context != (void *) set)
			{
				Assert(!"Memory context error");
				elog(WARNING, "problem in alloc set %s: bogus aset link in block %p, chunk %p (%s:%d)",
					 name, block, chunk, CDB_MCXT_WHERE(&set->header));
			}

			/*
			 * Check for overwrite of "unallocated" space in chunk
			 */
			if (dsize > 0 && dsize < chsize &&
				!sentinel_ok(chunk, ALLOC_CHUNKHDRSZ + dsize))
				elog(WARNING, "problem in alloc set %s: detected write past chunk end in block %p, chunk %p (%s:%d)",
						name, block, chunk, CDB_MCXT_WHERE(&set->header));

			blk_data += chsize;
			nchunks++;

			bpoz += ALLOC_CHUNKHDRSZ + chsize;
		}

		if ((blk_data + (nchunks * ALLOC_CHUNKHDRSZ)) != blk_used)
		{
			Assert(!"Memory context error");
			elog(WARNING, "problem in alloc set %s: found inconsistent memory block %p (%s:%d)",
					name, block, CDB_MCXT_WHERE(&set->header));
		}
	}
}

#endif   /* MEMORY_CONTEXT_CHECKING */
