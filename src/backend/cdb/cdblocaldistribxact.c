/*-------------------------------------------------------------------------
 *
 * cdblocaldistribxact.c
 *
 * Maintains state of current distributed transactions on each (local)
 * database instance.  Driven by added GP code in the xact.c module.
 *
 * Also support a cache of recently seen committed transactions found by the
 * visibility routines for better performance.  Used to avoid reading the
 * distributed log SLRU files too frequently.
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdblocaldistribxact.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/transam.h"
#include "access/twophase.h"
#include "cdb/cdblocaldistribxact.h"
#include "cdb/cdbvars.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "tdb/storage_processor.h"

/*  ***************************************************************************** */

static char *
LocalDistribXactStateToString(LocalDistribXactState state)
{
	switch (state)
	{
		case LOCALDISTRIBXACT_STATE_ACTIVE:
			return "Active";

		case LOCALDISTRIBXACT_STATE_COMMITTED:
			return "Committed";

		case LOCALDISTRIBXACT_STATE_ABORTED:
			return "Aborted";

		case LOCALDISTRIBXACT_STATE_PREPARED:
			return "Prepared";

		default:
			return "Unknown";
	}
}

/*  ***************************************************************************** */
void
LocalDistribXact_ChangeState(int pgprocno,
							 LocalDistribXactState newState)
{
	PGPROC *proc = &ProcGlobal->allProcs[pgprocno];
	PGXACT *pgxact = &ProcGlobal->allPgXact[pgprocno];
	LocalDistribXactState oldState;
	DistributedTransactionId distribXid;

	Assert(proc->localDistribXactData.state != LOCALDISTRIBXACT_STATE_NONE);

	oldState = proc->localDistribXactData.state;
	distribXid = proc->localDistribXactData.distribXid;

	/*
	 * Validate current state given new state.
	 */
	switch (newState)
	{
		case LOCALDISTRIBXACT_STATE_PREPARED:
			if (oldState != LOCALDISTRIBXACT_STATE_ACTIVE)
				elog(PANIC,
					 "Expected distributed transaction xid = %u to local element to be in state \"Active\" and "
					 "found state \"%s\"",
					 distribXid,
					 LocalDistribXactStateToString(oldState));
			break;

		case LOCALDISTRIBXACT_STATE_COMMITTED:
			if (oldState != LOCALDISTRIBXACT_STATE_ACTIVE)
				elog(PANIC,
					 "Expected distributed transaction xid = %u to local element to be in state \"Active\" or \"Commit Delivery\" and "
					 "found state \"%s\"",
					 distribXid,
					 LocalDistribXactStateToString(oldState));
			break;

		case LOCALDISTRIBXACT_STATE_ABORTED:
			if (oldState != LOCALDISTRIBXACT_STATE_ACTIVE)
				elog(PANIC,
					 "Expected distributed transaction xid = %u to local element to be in state \"Active\" or \"Abort Delivery\" and "
					 "found state \"%s\"",
					 distribXid,
					 LocalDistribXactStateToString(oldState));
			break;

		case LOCALDISTRIBXACT_STATE_ACTIVE:
			elog(PANIC, "Unexpected distributed to local transaction new state: '%s'",
				 LocalDistribXactStateToString(newState));
			break;

		default:
			elog(PANIC, "Unrecognized distributed to local transaction state: %d",
				 (int) newState);
	}

	proc->localDistribXactData.state = newState;

	elog((Debug_print_full_dtm ? LOG : DEBUG5),
		 "Moved distributed transaction xid = %u (local xid = %u) from \"%s\" to \"%s\"",
		 distribXid,
		 pgxact->xid,
		 LocalDistribXactStateToString(oldState),
		 LocalDistribXactStateToString(newState));
}

#define MAX_LOCAL_DISTRIB_DISPLAY_BUFFER 100
static char LocalDistribDisplayBuffer[MAX_LOCAL_DISTRIB_DISPLAY_BUFFER];
static __thread char LocalThreadDistribDisplayBuffer[MAX_LOCAL_DISTRIB_DISPLAY_BUFFER];

char *
LocalDistribXact_DisplayString(int pgprocno)
{
	PGPROC *proc = &ProcGlobal->allProcs[pgprocno];
	PGXACT *pgxact = &ProcGlobal->allPgXact[pgprocno];
	int			snprintfResult;
	if (am_kv_storage)
	{
		snprintfResult =
			snprintf(
					LocalThreadDistribDisplayBuffer,
					MAX_LOCAL_DISTRIB_DISPLAY_BUFFER,
					"distributed transaction {timestamp %u, xid %u} for local xid %u",
					proc->localDistribXactData.distribTimeStamp,
					proc->localDistribXactData.distribXid,
					pgxact->xid);

		Assert(snprintfResult >= 0);
		Assert(snprintfResult < MAX_LOCAL_DISTRIB_DISPLAY_BUFFER);

		return LocalThreadDistribDisplayBuffer;
	}
	else
	{
		snprintfResult =
			snprintf(
					LocalDistribDisplayBuffer,
					MAX_LOCAL_DISTRIB_DISPLAY_BUFFER,
					"distributed transaction {timestamp %u, xid %u} for local xid %u",
					proc->localDistribXactData.distribTimeStamp,
					proc->localDistribXactData.distribXid,
					pgxact->xid);

		Assert(snprintfResult >= 0);
		Assert(snprintfResult < MAX_LOCAL_DISTRIB_DISPLAY_BUFFER);

		return LocalDistribDisplayBuffer;
	}
}

/*  ***************************************************************************** */

/* Memory context for long-lived local-distributed commit pairs. */
static MemoryContext LocalDistribCacheMemCxt = NULL;
static __thread MemoryContext LocalThreadDistribCacheMemCxt = NULL;

/* Hash table for the long-lived local-distributed commit pairs. */
static HTAB *LocalDistribCacheHtab;
static __thread HTAB *LocalThreadDistribCacheHtab;
/*
 * A cached local-distributed transaction pair.
 *
 * We also cache just local-only transactions, so in that case distribXid
 * will be InvalidDistributedTransactionId.
 */
typedef struct LocalDistribXactCacheEntry
{
	/*
	 * Distributed and local xids.
	 */
	TransactionId localXid;
	/* MUST BE FIRST: Hash table key. */

	DistributedTransactionId distribXid;

	int64		visits;

	dlist_node	lruDoubleLinks;
	/* list link for LRU */

} LocalDistribXactCacheEntry;

/*
 * Globals for local-distributed cache.
 */
typedef struct LocalDistribXactCache
{
	int32		count;

	dlist_head	lruDoublyLinkedHead;

	int64		hitCount;
	int64		totalCount;
	int64		addCount;
	int64		removeCount;

} LocalDistribXactCacheData;
static LocalDistribXactCacheData LocalDistribXactCache = 
			{0, DLIST_STATIC_INIT(LocalDistribXactCache.lruDoublyLinkedHead), 0, 0, 0, 0};
static __thread LocalDistribXactCacheData LocalThreadDistribXactCache = 
			{0, DLIST_STATIC_INIT(LocalDistribXactCache.lruDoublyLinkedHead), 0, 0, 0, 0};

bool
LocalDistribXactCache_CommittedFind(
									TransactionId localXid,
									DistributedTransactionTimeStamp distribTransactionTimeStamp,
									DistributedTransactionId *distribXid)
{
	LocalDistribXactCacheEntry *entry;
	bool		found;

	/* Before doing anything, see if we are enabled. */
	if (gp_max_local_distributed_cache == 0)
		return false;

	if (!am_kv_storage && LocalDistribCacheMemCxt == NULL)
	{
		HASHCTL		hash_ctl;

		/* Create the memory context where cross-transaction state is stored */
		LocalDistribCacheMemCxt = AllocSetContextCreate(TopMemoryContext,
														"Local-distributed commit cache context",
														ALLOCSET_DEFAULT_MINSIZE,
														ALLOCSET_DEFAULT_INITSIZE,
														ALLOCSET_DEFAULT_MAXSIZE);

		Assert(LocalDistribCacheHtab == NULL);

		/* Create the hashtable proper */
		MemSet(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(TransactionId);
		hash_ctl.entrysize = sizeof(LocalDistribXactCacheEntry);
		hash_ctl.hash = tag_hash;
		hash_ctl.hcxt = LocalDistribCacheMemCxt;
		LocalDistribCacheHtab = hash_create("Local-distributed commit cache",
											25, /* start small and extend */
											&hash_ctl,
											HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

		MemSet(&LocalDistribXactCache, 0, sizeof(LocalDistribXactCache));
		dlist_init(&LocalDistribXactCache.lruDoublyLinkedHead);
	}
	else if (am_kv_storage && LocalThreadDistribCacheMemCxt == NULL)
	{
		HASHCTL		hash_ctl;

		/* Create the memory context where cross-transaction state is stored */
		LocalThreadDistribCacheMemCxt = AllocSetContextCreate(TopMemoryContext,
														"Local-distributed commit cache context",
														ALLOCSET_DEFAULT_MINSIZE,
														ALLOCSET_DEFAULT_INITSIZE,
														ALLOCSET_DEFAULT_MAXSIZE);

		Assert(LocalThreadDistribCacheHtab == NULL);

		/* Create the hashtable proper */
		MemSet(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(TransactionId);
		hash_ctl.entrysize = sizeof(LocalDistribXactCacheEntry);
		hash_ctl.hash = tag_hash;
		hash_ctl.hcxt = LocalThreadDistribCacheMemCxt;
		LocalThreadDistribCacheHtab = hash_create("Local-distributed commit cache",
											25, /* start small and extend */
											&hash_ctl,
											HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

		MemSet(&LocalThreadDistribXactCache, 0, sizeof(LocalThreadDistribXactCache));
		dlist_init(&LocalThreadDistribXactCache.lruDoublyLinkedHead);

	}
	if (am_kv_storage)
		entry = (LocalDistribXactCacheEntry *) hash_search(
													   LocalThreadDistribCacheHtab,
													   &localXid,
													   HASH_FIND,
													   &found);
	else
		entry = (LocalDistribXactCacheEntry *) hash_search(
													   LocalDistribCacheHtab,
													   &localXid,
													   HASH_FIND,
													   &found);
	if (found)
	{
		/*
		 * Maintain LRU ordering.
		 */
		dlist_delete(&entry->lruDoubleLinks);
		if (am_kv_storage)
			dlist_push_head(&LocalThreadDistribXactCache.lruDoublyLinkedHead, &entry->lruDoubleLinks);
		else
			dlist_push_head(&LocalDistribXactCache.lruDoublyLinkedHead, &entry->lruDoubleLinks);

		*distribXid = entry->distribXid;

		entry->visits++;

		if (am_kv_storage)
			LocalThreadDistribXactCache.hitCount++;
		else
			LocalDistribXactCache.hitCount++;
	}
	if (am_kv_storage)
		LocalThreadDistribXactCache.totalCount++;
	else
		LocalDistribXactCache.totalCount++;

	return found;
}

void
LocalDistribXactCache_AddCommitted(
								   TransactionId localXid,
								   DistributedTransactionTimeStamp distribTransactionTimeStamp,
								   DistributedTransactionId distribXid)
{
	LocalDistribXactCacheEntry *entry;
	bool		found;

	/* Before doing anything, see if we are enabled. */
	if (gp_max_local_distributed_cache == 0)
		return;

	if (am_kv_storage)
	{
		Assert(LocalThreadDistribCacheMemCxt != NULL);
		Assert(LocalThreadDistribCacheHtab != NULL);		
	}
	else
	{
		Assert(LocalDistribCacheMemCxt != NULL);
		Assert(LocalDistribCacheHtab != NULL);
	}


	if (!am_kv_storage && LocalDistribXactCache.count >= gp_max_local_distributed_cache)
	{
		LocalDistribXactCacheEntry *lastEntry;
		LocalDistribXactCacheEntry *removedEntry;

		Assert(LocalDistribXactCache.count == gp_max_local_distributed_cache);

		/*
		 * Remove oldest.
		 */
		lastEntry = (LocalDistribXactCacheEntry *)
			dlist_container(LocalDistribXactCacheEntry,
							lruDoubleLinks,
							dlist_tail_node(&LocalDistribXactCache.lruDoublyLinkedHead));
		Assert(lastEntry != NULL);
		dlist_delete(&lastEntry->lruDoubleLinks);

		removedEntry = (LocalDistribXactCacheEntry *)
			hash_search(LocalDistribCacheHtab, &lastEntry->localXid,
						HASH_REMOVE, NULL);
		Assert(lastEntry == removedEntry);

		LocalDistribXactCache.count--;

		LocalDistribXactCache.removeCount++;
	}
	else if (am_kv_storage && LocalThreadDistribXactCache.count >= gp_max_local_distributed_cache)
	{
		LocalDistribXactCacheEntry *lastEntry;
		LocalDistribXactCacheEntry *removedEntry;

		Assert(LocalThreadDistribXactCache.count == gp_max_local_distributed_cache);

		/*
		 * Remove oldest.
		 */
		lastEntry = (LocalDistribXactCacheEntry *)
			dlist_container(LocalDistribXactCacheEntry,
							lruDoubleLinks,
							dlist_tail_node(&LocalThreadDistribXactCache.lruDoublyLinkedHead));
		Assert(lastEntry != NULL);
		dlist_delete(&lastEntry->lruDoubleLinks);

		removedEntry = (LocalDistribXactCacheEntry *)
			hash_search(LocalThreadDistribCacheHtab, &lastEntry->localXid,
						HASH_REMOVE, NULL);
		Assert(lastEntry == removedEntry);

		LocalThreadDistribXactCache.count--;

		LocalThreadDistribXactCache.removeCount++;
	}
	if (am_kv_storage)
		/* Now we can add entry to hash table */
		entry = (LocalDistribXactCacheEntry *) hash_search(
													   LocalThreadDistribCacheHtab,
													   &localXid,
													   HASH_ENTER,
													   &found);
	else
		/* Now we can add entry to hash table */
		entry = (LocalDistribXactCacheEntry *) hash_search(
													   LocalDistribCacheHtab,
													   &localXid,
													   HASH_ENTER,
													   &found);
	if (found)
	{
		elog(ERROR, "Add should not have found local xid = %x", localXid);
	}
	if (am_kv_storage)
		dlist_push_head(&LocalThreadDistribXactCache.lruDoublyLinkedHead,
					&entry->lruDoubleLinks);
	else
		dlist_push_head(&LocalDistribXactCache.lruDoublyLinkedHead,
					&entry->lruDoubleLinks);

	entry->localXid = localXid;
	entry->distribXid = distribXid;
	entry->visits = 1;

	if (am_kv_storage)
	{
		LocalThreadDistribXactCache.count++;

		LocalThreadDistribXactCache.addCount++;
	}
	else
	{
		LocalDistribXactCache.count++;

		LocalDistribXactCache.addCount++;
	}
}

void
LocalDistribXactCache_ShowStats(char *nameStr)
{
	if (am_kv_storage)
		elog(LOG, "%s: Local-distributed cache counts "
		 "(hits " INT64_FORMAT ", total " INT64_FORMAT ", adds " INT64_FORMAT ", removes " INT64_FORMAT ")",
		 nameStr,
		 LocalThreadDistribXactCache.hitCount,
		 LocalThreadDistribXactCache.totalCount,
		 LocalThreadDistribXactCache.addCount,
		 LocalThreadDistribXactCache.removeCount);
	else
		elog(LOG, "%s: Local-distributed cache counts "
		 "(hits " INT64_FORMAT ", total " INT64_FORMAT ", adds " INT64_FORMAT ", removes " INT64_FORMAT ")",
		 nameStr,
		 LocalDistribXactCache.hitCount,
		 LocalDistribXactCache.totalCount,
		 LocalDistribXactCache.addCount,
		 LocalDistribXactCache.removeCount);
}
