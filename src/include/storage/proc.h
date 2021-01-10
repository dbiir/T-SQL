/*-------------------------------------------------------------------------
 *
 * proc.h
 *	  per-process shared memory data structures
 *
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/proc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PROC_H_
#define _PROC_H_

#include "access/xlogdefs.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/spin.h"
#include "storage/pg_sema.h"
#include "utils/timestamp.h"
#include "access/xlog.h"

#include "cdb/cdblocaldistribxact.h"  /* LocalDistribXactData */
#include "cdb/cdbtm.h"  /* TMGXACT */

/*
 * Each backend advertises up to PGPROC_MAX_CACHED_SUBXIDS TransactionIds
 * for non-aborted subtransactions of its current top transaction.  These
 * have to be treated as running XIDs by other backends.
 *
 * We also keep track of whether the cache overflowed (ie, the transaction has
 * generated at least one subtransaction that didn't fit in the cache).
 * If none of the caches have overflowed, we can assume that an XID that's not
 * listed anywhere in the PGPROC array is not a running transaction.  Else we
 * have to look at pg_subtrans.
 */
#define PGPROC_MAX_CACHED_SUBXIDS 64	/* XXX guessed-at value */

struct XidCache
{
	TransactionId xids[PGPROC_MAX_CACHED_SUBXIDS];
};

/* Flags for PGXACT->vacuumFlags */
#define		PROC_IS_AUTOVACUUM	0x01	/* is it an autovac worker? */
#if 0 /* Upstream code not applicable to GPDB */
#define		PROC_IN_VACUUM		0x02	/* currently running lazy vacuum */
#endif
#define		PROC_IN_ANALYZE		0x04	/* currently running analyze */
#define		PROC_VACUUM_FOR_WRAPAROUND	0x08	/* set by autovac only */
#define		PROC_IN_LOGICAL_DECODING	0x10	/* currently doing logical
												 * decoding outside xact */

/* flags reset at EOXact */
#define		PROC_VACUUM_STATE_MASK \
	(/* PROC_IN_VACUUM | */ PROC_IN_ANALYZE | PROC_VACUUM_FOR_WRAPAROUND)

/*
 * We allow a small number of "weak" relation locks (AccesShareLock,
 * RowShareLock, RowExclusiveLock) to be recorded in the PGPROC structure
 * rather than the main lock table.  This eases contention on the lock
 * manager LWLocks.  See storage/lmgr/README for additional details.
 */
#define		FP_LOCK_SLOTS_PER_BACKEND 16

/*
 * Each backend has a PGPROC struct in shared memory.  There is also a list of
 * currently-unused PGPROC structs that will be reallocated to new backends.
 *
 * links: list link for any list the PGPROC is in.  When waiting for a lock,
 * the PGPROC is linked into that lock's waitProcs queue.  A recycled PGPROC
 * is linked into ProcGlobal's freeProcs list.
 *
 * Note: twophase.c also sets up a dummy PGPROC struct for each currently
 * prepared transaction.  These PGPROCs appear in the ProcArray data structure
 * so that the prepared transactions appear to be still running and are
 * correctly shown as holding locks.  A prepared transaction PGPROC can be
 * distinguished from a real one at need by the fact that it has pid == 0.
 * The semaphore and lock-activity fields in a prepared-xact PGPROC are unused,
 * but its myProcLocks[] lists are valid.
 */
struct PGPROC
{
	/* proc->links MUST BE FIRST IN STRUCT (see ProcSleep,ProcWakeup,etc) */
	SHM_QUEUE	links;			/* list link if process is in a list */

	PGSemaphoreData sem;		/* ONE semaphore to sleep on */
	int			waitStatus;		/* STATUS_WAITING, STATUS_OK or STATUS_ERROR */

	Latch		procLatch;		/* generic latch for process */

	LocalTransactionId lxid;	/* local id of top-level transaction currently
								 * being executed by this proc, if running;
								 * else InvalidLocalTransactionId */

	/*
	 * Distributed transaction information. This is only maintained on QE's
	 * and accessed by the backend itself, so this doesn't need to be
	 * protected by any lock. On QD currentGXact provides this info, hence
	 * redundant info is not maintained here for QD. In fact, it could be just
	 * a global variable in backend-private memory, but it seems useful to
	 * have this information available for debugging purposes.
	 */
	LocalDistribXactData localDistribXactData;

	int			pid;			/* Backend's process ID; 0 if prepared xact */
	int			pgprocno;

	/* These fields are zero while a backend is still starting up: */
	BackendId	backendId;		/* This backend's backend ID (if assigned) */
	Oid			databaseId;		/* OID of database this backend is using */
	Oid			roleId;			/* OID of role using this backend */
    int         mppSessionId;   /* serial num of the qDisp process */
    int         mppLocalProcessSerial;  /* this backend's PGPROC serial num */
    bool		mppIsWriter;	/* The writer gang member, holder of locks */

	/*
	 * While in hot standby mode, shows that a conflict signal has been sent
	 * for the current transaction. Set/cleared while holding ProcArrayLock,
	 * though not required. Accessed without lock, if needed.
	 */
	bool		recoveryConflictPending;

	/* Info about LWLock the process is currently waiting for, if any. */
	bool		lwWaiting;		/* true if waiting for an LW lock */
	uint8		lwWaitMode;		/* lwlock mode being waited for */
	struct PGPROC *lwWaitLink;	/* next waiter for same LW lock */

	/* Info about lock the process is currently waiting for, if any. */
	/* waitLock and waitProcLock are NULL if not currently waiting. */
	LOCK	   *waitLock;		/* Lock object we're sleeping on ... */
	PROCLOCK   *waitProcLock;	/* Per-holder info for awaited lock */
	LOCKMODE	waitLockMode;	/* type of lock we're waiting for */
	LOCKMASK	heldLocks;		/* bitmask for lock types already held on this
								 * lock object by this backend */

	/*
	 * Info to allow us to wait for synchronous replication, if needed.
	 * waitLSN is InvalidXLogRecPtr if not waiting; set only by user backend.
	 * syncRepState must not be touched except by owning process or WALSender.
	 * syncRepLinks used only while holding SyncRepLock.
	 */
	XLogRecPtr	waitLSN;		/* waiting for this LSN or higher */
	int			syncRepState;	/* wait state for sync rep */
	SHM_QUEUE	syncRepLinks;	/* list link if process is in syncrep queue */

	/*
	 * All PROCLOCK objects for locks held or awaited by this backend are
	 * linked into one of these lists, according to the partition number of
	 * their lock.
	 */
	SHM_QUEUE	myProcLocks[NUM_LOCK_PARTITIONS];

	struct XidCache subxids;	/* cache for subtransaction XIDs */

	/*
	 * Info for Resource Scheduling, what portal (i.e statement) we might
	 * be waiting on.
	 */
	uint32		waitPortalId;	/* portal id we are waiting on */

	/*
	 * Information for our combocid-map (populated in writer/dispatcher backends only)
	 */
	uint32		combocid_map_count; /* how many entries in the map ? */

	/*
	 * Current command_id for the running query
	 * This counter is not dead code although there is no consumer in the gpdb
	 * code tree, it is required by external monitoring infrastructure.
	 * As a monitoring approach, each query execution is assigned with a unique
	 * ID. The queryCommandId is part of the ID. Monitoring extension with
	 * shared memory access can use queryCommandId to map query execution with
	 * a backend entity to access related metrics information.
	 */
	int			queryCommandId;

	bool serializableIsoLevel; /* true if proc has serializable isolation level set */

	/*
	 * Information for resource group
	 */
	void		*resSlot;	/* the resource group slot granted.
   							 * NULL indicates the resource group is
							 * locked for drop. */

	/* Per-backend LWLock.  Protects fields below. */
	LWLock	   *backendLock;	/* protects the fields below */

	/* Lock manager data, recording fast-path locks taken by this backend. */
	uint64		fpLockBits;		/* lock modes held for each fast-path slot */
	uint64		fpHoldTillEndXactBits;	/* HoldTillEndXactBits for each slot */
	Oid			fpRelId[FP_LOCK_SLOTS_PER_BACKEND];		/* slots for rel oids */
	bool		fpVXIDLock;		/* are we holding a fast-path VXID lock? */
	LocalTransactionId fpLocalTransactionId;	/* lxid for fast-path VXID
												 * lock */
};

/* NOTE: "typedef struct PGPROC PGPROC" appears in storage/lock.h. */


extern PGDLLIMPORT PGPROC *MyProc;
extern PGDLLIMPORT struct PGXACT *MyPgXact;
extern PGDLLIMPORT struct TMGXACT *MyTmGxact;

/* Special for MPP reader gangs */
extern PGDLLIMPORT PGPROC *lockHolderProcPtr;

/*
 * Prior to PostgreSQL 9.2, the fields below were stored as part of the
 * PGPROC.  However, benchmarking revealed that packing these particular
 * members into a separate array as tightly as possible sped up GetSnapshotData
 * considerably on systems with many CPU cores, by reducing the number of
 * cache lines needing to be fetched.  Thus, think very carefully before adding
 * anything else here.
 */
typedef struct PGXACT
{
	TransactionId xid;			/* id of top-level transaction currently being
								 * executed by this proc, if running and XID
								 * is assigned; else InvalidTransactionId */

	TransactionId xmin;			/* minimal running XID as it was when we were
								 * starting our xact, excluding LAZY VACUUM:
								 * vacuum must not remove tuples deleted by
								 * xid >= xmin ! */

	uint8		vacuumFlags;	/* vacuum-related flags, see above */
	bool		overflowed;
	bool		delayChkpt;		/* true if this proc delays checkpoint start;
								 * previously called InCommit */

	uint8		nxids;
} PGXACT;

/*
 * There is one ProcGlobal struct for the whole database cluster.
 */
typedef struct PROC_HDR
{
	/* Array of PGPROC structures (not including dummies for prepared txns) */
	PGPROC	   *allProcs;
	/* Array of PGXACT structures (not including dummies for prepared txns) */
	PGXACT	   *allPgXact;
	/* Array of TMGXACT structures (not including dummies for prepared txns) */
	TMGXACT	   *allTmGxact;
	/* Length of allProcs array */
	uint32		allProcCount;
	/* Head of list of free PGPROC structures */
	PGPROC	   *freeProcs;
	/* Head of list of autovacuum's free PGPROC structures */
	PGPROC	   *autovacFreeProcs;
	/* Head of list of bgworker free PGPROC structures */
	PGPROC	   *bgworkerFreeProcs;
	/* WALWriter process's latch */
	Latch	   *walwriterLatch;
	/* Checkpointer process's latch */
	Latch	   *checkpointerLatch;
	/* Current shared estimate of appropriate spins_per_delay value */
	int			spins_per_delay;
	/* The proc of the Startup process, since not in ProcArray */
	PGPROC	   *startupProc;
	int			startupProcPid;
	/* Buffer id of the buffer that Startup process waits for pin on, or -1 */
	int			startupBufferPinWaitBufId;

    /* Counter for assigning serial numbers to processes */
    int         mppLocalProcessCounter;
} PROC_HDR;

extern PGDLLIMPORT PROC_HDR *ProcGlobal;

extern PGPROC *PreparedXactProcs;

/*
 * We set aside some extra PGPROC structures for auxiliary processes,
 * ie things that aren't full-fledged backends but need shmem access.
 *
 * Background writer, checkpointer and WAL writer run during normal operation.
 * Startup process and WAL receiver also consume 2 slots, but WAL writer is
 * launched only after startup has exited, so we only need 4 slots.
 */
#define NUM_AUXILIARY_PROCS		4


/* configurable options */
extern PGDLLIMPORT int DeadlockTimeout;
extern int	StatementTimeout;
extern int	LockTimeout;
extern bool log_lock_waits;


/*
 * Function Prototypes
 */
extern int	ProcGlobalSemas(void);
extern Size ProcGlobalShmemSize(void);
extern void InitProcGlobal(void);
extern void InitProcess(void);
extern void InitProcessPhase2(void);
extern void InitAuxiliaryProcess(void);

extern void PublishStartupProcessInformation(void);
extern void SetStartupBufferPinWaitBufId(int bufid);
extern int	GetStartupBufferPinWaitBufId(void);

extern bool HaveNFreeProcs(int n);
extern void ProcReleaseLocks(bool isCommit);

extern void ProcQueueInit(PROC_QUEUE *queue);
extern int	ProcSleep(LOCALLOCK *locallock, LockMethod lockMethodTable);
extern PGPROC *ProcWakeup(PGPROC *proc, int waitStatus);
extern void ProcLockWakeup(LockMethod lockMethodTable, LOCK *lock);
extern void CheckDeadLock(void);
extern bool IsWaitingForLock(void);
extern void LockErrorCleanup(void);

extern void ProcWaitForSignal(void);
extern void ProcSendSignal(int pid);

extern int ResProcSleep(LOCKMODE lockmode, LOCALLOCK *locallock, void *incrementSet);

extern void ResLockWaitCancel(void);
extern bool ProcCanSetMppSessionId(void);
extern void ProcNewMppSessionId(int *newSessionId);

#endif   /* PROC_H */
