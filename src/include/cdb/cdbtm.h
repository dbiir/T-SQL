/*-------------------------------------------------------------------------
 * cdbtm.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBTM_H
#define CDBTM_H

#include "storage/lwlock.h"
#include "lib/stringinfo.h"
#include "access/xlogdefs.h"
#include "cdb/cdbdistributedsnapshot.h"
#include "cdb/cdblocaldistribxact.h"
#include "cdb/cdbdtxcontextinfo.h"
#include "cdb/cdbpublic.h"
#include "nodes/plannodes.h"
#include "storage/s_lock.h"

struct Gang;

/**
 * DTX states, used to track the state of the distributed transaction
 *   from the QD's point of view.
 */
typedef enum
{
	/**
	 * Uninitialized transaction
	 */
	DTX_STATE_NONE = 0,

	/**
	 * The distributed transaction is active and requires distributed coordination
	 *   (because it is explicit or an implicit writer transaction)
	 */
	DTX_STATE_ACTIVE_DISTRIBUTED,

	/**
	 * For one-phase optimization commit, we haven't run the commit yet
	 */
	DTX_STATE_ONE_PHASE_COMMIT,
	DTX_STATE_PERFORMING_ONE_PHASE_COMMIT,

	/**
	 * For two-phase commit, the first phase is about to run
	 */
	DTX_STATE_PREPARING,

	/**
	 * For two-phase commit, the first phase has completed
	 */
	DTX_STATE_PREPARED,
	DTX_STATE_INSERTING_COMMITTED,
	DTX_STATE_INSERTED_COMMITTED,
	DTX_STATE_FORCED_COMMITTED,
	DTX_STATE_NOTIFYING_COMMIT_PREPARED,
	DTX_STATE_INSERTING_FORGET_COMMITTED,
	DTX_STATE_INSERTED_FORGET_COMMITTED,

	/**
	 * Transaction rollback has been requested and QD is notifying all QD processes.
	 *
	 * _NO_PREPARED means that no QEs have started work on the first phase of two-phase commit.
	 */
	DTX_STATE_NOTIFYING_ABORT_NO_PREPARED,

	/**
	 * Transaction rollback has been requested and QD is notifying all QD processes.
	 *
	 * _SOME_PREPARED means that at least one QE has done the first phase of two-phase commit.
	 */
	DTX_STATE_NOTIFYING_ABORT_SOME_PREPARED,

	/**
	 * Transaction rollback has been requested and QD is notifying all QD processes.
	 *
	 * _PREPARED means that the QE processes have done the first phase of two-phase commit.
	 */
	DTX_STATE_NOTIFYING_ABORT_PREPARED,
	DTX_STATE_RETRY_COMMIT_PREPARED,
	DTX_STATE_RETRY_ABORT_PREPARED
}	DtxState;

/**
 * Transaction Management QD to QE protocol commands
 *
 */
typedef enum
{
	DTX_PROTOCOL_COMMAND_NONE = 0,

	/**
	 * Instruct the QE to go into implied writer state if not already.  This
	 *   is used when promoting a direct-dispatch transaction to a full-cluster
	 *   transaction
	 */
	DTX_PROTOCOL_COMMAND_ABORT_NO_PREPARED = 1,
	DTX_PROTOCOL_COMMAND_PREPARE,
	DTX_PROTOCOL_COMMAND_ABORT_SOME_PREPARED,
	DTX_PROTOCOL_COMMAND_COMMIT_ONEPHASE,
	DTX_PROTOCOL_COMMAND_COMMIT_PREPARED,
	/* for explicit transaction that doesn't write any xlog */
	DTX_PROTOCOL_COMMAND_COMMIT_NOT_PREPARED,
	DTX_PROTOCOL_COMMAND_ABORT_PREPARED,
	DTX_PROTOCOL_COMMAND_RETRY_COMMIT_PREPARED,
	DTX_PROTOCOL_COMMAND_RETRY_ABORT_PREPARED,
	DTX_PROTOCOL_COMMAND_RECOVERY_COMMIT_PREPARED,
	DTX_PROTOCOL_COMMAND_RECOVERY_ABORT_PREPARED,

	DTX_PROTOCOL_COMMAND_SUBTRANSACTION_BEGIN_INTERNAL,
	DTX_PROTOCOL_COMMAND_SUBTRANSACTION_ROLLBACK_INTERNAL,
	DTX_PROTOCOL_COMMAND_SUBTRANSACTION_RELEASE_INTERNAL,

	DTX_PROTOCOL_COMMAND_LAST = DTX_PROTOCOL_COMMAND_SUBTRANSACTION_RELEASE_INTERNAL
} DtxProtocolCommand;

/* DTX Context above xact.c */
typedef enum
{
	/**
	 * There is no distributed transaction.  This is the initial state and,
	 *   for utility mode connections or master-only queries, the only state.
	 *
	 * It is also the state to which the QD and QE return to between transactions.
	 */
	DTX_CONTEXT_LOCAL_ONLY,

	/**
	 * On QD: the process is currently part of a distributed
	 *    transaction.  Whether or not the transaction has been started on a QE
	 *    is not a part of the QD state -- that is tracked by assigning one of the
	 *    DTX_CONTEXT_QE* values on the QE process, and by updating the state field of the
	 *    currentGxact.
	 */
	DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE,

	/**
	 * On QD: indicates that the second-phase of the two-phase commit is being retried.
	 */
	DTX_CONTEXT_QD_RETRY_PHASE_2,

	/**
	 * TODO: how is something an Entry db?  fix this documentation
	 */
	DTX_CONTEXT_QE_ENTRY_DB_SINGLETON,

	/**
	 * On a QE that is the root of a query, this context means that the
	 *   distributed transaction was opened implicitly by a non-writing query
	 *   in an auto-commit transaction.
	 *
	 * This is essentially the same as _TWO_PHASE_IMPLICIT_WRITER except we know the
	 *   query won't dirty anything and so we don't need a two-phase commit.
	 */
	DTX_CONTEXT_QE_AUTO_COMMIT_IMPLICIT,

	/**
	 * On a QE, this context means that the distributed transaction was opened explicitly
	 *   by a BEGIN query statement
	 *
	 * Note that this state can happen even if there are no writer queries (because this
	 *   state is entered when the BEGIN statement is processed).
	 */
	DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER,

	/**
	 * On a QE that is the root of a query, this context means that the
	 *   distributed transaction was opened implicitly by a writing query
	 *   in an auto-commit transaction, and this is the writing QE process
	 *
	 * Same as _AUTO_COMMIT_IMPLICIT except that we need a two-phase commit because
	 *   of the dirty data.
	 */
	DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER,

	/**
	 * On a QE that is not the root of a query, this context is used.  This can happen
	 *   regardless of whether the overall transaction for the query is implicit or explicit,
	 *   and auto-commit vs two-phase
	 */
	DTX_CONTEXT_QE_READER,

	/**
	 * On the QE, the two-phase commit has been prepared (first phase complete).
	 */
	DTX_CONTEXT_QE_PREPARED,

	/**
	 * On the QE, the two-phase commit has been committed or aborted (second phase complete).
	 */
	DTX_CONTEXT_QE_FINISH_PREPARED
} DtxContext;



typedef struct TMGXACT_UTILITY_MODE_REDO
{
	bool		committed;
	TMGXACT_LOG	gxact_log;
}	TMGXACT_UTILITY_MODE_REDO;

typedef struct TMGXACT
{
	/*
	 * These fields will be recorded in the log.  They are the same
	 * as those in the TMGXACT_LOG struct.  We will be copying the
	 * fields individually, so they dont have to match the same order,
	 * but it a good idea.
	 */
	char						gid[TMGIDSIZE];

	/*
	 * Like PGPROC->xid to local transaction, gxid is set if distributed
	 * transaction needs two-phase, and it's reset when distributed
	 * transaction ends, with ProcArrayLock held.
	 */
	DistributedTransactionId	gxid;

	/*
	 * Memory only fields.
	 */
 	DtxState				state;

	int							sessionId;
	
	bool						explicitBeginRemembered;

	/* Used on QE, indicates the transaction applies one-phase commit protocol */
	bool						isOnePhaseCommit;

	/*
	 * This is similar to xmin of PROC, stores lowest dxid on first snapshot
	 * by process with this as currentGXact.
	 */
	DistributedTransactionId	xminDistributedSnapshot;

	bool						badPrepareGangs;

	bool						writerGangLost;

	Bitmapset					*twophaseSegmentsMap;
	List						*twophaseSegments;
}	TMGXACT;

typedef struct TMGXACTSTATUS
{
	DistributedTransactionId	gxid;
	char						gid[TMGIDSIZE];
 	DtxState					state;
	int							sessionId;
	DistributedTransactionId	xminDistributedSnapshot;
} TMGXACTSTATUS;

typedef struct TMGALLXACTSTATUS
{
	int		next;
	int		count;

	TMGXACTSTATUS		*statusArray;
} TMGALLXACTSTATUS;


typedef struct TmControlBlock
{
	DistributedTransactionTimeStamp	distribTimeStamp;
	DistributedTransactionId	seqno;
	bool						DtmStarted;
	uint32						NextSnapshotId;
	int							num_committed_xacts;

    /* Array [0..max_tm_gxacts-1] of TMGXACT_LOG ptrs is appended starting here */
	TMGXACT_LOG  			    committed_gxact_array[1];
}	TmControlBlock;


#define TMCONTROLBLOCK_BYTES(num_gxacts) \
	(offsetof(TmControlBlock, committed_gxact_array) + sizeof(TMGXACT_LOG) * (num_gxacts))

#define DTM_DEBUG3 (Debug_print_full_dtm ? LOG : DEBUG3)
#define DTM_DEBUG5 (Debug_print_full_dtm ? LOG : DEBUG5)

extern volatile bool *shmDtmStarted;
extern int max_tm_gxacts;

extern DtxContext DistributedTransactionContext;

/* state variables for how much of the log file has been flushed */
extern volatile bool *shmDtmStarted;
extern volatile DistributedTransactionTimeStamp *shmDistribTimeStamp;
extern volatile DistributedTransactionId *shmGIDSeq;
extern uint32 *shmNextSnapshotId;
extern TMGXACT_LOG *shmCommittedGxactArray;
extern volatile int *shmNumCommittedGxacts;

extern char *DtxStateToString(DtxState state);
extern char *DtxProtocolCommandToString(DtxProtocolCommand command);
extern char *DtxContextToString(DtxContext context);
extern DistributedTransactionTimeStamp getDtxStartTime(void);
extern void dtxCrackOpenGid(const char	*gid,
							DistributedTransactionTimeStamp	*distribTimeStamp,
							DistributedTransactionId		*distribXid);
extern DistributedTransactionId getDistributedTransactionId(void);
extern bool getDistributedTransactionIdentifier(char *id);

extern void initGxact(TMGXACT *gxact, bool resetXid);
extern void activeCurrentGxact(void);
extern void	prepareDtxTransaction(void);
extern bool isPreparedDtxTransaction(void);
extern void getDtxLogInfo(TMGXACT_LOG *gxact_log);
extern bool notifyCommittedDtxTransactionIsNeeded(void);
extern void notifyCommittedDtxTransaction(void);
extern void	rollbackDtxTransaction(void);

extern bool includeInCheckpointIsNeeded(TMGXACT *gxact);
extern void insertingDistributedCommitted(void);
extern void insertedDistributedCommitted(void);

extern void redoDtxCheckPoint(TMGXACT_CHECKPOINT *gxact_checkpoint);
extern void redoDistributedCommitRecord(TMGXACT_LOG *gxact_log);
extern void redoDistributedForgetCommitRecord(TMGXACT_LOG *gxact_log);

extern void setupTwoPhaseTransaction(void);
extern bool isCurrentDtxTwoPhase(void);
extern DtxState getCurrentDtxState(void);

extern void sendDtxExplicitBegin(void);
extern bool isDtxExplicitBegin(void);

extern bool dispatchDtxCommand(const char *cmd);

extern void tmShmemInit(void);
extern int	tmShmemSize(void);

extern void verify_shared_snapshot_ready(void);

int			mppTxnOptions(bool needTwoPhase);
int			mppTxOptions_IsoLevel(int txnOptions);
bool		isMppTxOptions_ReadOnly(int txnOptions);
bool		isMppTxOptions_NeedTwoPhase(int txnOptions);
bool		isMppTxOptions_ExplicitBegin(int txnOptions);

extern void getAllDistributedXactStatus(TMGALLXACTSTATUS **allDistributedXactStatus);
extern bool getNextDistributedXactStatus(TMGALLXACTSTATUS *allDistributedXactStatus, TMGXACTSTATUS **distributedXactStatus);
extern void setupRegularDtxContext (void);
extern void setupQEDtxContext (DtxContextInfo *dtxContextInfo);
extern void finishDistributedTransactionContext (char *debugCaller, bool aborted);
extern void performDtxProtocolCommand(DtxProtocolCommand dtxProtocolCommand,
					int flags,
					const char *loggingStr, const char *gid, 
					DistributedTransactionId gxid, DtxContextInfo *contextInfo);
extern void UtilityModeFindOrCreateDtmRedoFile(void);
extern void UtilityModeCloseDtmRedoFile(void);

extern bool doDispatchSubtransactionInternalCmd(DtxProtocolCommand cmdType);
extern bool doDispatchDtxProtocolCommand(DtxProtocolCommand dtxProtocolCommand, int flags,
							 char *gid, DistributedTransactionId gxid,
							 bool *badGangs, bool raiseError, List *twophaseSegments,
							 char *serializedDtxContextInfo, int serializedDtxContextInfoLen);

extern void markCurrentGxactWriterGangLost(void);

extern bool currentGxactWriterGangLost(void);

extern void addToGxactTwophaseSegments(struct Gang* gp);

extern DistributedTransactionId generateGID(void);
extern void ClearTransactionState(TransactionId latestXid);

extern int dtx_recovery_start(void);

extern void DtxRecoveryMain(Datum main_arg);
extern bool DtxRecoveryStartRule(Datum main_arg);

#endif   /* CDBTM_H */
