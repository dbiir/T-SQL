/*-------------------------------------------------------------------------
 *
 * rwset.h
 *	  read and write set maintain for optimistic concurrency control
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/rwset.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RWSET_H
#define RWSET_H
#include "nodes/nodes.h"
#include "tdb/kv_universal.h"
#include "utils/hsearch.h"
#if 0
/* maximum key size */
#define SHMEM_KEYXIDS_KEYSIZE		(256)
/* maximum count of read set*/
#define SHMEM_READ_SET_SIZE			(10240)
/* maximum count of read set*/
#define SHMEM_WRITE_SET_SIZE		(1024)
/* maximum count of read set*/
#define SHMEM_WRITE_SET_LENGTH		(1024)
/* init size of keys' hash table */
#define SHMEM_KEYXIDS_INIT_SIZE		(1024)
/* maximum size of keys' hash table */
#define SHMEM_KEYXIDS_SIZE			(5000000)
/* maximum size of keys' read txn list */
#define SHMEM_READXIDS_SIZE			(3000)
#else
/* maximum key size */
#define SHMEM_KEYXIDS_KEYSIZE		(256)
/* maximum count of read set*/
#define SHMEM_READ_SET_SIZE			(256)
/* maximum count of read set*/
#define SHMEM_WRITE_SET_SIZE		(256)
/* maximum count of read set*/
#define SHMEM_WRITE_SET_LENGTH		(256)
/* init size of keys' hash table */
#define SHMEM_KEYXIDS_INIT_SIZE		(256)
/* maximum size of keys' hash table */
#define SHMEM_KEYXIDS_SIZE			(5000)
/* maximum size of keys' read txn list */
#define SHMEM_READXIDS_SIZE			(3000)
#endif
typedef	enum
{
	PENDING,
	RUNNING,
	VALIDATED,
	COMMITTED,
	ABORTED
} ShmTransactionStatus;

typedef struct ReadWriteSetSlot
{
	int32	slotindex;
	int32	slotid;
	pid_t	pid;
	DistributedTransactionId gxid;
	DistributedTransactionId val_gxid;
	uint64	lower;
	uint64	upper;
	ShmTransactionStatus status;
	HTAB 	*readSet;
	HTAB	*writeSet;
} ReadWriteSetSlot;

typedef struct ReadWriteSetStruct
{
	int	numSlots;
	int	maxSlots;
	int	nextSlot;

	ReadWriteSetSlot	*slots;
	char	*keySet;

} ReadWriteSetStruct;

typedef struct ShmemReadWriteSetKey
{
	char	key[SHMEM_KEYXIDS_KEYSIZE];
	Size	keyLen;
	uint64	lts;
} ShmemReadWriteSetKey;

/* this is a hash bucket in the shmem key-xids hash table */
typedef struct ShmemXidsEnt
{
	char	key[SHMEM_KEYXIDS_KEYSIZE];
	Size	keyLen;
	int		ptr;
	DistributedTransactionId	readTxn[SHMEM_READXIDS_SIZE];
	DistributedTransactionId	writeTxn;
} ShmemXidsEnt;

typedef struct ShmemRtsEnt
{
	char	key[SHMEM_KEYXIDS_KEYSIZE];
	Size	keyLen;
	uint64	rts;
} ShmemRtsEnt;

typedef struct ShmemFinishedGxidEnt
{
	DistributedTransactionId gxid;
	char	writeSet[SHMEM_WRITE_SET_LENGTH];
	Size	writeSetCount;
	Size	writeSetLen;
} ShmemFinishedGxidEnt;

extern volatile ReadWriteSetStruct *readWriteSetArray;
extern volatile ReadWriteSetSlot *CurrentReadWriteSetSlot;
extern Size slotCount;
extern Size slotSize;

extern HTAB *ShmemKeyXids; /* primary index hashtable for shmem */
extern HTAB *ShmemRts;
extern HTAB *ShmemFinishedGxids;

extern Size ReadWriteSetShmemSize(void);
extern void CreateReadWriteSetArray(void);
extern Size KeyXidsHashTableShmemSize(void);
extern void InitKeyXidsHashTable(void);

extern char *ReadWriteSetDump(void);
extern ReadWriteSetSlot* FindTargetTxn(DistributedTransactionId id);
extern int FindTargetTxnID(DistributedTransactionId id);
extern void ReadWriteSetSlotAdd(char *creatorDescription, int32 slotId);

extern void AppendKeyToReadWriteSet(Size len, char *data, CmdType type);
extern void ReadWriteSetSlotRemove(char *creatorDescription);
extern void AppendReadXidWithKey(Size len, char *data, CmdType type);
extern void KeyXidsEntRemove(ShmemReadWriteSetKey* key, bool isread);

extern void UpdateRts(ShmemReadWriteSetKey* readkey);

extern bool CheckLogicalTsIntervalValid(uint64 lower, uint64 upper);

extern bool CheckTxnStatus();

extern int BoccValidation(void);
extern int FoccValidation(void);

extern uint64 LogicalCommitTsAllocation(void);
extern int DTAValidation(void);
extern void CleanUp();

typedef struct TransactionStatistics
{
	slock_t txn_mutex;
    int allTransactionNum;
	int RollBackTransactionNum;
    int CommitTransactionNum;
	int Loop;
} TransactionStatistics;

extern TransactionStatistics *transaction_statistics;
extern void TransactionStatisticsShmemInit(void);
extern Size TransactionStatisticsShmemSize(void);
extern void RollBackCountAdd(void);
extern void AllTransCountAdd(void);
extern int GetRollBackCount(void);
extern void CommitTranCountAdd(void);
#endif   /* RWSET_H */

