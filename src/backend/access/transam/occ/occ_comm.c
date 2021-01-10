#include "postgres.h"

#include "access/rwset.h"
#include "tdb/kv_struct.h"
#include "tdb/timestamp_transaction/lts_generate_key.h"
#include "storage/shmem.h"
#include "access/twophase.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "cdb/cdbtm.h"
#include "tdb/tdbkvam.h"
#include "tdb/storage_param.h"
#include "cdb/cdbdtxts.h"
#include <unistd.h>

volatile ReadWriteSetStruct *readWriteSetArray;
volatile ReadWriteSetSlot *CurrentReadWriteSetSlot = NULL;

Size slotCount = 0;
Size slotSize = 0;

HTAB *ShmemKeyXids = NULL; /* primary index hashtable for shmem */
HTAB *ShmemRts = NULL;
HTAB *ShmemFinishedGxids = NULL;

Size ReadWriteSetShmemSize(void)
{
	Size size;

	/* TODO: ensure maximum key number that
	 * stored in per transacton's read/write set */
	slotSize = sizeof(ReadWriteSetSlot);

	slotSize = add_size(slotSize, hash_estimate_size(SHMEM_READ_SET_SIZE,
											 sizeof(ShmemReadWriteSetKey)));
	slotSize = add_size(slotSize, hash_estimate_size(SHMEM_WRITE_SET_SIZE,
											 sizeof(ShmemReadWriteSetKey)));
	slotSize = MAXALIGN(slotSize);

	/* Maximum active transctions */
	slotCount = 2 * (MaxBackends + max_prepared_xacts);

	size = offsetof(ReadWriteSetStruct, keySet);
	size = add_size(size, mul_size(slotSize, slotCount));

	return MAXALIGN(size);
}

static Size ReadWriteSetWithoutHTABShmemSize(void)
{
	Size size;

	/* TODO: ensure maximum key number that
	 * stored in per transacton's read/write set */
	slotSize = sizeof(ReadWriteSetSlot);

	slotSize = MAXALIGN(slotSize);

	/* Maximum active transctions */
	slotCount = 2 * (MaxBackends + max_prepared_xacts);

	size = offsetof(ReadWriteSetStruct, keySet);
	size = add_size(size, mul_size(slotSize, slotCount));

	return MAXALIGN(size);
}

void CreateReadWriteSetArray(void)
{
	bool found;
	int i;
	char *keySet_base;

	readWriteSetArray = (ReadWriteSetStruct *)
		ShmemInitStruct("Shared Read/Write Set Buffer", ReadWriteSetWithoutHTABShmemSize(), &found);

	Assert(slotCount != 0);

	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		readWriteSetArray->numSlots = 0;
		readWriteSetArray->maxSlots = slotCount;
		readWriteSetArray->nextSlot = 0;

		readWriteSetArray->slots = (ReadWriteSetSlot *)&readWriteSetArray->keySet;

		keySet_base = (char *)&(readWriteSetArray->slots[readWriteSetArray->maxSlots]);

		for (i = 0; i < readWriteSetArray->maxSlots; i++)
		{
			ReadWriteSetSlot *tmpSlot = &readWriteSetArray->slots[i];

			tmpSlot->slotid = -1;
			tmpSlot->slotindex = i;
			tmpSlot->lower = 0;
			tmpSlot->upper = kMaxSequenceNumber;
			tmpSlot->gxid = 0;
			tmpSlot->status = PENDING;
			
			HASHCTL info;
			int hash_flags;
			info.keysize = SHMEM_KEYXIDS_KEYSIZE;
			info.match = memcmp;
			info.hash = tag_hash;
			info.keycopy = memcpy;
			hash_flags = HASH_ELEM | HASH_COMPARE;
			hash_flags |= HASH_FUNCTION | HASH_KEYCOPY;
			info.entrysize = sizeof(ShmemReadWriteSetKey);
			
			char hashname[30];
			sprintf(hashname,"ShmemHashReadSet%d",i);
			tmpSlot->readSet = ShmemInitHash(hashname,
								 SHMEM_KEYXIDS_INIT_SIZE, SHMEM_READ_SET_SIZE,
								 &info, hash_flags);

			sprintf(hashname,"ShmemHashWriteSet%d",i);
			tmpSlot->writeSet = ShmemInitHash(hashname,
								 SHMEM_KEYXIDS_INIT_SIZE, SHMEM_WRITE_SET_SIZE,
								 &info, hash_flags);
		}
	}
}

Size KeyXidsHashTableShmemSize(void)
{
	Size size = 0;
	size = add_size(size, hash_estimate_size(SHMEM_KEYXIDS_SIZE,
											 sizeof(ShmemXidsEnt)));
	size = add_size(size, hash_estimate_size(SHMEM_KEYXIDS_SIZE,
											 sizeof(ShmemRtsEnt)));
	size = add_size(size, hash_estimate_size(SHMEM_KEYXIDS_SIZE,
											 sizeof(ShmemFinishedGxidEnt)));
	return MAXALIGN(size);
}

void InitKeyXidsHashTable(void)
{
	HASHCTL info;
	int hash_flags;

	info.keysize = SHMEM_KEYXIDS_KEYSIZE;
	info.entrysize = sizeof(ShmemXidsEnt);
	info.match = memcmp;
	info.hash = tag_hash;
	info.keycopy = memcpy;
	hash_flags = HASH_ELEM | HASH_COMPARE;
	hash_flags |= HASH_FUNCTION | HASH_KEYCOPY;
	ShmemKeyXids = ShmemInitHash("ShmemKeyXidsHashTable",
								 SHMEM_KEYXIDS_INIT_SIZE, SHMEM_KEYXIDS_SIZE,
								 &info, hash_flags);

	info.entrysize = sizeof(ShmemRtsEnt);
	ShmemRts = ShmemInitHash("ShmemKeyRtsHashTable",
							 SHMEM_KEYXIDS_INIT_SIZE, SHMEM_KEYXIDS_SIZE,
							 &info, hash_flags);

	HASHCTL gxidinfo;
	int gxid_hash_flags = HASH_ELEM;
	gxid_hash_flags |= HASH_FUNCTION;
	gxidinfo.hash = oid_hash;
	gxidinfo.keysize = sizeof(DistributedTransactionId);
	gxidinfo.entrysize = sizeof(ShmemFinishedGxidEnt);
	ShmemFinishedGxids = ShmemInitHash("ShmemFinishedGxidsHashTable",
									   SHMEM_KEYXIDS_INIT_SIZE, SHMEM_KEYXIDS_SIZE,
									   &gxidinfo, gxid_hash_flags);
}

char *
ReadWriteSetDump(void)
{
	StringInfoData str;
	volatile ReadWriteSetStruct *arrayP = readWriteSetArray;
	initStringInfo(&str);

	appendStringInfo(&str, "ReadWriteSet Slot Dump: currSlots: %d maxSlots: %d ",
					 arrayP->numSlots, arrayP->maxSlots);

	/* TODO: missing some detail infomation */

	return str.data;
}

ReadWriteSetSlot* FindTargetTxn(DistributedTransactionId id) 
{
	for (int i = 0; i < readWriteSetArray->numSlots; i++)
	{
		ReadWriteSetSlot *tmpSlot = &readWriteSetArray->slots[i];
		if (id == tmpSlot->gxid)
			return tmpSlot;
	}
	return NULL;
}

int FindTargetTxnID(DistributedTransactionId id) 
{
	for (int i = 0; i < readWriteSetArray->numSlots; i++)
	{
		ReadWriteSetSlot *tmpSlot = &readWriteSetArray->slots[i];
		if (id == tmpSlot->gxid)
			return i;
	}
	return -1;
}

/* Assign a read-write set to a new transaction */
void ReadWriteSetSlotAdd(char *creatorDescription, int32 slotId)
{
	ReadWriteSetSlot *slot;
	volatile ReadWriteSetStruct *arrayP = readWriteSetArray;
	int nextSlot = -1;
	int i;
	int retryCount = 10; /* TODO: set in cdb/cdbvars.c */
	if (!LWLockHeldByMe(ReadWriteSetArrayLock))
		LWLockAcquire(ReadWriteSetArrayLock, LW_EXCLUSIVE);
	slot = NULL;
	for (i = 0; i < arrayP->maxSlots; i++)
	{
		ReadWriteSetSlot *testSlot = &arrayP->slots[i];

		if (testSlot->slotindex > arrayP->maxSlots)
			elog(ERROR, "Read Write Set Array appears corrupted: %s", ReadWriteSetDump());

		if (testSlot->slotid == slotId)
		{
			slot = testSlot;
			break;
		}
	}

	if (slot != NULL)
	{
		if (LWLockHeldByMe(ReadWriteSetArrayLock))
			LWLockRelease(ReadWriteSetArrayLock);
		elog(DEBUG1, "ReadWriteSetSlotAdd: found existing entry for our session-id. id %d retry %d pid %u", slotId, retryCount, (int)slot->pid);
		return;
	}

	if (arrayP->numSlots >= arrayP->maxSlots || arrayP->nextSlot == -1)
	{
		if (LWLockHeldByMe(ReadWriteSetArrayLock))
			LWLockRelease(ReadWriteSetArrayLock);
		/*
		 * Ooops, no room.  this shouldn't happen as something else should have
		 * complained if we go over MaxBackends.
		 */
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("sorry, too many clients already."),
				 errdetail("There are no more available slots in the readWriteSetArray."),
				 errhint("Another piece of code should have detected that we have too many clients."
						 " this probably means that someone isn't releasing their slot properly.")));
	}

	slot = &arrayP->slots[arrayP->nextSlot];

	slot->slotindex = arrayP->nextSlot;

	/*
	 * find the next available slot
	 */
	for (i = arrayP->nextSlot + 1; i < arrayP->maxSlots; i++)
	{
		ReadWriteSetSlot *tmpSlot = &arrayP->slots[i];

		if (tmpSlot->slotid == -1)
		{
			nextSlot = i;
			break;
		}
	}

	/* mark that there isn't a nextSlot if the above loop didn't find one */
	if (nextSlot == arrayP->nextSlot)
		arrayP->nextSlot = -1;
	else
		arrayP->nextSlot = nextSlot;

	arrayP->numSlots += 1;

	/* initialize some things */
	slot->slotid = slotId;
	
	slot->gxid = 0;
	slot->pid = MyProcPid;
	
	if (global_tmp_timestamp.lts_low != 0)
		slot->lower = global_tmp_timestamp.lts_low;
	else
		slot->lower = 0;
	if (global_tmp_timestamp.lts_high != 0 &&
		global_tmp_timestamp.lts_high != kMaxSequenceNumber)
		slot->upper = global_tmp_timestamp.lts_high;
	else
		slot->upper = kMaxSequenceNumber;
	slot->status = RUNNING;

	if (slot == NULL)
	{
		ereport(ERROR,
				(errmsg("%s could not set the ReadWriteSet Slot!",
						creatorDescription),
				 errdetail("Tried to set the read/write set slot with id: %d "
						   "and failed. ReadWriteSet dump: %s",
						   slotId,
						   ReadWriteSetDump())));
	}

	CurrentReadWriteSetSlot = slot;

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "%s added ReadWriteSet slot for gp_session_id = %d (address %p)",
		 creatorDescription, slotId, CurrentReadWriteSetSlot);
	if (LWLockHeldByMe(ReadWriteSetArrayLock))
		LWLockRelease(ReadWriteSetArrayLock);
}

void AppendKeyToReadWriteSet(Size len, char *data, CmdType type)
{
	if (!CurrentReadWriteSetSlot || !type || len == 0)
		return;

	TupleKeySlice rawkey = {(TupleKey)data, len};
	Size rawlen = len;
	volatile ReadWriteSetSlot *rwset = CurrentReadWriteSetSlot;

	if (!rwset->gxid)
		rwset->gxid = getDistributedTransactionId();

	char *rts_data = palloc0(SHMEM_KEYXIDS_KEYSIZE);
	memcpy(rts_data, rawkey.data, rawlen);

	if (type == CMD_SELECT)
	{
		Assert(len <= SHMEM_KEYXIDS_KEYSIZE);
		ShmemReadWriteSetKey *readkey;
		bool found;
		readkey = (ShmemReadWriteSetKey *)hash_search(CurrentReadWriteSetSlot->readSet, 
													  rts_data, HASH_ENTER_NULL, &found);
		if (readkey != NULL)
		{
			if (!found)
			{
				memcpy(readkey->key, rawkey.data, rawlen);
				readkey->keyLen = rawlen;
			}
		}
	}
	else if (type == CMD_UPDATE)
	{
		Assert(len <= SHMEM_KEYXIDS_KEYSIZE);
		ShmemReadWriteSetKey *readkey;
		bool found;
		readkey = (ShmemReadWriteSetKey *)hash_search(CurrentReadWriteSetSlot->readSet, 
													  rts_data, HASH_ENTER_NULL, &found);
		if (readkey != NULL)
		{
			if (!found)
			{
				memcpy(readkey->key, rawkey.data, rawlen);
				readkey->keyLen = rawlen;
			}
		}
		Assert(len <= SHMEM_KEYXIDS_KEYSIZE);
		ShmemReadWriteSetKey *writekey;
		writekey = (ShmemReadWriteSetKey *)hash_search(CurrentReadWriteSetSlot->writeSet, 
													   rts_data, HASH_ENTER_NULL, &found);
		if (writekey != NULL)
		{
			if (!found)
			{
				memcpy(writekey->key, rawkey.data, rawlen);
				writekey->keyLen = rawlen;
			}
		}
	}
}

static void BoccReadWriteSetSlotRemove()
{
	volatile ReadWriteSetSlot *slot = CurrentReadWriteSetSlot;
	int slotId = slot->slotid;

	ShmemFinishedGxidEnt *result;
	bool found;
	if (!LWLockHeldByMe(FinishedGxidsLock))
		LWLockAcquire(FinishedGxidsLock, LW_EXCLUSIVE);
	if (slot->gxid != 0)
	{
		// TODO: replace 'ShmemFinishedGxids' with queue
		DistributedTransactionId gxid = slot->gxid;
		result = (ShmemFinishedGxidEnt *)hash_search(ShmemFinishedGxids, &gxid, HASH_ENTER_NULL, &found);
		if (result)
		{
			if (!found)
			{
				result->gxid = slot->gxid;
				HASH_SEQ_STATUS status; 

				hash_seq_init(&status, slot->writeSet);
				ShmemReadWriteSetKey *writekey;
				result->writeSetCount = 0;
				result->writeSetLen = 0;
				while ((writekey = (ShmemReadWriteSetKey*)hash_seq_search(&status)) != NULL)
				{
					Size len = 0;
					memcpy(result->writeSet + result->writeSetLen, &writekey->keyLen, sizeof(Size));
					result->writeSetLen += sizeof(Size);
					memcpy(result->writeSet + result->writeSetLen, writekey->key, len);
					result->writeSetCount ++;
					result->writeSetLen += len;
				}
			}
		}
		else
		{
			elog(DEBUG5, "No space in shared memory now. Please restart the server");
			if (LWLockHeldByMe(FinishedGxidsLock))
				LWLockRelease(FinishedGxidsLock);
		}
	}

	DistributedTransactionId current_min = kMaxSequenceNumber;
	for (int i = 0; i < readWriteSetArray->numSlots; i++)
	{
		ReadWriteSetSlot *tmpSlot = &readWriteSetArray->slots[i];
		if (current_min > tmpSlot->gxid)
			current_min = tmpSlot->gxid;
	}

	HASH_SEQ_STATUS status; 
	hash_seq_init(&status, ShmemFinishedGxids);
	ShmemFinishedGxidEnt *finishgxid;
	while ((finishgxid = (ShmemFinishedGxidEnt*)hash_seq_search(&status)) != NULL)
	{
		if (finishgxid->gxid < current_min)
		{
			bool found;
			DistributedTransactionId removegxid = finishgxid->gxid;
			hash_search(ShmemFinishedGxids, &removegxid, HASH_REMOVE, &found);
		}
	}
	if (LWLockHeldByMe(FinishedGxidsLock))
		LWLockRelease(FinishedGxidsLock);
}
#if 0
void ReadWriteSetSlotRemove(char *creatorDescription)
{
	if (!CurrentReadWriteSetSlot)
		return;

	volatile ReadWriteSetSlot *slot = CurrentReadWriteSetSlot;
	int slotId = slot->slotid;

	if (transam_mode == TRANSAM_MODE_BOCC)
		BoccReadWriteSetSlotRemove();
	if (!LWLockHeldByMe(ReadWriteSetArrayLock))
		LWLockAcquire(ReadWriteSetArrayLock, LW_EXCLUSIVE);
	/**
	 * Determine if we need to modify the next available slot to use. 
	 * We only do this is our slotindex is lower then the existing one.
	 */
	if (readWriteSetArray->nextSlot == -1 || slot->slotindex < readWriteSetArray->nextSlot)
	{
		if (slot->slotindex > readWriteSetArray->maxSlots)
			elog(ERROR, "Read Write set slot has a bogus slotindex: %d. slot array dump: %s",
				 slot->slotindex, ReadWriteSetDump());

		readWriteSetArray->nextSlot = slot->slotindex;
	}

	/* reset the slotid which marks it as being unused. */
	slot->slotid = -1;
	slot->gxid = 0;
	slot->pid = 0;
	slot->lower = 0;
	slot->upper = kMaxSequenceNumber;
	slot->status = PENDING;
	clean_up_hash(slot->readSet);
	clean_up_hash(slot->writeSet);
	readWriteSetArray->numSlots -= 1;

	if (LWLockHeldByMe(ReadWriteSetArrayLock))
		LWLockRelease(ReadWriteSetArrayLock);

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "ReadWriteSetSlot removed slot for slotId = %d, creator = %s (address %p)",
		 slotId, creatorDescription, CurrentReadWriteSetSlot);
}
#else
void ReadWriteSetSlotRemove(char *creatorDescription)
{
	if (!CurrentReadWriteSetSlot)
		return;

	volatile ReadWriteSetSlot *slot = CurrentReadWriteSetSlot;
	int slotId = slot->slotid;

	if (transam_mode == TRANSAM_MODE_BOCC)
		BoccReadWriteSetSlotRemove();
	if (!LWLockHeldByMe(ReadWriteSetArrayLock))
		LWLockAcquire(ReadWriteSetArrayLock, LW_EXCLUSIVE);
	/**
	 * Determine if we need to modify the next available slot to use. 
	 * We only do this is our slotindex is lower then the existing one.
	 */
	if (readWriteSetArray->nextSlot == -1 || slot->slotindex < readWriteSetArray->nextSlot)
	{
		if (slot->slotindex > readWriteSetArray->maxSlots)
			elog(ERROR, "Read Write set slot has a bogus slotindex: %d. slot array dump: %s",
				 slot->slotindex, ReadWriteSetDump());

		readWriteSetArray->nextSlot = slot->slotindex;
	}

	/* reset the slotid which marks it as being unused. */
	slot->slotid = -1;
	slot->gxid = 0;
	slot->pid = 0;
	slot->lower = 0;
	slot->upper = kMaxSequenceNumber;
	slot->status = PENDING;
	readWriteSetArray->numSlots -= 1;

	if (LWLockHeldByMe(ReadWriteSetArrayLock))
		LWLockRelease(ReadWriteSetArrayLock);

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "ReadWriteSetSlot removed slot for slotId = %d, creator = %s (address %p)",
		 slotId, creatorDescription, CurrentReadWriteSetSlot);
}
#endif
void AppendReadXidWithKey(Size len, char *data, CmdType type)
{
	if (!ShmemKeyXids || !ShmemRts || !type || len == 0)
		return;

	char *key_data = palloc0(SHMEM_KEYXIDS_KEYSIZE);
	memcpy(key_data, data, len);

	if (type == CMD_SELECT || type == CMD_UPDATE)
	{
		bool found;
ptxn_retry:
		if (!LWLockHeldByMe(KeyXidsHashTableLock))
			LWLockAcquire(KeyXidsHashTableLock, LW_EXCLUSIVE);
		/**
		 * append current gxid into read txn of this key
		 */
		ShmemXidsEnt *result;
		result = (ShmemXidsEnt *)hash_search(ShmemKeyXids, key_data, HASH_ENTER_NULL, &found);

		if (result == NULL)
		{
			if (LWLockHeldByMe(KeyXidsHashTableLock))
				LWLockRelease(KeyXidsHashTableLock);
			elog(DEBUG5, "No space in shared memory now. Please restart the server");
			return;
		}
		if (!found)
		{
			memcpy(result->key, key_data, len);
			result->keyLen = len;
			result->ptr = 0;
			result->writeTxn = 0;
		}
		volatile ReadWriteSetSlot *rwset = CurrentReadWriteSetSlot;
		if (!rwset->gxid)
			rwset->gxid = getDistributedTransactionId();
		if (result->ptr < SHMEM_READXIDS_SIZE)
		{
			result->readTxn[result->ptr] = rwset->gxid;
			result->ptr++;
		}
		/* if current txn is pessimistic txn, we need write the*/
		if (type == CMD_UPDATE && transaction_op_type == TRANSACTION_TYPE_P)
		{
			if (!result->writeTxn || result->writeTxn == rwset->gxid)
				result->writeTxn = rwset->gxid;
			else
			{
				if (LWLockHeldByMe(KeyXidsHashTableLock))
					LWLockRelease(KeyXidsHashTableLock);
				
				usleep(5000); //5ms
				goto ptxn_retry;
			}
		}
		// Adjust the upper of current txn < writetrx.lower
		if (result->writeTxn)
		{
			int id = FindTargetTxnID(result->writeTxn);
			if (id != -1) 
			{
				// ReadWriteSetSlot* tmpSlot = FindTargetTxn(result->writeTxn);
				ReadWriteSetSlot* tmpSlot = &readWriteSetArray->slots[id];
				if (tmpSlot->lower < rwset->upper)
				{
					rwset->upper = tmpSlot->lower;
					if (rwset->upper == 0)
						core_dump();
				}
			}
		}
		if (LWLockHeldByMe(KeyXidsHashTableLock))
			LWLockRelease(KeyXidsHashTableLock);
	}
	pfree(key_data);
}

void KeyXidsEntRemove(ShmemReadWriteSetKey* key, bool isread)
{
	if (!ShmemKeyXids)
		return;
	if (!LWLockHeldByMe(KeyXidsHashTableLock))
		LWLockAcquire(KeyXidsHashTableLock, LW_EXCLUSIVE);

	ShmemXidsEnt *result;
	bool foundPtr;
	result = (ShmemXidsEnt *)
		hash_search(ShmemKeyXids, key->key, HASH_FIND, &foundPtr);
	if (!result)
	{
		if (LWLockHeldByMe(KeyXidsHashTableLock))
		LWLockRelease(KeyXidsHashTableLock);
		return;
	}
	if (!isread && result->writeTxn == CurrentReadWriteSetSlot->gxid)
	{
		result->writeTxn = 0;
	}
	else
	{
		for (int i = 0; i < result->ptr; i++)
		{
			if (CurrentReadWriteSetSlot->gxid == result->readTxn[i])
			{
				result->readTxn[i] = result->readTxn[result->ptr - 1];
				result->readTxn[result->ptr - 1] = 0;
				result->ptr--;
				break;
			}
		}
	}
	if (LWLockHeldByMe(KeyXidsHashTableLock))
		LWLockRelease(KeyXidsHashTableLock);
}

void UpdateRts(ShmemReadWriteSetKey* readkey)
{
	HASH_SEQ_STATUS status; 
	bool found;
	if (!LWLockHeldByMe(RtsHashTableLock))
		LWLockAcquire(RtsHashTableLock, LW_EXCLUSIVE);
	ShmemRtsEnt* result = (ShmemRtsEnt *)hash_search(ShmemRts, readkey->key, HASH_ENTER_NULL, &found);

	if (result == NULL)
	{
		if (LWLockHeldByMe(RtsHashTableLock))
			LWLockRelease(RtsHashTableLock);

		hash_seq_term(&status);
		return;
	}
	if (!found)
	{
		memcpy(result->key, readkey->key, SHMEM_KEYXIDS_KEYSIZE);
		result->keyLen = readkey->keyLen;
		result->rts = readkey->lts;
	}

	if (result->rts < CurrentReadWriteSetSlot->lower)
		result->rts = CurrentReadWriteSetSlot->lower;
	if (LWLockHeldByMe(RtsHashTableLock))
		LWLockRelease(RtsHashTableLock);
}

bool CheckLogicalTsIntervalValid(uint64 lower, uint64 upper)
{
	if (!CurrentReadWriteSetSlot)
		return false;

	volatile ReadWriteSetSlot *rwset = CurrentReadWriteSetSlot;

	if (upper != 0 && rwset->upper > upper)
		rwset->upper = upper;

	if (lower != 0 && rwset->lower < lower)
		rwset->lower = lower;

	if (rwset->lower < rwset->upper)
		return true;
	else
	{
		rwset->status = ABORTED;
		return false;
	}
}
