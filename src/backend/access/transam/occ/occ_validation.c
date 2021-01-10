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

uint64
LogicalCommitTsAllocation()
{
	if (CurrentReadWriteSetSlot->lower < CurrentReadWriteSetSlot->upper)
	{
		CurrentReadWriteSetSlot->upper = CurrentReadWriteSetSlot->lower + 1;
		return CurrentReadWriteSetSlot->lower;
	}
	else
		return 0;
}

/*
 * Backward validation.
 * Follow the definition in "On Optimistic Methods for Concurrency Control(1981)".
 * The read set of the validating transaction(Tv) cannot intersects with
 * the write set of all transaction that is committed in [Tv.start_time+1, Tv.validate_time]
 */
int BoccValidation(void)
{
	if (!CurrentReadWriteSetSlot)
		return 1;

	volatile ReadWriteSetSlot *rwset = CurrentReadWriteSetSlot;

	DistributedTransactionId current_gxid = generateGID();

	LWLockAcquire(ReadWriteSetArrayLock, LW_EXCLUSIVE);
	// TODO: Replace the hash table named as 'ShmemFinishedGxids' with queue
	HASH_SEQ_STATUS status; 
	hash_seq_init(&status, ShmemFinishedGxids);
	ShmemFinishedGxidEnt *result;
	while ((result = (ShmemFinishedGxidEnt*)hash_seq_search(&status)) != NULL)
	{
		if (result->gxid >= current_gxid)
			continue;
		if (result->gxid <= rwset->gxid)
			continue;

		Size inner_offset = 0;
		for (int k = 0; k < result->writeSetCount; k++)
		{
			Size *inner_len = (Size *)(result->writeSet + inner_offset);
			inner_offset += sizeof(Size);

			TupleKeySlice innerkey = {(TupleKey)(result->writeSet + inner_offset), *inner_len};
			Size rawlen = 0;
			TupleKey rawkey = (TupleKey)get_TupleKeySlice_primarykey_prefix_lts(innerkey, &rawlen);
			char *rts_data = palloc0(SHMEM_KEYXIDS_KEYSIZE);
			memcpy(rts_data, rawkey, rawlen);

			ShmemReadWriteSetKey *readkey;
			bool found;
			readkey = (ShmemReadWriteSetKey *)hash_search(rwset->readSet, 
													  rts_data, HASH_FIND, &found);
			pfree(rts_data);
			if (readkey != NULL && found)
			{
				LWLockRelease(ReadWriteSetArrayLock);
				return 0;
			}
			inner_offset += *inner_len;
		}

	}
	LWLockRelease(ReadWriteSetArrayLock);
	rwset->val_gxid = current_gxid;
	return 1;
}

/*
 * Forward validation.
 * Follow the definition in .
 * Check write set of Tv does not overlap with read sets of all active Transactions.
 */
int FoccValidation(void)
{
	if (!CurrentReadWriteSetSlot)
		return 1;

	volatile ReadWriteSetSlot *rwset = CurrentReadWriteSetSlot;

	LWLockAcquire(ReadWriteSetArrayLock, LW_EXCLUSIVE);

	for (int i = 0; i < readWriteSetArray->numSlots; i++)
	{
		ReadWriteSetSlot *tmpSlot = &readWriteSetArray->slots[i];

		if (tmpSlot->slotid == CurrentReadWriteSetSlot->slotid)
			continue;

		HASH_SEQ_STATUS status; 
		hash_seq_init(&status, rwset->writeSet);
		ShmemReadWriteSetKey *writekey;
		while ((writekey = (ShmemReadWriteSetKey*)hash_seq_search(&status)) != NULL)
		{
			ShmemReadWriteSetKey *readkey;
			bool found;
			readkey = (ShmemReadWriteSetKey *)hash_search(tmpSlot->readSet, 
													  writekey->key, HASH_FIND, &found);
			if (readkey != NULL && found)
			{
				LWLockRelease(ReadWriteSetArrayLock);
				return 0;
			}
		}
	}
	LWLockRelease(ReadWriteSetArrayLock);

	return 1;
}

int DTAValidation(void)
{
	if (!CurrentReadWriteSetSlot)
		return 1;

	volatile ReadWriteSetSlot *rwset = CurrentReadWriteSetSlot;
	rwset->status = RUNNING;

	HASH_SEQ_STATUS status; 
	hash_seq_init(&status, rwset->writeSet);
	ShmemReadWriteSetKey *writekey;
	// reset the wts to 0
	while ((writekey = (ShmemReadWriteSetKey*)hash_seq_search(&status)) != NULL)
	{
		ShmemRtsEnt *rts_result;
		bool found = false;
		LWLockAcquire(RtsHashTableLock, LW_EXCLUSIVE);
		rts_result = (ShmemRtsEnt *)hash_search(ShmemRts, writekey->key, HASH_FIND, &found);

		if (rts_result && rwset->lower <= rts_result->rts)
			rwset->lower = rts_result->rts + 1;
			
		LWLockRelease(RtsHashTableLock);

		/*
         * Modify the timestamps of other transactions based on the read-write set.
         */
		ShmemXidsEnt *result;
		LWLockAcquire(KeyXidsHashTableLock, LW_EXCLUSIVE);
		result = (ShmemXidsEnt *)hash_search(ShmemKeyXids, writekey->key, HASH_FIND, &found);

		if (!found)
		{
			continue;
		}
		// set this key is locked
		if (result->writeTxn && result->writeTxn != rwset->gxid)
		{
    		hash_seq_term(&status);
			rwset->status = ABORTED;
			LWLockRelease(KeyXidsHashTableLock);
			return 0;
		}
		else if (transaction_op_type == TRANSACTION_TYPE_O)
			result->writeTxn = rwset->gxid;

		for (int k = 0; k < result->ptr; k++)
		{
			int id = FindTargetTxnID(result->readTxn[k]);
			if (id == -1) continue;
			ReadWriteSetSlot* tmpSlot = &readWriteSetArray->slots[id];
			// ReadWriteSetSlot* tmpSlot = FindTargetTxn(result->readTxn[k]);

			if (tmpSlot->status == COMMITTED || tmpSlot->status == VALIDATED)
			{
				if (rwset->lower < tmpSlot->upper)
					rwset->lower = tmpSlot->upper;
			}
			else if (tmpSlot->status == RUNNING)
			{
				if (rwset->lower < tmpSlot->upper)
				{
					if (rwset->lower <= tmpSlot->lower)
					{
						if ((tmpSlot->lower + 1) < tmpSlot->upper)
						{
							rwset->lower = tmpSlot->lower + 1;
							tmpSlot->upper = rwset->lower;
							if (tmpSlot->upper == 0)
								core_dump();
						}
					}
					else
					{
						tmpSlot->upper = rwset->lower;
						if (tmpSlot->upper == 0)
							core_dump();
					}
				}
			}
		}
		LWLockRelease(KeyXidsHashTableLock);
	}

	if (rwset->lower < rwset->upper)
	{
		rwset->status = VALIDATED;
		return 1;
	}
	else
	{
		rwset->status = ABORTED;
		return 0;
	}
}

void CleanUp(void){
	if (!CurrentReadWriteSetSlot)
		return;
	uint64 logical_commit_ts = 0;
	if (CurrentReadWriteSetSlot->status == VALIDATED)
		logical_commit_ts = LogicalCommitTsAllocation();
	volatile ReadWriteSetSlot *rwset = CurrentReadWriteSetSlot;

	HASH_SEQ_STATUS status; 
	hash_seq_init(&status, rwset->readSet);
	ShmemReadWriteSetKey *readkey;
	while ((readkey = (ShmemReadWriteSetKey*)hash_seq_search(&status)) != NULL)
	{
		ShmemRtsEnt *result;
		bool found;
		if (CurrentReadWriteSetSlot->status == VALIDATED)
			UpdateRts(readkey);
		Size len = 0;
		KeyXidsEntRemove(readkey, true);
	}
	
	hash_seq_init(&status, rwset->writeSet);
	ShmemReadWriteSetKey *writekey;
	// reset the wts to 0
	while ((writekey = (ShmemReadWriteSetKey*)hash_seq_search(&status)) != NULL)
	{
		KeyXidsEntRemove(writekey, false);
	}

	if (CurrentReadWriteSetSlot->status == VALIDATED)
		CurrentReadWriteSetSlot->status = COMMITTED;
}

bool CheckTxnStatus()
{
	return !CurrentReadWriteSetSlot ? true : (CurrentReadWriteSetSlot->status == ABORTED ? false : true);
}