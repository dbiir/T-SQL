/*-------------------------------------------------------------------------
 *
 * storage_param.c
 *	  Thread locks used inside the storage process
 *
 * Portions Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *      src/backend/access/rocksdb/kv.c
 *
 *-------------------------------------------------------------------------
 */
#include "tdb/storage_param.h"
#include "storage/lwlock.h"
#define MAXHOSTNAMELEN 63

int transaction_type = KVENGINE_TRANSACTIONDB;

bool enable_thread_lock = false;
bool enable_dynamic_lock = true;
bool enable_req_failed_retry = false;
bool memorycontext_lock = true;
bool enable_paxos = false;
bool enable_range_distribution = false;
StorageThreadLockData StorageThreadLock;
__thread StorageThreadHaveLock StorageHaveLock;

int *StoragePort = NULL;
int PortNum = MAXHOSTNAMELEN;

void
StoragePaxosPortShmemInit(void)
{
	bool found;
	StoragePort = (int*)
		ShmemInitStruct("StoragePort", sizeof(int) * PortNum, &found);
	if (!IsUnderPostmaster)
	{
		Assert(!found);
	}
	else
	{
		Assert(found);
	}
}

Size
StoragePaxosPortShmemSize(void)
{
	return sizeof(int) * PortNum;
}

void
AcquireThreadLock(int id)
{
    if (!am_kv_storage || !enable_dynamic_lock)
        return;
#ifndef USE_SPIN_LOCK
    pthread_mutex_lock(&(StorageThreadLock._Thread_Postgres_Lock[id]));
#else
	SpinLockAcquire(&(StorageThreadLock._Thread_Postgres_Lock[id]));
#endif
    StorageHaveLock.HAVE_Postgres_LOCK[id] = true;
}

void
ReleaseThreadLock(int id)
{
    if (!am_kv_storage || !enable_dynamic_lock)
        return;
#ifndef USE_SPIN_LOCK
    pthread_mutex_unlock(&(StorageThreadLock._Thread_Postgres_Lock[id]));
#else
	SpinLockRelease(&(StorageThreadLock._Thread_Postgres_Lock[id]));
#endif
    StorageHaveLock.HAVE_Postgres_LOCK[id] = false;
}

void
InitThreadHaveLock()
{
    if (!am_kv_storage)
        return;
    StorageHaveLock.HAVE_Postgres_LOCK = palloc0(sizeof(bool) * StorageThreadLock.lock_count);
    StorageHaveLock.lock_count = StorageThreadLock.lock_count;
    for (int i = 0; i < StorageHaveLock.lock_count; i++)
    {
        StorageHaveLock.HAVE_Postgres_LOCK[i] = false;
    }
}

void
InitThreadLock()
{
    if (!am_kv_storage)
        return;
    int lwlocknum = NumLWLocks();
    StorageThreadLock._Thread_Postgres_Lock = palloc0(sizeof(pthread_mutex_t) * lwlocknum);
    StorageHaveLock.HAVE_Postgres_LOCK = palloc0(sizeof(bool) * lwlocknum);
    StorageThreadLock.lock_count = lwlocknum;
    StorageHaveLock.lock_count = lwlocknum;
    for (int i = 0; i < lwlocknum; i++)
    {
#ifndef USE_SPIN_LOCK
        pthread_mutex_init (&(StorageThreadLock._Thread_Postgres_Lock[i]), NULL);
#else
		SpinLockInit (&(StorageThreadLock._Thread_Postgres_Lock[i]));
#endif
        StorageHaveLock.HAVE_Postgres_LOCK[i] = false;
    }

}

void
InitTxnLock()
{
    if (!am_kv_storage)
        return;
    for (int i = 0; i < TXN_BUCKET; i++)
    {
#ifndef USE_SPIN_LOCK
        pthread_mutex_init (&(StorageThreadLock._Thread_TxnHTAB_Lock[i]), NULL);
#else
		SpinLockInit (&(StorageThreadLock._Thread_TxnHTAB_Lock[i]));
#endif
        StorageHaveLock.HAVE_TxnHTAB_LOCK[i] = false;
    }
}

void
destroyThreadHaveLock()
{
    pfree(StorageHaveLock.HAVE_Postgres_LOCK);
    StorageHaveLock.HAVE_Postgres_LOCK = NULL;
}

void
destroyThreadLock()
{
    pfree(StorageThreadLock._Thread_Postgres_Lock);
    StorageThreadLock._Thread_Postgres_Lock = NULL;
    pfree(StorageHaveLock.HAVE_Postgres_LOCK);
    StorageHaveLock.HAVE_Postgres_LOCK = NULL;
}
