/*-------------------------------------------------------------------------
 *
 * storage_param.h
 *	  Thread locks used inside the storage process
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/storage_param.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_PARAM_H
#define STORAGE_PARAM_H

#include "tdb/kvengine.h"
#include "utils/hsearch.h"
//#include "tdb/storage_processor.h"
#define new_delete
#define MAX_HASH_KEY_LEN (128 * 1024)
#define USE_SPIN_LOCK

extern MemoryContext TxnRWSetContext;
typedef struct StorageThreadLockData
{
#ifndef USE_SPIN_LOCK
    pthread_mutex_t _Thread_MemoryContext_Lock;
    pthread_mutex_t _Thread_VisionJudge_Lock;

    pthread_mutex_t _Thread_WBHTAB_Lock;
    pthread_mutex_t _Thread_TxnHTAB_Lock;
    pthread_mutex_t _Thread_LWinte_Lock;
    pthread_mutex_t _Thread_LWarray_Lock;
    pthread_mutex_t _Thread_Update_Lock;
    pthread_mutex_t _Thread_TupleLockHash_Lock;
	pthread_mutex_t *_Thread_Postgres_Lock;
#else
    slock_t _Thread_MemoryContext_Lock;
    slock_t _Thread_VisionJudge_Lock;

    slock_t _Thread_WBHTAB_Lock;
    slock_t _Thread_LWinte_Lock;
    slock_t _Thread_LWarray_Lock;
    slock_t _Thread_Update_Lock;
    slock_t _Thread_TupleLockHash_Lock;
	slock_t _Thread_RWSET_Lock;

	slock_t *_Thread_Postgres_Lock;
    slock_t _Thread_TxnHTAB_Lock[TXN_BUCKET];
#endif
    int lock_count;
}StorageThreadLockData;

typedef struct StorageThreadHaveLock
{
    bool HAVE_MemoryContext_LOCK;
    bool HAVE_VisionJudge_LOCK;
    bool HAVE_WBHTAB_LOCK;
    bool HAVE_LWinte_LOCK;
    bool HAVE_LWarray_LOCK;
    bool HAVE_Update_LOCK;
    bool HAVE_TupleLockHash_LOCK;
	bool *HAVE_Postgres_LOCK;
    bool HAVE_TxnHTAB_LOCK[TXN_BUCKET];

	bool HAVE_RWSET_LOCK;
    int lock_count;
}StorageThreadHaveLock;

typedef struct DataSliceHash
{
    char key[MAX_HASH_KEY_LEN];
}DataSliceHash;
typedef struct TuplekvLock
{
    DataSliceHash dataslice;
    pthread_mutex_t tuple_lock;
    int num;
}TuplekvLock;

extern int transaction_type;
extern StorageThreadLockData StorageThreadLock;
extern __thread StorageThreadHaveLock StorageHaveLock;
extern bool enable_thread_lock;
extern bool enable_dynamic_lock;
extern bool enable_req_failed_retry;
extern bool memorycontext_lock;
extern bool enable_paxos;
extern bool enable_range_distribution;
extern int *StoragePort;
extern int PortNum;

extern bool am_kv_storage;
#ifndef USE_SPIN_LOCK
#define ADD_THREAD_LOCK_EXEC(_LOCK_NAME_)  \
do\
{\
    if (enable_thread_lock && am_kv_storage && !StorageHaveLock.HAVE_##_LOCK_NAME_##_LOCK)\
    {\
        pthread_mutex_lock(&StorageThreadLock._Thread_##_LOCK_NAME_##_Lock);\
        StorageHaveLock.HAVE_##_LOCK_NAME_##_LOCK = true;\
    }\
} while (0);

#define REMOVE_THREAD_LOCK_EXEC(_LOCK_NAME_)  \
do\
{\
    if (enable_thread_lock && am_kv_storage && StorageHaveLock.HAVE_##_LOCK_NAME_##_LOCK)\
    {\
        pthread_mutex_unlock(&StorageThreadLock._Thread_##_LOCK_NAME_##_Lock);\
        StorageHaveLock.HAVE_##_LOCK_NAME_##_LOCK = false;\
    }\
} while (0);

#else

#define ADD_THREAD_LOCK_EXEC(_LOCK_NAME_)  \
do\
{\
    if (enable_thread_lock && am_kv_storage && !StorageHaveLock.HAVE_##_LOCK_NAME_##_LOCK)\
    {\
        SpinLockAcquire(&StorageThreadLock._Thread_##_LOCK_NAME_##_Lock);\
        StorageHaveLock.HAVE_##_LOCK_NAME_##_LOCK = true;\
    }\
} while (0);

#define REMOVE_THREAD_LOCK_EXEC(_LOCK_NAME_)  \
do\
{\
    if (enable_thread_lock && am_kv_storage && StorageHaveLock.HAVE_##_LOCK_NAME_##_LOCK)\
    {\
        SpinLockRelease(&StorageThreadLock._Thread_##_LOCK_NAME_##_Lock);\
        StorageHaveLock.HAVE_##_LOCK_NAME_##_LOCK = false;\
    }\
} while (0);

#define ADD_TxnHTAB_LOCK_EXEC(_LOCK_ID_)  \
do\
{\
    if (enable_thread_lock && am_kv_storage && \
		!StorageHaveLock.HAVE_TxnHTAB_LOCK[_LOCK_ID_])\
    {\
        SpinLockAcquire(&StorageThreadLock._Thread_TxnHTAB_Lock[_LOCK_ID_]);\
        StorageHaveLock.HAVE_TxnHTAB_LOCK[_LOCK_ID_] = true;\
    }\
} while (0);

#define REMOVE_TxnHTAB_LOCK_EXEC(_LOCK_ID_)  \
do\
{\
    if (enable_thread_lock && am_kv_storage && \
		StorageHaveLock.HAVE_TxnHTAB_LOCK[_LOCK_ID_])\
    {\
        SpinLockRelease(&StorageThreadLock._Thread_TxnHTAB_Lock[_LOCK_ID_]);\
        StorageHaveLock.HAVE_TxnHTAB_LOCK[_LOCK_ID_] = false;\
    }\
} while (0);

#endif

/* this function will cause core dump. I use this function to find one error */
static inline void 
core_dump(void)
{
    char *a = NULL;
    a[3] = 'c';
}

extern void AcquireThreadLock(int id);
extern void ReleaseThreadLock(int id);
extern void InitThreadLock(void);
extern void InitTxnLock(void);
extern void InitThreadHaveLock(void);
extern void destroyThreadLock(void);
extern void destroyThreadHaveLock(void);

extern void StoragePaxosPortShmemInit(void);
extern Size StoragePaxosPortShmemSize(void);
#endif
