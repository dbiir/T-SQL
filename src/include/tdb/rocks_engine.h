/*-------------------------------------------------------------------------
 *
 * rocks_engine.h
 *	  prototypes for functions in backend/tdb/rocks_engine.c
 *
 *
 * Portions Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/rocks_engine.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ROCKS_ENGINE_H
#define ROCKS_ENGINE_H

#include "utils/rel.h"
#include "utils/tqual.h"
#include "rocksdb/c.h"
#include "tdb/kvengine.h"

typedef enum
{
	PUT_OP,
	DELETE_OP,
}ROCKSDBOP;

typedef struct OPTYPE
{
	int OPtype;
} OPTYPE;

typedef struct PutOP
{
	OPTYPE OPtype;
	TupleKeySlice key;
	TupleValueSlice value;
} PutOP;

typedef struct DeleteOP
{
	OPTYPE OPtype;
	TupleKeySlice key;
} DeleteOP;

typedef struct RocksEngine
{
	KVEngineInterface 		interface;
	rocksdb_t				*PRIVATE_db;
	rocksdb_backup_engine_t *PRIVATE_be;
	rocksdb_options_t		*PRIVATE_opt;
} RocksEngine;

typedef struct RocksEngineIterator
{
	KVEngineIteratorInterface	interface;
	rocksdb_iterator_t			*PRIVATE_it;
	rocksdb_readoptions_t		*PRIVATE_readopt;
    rocksdb_snapshot_t          *PRIATE_snapshot;
} RocksEngineIterator;

typedef struct RocksBatchEngine
{
	KVEngineBatchInterface 		interface;
	rocksdb_writebatch_wi_t		*PRIVATE_writebatch_wi;
} RocksBatchEngine;

typedef struct RocksBatchEngineEntry
{
	DistributedTransactionId	key;
	RocksBatchEngine			val;
} RocksBatchEngineEntry;

extern HTAB *writebatch_env_htab;
extern HTAB *TuplekvLock_env_htab;

/* universal function */
extern rocksdb_options_t* create_and_init_options(void);
extern rocksdb_t* open_db(rocksdb_options_t* options);
extern rocksdb_backup_engine_t* open_backup_engine(rocksdb_options_t* options);
extern void rocks_engine_destroy(KVEngineInterface* interface);

extern void rocks_engine_init_iterator_interface(KVEngineIteratorInterface* interface);
extern void rocks_engine_init_iterator_interface_pre(KVEngineIteratorInterface* interface);

extern void StorageInitTupleLockHash(void);
extern void acquireTuplekvLock(TupleKeySlice key);
extern void releaseTuplekvLock(TupleKeySlice key);
extern void acquireTuplekvLockAndBool(TupleKeySlice key, bool *have_lock);
extern void releaseTuplekvLockAndBool(TupleKeySlice key, bool *have_lock);

extern bool is_cf_options_valid(const char *cfoptions_str);
extern void fill_cf_name(Oid relid, const char *relname, Name cfname);


/* normal engine */
extern KVEngineInterface* rocks_create_normal_engine(void);
extern void rocks_init_engine_interface(KVEngineInterface* interface);

// extern KVEngineIteratorInterface* rocks_engine_create_iterator(KVEngineInterface* interface, bool isforward);
extern KVEngineBatchInterface* rocks_engine_create_batch(KVEngineInterface* interface, DistributedTransactionId gxid, bool flag);

// extern TupleValueSlice rocks_engine_get(KVEngineInterface* interface, TupleKeySlice key, uint64_t *cts, uint64_t *nts);
// extern void rocks_engine_put(KVEngineInterface* interface, TupleKeySlice key, TupleValueSlice value);
// extern void rocks_engine_delete_direct(KVEngineInterface* interface, TupleKeySlice key);

extern void rocks_engine_iterator_destroy(KVEngineIteratorInterface* interface);
extern void rocks_engine_iterator_seek(KVEngineIteratorInterface* interface, TupleKeySlice key);
extern bool rocks_engine_iterator_is_valid(KVEngineIteratorInterface* interface);
extern void rocks_engine_iterator_next(KVEngineIteratorInterface* interface);
extern void rocks_engine_iterator_get(KVEngineIteratorInterface* interface, TupleKeySlice* key, TupleValueSlice* value);

extern void rocks_engine_iterator_next_pre(KVEngineIteratorInterface* interface);
extern void rocks_engine_iterator_seek_pre(KVEngineIteratorInterface* interface, TupleKeySlice key);
extern void rocks_engine_iterator_ts(KVEngineIteratorInterface* interface, uint64_t *cts, uint64_t *nts);
extern void rocks_engine_batch_commit_and_destroy(KVEngineInterface* interface, KVEngineBatchInterface* batch_interface, DistributedTransactionId gxid);
extern void rocks_engine_batch_abort_and_destroy(KVEngineInterface* interface, KVEngineBatchInterface* batch_interface, DistributedTransactionId gxid);
extern void rocks_engine_batch_put(KVEngineBatchInterface* interface, TupleKeySlice key, TupleValueSlice value);
extern void rocks_engine_batch_delete_direct(KVEngineBatchInterface* interface, TupleKeySlice key);
extern TupleValueSlice rocks_engine_batch_get(KVEngineInterface* interface, KVEngineBatchInterface* batch_interface, TupleKeySlice key, uint64_t *cts, uint64_t *nts);
extern KVEngineIteratorInterface* rocks_engine_create_batch_iterator(KVEngineIteratorInterface* engine_it, KVEngineBatchInterface* batch_interface);

extern void rocks_engine_init_batch_interface(KVEngineBatchInterface* interface);

extern void StorageInitWritebatchHash(void);

/* transaction engine */
typedef struct OptTransDBEngine
{
    RocksEngine                                 engine;
    rocksdb_optimistictransactiondb_t           *otxn_db;
    rocksdb_optimistictransaction_options_t*    otxn_options;
    
    int                                         cf_num;
    char                                        *cf_name[10];
    rocksdb_column_family_handle_t*             cf_handle[10];
}OptTransDBEngine;

typedef struct PesTransDBEngine
{
    RocksEngine                                 engine;
    rocksdb_transactiondb_t                     *ptxn_db;
    rocksdb_transactiondb_options_t*            ptxn_options;
}PesTransDBEngine;

typedef struct KeyRange
{
	TupleKeySlice							startkey;
	TupleKeySlice							endkey;
	bool 									isSingle;
	bool									isDelete;
} KeyRange;

typedef	enum
{
	PENDING,
	RUNNING,
	VALIDATED,
	COMMITTED,
	ABORTED
} DTATxnStatus;

typedef struct TXNEngine
{
	KVEngineTransactionInterface            interface;
	rocksdb_transaction_t                   *PRIVATE_txn;
	uint64						            start_ts;
	uint64						            commit_ts;
    slock_t									mutex;
    int                                     cf_num;
    char**                                  cf_name;
    rocksdb_column_family_handle_t*         cf_handle[10];
	List* 									readSet;
	List*									writeSet;
	uint64									lower;
	uint64									upper;
	DTATxnStatus							status;
	int 									use_count;
} TXNEngine;

typedef struct TXNEngineEntry
{
	DistributedTransactionId	key;
	TXNEngine			val;
} TXNEngineEntry;
extern HTAB *transaction_env_htab[TXN_BUCKET];
extern KVEngineInterface* rocks_create_pessimistic_transaction_engine();
extern void rocks_init_pessimistic_engine_interface(KVEngineInterface* interface);
extern void StorageInitTransactionBatchHash(void);

extern KVEngineIteratorInterface* rocks_transaction_create_iterator(KVEngineTransactionInterface* interface, bool isforward, int cf_name);
extern void set_txnengine_ts(KVEngineTransactionInterface* interface, uint64_t lower, uint64_t upper);
extern void get_txnengine_ts(KVEngineTransactionInterface* interface, uint64_t* lower, uint64_t* upper);
extern void insert_readset(KVEngineTransactionInterface* interface, TupleKeySlice startkey, TupleKeySlice endkey, bool isSingle, bool isDelete);
extern void insert_writeset(KVEngineTransactionInterface* interface, TupleKeySlice startkey, TupleKeySlice endkey, bool isSingle, bool isDelete);
extern List* get_readset(KVEngineTransactionInterface* interface);
extern List* get_writeset(KVEngineTransactionInterface* interface);
extern bool txn_ts_is_validate(KVEngineTransactionInterface* interface);

extern void acquire_txnengine_mutex(KVEngineTransactionInterface* interface);
extern void release_txnengine_mutex(KVEngineTransactionInterface* interface);
extern KVEngineInterface* rocks_create_optimistic_transactions_engine(void);
#endif

