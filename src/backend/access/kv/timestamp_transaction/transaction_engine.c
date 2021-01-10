/*-------------------------------------------------------------------------
 *
 * transaction_engine.h
 *	  functions for rocksdb transaction_engine engine.
 *
 *
 * Portions Copyright (c) 2019-Present, TDSQL
 *
 * src/backend/tdb/rocks_engine.c
 *
 *-------------------------------------------------------------------------
 */
#include <stdlib.h>
#include <unistd.h>

#include "postgres.h"
#include "tdb/kvengine.h"
#include "tdb/rocks_engine.h"
#include "tdb/storage_param.h"
#include "rocksdb/c.h"

#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/gp_fastsequence.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "tdb/rocks_engine.h"
#include "tdb/timestamp_transaction/http.h"
#include "tdb/storage_param.h"
#include "librw/librm.h"

static rocksdb_optimistictransactiondb_t* open_opt_db(OptTransDBEngine *rocks_engine);
static void rocks_init_optengine_interface(KVEngineInterface* interface);
static void rocks_transaction_init_txn_interface(KVEngineTransactionInterface* interface);

static TupleValueSlice rocks_transaction_get(KVEngineTransactionInterface* interface, 
                                             TupleKeySlice key, int cf_name, uint64_t *cts, uint64_t *nts);
static void rocks_transaction_put(KVEngineTransactionInterface* interface, TupleKeySlice key, 
                                  TupleValueSlice value, int cf_name);
static void rocks_transaction_delete(KVEngineTransactionInterface* interface, 
                                     TupleKeySlice key, int cf_name);
static bool rocks_transaction_prepare(KVEngineTransactionInterface* interface, 
									KVEngineInterface *engine,
									DistributedTransactionId gxid, 
									uint64 start_ts);
static bool rocks_transaction_commit_and_destroy(KVEngineTransactionInterface* interface, 
                                                 DistributedTransactionId gxid, 
                                                 uint64 commit_ts);
static bool rocks_transaction_commit_with_lts_and_destroy(KVEngineTransactionInterface* interface, 
                                    DistributedTransactionId gxid, 
                                    uint64 commit_ts, 
                                    uint32 lts);
static void rocks_transaction_abort_and_destroy(KVEngineTransactionInterface* interface, 
                                    DistributedTransactionId gxid);
static void rocks_transaction_destroy(
                                    KVEngineTransactionInterface* interface, 
                                    DistributedTransactionId gxid);
static KVEngineTransactionInterface* open_txnengine_txn(KVEngineInterface* rocks_engine, 
                                    DistributedTransactionId gxid, 
                                    bool flag, uint64 start_ts);
static TupleValueSlice rocks_transaction_get_for_update(KVEngineTransactionInterface* interface, TupleKeySlice key, uint64_t *cts, uint64_t *nts);
static void rocks_optengine_destroy(KVEngineInterface* interface);
static void rocks_optengine_compaction(KVEngineInterface* interface);
static void rocks_optengine_flush(KVEngineInterface* interface);
static TupleValueSlice rocks_engine_get(KVEngineInterface* interface, TupleKeySlice key, uint64_t *cts, uint64_t *nts);
static void rocks_engine_put(KVEngineInterface* interface, TupleKeySlice key, TupleValueSlice value);
static void rocks_engine_delete_direct(KVEngineInterface* interface, TupleKeySlice key);
static KVEngineIteratorInterface* rocks_engine_create_iterator(KVEngineInterface* interface, bool isforward);

KVEngineInterface*
rocks_create_optimistic_transactions_engine()
{
	OptTransDBEngine *rocks_engine = palloc0(sizeof(*rocks_engine));
	rocks_engine->engine.PRIVATE_opt = create_and_init_options();
	rocks_engine->otxn_db = open_opt_db(rocks_engine);
    rocks_engine->otxn_options = rocksdb_optimistictransaction_options_create();
	rocks_engine->engine.PRIVATE_be = open_backup_engine(rocks_engine->engine.PRIVATE_opt);
	rocks_init_optengine_interface((KVEngineInterface*) rocks_engine);
	return (KVEngineInterface*) rocks_engine;
}

static void 
insert_name(OptTransDBEngine *rocks_engine, int index, char* name, uint64_t name_len)
{
    rocks_engine->cf_name[index] = palloc(name_len + 1);
    strcpy(rocks_engine->cf_name[index], name);
}

static rocksdb_optimistictransactiondb_t*
open_opt_db(OptTransDBEngine *rocks_engine)
{
    rocksdb_options_t* options = rocks_engine->engine.PRIVATE_opt;
    rocks_engine->cf_num = 3;
    insert_name(rocks_engine, 0, ROCKS_DEFAULT_CF, strlen(ROCKS_DEFAULT_CF));
    insert_name(rocks_engine, 1, ROCKS_LTS_CF, strlen(ROCKS_LTS_CF));
    insert_name(rocks_engine, 2, RocksDBSystemCatalog, strlen(RocksDBSystemCatalog));

	const rocksdb_options_t* cf_options[3] = {options, options, options};

	char *err = NULL;
	rocksdb_optimistictransactiondb_t *db = 
            rocksdb_optimistictransactiondb_open_column_families
            (options, RocksDBPath, 3, rocks_engine->cf_name, cf_options, rocks_engine->cf_handle, &err);
	if (err)
	{
		ereport((ERROR),(errmsg_internal("engine open failed, (err message:%s)",
					err)));
	}
	return db;
}

static void
rocks_init_optengine_interface(KVEngineInterface* interface)
{
	interface->destroy = rocks_optengine_destroy;
	interface->compaction = rocks_optengine_compaction;
    interface->create_txn = open_txnengine_txn;

	interface->create_iterator = rocks_engine_create_iterator;
	interface->get = rocks_engine_get;
	interface->put = rocks_engine_put;
	interface->delete_direct = rocks_engine_delete_direct;
}

static KVEngineIteratorInterface*
rocks_engine_create_iterator(KVEngineInterface* interface, bool isforward)
{
	OptTransDBEngine *rocks_engine = (OptTransDBEngine*) interface;
	RocksEngineIterator *rocks_it = palloc0(sizeof(*rocks_it));
	rocks_it->PRIVATE_readopt = rocksdb_readoptions_create();
	rocks_it->PRIVATE_it = rocksdb_create_iterator(rocks_engine->otxn_db, rocks_it->PRIVATE_readopt);
	if (isforward)
		rocks_engine_init_iterator_interface((KVEngineIteratorInterface*) rocks_it);
	else
		rocks_engine_init_iterator_interface_pre((KVEngineIteratorInterface*) rocks_it);
	return (KVEngineIteratorInterface*) rocks_it;
}

static TupleValueSlice
rocks_engine_get(KVEngineInterface* interface, TupleKeySlice key, uint64_t *cts, uint64_t *nts)
{
	OptTransDBEngine* rocks_engine = (OptTransDBEngine*) interface;
	TupleValueSlice value = {NULL, 0};
	uint64_t ccts = 0, nnts = 0;
	rocksdb_readoptions_t *readopt = rocksdb_readoptions_create();
	char *err = NULL;
	value.data = (TupleValue) rocksdb_get(rocks_engine->otxn_db, readopt, (char*) key.data, key.len, &value.len, &err);
	if (err)
		ereport(ERROR,
			(errmsg("Rocksdb: get exec failed, err = %s",
					err)));
	rocksdb_readoptions_destroy(readopt);
	if (cts != NULL && nts != NULL) 
	{
		*cts = ccts;
		*nts = nnts;
	}
	return value;
}

static void
rocks_engine_put(KVEngineInterface* interface, TupleKeySlice key, TupleValueSlice value)
{
	OptTransDBEngine* rocks_engine = (OptTransDBEngine*) interface;
	rocksdb_writeoptions_t *writeopt = rocksdb_writeoptions_create();
	char *err = NULL;
	rocksdb_put(rocks_engine->otxn_db, writeopt, (char*) key.data, key.len, (char*) value.data, value.len, &err);
	if (err)
		ereport(ERROR,
			(errmsg("Rocksdb: put exec failed, err = %s",
					err)));
	rocksdb_writeoptions_destroy(writeopt);
}

static void
rocks_engine_delete_direct(KVEngineInterface* interface, TupleKeySlice key)
{
	OptTransDBEngine* rocks_engine = (OptTransDBEngine*) interface;
	rocksdb_writeoptions_t *writeopt = rocksdb_writeoptions_create();
	char *err = NULL;
	rocksdb_delete(rocks_engine->otxn_db, writeopt, (char*) key.data, key.len, &err);
	if (err)
		ereport(ERROR,
			(errmsg("Rocksdb: delete direct exec failed, err = %s",
					err)));
	rocksdb_writeoptions_destroy(writeopt);
}

static void
rocks_optengine_compaction(KVEngineInterface* interface)
{
	OptTransDBEngine *rocks_engine = (OptTransDBEngine*) interface;

    rocksdb_compact_range(rocks_engine->otxn_db, NULL, 0, NULL, 0);

}

static void
rocks_optengine_flush(KVEngineInterface* interface)
{
	OptTransDBEngine *rocks_engine = (OptTransDBEngine*) interface;
	rocksdb_flushoptions_t *flush_option = rocksdb_flushoptions_create();
	char *err = NULL;
    rocksdb_flush(rocks_engine->otxn_db, flush_option, err);

}

static void
rocks_optengine_destroy(KVEngineInterface* interface)
{
	OptTransDBEngine *rocks_engine = (OptTransDBEngine*) interface;
	char *err = NULL;
	rocksdb_backup_engine_create_new_backup(rocks_engine->engine.PRIVATE_be,
                                    rocks_engine->otxn_db, &err);
	if (err)
		ereport(ERROR,
			(errmsg("Rocksdb: destroy exec failed, err = %s",
					err)));
    rocksdb_compact_range(rocks_engine->otxn_db, NULL, 0, NULL, 0);
    rocksdb_options_destroy(rocks_engine->engine.PRIVATE_opt);
	rocksdb_backup_engine_close(rocks_engine->engine.PRIVATE_be);
    rocksdb_optimistictransaction_options_destroy(rocks_engine->otxn_options);
    rocksdb_column_family_handle_destroy(rocks_engine->cf_handle[0]);
    rocksdb_column_family_handle_destroy(rocks_engine->cf_handle[1]);
    rocksdb_column_family_handle_destroy(rocks_engine->cf_handle[2]);
	rocksdb_close(rocks_engine->otxn_db);
	pfree(rocks_engine);
    rocks_engine = NULL;
}

static uint32
caculate_bucket(DistributedTransactionId gxid)
{
	uint32 hashid = gxid / PENGUINDB_GLOBAL_MASTER_NUMBER;
	return hashid % TXN_BUCKET;
}

static KVEngineTransactionInterface*
open_txnengine_txn(KVEngineInterface* rocks_engine, DistributedTransactionId gxid,
                   bool flag, uint64 start_ts)
{
    bool found;
	uint32 hashid = caculate_bucket(gxid);
	if (flag)
	{
		ADD_TxnHTAB_LOCK_EXEC(hashid);
		TXNEngineEntry *rocks_batch_entry = hash_search(transaction_env_htab[hashid], (void*) &gxid, HASH_ENTER_NULL, &found);
		REMOVE_TxnHTAB_LOCK_EXEC(hashid);
		if (!found)
		{
			MemSet(&rocks_batch_entry->val, 0, sizeof(rocks_batch_entry->val));
            OptTransDBEngine* engine = (OptTransDBEngine*)rocks_engine;
            rocksdb_writeoptions_t *woptions = rocksdb_writeoptions_create();
			rocksdb_transaction_t* txn_cf = rocksdb_optimistictransaction_begin(
                                engine->otxn_db, woptions, engine->otxn_options, NULL);
            rocks_batch_entry->val.PRIVATE_txn = txn_cf;
			rocks_batch_entry->val.start_ts = start_ts;
			SpinLockInit(&rocks_batch_entry->val.mutex);
            rocks_batch_entry->val.cf_name = engine->cf_name;
            rocks_batch_entry->val.cf_num = engine->cf_num;
            for (int i = 0; i < 10; i++)
            {
                rocks_batch_entry->val.cf_handle[i] = engine->cf_handle[i];
            }
			rocks_batch_entry->val.readSet = NULL;
			rocks_batch_entry->val.writeSet = NULL;
			rocks_batch_entry->val.lower = 0;
			rocks_batch_entry->val.upper = kMaxSequenceNumber;
			rocks_batch_entry->val.status = RUNNING;
			rocks_batch_entry->val.use_count = 0;
			rocks_transaction_init_txn_interface((KVEngineTransactionInterface*) &rocks_batch_entry->val);
			rocksdb_writeoptions_destroy(woptions);
		}
		else if (!rocks_batch_entry)
        {
            core_dump();
        }
		else
		{
			rocks_batch_entry->val.start_ts = start_ts;	
		}
		
		// SpinLockAcquire(&rocks_batch_entry->val.mutex);
		return (KVEngineTransactionInterface*) &rocks_batch_entry->val;	
	}
	else
	{
		ADD_TxnHTAB_LOCK_EXEC(hashid);
		TXNEngineEntry *rocks_batch_entry = hash_search(transaction_env_htab[hashid], (void*) &gxid, HASH_FIND, &found);
		REMOVE_TxnHTAB_LOCK_EXEC(hashid);

		if (!found)
			return NULL;
		else
		{
			// SpinLockAcquire(&rocks_batch_entry->val.mutex);
			return (KVEngineTransactionInterface*) &rocks_batch_entry->val;
		}
	}
}

void 
set_txnengine_ts(KVEngineTransactionInterface* interface, uint64_t lower, uint64_t upper)
{
	acquire_txnengine_mutex(interface);
	TXNEngine *txn_engine = (TXNEngine*) interface;
	if (lower != 0)
		txn_engine->lower = txn_engine->lower > lower ?
							txn_engine->lower : lower;
	if (upper != 0)
		txn_engine->upper = txn_engine->upper < upper ?
							txn_engine->upper : upper;
	release_txnengine_mutex(interface);
}

void 
get_txnengine_ts(KVEngineTransactionInterface* interface, uint64_t* lower, uint64_t* upper)
{
	acquire_txnengine_mutex(interface);
	TXNEngine *txn_engine = (TXNEngine*) interface;
	*lower = txn_engine->lower;
	*upper = txn_engine->upper;
	release_txnengine_mutex(interface);
}

void
insert_readset(KVEngineTransactionInterface* interface, TupleKeySlice startkey, TupleKeySlice endkey, bool isSingle, bool isDelete)
{
	ADD_THREAD_LOCK_EXEC(RWSET);
	MemoryContext oldThreadContext = CurrentThreadMemoryContext;
	CurrentThreadMemoryContext = TxnRWSetContext;

	TXNEngine *txn_engine = (TXNEngine*) interface;
	KeyRange *keyrange = (KeyRange*)palloc0(sizeof(KeyRange));
	keyrange->startkey = copy_key(startkey);
	keyrange->endkey = copy_key(endkey);
	keyrange->isSingle = isSingle;
	keyrange->isDelete = isDelete;
	txn_engine->readSet = lappend(txn_engine->readSet, keyrange);

	CurrentThreadMemoryContext = oldThreadContext;
	REMOVE_THREAD_LOCK_EXEC(RWSET);
}

void 
insert_writeset(KVEngineTransactionInterface* interface, TupleKeySlice startkey, TupleKeySlice endkey, bool isSingle, bool isDelete)
{
	ADD_THREAD_LOCK_EXEC(RWSET);
	MemoryContext oldThreadContext = CurrentThreadMemoryContext;
	CurrentThreadMemoryContext = TxnRWSetContext;

	TXNEngine *txn_engine = (TXNEngine*) interface;
	KeyRange *keyrange = (KeyRange*)palloc0(sizeof(KeyRange));
	keyrange->startkey = copy_key(startkey);
	keyrange->endkey = copy_key(endkey);
	keyrange->isSingle = isSingle;
	keyrange->isDelete = isDelete;
	txn_engine->writeSet = lappend(txn_engine->writeSet, keyrange);

	CurrentThreadMemoryContext = oldThreadContext;
	REMOVE_THREAD_LOCK_EXEC(RWSET);
}

List* 
get_readset(KVEngineTransactionInterface* interface)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	return txn_engine->readSet;
}

List* 
get_writeset(KVEngineTransactionInterface* interface)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	return txn_engine->writeSet;
}

bool
txn_ts_is_validate(KVEngineTransactionInterface* interface)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	return txn_engine->lower < txn_engine->upper;
}

void 
acquire_txnengine_mutex(KVEngineTransactionInterface* interface)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	SpinLockAcquire(&txn_engine->mutex);
}
void
release_txnengine_mutex(KVEngineTransactionInterface* interface)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	SpinLockRelease(&txn_engine->mutex);
}

void
rocks_transaction_init_txn_interface(KVEngineTransactionInterface* interface)
{
	interface->abort_and_destroy = rocks_transaction_abort_and_destroy;
	interface->commit_and_destroy = rocks_transaction_commit_and_destroy;
	interface->prepare = rocks_transaction_prepare;
    interface->commit_with_lts_and_destroy = rocks_transaction_commit_with_lts_and_destroy;
    interface->destroy = rocks_transaction_destroy;
	interface->create_iterator = rocks_transaction_create_iterator;
	interface->get = rocks_transaction_get;
	interface->put = rocks_transaction_put;
	interface->delete = rocks_transaction_delete;
	interface->get_for_update = rocks_transaction_get_for_update;

	interface->set_ts = set_txnengine_ts;
	interface->get_ts = get_txnengine_ts;
	interface->insert_read_set = insert_readset;
	interface->insert_write_set = insert_writeset;
	interface->get_write_set = get_writeset;
	interface->get_read_set = get_readset;
	interface->ts_is_validate = txn_ts_is_validate;
}

KVEngineIteratorInterface*
rocks_transaction_create_iterator(KVEngineTransactionInterface* interface, 
                                  bool isforward, int cf_name)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	RocksEngineIterator *rocks_it = palloc0(sizeof(*rocks_it));
	rocks_it->PRIVATE_readopt = rocksdb_readoptions_create();
	rocksdb_readoptions_set_sequence(rocks_it->PRIVATE_readopt, 
                                     txn_engine->start_ts);

    rocksdb_column_family_handle_t* cfh = txn_engine->cf_handle[cf_name];

    if (cfh == NULL)
	{
        return NULL;
	}

	rocks_it->PRIVATE_it = 
                rocksdb_transaction_create_iterator_cf(txn_engine->PRIVATE_txn, 
                                                rocks_it->PRIVATE_readopt, cfh);

	if (isforward)
		rocks_engine_init_iterator_interface((KVEngineIteratorInterface*) rocks_it);
	else
		rocks_engine_init_iterator_interface_pre((KVEngineIteratorInterface*) rocks_it);
    
	return (KVEngineIteratorInterface*) rocks_it;
}

TupleValueSlice
rocks_transaction_get(KVEngineTransactionInterface* interface, 
                      TupleKeySlice key, int cf_name, uint64_t *cts, uint64_t *nts)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	TupleValueSlice value = {NULL, 0};
	uint64_t ccts = 0, nnts = kMaxSequenceNumber;
	rocksdb_readoptions_t *readopt = rocksdb_readoptions_create();
	rocksdb_readoptions_set_sequence(readopt, txn_engine->start_ts);
	char *err = NULL;
    rocksdb_column_family_handle_t** cff = txn_engine->cf_handle;
    rocksdb_column_family_handle_t* cfh = cff[cf_name];

    Assert(cfh != NULL);

	value.data = (TupleValue) 
                  rocksdb_transaction_get_cf_ts(txn_engine->PRIVATE_txn, readopt, cfh, 
                                        (char*) key.data, key.len, &value.len, &ccts, &nnts, &err);

	if (err) {	
		// ereport(ERROR,
		// 	(errmsg("Rocksdb: get exec failed, err = %s", err)));
	}
	rocksdb_readoptions_destroy(readopt);
	if (cts != NULL && nts != NULL) 
	{
		if (ccts != 0)
			*cts = ccts;
		if (nnts != 0)
			*nts = nnts;
	}
	return value;
}

TupleValueSlice
rocks_transaction_get_for_update(KVEngineTransactionInterface* interface, TupleKeySlice key, uint64_t *cts, uint64_t *nts)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	TupleValueSlice value = {NULL, 0};
	uint64_t ccts = 0, nnts = kMaxSequenceNumber;
	rocksdb_readoptions_t *readopt = rocksdb_readoptions_create();
	rocksdb_readoptions_set_sequence(readopt, txn_engine->start_ts);
	char *err = NULL;

	value.data = (TupleValue) rocksdb_transaction_get_for_update_ts(txn_engine->PRIVATE_txn, readopt, 
                    (char*) key.data, key.len, &value.len, (unsigned char)1, &ccts, &nnts, &err);

	if (err)
	{

		// ereport(ERROR,
		// 	(errmsg("Rocksdb: get for exec failed, err = %s",
		// 			err)));
	}
	rocksdb_readoptions_destroy(readopt);
	if (cts != NULL && nts != NULL) 
	{
		if (ccts != 0)
			*cts = ccts;
		if (nnts != 0)
			*nts = nnts;
	}
	return value;
}

void
rocks_transaction_put(KVEngineTransactionInterface* interface, 
                      TupleKeySlice key, TupleValueSlice value, int cf_name)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;

	char *err = NULL;
    rocksdb_column_family_handle_t** cff = txn_engine->cf_handle;
    rocksdb_column_family_handle_t* cfh = cff[cf_name];

    Assert(cfh != NULL);

	rocksdb_transaction_put_cf(txn_engine->PRIVATE_txn, cfh, (char*) key.data, key.len, 
                                                (char*) value.data, value.len, &err);
	if (err)
	{
		// ereport(ERROR,
		// 	(errmsg("Rocksdb: put exec failed, err = %s",
		// 			err)));
	}
}

void
rocks_transaction_delete(KVEngineTransactionInterface* interface, 
                         TupleKeySlice key, int cf_name)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;

	char *err = NULL;
    rocksdb_column_family_handle_t** cff = txn_engine->cf_handle;
    rocksdb_column_family_handle_t* cfh = cff[cf_name];

    Assert(cfh != NULL);
    rocksdb_transaction_delete_cf(txn_engine->PRIVATE_txn, cfh, 
                                  (char*) key.data, key.len, &err);

	if (err)
	{
		// ereport(WARNING,
		// 	(errmsg("Rocksdb: delete direct exec failed, err = %s", err)));
	}
}

static void 
acquire_two_txn_lock(KVEngineTransactionInterface* a, uint32 aid,
					 KVEngineTransactionInterface* b, uint32 bid)
{
	if (aid < bid)
	{
		acquire_txnengine_mutex(a);
		acquire_txnengine_mutex(b);
	}
	else if (aid > bid)
	{
		acquire_txnengine_mutex(b);
		acquire_txnengine_mutex(a);	
	}
	else
	{
		acquire_txnengine_mutex(b);
	}
}

static void 
release_two_txn_lock(KVEngineTransactionInterface* a, uint32 aid,
					 KVEngineTransactionInterface* b, uint32 bid)
{
	if (aid < bid)
	{
		release_txnengine_mutex(b);
		release_txnengine_mutex(a);
	}
	else if (aid > bid)
	{
		release_txnengine_mutex(a);	
		release_txnengine_mutex(b);
	}
	else
	{
		release_txnengine_mutex(b);
	}
}

bool
rocks_transaction_prepare(KVEngineTransactionInterface* interface, 
						  KVEngineInterface *engine,
						  DistributedTransactionId gxid, 
						  uint64 start_ts)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	//TODO:here we handle the dta validation.
	
	List *writeset = txn_engine->writeSet;
	ListCell *wl;
	bool success = true;
	// ereport(LOG,
	// 		(errmsg("DTA Prepare: started, txn:%d lower:%lu upper:%lu", gxid, txn_engine->lower, txn_engine->upper)));
	foreach(wl, writeset)
	{
		KeyRange* WriteSetCeil = (KeyRange*) lfirst(wl);
		uint64 rts = 0;

		rts = RtsCache_getRts(WriteSetCeil->startkey.data, 
								WriteSetCeil->startkey.len, 
								WriteSetCeil->endkey.data, 
								WriteSetCeil->isSingle ? 0 : WriteSetCeil->endkey.len);

		
		acquire_txnengine_mutex(interface);
		if (txn_engine->lower <= rts)
			txn_engine->lower = rts + 1;
		release_txnengine_mutex(interface);

		int writexid_size = 0;
		uint64_t* wxid  = KeyXidCache_addWriteXidWithMutex(WriteSetCeil->startkey.data, 
								WriteSetCeil->startkey.len, 
								WriteSetCeil->endkey.data, 
								WriteSetCeil->isSingle ? 0 : WriteSetCeil->endkey.len,
								(uint64_t)gxid,
								WriteSetCeil->isDelete,
								&writexid_size);
		for (int i = 0; i < writexid_size; i++)
		{
			if ((uint32)wxid[i] != gxid)
			{
				txn_engine->status = ABORTED;
				// ereport(LOG,
				// (errmsg("DTA Prepare: wxid failed, txn:%d lower:%lu upper:%lu a_wxid:%lu", gxid, txn_engine->lower, txn_engine->upper, wxid[i])));
				goto end;
			}
		}

		int readxid_size = 0;
		uint64_t* readgxid = KeyXidCache_getReadXid(WriteSetCeil->startkey.data, 
								WriteSetCeil->startkey.len, 
								WriteSetCeil->endkey.data, 
								WriteSetCeil->isSingle ? 0 : WriteSetCeil->endkey.len,
								WriteSetCeil->isDelete,
								&readxid_size);
		for (int i = 0; i < readxid_size; i++)
		{
			if ((DistributedTransactionId)readgxid[i] == gxid)
				continue;
			KVEngineTransactionInterface *another_txn =
				engine->create_txn(engine, (DistributedTransactionId)readgxid[i], 
									false, start_ts);
			if (another_txn == NULL)
				continue;
			acquire_two_txn_lock(interface, gxid, another_txn, (DistributedTransactionId)readgxid[i]);
			TXNEngine *another_txn_engine = (TXNEngine*) another_txn;
			// if (!another_txn->ts_is_validate(another_txn))
			if (another_txn_engine->lower >= another_txn_engine->upper )
			{
				release_two_txn_lock(interface, gxid, another_txn, (DistributedTransactionId)readgxid[i]);
				// ereport(LOG,
				// (errmsg("DTA Prepare: a_txn invalid, txn:%d lower:%lu upper:%lu a_lower:%lu a_upper:%lu", gxid, txn_engine->lower, txn_engine->upper, another_txn_engine->lower, another_txn_engine->upper)));
				continue;
			}
			if (another_txn_engine->status == COMMITTED || 
				another_txn_engine->status == VALIDATED)
			{
				// ereport(LOG,
				// (errmsg("DTA Prepare: a_txn commited, txn:%d lower:%lu upper:%lu a_lower:%lu a_upper:%lu", gxid, txn_engine->lower, txn_engine->upper, another_txn_engine->lower, another_txn_engine->upper)));
				
				txn_engine->lower = txn_engine->lower < another_txn_engine->upper ?
									txn_engine->lower : another_txn_engine->upper;
			}
			else if (another_txn_engine->status == RUNNING)
			{
				if (txn_engine->lower <= another_txn_engine->lower)
				{
					// ereport(LOG,
					// (errmsg("DTA Prepare: txn.lower < a_txn.lower, txn:%d lower:%lu upper:%lu a_lower:%lu a_upper:%lu", gxid, txn_engine->lower, txn_engine->upper, another_txn_engine->lower, another_txn_engine->upper)));

					txn_engine->lower = another_txn_engine->lower + 1;
					another_txn_engine->upper = 
							another_txn_engine->upper < txn_engine->lower ? 
							another_txn_engine->upper : txn_engine->lower;
				}
				else if (txn_engine->lower <= another_txn_engine->upper)
				{
					// ereport(LOG,
					// (errmsg("DTA Prepare: txn.lower < a_txn.upper, txn:%d lower:%lu upper:%lu a_lower:%lu a_upper:%lu", gxid, txn_engine->lower, txn_engine->upper, another_txn_engine->lower, another_txn_engine->upper)));

					another_txn_engine->upper = txn_engine->lower;
				}
			}
			release_two_txn_lock(interface, gxid, another_txn, (DistributedTransactionId)readgxid[i]);
			// if (!interface->ts_is_validate(interface))
			if (txn_engine->lower >= txn_engine->upper)
				goto validateend;
		}
	}
validateend:
	// if (interface->ts_is_validate(interface))
	if (txn_engine->lower < txn_engine->upper)
	{
		txn_engine->status = VALIDATED;
		success = true;
		// ereport(LOG,
		// 	(errmsg("DTA Prepare: success, txn:%d lower:%lu upper:%lu", gxid, txn_engine->lower, txn_engine->upper)));
	}
	else
	{
		txn_engine->status = ABORTED;
		success = false;
		// ereport(LOG,
		// 	(errmsg("DTA Prepare: failed, txn:%d lower:%lu upper:%lu", gxid, txn_engine->lower, txn_engine->upper)));
	}
end:
	return success;
}

bool
rocks_transaction_commit_and_destroy(KVEngineTransactionInterface* interface, 
                                     DistributedTransactionId gxid, 
                                     uint64 commit_ts)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	txn_engine->lower = commit_ts;
	txn_engine->upper = commit_ts + 1;
	char *err = NULL;
	bool success = true;
    rocksdb_transaction_commit_with_ts(txn_engine->PRIVATE_txn, &err, commit_ts);
    if (err)
    {
		success = false;
        int test = 0;
        if (test)
            ereport(WARNING,
			(errmsg("Rocksdb: commit exec failed, err = %s", err)));
    }
	txn_engine->commit_ts = commit_ts;
	txn_engine->status = COMMITTED;

	rocks_transaction_destroy(interface, gxid);
	return success;
}

bool
rocks_transaction_commit_with_lts_and_destroy(KVEngineTransactionInterface* interface, 
                                        DistributedTransactionId gxid, 
                                        uint64 commit_ts, uint32 lts)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	char *err = NULL;
	bool success = true;
    rocksdb_transaction_commit_with_lts(txn_engine->PRIVATE_txn, &err, commit_ts, lts);
	if (err)
    {
		success = false;
        int test = 0;
        if (test)
            ereport(WARNING,
			(errmsg("Rocksdb: commit lts exec failed, err = %s", err)));
    }
	txn_engine->commit_ts = commit_ts;
	txn_engine->status = COMMITTED;

	rocks_transaction_destroy(interface, gxid);
	return success;
}

void
rocks_transaction_abort_and_destroy(KVEngineTransactionInterface* interface, 
                                    DistributedTransactionId gxid)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	char *err = NULL;

    rocksdb_transaction_rollback(txn_engine->PRIVATE_txn, &err);
	if (err)
    {
        int test = 0;
        if (test)
            ereport(WARNING,
			(errmsg("Rocksdb: abort exec failed, err = %s", err)));
    }
	txn_engine->status = ABORTED;
    rocks_transaction_destroy(interface, gxid);
}

void
rocks_transaction_destroy(KVEngineTransactionInterface* interface, 
                          DistributedTransactionId gxid)
{
	TXNEngine *txn_engine = (TXNEngine*) interface;
	bool validate = txn_engine->lower < txn_engine->upper;
	List *readset = txn_engine->readSet;
	ListCell *rl;
	uint32 hashid = caculate_bucket(gxid);
	foreach(rl, readset)
	{
		KeyRange* ReadSetCeil = (KeyRange*) lfirst(rl);
		if (validate) 
		{
			RtsCache_addWithMutex(ReadSetCeil->startkey.data, 
								ReadSetCeil->startkey.len, 
								ReadSetCeil->endkey.data, 
								ReadSetCeil->isSingle ? 0 : ReadSetCeil->endkey.len,
								txn_engine->commit_ts);
		}
		KeyXidCache_removeReadXidWithMutex(ReadSetCeil->startkey.data, 
								ReadSetCeil->startkey.len, 
								ReadSetCeil->endkey.data, 
								ReadSetCeil->isSingle ? 0 : ReadSetCeil->endkey.len,
								(uint64_t)gxid);
		ADD_THREAD_LOCK_EXEC(RWSET);
		pfree(ReadSetCeil->startkey.data);
		pfree(ReadSetCeil->endkey.data);
		REMOVE_THREAD_LOCK_EXEC(RWSET);
	}

	List *writeset = txn_engine->writeSet;
	ListCell *wl;
	foreach(wl, writeset)
	{
		KeyRange* WriteSetCeil = (KeyRange*) lfirst(wl);
		KeyXidCache_removeWriteXidWithMutex(WriteSetCeil->startkey.data, 
								WriteSetCeil->startkey.len, 
								WriteSetCeil->endkey.data, 
								WriteSetCeil->isSingle ? 0 : WriteSetCeil->endkey.len,
								(uint64_t)gxid);
		ADD_THREAD_LOCK_EXEC(RWSET);
		pfree(WriteSetCeil->startkey.data);
		pfree(WriteSetCeil->endkey.data);
		REMOVE_THREAD_LOCK_EXEC(RWSET);
	}
	ADD_THREAD_LOCK_EXEC(RWSET);
	list_free(txn_engine->readSet);
	txn_engine->readSet = NULL;
	list_free(txn_engine->writeSet);
	txn_engine->writeSet = NULL;
	REMOVE_THREAD_LOCK_EXEC(RWSET);

    rocksdb_transaction_destroy(txn_engine->PRIVATE_txn);
	release_txnengine_mutex(interface);
	ADD_TxnHTAB_LOCK_EXEC(hashid);
	hash_search(transaction_env_htab[hashid], (void*) &gxid, HASH_REMOVE, NULL);
	REMOVE_TxnHTAB_LOCK_EXEC(hashid);
}
