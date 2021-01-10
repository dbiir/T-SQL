/*-------------------------------------------------------------------------
 *
 * rocks_engine.h
 *	  functions for rocksdb storage engine.
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


static KVEngineIteratorInterface* rocks_engine_create_iterator(KVEngineInterface* interface, bool isforward);
// extern KVEngineBatchInterface* rocks_engine_create_batch(KVEngineInterface* interface, DistributedTransactionId gxid, bool flag);

static TupleValueSlice rocks_engine_get(KVEngineInterface* interface, TupleKeySlice key, uint64_t *cts, uint64_t *nts);
static void rocks_engine_put(KVEngineInterface* interface, TupleKeySlice key, TupleValueSlice value);
static void rocks_engine_delete_direct(KVEngineInterface* interface, TupleKeySlice key);

KVEngineInterface*
rocks_create_normal_engine()
{
	RocksEngine *rocks_engine = palloc0(sizeof(*rocks_engine));
	rocks_engine->PRIVATE_opt = create_and_init_options();
	rocks_engine->PRIVATE_db = open_db(rocks_engine->PRIVATE_opt);
	rocks_engine->PRIVATE_be = open_backup_engine(rocks_engine->PRIVATE_opt);
	rocks_init_engine_interface((KVEngineInterface*) rocks_engine);
	return (KVEngineInterface*) rocks_engine;
}

void
rocks_init_engine_interface(KVEngineInterface* interface)
{
	interface->destroy = rocks_engine_destroy;
	interface->create_iterator = rocks_engine_create_iterator;
	interface->create_batch = rocks_engine_create_batch;
	interface->get = rocks_engine_get;
	interface->put = rocks_engine_put;
	interface->delete_direct = rocks_engine_delete_direct;
}

KVEngineIteratorInterface*
rocks_engine_create_iterator(KVEngineInterface* interface, bool isforward)
{
	RocksEngine *rocks_engine = (RocksEngine*) interface;
	RocksEngineIterator *rocks_it = palloc0(sizeof(*rocks_it));
	rocks_it->PRIVATE_readopt = rocksdb_readoptions_create();
	rocks_it->PRIVATE_it = rocksdb_create_iterator(rocks_engine->PRIVATE_db, rocks_it->PRIVATE_readopt);
	if (isforward)
		rocks_engine_init_iterator_interface((KVEngineIteratorInterface*) rocks_it);
	else
		rocks_engine_init_iterator_interface_pre((KVEngineIteratorInterface*) rocks_it);
	return (KVEngineIteratorInterface*) rocks_it;
}


KVEngineBatchInterface*
rocks_engine_create_batch(KVEngineInterface* interface, DistributedTransactionId gxid, bool flag)
{
	bool found;
	if (flag)
	{
		ADD_THREAD_LOCK_EXEC(WBHTAB);
		RocksBatchEngineEntry *rocks_batch_entry = hash_search(writebatch_env_htab, (void*) &gxid, HASH_ENTER, &found);
		REMOVE_THREAD_LOCK_EXEC(WBHTAB);
		if (!found)
		{
			MemSet(&rocks_batch_entry->val, 0, sizeof(rocks_batch_entry->val));
			rocks_batch_entry->val.PRIVATE_writebatch_wi = rocksdb_writebatch_wi_create(0,1);
			rocks_engine_init_batch_interface((KVEngineBatchInterface*) &rocks_batch_entry->val);
			return (KVEngineBatchInterface*) &rocks_batch_entry->val;
		}
		else
			return (KVEngineBatchInterface*) &rocks_batch_entry->val;
	}
	else
	{
		ADD_THREAD_LOCK_EXEC(WBHTAB);
		RocksBatchEngineEntry *rocks_batch_entry = hash_search(writebatch_env_htab, (void*) &gxid, HASH_FIND, &found);
		REMOVE_THREAD_LOCK_EXEC(WBHTAB);

		if (!found)
			return NULL;
		else
			return (KVEngineBatchInterface*) &rocks_batch_entry->val;
	}
}

void
rocks_engine_init_batch_interface(KVEngineBatchInterface* interface)
{
	interface->abort_and_destroy = rocks_engine_batch_abort_and_destroy;
	interface->commit_and_destroy = rocks_engine_batch_commit_and_destroy;
	interface->create_batch_iterator = rocks_engine_create_batch_iterator;
	interface->get = rocks_engine_batch_get;
	interface->put = rocks_engine_batch_put;
	interface->delete = rocks_engine_batch_delete_direct;
}

TupleValueSlice
rocks_engine_get(KVEngineInterface* interface, TupleKeySlice key, uint64_t *cts, uint64_t *nts)
{
	RocksEngine* rocks_engine = (RocksEngine*) interface;
	TupleValueSlice value = {NULL, 0};
	uint64_t ccts = 0, nnts = 0;
	rocksdb_readoptions_t *readopt = rocksdb_readoptions_create();
	char *err = NULL;
	value.data = (TupleValue) rocksdb_get(rocks_engine->PRIVATE_db, readopt, (char*) key.data, key.len, &value.len, &err);
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

void
rocks_engine_put(KVEngineInterface* interface, TupleKeySlice key, TupleValueSlice value)
{
	RocksEngine* rocks_engine = (RocksEngine*) interface;
	rocksdb_writeoptions_t *writeopt = rocksdb_writeoptions_create();
	char *err = NULL;
	rocksdb_put(rocks_engine->PRIVATE_db, writeopt, (char*) key.data, key.len, (char*) value.data, value.len, &err);
	if (err)
		ereport(ERROR,
			(errmsg("Rocksdb: put exec failed, err = %s",
					err)));
	rocksdb_writeoptions_destroy(writeopt);
}

void
rocks_engine_delete_direct(KVEngineInterface* interface, TupleKeySlice key)
{
	RocksEngine* rocks_engine = (RocksEngine*) interface;
	rocksdb_writeoptions_t *writeopt = rocksdb_writeoptions_create();
	char *err = NULL;
	rocksdb_delete(rocks_engine->PRIVATE_db, writeopt, (char*) key.data, key.len, &err);
	if (err)
		ereport(ERROR,
			(errmsg("Rocksdb: delete direct exec failed, err = %s",
					err)));
	rocksdb_writeoptions_destroy(writeopt);
}

/*
 * WriteBatch With Index API
 */
void
rocks_engine_batch_commit_and_destroy(KVEngineInterface* interface, KVEngineBatchInterface* batch_interface, DistributedTransactionId gxid)
{
	RocksEngine* rocks_engine = (RocksEngine*) interface;
	RocksBatchEngine* rocks_batch = (RocksBatchEngine*) batch_interface;
	rocksdb_writeoptions_t *writeopt = rocksdb_writeoptions_create();
	char *err = NULL;
	if (rocks_batch->PRIVATE_writebatch_wi)
	{
		rocksdb_write_writebatch_wi(rocks_engine->PRIVATE_db, writeopt, rocks_batch->PRIVATE_writebatch_wi, &err);
		rocksdb_writebatch_wi_destroy(rocks_batch->PRIVATE_writebatch_wi);
	}

	ADD_THREAD_LOCK_EXEC(WBHTAB);
	hash_search(writebatch_env_htab, (void*) &gxid, HASH_REMOVE, NULL);
	REMOVE_THREAD_LOCK_EXEC(WBHTAB);
	rocksdb_writeoptions_destroy(writeopt);
}

void
rocks_engine_batch_abort_and_destroy(KVEngineInterface* interface, KVEngineBatchInterface* batch_interface, DistributedTransactionId gxid)
{
	RocksBatchEngine* rocks_batch = (RocksBatchEngine*) batch_interface;
	rocksdb_writeoptions_t *writeopt = rocksdb_writeoptions_create();
	if (rocks_batch->PRIVATE_writebatch_wi)
	{
		rocksdb_writebatch_wi_destroy(rocks_batch->PRIVATE_writebatch_wi);
	}
	ADD_THREAD_LOCK_EXEC(WBHTAB);
	hash_search(writebatch_env_htab, (void*) &gxid, HASH_REMOVE, NULL);
	REMOVE_THREAD_LOCK_EXEC(WBHTAB);
	rocksdb_writeoptions_destroy(writeopt);
}

TupleValueSlice
rocks_engine_batch_get(KVEngineInterface* interface, KVEngineBatchInterface* batch_interface, TupleKeySlice key, uint64_t *cts, uint64_t *nts)
{
	RocksEngine* rocks_engine = (RocksEngine*) interface;
	RocksBatchEngine* rocks_batch = (RocksBatchEngine*) batch_interface;
	TupleValueSlice value = {NULL, 0};
	rocksdb_readoptions_t *readopt = rocksdb_readoptions_create();
	char *err = NULL;
	value.data = (TupleValue) rocksdb_writebatch_wi_get_from_batch_and_db(
														rocks_batch->PRIVATE_writebatch_wi,
														rocks_engine->PRIVATE_db,
														readopt, (char*) key.data,
														key.len, &value.len, &err);
	if (err)
		ereport(ERROR,
			(errmsg("Rocksdb: batch get exec failed, err = %s",
					err)));
	rocksdb_readoptions_destroy(readopt);
	return value;
}

void
rocks_engine_batch_delete_direct(KVEngineBatchInterface* interface, TupleKeySlice key)
{
	RocksBatchEngine* rocks_batch = (RocksBatchEngine*) interface;
	char *err = NULL;
	rocksdb_writebatch_wi_delete(rocks_batch->PRIVATE_writebatch_wi, (char*) key.data, key.len);
	if (err)
		ereport(ERROR,
			(errmsg("Rocksdb: delete direct exec failed, err = %s",
					err)));
}

void
rocks_engine_batch_put(KVEngineBatchInterface* interface, TupleKeySlice key, TupleValueSlice value)
{
	RocksBatchEngine* rocks_batch = (RocksBatchEngine*) interface;
	rocksdb_writebatch_wi_put(rocks_batch->PRIVATE_writebatch_wi, (char*) key.data,  key.len, (char*) value.data, value.len);
}

KVEngineIteratorInterface*
rocks_engine_create_batch_iterator(KVEngineIteratorInterface* engine_it, KVEngineBatchInterface* batch_interface)
{
	RocksEngineIterator* rocks_it = (RocksEngineIterator*) engine_it;
	RocksBatchEngine* rocks_batch = (RocksBatchEngine*) batch_interface;
	rocksdb_iterator_t* iter = rocksdb_writebatch_wi_create_iterator_with_base(rocks_batch->PRIVATE_writebatch_wi, rocks_it->PRIVATE_it);
	rocks_it->PRIVATE_it = iter;
	return (KVEngineIteratorInterface*) rocks_it;
}
