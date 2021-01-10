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

HTAB *writebatch_env_htab = NULL;
HTAB *transaction_env_htab[TXN_BUCKET];
HTAB *TuplekvLock_env_htab = NULL;

rocksdb_options_t*
create_and_init_options()
{
	rocksdb_options_t *options = rocksdb_options_create();

	/* Optimize RocksDB. This is the easiest way to get RocksDB to perform well. */
	long cpus = sysconf(_SC_NPROCESSORS_ONLN);	/* get # of online cores */
	
	rocksdb_options_increase_parallelism(options, (int)(cpus));
	//rocksdb_options_optimize_level_style_compaction(options, 1 << 30);
	rocksdb_options_set_max_background_compactions(options, (int)(cpus));
	//rocksdb_env_set_background_threads(options, (int)(cpus));

	/* Create the DB if it's not already present. */
	rocksdb_options_set_create_if_missing(options, 1);

	return options;
}

rocksdb_t*
open_db(rocksdb_options_t* options)
{
	const char* cf_name[3] = {"default", "lts", RocksDBSystemCatalog};
	const rocksdb_options_t* cf_options[3] = {options, options, options};
	rocksdb_column_family_handle_t* cf_handle[3];
	char *err = NULL;
	rocksdb_t *db = rocksdb_open_column_families(options, RocksDBPath, 3, cf_name, cf_options, cf_handle, &err);
	if (err)
	{
		ereport((ERROR),(errmsg_internal("engine open failed, (err message:%s)",
					err)));
	}
	rocksdb_column_family_handle_destroy(cf_handle[1]);
	return db;
}

rocksdb_backup_engine_t*
open_backup_engine(rocksdb_options_t* options)
{
	char *err = NULL;
	rocksdb_backup_engine_t *be = rocksdb_backup_engine_open(options, RocksDBBackupPath, &err);
	if (err)
	{
		ereport((ERROR),(errmsg_internal("backup engine open failed, (err message:%s)",
					err)));
	}
	return be;
}

void
rocks_engine_destroy(KVEngineInterface* interface)
{
	RocksEngine *rocks_engine = (RocksEngine*) interface;
	char *err = NULL;
	rocksdb_backup_engine_create_new_backup(rocks_engine->PRIVATE_be,
                                    rocks_engine->PRIVATE_db, &err);
	if (err)
		ereport(ERROR,
			(errmsg("Rocksdb: destory exec failed, err = %s",
					err)));
	rocksdb_options_destroy(rocks_engine->PRIVATE_opt);
	rocksdb_backup_engine_close(rocks_engine->PRIVATE_be);
	rocksdb_close(rocks_engine->PRIVATE_db);
	pfree(rocks_engine);
    rocks_engine = NULL;
}

/*Iter section*/
void
rocks_engine_init_iterator_interface(KVEngineIteratorInterface* interface)
{
	interface->destroy = rocks_engine_iterator_destroy;
	interface->seek = rocks_engine_iterator_seek;
	interface->is_valid = rocks_engine_iterator_is_valid;
	interface->next = rocks_engine_iterator_next;
	interface->get = rocks_engine_iterator_get;
	interface->ts = rocks_engine_iterator_ts;
}

void
rocks_engine_init_iterator_interface_pre(KVEngineIteratorInterface* interface)
{
	interface->destroy = rocks_engine_iterator_destroy;
	interface->seek = rocks_engine_iterator_seek_pre;
	interface->is_valid = rocks_engine_iterator_is_valid;
	interface->next = rocks_engine_iterator_next_pre;
	interface->get = rocks_engine_iterator_get;
	interface->ts = rocks_engine_iterator_ts;
}

void
rocks_engine_iterator_destroy(KVEngineIteratorInterface* interface)
{
	RocksEngineIterator* rocks_it = (RocksEngineIterator*) interface;
	rocksdb_readoptions_destroy(rocks_it->PRIVATE_readopt);
	rocksdb_iter_destroy(rocks_it->PRIVATE_it);
	pfree(rocks_it);
    rocks_it = NULL;
}

void
rocks_engine_iterator_seek(KVEngineIteratorInterface* interface, TupleKeySlice key)
{
	RocksEngineIterator* rocks_it = (RocksEngineIterator*) interface;
	rocksdb_iter_seek(rocks_it->PRIVATE_it, (char*) key.data, key.len);
}

void
rocks_engine_iterator_seek_pre(KVEngineIteratorInterface* interface, TupleKeySlice key)
{
	RocksEngineIterator* rocks_it = (RocksEngineIterator*) interface;
	rocksdb_iter_seek_for_prev(rocks_it->PRIVATE_it, (char*) key.data, key.len);
}

bool
rocks_engine_iterator_is_valid(KVEngineIteratorInterface* interface)
{
	RocksEngineIterator* rocks_it = (RocksEngineIterator*) interface;
	return rocksdb_iter_valid(rocks_it->PRIVATE_it);
}

void
rocks_engine_iterator_ts(KVEngineIteratorInterface* interface, uint64_t *cts, uint64_t *nts)
{
	RocksEngineIterator* rocks_it = (RocksEngineIterator*) interface;
	uint64_t ccts = 0, nnts = kMaxSequenceNumber;
	rocksdb_iter_ts(rocks_it->PRIVATE_it, &ccts, &nnts);
	if (ccts != 0)
		*cts = ccts;
	if (nnts != 0)
		*nts = nnts;
}

void
rocks_engine_iterator_next(KVEngineIteratorInterface* interface)
{
	RocksEngineIterator* rocks_it = (RocksEngineIterator*) interface;
	rocksdb_iter_next(rocks_it->PRIVATE_it);
}

void
rocks_engine_iterator_next_pre(KVEngineIteratorInterface* interface)
{
	RocksEngineIterator* rocks_it = (RocksEngineIterator*) interface;
	rocksdb_iter_prev(rocks_it->PRIVATE_it);
}

void
rocks_engine_iterator_get(KVEngineIteratorInterface* interface, TupleKeySlice* key, TupleValueSlice* value)
{
	RocksEngineIterator* rocks_it = (RocksEngineIterator*) interface;
	key->data = (TupleKey) rocksdb_iter_key(rocks_it->PRIVATE_it, &key->len);
	value->data = (TupleValue) rocksdb_iter_value(rocks_it->PRIVATE_it, &value->len);
}

/*
 * Hash table for xid->writebatch
 */
void
StorageInitWritebatchHash()
{
	HASHCTL info;
	info.keysize = sizeof(DistributedTransactionId);
	info.entrysize = sizeof(RocksBatchEngineEntry);
	info.hash = oid_hash;
	writebatch_env_htab = hash_create("Writebatch hash", 2*MaxBackends, &info, HASH_ELEM | HASH_FUNCTION);
}

void
StorageInitTransactionBatchHash()
{
	HASHCTL info;
	info.keysize = sizeof(DistributedTransactionId);
	info.entrysize = sizeof(TXNEngineEntry);
	info.hash = gxid_hash;
	for (int i = 0; i < TXN_BUCKET; i++)
	{
		char name[50] = {0};
		printf(name, "Transactionbatch hash %d ", i);
		transaction_env_htab[i] = hash_create(name, 2*MaxBackends, &info, HASH_ELEM | HASH_FUNCTION);
	}
}

bool is_cf_options_valid(const char *cfoptions_str)
{
	char *err = NULL;
	rocksdb_options_t *options = rocksdb_options_create();
	rocksdb_get_options_from_string(options, cfoptions_str, options, &err);
	return err == NULL;
}

void
StorageInitTupleLockHash()
{
	HASHCTL info;
	info.keysize = sizeof(DataSliceHash);
	info.entrysize = sizeof(TuplekvLock);
	info.hash = tag_hash;
	TuplekvLock_env_htab = hash_create("TuplekvLock hash", 100000, &info, HASH_ELEM | HASH_FUNCTION);
}

void
acquireTuplekvLock(TupleKeySlice key)
{
    DataSliceHash *data = palloc0(sizeof(DataSliceHash));
    memcpy(data->key, key.data, key.len);
    bool found;
    ADD_THREAD_LOCK_EXEC(TupleLockHash);
    TuplekvLock *kvlock = hash_search(TuplekvLock_env_htab, (void*) data, HASH_ENTER, &found);
    range_free(data);
    if (!found)
    {
        memset(kvlock->dataslice.key, 0, sizeof(DataSliceHash));

        memcpy(kvlock->dataslice.key, key.data, key.len);
        kvlock->num = 0;
        pthread_mutex_init (&(kvlock->tuple_lock), NULL);
    }
    REMOVE_THREAD_LOCK_EXEC(TupleLockHash);

    ADD_THREAD_LOCK_EXEC(TupleLockHash);
    kvlock->num ++;
    REMOVE_THREAD_LOCK_EXEC(TupleLockHash);
    pthread_mutex_lock(&kvlock->tuple_lock);
}

void
acquireTuplekvLockAndBool(TupleKeySlice key, bool *have_lock)
{
    if (*have_lock == true)
        return;
    else
    {
        acquireTuplekvLock(key);
        *have_lock = true;
    }
}   

void
releaseTuplekvLock(TupleKeySlice key)
{
    DataSliceHash *data = palloc0(sizeof(DataSliceHash));

    memcpy(data->key, key.data, key.len);
    bool found;
    ADD_THREAD_LOCK_EXEC(TupleLockHash);
    TuplekvLock *kvlock = hash_search(TuplekvLock_env_htab, (void*) data, HASH_FIND, &found);
    REMOVE_THREAD_LOCK_EXEC(TupleLockHash);
    if (!found)
    {
        core_dump();
    }
    else
    {
        ADD_THREAD_LOCK_EXEC(TupleLockHash);
        kvlock->num --;
        REMOVE_THREAD_LOCK_EXEC(TupleLockHash);
        pthread_mutex_unlock(&kvlock->tuple_lock);
    }
    /*ADD_THREAD_LOCK_EXEC(TupleLockHash);
    if (kvlock->num == 0)
        hash_search(TuplekvLock_env_htab, (void*) data, HASH_REMOVE, NULL);
    REMOVE_THREAD_LOCK_EXEC(TupleLockHash);*/
    range_free(data);
}

void
releaseTuplekvLockAndBool(TupleKeySlice key, bool *have_lock)
{
    if (*have_lock == false)
        return;
    else
    {
        releaseTuplekvLock(key);
        *have_lock = false;
    }
}   
/**
 * Generate and fill the column family name into cfname.
 * cfname is compose of relname_relid.
 * relname is a NameData, which has mostly 64 chars.
 * but if it is longer than 32, it will be truncated in cfname.
 */
void fill_cf_name(Oid relid, const char *relname, Name cfname)
{
	char	tmp_relname[NAMEDATALEN];
	int 		tmp_relname_len = 0;

	Assert(relname != NULL);

	strlcpy(tmp_relname, relname, NAMEDATALEN);
	tmp_relname[32] = '\0';	/* truncate relname */
	namestrcpy(cfname, tmp_relname);
	tmp_relname_len = strlen(tmp_relname);

	cfname->data[tmp_relname_len] = '_';
	cfname->data[tmp_relname_len + 1] = '\0';

	sprintf(tmp_relname, "%d", relid);
	strlcat(cfname->data, tmp_relname, NAMEDATALEN);
}
