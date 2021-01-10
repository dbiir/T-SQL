/*------------------------------------------------------------------------------
 *
 * storage_engine.h
 *
 * Copyright (c) 2019 TDSQL.
 *
 * interfaces for storage engines.
 *
 * IDENTIFICATION
 *	    src/include/access/storage_engine.h
 *
 *------------------------------------------------------------------------------
 */
#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include "c.h"
#include "postgres_ext.h"


typedef struct Timestamp
{
    TransactionId xmin;
    TransactionId xmax;
} Timestamp;

typedef struct Key
{
    char *k;
    int size;
} Key;

typedef struct MVCCKey
{
    Key *key;
    Timestamp *timestamp;
} MVCCKey;

typedef struct KeyValue
{
    MVCCKey *key;
    char *value;
    int value_size;
} KeyValue;

typedef IteratorStats
{
    //...
} IteratorStats;

typedef struct Iterator
{
    void (*Close) (char **err);
    void (*Seek) (const MVCCKey *key, char **err);
    bool (*Valid) (char **err);
    bool (*Next) (char **err);
    bool (*Prev) (char **err);
    MVCCKey* (*Key) (char **err);
    KeyValue* (*Value) (char **err);
    void (*SetLowerBound) (const Key *key, char **err);
    void (*SetUpperBound) (const Key *key, char **err);
    void (*ClearRange) (const MVCCKey *start, const MVCCKey *end, char **err);

    // get iterator statistics
    IteratorStats* (*GetStats) (char **err);
} Iterator;

typedef struct ReaderStats
{
    // ...
} ReaderStats;

typedef struct Reader
{
    void (*Open) (char **err);
    void (*Close) (char **err);
    bool (*IsClosed) (char **err);
    KeyValue* (*Get) (const MVCCKey *key, char **err);
    Iterator* (*Scan) (const Key *start, const Key *end, const long maxCount, void (*func)(KeyValue*), char **err);
    Iterator* (*Snapshot) (const Key *start, const Key *end, const Timestamp* timestamp, const long maxCount, void (*func)(KeyValue*), char **err);

    // get reader statistics
    ReaderStats* (*GetStats) (char **err);
} Reader;

typedef struct Batch
{
    void (*add) (const KeyValue *kv);
    void (*remove) (const MVCCKey *key);
    int length;
}

typedef struct WriterStats
{
    // ...
} WriterStats;

typedef struct Writer
{
    void (*Open) (char **err);
    void (*Close) (char **err);
    bool (*IsClosed) (char **err);
    void (*Clear) (char **err);
    void (*ClearRange) (const MVCCKey *start, const MVCCKey *end, char **err) ();
    void (*WriteBatch) (const Batch *batch, char **err);
    void (*WriteIterator) (const Iterator *iter, char **err);
    void (*Put) (const KeyValue *kv, char **err);
    void (*Merge) (const MVCCKey *key, char **err);
    void (*Commit) (bool sync) (char **err);
    Iterator* (*Distinct) (char **err);

    // get writer statistics
    WriterStats* (*GetStats) (char **err);
} Writer;

typedef struct StorageAttr
{
    //...
} StorageAttr;

typedef struct StorageStats
{
    // ...
} StorageStats;

typedef struct StorageEngine
{
    StorageAttr* (*GetAttr) (char **err);
    void (*Flush) (char **err);
    void (*CompactRange) (const Key *start, const Key *end, char **err);
    Reader* (*NewReader) (const char *spaceName, char **err);
    Writer* (*NewWriter) (const char *spaceName, char **err);

    StorageStats* (*GetStats) (char **err);
} StorageEngine;

StorageEngine* (*GetStorageEngine) (char **err);

typedef struct MetaEntry
{
    char *spaceName;
    char *relname;
    List *identkeys;
    //...
} MetaEntry;

typedef struct StorageMeta
{
    void (*InsertMetaEntry) (Oid relid, const MetaEntry *entry, char **err);
    void (*UpdateMetaEntry) (Oid relid, const MetaEntry *entry, char **err);
    MetaEntry* (*GetMetaEntry) (Oid relid, char **err);
    void (*RemoveMetaEntry) (Oid relid, char **err);
} StorageMeta;

#endif
