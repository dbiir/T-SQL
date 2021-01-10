
#ifndef KVENGINE_H
#define KVENGINE_H

#include "tdb/tdbkvam.h"

typedef enum KVEngineType
{
	KVENGINE_ROCKSDB,
	KVENGINE_TRANSACTIONDB,
} KVEngineType;

typedef struct KVEngineIteratorInterface
{
	void (*destroy)(struct KVEngineIteratorInterface *);
	void (*seek)(struct KVEngineIteratorInterface *, TupleKeySlice);
	bool (*is_valid)(struct KVEngineIteratorInterface *);
	void (*next)(struct KVEngineIteratorInterface *);
	void (*get)(struct KVEngineIteratorInterface *, TupleKeySlice *, TupleValueSlice *); /* do not free data */
	void (*ts)(struct KVEngineIteratorInterface *, uint64_t *, uint64_t *);
} KVEngineIteratorInterface;

typedef struct KVEngineInterface
{
	void (*destroy)(struct KVEngineInterface *);
	void (*compaction)(struct KVEngineInterface *);
	struct KVEngineIteratorInterface *(*create_iterator)(struct KVEngineInterface *, bool isforward);
	struct KVEngineBatchInterface *(*create_batch)(struct KVEngineInterface *, DistributedTransactionId gxid, bool flag);
	struct KVEngineTransactionInterface *(*create_txn)(struct KVEngineInterface *, DistributedTransactionId gxid, bool flag, uint64 startts);
	TupleValueSlice (*get)(struct KVEngineInterface *, TupleKeySlice);
	void (*put)(struct KVEngineInterface *, TupleKeySlice, TupleValueSlice);
	void (*delete_direct)(struct KVEngineInterface *, TupleKeySlice);
} KVEngineInterface;

typedef struct KVEngineBatchInterface
{
	void (*commit_and_destroy)(struct KVEngineInterface *, struct KVEngineBatchInterface *, DistributedTransactionId gxid);
	void (*abort_and_destroy)(struct KVEngineInterface *, struct KVEngineBatchInterface *, DistributedTransactionId gxid);
	struct KVEngineIteratorInterface *(*create_batch_iterator)(struct KVEngineIteratorInterface *, struct KVEngineBatchInterface *);
	TupleValueSlice (*get)(struct KVEngineInterface *, struct KVEngineBatchInterface *, TupleKeySlice);
	void (*put)(struct KVEngineBatchInterface *, TupleKeySlice, TupleValueSlice);
	void (*delete)(struct KVEngineBatchInterface *interface, TupleKeySlice key);
} KVEngineBatchInterface;

typedef struct KVEngineTransactionInterface
{
	bool (*prepare)(struct KVEngineTransactionInterface *, struct KVEngineInterface *, DistributedTransactionId gxid, uint64 start_ts);
	bool (*commit_and_destroy)(struct KVEngineTransactionInterface *, DistributedTransactionId gxid, uint64 commit_ts);
	bool (*commit_with_lts_and_destroy)(struct KVEngineTransactionInterface *, DistributedTransactionId gxid, uint64 commit_ts, uint32 lts);
	void (*abort_and_destroy)(struct KVEngineTransactionInterface *, DistributedTransactionId gxid);
	void (*destroy)(struct KVEngineTransactionInterface *, DistributedTransactionId gxid);

	void (*set_ts)(struct KVEngineTransactionInterface*, uint64_t, uint64_t);
	void (*insert_read_set)(struct KVEngineTransactionInterface*, TupleKeySlice, TupleKeySlice, bool, bool);
	void (*insert_write_set)(struct KVEngineTransactionInterface*, TupleKeySlice, TupleKeySlice, bool, bool);
	List* (*get_write_set)(struct KVEngineTransactionInterface*);
	List* (*get_read_set)(struct KVEngineTransactionInterface*);
	bool (*ts_is_validate)(struct KVEngineTransactionInterface*);
	void (*get_ts)(struct KVEngineTransactionInterface*, uint64_t*, uint64_t*);
	struct KVEngineIteratorInterface *(*create_iterator)(struct KVEngineTransactionInterface *, bool isforward, int cf_name);
	TupleValueSlice (*get)(struct KVEngineTransactionInterface *, TupleKeySlice, int cf_name, uint64_t *, uint64_t *);
	void (*put)(struct KVEngineTransactionInterface *, TupleKeySlice, TupleValueSlice, int cf_name);
	void (*delete)(struct KVEngineTransactionInterface *, TupleKeySlice, int cf_name);
	TupleValueSlice (*get_for_update)(struct KVEngineTransactionInterface *, TupleKeySlice, uint64_t *, uint64_t *);
} KVEngineTransactionInterface;

#endif
