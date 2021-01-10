/*-------------------------------------------------------------------------
 *
 * kv_universal.h
 *
 * NOTES
 *	  Used to store some of the more common functions of the kv storage
 *    engine, such as functions used to determine visibility or to
 *    encapsulate data structures.
 *
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/kv_universal.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef KV_UNIVERSAL_H
#define KV_UNIVERSAL_H
#include "postgres.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tupmacs.h"
#include "access/xlogutils.h"
#include "access/heapam.h"
#include "access/memtup.h"
#include "nodes/primnodes.h"
#include "storage/block.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "access/memtup.h"
#include "executor/tuptable.h"
#include "tdb/kv_struct.h"
#include "tdb/range_struct.h"

extern char* TransferSegIDToIPPortList(SegmentID* segid, int segcount, int *length);

extern HeapTuple kvengine_make_fake_heap(TupleKeySlice key, TupleValueSlice value);
extern Relation kvengine_make_fake_relation(void);
extern void kvengine_free_fake_heap(HeapTuple htup);
extern void kvengine_free_fake_relation(Relation rel);
extern bool kvengine_judge_dirty(TupleValue value, HeapTuple htup);
extern DataSlice* get_slice_from_buffer(void* buffer);
extern void initial_start_key(KVEngineScanDesc scan, TupleKeySlice *key);
extern void initial_next_key(KVEngineScanDesc scan, TupleKeySlice *key);
extern void initial_end_key(KVEngineScanDesc scan, TupleKeySlice *key);
extern void initial_end_next_key(KVEngineScanDesc scan, TupleKeySlice *key);
extern TupleKeySlice initial_key(Relation rel, Oid indexoid, int *pk_colnos, int att_num, int pk_att_num, TupleTableSlot* slot, Datum* values, bool* isnull, bool isend, TransactionId xid, CommandId cid, bool isSecondary);
extern void kvengine_get_by_pk(KVEngineScanDesc scan, TupleTableSlot* slot, TupleKeySlice* all_key);
extern TupleKeySlice get_tuple_key_from_buffer(char* buffer);
extern TupleValueSlice get_tuple_value_from_buffer(char* buffer);

extern TupleValueSlice pick_tuple_value_from_buffer(char *buffer);
extern TupleKeySlice pick_tuple_key_from_buffer(char *buffer);
extern void save_tuple_value_into_buffer(char *buffer, TupleValueSlice value);
extern void save_tuple_key_into_buffer(char *buffer, TupleKeySlice key);

extern Oid* get_attr_type(Relation rel, int* colnos, int natts);
extern Oid get_pk_oid(Relation rel);
extern List* get_index_oid(Relation rel);
extern int* get_index_colnos(Oid index_oid, int* natts);

extern TupleKeySlice build_key(InitKeyDesc initkey);
extern InitKeyDesc init_basis_in_keydesc(int key_type);
extern void get_value_from_slot(TupleTableSlot* slot,
					int *colnos,
					int att_num,
					Datum *values,
					bool *isnull);
extern void init_pk_in_keydesc(InitKeyDesc *initkey,
			Oid	pk_oid,
			Oid* pk_att_type,
			Datum* pk_value,
			bool* pkisnull,
			int pk_natts);
extern void init_sk_in_keydesc(InitKeyDesc *initkey,
			Oid	sk_oid,
			Oid* sk_att_type,
			Datum* sk_value,
			bool* skisnull,
			int sk_natts);

extern RocksUpdateFailureData initRocksUpdateFailureData(void);
extern Size getRocksUpdateFailureDataLens(RocksUpdateFailureData rufd);
extern char* encodeRocksUpdateFailureData(RocksUpdateFailureData rufd);
extern RocksUpdateFailureData decodeRocksUpdateFailureData(char* buffer);
extern TupleKeySlice copy_key(TupleKeySlice key);
extern TupleValueSlice copy_value(TupleValueSlice value);
extern bool equal_key(TupleKeySlice a, TupleKeySlice b);

#endif
