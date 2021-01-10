/*----------------------------------
 *
 * tdbkvam.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/tdbkvam.h
 *----------------------------------
 */

#ifndef TDB_ROCKSAM_H
#define TDB_ROCKSAM_H

#include "c.h"
#include "tdb/kv_universal.h"


extern KVEngineDeleteDesc kvengine_delete_init(Relation rel);
extern HTSU_Result kvengine_delete(KVEngineDeleteDesc desc, TupleKeySlice key, CommandId cid, Snapshot crosscheck, RocksUpdateFailureData* rufd);
extern void kvengine_delete_finish(KVEngineDeleteDesc desc);
extern KVEngineInsertDesc kvengine_insert_init(Relation rel);
extern Oid kvengine_insert(KVEngineInsertDesc desc, TupleTableSlot* slot, CommandId cid);
extern void kvengine_his_insert(Relation rel, TupleTableSlot* slot);
extern void kvengine_refresh_historical_kvs(bool isvacuum);
extern bool kvengine_index_insert(KVEngineInsertDesc desc, TupleTableSlot *slot, Oid indexOid, bool checkUnique);
extern void kvengine_insert_finish(KVEngineInsertDesc desc);
extern KVEngineScanDesc kvengine_beginscan(Relation relation, Snapshot snapshot);
extern KVEngineScanDesc kvengine_index_begin_scan(IndexScanDesc indexscandesc);
extern KVEngineScanDesc kvengine_beginscan_check_unique(Relation relation, Oid indexOid, Snapshot snapshot, TupleTableSlot *slot, Datum* values);
extern void kvengine_getnext(KVEngineScanDesc scan, ScanDirection direction, TupleTableSlot* slot);
extern void kvengine_indexgetnext(KVEngineScanDesc scan, TupleTableSlot* slot);
extern void kvengine_endscan(KVEngineScanDesc scan);
extern void kvengine_rescan(KVEngineScanDesc scan);

extern bool kv_index_insert(Relation indexRelation, TupleTableSlot *slot, ItemPointer heap_t_ctid, Relation rocksRelation, IndexUniqueCheck checkUnique);

extern ResponseHeader* send_kv_request(RequestHeader *requestmessage);
extern GetResponse* kvengine_send_get_req(TupleKeySlice key);
extern PutResponse* kvengine_send_put_req(TupleKeySlice key, TupleValueSlice value, RangeID rangeid, bool checkUnique, bool isSecondary, Snapshot SnapshotDirty);

extern DeleteDirectResponse* kvengine_send_delete_direct_req(TupleKeySlice key);
extern ScanResponse* kvengine_delete_req(TupleKeySlice key, Snapshot snap);
extern ScanResponse* kvengine_send_index_scan_req(TupleKeySlice origstartkey, TupleKeySlice origendkey, TupleKeySlice startkey, TupleKeySlice endkey, Snapshot snap, bool forward, bool second, int req_type, CmdType Scantype);

extern void kvengine_free_scandesc_key_value(KVEngineScanDesc scan);
extern void kvengine_free_scandesc(KVEngineScanDesc scan);
extern void second_and_search_again(KVEngineScanDesc scan, TupleTableSlot* slot);
extern void pick_to_slot(KVEngineScanDesc scan, TupleTableSlot* slot);
extern void kvengine_supplement_tuples(KVEngineScanDesc scan, int req_type);
extern HTSU_Result handle_delete(TupleKeySlice key, TupleValueSlice value, Relation rel, CommandId cid, Snapshot crosscheck);
extern bool kvengine_check_unique_and_insert(Relation rel, TupleKeySlice key, TupleValueSlice value, RangeID rangeid, bool isSecondary);
extern bool kvengine_delete_direct(TupleKeySlice key);

extern KVEngineScanDesc initKVEngineScanDesc(Relation rel, Oid index_id, Snapshot snapshot, TupleKeySlice* startkey, TupleKeySlice* endkey, bool isforward);
extern KVEngineScanDesc kvengine_beginscan_by_key(Relation relation, Oid index_id, Snapshot snapshot, TupleKeySlice* startkey, TupleKeySlice* endkey, bool isforward);
extern bool kvengine_commit(void);
extern bool kvengine_abort(void);
extern bool kvengine_prepare(void);
extern bool kvengine_clear(void);

extern bool Rangeengine_create_paxos(RangeID rangeid, SegmentID* seglist, int segcount);
extern bool Rangeengine_remove_paxos(RangeID rangeid);
extern bool Rangeengine_add_replica(RangeID rangeid, SegmentID segid);
extern bool Rangeengine_remove_replica(RangeID rangeid, SegmentID segid);

extern void put_value_into_slot(TupleKeySlice key, TupleValueSlice value, TupleTableSlot *slot);
extern HTSU_Result kvengine_update(KVEngineUpdateDesc desc, TupleTableSlot* slot, TupleKeySlice key, CommandId cid, Snapshot crosscheck, RocksUpdateFailureData* rufd, int retry_time);
extern void kvengine_update_finish(KVEngineUpdateDesc desc);
extern KVEngineUpdateDesc kvengine_update_init(Relation rel);
extern void kvengine_acquire_tuplock(Relation relation, TupleKeySlice key, LockTupleMode mode, bool nowait, bool *have_tuple_lock);
extern void range_free(void *pointer);

extern void rfree(void *pointer);
extern void* ralloc0(Size size);

extern MultiPutResponse* kvengine_send_multi_put_req(void);
extern Size get_multi_send_buffer_size(void);
extern char* store_kvs_into_buffer_and_clean(void* buf);
extern ScanResponse* kvengine_send_history_scan_req(TupleKeySlice startkey, TupleKeySlice endkey, long time, bool isforward);

extern void kvengine_history_supplement_tuples(KVEngineScanDesc scan, TupleTableSlot* slot);

extern void pick_his_to_slot(KVEngineScanDesc scan, TupleTableSlot* slot);

extern KVEngineScanDesc kvengine_history_beginscan(IndexScanDesc indexscandesc, long xmin_ts, long xmax_ts);
extern KVEngineScanDesc kvengine_his_normal_beginscan(Relation relation, long xmin_ts, long xmax_ts);
extern void kvengine_history_getnext(KVEngineScanDesc scan, TupleTableSlot* slot);

#endif
