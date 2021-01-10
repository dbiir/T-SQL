/*-------------------------------------------------------------------------
 *
 * kvam.c
 *		kv relation access method code
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *		src/backend/access/rocksdb/kv.c
 *
 *-------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <unistd.h>

#include "postgres.h"

#include "common/relpath.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute_encoding.h"
#include "utils/syscache.h"
#include "storage/lmgr.h"
#include "tdb/encode_type.h"
#include "tdb/session_processor.h"
#include "tdb/kv_universal.h"
#include "tdb/tdbkvam.h"
#include "tdb/historical_transfer.h"
#include "tdb/rangestatistics.h"
#include "cdb/cdbvars.h"
#include "utils/guc.h"
#include "tdb/route.h"
#include "tdb/range.h"
#include "tdb/storage_param.h"

#include "tdb/timestamp_transaction/timestamp_generate_key.h"
#include "tdb/pg_transaction/pgtransaction_generate_key.h"
#include "storage/lmgr.h"
#include "tdb/timestamp_transaction/lts_generate_key.h"
#include "tdb/timestamp_transaction/hlc.h"

#include "cdb/cdbdtxts.h"
#include "access/rwset.h"

#include "postmaster/autovacuum.h"
#include "postmaster/startup.h"

struct 
{
	HistoricalKV kvs[MAX_HIS_TRANSFER_KV_NUM];
	int num;
} his_transfer_kv_buffer;

/*
 * Each tuple lock mode has a corresponding heavyweight lock, and one or two
 * corresponding MultiXactStatuses (one to merely lock tuples, another one to
 * update them).  This table (and the macros below) helps us determine the
 * heavyweight lock mode and MultiXactStatus values to use for any particular
 * tuple lock strength.
 *
 * Don't look at lockstatus/updstatus directly!  Use get_mxact_status_for_lock
 * instead.
 */
static const struct
{
	LOCKMODE hwlock;
	int lockstatus;
	int updstatus;
}

tupleLockExtraInfo[MaxLockTupleMode + 1] =
	{
		{
			/* LockTupleKeyShare */
			AccessShareLock,
			MultiXactStatusForKeyShare,
			-1 /* KeyShare does not allow updating tuples */
		},
		{
			/* LockTupleShare */
			RowShareLock,
			MultiXactStatusForShare,
			-1 /* Share does not allow updating tuples */
		},
		{/* LockTupleNoKeyExclusive */
		 ExclusiveLock,
		 MultiXactStatusForNoKeyUpdate,
		 MultiXactStatusNoKeyUpdate},
		{/* LockTupleExclusive */
		 AccessExclusiveLock,
		 MultiXactStatusForUpdate,
		 MultiXactStatusUpdate}};

/*
 * Acquire heavyweight locks on tuples, using a LockTupleMode strength value.
 * This is more readable than having every caller translate it to lock.h's
 * LOCKMODE.
 */
#define LockTupleKVTuplock(rel, tup, mode) \
	LockKVTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)
#define UnlockTupleKVTuplock(rel, tup, mode) \
	UnlockKVTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)
#define ConditionalLockTupleKVTuplock(rel, tup, mode) \
	ConditionalLockKVTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)

static ScanResponse *kvengine_send_cycle_get_req(TupleKeySlice *all_key, int num, Snapshot snap, CmdType Scantype);

void *
ralloc0(Size size)
{
	void *pointer = malloc(size);
	memset(pointer, 0, size);
	return pointer;
}
void rfree(void *pointer)
{
	free(pointer);
	return;
}

void range_free(void *pointer)
{
	if (pointer != NULL)
		pfree(pointer);
	pointer = NULL;
	return;
}

DataSlice *
get_slice_from_buffer(void *buffer)
{
	Size keylen = *(Size *)buffer;
	DataSlice *req_key = (DataSlice *)palloc0(keylen + sizeof(Size));
	req_key->len = keylen;
	memcpy((void *)req_key, (void *)buffer, req_key->len + sizeof(Size));
	return req_key;
}

TupleValueSlice
get_tuple_value_from_buffer(char *buffer)
{
	TupleValueSlice value;
	pick_slice_from_buffer(buffer, value);
	return value;
}

TupleKeySlice
get_tuple_key_from_buffer(char *buffer)
{
	TupleKeySlice key;
	pick_slice_from_buffer(buffer, key);
	return key;
}

TupleValueSlice
pick_tuple_value_from_buffer(char *buffer)
{
	TupleValueSlice value;
	value.len = *(Size *)(buffer);
	value.data = (TupleValue)((buffer) + sizeof(Size));
	return value;
}

TupleKeySlice
pick_tuple_key_from_buffer(char *buffer)
{
	TupleKeySlice key;
	key.len = *(Size *)(buffer);
	key.data = (TupleKey)((buffer) + sizeof(Size));
	return key;
}

void
save_tuple_value_into_buffer(char *buffer, TupleValueSlice value)
{
	Size *len = (Size*)buffer;
	*len = value.len;
	memcpy((void *)(buffer + sizeof(Size)), (void *)(value.data), (value.len));
}

void
save_tuple_key_into_buffer(char *buffer, TupleKeySlice key)
{
	Size *len = (Size*)buffer;
	*len = key.len;
	memcpy((void *)(buffer + sizeof(Size)), (void *)(key.data), (key.len));
}
/*
 * Execution layer sends data
 */
ResponseHeader *
send_kv_request(RequestHeader *req)
{
	ResponseHeader *res = SessionProcessKV(req);
	//Assert(res->type == req->type);
	return res;
}

/*
 *
 */
HeapTuple
kvengine_make_fake_heap(TupleKeySlice key, TupleValueSlice value)
{
	if (value.data == NULL)
		return NULL;
	HeapTuple htup = (HeapTuple)palloc0(sizeof *htup);

	/* Alloc memory for HeapTupleHeader. Need not mind var-len bitmap. */
	htup->t_data = (HeapTupleHeader)palloc0(sizeof *htup->t_data);
	htup->t_len = sizeof *htup->t_data;
	htup->t_data->t_infomask = value.data->sysattrs.infomask;
	htup->t_data->t_infomask2 = value.data->sysattrs.infomask2;
	if (IsLTSKey(key))
		htup->t_data->t_choice.t_heap.t_xmin = 0;
	else
		get_TupleKeySlice_xmin(key, htup->t_data->t_choice.t_heap.t_xmin);
	htup->t_data->t_choice.t_heap.t_xmax = value.data->sysattrs.xmax;
	htup->t_data->t_choice.t_heap.t_field3.t_cid = value.data->sysattrs.cid;
	//get_TupleKeySlice_cmin(key, htup->t_data->t_choice.t_heap.t_field3.t_cid);

	return htup;
}

void kvengine_free_fake_heap(HeapTuple htup)
{
	if (htup == NULL)
		return;
	range_free(htup->t_data);
	range_free(htup);
}

Relation
kvengine_make_fake_relation()
{
	Relation rel = (Relation)palloc0(sizeof(*rel));
	rel->rd_rel = (Form_pg_class)palloc0(sizeof(*rel->rd_rel));
	rel->rd_rel->relstorage = RELSTORAGE_ROCKSDB;
	return rel;
}

void kvengine_free_fake_relation(Relation rel)
{
	range_free(rel->rd_rel);
	range_free(rel);
}

/*
 *
 */
bool kvengine_judge_dirty(TupleValue value, HeapTuple htup)
{
	bool dirty = false;

	set_if_diff(value->sysattrs.infomask, htup->t_data->t_infomask, dirty);
	set_if_diff(value->sysattrs.infomask2, htup->t_data->t_infomask2, dirty);
	set_if_diff(value->sysattrs.cid, htup->t_data->t_choice.t_heap.t_field3.t_cid, dirty);
	set_if_diff(value->sysattrs.xmax, htup->t_data->t_choice.t_heap.t_xmax, dirty);

	return dirty;
}

GetResponse *
kvengine_send_get_req(TupleKeySlice key)
{
	Dataslice req_key = (Dataslice)palloc0(key.len + sizeof(Size));
	make_data_slice(*req_key, key);

	Size size = sizeof(GetRequest) + req_key->len + sizeof(Size);
	GetRequest *req = (GetRequest *)palloc0(size);
	SessionSaveTransactionState((RequestHeader *)req);
	req->header.type = ROCKSDB_GET;
	req->header.size = size;
	save_slice_into_buffer(req->key, req_key);
	range_free(req_key);
	ResponseHeader *res = send_kv_request((RequestHeader *)req);
	range_free(req);
	if (transam_mode == TRANSAM_MODE_NEW_OCC)
	{
		CheckLogicalTsIntervalValid(res->cts, res->nts);
	}
	return (GetResponse *)res;
}

ScanResponse *
kvengine_delete_req(TupleKeySlice key, Snapshot snap)
{
	Dataslice req_key = (Dataslice)palloc0(key.len + sizeof(Size));
	make_data_slice(*req_key, key);

	Size size = sizeof(ScanWithPkeyRequest) + req_key->len + sizeof(Size);
	ScanWithPkeyRequest *req = (ScanWithPkeyRequest *)palloc0(size);
	SessionSaveTransactionState((RequestHeader *)req);
	req->header.type = ROCKSDB_DELETE;
	req->header.size = size;
	if (snap != InvalidSnapshot)
		req->snapshot = *snap;
	save_slice_into_buffer(req->start_and_end_key, req_key);
	pfree(req_key);
	ResponseHeader *res = send_kv_request((RequestHeader *)req);
	range_free(req);
	if (transam_mode == TRANSAM_MODE_NEW_OCC)
	{
		CheckLogicalTsIntervalValid(res->cts, res->nts);
	}
	return (ScanResponse *)res;
}

bool Rangeengine_create_paxos(RangeID rangeid, SegmentID *seglist, int segcount)
{
	int length = 0;
	char *segchar = TransferSegIDToIPPortList(seglist, segcount, &length);
	SegmentID *currentseg = (SegmentID *)palloc0(sizeof(SegmentID));
	currentseg[0] = GpIdentity.segindex;
	int current_length = 1;
	char *current = TransferSegIDToIPPortList(currentseg, 1, &current_length);

	CreateGroupRequest *req = (CreateGroupRequest *)palloc0(sizeof(CreateGroupRequest) + length);
	req->groupid = rangeid;
	SessionSaveTransactionState((RequestHeader *)req);
	req->header.writebatch = false;
	req->header.type = CREATEGROUP;
	req->header.size = sizeof(CreateGroupRequest) + length;

	memcpy(req->MyIPPort, current, current_length);
	memcpy(req->IPPortList, segchar, length);
	CreateGroupResponse *res = (CreateGroupResponse *)send_kv_request((RequestHeader *)req);
	range_free(segchar);
	range_free(current);
	range_free(currentseg);
	range_free(req);
	return res->success;
	//return true;
}

bool Rangeengine_remove_paxos(RangeID rangeid)
{
	RemoveGroupRequest *req = (RemoveGroupRequest *)palloc0(sizeof(RemoveGroupRequest));
	req->groupid = rangeid;
	SessionSaveTransactionState((RequestHeader *)req);
	req->header.writebatch = false;
	req->header.type = REMOVEGROUP;
	req->header.size = sizeof(RemoveGroupRequest);

	RemoveGroupResponse *res = (RemoveGroupResponse *)send_kv_request((RequestHeader *)req);
	range_free(req);
	return res->success;
}

bool Rangeengine_add_replica(RangeID rangeid, SegmentID segid)
{
	SegmentID *currentseg = (SegmentID *)palloc0(sizeof(SegmentID));
	currentseg[0] = segid;
	int current_length = 1;
	char *current = TransferSegIDToIPPortList(currentseg, 1, &current_length);

	AddGroupMemberRequest *req = (AddGroupMemberRequest *)palloc0(sizeof(AddGroupMemberRequest) + current_length);
	req->groupid = rangeid;
	SessionSaveTransactionState((RequestHeader *)req);
	req->header.writebatch = false;
	req->header.type = ADDGROUPMEMBER;
	req->header.size = sizeof(AddGroupMemberRequest) + current_length;

	memcpy(req->NodeIPPort, current, current_length);

	AddGroupMemberResponse *res = (AddGroupMemberResponse *)send_kv_request((RequestHeader *)req);
	range_free(req);
	range_free(currentseg);
	range_free(current);
	return res->success;
}

bool Rangeengine_remove_replica(RangeID rangeid, SegmentID segid)
{
	SegmentID *currentseg = (SegmentID *)palloc0(sizeof(SegmentID));
	currentseg[0] = segid;
	int current_length = 1;
	char *current = TransferSegIDToIPPortList(currentseg, 1, &current_length);

	RemoveGroupMemberRequest *req =
		(RemoveGroupMemberRequest *)palloc0(sizeof(RemoveGroupMemberRequest) + current_length);
	req->groupid = rangeid;
	SessionSaveTransactionState((RequestHeader *)req);
	req->header.writebatch = false;
	req->header.type = REMOVEGROUPMEMBER;
	req->header.size = sizeof(RemoveGroupMemberRequest) + current_length;

	memcpy(req->NodeIPPort, current, current_length);

	RemoveGroupMemberResponse *res =
		(RemoveGroupMemberResponse *)send_kv_request((RequestHeader *)req);
	range_free(req);
	range_free(currentseg);
	range_free(current);
	return res->success;
}

PutResponse *
kvengine_send_put_req(TupleKeySlice key, TupleValueSlice value, RangeID rangeid,
					  bool checkUnique, bool isSecondary, Snapshot SnapshotDirty)
{
	Dataslice req_key = (Dataslice)palloc0(key.len + sizeof(Size));
	Dataslice req_value = (Dataslice)palloc0(value.len + sizeof(Size));
	make_data_slice(*req_key, key);
	make_data_slice(*req_value, value);

	Size size = sizeof(PutRequest) + req_key->len + req_value->len + sizeof(Size) * 2;
	PutRequest *req = (PutRequest *)palloc0(size);
	SessionSaveTransactionState((RequestHeader *)req);
	Size index = size_of_Keylen(*req_key);
	save_slice_into_buffer(req->k_v, req_key);
	range_free(req_key);
	save_slice_into_buffer((char *)req->k_v + index, req_value);
	range_free(req_value);
	req->header.type = ROCKSDB_PUT;
	req->header.size = size;
	req->isSecondary = isSecondary;
	req->checkUnique = checkUnique;
	req->rangeid = rangeid;
	if (checkUnique)
		req->snapshot = *SnapshotDirty;
	/*
	 * The response just tell session the request has been processed. It contains
	 * no informations so we need not return it.
	 */
	ResponseHeader *res = send_kv_request((RequestHeader *)req);
	range_free(req);
	if (transam_mode == TRANSAM_MODE_NEW_OCC)
	{
		CheckLogicalTsIntervalValid(res->cts, res->nts);
	}
	return (PutResponse *)res;
}

DeleteDirectResponse *
kvengine_send_delete_direct_req(TupleKeySlice key)
{
	Dataslice req_key = (Dataslice)palloc0(key.len + sizeof(Size));
	make_data_slice(*req_key, key);

	Size size = sizeof(DeleteDirectRequest) + req_key->len + sizeof(Size);
	DeleteDirectRequest *req = (DeleteDirectRequest *)palloc0(size);
	SessionSaveTransactionState((RequestHeader *)req);

	save_slice_into_buffer(req->key, req_key);
	pfree(req_key);
	req->header.type = ROCKSDB_DELETE_DIRECT;
	req->header.size = size;
	/*
	 * The response just tell session the request has been processed. It contains
	 * no informations so we need not return it.
	 */
	ResponseHeader *res = send_kv_request((RequestHeader *)req);
	range_free(req);

	return (DeleteDirectResponse *)res;
}

ScanResponse *
kvengine_send_index_scan_req(TupleKeySlice origstartkey, TupleKeySlice origendkey,
							 TupleKeySlice startkey, TupleKeySlice endkey,
							 Snapshot snap, bool forward, bool second, int req_type,
							 CmdType Scantype)
{
	Dataslice ostart_key = (Dataslice)palloc0(origstartkey.len + sizeof(Size));
	if (origstartkey.len > MAX_SLICE_LEN)
	{
		core_dump();
	}
	make_data_slice(*ostart_key, origstartkey);

	Dataslice oend_key = (Dataslice)palloc0(origendkey.len + sizeof(Size));
	if (origendkey.len > MAX_SLICE_LEN)
	{
		core_dump();
	}
	make_data_slice(*oend_key, origendkey);

	Dataslice start_key = (Dataslice)palloc0(startkey.len + sizeof(Size));
	if (startkey.len > MAX_SLICE_LEN)
	{
		core_dump();
	}
	make_data_slice(*start_key, startkey);

	Dataslice end_key = (Dataslice)palloc0(endkey.len + sizeof(Size));
	if (endkey.len > MAX_SLICE_LEN)
	{
		core_dump();
	}
	make_data_slice(*end_key, endkey);

	Size size = sizeof(ScanWithKeyRequest) + 
				startkey.len + sizeof(Size) + 
				endkey.len + sizeof(Size) + 
				origstartkey.len + sizeof(Size) +
				origendkey.len + sizeof(Size);

	ScanWithKeyRequest *req = (ScanWithKeyRequest *)palloc0(size);
	SessionSaveTransactionState((RequestHeader *)req);
	req->header.type = req_type;
	req->header.size = size;
	req->issecond = second;
	req->Scantype = Scantype;
	char *temp = req->start_and_end_key;
	save_slice_into_buffer(temp, start_key);
	pfree(start_key);
	save_slice_into_buffer(temp + startkey.len + sizeof(Size), end_key);
	pfree(end_key);
	save_slice_into_buffer(temp + startkey.len + sizeof(Size) +
								+ endkey.len + sizeof(Size), ostart_key);
	pfree(ostart_key);
	save_slice_into_buffer(temp + startkey.len + sizeof(Size) +
								+ endkey.len + sizeof(Size) +
								+ origstartkey.len + sizeof(Size), oend_key);
	pfree(oend_key);
	req->max_num = MAX_SCAN_KV_NUM;
	if (snap != InvalidSnapshot)
		req->snapshot = *snap;
	req->isforward = forward;
	ResponseHeader *res = send_kv_request((RequestHeader *)req);
	Assert(res->type == req_type);
	range_free(req);
	if (transam_mode == TRANSAM_MODE_NEW_OCC)
	{
		CheckLogicalTsIntervalValid(res->cts, res->nts);
	}
	return (ScanResponse *)res;
}

ScanResponse*
kvengine_send_history_scan_req(TupleKeySlice startkey, TupleKeySlice endkey, long time, bool isforward)
{
	Dataslice start_key = palloc0(startkey.len + sizeof(Size));
	make_data_slice(*start_key, startkey);
	Dataslice end_key = palloc0(endkey.len + sizeof(Size));
	make_data_slice(*end_key, endkey);

	Size size = sizeof(ScanWithTimeRequest) + startkey.len + sizeof(Size) + endkey.len + sizeof(Size) ;
	ScanWithTimeRequest *req = palloc0(size);
	SessionSaveTransactionState((RequestHeader*) req);
	req->header.type = ROCKSDB_HISTORY_SCAN;
	req->header.size = size;
	char* temp = req->start_and_end_key;
	save_slice_into_buffer(temp, start_key);
	save_slice_into_buffer(temp + startkey.len + sizeof(Size), end_key);
	req->max_num = MAX_SCAN_KV_NUM;
	req->time = time;
	req->isforward = isforward;
	ResponseHeader* res = send_kv_request((RequestHeader*) req);
	Assert(res->type == ROCKSDB_HISTORY_SCAN);
	return (ScanResponse*) res;
}

ScanResponse *
kvengine_send_cycle_get_req(TupleKeySlice *all_key, int num, Snapshot snap, CmdType Scantype)
{
	Size size = sizeof(ScanWithKeyRequest);
	for (int i = 0; i < num; i++)
	{
		size += all_key[i].len + sizeof(Size);
	}
	ScanWithKeyRequest *req = (ScanWithKeyRequest *)palloc0(size);
	SessionSaveTransactionState((RequestHeader *)req);
	req->header.type = ROCKSDB_CYCLEGET;
	req->header.size = size;
	req->Scantype = Scantype;
	char *temp = req->start_and_end_key;
	for (int i = 0; i < num; i++)
	{
		Dataslice allKey = (Dataslice)palloc0(all_key[i].len + sizeof(Size));
		make_data_slice(*allKey, all_key[i]);
		save_slice_into_buffer(temp, allKey);
		pfree(allKey);
		temp += all_key[i].len + sizeof(Size);
	}

	req->max_num = num;
	if (snap != InvalidSnapshot)
		req->snapshot = *snap;
	req->isforward = true;
	ResponseHeader *res = send_kv_request((RequestHeader *)req);
	Assert(res->type == ROCKSDB_CYCLEGET);
	range_free(req);
	if (transam_mode == TRANSAM_MODE_NEW_OCC)
	{
		CheckLogicalTsIntervalValid(res->cts, res->nts);
	}
	return (ScanResponse *)res;
}

MultiPutResponse*
kvengine_send_multi_put_req()
{
	Size buffer_size = get_multi_send_buffer_size();
	Size size = sizeof(MultiPutRequest) + buffer_size;
	MultiPutRequest *req = palloc0(size);
	SessionSaveTransactionState((RequestHeader*) req);
	req->header.type = ROCKSDB_MULTI_PUT;
	req->header.size = size;
	char *buf_end = store_kvs_into_buffer_and_clean(req->buffer);
	Assert(buf_end - req->buffer == buffer_size);
	ResponseHeader *res = send_kv_request((RequestHeader*) req);
	pfree(req);
	return (MultiPutResponse*) res;
}

Size
get_multi_send_buffer_size()
{
	Size buffer_size = 0;
	for (int i = 0; i < his_transfer_kv_buffer.num; ++i)
	{
		buffer_size += sizeof(Size) * 2;
		buffer_size += his_transfer_kv_buffer.kvs[i].key.len;
		buffer_size += his_transfer_kv_buffer.kvs[i].value.len;
	}
	return buffer_size;
}

char*
store_kvs_into_buffer_and_clean(void* buf)
{
	for (int i = 0; i < his_transfer_kv_buffer.num; ++i)
	{
		put_kv_into_buffer_and_move_buf(buf, his_transfer_kv_buffer.kvs[i]);
		clean_slice(his_transfer_kv_buffer.kvs[i].key);
		clean_slice(his_transfer_kv_buffer.kvs[i].value);
	}
	his_transfer_kv_buffer.num = 0;
	return buf;
}

RefreshHistoryResponse*
kvengine_send_refresh_history_req()
{
	RefreshHistoryRequest *req = palloc0(sizeof(*req));
	req->header.size = sizeof(*req);
	SessionSaveTransactionState(req);
	req->header.type = ROCKSDB_REFRESH_HISTORY;
	ResponseHeader *res = send_kv_request((RequestHeader*) req);
	return (RefreshHistoryResponse*) res;
}

/*
 * Execution layer insertion initialization
 */
KVEngineInsertDesc
kvengine_insert_init(Relation rel)
{
	KVEngineInsertDesc desc = (KVEngineInsertDesc)palloc0(sizeof *desc);
	Assert(RelationIsRocksDB(rel));
	desc->rki_rel = rel;
	return desc;
}

static void
generate_primary_key_value(KVEngineInsertDesc desc,
						   TupleTableSlot *slot, CommandId cid,
						   TupleKeySlice *key, TupleValueSlice *value)
{
	TransactionId xid = GetCurrentTransactionId();
	bool isFrozen = (xid == FrozenTransactionId);

	/*
	 * Get heap tuple to initial infomasks, eventually we store both infomasks
	 * and mem tuple into RocksDB.
	 */
	HeapTuple htup = ExecMaterializeSlot(slot);
	MemTuple mtup = ExecFetchSlotMemTuple(slot);
	Assert(!memtuple_get_hasext(mtup));

	/* [hongyaozhao] get primary key value so we can put the primary key as a part of rocks_key */
	Oid pk_oid = get_pk_oid(desc->rki_rel);
	Assert(OidIsValid(pk_oid));
	Size mtup_size = memtuple_get_size(mtup);
	Size value_len = sizeof(TupleValueSysAttrs) + mtup_size;

	/* make value */
	HeapTuple heaptup = heap_prepare_insert(desc->rki_rel, htup, xid, cid, 0 /*options*/, isFrozen);
	TupleValue kvengine_value = (TupleValue)palloc0(value_len);
	kvengine_value->sysattrs.xmax = InvalidTransactionId;
	kvengine_value->sysattrs.infomask = heaptup->t_data->t_infomask;
	kvengine_value->sysattrs.infomask2 = heaptup->t_data->t_infomask2;
	kvengine_value->sysattrs.cid = cid;
	kvengine_value->sysattrs.natts = slot->tts_tupleDescriptor->natts;
	kvengine_value->sysattrs.new_cid = 0;
	kvengine_value->sysattrs.has_new_cid = false;
	memcpy((char *)&kvengine_value->memtuple, (char *)mtup, mtup_size);
	// should we release heaptup here?
	// TODO: compare addresses between htup and heaptup

	/* [hongyaozhao] Change the encoding format of pk_value */
	int pk_natts = 0;
	int *pk_colnos = get_index_colnos(pk_oid, &pk_natts);

	InitKeyDesc initkey;
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		initkey = init_basis_in_keydesc(KEY_WITH_XMINCMIN);
		break;
	case KVENGINE_TRANSACTIONDB:
		initkey = init_basis_in_keydesc(RAW_KEY);
		break;
	default:
		break;
	}

	initkey.init_type = PRIMARY_KEY;
	initkey.rel_id = desc->rki_rel->rd_id;
	Datum *pkvalues = (Datum *)palloc0(sizeof(Datum) * pk_natts);
	bool *pkisnull = (bool *)palloc0(sizeof(bool) * pk_natts);
	get_value_from_slot(slot, pk_colnos, pk_natts, pkvalues, pkisnull);

	init_pk_in_keydesc(&initkey, pk_oid,
					   get_attr_type(desc->rki_rel, pk_colnos, pk_natts),
					   pkvalues, pkisnull, pk_natts);
	initkey.xid = xid;
	initkey.isend = false;
	initkey.cid = cid;
	/* make key */
	*key = build_key(initkey);
	pfree(pk_colnos);
	pfree(pkvalues);
	pfree(pkisnull);
	//TupleKeySlice key = {kvengine_key, };
	value->data = kvengine_value;
	value->len = value_len;
}
/*
 * Executive layer insertion
 */
Oid 
kvengine_insert(KVEngineInsertDesc desc, TupleTableSlot *slot, CommandId cid)
{
	TupleKeySlice key = {NULL, 0};
	TupleValueSlice value = {NULL, 0};
	generate_primary_key_value(desc, slot, cid, &key, &value);
	Assert(sizeof(value.data->sysattrs) + memtuple_get_size(&value.data->memtuple) == value.len);

	kvengine_check_unique_and_insert(desc->rki_rel, key, value, slot->tts_rangeid, false);

	/* update the statistics */
	if (enable_range_distribution && !enable_paxos)
		UpdateStatisticsInsert(slot->tts_rangeid, key, value);

	pfree(key.data);
	pfree(value.data);
	return InvalidOid;
}

void
kvengine_his_insert(Relation rel, TupleTableSlot* slot)
{
	MemoryContext oldThreadContext = CurrentMemoryContext;
	CurrentMemoryContext = TopMemoryContext;
	HistoricalKV kv = encode_temp_historical_kv(rel, slot);
	CurrentMemoryContext = oldThreadContext;

	Assert(his_transfer_kv_buffer.num < MAX_HIS_TRANSFER_KV_NUM);
	his_transfer_kv_buffer.kvs[his_transfer_kv_buffer.num ++] = kv;
	if (his_transfer_kv_buffer.num == MAX_HIS_TRANSFER_KV_NUM)
		kvengine_send_multi_put_req();
	
}

void
kvengine_refresh_historical_kvs(bool isvacuum)
{
	if (his_transfer_kv_buffer.num > 0)
	{
		kvengine_send_multi_put_req();
	}
	if (IsUnderPostmaster && isvacuum)
		kvengine_send_refresh_history_req();
}

bool kvengine_index_insert(KVEngineInsertDesc desc, TupleTableSlot *slot,
						   Oid indexOid, bool checkUnique)
{
	/* [hongyaozhao] get primary key value so we can put the primary key as a part of rocks_key */
	Oid pk_oid = get_pk_oid(desc->rki_rel);
	Assert(OidIsValid(pk_oid));
	/* [hongyaozhao] Change the encoding format of pk_value */
	int pk_natts = 0;
	int index_natts = 0;
	int *pk_colnos = get_index_colnos(pk_oid, &pk_natts);
	int *index_colnos = get_index_colnos(indexOid, &index_natts);

	InitKeyDesc initkey;
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		initkey = init_basis_in_keydesc(KEY_WITH_XMINCMIN);
		break;
	case KVENGINE_TRANSACTIONDB:
		initkey = init_basis_in_keydesc(RAW_KEY);
		break;
	default:
		break;
	}
	initkey.init_type = SECONDE_KEY;
	initkey.rel_id = desc->rki_rel->rd_id;
	Datum *pkvalues = (Datum *)palloc0(sizeof(Datum) * pk_natts);
	bool *pkisnull = (bool *)palloc0(sizeof(bool) * pk_natts);
	get_value_from_slot(slot, pk_colnos, pk_natts, pkvalues, pkisnull);

	Datum *skvalues = (Datum *)palloc0(sizeof(Datum) * index_natts);
	bool *skisnull = (bool *)palloc0(sizeof(bool) * index_natts);
	get_value_from_slot(slot, index_colnos, index_natts, skvalues, skisnull);

	init_pk_in_keydesc(&initkey, pk_oid, get_attr_type(desc->rki_rel, pk_colnos, pk_natts),
					   pkvalues, pkisnull, pk_natts);
	init_sk_in_keydesc(&initkey, indexOid, get_attr_type(desc->rki_rel, index_colnos, index_natts),
					   skvalues, skisnull, index_natts);
	initkey.xid = 0;
	initkey.isend = false;
	initkey.cid = 0;
	/* make key */
	TupleKeySlice key = build_key(initkey);

	/*MemTuple	mtup = ExecFetchSlotMemTuple(slot);
	Assert(!memtuple_get_hasext(mtup));
	Size mtup_size = memtuple_get_size(mtup);*/
	Size value_len = sizeof(TupleValueSysAttrs);
	/* make value as pk value */
	TupleValue kvengine_value = (TupleValue)palloc0(value_len);
	kvengine_value->sysattrs.xmax = InvalidTransactionId;
	kvengine_value->sysattrs.infomask = 0;
	kvengine_value->sysattrs.infomask2 = 0;
	kvengine_value->sysattrs.cid = 0;
	kvengine_value->sysattrs.natts = 0;
	kvengine_value->sysattrs.lockxid = 0;

	TupleValueSlice value = {kvengine_value, value_len};

	if (checkUnique)
		kvengine_check_unique_and_insert(desc->rki_rel, key, value, slot->tts_rangeid, true);
	else
		kvengine_send_put_req(key, value, slot->tts_rangeid, false, true, NULL);

	/* update the statistics */
	/* The operation to update the insert statistics section is currently
		decentralized below the paxos layer. */
	if (enable_range_distribution && !enable_paxos)
		UpdateStatisticsInsert(slot->tts_rangeid, key, value);

	pfree(pk_colnos);
	pfree(pkvalues);
	pfree(pkisnull);
	pfree(index_colnos);
	pfree(skvalues);
	pfree(skisnull);

	pfree(key.data);
	pfree(value.data);
	return true;
}

/*
 * Execution layer insert recycling
 */
void kvengine_insert_finish(KVEngineInsertDesc desc)
{
	pfree(desc);
}

static void
decode_scan_res_into_scandesc(KVEngineScanDesc scan, ScanResponse *scan_res, Size index)
{
	/* First, empty the possible original key and value in the scandesc. */
	kvengine_free_scandesc_key_value(scan);
	/* Save new data to scandesc. */
	scan->scan_num = scan_res->num;
	for (int i = 0; i < scan_res->num; ++i)
	{
		DataSlice *key = get_slice_from_buffer(scan_res->buffer + index);
		index += size_of_Keylen(*key);
		DataSlice *value = get_slice_from_buffer(scan_res->buffer + index);
		index += size_of_Keylen(*value);
		Oid *rangeid = (Oid *)(scan_res->buffer + index);
		scan->rangeid[i] = *rangeid;
		index += sizeof(Oid);
		save_data_slice(scan->cur_key[i], *key);
		save_data_slice(scan->cur_value[i], *value);
		range_free(key);
		range_free(value);
	}
	return;
}

/*
 * Execution layer delete initialization
 */
KVEngineDeleteDesc
kvengine_delete_init(Relation rel)
{
	KVEngineDeleteDesc desc = (KVEngineDeleteDesc)palloc0(sizeof *desc);
	Assert(RelationIsRocksDB(rel));
	desc->rkd_rel = rel;
	return desc;
}

/*
 * Through the key and value of the primary key, all secondary index 
 * keys corresponding to the primary key tuple are generated.
 */
static TupleKeySlice *
make_all_second_key(Relation rel, TupleTableSlot *slot, int *length)
{
	/* init build key desc*/
	InitKeyDesc initkey;
	switch (transaction_type)
	{
	case KVENGINE_ROCKSDB:
		initkey = init_basis_in_keydesc(KEY_WITH_XMINCMIN);
		break;
	case KVENGINE_TRANSACTIONDB:
		initkey = init_basis_in_keydesc(RAW_KEY);
		break;
	default:
		break;
	}
	initkey.init_type = SECONDE_KEY;
	initkey.rel_id = rel->rd_id;

	/* get primary key value */
	Oid pk_oid = get_pk_oid(rel);
	int pk_natts = 0;
	int *pk_colnos = get_index_colnos(pk_oid, &pk_natts);
	Datum *pkvalues = (Datum *)palloc0(sizeof(Datum) * pk_natts);
	bool *pkisnull = (bool *)palloc0(sizeof(bool) * pk_natts);
	get_value_from_slot(slot, pk_colnos, pk_natts, pkvalues, pkisnull);
	init_pk_in_keydesc(&initkey, pk_oid, get_attr_type(rel, pk_colnos, pk_natts), pkvalues, pkisnull, pk_natts);

	/* find second index oid */
	List *indexoidlist = get_index_oid(rel);
	ListCell *indexoidscan = NULL;
	int indexcount = list_length(indexoidlist);

	/* init a space to store all the key we want to delete */
	TupleKeySlice *allDeleteKey = (TupleKeySlice *)palloc0(mul_size(indexcount, sizeof(TupleKeySlice)));
	int keyindex = 0;

	foreach (indexoidscan, indexoidlist)
	{
		Oid indexOid = lfirst_oid(indexoidscan);
		HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexOid);
		Form_pg_index index = (Form_pg_index)GETSTRUCT(indexTuple);
		/* if this is primary key, we have to another place to delete it */
		if (index->indisprimary)
		{
			ReleaseSysCache(indexTuple);
			continue;
		}
		else
		{
			/* get second index value */
			int index_natts = 0;
			int *index_colnos = get_index_colnos(indexOid, &index_natts);

			Datum *skvalues = (Datum *)palloc0(sizeof(Datum) * index_natts);
			bool *skisnull = (bool *)palloc0(sizeof(bool) * index_natts);
			get_value_from_slot(slot, index_colnos, index_natts, skvalues, skisnull);

			init_sk_in_keydesc(&initkey, indexOid,
							   get_attr_type(rel, index_colnos, index_natts),
							   skvalues, skisnull, index_natts);
			initkey.xid = 0;
			initkey.isend = false;
			initkey.cid = 0;
			/* make key */
			TupleKeySlice key = build_key(initkey);
			pfree(index_colnos);
			pfree(skvalues);
			pfree(skisnull);
			allDeleteKey[keyindex++] = key;
		}
		ReleaseSysCache(indexTuple);
	}
	list_free(indexoidlist);
	pfree(pk_colnos);
	pfree(pkvalues);
	pfree(pkisnull);
	*length = keyindex;
	return allDeleteKey;
}

static Delete_UpdateRequest *
handle_update_lock(Delete_UpdateResponse *res)
{
	TupleKeySlice key = get_tuple_key_from_buffer(res->rfd);
	/*Size len = 0;
    get_TupleKeySlice_primarykey_prefix_wxc(key, &len);
    key.len = len;*/
	switch (res->result_type)
	{
	case UPDATE_ADD_TUPLE_LOCK:
	{
		bool have_tuple_lock = false;
		Relation rel = heap_open(key.data->rel_id, AccessShareLock);
		kvengine_acquire_tuplock(rel, key, LockTupleExclusive, false, &have_tuple_lock);
		Delete_UpdateRequest *req = (Delete_UpdateRequest *)palloc0(sizeof(Delete_UpdateRequest));
		SessionSaveTransactionState((RequestHeader *)req);
		req->header.type = ROCKSDB_UPDATE;
		req->header.size = sizeof(Delete_UpdateRequest);
		req->result = 0;
		req->result_type = GO_ON;
		range_free(key.data);
		heap_close(rel, AccessShareLock);
		return req;
	}
	break;
	case UPDATE_RELEASE_TUPLE_LOCK:
	{
		Relation rel = heap_open(key.data->rel_id, AccessShareLock);
		UnlockTupleKVTuplock(rel, key, LockTupleExclusive);

		Delete_UpdateRequest *req = (Delete_UpdateRequest *)palloc0(sizeof(Delete_UpdateRequest));
		SessionSaveTransactionState((RequestHeader *)req);
		req->header.type = ROCKSDB_UPDATE;
		req->header.size = sizeof(Delete_UpdateRequest);
		req->result = 0;
		req->result_type = GO_ON;
		range_free(key.data);
		heap_close(rel, AccessShareLock);
		return req;
	}
	break;
	case UPDATE_XACTID:
	{
		Relation rel = heap_open(key.data->rel_id, AccessShareLock);
		ItemPointer fakectid = palloc0(sizeof(ItemPointerData));
		fakectid->ip_blkid.bi_hi = 0;
		fakectid->ip_blkid.bi_lo = 0;
		fakectid->ip_posid = 0;
		int remain = 0;
		MultiXactIdWait((MultiXactId)res->xwait,
						(MultiXactStatus)res->status,
						res->infomask, rel, &fakectid,
						(XLTW_Oper)res->oper, &remain);
		Delete_UpdateRequest *req = (Delete_UpdateRequest *)palloc0(sizeof(Delete_UpdateRequest));
		SessionSaveTransactionState((RequestHeader *)req);
		req->header.type = ROCKSDB_UPDATE;
		req->header.size = sizeof(Delete_UpdateRequest);
		req->result = 0;
		req->remain = remain;
		req->result_type = GO_ON;
		range_free(key.data);
		heap_close(rel, AccessShareLock);
		return req;
	}
	break;
	case UPDATE_XACTTABLE:
	{
		Relation rel = heap_open(key.data->rel_id, AccessShareLock);
		XactLockTableKVWait(res->xwait, rel, (XLTW_Oper)res->oper);
		Delete_UpdateRequest *req = (Delete_UpdateRequest *)palloc0(sizeof(Delete_UpdateRequest));
		SessionSaveTransactionState((RequestHeader *)req);
		req->header.type = ROCKSDB_UPDATE;
		req->header.size = sizeof(Delete_UpdateRequest);
		req->result = 0;
		req->result_type = GO_ON;
		range_free(key.data);
		heap_close(rel, AccessShareLock);
		return req;
	}
	break;
	default:
		range_free(key.data);
		break;
	}
	return NULL;
}

/*
 * The delete operation is divided into two requests.
 * The first is to get the value from the underlying through the primary key
 * to be deleted.
 * The second time is in session layer after the completion of all secondary
 * index key and value generation, unified delete in storage.
 */
HTSU_Result
kvengine_delete(KVEngineDeleteDesc desc,
				TupleKeySlice key,
				CommandId cid,
				Snapshot crosscheck,
				RocksUpdateFailureData *rufd)
{
	/*
	 * The first time, you get the value from the underlying through the 
	 * primary key you want to delete.
	 */
	Dataslice req_key = (Dataslice)palloc0(key.len + sizeof(Size));
	make_data_slice(*req_key, key);
	GetResponse *get_res = kvengine_send_get_req(key);
	/*
	 * Second, generate all the corresponding secondary index keys by 
	 * returning the primary key value
	 */
	TupleValueSlice value = get_tuple_value_from_buffer(get_res->value);
	Dataslice req_value = (Dataslice)palloc0(value.len + sizeof(Size));
	make_data_slice(*req_value, value);

	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(desc->rkd_rel));
	put_value_into_slot(key, value, slot);
	int key_count = 0;
	TupleKeySlice *AllDeleteSecondKey = make_all_second_key(desc->rkd_rel, slot, &key_count);
	ExecDropSingleTupleTableSlot(slot);
	/*
	 * Third, encapsulate all keys to be removed into the request.
	 * Storage completes the delete operation.
	 */
	Size size = sizeof(Delete_UpdateRequest) + size_of_Keylen(*req_key) + size_of_Keylen(*req_value);
	for (int i = 0; i < key_count; i++)
	{
		size += AllDeleteSecondKey[i].len + sizeof(Size);
	}
	Delete_UpdateRequest *req = (Delete_UpdateRequest *)palloc0(size);
	SessionSaveTransactionState((RequestHeader *)req);

	char *tmp = req->key;
	save_slice_into_buffer(tmp, req_key);
	tmp += size_of_Keylen(*req_key);
	pfree(req_key);

	save_slice_into_buffer(tmp, req_value);
	tmp += size_of_Keylen(*req_value);
	pfree(req_value);
	for (int i = 0; i < key_count; i++)
	{
		Dataslice copy_key = (Dataslice)palloc0(AllDeleteSecondKey[i].len + sizeof(Size));
		make_data_slice(*copy_key, AllDeleteSecondKey[i]);
		save_slice_into_buffer(tmp, copy_key);
		tmp += size_of_Keylen(*copy_key);
		pfree(AllDeleteSecondKey[i].data);
		pfree(copy_key);
	}
	pfree(AllDeleteSecondKey);
	req->key_count = key_count + 2;
	req->header.type = ROCKSDB_DELETE;
	req->header.size = size;
	if (crosscheck != InvalidSnapshot)
		req->snapshot = *crosscheck;
	req->cid = cid;
	req->xid = GetCurrentTransactionId();

	for (;;)
	{
		ResponseHeader *res = send_kv_request((RequestHeader *)req);
		range_free(req);
		Delete_UpdateResponse *de_res = (Delete_UpdateResponse *)res;
		if (de_res->result_type == UPDATE_COMPLETE)
		{
			*rufd = decodeRocksUpdateFailureData(de_res->rfd);
			return de_res->result;
		}
		else
		{
			req = handle_update_lock(de_res);
		}
	}
}

#if 1
/*
 * Execution layer delete initialization
 */
KVEngineUpdateDesc
kvengine_update_init(Relation rel)
{
	KVEngineUpdateDesc desc = (KVEngineUpdateDesc)palloc0(sizeof *desc);
	Assert(RelationIsRocksDB(rel));
	desc->rkd_rel = rel;
	return desc;
}

HTSU_Result
kvengine_update(KVEngineUpdateDesc desc,
				TupleTableSlot *slot,
				TupleKeySlice delete_key,
				CommandId cid,
				Snapshot crosscheck,
				RocksUpdateFailureData *rufd,
				int retry_time)
{
	/*
	 * The first time, you get the value from the underlying through the 
	 * primary key you want to delete.
	 */
	Dataslice delete_pkey = (Dataslice)palloc0(delete_key.len + sizeof(Size));
	make_data_slice(*delete_pkey, delete_key);
	GetResponse *get_res = kvengine_send_get_req(delete_key);
	/*
	 * Second, generate all the corresponding secondary index keys by 
	 * returning the primary key value
	 */
	TupleValueSlice value = get_tuple_value_from_buffer(get_res->value);
	if (value.len == 0 || value.data == NULL)
	{
		//TODO: find the reason why we cannot get any message.
		return HeapTupleMayBeUpdated;
	}
	Dataslice delete_pvalue = (Dataslice)palloc0(value.len + sizeof(Size));
	make_data_slice(*delete_pvalue, value);

	TupleTableSlot *deleteslot = MakeSingleTupleTableSlot(RelationGetDescr(desc->rkd_rel));
	put_value_into_slot(delete_key, value, deleteslot);
	int key_count = 0;
	TupleKeySlice *AllDeleteSecondKey = make_all_second_key(desc->rkd_rel, deleteslot, &key_count);
	ExecDropSingleTupleTableSlot(deleteslot);
	/*
	 * Third, generate the key to be inserted.
	 */
	TupleKeySlice putkey = {NULL, 0};
	TupleValueSlice putvalue = {NULL, 0};
	generate_primary_key_value((KVEngineInsertDesc)desc, slot, cid, &putkey, &putvalue);
	Dataslice put_pkey = (Dataslice)palloc0(putkey.len + sizeof(Size));
	make_data_slice(*put_pkey, putkey);
	Dataslice put_pvalue = (Dataslice)palloc0(putvalue.len + sizeof(Size));
	make_data_slice(*put_pvalue, putvalue);
	/*
	 * Fourth, encapsulate all keys to be deleted and inserted into requset.
	 */
	Size size = sizeof(Delete_UpdateRequest) + size_of_Keylen(*put_pkey) + size_of_Keylen(*put_pvalue);
	size += size_of_Keylen(*delete_pkey) + size_of_Keylen(*delete_pvalue);
	for (int i = 0; i < key_count; i++)
	{
		size += AllDeleteSecondKey[i].len + sizeof(Size);
	}
	Delete_UpdateRequest *req = (Delete_UpdateRequest *)palloc0(size);
	SessionSaveTransactionState((RequestHeader *)req);

	char *tmp = req->key;
	save_slice_into_buffer(tmp, put_pkey);
	tmp += size_of_Keylen(*put_pkey);
	pfree(put_pkey);

	save_slice_into_buffer(tmp, put_pvalue);
	tmp += size_of_Keylen(*put_pvalue);
	pfree(put_pvalue);

	save_slice_into_buffer(tmp, delete_pkey);
	tmp += size_of_Keylen(*delete_pkey);
	pfree(delete_pkey);

	// save_slice_into_buffer(tmp, delete_pvalue);
	// tmp += size_of_Keylen(*delete_pvalue);
	// pfree(delete_pvalue);

	for (int i = 0; i < key_count; i++)
	{
		Dataslice copy_key = (Dataslice)palloc0(AllDeleteSecondKey[i].len + sizeof(Size));
		make_data_slice(*copy_key, AllDeleteSecondKey[i]);
		save_slice_into_buffer(tmp, copy_key);
		tmp += size_of_Keylen(*copy_key);
		pfree(AllDeleteSecondKey[i].data);
		pfree(copy_key);
	}
	pfree(AllDeleteSecondKey);
	req->key_count = key_count + 3;
	req->retry_time = retry_time;
	req->header.type = ROCKSDB_UPDATE;
	req->header.size = size;
	if (crosscheck != InvalidSnapshot)
		req->snapshot = *crosscheck;
	req->cid = cid;
	req->result_type = 0;
	req->xid = GetCurrentTransactionId();

	for (;;)
	{
		ResponseHeader *res = send_kv_request((RequestHeader *)req);
		range_free(req);
		Delete_UpdateResponse *updateres = (Delete_UpdateResponse *)res;
		if (updateres->result_type == UPDATE_COMPLETE)
		{
			*rufd = decodeRocksUpdateFailureData(updateres->rfd);
			return updateres->result;
		}
		else
			req = handle_update_lock(updateres);
	}
}

void kvengine_update_finish(KVEngineUpdateDesc desc)
{
	pfree(desc);
}
#endif

/*
 * Acquire heavyweight lock on the given tuple, in preparation for acquiring
 * its normal, Xmax-based tuple lock.
 *
 * have_tuple_lock is an input and output parameter: on input, it indicates
 * whether the lock has previously been acquired (and this function does
 * nothing in that case).  If this function returns success, have_tuple_lock
 * has been flipped to true.
 */
void kvengine_acquire_tuplock(Relation relation,
							  TupleKeySlice key,
							  LockTupleMode mode,
							  bool nowait,
							  bool *have_tuple_lock)
{
	if (*have_tuple_lock)
		return;

	if (!nowait)
		LockTupleKVTuplock(relation, key, mode);
	else if (!ConditionalLockTupleKVTuplock(relation, key, mode))
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("could not obtain lock on row in relation \"%s\"",
						RelationGetRelationName(relation))));

	*have_tuple_lock = true;
}

/*
 * Execution layer delete recycling
 */
void kvengine_delete_finish(KVEngineDeleteDesc desc)
{
	pfree(desc);
}

void initial_start_key(KVEngineScanDesc scan, TupleKeySlice *key)
{
	TupleKeySlice start_key;
	start_key.data = key->data;
	start_key.len = key->len;
	if (key->data == NULL)
	{
		InitKeyDesc initkey;
		switch (transaction_type)
		{
		case KVENGINE_ROCKSDB:
			initkey = init_basis_in_keydesc(KEY_WITH_XMINCMIN);
			break;
		case KVENGINE_TRANSACTIONDB:
			initkey = init_basis_in_keydesc(RAW_KEY);
			break;
		default:
			break;
		}
		initkey.init_type = VALUE_NULL_KEY;
		initkey.rel_id = scan->rks_rel->rd_id;
		initkey.pk_id = get_pk_oid(scan->rks_rel);
		initkey.isend = false;
		start_key = build_key(initkey);
	}
	if (start_key.len > MAX_SLICE_LEN)
	{
		core_dump();
	}
	scan->start_key.data = start_key.data;
	scan->start_key.len = start_key.len;
}

void initial_next_key(KVEngineScanDesc scan, TupleKeySlice *key)
{
	TupleKeySlice next_key;
	next_key.data = key->data;
	next_key.len = key->len;
	if (key->data == NULL)
	{
		InitKeyDesc initkey;
		switch (transaction_type)
		{
		case KVENGINE_ROCKSDB:
			initkey = init_basis_in_keydesc(KEY_WITH_XMINCMIN);
			break;
		case KVENGINE_TRANSACTIONDB:
			initkey = init_basis_in_keydesc(RAW_KEY);
			break;
		default:
			break;
		}
		initkey.init_type = VALUE_NULL_KEY;
		initkey.rel_id = scan->rks_rel->rd_id;
		initkey.pk_id = get_pk_oid(scan->rks_rel);
		initkey.isend = false;
		next_key = build_key(initkey);
	}
	if (next_key.len > MAX_SLICE_LEN)
	{
		core_dump();
	}
	scan->next_key.data = next_key.data;
	scan->next_key.len = next_key.len;
}

void initial_end_key(KVEngineScanDesc scan, TupleKeySlice *key)
{
	TupleKeySlice end_key;
	end_key.data = key->data;
	end_key.len = key->len;
	if (key->data == NULL)
	{
		InitKeyDesc initkey;
		switch (transaction_type)
		{
		case KVENGINE_ROCKSDB:
			initkey = init_basis_in_keydesc(KEY_WITH_XMINCMIN);
			break;
		case KVENGINE_TRANSACTIONDB:
			initkey = init_basis_in_keydesc(RAW_KEY);
			break;
		default:
			break;
		}
		initkey.init_type = VALUE_NULL_KEY;
		initkey.rel_id = scan->rks_rel->rd_id;
		initkey.pk_id = get_pk_oid(scan->rks_rel);
		initkey.isend = true;
		end_key = build_key(initkey);
	}
	if (end_key.len > MAX_SLICE_LEN)
	{
		core_dump();
	}
	scan->end_key.data = end_key.data;
	scan->end_key.len = end_key.len;
}

void initial_end_next_key(KVEngineScanDesc scan, TupleKeySlice *key)
{
	TupleKeySlice endnext_key;
	endnext_key.data = key->data;
	endnext_key.len = key->len;
	if (key->data == NULL)
	{
		InitKeyDesc initkey;
		switch (transaction_type)
		{
		case KVENGINE_ROCKSDB:
			initkey = init_basis_in_keydesc(KEY_WITH_XMINCMIN);
			break;
		case KVENGINE_TRANSACTIONDB:
			initkey = init_basis_in_keydesc(RAW_KEY);
			break;
		default:
			break;
		}
		initkey.init_type = VALUE_NULL_KEY;
		initkey.rel_id = scan->rks_rel->rd_id;
		initkey.pk_id = get_pk_oid(scan->rks_rel);
		initkey.isend = true;
		endnext_key = build_key(initkey);
	}
	if (endnext_key.len > MAX_SLICE_LEN)
	{
		core_dump();
	}
	scan->endnext_key.data = endnext_key.data;
	scan->endnext_key.len = endnext_key.len;
}


KVEngineScanDesc
kvengine_beginscan(Relation relation, Snapshot snapshot)
{
	RelationIncrementReferenceCount(relation);
	Oid pk_oid = get_pk_oid(relation);
	TupleKeySlice *key = palloc0(sizeof(TupleKeySlice));
	key->data = NULL;
	key->len = 0;
	bool isforward = false;
	if (rocksdb_scan_foward == ROCKSDBSCAN_FORWARD)
		isforward = true;
	else
		isforward = false;
	// if (transam_mode == TRANSAM_MODE_BOCC || 
	// 	transam_mode == TRANSAM_MODE_OCC/* || 
	// 	consistency_mode == CONSISTENCY_MODE_CAUSAL*/)
	// 	isforward = true;
	return initKVEngineScanDesc(relation, pk_oid, snapshot, key, key, isforward);
}

KVEngineScanDesc
kvengine_beginscan_by_key(Relation relation,
						  Oid index_id,
						  Snapshot snapshot,
						  TupleKeySlice *startkey,
						  TupleKeySlice *endkey,
						  bool isforward)
{
	RelationIncrementReferenceCount(relation);
	return initKVEngineScanDesc(relation, index_id, snapshot, startkey, endkey, isforward);
}

KVEngineScanDesc
initKVEngineScanDesc(Relation rel,
					 Oid index_id,
					 Snapshot snapshot,
					 TupleKeySlice *startkey,
					 TupleKeySlice *endkey,
					 bool isforward)
{
	/* Alloc and initial scan description. */
	KVEngineScanDesc scan = (KVEngineScanDesc)palloc0(sizeof(*scan));
	scan->rks_rel = rel;
	scan->snapshot = snapshot;
	scan->isforward = isforward;
	scan->scan_index = 0;
	scan->scan_num = 0;
	scan->index_id = index_id;
	initial_start_key(scan, startkey);
	initial_next_key(scan, startkey);
	initial_end_key(scan, endkey);
	initial_end_next_key(scan, endkey);
	return scan;
}

void kvengine_getnext(KVEngineScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	Assert(scan->scan_index <= scan->scan_num);

	if (scan->scan_index == scan->scan_num)
	{
		scan->scan_index = 0;
		if (scan->isforward)
		{
			if (scan->next_key.len > 0)
			{
				kvengine_supplement_tuples(scan, ROCKSDB_SCAN);
			}
			else
			{
				Assert(!scan->next_key.data);
				scan->scan_num = 0;
			}
		}
		else
		{
			if (scan->end_key.len > 0)
			{
				kvengine_supplement_tuples(scan, ROCKSDB_SCAN);
			}
			else
			{
				Assert(!scan->end_key.data);
				scan->scan_num = 0;
			}
		}
		second_and_search_again(scan, slot);
	}
	if (scan->scan_num == 0)
		ExecClearTuple(slot);
	else
		pick_to_slot(scan, slot);
}

void second_and_search_again(KVEngineScanDesc scan, TupleTableSlot *slot)
{
	Oid pk_Oid = get_pk_oid(scan->rks_rel);
	if (pk_Oid != scan->index_id)
	{
		TupleKeySlice *allKey = (TupleKeySlice *)palloc0(
			mul_size(scan->scan_num, sizeof(TupleKeySlice)));
		for (int i = 0; i < scan->scan_num; i++)
		{
			TupleKeySlice temp;
			if (transaction_type == KVENGINE_ROCKSDB)
				temp = decode_pkey(scan->cur_key[i], scan->rks_rel, pk_Oid, 0, 0);
			else
				temp = decode_pkey_raw(scan->cur_key[i], scan->rks_rel, pk_Oid);
			
			allKey[i] = temp;
		}
		kvengine_get_by_pk(scan, slot, allKey);
	}
}

void kvengine_supplement_tuples(KVEngineScanDesc scan, int req_type)
{
	ScanResponse *scan_res = kvengine_send_index_scan_req(scan->start_key, 
														  scan->endnext_key,
														  scan->next_key, scan->end_key,
														  scan->snapshot, scan->isforward,
														  scan->issecond, req_type,
														  scan->Scantype);
	/*
	 * Transfer data from response to scan. Cause scanResponse only remains
     * valid until the next receive operation is performed on the queue, we
     * need save KV results to ScanDesc.
	 */
	DataSlice *next_key = get_slice_from_buffer(scan_res->buffer);
	Size index = size_of_Keylen(*next_key);
	if (scan->isforward)
	{
		if (next_key->len > 0)
			save_data_slice(scan->next_key, *next_key);
		else
			clean_slice(scan->next_key);
	}
	else
	{
		if (next_key->len > 0)
			save_data_slice(scan->end_key, *next_key);
		else
			clean_slice(scan->end_key);
	}
	range_free(next_key);
	decode_scan_res_into_scandesc(scan, scan_res, index);
}

void kvengine_get_by_pk(KVEngineScanDesc scan, TupleTableSlot *slot, TupleKeySlice *all_key)
{
	ScanResponse *scan_res = kvengine_send_cycle_get_req(all_key,
														 scan->scan_num, 
														 scan->snapshot, 
														 scan->Scantype);
	decode_scan_res_into_scandesc(scan, scan_res, 0);
}

void put_value_into_slot(TupleKeySlice key, TupleValueSlice value, TupleTableSlot *slot)
{
	ExecStoreAllNullTuple(slot);
	ExecStoreKey(key, slot, false /* shouldFree */);
	for (Size i = 0; i < value.data->sysattrs.natts; ++i)
		slot->PRIVATE_tts_values[i] = memtuple_getattr(&value.data->memtuple,
													   slot->tts_mt_bind, i + 1, 
													   &(slot->PRIVATE_tts_isnull[i]));
}

void pick_to_slot(KVEngineScanDesc scan, TupleTableSlot *slot)
{
	Assert(scan->scan_index < scan->scan_num);
	TupleKeySlice key = scan->cur_key[scan->scan_index];
	TupleValueSlice value = scan->cur_value[scan->scan_index];

	put_value_into_slot(key, value, slot);
	slot->tts_rangeid = scan->rangeid[scan->scan_index];
	++(scan->scan_index);

}

void kvengine_endscan(KVEngineScanDesc scan)
{
	if (scan->rks_rel != NULL)
		RelationDecrementReferenceCount(scan->rks_rel);
	kvengine_free_scandesc(scan);
}

void kvengine_free_scandesc_key_value(KVEngineScanDesc scan)
{
	for (int i = 0; i < scan->scan_num; i++)
	{
		clean_slice(scan->cur_key[i]);
		clean_slice(scan->cur_value[i]);
	}
}

void kvengine_free_scandesc(KVEngineScanDesc scan)
{
	kvengine_free_scandesc_key_value(scan);
	clean_slice(scan->next_key);
	clean_slice(scan->end_key);
	pfree(scan);
}

void kvengine_rescan(KVEngineScanDesc scan)
{
	scan->scan_index = 0;
	scan->scan_num = 0;
	clean_slice(scan->next_key);
	clean_slice(scan->end_key);
	TupleKeySlice *key = palloc0(sizeof(TupleKeySlice));
	key->data = NULL;
	key->len = 0;
	initial_start_key(scan, key);
	initial_next_key(scan, key);
	initial_end_key(scan, key);
	initial_end_next_key(scan, key);
}

bool kvengine_check_unique_and_insert(Relation rel,
									  TupleKeySlice key, TupleValueSlice value,
									  RangeID rangeid, bool isSecondary)
{
	while (true)
	{
		SnapshotData SnapshotDirty;
		InitDirtySnapshot(SnapshotDirty);

		PutResponse *putres = kvengine_send_put_req(key, value,
													rangeid, true, isSecondary, &SnapshotDirty);
		bool is_unique = putres->checkUnique;

		if (is_unique)
			return true;
		TransactionId xmin = putres->xmin;
		TransactionId xmax = putres->xmax;

		TransactionId xwait = (TransactionIdIsValid(xmin)) ? xmin : xmax;
		if (rel == NULL)
		{
			return false;
		}
		/* now I just ereport */
		if (!TransactionIdIsValid(xwait) || TransactionIdEquals(xwait, GetTopTransactionIdIfAny()))
		{
			return false;
		}
		XactLockTableKVWait(xwait, rel, XLTW_InsertIndex);
	}
}

bool kvengine_delete_direct(TupleKeySlice key)
{
	DeleteDirectResponse *dres = kvengine_send_delete_direct_req(key);
	return dres->success;
}

bool kvengine_prepare(void)
{
	if (!CurrentReadWriteSetSlot)
		return true;
	Size size = sizeof(RequestHeader);
	RequestHeader *req = (RequestHeader *)palloc0(size);
	SessionSaveTransactionState(req);
	req->writebatch = true;
	req->type = ROCKSDB_PREPARE;
	req->size = size;

	ResponseHeader *res = send_kv_request(req);
	PrepareResponse *cres = (PrepareResponse*)res;
	range_free(req);
	if (res)
	{
		if (CheckLogicalTsIntervalValid(res->cts, res->nts))
			return true;
	}
	return false;
}

bool kvengine_commit(void)
{
	if (transam_mode == TRANSAM_MODE_NEW_OCC && 
		!CurrentReadWriteSetSlot)
		return true;
	Size size = sizeof(RequestHeader);
	RequestHeader *req = (RequestHeader *)palloc0(size);
	SessionSaveTransactionState(req);
	req->writebatch = true;
	req->type = ROCKSDB_COMMIT;
	req->size = size;

	/*
	 * The response just tell session the request has been processed. It contains
	 * no informations so we need not return it.
	 */
	ResponseHeader *res = send_kv_request(req);
	CommitResponse *cres = (CommitResponse*)res;
	range_free(req);
	if (res)
	{
		CommitTranCountAdd();
		if (transam_mode == TRANSAM_MODE_NEW_OCC)
		{
			CheckLogicalTsIntervalValid(res->cts, res->nts);
		}
		if (consistency_mode == CONSISTENCY_MODE_CAUSAL)
			global_tmp_timestamp.start_ts = global_tmp_timestamp.commit_ts;
		if (consistency_mode == CONSISTENCY_MODE_SEQUENCE) 
		{
			update_with_cts(global_tmp_timestamp.commit_ts);
		}
		return true;
	}
	else
	{
		RollBackCountAdd();
		return false;
	}
}

bool kvengine_abort(void)
{
	if (transam_mode == TRANSAM_MODE_NEW_OCC && 
		!CurrentReadWriteSetSlot)
		return true;
	Size size = sizeof(RequestHeader);
	RequestHeader *req = (RequestHeader *)palloc0(size);
	SessionSaveTransactionState(req);
	req->writebatch = true;
	req->type = ROCKSDB_ABORT;
	req->size = size;
	RollBackCountAdd();
	/*
	 * The response just tell session the request has been processed. It contains
	 * no informations so we need not return it.
	 */
	ResponseHeader *res = send_kv_request(req);
	range_free(req);
	if (res)
		return true;
	else
		return false;
}

bool kvengine_clear(void)
{
	if (!CurrentReadWriteSetSlot)
		return true;
	Size size = sizeof(RequestHeader);
	RequestHeader *req = (RequestHeader *)palloc0(size);
	SessionSaveTransactionState(req);
	req->writebatch = true;
	req->type = ROCKSDB_CLEAR;
	req->size = size;

	ResponseHeader *res = send_kv_request(req);
	PrepareResponse *cres = (PrepareResponse*)res;
	range_free(req);
	if (res)
		return true;
	else
		return false;
}

KVEngineScanDesc
kvengine_his_normal_beginscan(Relation relation, long xmin_ts, long xmax_ts)
{
	RelationIncrementReferenceCount(relation);

	/* Alloc and initial scan description. */
	KVEngineScanDesc scan = palloc0(sizeof(*scan));
	scan->rks_rel = relation;
	scan->isforward = true;
	scan->scan_index = 0;
	scan->scan_num = 0;
	Oid pk_oid = get_pk_oid(scan->rks_rel);
	scan->index_id = pk_oid;
	GenericSlice startKeyArray = encode_comp_scan_historical_key(relation, NULL, NULL, NULL, 0, 0, 0, false);
	GenericSlice endKeyArray = encode_comp_scan_historical_key(relation, NULL, NULL, NULL, 0, xmin_ts + 1, xmax_ts + 1, true);
	initial_next_key(scan, (TupleKeySlice*) &startKeyArray);
	initial_end_key(scan, (TupleKeySlice*) &endKeyArray);
	return scan;
}

void
kvengine_history_getnext(KVEngineScanDesc scan, TupleTableSlot* slot)
{
	Assert(scan->scan_index <= scan->scan_num);
	if (scan->scan_num == 0 || scan->scan_index == scan->scan_num)
	{
		scan->scan_index = 0;
		if (scan->next_key.len > 0)
		{
			kvengine_history_supplement_tuples(scan, slot);
		}
		else
		{
			// Assert(!scan->next_key.data);
			scan->scan_num = 0;
		}
	}
	if (scan->scan_num == 0)
		ExecClearTuple(slot);
	else
		pick_his_to_slot(scan, slot);
}

void
kvengine_history_supplement_tuples(KVEngineScanDesc scan, TupleTableSlot* slot)
{
	ScanResponse* scan_res = kvengine_send_history_scan_req(scan->next_key, 
											scan->end_key, scan->time, scan->isforward);
	/*
	 * Transfer data from response to scan. Cause scanResponse only remains valid until the next
	 * receive operation is performed on the queue, we need save KV results to ScanDesc.
	 */
	DataSlice *next_key = get_slice_from_buffer(scan_res->buffer);
	Size index = size_of_Keylen(*next_key);
	if (scan->isforward)
	{
		if (next_key->len > 0)
			save_data_slice(scan->next_key, *next_key);
		else
			clean_slice(scan->next_key);
	}
	else
	{
		if (next_key->len > 0)
			save_data_slice(scan->end_key, *next_key);
		else
			clean_slice(scan->end_key);
	}
	range_free(next_key);

	// Size index = 0;
	scan->scan_num = scan_res->num;

	for (int i = 0; i < scan_res->num; ++i)
	{
		DataSlice *key = get_slice_from_buffer(scan_res->buffer + index);
		index += size_of_Keylen(*key);
		DataSlice *value = get_slice_from_buffer(scan_res->buffer + index);
		index += size_of_Keylen(*value);
		index += sizeof(Oid);
		save_data_slice(scan->cur_key[i], *key);
		save_data_slice(scan->cur_value[i], *value);
	}
	// scan->next_key.len = 0;
}

void
pick_his_to_slot(KVEngineScanDesc scan, TupleTableSlot* slot)
{
	Assert(scan->scan_index < scan->scan_num);
	GenericSlice key = *(GenericSlice*)&(scan->cur_key[scan->scan_index]);
	GenericSlice value = *(GenericSlice*)&(scan->cur_value[scan->scan_index]);
	HistoricalKV kv = {key, value};
	ExecStoreAllNullTuple(slot);

	ExecStoreKey(*(TupleKeySlice*)&key, slot, false /* shouldFree */);
	/* 
	 * We use natts recorded in our value instead of TupleDesc.natts because relation's columns may has been added
	 * ALTER TABLE. It would cause index overflow when we go through values stored in memtuple and some of values
	 * my be undefined.
	 */
	if (CompletedHistoricalKVInterval == -2) 
	{
		TupleValueSlice tvalue = *(TupleValueSlice*)&value;
		for (Size i = 0; i < tvalue.data->sysattrs.natts; ++i)
			slot->PRIVATE_tts_values[i] = memtuple_getattr(&tvalue.data->memtuple,
													   slot->tts_mt_bind, i + 1, 
													   &(slot->PRIVATE_tts_isnull[i]));
	}
	else 
	{
		int natts = 0;
		GenericSlice * slotvalue = decode_values(kv, &natts);
		for (Size i = 0; i < natts; ++i) {
			if(slotvalue[i].len != -1) // attention that typedef size_t Size
				slot->PRIVATE_tts_values[i] = fetchatt(slot->tts_mt_bind->tupdesc->attrs[i], slotvalue[i].data); // convert every data into Datum
			else
			{
				slot->PRIVATE_tts_values[i] = NULL;
			}
			
			// slot->PRIVATE_tts_values[i] = NULL;
			slot->PRIVATE_tts_isnull[i] = slotvalue[i].data == NULL ? true : false;
			// slot->PRIVATE_tts_isnull[i] = false;
			// slot->PRIVATE_tts_isnull[i] = true;
		}
	}
	++(scan->scan_index);
}