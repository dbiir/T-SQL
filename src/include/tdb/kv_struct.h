/*-------------------------------------------------------------------------
 *
 * kv_struct.h
 *
 * NOTES
 *	  Save the data structure used by the kv storage engine.
 *
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/kv_struct.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KV_STRUCT_H
#define KV_STRUCT_H

#include "rocksdb/c.h"
#include "access/memtup.h"
#include "utils/rel.h"
#include "utils/snapshot.h"
#include "tdb/encode_type.h"

typedef uint32 pk_len_type; /* pk value length type */
#define MAX_CHAR (0xff)
#define MIN_CHAR (0x00)
#define UNVALID_RANGE_ID 0
#define UNVALID_REPLICA_ID -1
#define MAX_RANGE_ID ((unsigned int)-1)
#define UNVALID_SEG_ID -1
#define MIN_TABLE_ID 0
#define MIN_INDEX_ID 0
#define MAX_TABLE_ID ((unsigned int)-1)
#define MAX_INDEX_ID ((unsigned int)-1)

#define RocksDBPath "pg_rksdb"
#define RocksDBBackupPath "pg_rksbk"
#define RocksDBSystemCatalog "system_catalog"

#define ROCKS_DEFAULT_CF "default"
#define ROCKS_LTS_CF "lts"

#define ROCKS_DEFAULT_CF_I 0
#define ROCKS_LTS_CF_I 1
#define MAX_SCAN_KV_NUM 1000
/* Currently define the maximum number of scans that can be obtained at a time. */
#define SCAN_REQUEST_NUM MAX_SCAN_KV_NUM * 2
#define MAX_SLICE_LEN 1048576

#define MAX_KEY_SIZE (256)
#define MAX_VALUE_SIZE (32768)

#define get_min(a, b) ((a) > (b) ? b : a)

/* compute the size of TupleKeyData */
#define size_of_Keylen(slice) \
	((slice).len + sizeof(Size))

static const uint64_t kMaxSequenceNumber = 0xffffffffffffffff;

/* Do copy and set flag true if two values are different. */
#define set_if_diff(dist, src, flag) \
	do                               \
	{                                \
		if ((dist) != (src))         \
		{                            \
			(dist) = (src);          \
			(flag) = true;           \
		}                            \
	} while (0)

/* Copy slice from src to dist with type of data. */
#define copy_slice(dist, src)                                                 \
	do                                                                        \
	{                                                                         \
		Assert((src).len && (src).data);                                      \
		Assert(((dist).len && (dist).data) || (!(dist).len && !(dist).data)); \
		if ((dist).len != (src).len)                                          \
		{                                                                     \
			/* realloc data */                                                \
			if ((dist).data)                                                  \
				pfree((dist).data);                                           \
			(dist).data = palloc0((src).len);                                 \
		}                                                                     \
		(dist).len = (src).len;                                               \
		memcpy((void *)(dist).data, (void *)(src).data, (src).len);           \
	} while (0)

#define make_data_slice(/*DataSlice*/ dist, src)                               \
	do                                                                         \
	{                                                                          \
		Size len = (src).len;                                                  \
		if (len > MAX_SLICE_LEN)                                               \
			ereport(ERROR, (errmsg("KV: slice size is over!! %d", (int)len))); \
		(dist).len = len;                                                      \
		memcpy((void *)(dist).data, (void *)(src).data, len);                  \
	} while (0)

#define save_data_slice(dist, /*DataSlice*/ src)                               \
	do                                                                         \
	{                                                                          \
		Size len = (src).len;                                                  \
		if (len > MAX_SLICE_LEN)                                               \
			ereport(ERROR, (errmsg("KV: slice size is over!! %d", (int)len))); \
		(dist).len = len;                                                      \
		if ((dist).data)                                                       \
			pfree((dist).data);                                                \
		(dist).data = palloc0(len);                                            \
		memcpy((void *)(dist).data, (void *)(src).data, len);                  \
	} while (0)

#define alloc_slice(slice, size)             \
	do                                       \
	{                                        \
		(slice).len = size;                  \
		Assert((slice).len > 0);             \
		(slice).data = palloc0((slice).len); \
	} while (0)

#define equal_slice(slice_a, slice_b)		\
	((slice_a).len == (slice_b).len && memcmp((slice_a).data, (slice_b).data, (slice_a).len) == 0)

#define clean_slice(slice)       \
	do                           \
	{                            \
		if ((slice).data)        \
			pfree((slice).data); \
		(slice).data = NULL;     \
		(slice).len = 0;         \
	} while (0)

#define slice_is_empty(slice) ((slice).data == NULL)

#define save_slice_into_buffer(buffer, key /*Dataslice*/)                   \
	do                                                                      \
	{                                                                       \
		memcpy((void *)(buffer), (void *)(key), (key)->len + sizeof(Size)); \
	} while (0)

#define pick_slice_from_buffer(buffer /* char* */, slice)                   \
	do                                                                      \
	{                                                                       \
		Size len = *(Size *)(buffer);                                       \
		(slice).len = len;                                                  \
		(slice).data = palloc0((slice).len);                                \
		memcpy((char *)(slice).data, (char *)(buffer) + sizeof(Size), len); \
	} while (0)

#define set_TupleKeyData_hc(TupleKey, ishis) \
	do\
	{\
		(TupleKey)->other_data[0] = ishis?'h':'c';\
	} while (0)

#define set_TupleKeyData_all_value(TupleKey, value, value_length)    \
	do                                                               \
	{                                                                \
		char *key_buffer = (char *)(TupleKey)->other_data;           \
		memcpy((void *)key_buffer, (void *)(value), (value_length)); \
	} while (0)

#define set_TupleKeyData_pk_value_len(TupleKey, pk_length, offset) \
	do                                                             \
	{                                                              \
		char *length = (char *)(TupleKey)->other_data + (offset);  \
		pk_len_type *pk_value_length = (pk_len_type *)length;      \
		*pk_value_length = (pk_len_type)(pk_length);               \
	} while (0)

enum KVReqType
{
	/* rocksdb */
	ROCKSDB_PUT = 0,
	ROCKSDB_PUTRTS,
	ROCKSDB_GET,
	ROCKSDB_SCAN,
	ROCKSDB_CYCLEGET,
	ROCKSDB_DELETE,
	ROCKSDB_MULTI_PUT,
	ROCKSDB_REFRESH_HISTORY,
	ROCKSDB_HISTORY_SCAN,
	ROCKSDB_REQ_TYPE_NUM,
	ROCKSDB_UPDATE,
	ROCKSDB_DELETE_DIRECT,
	ROCKSDB_RANGESCAN,
	ROCKSDB_PREPARE,
	ROCKSDB_COMMIT,
	ROCKSDB_ABORT,
	ROCKSDB_CLEAR,
	ROCKSDB_DETACH,
	STATISTIC_READ,
	STATISTIC_WRITE,
	/* null type */
	GO_ON,
	/* range management*/
	RANGE_SCAN,
	FIND_MIDDLE_KEY,
	INIT_STATISTICS,
	RANGE_CREATE,
	ADDREPLICA,
	REMOVEREPLICA,
	CREATEGROUP,
	REMOVEGROUP,
	ADDGROUPMEMBER,
	REMOVEGROUPMEMBER,
};

enum KeyInitType
{
	PRIMARY_KEY,
	SECONDE_KEY,
	VALUE_NULL_KEY,
	INDEX_NULL_KEY,
	ALL_NULL_KEY,
	SYSTEM_NULL_KEY,
};

typedef struct
{
	void *data;
	Size len;
} GenericSlice;

typedef struct
{
	Size len;
	char data[1];
} DataSlice;

typedef DataSlice *Dataslice;

typedef struct
{
	TransactionId xmin;
	TransactionId xmax; //also be the new xmin.
	uint16 infomask;
	uint16 infomask2;
	CommandId cid; //if no xmax, the cid is cmin, else the cid is cmax.
	CommandId new_cid;
	bool has_new_cid;
	TransactionId lockxid;
	int natts; /* number of attrs in memtuple */

} TupleValueSysAttrs;

typedef struct
{
	TupleValueSysAttrs sysattrs; /* fix len */
	MemTupleData memtuple;		 /* var len */
} TupleValueData;

typedef TupleValueData *TupleValue;

typedef struct
{
	TupleValue data;
	Size len;
} TupleValueSlice;

/*
 * key type 
 * "p": greenplum key
 * "r": raw key, or to say "gts key"
 * "l": lts key
 */
#define GREENPLUM_KEY 'p'
#define GTS_KEY 'r'
#define LTS_KEY 'l'
/*
 * his key type
 * "t": history temp key
 * "c": history complete key
 * "d": history different key
 */
#define HIS_TEMP_KEY 't'
#define HIS_COMP_KEY 'c'
#define HIS_DIFF_KEY 'd'
#define HIS_DIFFCOUNT_KEY 'n'

#define KEY_HEADER_LENGTH 12
/* TupleXXXdata is the lowest struct t store key or value 's data
 * also the data to store in rocksdb
 */
typedef struct
{
	char type;
	Oid rel_id;
	Oid indexOid;
	char other_data[1]; /* include the pk value, xmin and cmin */
						/* the data in the other_data include the below
	 * Datum key_value;
	 * pk_len_type pk_value_length;	//store the pk value length, so we can decode the pk value from non-unique secondary key easily
	 * TransactionId xmin,cmin;
+	 * his: TransactionId xmin,cmin
	 */
} TupleKeyData;

typedef TupleKeyData *TupleKey;

typedef struct TupleKeySlice
{
	TupleKey data;
	Size len;
} TupleKeySlice;

typedef enum
{
	RAW_KEY,
	KEY_WITH_XMINCMIN,
	KEY_WITH_TEM,
} KEY_GEN_TYPE;

typedef struct
{
	int init_type; /* key's type, see above */

	Oid rel_id;
	/* primary key */
	Oid pk_id;
	int pk_att_num;
	Oid *pk_att_types;
	Datum *pk_values;
	bool *pk_isnull;
	/* secondary index key */
	Oid second_index_id;
	int second_att_num;
	Oid *second_att_types;
	Datum *second_values;
	bool *second_isnull;
	/* judge the key is the upper span */
	bool isend;

	int generate_key_type;
	/* MVCC */
	TransactionId xid;
	CommandId cid;

	/* History */
	char his_type;
	long xmin;
	long xmax;
} InitKeyDesc;

typedef struct KVEngineDeleteDescData
{
	Relation rkd_rel;
} KVEngineDeleteDescData;

typedef KVEngineDeleteDescData *KVEngineDeleteDesc;

typedef struct KVEngineUpdateDescData
{
	Relation rkd_rel;
	int retrytime;
} KVEngineUpdateDescData;

typedef KVEngineUpdateDescData *KVEngineUpdateDesc;

typedef struct KVEngineInsertDescData
{
	Relation rki_rel;
} KVEngineInsertDescData;

typedef struct KVEngineInsertDescData *KVEngineInsertDesc;

typedef struct KVEngineScanDescData
{
	Relation rks_rel;
	Oid index_id;
	Snapshot snapshot;
	bool isforward;
	bool startequal;
	bool endequal;
	bool issecond;
	long time;	/* for temporal query */
	TupleKeySlice start_key;
	TupleKeySlice next_key;
	TupleKeySlice end_key;
	TupleKeySlice endnext_key;
	int scan_index; /* the position of currently cached data which we need to read */
	int scan_num;   /* Number of cached kvs */
	TupleKeySlice cur_key[SCAN_REQUEST_NUM];
	TupleValueSlice cur_value[SCAN_REQUEST_NUM];
	Oid rangeid[SCAN_REQUEST_NUM];
	List *fliter;
	CmdType Scantype; /* we use this to decide get or get_for_update */
} KVEngineScanDescData;

typedef struct KVEngineScanDescData *KVEngineScanDesc;

typedef struct RequestHeader
{
	int type;
	int req_id;

	int txn_mode; /* transaction mode, occ, new occ etc*/
	int con_mode; /* consistency mode, sequence mode, casual mode etc*/
	int transaction_op_type;
	Size size;
	TransactionId trx_id;
	SubTransactionId subtrx_id;
	DistributedTransactionId gxid;
	int trx_state;
	bool writebatch;
	int gp_session_id;
	int pid;
	int combocid_map_count;
	uint64 start_ts;
	uint64 commit_ts;
	uint64 lower_lts;
	uint64 upper_lts;
	int comp_his_interval;

} RequestHeader;

typedef struct ResponseHeader
{
	int type;
	int req_id;
	Size size;
	uint64_t cts;
	uint64_t nts;
} ResponseHeader;

typedef struct GetRequest
{
	RequestHeader header;
	//DataSlice key;
	char key[1];
} GetRequest;

typedef struct PutRequest
{
	RequestHeader header;
	bool checkUnique;
	bool isSecondary;
	Oid rangeid;
	SnapshotData snapshot;
	char k_v[1];
	/*DataSlice key;
	DataSlice value;*/
} PutRequest;

typedef struct Delete_UpdateRequest
{
	RequestHeader header;
	int result_type;
	Oid rangeid;
	SnapshotData snapshot;
	CommandId cid;
	TransactionId xid;
	int key_count;

	int result;
	int remain;

	int retry_time;
	char key[1];
	/*
	DataSlice pkey;
	DataSlice pvalue;
	DataSlice secondkey[0];
	DataSlice secondkey[1];
	......
	*/
} Delete_UpdateRequest;

typedef struct DeleteDirectRequest
{
	RequestHeader header;
	char key[1];
	/*DataSlice key;*/
} DeleteDirectRequest;

typedef struct CheckUniqueRequest
{
	RequestHeader header;
	SnapshotData snapshot;
	//DataSlice key;
	char start_key[1];
} CheckUniqueRequest;

typedef struct CheckUniqueResponse
{
	ResponseHeader header;
	//DataSlice key;
	bool isunique;
	TransactionId xmin;
	TransactionId xmax;
} CheckUniqueResponse;

typedef struct ScanWithPkeyRequest
{
	RequestHeader header;
	SnapshotData snapshot;
	//DataSlice start_key;
	char start_and_end_key[1];
} ScanWithPkeyRequest;

typedef struct ScanWithKeyRequest
{
	RequestHeader header;
	Size max_num;
	SnapshotData snapshot;
	bool isforward;
	bool isnext;
	bool issecond;
	CmdType Scantype;
	//DataSlice start_key;
	char start_and_end_key[1];
} ScanWithKeyRequest;

typedef struct ScanWithTimeRequest
{
	RequestHeader header;
    Size max_num;
	bool isnext;
	bool isforward;
	long time;
	//DataSlice start_key;
	char start_and_end_key[1];
} ScanWithTimeRequest;

typedef struct SplitRequest
{
	RequestHeader header;
	//DataSlice key;
	Size all_size;
	char start_and_end_key[1];
} SplitRequest;

typedef struct CreateRangeRequest
{
	RequestHeader header;
	char rangedesc[1];
	//RangeDesc rangedesc;
} CreateRangeRequest;

/* send to leader and new replica */
typedef struct AddReplicaRequest
{
	RequestHeader header;
	Oid rangeid;
	char rangedesc[1];
	//RangeDesc rangedesc;
} AddReplicaRequest;

typedef struct RemoveReplicaRequest
{
	RequestHeader header;
	Oid rangeid;
	Oid replicaid;

} RemoveReplicaRequest;

typedef struct InitStatisticsRequest
{
	RequestHeader header;
	//DataSlice key;
	int new_stat_id;
	Oid rangeid;
	char start_and_end_key[1];
} InitStatisticsRequest;

typedef struct
{
	RequestHeader header;
	char MyIPPort[20];
	//"127.0.0.1:9999"
	Oid groupid;
	char IPPortList[1];
	//"127.0.0.1:9999,127.0.0.1:10000"
} CreateGroupRequest;

typedef struct
{
	RequestHeader header;
	Oid groupid;
} RemoveGroupRequest;

typedef struct
{
	RequestHeader header;
	Oid groupid;
	char NodeIPPort[1];
	//"127.0.0.1:9999"
} AddGroupMemberRequest;

typedef struct
{
	RequestHeader header;
	Oid groupid;
	char NodeIPPort[1];
	//"127.0.0.1:9999"
} RemoveGroupMemberRequest;

typedef struct PrepareResponse
{
	ResponseHeader header;
	bool success;
} PrepareResponse;

typedef struct CommitResponse
{
	ResponseHeader header;
	bool success;
} CommitResponse;

typedef struct ClearResponse
{
	ResponseHeader header;
	bool success;
} ClearResponse;

typedef struct GetResponse
{
	ResponseHeader header;
	char value[1]; /* value buffer */
				   //DataSlice value;
} GetResponse;

typedef struct PutResponse
{
	ResponseHeader header;
	bool checkUnique;
	TransactionId xmin;
	TransactionId xmax;
} PutResponse;

typedef struct DeleteDirectResponse
{
	ResponseHeader header;
	bool success;
} DeleteDirectResponse;

typedef enum Update_stage
{
	UPDATE_COMPLETE,
	UPDATE_ADD_TUPLE_LOCK,
	UPDATE_RELEASE_TUPLE_LOCK,
	UPDATE_XACTID,
	UPDATE_XACTTABLE,
} Update_stage;

typedef struct Delete_UpdateResponse
{
	ResponseHeader header;
	/*
     * 0 means update completed;
     * 1 means acquire tuple lock;
     * 2 means release tuple lock;
     * 2 means XactIdWait;
     * 3 means tablewait;
     */
	int result_type;
	HTSU_Result result;
	TransactionId xwait;
	int oper;   //XLTW_Oper
	int status; //MultiXactStatus
	uint16 infomask;
	char rfd[1];
} Delete_UpdateResponse;

typedef struct ScanResponse
{
	ResponseHeader header;
	Size num;
	char buffer[1];
	/*DataSlice next_key;
	DataSlice key[SCAN_REQUEST_NUM];
	DataSlice value[SCAN_REQUEST_NUM];*/
} ScanResponse;

typedef struct StatisticsResponse
{
	ResponseHeader header;
	bool success;
} StatisticsResponse;

typedef struct RefreshHistoryRequest
{
	RequestHeader header;
} RefreshHistoryRequest;

typedef struct RefreshHistoryResponse
{
	ResponseHeader header;
} RefreshHistoryResponse;

typedef struct MultiPutRequest
{
	RequestHeader header;
	char buffer[1];
} MultiPutRequest;

typedef struct MultiPutResponse
{
	ResponseHeader header;
} MultiPutResponse;


typedef struct RangeResponse
{
	ResponseHeader header;
	Size num;
	char buffer[1];
} RangeResponse;

typedef struct InitStatisticsResponse
{
	RequestHeader header;
	//DataSlice key;
	int new_stat_id;
	bool success;
} InitStatisticsResponse;

typedef struct
{
	ResponseHeader header;
	bool success;
} CreateGroupResponse;

typedef struct
{
	ResponseHeader header;
	bool success;
} RemoveGroupResponse;

typedef struct
{
	ResponseHeader header;
	bool success;
} AddGroupMemberResponse;

typedef struct
{
	ResponseHeader header;
	bool success;
} RemoveGroupMemberResponse;

typedef struct RocksUpdateFailureData
{
	TransactionId xmax;
	CommandId cmax;
	TupleKeySlice key;
	TupleValueSlice value;
} RocksUpdateFailureData;
#endif
