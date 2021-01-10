
/*-------------------------------------------------------------------------
 *
 * pgtransaction_generate_key.c
 *		some universal function about kv build
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *		kv/pgtransaction_generate_key.c
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
#include "tdb/range_struct.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "tdb/range_plan.h"
#include "tdb/storage_param.h"

#include "tdb/kv_universal.h"
#include "tdb/his_transaction/his_generate_key.h"

/* compute the size of TupleKeyData */
#define compute_size_of_TupleKeyData(length) \
		(KEY_HEADER_LENGTH + \
		(length) + \
		sizeof(long) * 2)

#define compute_size_of_TupleKeyData_Secondary(length) \
		(KEY_HEADER_LENGTH + \
		(length) + \
		sizeof(pk_len_type) + sizeof(long) * 2)

#define get_second_key_pklen_offset(SourceSlice) \
	((SourceSlice).len - sizeof(TransactionId) - sizeof(CommandId) - sizeof(pk_len_type))

#define get_second_key_pk_length(SourceSlice) \
	((Size)(*(pk_len_type*)((char*) (SourceSlice).data + get_second_key_pklen_offset(SourceSlice))))

#define get_primary_key_pklen_exclude_xmin_cmin(SourceSlice) \
	((SourceSlice).len - sizeof(TransactionId) - sizeof(CommandId))

static TupleKeySlice build_null_k(InitKeyDesc initkey, int null_level, int isend);
static TupleKeySlice build_pk(InitKeyDesc initkey);
static TupleKeySlice build_sk(InitKeyDesc initkey);
static void* put_pk_into_buffer(InitKeyDesc initkey, Size *pkey_len);
static void* put_sk_into_buffer(InitKeyDesc initkey, Size *secondkey_len, Size *pkey_len);
#if 0 
void
get_primary_key_pk_value(TupleKeySlice SourceSlice, char* TargetBuffer, Size *length)
{
	(*length) = get_primary_key_pklen_exclude_xmin_cmin(SourceSlice) - sizeof(Oid) - sizeof(Oid);
	(TargetBuffer) = (char*) (SourceSlice).data->other_data;
}

void
get_second_key_pk_value(TupleKeySlice SourceSlice, char* TargetBuffer, Size *length)
{
	(*length) = get_second_key_pk_length(SourceSlice);
	Size offset = get_second_key_pklen_offset(SourceSlice) - (*length);
	(TargetBuffer) = (char*) (SourceSlice).data + offset;
}
#endif
#if 0
TupleKeySlice
decode_pkey(TupleKeySlice second_key, Relation rel, Oid pkoid,
				TransactionId xid, CommandId cid)
{
	Size length_offset = get_second_key_pklen_offset(second_key);
	Size pk_len = get_second_key_pk_length(second_key);
	Size pk_value_offset = length_offset - pk_len;

	Size key_len = compute_size_of_TupleKeyData(pk_len);
	TupleKey kvengine_key = palloc0(key_len);
    kvengine_key->type = initkey.his_type;
	kvengine_key->rel_id = rel->rd_id;
	kvengine_key->indexOid = pkoid;

	set_TupleKeyData_all_value(kvengine_key, (char*)second_key.data + pk_value_offset, pk_len);
	set_TupleKeyData_xmin(xid, kvengine_key, pk_len);
	set_TupleKeyData_xmax(cid, kvengine_key, pk_len);

	TupleKeySlice key = {kvengine_key, key_len};
	return key;
}
#endif
TupleKeySlice
build_null_k(InitKeyDesc initkey, int null_level, int isend)
{
	switch (null_level)
	{
	case ALL_NULL_KEY:
		{
			TupleKey kvengine_key = palloc0(sizeof(TupleKeyData));
			if (isend)
			{
                kvengine_key->type = initkey.his_type;
				kvengine_key->rel_id = MAX_TABLE_ID;
				kvengine_key->indexOid = MAX_INDEX_ID;
			}
			else
			{
                kvengine_key->type = initkey.his_type;
				kvengine_key->rel_id = MIN_TABLE_ID;
				kvengine_key->indexOid = MIN_INDEX_ID;
			}
			MemSet(kvengine_key->other_data, 0, 4);
			TupleKeySlice key = {kvengine_key, sizeof(TupleKeyData)};
			return key;
		}
	case SYSTEM_NULL_KEY:
		{
 			TupleKey kvengine_key = palloc0(sizeof(TupleKeyData));
			if (isend)
			{
                kvengine_key->type = initkey.his_type;
				kvengine_key->rel_id = USER_DATA_START;
				kvengine_key->indexOid = MAX_INDEX_ID;
			}
			else
			{
                kvengine_key->type = initkey.his_type;
				kvengine_key->rel_id = MIN_TABLE_ID;
				kvengine_key->indexOid = MIN_INDEX_ID;
			}
			MemSet(kvengine_key->other_data, 0, 4);
			TupleKeySlice key = {kvengine_key, sizeof(TupleKeyData)};
			return key;
		}
	case INDEX_NULL_KEY:
		{
			TupleKey kvengine_key = palloc0(sizeof(TupleKeyData));
            kvengine_key->type = initkey.his_type;
			kvengine_key->rel_id = initkey.rel_id;
			if (isend)
			{
				kvengine_key->indexOid = MAX_INDEX_ID;
			}
			else
			{
				kvengine_key->indexOid = MIN_INDEX_ID;
			}
			MemSet(kvengine_key->other_data, 0, 4);
			TupleKeySlice key = {kvengine_key, sizeof(TupleKeyData)};
			return key;
		}
	case VALUE_NULL_KEY:
		{
			Size key_len = compute_size_of_TupleKeyData(4);
			TupleKey kvengine_key = palloc0(key_len);
            kvengine_key->type = initkey.his_type;
			kvengine_key->rel_id = initkey.rel_id;
			kvengine_key->indexOid = initkey.pk_id;	/* we assume that the index id is stored in pk_id */
			if (isend)
			{
				MemSet(kvengine_key->other_data, MAX_CHAR, 4);
			}
			else
			{
				MemSet(kvengine_key->other_data, MIN_CHAR, 4);
			}
			set_TupleKeyData_xmin(initkey.xmin, kvengine_key, 4);
			set_TupleKeyData_xmax(initkey.xmax, kvengine_key, 4);
			TupleKeySlice key = {kvengine_key, key_len};
			return key;
		}
	default:
		break;
	}
	TupleKeySlice key = {NULL, 0};
	return key;
}

TupleKeySlice
build_pk(InitKeyDesc initkey)
{
	Size temp_len;
	void *buffer = put_pk_into_buffer(initkey, &temp_len);

	Size key_len = compute_size_of_TupleKeyData(temp_len);

	TupleKey kvengine_key = palloc0(key_len);
    kvengine_key->type = initkey.his_type;
	kvengine_key->rel_id = initkey.rel_id;
	kvengine_key->indexOid = initkey.pk_id;
	/* [hongyaozhao] add primary key value as a part of rocks_key */
	set_TupleKeyData_all_value(kvengine_key, buffer, temp_len);
	
	set_TupleKeyData_xmin(initkey.xmin, kvengine_key, temp_len);
	set_TupleKeyData_xmax(initkey.xmax, kvengine_key, temp_len);

	TupleKeySlice key = {kvengine_key, key_len};
	pfree(buffer);
	return key;
}

// todo: Secondary indexes are not implemented yet
TupleKeySlice
build_sk(InitKeyDesc initkey)
{
	Size pkey_len, secondkey_len;
	void *buffer = put_sk_into_buffer(initkey, &secondkey_len, &pkey_len);
	Size temp_len = pkey_len + secondkey_len + sizeof(pk_len_type);

	Size key_len = compute_size_of_TupleKeyData(temp_len);
	TupleKey kvengine_key = palloc0(key_len);
    kvengine_key->type = initkey.his_type;
	kvengine_key->rel_id = initkey.rel_id;
	kvengine_key->indexOid = initkey.second_index_id;
	/* [hongyaozhao] add primary key value as a part of rocks_key */
	set_TupleKeyData_all_value(kvengine_key, buffer, pkey_len + secondkey_len);
	/* second index key different to pk is here */
	set_TupleKeyData_pk_value_len(kvengine_key, pkey_len, pkey_len + secondkey_len);

	set_TupleKeyData_xmin(initkey.xmin, kvengine_key, temp_len);
	set_TupleKeyData_xmax(initkey.xmax, kvengine_key, temp_len);

	TupleKeySlice key = {kvengine_key, compute_size_of_TupleKeyData_Secondary(pkey_len + secondkey_len)};
	return key;
}

void*
put_pk_into_buffer(InitKeyDesc initkey, Size *pkey_len)
{
	Size pk_len = 0;
	char *pk_buffer_first = palloc0(MAX_KEY_SIZE);
	char *tmp = pk_buffer_first;
	for (int i = 0; i < initkey.pk_att_num; ++i)
	{
		Size length = 0;
		datum_encode_into_static_buffer(tmp, initkey.pk_values[i], 
										initkey.pk_att_types[i], &length, 
										initkey.pk_isnull[i], initkey.isend);
		tmp += length;
		pk_len += length;
	}

	*pkey_len = pk_len;

	void *head = palloc0(pk_len);
	memcpy(head, pk_buffer_first, pk_len);
	pfree(pk_buffer_first);

	return head;
}

// todo: Secondary indexes are not implemented yet
void*
put_sk_into_buffer(InitKeyDesc initkey, Size *secondkey_len, Size *pkey_len)
{
	Size second_len = 0, pk_len = 0;
	char *second_buffer_first = palloc0(MAX_KEY_SIZE);
	char *tmp = second_buffer_first;
	for (int i = 0; i < initkey.second_att_num; ++i)
	{
		Size length = 0;
		datum_encode_into_static_buffer(tmp, initkey.second_values[i], 
										initkey.second_att_types[i], &length, 
										initkey.second_isnull[i], initkey.isend);
		tmp += length;
		second_len += length;
	}

	void *pk_buffer = put_pk_into_buffer(initkey, &pk_len);
	*secondkey_len = second_len;
	*pkey_len = pk_len;

	void *head = palloc0(second_len + pk_len);
	void *temp = head;
	memcpy(temp, second_buffer_first, second_len);
	temp += second_len;
	memcpy(temp, pk_buffer, pk_len);
	pfree(second_buffer_first);
	pfree(pk_buffer);
	return head;
}

/*
 * the function to build the tuple key, including primary key and secondary key with xmin and cmin
 * key format : tableid + indexid + indexColumnValue + primaryColumnValue + primaryValueLength + xmin + cmin
 * 
 * type: history key type
 * rel: offer table id
 * indexoid: offer index id
 * colnos: we can decode the column value from slot or values
 * att_num: column num
 * pk_att_start_num: the first pk colnum in colnos
 * slot: in slot, we can find values
 * values: if we don't have slot, we should have values.
 * isnull: Indicates which column is empty
 * isend: indicates whether this key is the rightkey(endkey). If so, we need scan reverse.
 * xmin: the min timestamp can read this key.
 * xmax: the max timestamp can read this key.
 */
TupleKeySlice
build_key_with_temporal(InitKeyDesc initkey)
{
	switch (initkey.init_type)
	{
	case PRIMARY_KEY:
		return build_pk(initkey);
	case SECONDE_KEY:
		// this can not do works.
		return build_sk(initkey);
	case VALUE_NULL_KEY:
		return build_null_k(initkey, VALUE_NULL_KEY, initkey.isend);
	case INDEX_NULL_KEY:
		// this can not go in.
		return build_null_k(initkey, INDEX_NULL_KEY, initkey.isend);
	case ALL_NULL_KEY:
		return build_null_k(initkey, ALL_NULL_KEY, initkey.isend);
	case SYSTEM_NULL_KEY:
		return build_null_k(initkey, SYSTEM_NULL_KEY, initkey.isend);
	default:
		break;
	}
	TupleKeySlice key = {NULL, 0};
	return key;
}
