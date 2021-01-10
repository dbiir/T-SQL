
/*-------------------------------------------------------------------------
 *
 * timestamp_generate_key.c
 *		some universal function about kv build
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *		kv/timestamp_generate_key.c
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
#include "tdb/range_struct.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "tdb/range_plan.h"
#include "tdb/storage_param.h"

#include "tdb/kv_universal.h"
#include "tdb/timestamp_transaction/timestamp_generate_key.h"

/* compute the size of TupleKeyData */
#define compute_size_of_TupleKeyData(length) \
	(KEY_HEADER_LENGTH + (length))

#define compute_size_of_TupleKeyData_Secondary(length) \
	(KEY_HEADER_LENGTH +                                   \
	 (length) +                                        \
	 sizeof(pk_len_type))

#define get_second_key_pklen_offset(SourceSlice) \
	((SourceSlice).len - sizeof(pk_len_type))

#define get_second_key_pk_length(SourceSlice) \
	((Size)(*(pk_len_type *)((char *)(SourceSlice).data + get_second_key_pklen_offset(SourceSlice))))

static void *put_sk_into_buffer_raw(InitKeyDesc initkey, Size *secondkey_len, Size *pkey_len);
static TupleKeySlice build_null_k_raw(InitKeyDesc initkey, int null_level, int isend);
static TupleKeySlice build_pk_raw(InitKeyDesc initkey);
static TupleKeySlice build_sk_raw(InitKeyDesc initkey);
static void *put_pk_into_buffer_raw(InitKeyDesc initkey, Size *pkey_len);
static void *put_sk_into_buffer_raw(InitKeyDesc initkey, Size *secondkey_len, Size *pkey_len);

TupleKeySlice
decode_pkey_raw(TupleKeySlice second_key, Relation rel, Oid pkoid)
{
	Size length_offset = get_second_key_pklen_offset(second_key);
	Size pk_len = get_second_key_pk_length(second_key);
	Size pk_value_offset = length_offset - pk_len;

	Size key_len = compute_size_of_TupleKeyData(pk_len);
	TupleKey kvengine_key = palloc0(key_len);
	kvengine_key->type = GTS_KEY;
	kvengine_key->rel_id = rel->rd_id;
	kvengine_key->indexOid = pkoid;

	set_TupleKeyData_all_value(kvengine_key, (char *)second_key.data + pk_value_offset, pk_len);

	TupleKeySlice key = {kvengine_key, key_len};
	return key;
}

TupleKeySlice
build_null_k_raw(InitKeyDesc initkey, int null_level, int isend)
{
	switch (null_level)
	{
	case ALL_NULL_KEY:
	{
		TupleKey kvengine_key = palloc0(sizeof(TupleKeyData));
		if (isend)
		{
			kvengine_key->type = GTS_KEY;
			kvengine_key->rel_id = MAX_TABLE_ID;
			kvengine_key->indexOid = MAX_INDEX_ID;
		}
		else
		{
			kvengine_key->type = GTS_KEY;
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
			kvengine_key->type = GTS_KEY;
			kvengine_key->rel_id = USER_DATA_START;
			kvengine_key->indexOid = MAX_INDEX_ID;
		}
		else
		{
			kvengine_key->type = GTS_KEY;
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
		kvengine_key->type = GTS_KEY;
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
		TupleKey kvengine_key = palloc0(sizeof(TupleKeyData));
		kvengine_key->type = GTS_KEY;
		kvengine_key->rel_id = initkey.rel_id;
		kvengine_key->indexOid = initkey.pk_id; /* we assume that the index id is stored in pk_id */
		if (isend)
		{
			MemSet(kvengine_key->other_data, MAX_CHAR, 4);
		}
		else
		{
			MemSet(kvengine_key->other_data, MIN_CHAR, 4);
		}
		TupleKeySlice key = {kvengine_key, sizeof(TupleKeyData)};
		return key;
	}
	default:
		break;
	}
	TupleKeySlice key = {NULL, 0};
	return key;
}

TupleKeySlice
build_pk_raw(InitKeyDesc initkey)
{
	Size temp_len;
	void *buffer = put_pk_into_buffer_raw(initkey, &temp_len);

	Size key_len = compute_size_of_TupleKeyData(temp_len);

	TupleKey kvengine_key = palloc0(key_len);
	kvengine_key->type = GTS_KEY;
	kvengine_key->rel_id = initkey.rel_id;
	kvengine_key->indexOid = initkey.pk_id;
	/* [hongyaozhao] add primary key value as a part of rocks_key */
	set_TupleKeyData_all_value(kvengine_key, buffer, temp_len);

	TupleKeySlice key = {kvengine_key, key_len};
	pfree(buffer);
	return key;
}

TupleKeySlice
build_sk_raw(InitKeyDesc initkey)
{
	Size pkey_len, secondkey_len;
	void *buffer = put_sk_into_buffer_raw(initkey, &secondkey_len, &pkey_len);
	Size temp_len = pkey_len + secondkey_len + sizeof(pk_len_type);

	Size key_len = compute_size_of_TupleKeyData(temp_len);
	TupleKey kvengine_key = palloc0(key_len);
	kvengine_key->type = GTS_KEY;
	kvengine_key->rel_id = initkey.rel_id;
	kvengine_key->indexOid = initkey.second_index_id;
	/* [hongyaozhao] add primary key value as a part of rocks_key */
	set_TupleKeyData_all_value(kvengine_key, buffer, pkey_len + secondkey_len);
	/* second index key different to pk is here */
	set_TupleKeyData_pk_value_len(kvengine_key, pkey_len, pkey_len + secondkey_len);

	TupleKeySlice key = {kvengine_key, compute_size_of_TupleKeyData_Secondary(pkey_len + secondkey_len)};
	return key;
}

#if 0
void *
put_pk_into_buffer_raw(InitKeyDesc initkey, Size *pkey_len)
{
	Size pk_len = 0;
	char **pk_buffer_array = palloc0(sizeof(char *) * initkey.pk_att_num);
	Size *pk_buffer_length = palloc0(sizeof(Size) * initkey.pk_att_num);

	for (int i = 0; i < initkey.pk_att_num; ++i)
	{
		pk_buffer_array[i] = datum_encode_into_buffer(initkey.pk_values[i], initkey.pk_att_types[i], &pk_buffer_length[i], initkey.pk_isnull[i], initkey.isend);

		pk_len += pk_buffer_length[i];
	}

	*pkey_len = pk_len;

	void *head = palloc0(pk_len);
	void *temp = head;

	for (int i = 0; i < initkey.pk_att_num; ++i)
	{
		memcpy(temp, pk_buffer_array[i], pk_buffer_length[i]);
		temp = (void *)((char *)temp + pk_buffer_length[i]);
		pfree(pk_buffer_array[i]);
	}
	pfree(pk_buffer_array);
	pfree(pk_buffer_length);
	return head;
}

void *
put_sk_into_buffer_raw(InitKeyDesc initkey, Size *secondkey_len, Size *pkey_len)
{
	Size second_len = 0, pk_len = 0;

	char **second_buffer_array = palloc0(sizeof(char *) * initkey.second_att_num);
	Size *second_buffer_length = palloc0(sizeof(Size) * initkey.second_att_num);
	for (int i = 0; i < initkey.second_att_num; ++i)
	{

		second_buffer_array[i] = datum_encode_into_buffer(initkey.second_values[i], initkey.second_att_types[i], &second_buffer_length[i], initkey.second_isnull[i], initkey.isend);

		second_len += second_buffer_length[i];
	}

	void *pk_buffer = put_pk_into_buffer_raw(initkey, &pk_len);
	*secondkey_len = second_len;
	*pkey_len = pk_len;

	void *head = palloc0(second_len + pk_len);
	void *temp = head;
	for (int i = 0; i < initkey.second_att_num; ++i)
	{
		memcpy(temp, second_buffer_array[i], second_buffer_length[i]);
		temp = (void *)((char *)temp + second_buffer_length[i]);
		pfree(second_buffer_array[i]);
	}
	memcpy(temp, pk_buffer, pk_len);
	pfree(second_buffer_array);
	pfree(second_buffer_length);
	pfree(pk_buffer);
	return head;
}
#else
void *
put_pk_into_buffer_raw(InitKeyDesc initkey, Size *pkey_len)
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

void *
put_sk_into_buffer_raw(InitKeyDesc initkey, Size *secondkey_len, Size *pkey_len)
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

	void *pk_buffer = put_pk_into_buffer_raw(initkey, &pk_len);
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
#endif
/*
 * the function to build the tuple key, including primary key and secondary key raw, without xmin and cmin
 * key format : tableid + indexid + indexColumnValue + primaryColumnValue + primaryValueLength
 *
 * rel: offer table id
 * indexoid: offer index id
 * colnos: we can decode the column value from slot or values
 * att_num: column num
 * pk_att_start_num: the first pk colnum in colnos
 * slot: in slot, we can find values
 * values: if we don't have slot, we should have values.
 * isnull: Indicates which column is empty
 * isend: indicates whether this key is the rightkey(endkey). If so, we need scan reverse.
 */
TupleKeySlice
build_key_raw(InitKeyDesc initkey)
{
	switch (initkey.init_type)
	{
	case PRIMARY_KEY:
		return build_pk_raw(initkey);
	case SECONDE_KEY:
		return build_sk_raw(initkey);
	case VALUE_NULL_KEY:
		return build_null_k_raw(initkey, VALUE_NULL_KEY, initkey.isend);
	case INDEX_NULL_KEY:
		return build_null_k_raw(initkey, INDEX_NULL_KEY, initkey.isend);
	case ALL_NULL_KEY:
		return build_null_k_raw(initkey, ALL_NULL_KEY, initkey.isend);
	case SYSTEM_NULL_KEY:
		return build_null_k_raw(initkey, SYSTEM_NULL_KEY, initkey.isend);
	default:
		break;
	}
	TupleKeySlice key = {NULL, 0};
	return key;
}

/* primary key prefix means : Tableid + PrimaryIndexid + PrimaryIndexvalue */
char *
get_TupleKeySlice_primarykey_prefix_raw(TupleKeySlice SourceSlice, Size *length)
{
	*length = SourceSlice.len;
	return (char *)(SourceSlice).data;
}

/* Secondary key prefix means : Tableid + Indexid + Indexvalue */
char *
get_TupleKeySlice_secondarykey_prefix_raw(TupleKeySlice SourceSlice, Size *length)
{
	Size pkey_len = ((Size)(*(pk_len_type *)((char *)(SourceSlice).data + SourceSlice.len - sizeof(pk_len_type))));
	*length = SourceSlice.len - sizeof(pk_len_type) - pkey_len;

	return (char *)(SourceSlice).data;
}
