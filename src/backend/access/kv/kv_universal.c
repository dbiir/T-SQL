
/*-------------------------------------------------------------------------
 *
 * kv_universal.c
 *		some universal function about kv build
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *		kv/kv_universal.c
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

#include "tdb/pg_transaction/pgtransaction_generate_key.h"
#include "tdb/timestamp_transaction/timestamp_generate_key.h"
#include "tdb/his_transaction/his_generate_key.h"
/*
 * param:
 * num: To get int type data of length
 * return: int length
 */
static int
intlen(int num)
{
	int tmpn = num;
	int len = 1;
	while (tmpn /= 10)
		len++;
	return len;
}

/*
 * param:
 * dest: Storage location after int is converted to char *
 * num: int value
 * intlen: int length
 */
static void
IntoStrWithIntlen(char *dest, int num, int intlen)
{
	int tmpn2 = num;
	char tmpc[intlen];
	int i = intlen;
	//tmpc[intlen] = '\0';
	while (tmpn2)
	{
		tmpc[i] = 48 + (tmpn2 % 10);
		if (!(tmpn2 /= 10))
			tmpc[i-1] = 40 + tmpn2;
		i--;
	}
	memcpy(dest, tmpc, sizeof(tmpc));
}

static void
intostr(char *dest, int num)
{
	int len = intlen(num);
	IntoStrWithIntlen(dest, num, len);
}

char*
TransferSegIDToIPPortList(SegmentID* segid, int segcount, int *length)
{
	char **IPport = palloc0(sizeof(char*) * (segcount));
	int *lengths = palloc0(sizeof(int*) * (segcount));
	int all_length = 0;
	for (int i = 0; i < (segcount); i++)
	{
		SegmentID seg = segid[i];
		if (seg < 0 || seg >= MAXSEGCOUNT)
		{
			ereport(WARNING,(errmsg("PAXOS: segid %d is invalid", seg)));
			return NULL;
		}
		char *hostip = ssm_statistics->ip[seg];
		if (strlen(hostip) == 0)
		{
			ereport(WARNING,(errmsg("PAXOS: host ip %s is not valid", hostip)));
			/*
			 * TODO:
			 * Since it is currently only tested on one machine,
			 * we can fill in the machine here and need to be modified later.
			 */
			hostip = "127.0.0.1";
		}

		char *ipp = palloc0(strlen(hostip) + intlen(StoragePort[seg + 1]) + 1);
		memcpy(ipp, hostip, strlen(hostip));
		char *temp = ipp + strlen(hostip);
		*temp = ':';
		temp++;
		intostr(temp, StoragePort[seg + 1]);
		lengths[i] = strlen(hostip) + intlen(StoragePort[seg + 1]) + 1;
		all_length += strlen(hostip) + intlen(StoragePort[seg + 1]) + 2;
		IPport[i] = ipp;
	}
	char *result = palloc0(all_length);
	char *temp = result;
	for (int i = 0; i < segcount; i++)
	{
		memcpy(temp, IPport[i], lengths[i]);
		temp += lengths[i];
		*temp = ',';
		temp++;
		pfree(IPport[i]);
	}
	temp--;
	*temp = '\0';
	(*length) = all_length;
	pfree(lengths);
	pfree(IPport);

	return result;
}

void
get_value_from_slot(TupleTableSlot* slot,
					int *colnos,
					int att_num,
					Datum *values,
					bool *isnull)
{
	for (int i = 0; i < att_num; i++)
	{
		values[i] = slot_getattr(slot, colnos[i], &isnull[i]);
	}
}

RocksUpdateFailureData
initRocksUpdateFailureData()
{
    RocksUpdateFailureData rufd;
    rufd.cmax = 0;
    rufd.xmax = 0;
    rufd.key.len = 0;
    rufd.key.data = NULL;
    rufd.value.len = 0;
    rufd.value.data = NULL;
    return rufd;
}

Size
getRocksUpdateFailureDataLens(RocksUpdateFailureData rufd)
{
    return sizeof(RocksUpdateFailureData) + rufd.key.len + rufd.value.len;
}

RocksUpdateFailureData
decodeRocksUpdateFailureData(char* buffer)
{
    RocksUpdateFailureData rufd = *(RocksUpdateFailureData*)buffer;
    char* tmp = buffer + sizeof(RocksUpdateFailureData);
    rufd.key.data = palloc0(rufd.key.len);
    if (rufd.key.len != 0)
    {
        rufd.key.data = (TupleKey)tmp;
        //memcpy(rufd.key.data, tmp, rufd.key.len);
        tmp += rufd.key.len;
    }
    else
        rufd.key.data = NULL;
    if (rufd.value.len != 0)
    {
        rufd.value.data = (TupleValue)tmp;
        //palloc0(rufd.value.len);
        memcpy(rufd.value.data, tmp, rufd.value.len);
    }
    else
        rufd.value.data = NULL;
    return rufd;
}

char*
encodeRocksUpdateFailureData(RocksUpdateFailureData rufd)
{
    Size size = getRocksUpdateFailureDataLens(rufd);
    char *buffer = palloc0(size);
    RocksUpdateFailureData *rufdp = (RocksUpdateFailureData*)buffer;
    *rufdp = rufd;
    char *tmp = buffer + sizeof(RocksUpdateFailureData);
    memcpy(tmp, rufd.key.data, rufd.key.len);
    tmp += rufd.key.len;
    memcpy(tmp, rufd.value.data, rufd.value.len);
    return buffer;
}

InitKeyDesc
init_basis_in_keydesc(int key_type)
{
	InitKeyDesc initkey;
    initkey.generate_key_type = key_type;
	initkey.rel_id = 0;
	initkey.init_type = ALL_NULL_KEY;
	initkey.pk_att_num = 0;
	initkey.pk_att_types = NULL;
	initkey.pk_id = 0;
	initkey.pk_values = NULL;
	initkey.pk_isnull = NULL;
	initkey.xid = 0;
	initkey.cid = 0;

	initkey.second_att_num = 0;
	initkey.second_att_types = NULL;
	initkey.second_index_id = 0;
	initkey.second_values = NULL;
	initkey.second_isnull = NULL;
	return initkey;
}

void
init_pk_in_keydesc(InitKeyDesc *initkey,
			Oid	pk_oid,
			Oid* pk_att_type,
			Datum* pk_value,
			bool* pkisnull,
			int pk_natts)
{
	initkey->pk_att_num = pk_natts;
	initkey->pk_att_types = pk_att_type;
	initkey->pk_id = pk_oid;
	initkey->pk_values = pk_value;
	initkey->pk_isnull = pkisnull;
}

void
init_sk_in_keydesc(InitKeyDesc *initkey,
			Oid	sk_oid,
			Oid* sk_att_type,
			Datum* sk_value,
			bool* skisnull,
			int sk_natts)
{
	initkey->second_att_num = sk_natts;
	initkey->second_att_types = sk_att_type;
	initkey->second_index_id = sk_oid;
	initkey->second_values = sk_value;
	initkey->second_isnull = skisnull;
}

/*
 * the function to build the tuple key, including primary key and secondary key
 * key format : tableid + indexid + indexColumnValue + primaryColumnValue + primaryValueLength + xmin + cmin
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
 * xid: transaction id, xmin
 * cid: command id, cmin
 */
TupleKeySlice
build_key(InitKeyDesc initkey)
{
	switch (initkey.generate_key_type)
	{
	case RAW_KEY:
		return build_key_raw(initkey);
	case KEY_WITH_XMINCMIN:
		return build_key_with_xmin_cmin(initkey);
	case KEY_WITH_TEM:
		return build_key_with_temporal(initkey);
	default:
		break;
	}
	TupleKeySlice key = {NULL, 0};
	return key;
}

Oid
get_pk_oid(Relation rel)
{
	List		*indexoidlist = NULL;
	ListCell	*indexoidscan = NULL;
	Oid			pk_oid = InvalidOid;
	/*
	 * Get the list of index OIDs for the table from the relation, and look up
	 * each one in the pg_index syscache until we find one marked primary key
	 * (hopefully there isn't more than one such).
	 * Then we take the primary key as part of the key value in rocksdb.
	 */
	indexoidlist = RelationGetIndexList(rel);
	if (indexoidlist == NIL) {
		list_free(indexoidlist);
		return pk_oid;
	}
	foreach(indexoidscan, indexoidlist)
	{
		Oid indexoid = lfirst_oid(indexoidscan);
		HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		Form_pg_index index = (Form_pg_index) GETSTRUCT(indexTuple);
		/* we're only interested if it is the primary key and valid */
		if (index->indisprimary && IndexIsValid(index))
		{
			pk_oid = indexoid;
			ReleaseSysCache(indexTuple);
			break;
		}
		ReleaseSysCache(indexTuple);
	}

	list_free(indexoidlist);

	return pk_oid;

}

List*
get_index_oid(Relation rel)
{
	List		*indexoidlist = NULL;
	//ListCell	*indexoidscan = NULL;
	//Oid			pk_oid = InvalidOid;
	/*
	 * Get the list of index OIDs for the table from the relation, and look up
	 * each one in the pg_index syscache until we find one marked primary key
	 * (hopefully there isn't more than one such).
	 * Then we take the primary key as part of the key value in rocksdb.
	 */
	indexoidlist = RelationGetIndexList(rel);
	return indexoidlist;
}

int*
get_index_colnos(Oid index_oid, int* natts)
{
	Assert(OidIsValid(index_oid));
	HeapTuple index_tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
	if (!HeapTupleIsValid(index_tuple))
		elog(ERROR, "cache lookup failed for index %u", index_oid);
	Form_pg_index index = (Form_pg_index) GETSTRUCT(index_tuple);

	*natts = index->indnatts;
	Size size = mul_size(sizeof(int), index->indnatts);
	int *colnos = palloc0(size);
	for (int i = 0; i < index->indnatts; i++)
	{
		colnos[i] = (int)index->indkey.values[i];
	}

	ReleaseSysCache(index_tuple);
	return colnos;
}

Oid*
get_attr_type(Relation rel, int* colnos, int natts)
{
	TupleDesc attr = rel->rd_att;
	Size size = mul_size(sizeof(Oid), natts);
	Oid *type_oid_list = palloc0(size);
	for(int i = 0; i < natts; i++)
	{
		type_oid_list[i] = attr->attrs[colnos[i] - 1]->atttypid;
	}
	return type_oid_list;
}

TupleKeySlice 
copy_key(TupleKeySlice key)
{
    TupleKeySlice newkey;
    newkey.data = palloc0(key.len);
    memcpy(newkey.data, key.data, key.len);
    newkey.len = key.len;
    return newkey;
}

TupleValueSlice 
copy_value(TupleValueSlice value)
{
    TupleValueSlice newvalue;
    newvalue.data = palloc0(value.len);
    memcpy(newvalue.data, value.data, value.len);
    newvalue.len = value.len;
    return newvalue;
}

bool 
equal_key(TupleKeySlice a, TupleKeySlice b)
{
	if (a.len != b.len)
		return false;
	else
		return memcmp(a.data, b.data, a.len) == 0;
}