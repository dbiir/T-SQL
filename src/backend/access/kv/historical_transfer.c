#include <stdlib.h>
#include <unistd.h>

#include "postgres.h"

#include "access/clog.h"
#include "tdb/tdbkvam.h"
#include "tdb/historical_transfer.h"
#include "tdb/his_transaction/his_generate_key.h"

// #define PRINT_VALUE

int CompletedHistoricalKVInterval = 100; 
unsigned long long temporal_data_full_key_storage_size = 0;
unsigned long long temporal_data_full_value_storage_size = 0;
unsigned long long temporal_data_delta_key_storage_size = 0;
unsigned long long temporal_data_delta_value_storage_size = 0;
/* [hzy9819] This number is used for record the complete relation data when change times reach it. */

typedef struct his_trxids
{
	TransactionId xmin; // created transaction id
	TransactionId xmax; // dead transaction id
} his_trxids;


static GenericSlice encode_temp_historical_key(Relation rel, TupleTableSlot * slot, long xmin_ts, long xmax_ts);

static GenericSlice encode_temp_historical_value(Relation rel, TupleTableSlot * slot, TransactionId xmin, TransactionId xmax);
static void * encode_values(Relation rel, TupleTableSlot * slot, void * buf, Size *length);
static int decode_diff_values(HistoricalKV kv, GenericSlice ** diff_values, int ** diff_attnos);
static his_trxids decode_trxids(HistoricalKV kv);
static his_timestamps decode_timestamps(HistoricalKV kv);


static GenericSlice encode_diff_historical_value(his_trxids trxids, GenericSlice diff_values[], int diff_attnos[], int diff_natts);
static int record_diff_atts(HistoricalKV cur, HistoricalKV pre, GenericSlice diff_values[], int diff_attnos[]);
static long convert_ending_long(long source);

GenericSlice
copy_generic_key(GenericSlice source)
{
	GenericSlice target = {0};
	alloc_slice(target, source.len);
	memcpy((char *)target.data, (char *)source.data, source.len);
	return target;
}

HistoricalKV
copy_his_kv(HistoricalKV source)
{
	GenericSlice key = {0};
	GenericSlice value = {0};
	alloc_slice(key, source.key.len);
	memcpy((char *)key.data, (char *)source.key.data, source.key.len);

	alloc_slice(value, source.value.len);
	memcpy((char *)value.data, (char *)source.value.data, source.value.len);

	HistoricalKV compkv = { key, value};
	return compkv;
}

void
free_generic_key(GenericSlice source)
{
	pfree(source.data);
}

void
free_his_kv(HistoricalKV source)
{
	pfree(source.key.data);
	pfree(source.value.data);
}

HistoricalKV
encode_temp_historical_kv(Relation rel, TupleTableSlot * slot)
{
	HeapTuple htup = ExecFetchSlotHeapTuple(slot);
	TransactionId xmin = HeapTupleHeaderGetXmin(htup->t_data);
	TransactionId xmax = HeapTupleHeaderGetRawXmax(htup->t_data);
	long xmin_ts = TransactionIdGetEndTS(xmin);
	long xmax_ts = TransactionIdGetEndTS(xmax);
	HistoricalKV kv = {encode_temp_historical_key(rel, slot, xmin_ts, xmax_ts),
						encode_temp_historical_value(rel, slot, xmin, xmax) };

	return kv;
}

GenericSlice
encode_temp_historical_key(Relation rel, TupleTableSlot * slot, long xmin_ts, long xmax_ts)
{
	TupleKeySlice key = {0};

	int pk_natts = 0; // number of columns in index
	Oid pk_oid = get_pk_oid(rel);
	if (pk_oid == InvalidOid)
		return *(GenericSlice*)&key;
	int * pk_colons = get_index_colnos(pk_oid, &pk_natts);

	InitKeyDesc initkey;
	initkey = init_basis_in_keydesc(KEY_WITH_TEM);
	initkey.init_type = PRIMARY_KEY;
	initkey.his_type = HIS_TEMP_KEY;
	initkey.rel_id = rel->rd_id;
	Datum *pkvalues = (Datum *)palloc0(sizeof(Datum) * pk_natts);
	bool *pkisnull = (bool *)palloc0(sizeof(bool) * pk_natts);
	get_value_from_slot(slot, pk_colons, pk_natts, pkvalues, pkisnull);
	init_pk_in_keydesc(&initkey, pk_oid,
					   get_attr_type(rel, pk_colons, pk_natts),
					   pkvalues, pkisnull, pk_natts);
	initkey.isend = false;
	initkey.xmax = xmax_ts;
	initkey.xmin = xmin_ts;
	key = build_key(initkey);
	pfree(pk_colons);
	pfree(pkvalues);
	pfree(pkisnull);

	return *(GenericSlice*)&key;
}

GenericSlice
encode_comp_scan_historical_key(Relation rel, Datum* value, bool* isnull, int pk_colnos[], int pk_natts, long xmin_ts, long xmax_ts, bool is_end)
{
	TupleKeySlice key = {0};

	Oid pk_oid = get_pk_oid(rel);
	if(pk_oid == InvalidOid)
		return *(GenericSlice*)&key;

	InitKeyDesc initkey;
	initkey = init_basis_in_keydesc(KEY_WITH_TEM);
	initkey.init_type = pk_natts == 0 ? VALUE_NULL_KEY : PRIMARY_KEY;
	initkey.his_type = HIS_COMP_KEY;
	initkey.rel_id = rel->rd_id;
	init_pk_in_keydesc(&initkey, pk_oid,
					   get_attr_type(rel, pk_colnos, pk_natts),
					   value, isnull, pk_natts);
	initkey.isend = is_end;
	initkey.xmax = xmax_ts;
	initkey.xmin = xmin_ts;
	key = build_key(initkey);


	return *(GenericSlice*)&key;
}

Oid
decode_rel_id(HistoricalKV kv) 
{
	TupleKey key = (TupleKey)kv.key.data;
	return key->rel_id;	
}

Size 
get_key_length_without_timestamp(HistoricalKV kv)
{
	return kv.key.len - sizeof(long) * 2;
}

GenericSlice
encode_temp_historical_value(Relation rel, TupleTableSlot * slot, TransactionId xmin, TransactionId xmax)
{
	GenericSlice value;
	if (CompletedHistoricalKVInterval == -2) // it means to storage full data
	{
		HeapTuple htup = ExecMaterializeSlot(slot);
		MemTuple mtup = ExecFetchSlotMemTuple(slot);
		Assert(!memtuple_get_hasext(mtup));
		
		Size mtup_size = memtuple_get_size(mtup);
		Size value_len = sizeof(TupleValueSysAttrs) + mtup_size;

		/* make value */
		HeapTuple heaptup = heap_prepare_insert(rel, htup, xmin, 0, 0 /*options*/, false);
		TupleValue kvengine_value = (TupleValue)palloc0(value_len);
		kvengine_value->sysattrs.xmin = xmin;
		kvengine_value->sysattrs.xmax = xmax;
		kvengine_value->sysattrs.infomask = heaptup->t_data->t_infomask;
		kvengine_value->sysattrs.infomask2 = heaptup->t_data->t_infomask2;
		kvengine_value->sysattrs.cid = 0;
		kvengine_value->sysattrs.natts = slot->tts_tupleDescriptor->natts;
		kvengine_value->sysattrs.new_cid = 0;
		kvengine_value->sysattrs.has_new_cid = false;
		memcpy((char *)&kvengine_value->memtuple, (char *)mtup, mtup_size);
		value.data = kvengine_value;
		value.len = value_len;
#ifdef PRINT_VALUE
		// void * buf = palloc(sizeof(TransactionId) * 2 + sizeof(Datum) * rel->rd_att->natts);
		// encode_values(rel, slot, buf);
		// pfree(buf);
#endif
	}
	else 
	{
		void * buf = palloc0(MAX_VALUE_SIZE);
		void * buf_head = buf;
		Size len = 0;
		buf = encode_values(rel, slot, buf, &len);
		value.len = sizeof(TransactionId) * 2 + sizeof(int) + len;
		alloc_slice(value, value.len);
		void * temp = value.data;
		put_value_into_buffer_and_move_buf(temp, xmin, TransactionId);
		put_value_into_buffer_and_move_buf(temp, xmax, TransactionId);
		put_value_into_buffer_and_move_buf(temp, rel->rd_att->natts, int);
		put_mem_into_buffer_and_move_buf(temp, buf_head, len);
		
		Assert((char *) temp - (char *) value.data == value.len);
		pfree(buf_head);
	}
	return value;	
}
/*
*	[hzy9819]
*	We organize the value by this way : |length + value + length + value ...|
*	typeof(length) = Size = size_t = unsigned int
*	if we meet a null on value, then we set Size = -1 (= maxint while unsigned int) and do not storage value
*/

void *
encode_values(Relation rel, TupleTableSlot * slot, void * buf, Size *length)
{
#ifdef PRINT_VALUE
	FILE *fptr;
	fptr = fopen("/tmp/value.txt", "a");
	fprintf(fptr,"value: ");
#endif
	MemTuple mtup = ExecFetchSlotMemTuple(slot);
	for (int i = 0; i < rel->rd_att->natts; ++i)
	{
		bool isnull = false;
		Size len;
		char * value = memtuple_get_attr_value(mtup, slot->tts_mt_bind, i + 1, &isnull, & len);
		/* [hzy9819] deal with varlena data*/
		/* typid = rel->rd_att->attrs[i];
		/getTypeOutputInfo(typid,
						  &typoutput, &typisvarlena);
		if(len == -1) 
		{
			Datum		val;	/* definitely detoasted Datum 

			val = PointerGetDatum(PG_DETOAST_DATUM(value));
			len = VARSIZE_ANY(val);
			value = (char *) val;
		}	*/					  
		if(isnull) 
		{
			* length += sizeof(Size);
			put_value_into_buffer_and_move_buf(buf, -1, Size); // move -1 into buffer at value == null
		} 
		else 
		{
			* length += sizeof(Size); 
			len = len == 0 ? strlen(value) : len; // calculate length if it's a pointer (at len == 0)
			put_value_into_buffer_and_move_buf(buf, len, Size);
			* length += len; 
			put_mem_into_buffer_and_move_buf(buf, value, len);
		}
		// Datum value = memtuple_getattr(mtup, slot->tts_mt_bind, i + 1, &(slot->PRIVATE_tts_isnull[i]));
		// Datum value = slot_getattr(slot, i + 1, &isnull);
		// put_value_into_buffer_and_move_buf(buf, isnull ? 0 : value, Datum);	
		/* Size typesize; // get the length of origin data
		char * buffer = datum_encode_into_buffer(value, rel->rd_att->tdtypeid, &typesize, 0, 0); // storage origin data into buffer via address(value)
		*length += typesize + sizeof(Size);
		put_mem_into_buffer_and_move_buf(buf, typesize, sizeof(Size));
		put_mem_into_buffer_and_move_buf(buf, buffer, typesize);
		pfree(buffer);
		*/
#ifdef PRINT_VALUE
		fprintf(fptr,"%d(len = %u) ", * (int *) value, len);
#endif
	}
#ifdef PRINT_VALUE
	fprintf(fptr,"\n");
	fclose(fptr);
#endif
	return buf;
}

/*
 *	[hzy9819]	
 *	here return the <buf, len> pair.
 *	translate value(|len + data + len + data...|) to a <buf, len> pair
 *	correspond to encode_values()
 */
GenericSlice *
decode_values(HistoricalKV kv, int * natts)
{
#ifdef PRINT_VALUE
	FILE *fptr;
	fptr = fopen("/tmp/decode_value.txt", "a");
	fprintf(fptr,"value: ");
#endif
	void *temp = kv.value.data + sizeof(TransactionId) * 2;
	*natts = *(int *)temp;
	temp += sizeof(int);
	// Size values_size = kv.value.len - sizeof(TransactionId) * 2;
	Assert(CheckHisSign((TupleKey)kv.key.data, HIS_TEMP_KEY) || CheckHisSign((TupleKey)kv.key.data, HIS_COMP_KEY));
	// *natts = values_size / sizeof(Datum);
	GenericSlice * values = palloc0(sizeof(GenericSlice) * (*natts));
	// Datum * values = palloc0(values_size);
	for (int i = 0; i < *natts; i++)
	{
		/* values[i].len = *(Size*)temp;
		temp += sizeof(Size);
		values[i].data = temp;
		temp += values[i].len; */
		Size len = * (Size *) temp;
		temp += sizeof(Size);
		if(len == -1) // is Null
		{
			values[i].len = len;
			values[i].data = NULL; // [ISSUE] do not know the length of data while data is NULL
		}
		else {
			values[i].len = len;
			values[i].data = temp;
			temp += len;
		}
	#ifdef PRINT_VALUE
		fprintf(fptr,"%d(len = %u) ", values[i].data == NULL ? -1 : * (int *) values[i].data, values[i].len);
	#endif
	}
#ifdef PRINT_VALUE
	fprintf(fptr,"\n");
	fclose(fptr);
#endif
	return values;
}

int decode_diff_values(HistoricalKV kv, GenericSlice ** diff_values, int ** diff_attnos)
{
	Assert(CheckHisSign((TupleKey)kv.key.data, HIS_DIFF_KEY));
	// struct { int attno; Datum value; } * diff_value_units = (char *) kv.value.data + sizeof(TransactionId) * 2;
	void * temp = kv.value.data + sizeof(TransactionId) * 2;
	int natts = *(int*)temp;
	temp += sizeof(int);
	* diff_values = palloc0(sizeof(GenericSlice) * natts);
	* diff_attnos = palloc0(sizeof(int) * natts);
	char* buffer = (char *) kv.value.data + sizeof(TransactionId) * 2;
	for (int i = 0; i < natts; ++i)
	{
#if 0
		// (* diff_attnos)[i] = diff_value_units[i].attno;
		(* diff_attnos)[i] = *(int*)buffer;
		buffer += sizeof(int);
		// (* diff_values)[i] = diff_value_units[i].value;
		(* diff_values)[i] = *(Datum*)buffer;
		buffer += sizeof(Datum);
#else
		(* diff_attnos)[i] = *(int*)temp;
		temp += sizeof(int);
		(* diff_values)[i].len = *(Size *)temp;
		temp += sizeof(Size);
		if ((* diff_values)[i].len == -1) 
		{
			(* diff_values)[i].data = NULL;
		}
		else 
		{
			(* diff_values)[i].data = palloc0((* diff_values)[i].len);
			memcpy((* diff_values)[i].data, temp, (* diff_values)[i].len);
			temp += (* diff_values)[i].len;
		}
#endif
	}
	return natts;
}

his_trxids
decode_trxids(HistoricalKV kv)
{
	TransactionId * trxid_ptr = kv.value.data;
	his_trxids trxids = { trxid_ptr[0], trxid_ptr[1] };
	return trxids;
}

his_timestamps
decode_timestamps(HistoricalKV kv)
{
	// long *ts_ptr = (long *) ((char *) kv.key.data + kv.key.len);
	// his_timestamps timestamps = { convert_ending_long(ts_ptr[-2]), convert_ending_long(ts_ptr[-1]) };
	// return timestamps;
	return decode_timestamps_internal(kv.key);
}

his_timestamps
decode_timestamps_internal(GenericSlice key)
{
	long xmax = 0, xmin = 0;
	get_TupleKeySlice_xmin(*(TupleKeySlice*)&key, xmin);
	get_TupleKeySlice_xmax(*(TupleKeySlice*)&key, xmax);
	his_timestamps timestamps = {xmin, xmax};
	return timestamps;

	// long *ts_ptr = (long *) ((char *) key.data + key.len);
	// his_timestamps timestamps = { convert_ending_long(ts_ptr[-2]), convert_ending_long(ts_ptr[-1]) };
	// return timestamps;
}

bool 
check_order(HistoricalKV cur, HistoricalKV pre)
{
	return decode_timestamps(cur).xmin_ts >= decode_timestamps(pre).xmax_ts;
}

/*
	calculate the diff of 2 value
	cur: current kv pair
	pre: previous kv pair
*/

HistoricalKV
generic_different_historical_kv(HistoricalKV cur, HistoricalKV pre)
{
	Assert(CheckHisSign((TupleKey)cur.key.data, HIS_TEMP_KEY));
	Assert(CheckHisSign((TupleKey)pre.key.data, HIS_TEMP_KEY) || CheckHisSign((TupleKey)pre.key.data, HIS_COMP_KEY));
	Assert(check_order(cur, pre));

	GenericSlice diff_key = {0};
	copy_slice(diff_key, cur.key);
	SetHisSign((TupleKey)diff_key.data, HIS_DIFF_KEY);

	GenericSlice diff_values[MaxHeapAttributeNumber] = {0};
	int diff_attnos[MaxHeapAttributeNumber] = {0};
	int diff_natts = record_diff_atts(cur, pre, diff_values, diff_attnos);
	GenericSlice diff_value = encode_diff_historical_value(decode_trxids(cur), diff_values, diff_attnos, diff_natts);

	HistoricalKV kv = { diff_key, diff_value };
	return kv;
}

int
record_diff_atts(HistoricalKV cur, HistoricalKV pre, GenericSlice diff_values[], int diff_attnos[])
{
	int cur_natts = 0, pre_natts = 0, diff_natts = 0;
	GenericSlice *cur_values = decode_values(cur, &cur_natts);
	GenericSlice *pre_values = decode_values(pre, &pre_natts);
	Assert(cur_natts == pre_natts);
	for (int i = 0; i < cur_natts; ++i)
	{
		// if (cur_values[i] != pre_values[i])
		if (cur_values[i].len != pre_values[i].len || 
			(cur_values[i].len != -1 &&
			memcmp(cur_values[i].data, pre_values[i].data, pre_values[i].len) != 0))
		{
			diff_values[diff_natts] = cur_values[i];
			diff_attnos[diff_natts] = i + 1;
			++ diff_natts;
		}
	}
	pfree(cur_values);
	pfree(pre_values);
	return diff_natts;
}

GenericSlice
encode_diff_historical_value(his_trxids trxids, GenericSlice diff_values[], int diff_attnos[], int diff_natts)
{
#ifdef PRINT_VALUE
	FILE *fptr;
	fptr = fopen("/tmp/diff.txt", "a");
	fprintf(fptr,"value: ");
#endif
	GenericSlice value;
	void *buf = palloc0(MAX_VALUE_SIZE);
	void *buf_head = buf;
	Size len = 0;
	for (int i = 0; i < diff_natts; ++i)
	{
		put_value_into_buffer_and_move_buf(buf, diff_attnos[i], int);
		len += sizeof(int);
		put_value_into_buffer_and_move_buf(buf, diff_values[i].len, Size);
		len += sizeof(Size);
		if (diff_values[i].data != NULL) 
		{
			put_mem_into_buffer_and_move_buf(buf, diff_values[i].data, diff_values[i].len);
			len += diff_values[i].len;
		}
	#ifdef PRINT_VALUE
		fprintf(fptr, "%d(%d, len = %d) ", * (int *) diff_values[i].data, diff_attnos[i], diff_values[i].len);
	#endif
	}
	alloc_slice(value, sizeof(TransactionId) * 2 + sizeof(int) + len);
	void *temp = value.data;
	put_value_into_buffer_and_move_buf(temp, trxids, his_trxids);
	put_value_into_buffer_and_move_buf(temp, diff_natts, int);
	put_mem_into_buffer_and_move_buf(temp, buf_head, len);

	Assert((char *) temp - (char *) value.data == value.len);
	pfree(buf_head);
#ifdef PRINT_VALUE
	fprintf(fptr,"\n");
	fclose(fptr);
#endif
	return value;
}

// bool*
// record_comp_atts(HistoricalKV comp, int *length)
// {
// 	*length = (comp.value.len - sizeof(TransactionId) * 2) / sizeof(Datum);
// 	bool *ischange = palloc0(*length * sizeof(bool));
// 	return ischange;
// }

void combine_diff_to_comp_kv(HistoricalKV *comp, HistoricalKV diff)
{
	Assert(CheckHisSign((TupleKey)comp->key.data, HIS_COMP_KEY));
	Assert(CheckHisSign((TupleKey)diff.key.data, HIS_DIFF_KEY));
	Assert(check_order(diff, *comp));

	GenericSlice *diff_values;
	int *diff_attnos;
	int diff_natts = decode_diff_values(diff, &diff_values, &diff_attnos);
	
	int comp_natts = 0;
	GenericSlice *comp_values = decode_values(*comp, &comp_natts);
	// Datum *cur_values = (char *) comp.value.data + sizeof(TransactionId) * 2;
	for (int i = 0; i < diff_natts; ++i)
	{
		// if(ischange[diff_attnos[i] - 1] == false) 
		{
			comp_values[diff_attnos[i] - 1] = diff_values[i];
			// ischange[diff_attnos[i] - 1] = true;
		}
	}
	void * buf = palloc0(MAX_VALUE_SIZE);
	void * buf_head = buf;
	Size len = 0;
	for (int i = 0; i < comp_natts; i++)
	{
		put_value_into_buffer_and_move_buf(buf, comp_values[i].len, Size);
		// put_mem_into_buffer_and_move_buf(buf, comp_values[i].len, sizeof(Size));
		len += sizeof(Size);
		if (comp_values[i].data != NULL) 
		{
			put_mem_into_buffer_and_move_buf(buf, comp_values[i].data, comp_values[i].len);	
			len += comp_values[i].len;
		}
	}
	GenericSlice result_value;
	alloc_slice(result_value, sizeof(TransactionId) * 2 + sizeof(int) + len);
	
	void * temp = result_value.data;
	put_mem_into_buffer_and_move_buf(temp, diff.value.data, sizeof(TransactionId) * 2);
	// put_value_into_buffer_and_move_buf(temp, xmin, TransactionId);
	// put_value_into_buffer_and_move_buf(temp, xmax, TransactionId);
	put_value_into_buffer_and_move_buf(temp, comp_natts, int);
	put_mem_into_buffer_and_move_buf(temp, buf_head, len);

	pfree(diff_values);
	pfree(diff_attnos);
	pfree(buf_head);
	pfree(comp->value.data);
	comp->value = result_value;
}

long
convert_ending_long(long source)
{
	long target;
    char *tar_bytes = (char *) &target;
	char *src_bytes = (char *) &source;
	for (int i = 0; i < sizeof(long); ++i)
		tar_bytes[i] = src_bytes[sizeof(long) - i - 1];
	return target;
}

GenericSlice
make_diff_count_key(HistoricalKV kv)
{
	GenericSlice key;
	key.len = kv.key.len - sizeof(long) * 2;
	key.data = palloc0(key.len);
	memcpy(key.data, kv.key.data, key.len);
	SetHisSign((TupleKey)key.data, HIS_DIFFCOUNT_KEY);
	return key;
}

bool
is_same_record(HistoricalKV kv_a, HistoricalKV kv_b)
{
	return kv_a.key.len == kv_b.key.len && 
		   memcmp((char *) kv_a.key.data + SIGN_TOTAL_OFFSET, (char *) kv_b.key.data + SIGN_TOTAL_OFFSET, 
		          get_key_length_without_timestamp(kv_a) - SIGN_TOTAL_OFFSET) == 0;

}

bool
is_same_record_version(HistoricalKV kv_a, HistoricalKV kv_b)
{
	return kv_a.key.len == kv_b.key.len && 
		   memcmp((char *) kv_a.key.data, (char *) kv_b.key.data, 
		          kv_a.key.len) == 0;

}