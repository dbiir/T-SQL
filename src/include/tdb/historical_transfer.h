#ifndef HISTORICAL_TRANSFER_H
#define HISTORICAL_TRANSFER_H

#include "rocksdb/c.h"
#include "tdb/kv_universal.h"

#define put_value_into_buffer_and_move_buf(buf, value, type)\
do\
{\
	* (type *) (buf) = (value);\
	(buf) = (type *) (buf) + 1;\
} while (0)

#define put_mem_into_buffer_and_move_buf(buf, ptr, len)\
do\
{\
	memcpy((char *) (buf), (char *) (ptr), (len));\
	(buf) = (char *) (buf) + (len);\
} while (0)

#define put_slice_into_buffer_and_move_buf(buf, slice)\
do\
{\
	put_value_into_buffer_and_move_buf(buf, slice.len, Size);\
	put_mem_into_buffer_and_move_buf(buf, slice.data, slice.len);\
} while (0)

#define put_kv_into_buffer_and_move_buf(buf, kv)\
do\
{\
	put_slice_into_buffer_and_move_buf(buf, kv.key);\
	put_slice_into_buffer_and_move_buf(buf, kv.value);\
} while (0)

#define pick_value_from_buffer_and_move_buf(buf, value, type)\
do\
{\
	(value) = * (type *) (buf);\
	(buf) = (type *) (buf) + 1;\
} while (0)

#define pick_mem_from_buffer_and_move_buf(buf, ptr, len)\
do\
{\
	(ptr) = (void *) (buf);\
	(buf) = (char *) (buf) + (len);\
} while (0)

#define pick_slice_from_buffer_and_move_buf(buf, slice)\
do\
{\
	pick_value_from_buffer_and_move_buf(buf, slice.len, Size);\
	pick_mem_from_buffer_and_move_buf(buf, slice.data, slice.len);\
} while (0)

#define pick_kv_from_buffer_and_move_buf(buf, kv)\
do\
{\
	pick_slice_from_buffer_and_move_buf(buf, kv.key);\
	pick_slice_from_buffer_and_move_buf(buf, kv.value);\
} while (0)


#define MAX_HIS_TRANSFER_KV_NUM 1024

extern int CompletedHistoricalKVInterval;
extern unsigned long long temporal_data_full_key_storage_size;
extern unsigned long long temporal_data_full_value_storage_size;
extern unsigned long long temporal_data_delta_key_storage_size;
extern unsigned long long temporal_data_delta_value_storage_size;

typedef struct HistoricalKV
{
	GenericSlice key;
	GenericSlice value;
} HistoricalKV;

typedef struct his_timestamps
{
	long xmin_ts; // created time-stamp
	long xmax_ts; // dead time-stamp
} his_timestamps;
extern bool check_order(HistoricalKV cur, HistoricalKV pre);
extern HistoricalKV encode_temp_historical_kv(Relation rel, TupleTableSlot* slot);
extern HistoricalKV generic_different_historical_kv(HistoricalKV cur, HistoricalKV pre);
extern Oid decode_rel_id(HistoricalKV kv);
extern Size get_key_length_without_timestamp(HistoricalKV kv);

extern void combine_diff_to_comp_kv(HistoricalKV *comp, HistoricalKV diff);
extern GenericSlice make_diff_count_key(HistoricalKV kv);
extern bool is_same_record(HistoricalKV kv_a, HistoricalKV kv_b);
extern bool is_same_record_version(HistoricalKV kv_a, HistoricalKV kv_b);
extern GenericSlice copy_generic_key(GenericSlice source);
extern HistoricalKV copy_his_kv(HistoricalKV source);
extern void free_his_kv(HistoricalKV source);
extern void free_generic_key(GenericSlice source);
extern bool* record_comp_atts(HistoricalKV comp, int *length);
extern GenericSlice encode_comp_scan_historical_key(Relation rel, Datum* value, bool* isnull, int pk_colnos[], int pk_natts, long xmin_ts, long xmax_ts, bool is_end);
extern GenericSlice* decode_values(HistoricalKV kv, int* natts);
extern his_timestamps decode_timestamps_internal(GenericSlice key);

#endif