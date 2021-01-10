/*-------------------------------------------------------------------------
 *
 * pgtransaction_generate_key.h
 *
 * NOTES
 *	  Used to store some of the more common functions of the kv storage
 *    engine, such as functions used to determine visibility or to
 *    encapsulate data structures.
 *
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/pgtransaction_generate_key.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef HIS_GEN_KEY_H
#define HIS_GEN_KEY_H
#include "postgres.h"
#include "tdb/kv_struct.h"

/* Do copy and set flag true if two values are different. */
#define get_TupleKeySlice_xmin(key, xmin)                              \
    do                                                                           \
    {                                                                            \
        Size xmin_other_data_offset = (key).len -                        \
                                      sizeof(long) - sizeof(long); \
        decode_uint64(&(xmin), (char *)(key).data +                    \
                                   xmin_other_data_offset);                      \
    } while (0)

#define get_TupleKeySlice_xmax(key, xmax)                                    \
    do                                                                                 \
    {                                                                                  \
        Size xmax_other_data_offset = (key).len - sizeof(long);           \
        decode_uint64(&(xmax), (char *)(key).data + xmax_other_data_offset); \
    } while (0)

#define set_TupleKeyData_xmin(xmin, TupleKey, offset)            \
    do                                                           \
    {                                                            \
        char *buffer = palloc0(sizeof(uint64));                  \
        Size size = encode_uint64(xmin, buffer);                 \
        memcpy(((TupleKey)->other_data + offset), buffer, size); \
        pfree(buffer);                                           \
    } while (0)

#define set_TupleKeyData_xmax(xmax, TupleKey, offset)                                    \
    do                                                                                   \
    {                                                                                    \
        char *buffer = palloc0(sizeof(uint64));                                          \
        Size size = encode_uint64(xmax, buffer);                                         \
        memcpy(((TupleKey)->other_data + offset + sizeof(long)), buffer, size); \
        pfree(buffer);                                                                   \
    } while (0)

#define CheckHisSign(key /* Tuplekey */, sign /* char */) ((key)->type == (sign))
#define SetHisSign(key /* Tuplekey * */, sign /* char */) ((key)->type = (sign))

#define SIGN_TOTAL_OFFSET 4
// extern TupleKeySlice decode_pkey(TupleKeySlice second_key, Relation rel, Oid pkoid, TransactionId xid, CommandId cid);
extern TupleKeySlice build_key_with_temporal(InitKeyDesc initkey);

// extern char *get_TupleKeySlice_primarykey_prefix_wxc(TupleKeySlice SourceSlice, Size *length);
// extern char *get_TupleKeySlice_secondarykey_prefix_wxc(TupleKeySlice SourceSlice, Size *length);
#endif
