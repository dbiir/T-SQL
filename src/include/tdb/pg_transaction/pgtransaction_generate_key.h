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

#ifndef PGTXN_GEN_KEY_H
#define PGTXN_GEN_KEY_H
#include "postgres.h"
#include "tdb/kv_struct.h"

/* Do copy and set flag true if two values are different. */
#define get_TupleKeySlice_xmin(TupleKeySlice, xmin)                              \
    do                                                                           \
    {                                                                            \
        Size xmin_other_data_offset = TupleKeySlice.len -                        \
                                      sizeof(TransactionId) - sizeof(CommandId); \
        decode_uint32(&(xmin), (char *)(TupleKeySlice).data +                    \
                                   xmin_other_data_offset);                      \
    } while (0)

#define get_TupleKeySlice_cmin(TupleKeySlice, cmin)                                    \
    do                                                                                 \
    {                                                                                  \
        Size cmin_other_data_offset = TupleKeySlice.len - sizeof(CommandId);           \
        decode_uint32(&(cmin), (char *)(TupleKeySlice).data + cmin_other_data_offset); \
    } while (0)

#define set_TupleKeyData_xmin(xmin, TupleKey, offset)            \
    do                                                           \
    {                                                            \
        char *buffer = palloc0(sizeof(uint32));                  \
        Size size = encode_uint32(xmin, buffer);                 \
        memcpy(((TupleKey)->other_data + offset), buffer, size); \
        pfree(buffer);                                           \
    } while (0)

#define set_TupleKeyData_cmin(cmin, TupleKey, offset)                                    \
    do                                                                                   \
    {                                                                                    \
        char *buffer = palloc0(sizeof(uint32));                                          \
        Size size = encode_uint32(cmin, buffer);                                         \
        memcpy(((TupleKey)->other_data + offset + sizeof(TransactionId)), buffer, size); \
        pfree(buffer);                                                                   \
    } while (0)

extern TupleKeySlice decode_pkey(TupleKeySlice second_key, Relation rel, Oid pkoid, TransactionId xid, CommandId cid);
extern TupleKeySlice build_key_with_xmin_cmin(InitKeyDesc initkey);

extern char *get_TupleKeySlice_primarykey_prefix_wxc(TupleKeySlice SourceSlice, Size *length);
extern char *get_TupleKeySlice_secondarykey_prefix_wxc(TupleKeySlice SourceSlice, Size *length);

extern void get_second_key_pk_value(TupleKeySlice SourceSlice, char *TargetBuffer, Size *length);
extern void get_primary_key_pk_value(TupleKeySlice SourceSlice, char *TargetBuffer, Size *length);
#endif
