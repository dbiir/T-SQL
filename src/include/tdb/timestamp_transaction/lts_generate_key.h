/*-------------------------------------------------------------------------
 *
 * raw_generate_key.c
 *
 * NOTES
 *	  It is used to generate key with lts.
 *
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/lts_generate_key.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef LTSTXN_GEN_KEY_H
#define LTSTXN_GEN_KEY_H
#include "postgres.h"
#include "tdb/kv_struct.h"

extern TupleKeySlice decode_pkey_lts(TupleKeySlice second_key, Relation rel, Oid pkoid);
extern TupleKeySlice build_key_lts(InitKeyDesc initkey);
extern uint32 get_lts_suffix(TupleKeySlice SourceSlice);
extern char* get_TupleKeySlice_primarykey_prefix_lts(TupleKeySlice SourceSlice, Size *length);
extern char* get_TupleKeySlice_secondarykey_prefix_lts(TupleKeySlice SourceSlice, Size *length);
extern bool IsLTSKey(TupleKeySlice tempkey);
extern bool IsPartLTSKey(TupleKeySlice tempkey);
#endif
