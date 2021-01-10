/*-------------------------------------------------------------------------
 *
 * timestamp_generate_key.c
 *
 * NOTES
 *	  Used to store some of the more common functions of the kv storage
 *    engine, such as functions used to determine visibility or to
 *    encapsulate data structures.
 *
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/timestamp_generate_key.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef TIMETXN_GEN_KEY_H
#define TIMETXN_GEN_KEY_H
#include "postgres.h"
#include "tdb/kv_struct.h"

extern TupleKeySlice decode_pkey_raw(TupleKeySlice second_key, Relation rel, Oid pkoid);
extern TupleKeySlice build_key_raw(InitKeyDesc initkey);
extern char* get_TupleKeySlice_primarykey_prefix_raw(TupleKeySlice SourceSlice, Size *length);
extern char* get_TupleKeySlice_secondarykey_prefix_raw(TupleKeySlice SourceSlice, Size *length);
#endif
