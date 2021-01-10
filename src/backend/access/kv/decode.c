/*-------------------------------------------------------------------------
 *
 * decode.c
 *
 * NOTES
 *	  This file contains descodings for types such as int, float, etc.
 *    to ensure that rocksdb can stored these pk value in order.
 *
 *    At the moment, I have not found out where decoding is needed,
 *    so these functions are not used at present, and there may have some bugs.
 *
 *  Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *	  src/backend/access/kv/encode.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam_xlog.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "catalog/index.h"
#include "catalog/pg_namespace.h"
#include "commands/vacuum.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/guc.h"

#include "catalog/indexing.h"

#include "tdb/encode_type.h"

void
decode_uint64(uint64* i, void* buffer)
{
    append((void*)i, (void*)((char*)buffer), sizeof(uint64));
}

void
decode_uint32(uint32* i, void* buffer)
{
    append((void*)i, (void*)((char*)buffer), sizeof(uint32));
}

void
decode_int64(int64* i, void* buffer)
{
    append((void*)i, (void*)((char*)buffer + sizeof(encode_type)), sizeof(int64));
}

void
decode_int32(int32* i, void* buffer)
{
    append((void*)i, (void*)((char*)buffer + sizeof(encode_type)), sizeof(int32));
}

void
decode_float32(float* f, void* buffer)
{
    encode_type type = encodedNull;
    memcpy((void*)&type, buffer, sizeof(uint16));
    append((void*)f, (void*)((char*)buffer + sizeof(encode_type)), sizeof(float));
    if (type == typeNeg)
    {
        uint32 i = ~*(uint32*)&f;
        *f = *(float*)&i;
    }
}

void
decode_float64(double* f, void* buffer)
{
    encode_type type = encodedNull;
    memcpy((void*)&type, buffer, sizeof(uint16));
    append((void*)f, (void*)((char*)buffer + sizeof(encode_type)), sizeof(double));
    if (type == typeNeg)
    {
        uint64 i = ~*(uint64*)&f;
        *f = *(double*)&i;
    }
}

