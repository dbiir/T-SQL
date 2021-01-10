/*-------------------------------------------------------------------------
 *
 * encode_type.h
 *
 * NOTES
 *	  This file contains the type after the dump.
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *	   src/include/tdb/encode_type.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ENCODE_TYPE_H
#define ENCODE_TYPE_H
#include "c.h"


#define byte_length 1
#define size_of_encode_int64 (sizeof(int64) + sizeof(encode_type))
#define size_of_encode_int32 (sizeof(int32) + sizeof(encode_type))
#define size_of_encode_int16 (sizeof(int16) + sizeof(encode_type))
#define size_of_encode_float32 (sizeof(float) + sizeof(encode_type))
#define size_of_encode_float64 (sizeof(double) + sizeof(encode_type))

typedef enum
{
    encodedNull = 0x00,
	encodedNotNull = 0x01,
    typeNaN = 0x02,
    typeNeg = 0x03,
    typeZero = 0x04,
    typePos = 0x05,
    typeNaNDesc = 0x06,
}encode_type;

typedef enum
{
    stringEnd = 0x0001,
    stringInterval = 0x00ff,
}string_type;

extern void append(void* dest, void* source, Size length);
extern Size encode_uint64(uint64 i, void* buffer);
extern Size encode_uint32(uint32 i, void* buffer);
extern Size encode_int64(int64 i, void* buffer, bool isnull, bool isend);
extern Size encode_int32(int32 i, void* buffer, bool isnull, bool isend);
extern Size encode_int16(int16 i, void* buffer, bool isnull, bool isend);
extern Size encode_float64(double f, void* buffer, bool isnull, bool isend);
extern Size encode_float32(float f, void* buffer, bool isnull, bool isend);

extern void decode_uint64(uint64* i, void* buffer);
extern void decode_uint32(uint32* i, void* buffer);
extern void decode_int64(int64* i, void* buffer);
extern void decode_int32(int32* i, void* buffer);
extern void decode_float64(double* f, void* buffer);
extern void decode_float32(float* f, void* buffer);
extern char* datum_encode_into_buffer(Datum source, Oid type_oid, Size* dest_length, bool isnull, bool isend);

extern void datum_encode_into_static_buffer(char* start, Datum source, Oid type_oid, Size* dest_length, bool isnull, bool isend);

#endif