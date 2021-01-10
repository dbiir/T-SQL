/*-------------------------------------------------------------------------
 *
 * encode.c
 *
 * NOTES
 *	  This file contains encodings for types such as int, float, etc.
 *    to ensure that rocksdb can stored these pk value in order.
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
#include "utils/numeric.h"
#include "catalog/indexing.h"
#include "access/tuptoaster.h"
#include "tdb/encode_type.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/builtins.h"
/*
 *  The entry function that encodes pk_values
 */
char*
datum_encode_into_buffer(Datum source, Oid type_oid, Size* dest_length, bool isnull, bool isend)
{
    switch (type_oid)
    {
        /*
		 * ======= NUMERIC TYPES ========
		 */
        case INT2OID:			/* -32 thousand to 32 thousand, 2-byte storage */
            {
                char *buffer = palloc0(size_of_encode_int16);
                int16 i = DatumGetInt16(source);	/* cast to 8 byte before
                                                        * hashing */
                *dest_length = encode_int16(i, buffer, isnull, isend);
                return buffer;
            }
        /* int32 (pg_node_tree) */
        case INT4OID:
            {
                char *buffer = palloc0(size_of_encode_int32);
                int32 i = DatumGetInt32(source);
                *dest_length = encode_int32(i, buffer, isnull, isend);
                return buffer;
            }
        /* int64 */
        case INT8OID:
            {
                char *buffer = palloc0(size_of_encode_int64);
                int64 i = DatumGetInt64(source);
                *dest_length = encode_int64(i, buffer, isnull, isend);
                return buffer;
            }
        /* float32 */
        case FLOAT4OID:
            {
                char *buffer = palloc0(size_of_encode_float32);
                float i = DatumGetFloat4(source);
                if (i == (float)0)
                    i = 0.0;
                *dest_length = encode_float32(i, buffer, isnull, isend);
                return buffer;
            }
        /* float64 */
        case FLOAT8OID:
            {
                char *buffer = palloc0(size_of_encode_float64);
                double i = DatumGetFloat8(source);
                if (i == (double)0)
                    i = 0.0;
                *dest_length = encode_float64(i, buffer, isnull, isend);
                return buffer;
            }
		case BPCHAROID:
		case TEXTOID:
		case VARCHAROID:
		case BYTEAOID:
            {
				Oid			typoutput;
				bool		typIsVarlena;
				char	   *extval;
				StringInfo info = makeStringInfo();
				initStringInfo(info);
				if (isnull)
				{
					Datum *buffer = palloc0(sizeof(Datum));
					*buffer = isend ?  -1 : 0;
					*dest_length = sizeof(Datum);
					return (char*)buffer;
				}
				getTypeOutputInfo(type_oid, &typoutput, &typIsVarlena);
				extval = OidOutputFunctionCall(typoutput, source);
				appendStringInfo(info, "%s", extval);
				*dest_length = info->len;
				char *buffer = palloc0(info->len);
				memcpy(buffer, info->data, info->len);
				resetStringInfo(info);
				return buffer;
            }

		case NUMERICOID:
            {
                char *buffer = palloc0(size_of_encode_float64);
                bool null = isnull ? true : false;
                double i = 0.0;
                if (!null)
                {
                    Numeric num = DatumGetNumeric(source);

                    if (numeric_is_nan(num))
                        null = true;
                    else
                        i = numeric_to_double_no_overflow(num);
                        
                    if (i == (double)0)
                        i = 0.0;
                }
                *dest_length = encode_float64(i, buffer, null, isend);
                return buffer;
            }
#if 0
		case CHAROID:
            {
                char *buffer = palloc0(1);
                char char_buf = DatumGetChar(source);
                buffer = &char_buf;
                *dest_length = 1;
                return buffer;
            }
		

		case NAMEOID:
			namebuf = DatumGetName(source);
			len = NAMEDATALEN;
			buf = NameStr(*namebuf);
			if (len > 1)
				len = ignoreblanks((char *) buf, len);
			break;

		case OIDOID:
		case REGPROCOID:
		case REGPROCEDUREOID:
		case REGOPEROID:
		case REGOPERATOROID:
		case REGCLASSOID:
		case REGTYPEOID:
		case ANYENUMOID:
			intbuf = (int64) DatumGetUInt32(source);
			buf = &intbuf;
			len = sizeof(intbuf);
			break;
		case ANYRANGEOID:
			range = DatumGetRangeType(source);
			len = VARSIZE(range) - sizeof(RangeType);
			buf = (void*)(range + 1);
			break;

		case TIDOID:
			buf = DatumGetPointer(source);
			len = SizeOfIptrData;
			break;

		case TIMESTAMPOID:
			tsbuf = DatumGetTimestamp(source);
			buf = &tsbuf;
			len = sizeof(tsbuf);
			break;

		case TIMESTAMPTZOID:
			tstzbuf = DatumGetTimestampTz(source);
			buf = &tstzbuf;
			len = sizeof(tstzbuf);
			break;

		case DATEOID:
			datebuf = DatumGetDateADT(source);
			buf = &datebuf;
			len = sizeof(datebuf);
			break;

		case TIMEOID:
			timebuf = DatumGetTimeADT(source);
			buf = &timebuf;
			len = sizeof(timebuf);
			break;

		case TIMETZOID:

			timetzptr = DatumGetTimeTzADTP(source);
			buf = (unsigned char *) timetzptr;

			len = sizeof(timetzptr->time) + sizeof(timetzptr->zone);
			break;

		case INTERVALOID:
			intervalptr = DatumGetIntervalP(source);
			buf = (unsigned char *) intervalptr;

			len = sizeof(intervalptr->time) + sizeof(intervalptr->month);
			break;

		case ABSTIMEOID:
			abstime_buf = DatumGetAbsoluteTime(source);

			if (abstime_buf == INVALID_ABSTIME)
			{
				invalidbuf = INVALID_VAL;
				len = sizeof(invalidbuf);
				buf = &invalidbuf;
			}
			else
			{
				len = sizeof(abstime_buf);
				buf = &abstime_buf;
			}

			break;

		case RELTIMEOID:
			reltime_buf = DatumGetRelativeTime(source);

			if (reltime_buf == INVALID_RELTIME)
			{
				invalidbuf = INVALID_VAL;
				len = sizeof(invalidbuf);
				buf = &invalidbuf;
			}
			else
			{
				len = sizeof(reltime_buf);
				buf = &reltime_buf;
			}

			break;

		case TINTERVALOID:
			tinterval = DatumGetTimeInterval(source);

			if (tinterval->status == 0 ||
				tinterval->data[0] == INVALID_ABSTIME ||
				tinterval->data[1] == INVALID_ABSTIME)
			{
				invalidbuf = INVALID_VAL;
				len = sizeof(invalidbuf);
				buf = &invalidbuf;
			}
			else
			{
				tinterval_len = tinterval->data[1] - tinterval->data[0];
				len = sizeof(tinterval_len);
				buf = &tinterval_len;
			}

			break;

		case INETOID:
		case CIDROID:

			inetptr = DatumGetInetP(source);
			len = inet_getkey(inetptr, inet_hkey, sizeof(inet_hkey));
			buf = inet_hkey;
			break;

		case MACADDROID:

			macptr = DatumGetMacaddrP(source);
			len = sizeof(macaddr);
			buf = (unsigned char *) macptr;
			break;

		case BITOID:
		case VARBITOID:


	    	vbitptr = DatumGetVarBitP(source);
			len = VARBITBYTES(vbitptr);
			buf = (char *) VARBITS(vbitptr);
			break;

		case BOOLOID:
			bool_buf = DatumGetBool(source);
			buf = &bool_buf;
			len = sizeof(bool_buf);
			break;


		case ANYARRAYOID:
			arrbuf = DatumGetArrayTypeP(source);
			len = VARSIZE(arrbuf) - VARHDRSZ;
			buf = VARDATA(arrbuf);
			break;

		case OIDVECTOROID:
			oidvec_buf = (oidvector *) DatumGetPointer(source);
			len = oidvec_buf->dim1 * sizeof(Oid);
			buf = oidvec_buf->values;
			break;

		case CASHOID:
			cash_buf = DatumGetCash(source);
			len = sizeof(Cash);
			buf = &cash_buf;
			break;


		case UUIDOID:
			uuid_buf = DatumGetUUIDP(source);
			len = UUID_LEN;
			buf = (char *) uuid_buf;
			break;

		case COMPLEXOID:
			complex_ptr = DatumGetComplexP(source);
			complex_real = re(complex_ptr);
			complex_imag = im(complex_ptr);


			if (complex_real == (float8) 0)
			{
				complex_real = 0.0;
			}
			if (complex_imag == (float8) 0)
			{
				complex_imag = 0.0;
			}

			INIT_COMPLEX(&complex_buf, complex_real, complex_imag);
			len = sizeof(Complex);
			buf = (unsigned char *) &complex_buf;
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_YET),
					 errmsg("Type %u is not hashable.", type)));*/
        default:
        {
            char *buffer = palloc0(sizeof(Datum));
            memcpy(buffer, (void*)&source, sizeof(Datum));
            *dest_length = sizeof(Datum);
            return buffer;
        }
    
#endif
	}
	Datum *buffer = palloc0(sizeof(Datum));
	*dest_length = sizeof(Datum);
	if (isnull)
	{
		if (isend)
			*buffer = 0XFFFFFFFF;
		else
			*buffer = 0;
	}
	else
		*buffer = source;
	
    return (char*)buffer;
}

void
datum_encode_into_static_buffer(char* start, Datum source, Oid type_oid, Size* dest_length, bool isnull, bool isend)
{
    switch (type_oid)
    {
        case INT2OID:			/* -32 thousand to 32 thousand, 2-byte storage */
            {
                int16 i = DatumGetInt16(source);
                *dest_length = encode_int16(i, start, isnull, isend);
                return;
            }
        case INT4OID:
            {
                int32 i = DatumGetInt32(source);
                *dest_length = encode_int32(i, start, isnull, isend);
                return;
            }
        case INT8OID:
            {
                int64 i = DatumGetInt64(source);
                *dest_length = encode_int64(i, start, isnull, isend);
                return;
            }
        case FLOAT4OID:
            {
                float i = DatumGetFloat4(source);
                if (i == (float)0)
                    i = 0.0;
                *dest_length = encode_float32(i, start, isnull, isend);
                return;
            }
        case FLOAT8OID:
            {
                double i = DatumGetFloat8(source);
                if (i == (double)0)
                    i = 0.0;
                *dest_length = encode_float64(i, start, isnull, isend);
                return;
            }
		case BPCHAROID:
		case TEXTOID:
		case VARCHAROID:
		case BYTEAOID:
            {
				Oid			typoutput;
				bool		typIsVarlena;
				char	   *extval;
				StringInfo info = makeStringInfo();
				initStringInfo(info);
				if (isnull)
				{
					Datum buffer = isend ?  -1 : 0;
					*dest_length = sizeof(Datum);
					memcpy(start, &buffer, info->len);
					return;
				}
				getTypeOutputInfo(type_oid, &typoutput, &typIsVarlena);
				extval = OidOutputFunctionCall(typoutput, source);
				appendStringInfo(info, "%s", extval);
				*dest_length = info->len;
				memcpy(start, info->data, info->len);
				resetStringInfo(info);
				return;
            }
		case NUMERICOID:
            {
                bool null = isnull ? true : false;
                double i = 0.0;
                if (!null)
                {
                    Numeric num = DatumGetNumeric(source);

                    if (numeric_is_nan(num))
                        null = true;
                    else
                        i = numeric_to_double_no_overflow(num);
                        
                    if (i == (double)0)
                        i = 0.0;
                }
                *dest_length = encode_float64(i, start, null, isend);
                return;
            }
	}
	Datum buffer;
	*dest_length = sizeof(Datum);
	if (isnull)
	{
		if (isend)
			buffer = 0XFFFFFFFF;
		else
			buffer = 0;
	}
	else
		buffer = source;

	memcpy(start, &buffer, *dest_length);

    return;
}

void
append(void* dest, void* source, Size length)
{
    char *temp = (char*)dest;
    for(int j = length - 1; j >= 0; j--)
    {
        memcpy((void*)temp, (void*)((char*)source + j), byte_length);
        temp++;
    }
}

Size
encode_uint64(uint64 i, void* buffer)
{
    append(buffer, (void*)&i, sizeof(uint64));
    return sizeof(uint64);
}

Size
encode_uint32(uint32 i, void* buffer)
{
    append(buffer, (void*)&i, sizeof(uint32));
    return sizeof(uint32);
}

Size
encode_int64(int64 i, void* buffer, bool isnull, bool isend)
{
    encode_type type = encodedNull;
    if (isnull && isend)
    {
        type = typeNaNDesc;
    }
    else if (isnull && !isend)
    {
        type = typeNaN;
    }
    else if (i > 0)
    {
        type = typePos;
    }
    else if (i ==0)
    {
        type = typeZero;
    }
    else
    {
        type = typeNeg;
    }
    memcpy(buffer, (void*)&type, sizeof(type));
    append(((char*)buffer + sizeof(type)), (void*)&i, sizeof(int64));
    return size_of_encode_int64;
}

Size
encode_int32(int32 i, void* buffer, bool isnull, bool isend)
{
    encode_type type = encodedNull;
    if (isnull && isend)
    {
        type = typeNaNDesc;
    }
    else if (isnull && !isend)
    {
        type = typeNaN;
    }
    else if (i > 0)
    {
        type = typePos;
    }
    else if (i ==0)
    {
        type = typeZero;
    }
    else
    {
        type = typeNeg;
    }
    memcpy(buffer, (void*)&type, sizeof(type));
    append(((char*)buffer + sizeof(type)), (void*)&i, sizeof(int32));
    return size_of_encode_int32;
}

Size
encode_int16(int16 i, void* buffer, bool isnull, bool isend)
{
    encode_type type = encodedNull;
    if (isnull && isend)
    {
        type = typeNaNDesc;
    }
    else if (isnull && !isend)
    {
        type = typeNaN;
    }
    else if (i > 0)
    {
        type = typePos;
    }
    else if (i ==0)
    {
        type = typeZero;
    }
    else
    {
        type = typeNeg;
    }
    memcpy(buffer, (void*)&type, sizeof(type));
    append(((char*)buffer + sizeof(type)), (void*)&i, sizeof(int16));
    return size_of_encode_int16;
}

Size
encode_float64(double f, void* buffer, bool isnull, bool isend)
{
    encode_type type = encodedNull;
    if (isnull && isend)
    {
        type = typeNaNDesc;
    }
    else if (isnull && !isend)
    {
        type = typeNaN;
    }
    else if (f > 0)
    {
        type = typePos;
    }
    else if (f == 0)
    {
        type = typeZero;
    }
    else if (f < 0)
    {
        uint64 i = ~*(uint64*)&f;
        f = *(double*)&i;
        type = typeNeg;
    }
    else
    {
        type = encodedNull;
    }
    memcpy(buffer, (void*)&type, sizeof(type));
    append(((char*)buffer + sizeof(type)), (void*)&f, sizeof(double));
    return size_of_encode_float64;
}

Size
encode_float32(float f, void* buffer, bool isnull, bool isend)
{
    buffer = palloc0(sizeof(float) + sizeof(encode_type));
    encode_type type = encodedNull;
    if (isnull && isend)
    {
        type = typeNaNDesc;
    }
    else if (isnull && !isend)
    {
        type = typeNaN;
    }
    else if (f > 0)
    {
        type = typePos;
    }
    else if (f ==0)
    {
        type = typeZero;
    }
    else if (f < 0)
    {
        uint32 i = ~*(uint32*)&f;
        f = *(float*)&i;
        type = typeNeg;
    }
    else
    {
        type = typeNaN;
    }
    memcpy(buffer, (void*)&type, sizeof(type));
    append(((char*)buffer + sizeof(type)), (void*)&f, sizeof(float));
    return size_of_encode_float32;
}
