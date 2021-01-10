/*----------------------------------
 *
 * range_universal.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/range_universal.h
 *----------------------------------
 */
#ifndef TDB_ROCKRANGEUNIVERSAL_H
#define TDB_ROCKRANGEUNIVERSAL_H

#include "c.h"
#include "tdb/kv_universal.h"
#include "tdb/range_struct.h"

extern RangeID getMaxRangeID(void);
extern char* TransferRangeDescToBuffer(RangeDesc range, Size *length);
extern RangeDesc TransferBufferToRangeDesc(char* rangebuffer);

extern int getRootRangeLevel(void);
extern void putRootRangeLevel(int RootRangeLevel);
extern RangeDesc initVoidRangeDesc(void);
#endif
