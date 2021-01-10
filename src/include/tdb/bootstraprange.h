/*----------------------------------
 *
 * bootstraprange.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/bootstraprange.h
 *----------------------------------
 */
#ifndef BOOTSTRAPRANGE
#define BOOTSTRAPRANGE
#include "tdb/range_universal.h"
#include "tdb/route.h"
#include "tdb/range.h"
#include "tdb/rangestatistics.h"


extern void BootstrapInitRange(void);
extern void BootstrapInitStatistics(void);
#endif
