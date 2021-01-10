/*----------------------------------
 *
 * range_universal.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/rangestatistics.h
 *----------------------------------
 */
#ifndef RANGESTATISTICS
#define RANGESTATISTICS
#include "tdb/range_universal.h"

extern Size StatisticsQueueShmemSize(void);
extern void StatisticsQueueShmemInit(void);

extern bool UpdateStatisticsInsert(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value);
extern bool UpdateStatisticsRead(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value);
extern bool UpdateStatisticsDelete(RangeID targetRangeID, TupleKeySlice key, TupleValueSlice value);

extern void Init_Seg_Stat(void);
extern void InitRangeStatistics(RangeDesc Range, int stat_id);
extern bool checkRangeStatistics(RangeDesc Range);
extern void add_RangeStatistics(RangeDesc rangestat);
extern void m_s_add_RangeStatistics();
extern void m_s_add_segmentStatistics();
extern RangeSatistics* get_rangesatistics_from_list(RangeID targetRangeID);
extern int FindRangeStatisticsByRangeID(RangeID rangeid);

extern bool removeRangeStatistics(RangeDesc Range);
extern bool removeMSRangeStatistics(RangeDesc Range);
#endif
