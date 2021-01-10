/*-------------------------------------------------------------------------
 *
 * range_processor.h
 *	  the request and the relational function about statistics update connect to storage
 *
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/range_processor.h
 *
 *-------------------------------------------------------------------------
 */
#include "tdb/kvengine.h"
#include "tdb/tdbkvam.h"
#include "utils/hsearch.h"
#include "tdb/rangestatistics.h"
#include "tdb/storage_processor.h"
#include "tdb/session_processor.h"

extern List* kvengine_process_scan_all_range(void);
extern void kvengine_process_scan_one_range_all_key(TupleKeySlice startkey, TupleKeySlice endkey,
						RangeID rangeid, RangeSatistics* Rangestatis);
extern ResponseHeader* kvengine_process_rangescan_req(RequestHeader* req);
extern ResponseHeader* kvengine_process_init_statistics_req(RequestHeader* req);
extern bool Rangeengineprocessor_create_paxos(RangeID rangeid, SegmentID* seglist, int segcount);
