/*-------------------------------------------------------------------------
 *
 * storage_processor.h
 *	  the request and the relational function about storage connect to session
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/storage_processor.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STORAGE_HIS_H
#define STORAGE_HIS_H

#include "tdb/kvengine.h"
#include "tdb/tdbkvam.h"
#include "tdb/session_processor.h"
#include "utils/hsearch.h"
#include "tdb/storage_param.h"
#include "tdb/historical_transfer.h"
/* [hzy9819] some temporal function 
 * all of these function will be implemented in storage_processor.c
 */
static int scan_get_next_diff( KVScanDesc *desc, KVEngineIteratorInterface **engine_it_diff, 
					HistoricalKV *comp_kv, GenericSlice diff_end_key,
					key_his_cmp_func is_end, int interval);
extern bool cmp_history_range(GenericSlice rocksdb_key, GenericSlice target_key, bool isforward);
extern bool cmp_time(GenericSlice rocksdb_key, GenericSlice target_key);
extern KVScanHistoryDesc init_history_kv_scan(bool isforward, KVEngineTransactionInterface* txn);
extern void get_key_interval_from_hisscan_req(ScanWithTimeRequest* scan_req, GenericSlice* start_key, GenericSlice* end_key);

extern void put_as_comp_kv(HistoricalKV kv, KVEngineTransactionInterface* txn);
extern void put_as_diff_kv(HistoricalKV cur_kv, HistoricalKV pre_kv, KVEngineTransactionInterface* txn);
extern bool get_last_comp_kv(HistoricalKV temp_kv, HistoricalKV* last_comp_kv, KVEngineTransactionInterface* txn);
extern bool refresh_load_next_historical_kv(KVEngineTransactionInterface * txn, KVEngineIteratorInterface* iter, HistoricalKV* cur_kv, HistoricalKV* pre_kv);
extern int get_diff_kv_count(GenericSlice diff_count_key, KVEngineTransactionInterface* txn);
extern void put_diff_kv_count(GenericSlice diff_count_key, int diff_count, KVEngineTransactionInterface* txn);

extern ResponseHeader* kvengine_process_multi_put_req(RequestHeader* req);
extern ResponseHeader* kvengine_process_scan_history(RequestHeader* req);

#endif
