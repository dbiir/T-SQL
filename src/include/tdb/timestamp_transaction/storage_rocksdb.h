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

#ifndef STORAGE_ROCKSDB_H
#define STORAGE_ROCKSDB_H

#include "tdb/kvengine.h"
#include "tdb/tdbkvam.h"
#include "tdb/session_processor.h"
#include "utils/hsearch.h"
#include "tdb/storage_param.h"
extern KVScanDesc init_kv_optprocess_scan(KVEngineTransactionInterface* txn, bool isforward);

extern ResponseHeader* kvengine_optprocess_get_req(RequestHeader* req);
extern ResponseHeader* kvengine_optprocess_put_req(RequestHeader* req);
extern ResponseHeader* kvengine_optprocess_delete_normal_req(RequestHeader* req);
extern ResponseHeader* kvengine_optprocess_update_req(RequestHeader* req);
extern ResponseHeader* kvengine_optprocess_delete_direct_req(RequestHeader* req);

extern ResponseHeader* kvengine_optprocess_scan_req(RequestHeader* req);
extern ResponseHeader* kvengine_optprocess_multi_get_req(RequestHeader* req);

extern ResponseHeader* kvengine_optprocess_commit(RequestHeader* req);
extern ResponseHeader* kvengine_optprocess_abort(RequestHeader* req);
extern ResponseHeader* kvengine_optprocess_clear(RequestHeader* req);
extern ResponseHeader* kvengine_optprocess_prepare(RequestHeader* req);
#endif
