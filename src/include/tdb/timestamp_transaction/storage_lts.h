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

#ifndef STORAGE_LTS_H
#define STORAGE_LTS_H

#include "tdb/kvengine.h"
#include "tdb/tdbkvam.h"
#include "tdb/session_processor.h"
#include "utils/hsearch.h"
#include "tdb/storage_param.h"
extern KVScanDesc init_kv_ltsprocess_scan(KVEngineTransactionInterface* txn, bool isforward);

extern ResponseHeader* kvengine_ltsprocess_get_req(RequestHeader* req);
extern ResponseHeader* kvengine_ltsprocess_put_req(RequestHeader* req);
extern ResponseHeader* kvengine_ltsprocess_delete_normal_req(RequestHeader* req);
extern ResponseHeader* kvengine_ltsprocess_update_req(RequestHeader* req);
extern ResponseHeader* kvengine_ltsprocess_delete_direct_req(RequestHeader* req);

extern ResponseHeader* kvengine_ltsprocess_scan_req(RequestHeader* req);
extern ResponseHeader* kvengine_ltsprocess_multi_get_req(RequestHeader* req);

extern ResponseHeader* kvengine_ltsprocess_commit(RequestHeader* req);
extern ResponseHeader* kvengine_ltsprocess_abort(RequestHeader* req);
extern ResponseHeader* kvengine_ltsprocess_put_rts_req(RequestHeader* req);
#endif
