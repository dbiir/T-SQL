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

#ifndef STORAGE_PG_H
#define STORAGE_PG_H

#include "tdb/kvengine.h"
#include "tdb/tdbkvam.h"
#include "tdb/session_processor.h"
#include "utils/hsearch.h"
#include "tdb/storage_param.h"

extern ResponseHeader* kvengine_pgprocess_get_req(RequestHeader* req);
extern ResponseHeader* kvengine_pgprocess_put_req(RequestHeader* req);
extern ResponseHeader* kvengine_pgprocess_delete_normal_req(RequestHeader* req);
extern ResponseHeader* kvengine_pgprocess_update_req(RequestHeader* req);
extern ResponseHeader* kvengine_pgprocess_delete_direct_req(RequestHeader* req);
extern ResponseHeader* kvengine_pgprocess_scan_req(RequestHeader* req);
extern ResponseHeader* kvengine_pgprocess_multi_get_req(RequestHeader* req);
/*extern ResponseHeader* kvengine_pgprocess_delete_one_tuple_all_index(RequestHeader* req);*/
extern ResponseHeader* kvengine_pgprocess_commit(RequestHeader* req);
extern ResponseHeader* kvengine_pgprocess_abort(RequestHeader* req);

#endif
