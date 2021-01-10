/*-------------------------------------------------------------------------
 *
 * session_processor.h
 *	  the request and the relational function about session connect to storage
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/session_processor.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SESSION_PROCESSOR_H
#define SESSION_PROCESSOR_H

#include "c.h"
#include "postgres.h"
#include "storage/shm_mq.h"
#include "storage/dsm_impl.h"
#include "storage/dsm.h"
#include "tdb/tdbkvam.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "storage/shmem.h"
#include "storage/spin.h"

#define MAX_ATTACH_QUEUE_LEN 128
#define PG_KV_ENGINE_MQ_MAGIC 0x114514 /* meaningless */
#define SIG_KV_REQ (SIGRTMIN + 1)
#define SIG_DETACH (SIGRTMIN + 2)
#define SIG_ATTACH SIGUSR1
#define SIG_ATTACH_REQ (SIGRTMIN + 5)

enum KVEngineMQKey
{
	REQUEST_MQ_KEY = 0,
	RESPONSE_MQ_KEY
};

typedef struct AttachRequestData
{
	pid_t session_pid;
	dsm_handle handle;
} AttachRequestData;

typedef AttachRequestData* AttachRequest;

typedef struct KVEngineAttachQueueControl
{
	pid_t storage_pid;
	AttachRequestData attach_queue[MAX_ATTACH_QUEUE_LEN];
	Size queue_len;
	slock_t	amq_mutex;
} KVEngineAttachQueueControl;

extern KVEngineAttachQueueControl *attachQueueControl;

typedef struct KVEngineMQHandleData
{
	Node 			node_head;
	shm_mq_handle	*req_handle;
	shm_mq_handle	*res_handle;
	dsm_segment		*seg;
	int 			pid;
    bool            isruning;
	enum KVEngineMQHandleStatus
	{
		KV_MQ_NOT_ATTACHED, KV_MQ_SUCCESS, KV_MQ_FAILED
	} status;
} KVEngineMQHandleData;

typedef KVEngineMQHandleData* KVEngineMQHandle;
extern Size KVEnginePidTableShmemSize(void);
extern void KVEnginePidTableShmemInit(void);
extern ResponseHeader* SessionProcessKV(RequestHeader* req);

extern void SessionInitMQ(void);
extern void SessionDestroyMQ(void);

//extern void register_kv_signals(void);
#endif
