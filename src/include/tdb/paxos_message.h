/*-------------------------------------------------------------------------
 *
 * paxos_message.h
 *
 * NOTES
 *      Callback function after paxos operation.
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/paxos_message.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAXOS_MESSGAE
#define PAXOS_MESSGAE

#include "tdb/kv_struct.h"

typedef enum PaxosMsgType
{
	PAXOS_RUN_PUT,
	PAXOS_RUN_DELETE,
}PaxosMsgType;

typedef enum PaxosRunResult
{
	PAXOS_RUN_SUCCESS = 0,
	PAXOS_RUN_FAILED,
}PaxosRunResult;

extern int PaxosParse(void *Msg, int MsgLen);

extern void* TransferMsgToPaxos(PaxosMsgType type, TupleKeySlice key, TupleValueSlice value, Oid rangeid, int *msglength);

#endif
