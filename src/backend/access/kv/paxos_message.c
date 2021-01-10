/*-------------------------------------------------------------------------
 *
 * paxos_message.c
 *		A file temporarily used to encapsulate the operation information to be 
 *  synchronized by paxos. The operation information is mainly put operation 
 *  and delete operation.
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *		src/backend/access/kv/paxos_message.c
 *
 *-------------------------------------------------------------------------
 */


#include <stdlib.h>
#include <unistd.h>

#include "postgres.h"
#include "utils/memutils.h"
#include "tdb/storage_processor.h"
#include "tdb/paxos_message.h"
#include "tdb/rangestatistics.h"
#include "paxos/c_parse_include.h"

typedef struct PaxosMsgPut
{
	PaxosMsgType type;
	Oid rangeid;
	char kv[1];
}PaxosMsgPut;

typedef struct PaxosMsgDelete
{
	PaxosMsgType type;
	Oid rangeid;
	char k[1];
}PaxosMsgDelete;

static char* 
TransferPutIntoPaxosMsg(TupleKeySlice key, TupleValueSlice value, Oid rangeid, int *msglength)
{
	Size length = sizeof(PaxosMsgPut) + sizeof(Oid) + key.len + value.len + sizeof(Size) * 2;
	PaxosMsgPut *msg = (PaxosMsgPut*)palloc0(length);
	msg->type = PAXOS_RUN_PUT;
	msg->rangeid = rangeid;

	char* temp = (char*)msg->kv;
	Size* keylen = (Size*)temp;
	*keylen = key.len;
	temp += sizeof(Size);
	memcpy(temp, (char*)key.data, key.len);
	temp += key.len;

	keylen = (Size*)temp;
	*keylen = value.len;
	temp += sizeof(Size);
	memcpy(temp, (char*)value.data, value.len);
	*msglength = length;
	return (char*)msg;
}

static char* 
TransferDeleteIntoPaxosMsg(TupleKeySlice key, Oid rangeid, int *msglength)
{
	Size length = sizeof(PaxosMsgDelete) + sizeof(Oid) + key.len + sizeof(Size);
	PaxosMsgDelete *msg = (PaxosMsgDelete*)palloc0(length);
	msg->type = PAXOS_RUN_DELETE;
	msg->rangeid = rangeid;
	char* temp = (char*)msg->k;
	Size* keylen = (Size*)temp;
	*keylen = key.len;
	temp += sizeof(Size);
	memcpy(temp, (char*)key.data, key.len);
	*msglength = length;
	return (char*)msg;
}

static void 
TransferPaxosMsgToPut(char* buffer, Oid *rangeid, TupleKeySlice *key, TupleValueSlice *value)
{
	PaxosMsgPut *msg = (PaxosMsgPut*)buffer;
	*rangeid = msg->rangeid;
	Assert(msg->type == PAXOS_RUN_PUT);
	char* temp = (char*)msg->kv;
	key->len = *(Size*)temp;
	key->data = (TupleKey)palloc0(key->len);
	temp += sizeof(Size);
	memcpy(key->data, temp, key->len);
	temp += key->len;
	value->len = *(Size*)temp;
	temp += sizeof(Size);
	value->data = (TupleValue)palloc0(value->len);
	memcpy(value->data, temp, value->len);
}

static void 
TransferPaxosMsgToDelete(char* buffer, Oid *rangeid, TupleKeySlice *key)
{
	PaxosMsgDelete *msg = (PaxosMsgDelete*)buffer;
	*rangeid = msg->rangeid;
	Assert(msg->type == PAXOS_RUN_DELETE);
	char* temp = (char*)(&msg->k);
	key->len = *(Size*)temp;
	key->data = (TupleKey)palloc0(key->len);
	temp += sizeof(Size);
	memcpy(key->data, temp, key->len);
}

int
PaxosParse(void *Msg, int MsgLen)
{
	if (CurrentThreadMemoryContext == NULL)
		CurrentThreadMemoryContext = AllocSetContextCreate(CurrentMemoryContext,
							  "Storage Work Thread Context",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);
	PaxosMsgType type = *(PaxosMsgType*)Msg; /* this is the paxos pointer, here we shouldn't free it */
	bool success = false;
	switch (type)
	{
		case PAXOS_RUN_PUT:
			{
				TupleKeySlice key = {NULL, 0};
				TupleValueSlice value = {NULL, 0};
				Oid rangeid = -1;
				TransferPaxosMsgToPut(Msg, &rangeid, &key, &value);
				engine->put(engine, key, value);
				if (rangeid != -1)
					UpdateStatisticsInsert(rangeid, key, value);
				/*ereport(LOG,
					(errmsg("PAXOS: paxos run write key value success!")));*/
				pfree(key.data);
				pfree(value.data);
				success = true;
			}
			break;
		case PAXOS_RUN_DELETE:
			{
				TupleKeySlice key = {NULL, 0};
				Oid rangeid = -1;
				TupleValueSlice value = engine->get(engine, key);
				TransferPaxosMsgToDelete(Msg, &rangeid, &key);
				engine->delete_direct(engine, key);
				if (rangeid != -1)
					UpdateStatisticsDelete(rangeid, key, value);
				pfree(key.data);
				pfree(value.data);
				success = true;
			}
			break;
	default:
		break;
	}
	if (CurrentThreadMemoryContext != NULL)
	{
		MemoryContextDelete(CurrentThreadMemoryContext);
		CurrentThreadMemoryContext = NULL;
	}

	if (success == true)
		return PAXOS_RUN_SUCCESS;
	else
		return PAXOS_RUN_FAILED;
}

void*
TransferMsgToPaxos(PaxosMsgType type, TupleKeySlice key, TupleValueSlice value, Oid rangeid, int *msglength)
{
	switch (type)
	{
		case PAXOS_RUN_PUT:
			{
				return (void*)TransferPutIntoPaxosMsg(key, value, rangeid, msglength);
			}
			break;
		case PAXOS_RUN_DELETE:
			{
				return (void*)TransferDeleteIntoPaxosMsg(key, rangeid, msglength);
			}
		break;
	}
	return NULL;
}