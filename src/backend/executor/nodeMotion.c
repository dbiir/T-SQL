/*-------------------------------------------------------------------------
 *
 * nodeMotion.c
 *	  Routines to handle moving tuples around in Greenplum Database.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/executor/nodeMotion.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "nodes/execnodes.h"	/* Slice, SliceTable */
#include "cdb/cdbmotion.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbhash.h"
#include "executor/executor.h"
#include "executor/execdebug.h"
#include "executor/execUtils.h"
#include "executor/nodeMotion.h"
#include "lib/binaryheap.h"
#include "utils/tuplesort.h"
#include "utils/tuplesort_mk_details.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "tdb/range.h"
#include "tdb/route.h"

#include "access/xact.h"
#include "access/rwset.h"
#include "utils/guc.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "tdb/kvengine.h"
#include "tdb/kv_universal.h"
#include "tdb/storage_param.h"
/* #define MEASURE_MOTION_TIME */

#ifdef MEASURE_MOTION_TIME
#include <unistd.h>				/* gettimeofday */
#endif

/* #define CDB_MOTION_DEBUG */

#ifdef CDB_MOTION_DEBUG
#include "lib/stringinfo.h"		/* StringInfo */
#endif

/*
 * CdbTupleHeapInfo
 *
 * A priority queue element holding the next tuple of the
 * sorted tuple stream received from a particular sender.
 * Used by sorted receiver (Merge Receive).
 */
typedef struct CdbTupleHeapInfo
{
	/* Next tuple from this sender */
	GenericTuple tuple;

	/* Tuple Key stored in kv engine */
	TupleKeySlice key;

	/* Which sender did this tuple come from? */
	int			sourceRouteId;

	Datum		datum1;			/* value of first key column */
	bool		isnull1;		/* is first key column NULL? */
}			CdbTupleHeapInfo;

/*
 * CdbMergeComparatorContext
 *
 * This contains the information necessary to compare
 * two tuples (other than the tuples themselves).
 * It includes :
 *		1) the number and array of indexes into the tuples columns
 *			that are the basis for the ordering
 *			(numSortCols, sortColIdx)
 *		2) the FmgrInfo and flags of the compare function
 *			for each column being ordered
 *			(sortFunctions, cmpFlags)
 *		3) the tuple desc
 *			(tupDesc)
 * Used by sorted receiver (Merge Receive).  It is passed as the
 * context argument to the key comparator.
 */
typedef struct CdbMergeComparatorContext
{
	int			numSortCols;
	SortSupport sortKeys;
	TupleDesc	tupDesc;
	MemTupleBinding *mt_bind;

	CdbTupleHeapInfo *tupleheap_entries;
} CdbMergeComparatorContext;

static CdbMergeComparatorContext *CdbMergeComparator_CreateContext(CdbTupleHeapInfo *tupleheap_entries,
								 TupleDesc tupDesc,
								 int numSortCols,
								 AttrNumber *sortColIdx,
								 Oid *sortOperators,
								 Oid *sortCollations,
								 bool *nullsFirstFlags);

static void CdbMergeComparator_DestroyContext(CdbMergeComparatorContext *ctx);


/*=========================================================================
 * FUNCTIONS PROTOTYPES
 */
static TupleTableSlot *execMotionSender(MotionState *node);
static TupleTableSlot *execMotionUnsortedReceiver(MotionState *node);
static TupleTableSlot *execMotionSortedReceiver(MotionState *node);
static TupleTableSlot *execMotionSortedReceiver_mk(MotionState *node);

static void execMotionSortedReceiverFirstTime(MotionState *node);

static int	CdbMergeComparator(Datum lhs, Datum rhs, void *context);
static uint32 evalHashKey(ExprContext *econtext, List *hashkeys, CdbHash *h);
static uint32 evalRangeKey(ExprContext *econtext, List *rangekeys, List *rangetypes, Relation rel, RangeID *rangeid);
static void doSendEndOfStream(Motion *motion, MotionState *node);
static void doSendTuple(Motion *motion, MotionState *node, TupleTableSlot *outerTupleSlot);


/*=========================================================================
 */

#ifdef CDB_MOTION_DEBUG
static void
formatTuple(StringInfo buf, HeapTuple tup, TupleDesc tupdesc, Oid *outputFunArray)
{
	int			i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		bool		isnull;
		Datum		d = heap_getattr(tup, i + 1, tupdesc, &isnull);

		if (d && !isnull)
		{
			Datum		ds = OidFunctionCall1(outputFunArray[i], d);
			char	   *s = DatumGetCString(ds);
			char	   *name = NameStr(tupdesc->attrs[i]->attname);

			if (name && *name)
				appendStringInfo(buf, "  %s=\"%.30s\"", name, s);
			else
				appendStringInfo(buf, "  \"%.30s\"", s);
			pfree(s);
		}
	}
	appendStringInfoChar(buf, '\n');
}
#endif

/**
 * Is it a gather motion?
 */
bool
isMotionGather(const Motion *m)
{
	return (m->motionType == MOTIONTYPE_FIXED
			&& !m->isBroadcast);
}

/* ----------------------------------------------------------------
 *		ExecMotion
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecMotion(MotionState *node)
{
	Motion	   *motion = (Motion *) node->ps.plan;

	/* sanity check */
 	if (node->stopRequested)
 		ereport(ERROR,
 				(errcode(ERRCODE_INTERNAL_ERROR),
 				 errmsg("unexpected internal error"),
 				 errmsg("Already stopped motion node is executed again, data will lost"),
 				 errhint("Likely motion node is incorrectly squelched earlier")));

	/*
	 * at the top here we basically decide: -- SENDER vs. RECEIVER and --
	 * SORTED vs. UNSORTED
	 */
	if (node->mstype == MOTIONSTATE_RECV)
	{
		TupleTableSlot *tuple;
#ifdef MEASURE_MOTION_TIME
		struct timeval startTime;
		struct timeval stopTime;

		gettimeofday(&startTime, NULL);
#endif

		if (node->ps.state->active_recv_id >= 0)
		{
			if (node->ps.state->active_recv_id != motion->motionID)
			{
				/*
				 * See motion_sanity_walker() for details on how a deadlock
				 * may occur.
				 */
				elog(LOG, "DEADLOCK HAZARD: Updating active_motion_id from %d to %d",
					 node->ps.state->active_recv_id, motion->motionID);
				node->ps.state->active_recv_id = motion->motionID;
			}
		}
		else
			node->ps.state->active_recv_id = motion->motionID;

		if (motion->sendSorted)
		{
			if (gp_enable_motion_mk_sort)
				tuple = execMotionSortedReceiver_mk(node);
			else
				tuple = execMotionSortedReceiver(node);
		}
		else
			tuple = execMotionUnsortedReceiver(node);

		/*
		 * We tell the upper node as if this was the end of tuple stream if
		 * query-finish is requested.  Unlike other nodes, we skipped this
		 * check in ExecProc because this node in sender mode should send EoS
		 * to the receiver side, but the receiver side can simply stop
		 * processing the stream.  The sender side of this stream could still
		 * be sending more tuples, but this slice will eventually clean up the
		 * executor and eventually Stop message will be delivered to the
		 * sender side.
		 */
		if (QueryFinishPending)
			tuple = NULL;

		if (tuple == NULL)
			node->ps.state->active_recv_id = -1;
#ifdef MEASURE_MOTION_TIME
		gettimeofday(&stopTime, NULL);

		node->motionTime.tv_sec += stopTime.tv_sec - startTime.tv_sec;
		node->motionTime.tv_usec += stopTime.tv_usec - startTime.tv_usec;

		while (node->motionTime.tv_usec < 0)
		{
			node->motionTime.tv_usec += 1000000;
			node->motionTime.tv_sec--;
		}

		while (node->motionTime.tv_usec >= 1000000)
		{
			node->motionTime.tv_usec -= 1000000;
			node->motionTime.tv_sec++;
		}
#endif

		return tuple;
	}
	else if (node->mstype == MOTIONSTATE_SEND)
	{
		return execMotionSender(node);
	}
	else
	{
		elog(ERROR, "cannot execute inactive Motion");
		return NULL;
	}
}

static TupleTableSlot *
execMotionSender(MotionState *node)
{
	/* SENDER LOGIC */
	TupleTableSlot *outerTupleSlot;
	PlanState  *outerNode;
	Motion	   *motion = (Motion *) node->ps.plan;
	bool		done = false;
	int			numsegments = 0;

	/* need refactor */
	if (node->isExplictGatherMotion)
	{
		numsegments = motion->plan.flow->numsegments;
	}



#ifdef MEASURE_MOTION_TIME
	struct timeval time1;
	struct timeval time2;

	gettimeofday(&time1, NULL);
#endif

	AssertState(motion->motionType == MOTIONTYPE_HASH || motion->motionType == MOTIONTYPE_RANGE ||
				(motion->motionType == MOTIONTYPE_EXPLICIT && motion->segidColIdx > 0) ||
				(motion->motionType == MOTIONTYPE_FIXED));
	Assert(node->ps.state->interconnect_context);

	while (!done)
	{
		/* grab TupleTableSlot from our child. */
		outerNode = outerPlanState(node);
		outerTupleSlot = ExecProcNode(outerNode);

#ifdef MEASURE_MOTION_TIME
		gettimeofday(&time2, NULL);

		node->otherTime.tv_sec += time2.tv_sec - time1.tv_sec;
		node->otherTime.tv_usec += time2.tv_usec - time1.tv_usec;

		while (node->otherTime.tv_usec < 0)
		{
			node->otherTime.tv_usec += 1000000;
			node->otherTime.tv_sec--;
		}

		while (node->otherTime.tv_usec >= 1000000)
		{
			node->otherTime.tv_usec -= 1000000;
			node->otherTime.tv_sec++;
		}
#endif

		if (done || TupIsNull(outerTupleSlot))
		{
			doSendEndOfStream(motion, node);
			done = true;
		}
		else if (node->isExplictGatherMotion &&
				 GpIdentity.segindex != (gp_session_id % numsegments))
		{
			/*
			 * For explicit gather motion, receiver get data from the
			 * singleton segment explictly.
			 */
		}
		else
		{
			doSendTuple(motion, node, outerTupleSlot);
			/* doSendTuple() may have set node->stopRequested as a side-effect */

			if (node->stopRequested)
			{
				elog(gp_workfile_caching_loglevel, "Motion calling Squelch on child node");
				/* propagate stop notification to our children */
				ExecSquelchNode(outerNode);
				done = true;
			}
		}
#ifdef MEASURE_MOTION_TIME
		gettimeofday(&time1, NULL);

		node->motionTime.tv_sec += time1.tv_sec - time2.tv_sec;
		node->motionTime.tv_usec += time1.tv_usec - time2.tv_usec;

		while (node->motionTime.tv_usec < 0)
		{
			node->motionTime.tv_usec += 1000000;
			node->motionTime.tv_sec--;
		}

		while (node->motionTime.tv_usec >= 1000000)
		{
			node->motionTime.tv_usec -= 1000000;
			node->motionTime.tv_sec++;
		}
#endif
	}

	Assert(node->stopRequested || node->numTuplesFromChild == node->numTuplesToAMS);

	/* nothing else to send out, so we return NULL up the tree. */
	return NULL;
}


static TupleTableSlot *
execMotionUnsortedReceiver(MotionState *node)
{
	/* RECEIVER LOGIC */
	TupleTableSlot *slot;
	GenericTuple tuple;
	TupleKeySlice key = {0};
	RangeID rangeid = 0;
	Motion	   *motion = (Motion *) node->ps.plan;

	AssertState(motion->motionType == MOTIONTYPE_HASH || motion->motionType == MOTIONTYPE_RANGE ||
				(motion->motionType == MOTIONTYPE_EXPLICIT && motion->segidColIdx > 0) ||
				(motion->motionType == MOTIONTYPE_FIXED));

	Assert(node->ps.state->motionlayer_context);
	Assert(node->ps.state->interconnect_context);

	if (node->stopRequested)
	{
		SendStopMessage(node->ps.state->motionlayer_context,
						node->ps.state->interconnect_context,
						motion->motionID);
		return NULL;
	}

	tuple = RecvTupleFrom(node->ps.state->motionlayer_context,
						  node->ps.state->interconnect_context,
						  motion->motionID, &key, &rangeid, ANY_ROUTE);

	if (!tuple)
	{
#ifdef CDB_MOTION_DEBUG
		if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
			elog(DEBUG4, "motionID=%d saw end of stream", motion->motionID);
#endif
		Assert(node->numTuplesFromAMS == node->numTuplesToParent);
		Assert(node->numTuplesFromChild == 0);
		Assert(node->numTuplesToAMS == 0);
		return NULL;
	}

	node->numTuplesFromAMS++;
	node->numTuplesToParent++;

	/* store it in our result slot and return this. */
	slot = node->ps.ps_ResultTupleSlot;
	slot = ExecStoreGenericTuple(tuple, slot, true /* shouldFree */);
	slot = ExecStoreKey(key, slot, true /*shouldFree*/);

	/* append key to read/write set */
	if (transam_mode == TRANSAM_MODE_OCC || transam_mode == TRANSAM_MODE_BOCC)
		AppendKeyToReadWriteSet(key.len, (char *)key.data,
					node->ps.state->es_plannedstmt->commandType);

#ifdef CDB_MOTION_DEBUG
	if (node->numTuplesToParent <= 20)
	{
		StringInfoData buf;

		initStringInfo(&buf);
		appendStringInfo(&buf, "   motion%-3d rcv      %5d.",
						 motion->motionID,
						 node->numTuplesToParent);
		formatTuple(&buf, tuple, ExecGetResultType(&node->ps),
					node->outputFunArray);
		elog(DEBUG3, buf.data);
		pfree(buf.data);
	}
#endif
	slot->tts_rangeid = rangeid;
	return slot;
}



/*
 * General background on Sorted Motion:
 * -----------------------------------
 * NOTE: This function is only used for order-preserving motion.  There are
 * only 2 types of motion that order-preserving makes sense for: FIXED and
 * BROADCAST (HASH does not make sense). so we have:
 *
 * CASE 1:	 broadcast order-preserving fixed motion.  This should only be
 *			 called for SENDERs.
 *
 * CASE 2:	 single-destination order-preserving fixed motion.	The SENDER
 *			 side will act like Unsorted motion and won't call this. So only
 *			 the RECEIVER should be called for this case.
 *
 *
 * Sorted Receive Notes:
 * --------------------
 *
 * The 1st time we execute, we need to pull a tuple from each of our source
 * and store them in our tupleheap, this is what execMotionSortedFirstTime()
 * does.  Once that is done, we can pick the lowest (or whatever the
 * criterion is) value from amongst all the sources.  This works since each
 * stream is sorted itself.
 *
 * We keep track of which one was selected, this will be slot we will need
 * to fill during the next call.
 *
 * Subsequent calls to this function (after the 1st time) will start by
 * trying to receive a tuple for the slot that was emptied the previous call.
 * Then we again select the lowest value and return that tuple.
 *
 */

/* Sorted receiver using mk heap */
typedef struct MotionMKHeapReaderContext
{
	MotionState *node;
	int			srcRoute;
} MotionMKHeapReaderContext;

typedef struct MotionMKHeapContext
{
	MKHeapReader *readers;		/* Readers, one per sender */
	MKHeap	   *heap;			/* The mkheap */
	MKContext	mkctxt;			/* compare context */
} MotionMKHeapContext;

static bool
motion_mkhp_read(void *vpctxt, MKEntry *a)
{
	MotionMKHeapReaderContext *ctxt = (MotionMKHeapReaderContext *) vpctxt;
	MotionState *node = ctxt->node;

	GenericTuple inputTuple = NULL;
	Motion	   *motion = (Motion *) node->ps.plan;

	if (ctxt->srcRoute < 0)
	{
		/* routes have not been set yet so set them */
		ListCell   *lcProcess;
		int			routeIndex,
					readerIndex;
		MotionMKHeapContext *ctxt = node->tupleheap_mk;
		Slice	   *sendSlice = (Slice *) list_nth(node->ps.state->es_sliceTable->slices, motion->motionID);

		Assert(sendSlice->sliceIndex == motion->motionID);

		readerIndex = 0;
		foreach_with_count(lcProcess, sendSlice->primaryProcesses, routeIndex)
		{
			if (lfirst(lcProcess) != NULL)
			{
				MotionMKHeapReaderContext *readerContext;

				Assert(readerIndex < node->numInputSegs);

				readerContext = (MotionMKHeapReaderContext *) ctxt->readers[readerIndex].mkhr_ctxt;
				readerContext->srcRoute = routeIndex;
				readerIndex++;
			}
		}
		Assert(readerIndex == node->numInputSegs);
	}

	MemSet(a, 0, sizeof(MKEntry));

	/* Receive the successor of the tuple that we returned last time. */
	inputTuple = RecvTupleFrom(node->ps.state->motionlayer_context,
							   node->ps.state->interconnect_context,
							   motion->motionID,
							   NULL,
							   NULL,
							   ctxt->srcRoute);

	if (inputTuple)
	{
		a->ptr = inputTuple;
		return true;
	}

	return false;
}

static Datum
tupsort_fetch_datum_motion(MKEntry *a, MKContext *mkctxt, MKLvContext *lvctxt, bool *isNullOut)
{
	Datum		d;

	if (is_memtuple(a->ptr))
		d = memtuple_getattr((MemTuple) a->ptr, mkctxt->mt_bind, lvctxt->attno, isNullOut);
	else
		d = heap_getattr((HeapTuple) a->ptr, lvctxt->attno, mkctxt->tupdesc, isNullOut);
	return d;
}

static void
tupsort_free_datum_motion(MKEntry *e)
{
	pfree(e->ptr);
	e->ptr = NULL;
}

static void
create_motion_mk_heap(MotionState *node)
{
	MotionMKHeapContext *ctxt = palloc0(sizeof(MotionMKHeapContext));
	Motion	   *motion = (Motion *) node->ps.plan;
	int			nreader = node->numInputSegs;
	int			i = 0;

	Assert(nreader >= 1);

	create_mksort_context(
						  &ctxt->mkctxt,
						  motion->numSortCols, motion->sortColIdx,
						  motion->sortOperators,
						  motion->collations,
						  motion->nullsFirst,
						  NULL,
						  tupsort_fetch_datum_motion,
						  tupsort_free_datum_motion,
						  ExecGetResultType(&node->ps), false, 0 /* dummy does not matter */ );

	ctxt->readers = palloc0(sizeof(MKHeapReader) * nreader);

	for (i = 0; i < nreader; ++i)
	{
		MotionMKHeapReaderContext *hrctxt = palloc(sizeof(MotionMKHeapContext));

		hrctxt->node = node;
		hrctxt->srcRoute = -1;	/* set to a negative to indicate that we need
								 * to update it to the real value */
		ctxt->readers[i].reader = motion_mkhp_read;
		ctxt->readers[i].mkhr_ctxt = hrctxt;
	}

	node->tupleheap_mk = ctxt;
}

static void
destroy_motion_mk_heap(MotionState *node)
{
	/*
	 * Don't need to do anything.  Memory is allocated from query execution
	 * context.  By calling this, we are at the end of the life of a query.
	 */
}

static TupleTableSlot *
execMotionSortedReceiver_mk(MotionState *node)
{
	TupleTableSlot *slot = NULL;
	MKEntry		e;

	Motion	   *motion = (Motion *) node->ps.plan;
	MotionMKHeapContext *ctxt = node->tupleheap_mk;

	Assert(motion->motionType == MOTIONTYPE_FIXED &&
		   motion->sendSorted &&
		   ctxt
		);

	if (node->stopRequested)
	{
		SendStopMessage(node->ps.state->motionlayer_context,
						node->ps.state->interconnect_context,
						motion->motionID);
		return NULL;
	}

	if (!node->tupleheapReady)
	{
		Assert(ctxt->readers);
		Assert(!ctxt->heap);
		ctxt->heap = mkheap_from_reader(ctxt->readers, node->numInputSegs, &ctxt->mkctxt);
		node->tupleheapReady = true;
	}

	mke_set_empty(&e);
	mkheap_putAndGet(ctxt->heap, &e);
	if (mke_is_empty(&e))
		return NULL;

	slot = node->ps.ps_ResultTupleSlot;
	slot = ExecStoreGenericTuple(e.ptr, slot, true);
	return slot;
}

/* Sorted receiver using binary heap */
static TupleTableSlot *
execMotionSortedReceiver(MotionState *node)
{
	TupleTableSlot *slot;
	binaryheap *hp = node->tupleheap;
	GenericTuple tuple,
				inputTuple;
	TupleKeySlice key = {0},
				inputKey = {0};
	Motion	   *motion = (Motion *) node->ps.plan;
	CdbTupleHeapInfo *tupHeapInfo;
	RangeID rangeid = 0;

	AssertState(motion->motionType == MOTIONTYPE_FIXED &&
				motion->sendSorted &&
				hp != NULL);

	/* Notify senders and return EOS if caller doesn't want any more data. */
	if (node->stopRequested)
	{

		SendStopMessage(node->ps.state->motionlayer_context,
						node->ps.state->interconnect_context,
						motion->motionID);
		return NULL;
	}

	/* On first call, fill the priority queue with each sender's first tuple. */
	if (!node->tupleheapReady)
	{
		execMotionSortedReceiverFirstTime(node);
	}

	/*
	 * Delete from the priority queue the element that we fetched last time.
	 * Receive and insert the next tuple from that same sender.
	 */
	else
	{
		/* Old element is still at the head of the pq. */
		Assert(DatumGetInt32(binaryheap_first(hp)) == node->routeIdNext);

		/* Receive the successor of the tuple that we returned last time. */
		inputTuple = RecvTupleFrom(node->ps.state->motionlayer_context,
								   node->ps.state->interconnect_context,
								   motion->motionID,
								   &inputKey,
								   &rangeid,
								   node->routeIdNext);

		/* Substitute it in the pq for its predecessor. */
		if (inputTuple)
		{
			CdbTupleHeapInfo *info = &node->tupleheap_entries[node->routeIdNext];
			AttrNumber key1_attno = node->tupleheap_cxt->sortKeys[0].ssup_attno;

			info->tuple = inputTuple;
			info->key = inputKey;
			if (is_memtuple(inputTuple))
				info->datum1 = memtuple_getattr((MemTuple) inputTuple,
												node->tupleheap_cxt->mt_bind,
												key1_attno,
												&info->isnull1);
			else
				info->datum1 = heap_getattr((HeapTuple) inputTuple,
											key1_attno,
											node->tupleheap_cxt->tupDesc,
											&info->isnull1);

			binaryheap_replace_first(hp, Int32GetDatum(node->routeIdNext));

			node->numTuplesFromAMS++;

#ifdef CDB_MOTION_DEBUG
			if (node->numTuplesFromAMS <= 20)
			{
				StringInfoData buf;

				initStringInfo(&buf);
				appendStringInfo(&buf, "   motion%-3d rcv<-%-3d %5d.",
								 motion->motionID,
								 node->routeIdNext,
								 node->numTuplesFromAMS);
				formatTuple(&buf, inputTuple, ExecGetResultType(&node->ps),
							node->outputFunArray);
				elog(DEBUG3, buf.data);
				pfree(buf.data);
			}
#endif
		}

		/* At EOS, drop this sender from the priority queue. */
		else if (!binaryheap_empty(hp))
			binaryheap_remove_first(hp);
	}

	/* Finished if all senders have returned EOS. */
	if (binaryheap_empty(hp))
	{
		Assert(node->numTuplesFromAMS == node->numTuplesToParent);
		Assert(node->numTuplesFromChild == 0);
		Assert(node->numTuplesToAMS == 0);
		return NULL;
	}

	/*
	 * Our next result tuple, with lowest key among all senders, is now at the
	 * head of the priority queue.  Get it from there.
	 *
	 * We transfer ownership of the tuple from the pq element to our caller,
	 * but the pq element itself will remain in place until the next time we
	 * are called, to avoid an unnecessary rearrangement of the priority
	 * queue.
	 */
	node->routeIdNext = binaryheap_first(hp);
	tupHeapInfo = &node->tupleheap_entries[node->routeIdNext];
	tuple = tupHeapInfo->tuple;
	key = tupHeapInfo->key;
	/* Zap dangling tuple ptr for safety. PQ element doesn't own it anymore. */
	tupHeapInfo->tuple = NULL;
	tupHeapInfo->key.data = NULL;
	tupHeapInfo->key.len = 0;

	/* Update counters. */
	node->numTuplesToParent++;

	/* Store tuple in our result slot. */
	slot = node->ps.ps_ResultTupleSlot;
	slot = ExecStoreGenericTuple(tuple, slot, true /* shouldFree */ );
	slot = ExecStoreKey(key, slot, true /* shouldFree */);

	/* append key to read/write set */
	if (transam_mode == TRANSAM_MODE_OCC || transam_mode == TRANSAM_MODE_BOCC)
		AppendKeyToReadWriteSet(key.len, (char *)key.data,
					node->ps.state->es_plannedstmt->commandType);

#ifdef CDB_MOTION_DEBUG
	if (node->numTuplesToParent <= 20)
	{
		StringInfoData buf;

		initStringInfo(&buf);
		appendStringInfo(&buf, "   motion%-3d mrg<-%-3d %5d.",
						 motion->motionID,
						 node->routeIdNext,
						 node->numTuplesToParent);
		formatTuple(&buf, tuple, ExecGetResultType(&node->ps),
					node->outputFunArray);
		elog(DEBUG3, buf.data);
		pfree(buf.data);
	}
#endif

	/* Return result slot. */
	return slot;
}								/* execMotionSortedReceiver */


void
execMotionSortedReceiverFirstTime(MotionState *node)
{
	GenericTuple inputTuple;
	binaryheap *hp = node->tupleheap;
	Motion	   *motion = (Motion *) node->ps.plan;
	int			iSegIdx;
	ListCell   *lcProcess;
	TupleKeySlice inputKey = {0};
	RangeID rangeid = 0;
	CdbMergeComparatorContext *comparatorContext = node->tupleheap_cxt;
	AttrNumber	key1_attno = motion->sortColIdx[0];
	Slice	   *sendSlice = (Slice *) list_nth(node->ps.state->es_sliceTable->slices, motion->motionID);

	Assert(sendSlice->sliceIndex == motion->motionID);

	/*
	 * Get the first tuple from every sender, and stick it into the heap.
	 */
	foreach_with_count(lcProcess, sendSlice->primaryProcesses, iSegIdx)
	{
		if (lfirst(lcProcess) == NULL)
			continue;			/* skip this one: we are not receiving from it */

		/*
		 * another place where we are mapping segid space to routeid space. so
		 * route[x] = inputSegIdx[x] now.
		 */
		inputTuple = RecvTupleFrom(node->ps.state->motionlayer_context,
								   node->ps.state->interconnect_context,
								   motion->motionID, &inputKey, &rangeid, iSegIdx);

		if (inputTuple)
		{
			CdbTupleHeapInfo *info = &node->tupleheap_entries[iSegIdx];

			info->tuple = inputTuple;
			info->key = inputKey;
			binaryheap_add_unordered(hp, iSegIdx);

			if (is_memtuple(inputTuple))
				info->datum1 = memtuple_getattr((MemTuple) inputTuple,
												comparatorContext->mt_bind,
												key1_attno,
												&info->isnull1);
			else
				info->datum1 = heap_getattr((HeapTuple) inputTuple,
											key1_attno,
											comparatorContext->tupDesc,
											&info->isnull1);

			node->numTuplesFromAMS++;

#ifdef CDB_MOTION_DEBUG
			if (node->numTuplesFromAMS <= 20)
			{
				StringInfoData buf;

				initStringInfo(&buf);
				appendStringInfo(&buf, "   motion%-3d rcv<-%-3d %5d.",
								 motion->motionID,
								 iSegIdx,
								 node->numTuplesFromAMS);
				formatTuple(&buf, inputTuple, ExecGetResultType(&node->ps),
							node->outputFunArray);
				elog(DEBUG3, buf.data);
				pfree(buf.data);
			}
#endif
		}
	}
	Assert(iSegIdx == node->numInputSegs);

	/*
	 * Done adding the elements, now arrange the heap to satisfy the heap
	 * property. This is quicker than inserting the initial elements one by
	 * one.
	 */
	binaryheap_build(hp);

	node->tupleheapReady = true;
}								/* execMotionSortedReceiverFirstTime */


/* ----------------------------------------------------------------
 *		ExecInitMotion
 *
 * NOTE: have to be a bit careful, estate->es_cur_slice_idx is not the
 *		 ultimate correct value that it should be on the QE. this happens
 *		 after this call in mppexec.c.	This is ok since we don't need it,
 *		 but just be aware before you try and use it here.
 * ----------------------------------------------------------------
 */

MotionState *
ExecInitMotion(Motion *node, EState *estate, int eflags)
{
	MotionState *motionstate = NULL;
	TupleDesc	tupDesc;
	Slice	   *sendSlice = NULL;
	Slice	   *recvSlice = NULL;
	SliceTable *sliceTable = estate->es_sliceTable;

#ifdef CDB_MOTION_DEBUG
	int			i;
#endif

	Assert(node->motionID > 0);
	Assert(node->motionID <= sliceTable->nMotions);

	estate->currentSliceIdInPlan = node->motionID;
	estate->currentExecutingSliceId = node->motionID;

	/*
	 * create state structure
	 */
	motionstate = makeNode(MotionState);
	motionstate->ps.plan = (Plan *) node;
	motionstate->ps.state = estate;
	motionstate->mstype = MOTIONSTATE_NONE;
	motionstate->stopRequested = false;
	motionstate->hashExprs = NIL;
	motionstate->cdbhash = NULL;
	motionstate->isExplictGatherMotion = false;

	/* Look up the sending gang's slice table entry. */
	sendSlice = (Slice *) list_nth(sliceTable->slices, node->motionID);
	Assert(IsA(sendSlice, Slice));
	Assert(sendSlice->sliceIndex == node->motionID);

	/* QD must fill in the global slice table. */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

		/* Look up the receiving (parent) gang's slice table entry. */
		recvSlice = (Slice *) list_nth(sliceTable->slices, sendSlice->parentIndex);

		if (node->motionType == MOTIONTYPE_FIXED && !node->isBroadcast)
		{
			/* Sending to a single receiving process on the entry db? */
			/* Is receiving slice a root slice that runs here in the qDisp? */
			if (recvSlice->sliceIndex == recvSlice->rootIndex)
			{
				motionstate->mstype = MOTIONSTATE_RECV;
				Assert(recvSlice->gangType == GANGTYPE_UNALLOCATED ||
					   recvSlice->gangType == GANGTYPE_PRIMARY_WRITER);
			}
			else
			{
				/* sanity checks */
				if (recvSlice->gangSize != 1)
					elog(ERROR, "unexpected gang size: %d", recvSlice->gangSize);
			}
		}

		MemoryContextSwitchTo(oldcxt);
	}

	/* QE must fill in map from motionID to MotionState node. */
	else
	{
		Insist(Gp_role == GP_ROLE_EXECUTE);

		recvSlice = (Slice *) list_nth(sliceTable->slices, sendSlice->parentIndex);

		if (LocallyExecutingSliceIndex(estate) == recvSlice->sliceIndex)
		{
			/* this is recv */
			motionstate->mstype = MOTIONSTATE_RECV;
		}
		else if (LocallyExecutingSliceIndex(estate) == sendSlice->sliceIndex)
		{
			/* this is send */
			motionstate->mstype = MOTIONSTATE_SEND;
		}
		/* TODO: If neither sending nor receiving, don't bother to initialize. */
	}

	/*
	 * If it's gather motion and subplan's locus is CdbLocusType_Replicated,
	 * mark isExplictGatherMotion to true
	 */
	if (motionstate->mstype == MOTIONSTATE_SEND &&
		node->motionType == MOTIONTYPE_FIXED &&
		!node->isBroadcast &&
		outerPlan(node) &&
		outerPlan(node)->flow &&
		outerPlan(node)->flow->locustype == CdbLocusType_Replicated)
	{
		motionstate->isExplictGatherMotion = true;
	}

	motionstate->tupleheapReady = false;
	motionstate->sentEndOfStream = false;

	motionstate->otherTime.tv_sec = 0;
	motionstate->otherTime.tv_usec = 0;
	motionstate->motionTime.tv_sec = 0;
	motionstate->motionTime.tv_usec = 0;

	motionstate->numTuplesFromChild = 0;
	motionstate->numTuplesToAMS = 0;
	motionstate->numTuplesFromAMS = 0;
	motionstate->numTuplesToParent = 0;

	motionstate->stopRequested = false;
	motionstate->numInputSegs = sendSlice->gangSize;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &motionstate->ps);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &motionstate->ps);

	/*
	 * Initializes child nodes. If alien elimination is on, we skip children
	 * of receiver motion.
	 */
	if (!estate->eliminateAliens || motionstate->mstype == MOTIONSTATE_SEND)
	{
		outerPlanState(motionstate) = ExecInitNode(outerPlan(node), estate, eflags);
	}

	/*
	 * initialize tuple type.  no need to initialize projection info because
	 * this node doesn't do projections.
	 */
	ExecAssignResultTypeFromTL(&motionstate->ps);
	motionstate->ps.ps_ProjInfo = NULL;
	tupDesc = ExecGetResultType(&motionstate->ps);

	/* Set up motion send data structures */
	/* [hongyaozhao] Temporarily write the processing of range distribution here*/
	if (motionstate->mstype == MOTIONSTATE_SEND && 
		(node->motionType == MOTIONTYPE_HASH || node->motionType == MOTIONTYPE_RANGE))
	{
		int			nkeys;
		int			numsegments;

		nkeys = list_length(node->hashExprs);

		if (nkeys > 0)
			motionstate->hashExprs = (List *) ExecInitExpr((Expr *) node->hashExprs,
														   (PlanState *) motionstate);

		/*
		 * Create hash API reference
		 */
		if (estate->es_plannedstmt->planGen == PLANGEN_PLANNER)
		{
			Assert(node->plan.flow);
			Assert(node->plan.flow->numsegments > 0);

			/*
			 * For planner generated plan the size of receiver slice can be
			 * determined from flow.
			 */
			numsegments = node->plan.flow->numsegments;
		}
		else
		{
			/*
			 * For ORCA generated plan we could distribute to ALL as partially
			 * distributed tables are not supported by ORCA yet.
			 */
			numsegments = getgpsegmentCount();
		}

		motionstate->cdbhash = makeCdbHash(numsegments, nkeys, node->hashFuncs);
	}

	/* Merge Receive: Set up the key comparator and priority queue. */
	if (node->sendSorted && motionstate->mstype == MOTIONSTATE_RECV)
	{
		if (gp_enable_motion_mk_sort)
			create_motion_mk_heap(motionstate);
		else
		{
			/* Allocate context object for the key comparator. */
			motionstate->tupleheap_entries =
				palloc(motionstate->numInputSegs * sizeof(CdbTupleHeapInfo));
			/* Create the priority queue structure. */
			motionstate->tupleheap_cxt =
				CdbMergeComparator_CreateContext(motionstate->tupleheap_entries,
												 tupDesc,
												 node->numSortCols,
												 node->sortColIdx,
												 node->sortOperators,
												 node->collations,
												 node->nullsFirst);
			motionstate->tupleheap =
				binaryheap_allocate(motionstate->numInputSegs,
									CdbMergeComparator,
									motionstate->tupleheap_cxt);
		}
	}

	/*
	 * Perform per-node initialization in the motion layer.
	 */
	UpdateMotionLayerNode(motionstate->ps.state->motionlayer_context,
						  node->motionID,
						  node->sendSorted,
						  tupDesc);


#ifdef CDB_MOTION_DEBUG
	motionstate->outputFunArray = (Oid *) palloc(tupDesc->natts * sizeof(Oid));
	for (i = 0; i < tupDesc->natts; i++)
	{
		bool		typisvarlena;

		getTypeOutputInfo(tupDesc->attrs[i]->atttypid,
						  &motionstate->outputFunArray[i],
						  &typisvarlena);
	}
#endif

	return motionstate;
}

/* ----------------------------------------------------------------
 *		ExecEndMotion(node)
 * ----------------------------------------------------------------
 */
void
ExecEndMotion(MotionState *node)
{
	Motion	   *motion = (Motion *) node->ps.plan;
	uint16		motNodeID = motion->motionID;
#ifdef MEASURE_MOTION_TIME
	double		otherTimeSec;
	double		motionTimeSec;
#endif

	ExecFreeExprContext(&node->ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	/*
	 * Set the slice no for the nodes under this motion.
	 */
	Assert(node->ps.state != NULL);
	node->ps.state->currentSliceIdInPlan = motNodeID;
	int			parentExecutingSliceId = node->ps.state->currentExecutingSliceId;

	node->ps.state->currentExecutingSliceId = motNodeID;

	/*
	 * shut down the subplan
	 */
	ExecEndNode(outerPlanState(node));

#ifdef MEASURE_MOTION_TIME
	motionTimeSec = (double) node->motionTime.tv_sec + (double) node->motionTime.tv_usec / 1000000.0;

	if (node->mstype == MOTIONSTATE_RECV)
	{
		elog(DEBUG1,
			 "Motion Node %d (RECEIVER) Statistics:\n"
			 "Timing:  \n"
			 "\t Time receiving the tuple: %f sec\n"
			 "Counters: \n"
			 "\tnumTuplesFromChild: %d\n"
			 "\tnumTuplesFromAMS: %d\n"
			 "\tnumTuplesToAMS: %d\n"
			 "\tnumTuplesToParent: %d\n",
			 motNodeID,
			 motionTimeSec,
			 node->numTuplesFromChild,
			 node->numTuplesFromAMS,
			 node->numTuplesToAMS,
			 node->numTuplesToParent
			);
	}
	else if (node->mstype == MOTIONSTATE_SEND)
	{
		otherTimeSec = (double) node->otherTime.tv_sec + (double) node->otherTime.tv_usec / 1000000.0;
		elog(DEBUG1,
			 "Motion Node %d (SENDER) Statistics:\n"
			 "Timing:  \n"
			 "\t Time getting next tuple to send: %f sec \n"
			 "\t Time sending the tuple:          %f  sec\n"
			 "\t Percentage of time sending:      %2.2f%% \n"
			 "Counters: \n"
			 "\tnumTuplesFromChild: %d\n"
			 "\tnumTuplesToAMS: %d\n",
			 motNodeID,
			 otherTimeSec,
			 motionTimeSec,
			 (double) (motionTimeSec / (otherTimeSec + motionTimeSec)) * 100,
			 node->numTuplesFromChild,
			 node->numTuplesToAMS
			);
	}
#endif							/* MEASURE_MOTION_TIME */

	/* Merge Receive: Free the priority queue and associated structures. */
	if (node->tupleheap != NULL)
	{
		binaryheap_free(node->tupleheap);

		CdbMergeComparator_DestroyContext(node->tupleheap_cxt);
		node->tupleheap = NULL;
	}
	if (node->tupleheap_mk)
	{
		destroy_motion_mk_heap(node);
		node->tupleheap_mk = NULL;
	}

	/* Free the slices and routes */
	if (node->cdbhash != NULL)
	{
		pfree(node->cdbhash);
		node->cdbhash = NULL;
	}

	/*
	 * Free up this motion node's resources in the Motion Layer.
	 *
	 * TODO: For now, we don't flush the comm-layer.  NO ERRORS DURING AMS!!!
	 */
	EndMotionLayerNode(node->ps.state->motionlayer_context, motNodeID, /* flush-comm-layer */ false);

#ifdef CDB_MOTION_DEBUG
	if (node->outputFunArray)
		pfree(node->outputFunArray);
#endif

	/*
	 * Temporarily set currentExecutingSliceId to the parent value, since this
	 * motion might be in the top slice of an InitPlan.
	 */
	node->ps.state->currentExecutingSliceId = parentExecutingSliceId;
	EndPlanStateGpmonPkt(&node->ps);
	node->ps.state->currentExecutingSliceId = motNodeID;
}



/*=========================================================================
 * HELPER FUNCTIONS
 */

/*
 * CdbMergeComparator:
 * Used to compare tuples for a sorted motion node.
 */
static int
CdbMergeComparator(Datum lhs, Datum rhs, void *context)
{
	int			lSegIdx = DatumGetInt32(lhs);
	int			rSegIdx = DatumGetInt32(rhs);
	CdbMergeComparatorContext *ctx = (CdbMergeComparatorContext *) context;
	CdbTupleHeapInfo *linfo = (CdbTupleHeapInfo *) &ctx->tupleheap_entries[lSegIdx];
	CdbTupleHeapInfo *rinfo = (CdbTupleHeapInfo *) &ctx->tupleheap_entries[rSegIdx];
	GenericTuple ltup = linfo->tuple;
	GenericTuple rtup = rinfo->tuple;
	SortSupport	sortKeys = ctx->sortKeys;
	TupleDesc	tupDesc;
	int			nkey;
	int			numSortCols = ctx->numSortCols;
	int			compare;

	Assert(ltup && rtup);

	tupDesc = ctx->tupDesc;

	/* First column. We have the Datum for that extracted already. */
	compare = ApplySortComparator(linfo->datum1, linfo->isnull1,
								  rinfo->datum1, rinfo->isnull1,
								  &sortKeys[0]);
	if (compare != 0)
		return -compare;

	/* Rest of the columns. */
	for (nkey = 1; nkey < numSortCols; nkey++)
	{
		SortSupport ssup = &sortKeys[nkey];
		AttrNumber	attno = ssup->ssup_attno;
		Datum		datum1,
					datum2;
		bool		isnull1,
					isnull2;

		if (is_memtuple(ltup))
			datum1 = memtuple_getattr((MemTuple) ltup, ctx->mt_bind, attno, &isnull1);
		else
			datum1 = heap_getattr((HeapTuple) ltup, attno, tupDesc, &isnull1);

		if (is_memtuple(rtup))
			datum2 = memtuple_getattr((MemTuple) rtup, ctx->mt_bind, attno, &isnull2);
		else
			datum2 = heap_getattr((HeapTuple) rtup, attno, tupDesc, &isnull2);

		compare = ApplySortComparator(datum1, isnull1,
									  datum2, isnull2,
									  ssup);
		if (compare != 0)
			return -compare;
	}

	return 0;
}								/* CdbMergeComparator */


/* Create context object for use by CdbMergeComparator */
static CdbMergeComparatorContext *
CdbMergeComparator_CreateContext(CdbTupleHeapInfo *tupleheap_entries,
								 TupleDesc tupDesc,
								 int numSortCols,
								 AttrNumber *sortColIdx,
								 Oid *sortOperators,
								 Oid *sortCollations,
								 bool *nullsFirstFlags)
{
	CdbMergeComparatorContext *ctx;
	int			i;

	Assert(tupDesc &&
		   numSortCols > 0 &&
		   sortColIdx &&
		   sortOperators);

	/* Allocate and initialize the context object. */
	ctx = (CdbMergeComparatorContext *) palloc0(sizeof(*ctx));

	ctx->numSortCols = numSortCols;
	ctx->tupDesc = tupDesc;
	ctx->mt_bind = create_memtuple_binding(tupDesc);
	ctx->tupleheap_entries = tupleheap_entries;

	/* Prepare SortSupport data for each column */
	ctx->sortKeys = (SortSupport) palloc0(numSortCols * sizeof(SortSupportData));

	for (i = 0; i < numSortCols; i++)
	{
		SortSupport sortKey = &ctx->sortKeys[i];

		AssertArg(sortColIdx[i] != 0);
		AssertArg(sortOperators[i] != 0);

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = sortCollations[i];
		sortKey->ssup_nulls_first = nullsFirstFlags[i];
		sortKey->ssup_attno = sortColIdx[i];

		PrepareSortSupportFromOrderingOp(sortOperators[i], sortKey);
	}

	return ctx;
}								/* CdbMergeComparator_CreateContext */


void
CdbMergeComparator_DestroyContext(CdbMergeComparatorContext *ctx)
{
	if (!ctx)
		return;
	if (ctx->sortKeys)
		pfree(ctx->sortKeys);
}								/* CdbMergeComparator_DestroyContext */


/*
 * Experimental code that will be replaced later with new hashing mechanism
 */
uint32
evalHashKey(ExprContext *econtext, List *hashkeys, CdbHash * h)
{
	ListCell   *hk;
	MemoryContext oldContext;
	unsigned int target_seg;

	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/*
	 * If we have 1 or more distribution keys for this relation, hash them.
	 * However, If this happens to be a relation with an empty policy
	 * (partitioning policy with a NULL distribution key list) then we have no
	 * hash key value to feed in, so use cdbhashrandomseg() to pick a segment
	 * at random.
	 */
	if (list_length(hashkeys) > 0)
	{
		int			i;

		cdbhashinit(h);

		i = 0;
		foreach(hk, hashkeys)
		{
			ExprState  *keyexpr = (ExprState *) lfirst(hk);
			Datum		keyval;
			bool		isNull;

			/*
			 * Get the attribute value of the tuple
			 */
			keyval = ExecEvalExpr(keyexpr, econtext, &isNull, NULL);

			/*
			 * Compute the hash function
			 */
			cdbhash(h, i + 1, keyval, isNull);
			i++;
		}
		target_seg = cdbhashreduce(h);
	}
	else
	{
		target_seg = cdbhashrandomseg(h->numsegs);
	}

	MemoryContextSwitchTo(oldContext);

	return target_seg;
}

/*
 * Location of range distribution processing mechanism
 */
uint32
evalRangeKey(ExprContext *econtext, List *rangekeys, List *rangetypes, Relation rel, RangeID *rangeid)
{
	ListCell   *rk;
	ListCell   *rt;
	MemoryContext oldContext;

	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	Oid pk_oid = get_pk_oid(rel);
	int pk_natts = 0;
	get_index_colnos(pk_oid, &pk_natts);
	Oid *pk_type = palloc0(pk_natts * sizeof(Oid));
	Datum *pk_values = palloc0(pk_natts * sizeof(Datum));
	bool *pkisnull = palloc0(pk_natts * sizeof(bool));

	/*
	 * If we have 1 or more distribution keys for this relation, hash
	 * them. However, If this happens to be a relation with an empty
	 * policy (partitioning policy with a NULL distribution key list)
	 * then we have no hash key value to feed in, so use cdbhashnokey()
	 * to assign a hash value for us.
	 */
	if (list_length(rangekeys) > 0)
	{	
		for (int i = 0; i < pk_natts; i++)
		{
			forboth(rk, rangekeys, rt, rangetypes)
			{
				ExprState  *keyexpr = (ExprState *) lfirst(rk);
				Datum		keyval;
				bool		isNull;
				/*
				* Get the attribute value of the tuple
				*/
				keyval = ExecEvalExpr(keyexpr, econtext, &isNull, NULL);
				pk_values[i] = keyval;
				pkisnull[i] = isNull;
				pk_type[i] = lfirst_oid(rt);
			}
		}
	}
	else
	{
		return getgpsegmentCount();
	}

    InitKeyDesc initkey;
    switch (transaction_type)
    {
    case KVENGINE_ROCKSDB:
        initkey = init_basis_in_keydesc(KEY_WITH_XMINCMIN);
        break;
    case KVENGINE_TRANSACTIONDB:
        initkey = init_basis_in_keydesc(RAW_KEY);
        break;
    default:
        break;
    }
	initkey.rel_id = rel->rd_id;
	initkey.init_type = PRIMARY_KEY;
	init_pk_in_keydesc(&initkey, pk_oid, pk_type, pk_values, pkisnull, pk_natts);
	initkey.isend = false;
	TupleKeySlice source_key = build_key(initkey);
		
	RangeDesc targetRange = findUpRangeRoute(source_key);
	ReplicaID leaderID = targetRange.replica[0]->LeaderReplicaID;
	SegmentID targetID = targetRange.replica[leaderID]->segmentID;
	*rangeid = targetRange.rangeID;
	MemoryContextSwitchTo(oldContext);

	return targetID;
}


void
doSendEndOfStream(Motion *motion, MotionState *node)
{
	/*
	 * We have no more child tuples, but we have not successfully sent an
	 * End-of-Stream token yet.
	 */
	SendEndOfStream(node->ps.state->motionlayer_context,
					node->ps.state->interconnect_context,
					motion->motionID);
	node->sentEndOfStream = true;
}

static List* 
generateTypeOidList(List* hashExprs)
{
	/* Build list of hash key expression data types. */
	List *type_list = NIL;
	if (hashExprs)
	{
		List	   *eq = list_make1(makeString("="));
		ListCell   *cell;

		foreach(cell, hashExprs)
		{
			Node	   *expr = (Node *) lfirst(cell);
			Oid			typeoid = exprType(expr);
			Oid			eqopoid;
			Oid			lefttype;
			Oid			righttype;

			/* Get oid of the equality operator for this data type. */
			eqopoid = compatible_oper_opid(eq, typeoid, typeoid, true);
			if (eqopoid == InvalidOid)
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("no equality operator for typid %d",
									typeoid)));

			/* Get the equality operator's operand type. */
			op_input_types(eqopoid, &lefttype, &righttype);
			Assert(lefttype == righttype);

			/* If this type is a domain type, get its base type. */
			if (get_typtype(lefttype) == 'd')
				lefttype = getBaseType(lefttype);
			type_list = lappend_oid(type_list, lefttype);
		}
		list_free_deep(eq);
	}
	return type_list;
}

/*
 * A crufty confusing part of the current code is how contentId is used within
 * the motion structures and then how that gets translated to targetRoutes by
 * this motion nodes.
 *
 * WARNING: There are ALOT of assumptions in here about how the motion node
 *			instructions are encoded into motion and stuff.
 *
 * There are 3 types of sending that can happen here:
 *
 *	FIXED - sending to a single process.  the value in node->fixedSegIdxMask[0]
 *			is the contentId of who to send to.  But we can actually ignore that
 *			since now with slice tables, we should only have a single CdbProcess
 *			that we could send to for this motion node.
 *
 *
 *	BROADCAST - actually a subcase of FIXED, but handling is simple. send to all
 *				of our routes.
 *
 *	HASH -	maps hash values to segid.	this mapping is 1->1 such that a hash
 *			value of 2 maps to contentid of 2 (for now).  Since we can't ever
 *			use Hash to send to the QD, the QD's contentid of -1 is not an issue.
 *			Also, the contentid maps directly to the routeid.
 *
 */
void
doSendTuple(Motion *motion, MotionState *node, TupleTableSlot *outerTupleSlot)
{
	int16		targetRoute;
	SendReturnCode sendRC;
	ExprContext *econtext = node->ps.ps_ExprContext;
	TupleKeySlice key = {0};
	RangeID rangeid = 0;
	/* We got a tuple from the child-plan. */
	node->numTuplesFromChild++;

	if (motion->motionType == MOTIONTYPE_FIXED)
	{
		if (motion->isBroadcast)	/* Broadcast */
		{
			targetRoute = BROADCAST_SEGIDX;
		}
		else					/* Fixed Motion. */
		{
			/*
			 * Actually, since we can only send to a single output segment
			 * here, we are guaranteed that we only have a single targetRoute
			 * setup that we could possibly send to.  So we can cheat and just
			 * fix the targetRoute to 0 (the 1st route).
			 */
			targetRoute = 0;
		}
	}
	else if (motion->motionType == MOTIONTYPE_HASH) /* Redistribute */
	{
		uint32		hval = 0;

		econtext->ecxt_outertuple = outerTupleSlot;

		hval = evalHashKey(econtext, node->hashExprs, node->cdbhash);

#ifdef USE_ASSERT_CHECKING
		if (node->ps.state->es_plannedstmt->planGen == PLANGEN_PLANNER)
		{
			Assert(hval < node->ps.plan->flow->numsegments &&
				   "redistribute destination outside segment array");
		}
		else
		{
			Assert(hval < getgpsegmentCount() &&
				   "redistribute destination outside segment array");
		}
#endif							/* USE_ASSERT_CHECKING */

		/*
		 * hashSegIdx takes our uint32 and maps it to an int, and here we
		 * assign it to an int16. See below.
		 */
		targetRoute = hval;

		/*
		 * see MPP-2099, let's not run into this one again! NOTE: the
		 * definition of BROADCAST_SEGIDX is key here, it *cannot* be a valid
		 * route which our map (above) will *ever* return.
		 *
		 * Note the "mapping" is generated at *planning* time in
		 * makeDefaultSegIdxArray() in cdbmutate.c (it is the trivial map, and
		 * is passed around our system a fair amount!).
		 */
		Assert(targetRoute != BROADCAST_SEGIDX);
	}
	else if (motion->motionType == MOTIONTYPE_RANGE) /* range */
	{
		uint32		rval = 0;

		econtext->ecxt_outertuple = outerTupleSlot;

		RangeTblEntry rangetbl = *(RangeTblEntry*)list_nth(node->ps.state->es_range_table, 0);
		Oid rel_id = rangetbl.relid;
		Relation rel = heap_open(rel_id, AccessShareLock);

		List *typelist = generateTypeOidList(motion->hashExprs);

		rval = evalRangeKey(econtext, node->hashExprs,
				typelist, rel, &rangeid);

		heap_close(rel, AccessShareLock);

//#ifdef USE_ASSERT_CHECKING
		if (rval >= getgpsegmentCount())
		{
			ereport(ERROR,
				(errmsg("redistribute destination outside segment array. target id = %d", rval)));
		}
		Assert(rval < getgpsegmentCount() && "redistribute destination outside segment array");
//#endif							/* USE_ASSERT_CHECKING */

		/*
		 * hashSegIdx takes our uint32 and maps it to an int, and here we
		 * assign it to an int16. See below.
		 */
		targetRoute = rval;

		/*
		 * see MPP-2099, let's not run into this one again! NOTE: the
		 * definition of BROADCAST_SEGIDX is key here, it *cannot* be a valid
		 * route which our map (above) will *ever* return.
		 *
		 * Note the "mapping" is generated at *planning* time in
		 * makeDefaultSegIdxArray() in cdbmutate.c (it is the trivial map, and
		 * is passed around our system a fair amount!).
		 */
		Assert(targetRoute != BROADCAST_SEGIDX);
	}
	else						/* ExplicitRedistribute */
	{
		Datum		segidColIdxDatum;

		Assert(motion->segidColIdx > 0 && motion->segidColIdx <= list_length((motion->plan).targetlist));
		bool		is_null = false;

		segidColIdxDatum = slot_getattr(outerTupleSlot, motion->segidColIdx, &is_null);
		targetRoute = Int32GetDatum(segidColIdxDatum);
		Assert(!is_null);
	}

	key = outerTupleSlot->PRIVATE_tts_key;

	CheckAndSendRecordCache(node->ps.state->motionlayer_context,
							node->ps.state->interconnect_context,
							motion->motionID,
							targetRoute);

	/* send the tuple out. */
	sendRC = SendTuple(node->ps.state->motionlayer_context,
					   node->ps.state->interconnect_context,
					   motion->motionID,
					   key,
					   rangeid,
					   outerTupleSlot,
					   targetRoute);

	Assert(sendRC == SEND_COMPLETE || sendRC == STOP_SENDING);
	if (sendRC == SEND_COMPLETE)
		node->numTuplesToAMS++;
	else
		node->stopRequested = true;


#ifdef CDB_MOTION_DEBUG
	if (sendRC == SEND_COMPLETE && node->numTuplesToAMS <= 20)
	{
		StringInfoData buf;

		initStringInfo(&buf);
		appendStringInfo(&buf, "   motion%-3d snd->%-3d, %5d.",
						 motion->motionID,
						 targetRoute,
						 node->numTuplesToAMS);
		formatTuple(&buf, ExecFetchSlotGenericTuple(outerTupleSlot),
					ExecGetResultType(&node->ps),
					node->outputFunArray);
		elog(DEBUG3, buf.data);
		pfree(buf.data);
	}
#endif
}


/*
 * ExecReScanMotion
 *
 * Motion nodes do not allow rescan after a tuple has been fetched.
 *
 * When the planner knows that a NestLoop cannot have more than one outer
 * tuple, it can omit the usual Materialize operator atop the inner subplan,
 * which can lead to invocation of ExecReScanMotion before the motion node's
 * first tuple is fetched.  Rescan can be implemented as a no-op in this case.
 * (After ExecNestLoop fetches an outer tuple, it invokes rescan on the inner
 * subplan before fetching the first inner tuple.  That doesn't bother us,
 * provided there is only one outer tuple.)
 */
void
ExecReScanMotion(MotionState *node)
{
	if (node->mstype != MOTIONSTATE_RECV ||
		node->numTuplesToParent != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("illegal rescan of motion node: invalid plan"),
				 errhint("Likely caused by bad NL-join, try setting enable_nestloop to off")));
	}
	return;
}


/*
 * Mark this node as "stopped." When ExecProcNode() is called on a
 * stopped motion node it should behave as if there are no tuples
 * available.
 *
 * ExecProcNode() on a stopped motion node should also notify the
 * "other end" of the motion node of the stoppage.
 *
 * Note: once this is called, it is possible that the motion node will
 * never be called again, so we *must* send the stop message now.
 */
void
ExecSquelchMotion(MotionState *node)
{
	Motion	   *motion;

	AssertArg(node != NULL);

	motion = (Motion *) node->ps.plan;
	node->stopRequested = true;
	node->ps.state->active_recv_id = -1;

	/* pass down */
	SendStopMessage(node->ps.state->motionlayer_context,
					node->ps.state->interconnect_context,
					motion->motionID);
}
