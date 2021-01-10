/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * In GPDB, this also deals with AppendOnly and AOCS tables.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 */
#include "postgres.h"

#include "access/relscan.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "utils/rel.h"
#include "executor/nodeKVscan.h"

#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbaocsam.h"
#include "utils/snapmgr.h"

#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbaocsam.h"
#include "utils/snapmgr.h"
#include "parser/parsetree.h"

static void InitScanRelation(SeqScanState *node, EState *estate, int eflags, Relation currentRelation);
static TupleTableSlot *SeqNext(SeqScanState *node);

static void InitAOCSScanOpaque(SeqScanState *scanState, Relation currentRelation);

static bool check_flag;

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
SeqNext(SeqScanState *node)
{
	HeapTuple	tuple;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	/*
	 * get the next tuple from the table
	 */
	if (node->ss_currentScanDesc_ao)
	{
		appendonly_getnext(node->ss_currentScanDesc_ao, direction, slot);
	}
	else if (node->ss_currentScanDesc_aocs)
	{
		aocs_getnext(node->ss_currentScanDesc_aocs, direction, slot);
	}
    else if (node->rks_ScanDesc)
    {
        KVScanNext((ScanState*)node);
    }
	else
	{
		HeapScanDesc scandesc = node->ss_currentScanDesc_heap;

		tuple = heap_getnext(scandesc, direction);

		/*
		 * save the tuple and the buffer returned to us by the access methods in
		 * our scan tuple slot and return the slot.  Note: we pass 'false' because
		 * tuples returned by heap_getnext() are pointers onto disk pages and were
		 * not created with palloc() and so should not be pfree()'d.  Note also
		 * that ExecStoreTuple will increment the refcount of the buffer; the
		 * refcount will not be dropped until the tuple table slot is cleared.
		 */
		if (tuple)
			ExecStoreHeapTuple(tuple,	/* tuple to store */
						   slot,	/* slot to store in */
						   scandesc->rs_cbuf,		/* buffer associated with this
													 * tuple */
						   false);	/* don't pfree this pointer */
		else
			ExecClearTuple(slot);
	}

	return slot;
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
SeqRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

static
TupleTableSlot *
KVHisSeqNext(SeqScanState *node)
{
	EState	   *estate;
	ExprContext *econtext;
	ScanDirection direction;

	TupleTableSlot *slot;

	/*
	 * extract necessary information from index scan node
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	/* flip direction if this is an overall backward scan */
	if (ScanDirectionIsBackward(((IndexScan *) node->ss.ps.plan)->indexorderdir))
	{
		if (ScanDirectionIsForward(direction))
			direction = BackwardScanDirection;
		else if (ScanDirectionIsBackward(direction))
			direction = ForwardScanDirection;
	}
	econtext = node->ss.ps.ps_ExprContext;
	slot = node->ss.ss_ScanTupleSlot;
	KVEngineScanDesc rks_ScanDesc = node->rks_ScanDesc;
	rks_ScanDesc->Scantype = node->ss.ps.state->es_plannedstmt->commandType;
	kvengine_history_getnext(rks_ScanDesc, slot);
	
	return slot;
}


/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecSeqScan(SeqScanState *node)
{
	TupleTableSlot *slot = NULL;
	
	if (!check_flag)
		slot = 	ExecScan((ScanState *) node,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck);

	if (TupIsNull(slot))
	{
		Temporal   *temporal;
		Scan  *tableScanNode = (Scan *)node->ss.ps.plan;
		temporal = gettemporal(tableScanNode->scanrelid, node->ss.ps.state->es_range_table);
		if (temporal)
		{
			// if (node->ss_currentScanDesc_heap->xs_snapshot == NULL || node->ss_currentScanDesc_heap->xs_snapshot->satisfies == NULL)
			// 	ereport(LOG,
			// 			(errmsg("SEQ SCAN: no snapshot")));
			
			if (!node->rks_ScanDesc)
			{
				struct tm* tmp_time = (struct tm*)palloc(sizeof(struct tm));
				strptime(temporal->lower, "%Y-%m-%d %H:%M:%S", tmp_time);
				time_t xmin_ts = mktime(tmp_time);

				time_t xmax_ts = xmin_ts;
				if (temporal->upper)
				{
					strptime(temporal->upper, "%Y-%m-%d %H:%M:%S", tmp_time);
					xmax_ts = mktime(tmp_time);
				}
				node->rks_ScanDesc =
						kvengine_his_normal_beginscan(node->ss_currentScanDesc_heap->rs_rd, xmin_ts * 1000000, xmax_ts * 1000000);
				check_flag = true; 
				pfree(tmp_time);
			}

			// TupleTableSlot *slot;
			slot = node->ss.ss_ScanTupleSlot;

			slot = ExecScan(&node->ss,
						(ExecScanAccessMtd) KVHisSeqNext,
						(ExecScanRecheckMtd) SeqRecheck);

			if (TupIsNull(slot))
				check_flag = false;
		}
	}

	return slot;


}

/* ----------------------------------------------------------------
 *		InitScanRelation
 *
 *		Set up to access the scan relation.
 * ----------------------------------------------------------------
 */
static void
InitScanRelation(SeqScanState *node, EState *estate, int eflags, Relation currentRelation)
{
	/* initialize a heapscan */
	if (RelationIsAoRows(currentRelation))
	{
		Snapshot appendOnlyMetaDataSnapshot;

		appendOnlyMetaDataSnapshot = node->ss.ps.state->es_snapshot;
		if (appendOnlyMetaDataSnapshot == SnapshotAny)
		{
			/*
			 * the append-only meta data should never be fetched with
			 * SnapshotAny as bogus results are returned.
			 */
			appendOnlyMetaDataSnapshot = GetTransactionSnapshot();
		}

		node->ss_currentScanDesc_ao = appendonly_beginscan(
			currentRelation,
			node->ss.ps.state->es_snapshot,
			appendOnlyMetaDataSnapshot,
			0, NULL);
	}
	else if (RelationIsAoCols(currentRelation))
	{
		Snapshot appendOnlyMetaDataSnapshot;

		InitAOCSScanOpaque(node, currentRelation);

		appendOnlyMetaDataSnapshot = node->ss.ps.state->es_snapshot;
		if (appendOnlyMetaDataSnapshot == SnapshotAny)
		{
			/*
			 * the append-only meta data should never be fetched with
			 * SnapshotAny as bogus results are returned.
			 */
			appendOnlyMetaDataSnapshot = GetTransactionSnapshot();
		}

		node->ss_currentScanDesc_aocs =
			aocs_beginscan(currentRelation,
						   node->ss.ps.state->es_snapshot,
						   appendOnlyMetaDataSnapshot,
						   NULL /* relationTupleDesc */,
						   node->ss_aocs_proj);
	}
    else if (RelationIsRocksDB(currentRelation))
    {
        node->rks_ScanDesc = kvengine_beginscan(currentRelation,
										   estate->es_snapshot);
        //BeginScanKVRelation((ScanState*)node);
    }
	else
	{
		node->ss_currentScanDesc_heap = heap_beginscan(currentRelation,
										 estate->es_snapshot,
										 0,
										 NULL);
		Temporal   *temporal;
		Scan  *tableScanNode = (Scan *)node->ss.ps.plan;
		temporal = gettemporal(tableScanNode->scanrelid, node->ss.ps.state->es_range_table);
		if (temporal) 
		{
			struct tm* lower_time = (struct tm*)palloc(sizeof(struct tm));
			strptime(temporal->lower, "%Y-%m-%d %H:%M:%S", lower_time);
			time_t lower = mktime(lower_time);
			pfree(lower_time);
			// node->ss_currentScanDesc_heap->rs_pageatatime = false;
			node->ss_currentScanDesc_heap->is_temporal_query = true;
			node->ss_currentScanDesc_heap->lower = lower * 1000000;
			get_pk_oid(node->ss_currentScanDesc_heap->rs_rd);

			if (temporal->upper)
			{
				struct tm* upper_time = (struct tm*)palloc(sizeof(struct tm));
				strptime(temporal->upper, "%Y-%m-%d %H:%M:%S", upper_time);
				time_t upper = mktime(upper_time);
				pfree(upper_time);
				node->ss_currentScanDesc_heap->upper = upper * 1000000;
			}
			else
			{
				node->ss_currentScanDesc_heap->upper = lower * 1000000;
			}
		}
		else
		{
			node->ss_currentScanDesc_heap->is_temporal_query = false;
		}
	}
	node->ss.ss_currentRelation = currentRelation;

	/* and report the scan tuple slot's rowtype */
	ExecAssignScanType(&node->ss, RelationGetDescr(currentRelation));
}


/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState *
ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
	Relation	currentRelation;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scanrelid, eflags);

	return ExecInitSeqScanForPartition(node, estate, eflags, currentRelation);
}

SeqScanState *
ExecInitSeqScanForPartition(SeqScan *node, EState *estate, int eflags,
							Relation currentRelation)
{
	SeqScanState *seqscanstate;
	ScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	seqscanstate = makeNode(SeqScanState);
	scanstate = (ScanState *) seqscanstate;
	scanstate->ps.plan = (Plan *) node;
	scanstate->ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ps);

	/*
	 * initialize child expressions
	 */
	scanstate->ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) scanstate);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &scanstate->ps);
	ExecInitScanTupleSlot(estate, scanstate);

	/*
	 * initialize scan relation
	 */
	InitScanRelation(seqscanstate, estate, eflags, currentRelation);

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ps);
	ExecAssignScanProjectionInfo(scanstate);

	return seqscanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndSeqScan(SeqScanState *node)
{
	Relation	relation;

	/*
	 * get information from node
	 */
	relation = node->ss.ss_currentRelation;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	if (node->ss_currentScanDesc_heap)
	{
		heap_endscan(node->ss_currentScanDesc_heap);
		node->ss_currentScanDesc_heap = NULL;
	}
	if (node->ss_currentScanDesc_ao)
	{
		appendonly_endscan(node->ss_currentScanDesc_ao);
		node->ss_currentScanDesc_ao = NULL;
	}
	if (node->ss_currentScanDesc_aocs)
	{
		aocs_endscan(node->ss_currentScanDesc_aocs);
		node->ss_currentScanDesc_aocs = NULL;
	}
    if (node->rks_ScanDesc)
    {
        kvengine_endscan(node->rks_ScanDesc);
        node->rks_ScanDesc = NULL;
    }
	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSeqScan(SeqScanState *node)
{
	if (node->ss_currentScanDesc_ao)
	{
		appendonly_rescan(node->ss_currentScanDesc_ao,
						  NULL);			/* new scan keys */
	}
	else if (node->ss_currentScanDesc_aocs)
	{
		aocs_rescan(node->ss_currentScanDesc_aocs);
	}
	else if (node->ss_currentScanDesc_heap)
	{
		HeapScanDesc scan;

		scan = node->ss_currentScanDesc_heap;

		heap_rescan(scan,			/* scan desc */
					NULL);			/* new scan keys */
	}
	else
		elog(ERROR, "rescan called without scandesc");
	ExecScanReScan((ScanState *) node);
}

static void
InitAOCSScanOpaque(SeqScanState *scanstate, Relation currentRelation)
{
	/* Initialize AOCS projection info */
	bool	   *proj;
	int			ncol;
	int			i;

	Assert(currentRelation != NULL);

	ncol = currentRelation->rd_att->natts;
	proj = palloc0(ncol * sizeof(bool));
	GetNeededColumnsForScan((Node *) scanstate->ss.ps.plan->targetlist, proj, ncol);
	GetNeededColumnsForScan((Node *) scanstate->ss.ps.plan->qual, proj, ncol);

	for (i = 0; i < ncol; i++)
	{
		if (proj[i])
			break;
	}

	/*
	 * In some cases (for example, count(*)), no columns are specified.
	 * We always scan the first column.
	 */
	if (i == ncol)
		proj[0] = true;

	scanstate->ss_aocs_ncol = ncol;
	scanstate->ss_aocs_proj = proj;
}
