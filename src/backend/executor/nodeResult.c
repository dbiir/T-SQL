/*-------------------------------------------------------------------------
 *
 * nodeResult.c
 *	  support for constant nodes needing special code.
 *
 * DESCRIPTION
 *
 *		Result nodes are used in queries where no relations are scanned.
 *		Examples of such queries are:
 *
 *				select 1 * 2
 *
 *				insert into emp values ('mike', 15000)
 *
 *		(Remember that in an INSERT or UPDATE, we need a plan tree that
 *		generates the new rows.)
 *
 *		Result nodes are also used to optimise queries with constant
 *		qualifications (ie, quals that do not depend on the scanned data),
 *		such as:
 *
 *				select * from emp where 2 > 1
 *
 *		In this case, the plan generated is
 *
 *						Result	(with 2 > 1 qual)
 *						/
 *				   SeqScan (emp.*)
 *
 *		At runtime, the Result node evaluates the constant qual once,
 *		which is shown by EXPLAIN as a One-Time Filter.  If it's
 *		false, we can return an empty result set without running the
 *		controlled plan at all.  If it's true, we run the controlled
 *		plan normally and pass back the results.
 *
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeResult.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeResult.h"
#include "utils/memutils.h"

#include "catalog/pg_type.h"
#include "utils/lsyscache.h"

#include "cdb/cdbhash.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/memquota.h"
#include "executor/spi.h"
#include "tdb/range.h"
#include "tdb/storage_param.h"
#include "tdb/kv_universal.h"
static TupleTableSlot *NextInputSlot(ResultState *node);
static bool TupleMatchesHashFilter(ResultState *node, TupleTableSlot *resultSlot);
static bool TupleMatchesRangeFilter(ResultState *node, TupleTableSlot *resultSlot, Relation rel);
/**
 * Returns the next valid input tuple from the left subtree
 */
static TupleTableSlot *NextInputSlot(ResultState *node)
{
	Assert(outerPlanState(node));

	TupleTableSlot *inputSlot = NULL;

	while (!inputSlot)
	{
		PlanState  *outerPlan = outerPlanState(node);

		TupleTableSlot *candidateInputSlot = ExecProcNode(outerPlan);

		if (TupIsNull(candidateInputSlot))
		{
			/**
			 * No more input tuples.
			 */
			break;
		}

		ExprContext *econtext = node->ps.ps_ExprContext;

		/*
		 * Reset per-tuple memory context to free any expression evaluation
		 * storage allocated in the previous tuple cycle.  Note this can't happen
		 * until we're done projecting out tuples from a scan tuple.
		 */
		ResetExprContext(econtext);

		econtext->ecxt_outertuple = candidateInputSlot;

		/**
		 * Extract out qual in case result node is also performing filtering.
		 */
		List *qual = node->ps.qual;
		bool passesFilter = !qual || ExecQual(qual, econtext, false);

		if (passesFilter)
		{
			inputSlot = candidateInputSlot;
		}
	}

	return inputSlot;

}

/* ----------------------------------------------------------------
 *		ExecResult(node)
 *
 *		returns the tuples from the outer plan which satisfy the
 *		qualification clause.  Since result nodes with right
 *		subtrees are never planned, we ignore the right subtree
 *		entirely (for now).. -cim 10/7/89
 *
 *		The qualification containing only constant clauses are
 *		checked first before any processing is done. It always returns
 *		'nil' if the constant qualification is not satisfied.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecResult(ResultState *node)
{
	ExprContext *econtext;

	econtext = node->ps.ps_ExprContext;

	/*
	 * check constant qualifications like (2 > 1), if not already done
	 */
	if (node->rs_checkqual)
	{
		bool		qualResult = ExecQual((List *) node->resconstantqual,
										  econtext,
										  false);

		node->rs_checkqual = false;
		if (!qualResult)
			return NULL;
	}

	TupleTableSlot *outputSlot = NULL;

	while (!outputSlot)
	{
		TupleTableSlot *candidateOutputSlot = NULL;

		/*
		 * Check to see if we're still projecting out tuples from a previous scan
		 * tuple (because there is a function-returning-set in the projection
		 * expressions).  If so, try to project another one.
		 */
		if (node->isSRF && node->lastSRFCond == ExprMultipleResult)
		{
			ExprDoneCond isDone;

			candidateOutputSlot = ExecProject(node->ps.ps_ProjInfo, &isDone);

			Assert(isDone != ExprSingleResult);
			node->lastSRFCond = isDone;
		}

		/**
		 * If we did not find an input slot yet, we need to return from the outer plan node.
		 */
		if (TupIsNull(candidateOutputSlot) && outerPlanState(node))
		{
			TupleTableSlot *inputSlot = NextInputSlot(node);

			if (TupIsNull(inputSlot))
			{
				/**
				 * Did not find an input tuple. No point going further.
				 */
				break;
			}

			/*
			 * Reset per-tuple memory context to free any expression evaluation
			 * storage allocated in the previous tuple cycle.  Note this can't happen
			 * until we're done projecting out tuples from a scan tuple.
			 */
			ResetExprContext(econtext);

			econtext->ecxt_outertuple = inputSlot;

			ExprDoneCond isDone;

			/*
			 * form the result tuple using ExecProject(), and return it --- unless
			 * the projection produces an empty set, in which case we must loop
			 * back to see if there are more outerPlan tuples.
			 */
			candidateOutputSlot = ExecProject(node->ps.ps_ProjInfo, &isDone);
			if (isDone != ExprSingleResult)
			{
				node->isSRF = true;
				node->lastSRFCond = isDone;
			}
		}
		else if (TupIsNull(candidateOutputSlot) && !outerPlanState(node) && !(node->inputFullyConsumed))
		{
			ExprDoneCond isDone;

			/*
			 * form the result tuple using ExecProject(), and return it --- unless
			 * the projection produces an empty set, in which case we must loop
			 * back to see if there are more outerPlan tuples.
			 */
			candidateOutputSlot = ExecProject(node->ps.ps_ProjInfo, &isDone);
			node->inputFullyConsumed = true;
			if (isDone != ExprSingleResult)
			{
				node->isSRF = true;
				node->lastSRFCond = isDone;
			}
		}

		if (!TupIsNull(candidateOutputSlot))
		{

			Size table_num = list_length(node->ps.state->es_range_table);
			if (enable_range_distribution && table_num >= 1)
			{
				RangeTblEntry rangetbl = *(RangeTblEntry*)list_nth(node->ps.state->es_range_table, 0);
				Oid rel_id = rangetbl.relid;
				Relation rel = heap_open(rel_id, AccessShareLock);
				if (RelationIsRocksDB(rel) &&
					TupleMatchesRangeFilter(node, candidateOutputSlot, rel))
				{
					outputSlot = candidateOutputSlot;
				}
				heap_close(rel, AccessShareLock);
			}

			if (TupleMatchesHashFilter(node, candidateOutputSlot))
			{
				outputSlot = candidateOutputSlot;
			}

		}

		/*
		 * Under these conditions, we don't expect to find any more tuples.
		 */
		if (TupIsNull(candidateOutputSlot)
				&& (!node->isSRF
						|| (node->isSRF && node->inputFullyConsumed)
					)
			)
		{
			break;
		}
	}

	return outputSlot;
}

/**
 * Returns true if tuple matches hash filter.
 */
static bool
TupleMatchesHashFilter(ResultState *node, TupleTableSlot *resultSlot)
{
	Result	   *resultNode = (Result *)node->ps.plan;
	bool		res = true;

	Assert(resultNode);
	Assert(!TupIsNull(resultSlot));

	if (node->hashFilter)
	{
		int			i;

		cdbhashinit(node->hashFilter);
		for (i = 0; i < resultNode->numHashFilterCols; i++)
		{
			int			attnum = resultNode->hashFilterColIdx[i];
			Datum		hAttr;
			bool		isnull;

			hAttr = slot_getattr(resultSlot, attnum, &isnull);

			cdbhash(node->hashFilter, i + 1, hAttr, isnull);
		}

		int targetSeg = cdbhashreduce(node->hashFilter);

		res = (targetSeg == GpIdentity.segindex);
	}

	return res;
}

/*
 * Returns true if range/region id can be found.
 * Look for the structure of rangedesc from kv, compare the startkey
 * and endkey in the structure with the target key, and insert.
 */
static bool TupleMatchesRangeFilter(ResultState *node, TupleTableSlot *resultSlot, Relation rel)
{
	Result *resultNode = (Result *)node->ps.plan;

	Assert(resultNode);
	Assert(!TupIsNull(resultSlot));
	if (node->hashFilter)
	{
		Assert(node->hashFilter);

		Oid pk_oid = get_pk_oid(rel);
		int pk_natts = 0;
		int *pk_colnos = get_index_colnos(pk_oid, &pk_natts);
		Oid *pk_type = palloc0(pk_natts * sizeof(Oid));
		Datum *pk_values = palloc0(pk_natts * sizeof(Datum));
		bool *pkisnull = palloc0(pk_natts * sizeof(bool));

		DirectDispatchInfo dispatch = node->ps.plan->directDispatch;
		RangeID rangeid = dispatch.rangeid;
		for (int i = 0; i < pk_natts; i++)
		{
            for (i = 0; i < resultNode->numHashFilterCols; i++)
            {
                int			attnum = resultNode->hashFilterColIdx[i];
                Datum		hAttr;
                bool		isnull;
                Oid			att_type;
				Assert(attnum > 0);
				if (attnum != pk_colnos[i])
					continue;

                hAttr = slot_getattr(resultSlot, attnum, &isnull);
				pkisnull[i] = isnull;
				pk_values[i] = hAttr;

                if (!isnull)
				{
					att_type = resultSlot->tts_tupleDescriptor->attrs[attnum - 1]->atttypid;

					if (get_typtype(att_type) == 'd')
						att_type = getBaseType(att_type);

					/* CdbHash treats all array-types as ANYARRAYOID, it doesn't know how to hash
					* the individual types (why is this ?) */
					/*if (typeIsArrayType(att_type))
						att_type = ANYARRAYOID;*/
					pk_type[i] = att_type;

				}
            }
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

		RangeDesc range = findUpRangeDescByID(rangeid);
		/* Determine if range found it */
		if (range.replica == NULL)
		{
			return false;
		}

		int startresult = memcmp(source_key.data, range.startkey.data, source_key.len);
		int endresult = memcmp(source_key.data, range.endkey.data, source_key.len);
		resultSlot->tts_rangeid = rangeid;
		bool isleader = false;
		Replica replica = findUpReplicaOnThisSeg(range, &isleader);
		if (!isleader)
		{
			elog(ERROR, "insert cannot handle in a replica which is not the leader!");
		}
		else if (startresult >= 0 && endresult <= 0 && replica->replica_state == leader_working)
		{
			return true;
		}
	}
	return true;
}

/* ----------------------------------------------------------------
 *		ExecResultMarkPos
 * ----------------------------------------------------------------
 */
void
ExecResultMarkPos(ResultState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	if (outerPlan != NULL)
		ExecMarkPos(outerPlan);
	else
		elog(DEBUG2, "Result nodes do not support mark/restore");
}

/* ----------------------------------------------------------------
 *		ExecResultRestrPos
 * ----------------------------------------------------------------
 */
void
ExecResultRestrPos(ResultState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	if (outerPlan != NULL)
		ExecRestrPos(outerPlan);
	else
		elog(ERROR, "Result nodes do not support mark/restore");
}

/* ----------------------------------------------------------------
 *		ExecInitResult
 *
 *		Creates the run-time state information for the result node
 *		produced by the planner and initializes outer relations
 *		(child nodes).
 * ----------------------------------------------------------------
 */
ResultState *
ExecInitResult(Result *node, EState *estate, int eflags)
{
	ResultState *resstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) ||
		   outerPlan(node) != NULL);

	/*
	 * create state structure
	 */
	resstate = makeNode(ResultState);
	resstate->ps.plan = (Plan *) node;
	resstate->ps.state = estate;

	resstate->inputFullyConsumed = false;
	resstate->rs_checkqual = (node->resconstantqual == NULL) ? false : true;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &resstate->ps);

	resstate->isSRF = false;

	/*resstate->ps.ps_TupFromTlist = false;*/

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &resstate->ps);

	/*
	 * initialize child expressions
	 */
	resstate->ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) resstate);
	resstate->ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) resstate);
	resstate->resconstantqual = ExecInitExpr((Expr *) node->resconstantqual,
											 (PlanState *) resstate);

	/*
	 * initialize child nodes
	 */
	outerPlanState(resstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * we don't use inner plan
	 */
	Assert(innerPlan(node) == NULL);

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&resstate->ps);
	ExecAssignProjectionInfo(&resstate->ps, NULL);

	/*
	 * initialize hash filter
	 */
	if (node->numHashFilterCols > 0)
	{
		int			numSegments;

		if (resstate->ps.state->es_plannedstmt->planGen == PLANGEN_PLANNER)
		{
			Assert(node->plan.flow->numsegments > 0);

			/*
			 * For planner generated plan the size of receiver slice can be
			 * determined from flow.
			 */
			numSegments = node->plan.flow->numsegments;
		}
		else
		{
			/*
			 * For ORCA generated plan we could distribute to ALL as partially
			 * distributed tables are not supported by ORCA yet.
			 */
			numSegments = getgpsegmentCount();
		}

		resstate->hashFilter = makeCdbHash(numSegments, node->numHashFilterCols, node->hashFilterFuncs);
	}

	if (!IsResManagerMemoryPolicyNone()
			&& IsResultMemoryIntensive(node))
	{
		SPI_ReserveMemory(((Plan *)node)->operatorMemKB * 1024L);
	}

	return resstate;
}

/* ----------------------------------------------------------------
 *		ExecEndResult
 *
 *		frees up storage allocated through C routines
 * ----------------------------------------------------------------
 */
void
ExecEndResult(ResultState *node)
{
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	/*
	 * shut down subplans
	 */
	ExecEndNode(outerPlanState(node));

	EndPlanStateGpmonPkt(&node->ps);

}

void
ExecReScanResult(ResultState *node)
{
	node->inputFullyConsumed = false;
	node->isSRF = false;
	node->rs_checkqual = (node->resconstantqual == NULL) ? false : true;

	/*
	 * If chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree &&
		node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}
