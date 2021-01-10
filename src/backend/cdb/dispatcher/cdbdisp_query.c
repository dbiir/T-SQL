/*-------------------------------------------------------------------------
 *
 * cdbdisp_query.c
 *	  Functions to dispatch command string or plan to QExecutors.
 *
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/dispatcher/cdbdisp_query.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbmutate.h"
#include "cdb/cdbsrlz.h"
#include "cdb/tupleremap.h"
#include "nodes/execnodes.h"
#include "tcop/tcopprot.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/faultinjector.h"
#include "utils/resgroup.h"
#include "utils/resource_manager.h"
#include "utils/session_state.h"
#include "utils/typcache.h"
#include "miscadmin.h"

#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdisp_dtx.h"	/* for qdSerializeDtxContextInfo() */
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbcopy.h"
#include "cdb/cdbdtxts.h"
#include "executor/execUtils.h"

#define QUERY_STRING_TRUNCATE_SIZE (1024)

extern bool Test_print_direct_dispatch_info;

/*
 * We need an array describing the relationship between a slice and
 * the number of "child" slices which depend on it.
 */
typedef struct
{
	int			sliceIndex;
	int			children;
	Slice	   *slice;
} SliceVec;

/*
 * Parameter structure for Greenplum Database Queries
 */
typedef struct DispatchCommandQueryParms
{
	/*
	 * The SQL command
	 */
	const char *strCommand;
	int			strCommandlen;
	char	   *serializedQuerytree;
	int			serializedQuerytreelen;
	char	   *serializedPlantree;
	int			serializedPlantreelen;
	char	   *serializedQueryDispatchDesc;
	int			serializedQueryDispatchDesclen;
	char	   *serializedParams;
	int			serializedParamslen;

	/*
	 * Additional information.
	 */
	char	   *serializedOidAssignments;
	int			serializedOidAssignmentslen;

	/*
	 * serialized DTX context string
	 */
	char	   *serializedDtxContextInfo;
	int			serializedDtxContextInfolen;

    char       *serializedDtxTS;
    int         serializedDtxTSlen;
} DispatchCommandQueryParms;

static int fillSliceVector(SliceTable *sliceTable,
				int sliceIndex,
				SliceVec *sliceVector,
				int len);

static char *buildGpQueryString(DispatchCommandQueryParms *pQueryParms,
				   int *finalLen);

static DispatchCommandQueryParms *cdbdisp_buildPlanQueryParms(struct QueryDesc *queryDesc, bool planRequiresTxn);
static DispatchCommandQueryParms *cdbdisp_buildUtilityQueryParms(struct Node *stmt, int flags, List *oid_assignments);
static DispatchCommandQueryParms *cdbdisp_buildCommandQueryParms(const char *strCommand, int flags);

static void cdbdisp_dispatchCommandInternal(DispatchCommandQueryParms *pQueryParms,
											int flags, List *segments,
											CdbPgResults *cdb_pgresults);

static void
cdbdisp_dispatchX(QueryDesc *queryDesc,
			bool planRequiresTxn,
			bool cancelOnError);

static char *serializeParamListInfo(ParamListInfo paramLI, int *len_p);

static List * formIdleSegmentIdList(void);
/*
 * Compose and dispatch the MPPEXEC commands corresponding to a plan tree
 * within a complete parallel plan. (A plan tree will correspond either
 * to an initPlan or to the main plan.)
 *
 * If cancelOnError is true, then any dispatching error, a cancellation
 * request from the client, or an error from any of the associated QEs,
 * may cause the unfinished portion of the plan to be abandoned or canceled;
 * and in the event this occurs before all gangs have been dispatched, this
 * function does not return, but waits for all QEs to stop and exits to
 * the caller's error catcher via ereport(ERROR,...). Otherwise this
 * function returns normally and errors are not reported until later.
 *
 * If cancelOnError is false, the plan is to be dispatched as fully as
 * possible and the QEs allowed to proceed regardless of cancellation
 * requests, errors or connection failures from other QEs, etc.
 *
 * The CdbDispatchResults objects allocated for the plan are returned
 * in *pPrimaryResults. The caller, after calling
 * CdbCheckDispatchResult(), can examine the CdbDispatchResults
 * objects, can keep them as long as needed, and ultimately must free
 * them with cdbdisp_destroyDispatcherState() prior to deallocation of
 * the caller's memory context. Callers should use PG_TRY/PG_CATCH to
 * ensure proper cleanup.
 *
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 *
 * Note that the slice tree dispatched is the one specified in the EState
 * of the argument QueryDesc as es_cur__slice.
 *
 * Note that the QueryDesc params must include PARAM_EXEC_REMOTE parameters
 * containing the values of any initplans required by the slice to be run.
 * (This is handled by calls to addRemoteExecParamsToParamList() from the
 * functions preprocess_initplans() and ExecutorRun().)
 *
 * Each QE receives its assignment as a message of type 'M' in PostgresMain().
 * The message is deserialized and processed by exec_mpp_query() in postgres.c.
 */
void
CdbDispatchPlan(struct QueryDesc *queryDesc,
				bool planRequiresTxn,
				bool cancelOnError)
{
	PlannedStmt *stmt;
	bool		is_SRI = false;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(queryDesc != NULL && queryDesc->estate != NULL);

	/*
	 * This function is called only for planned statements.
	 */
	stmt = queryDesc->plannedstmt;
	Assert(stmt);

	/*
	 * Let's evaluate STABLE functions now, so we get consistent values on the
	 * QEs
	 *
	 * Also, if this is a single-row INSERT statement, let's evaluate
	 * nextval() and currval() now, so that we get the QD's values, and a
	 * consistent value for everyone
	 */
	if (queryDesc->operation == CMD_INSERT)
	{
		Assert(stmt->commandType == CMD_INSERT);

		/*
		 * We might look for constant input relation (instead of SRI), but I'm
		 * afraid that wouldn't scale.
		 */
		is_SRI = IsA(stmt->planTree, Result) &&stmt->planTree->lefttree == NULL;
	}

	if (queryDesc->operation == CMD_INSERT ||
		queryDesc->operation == CMD_SELECT ||
		queryDesc->operation == CMD_UPDATE ||
		queryDesc->operation == CMD_DELETE)
	{
		List	   *cursors;

		/*
		 * Need to be careful not to modify the original PlannedStmt, because
		 * it might be a cached plan. So make a copy. A shallow copy of the
		 * fields we don't modify should be enough.
		 */
		stmt = palloc(sizeof(PlannedStmt));
		memcpy(stmt, queryDesc->plannedstmt, sizeof(PlannedStmt));
		stmt->subplans = list_copy(stmt->subplans);

		stmt->planTree = (Plan *) exec_make_plan_constant(stmt, queryDesc->estate, is_SRI, &cursors);
		queryDesc->plannedstmt = stmt;

		queryDesc->ddesc->cursorPositions = (List *) copyObject(cursors);
	}

	/*
	 * Cursor queries and bind/execute path queries don't run on the
	 * writer-gang QEs; but they require snapshot-synchronization to get
	 * started.
	 *
	 * initPlans, and other work (see the function pre-evaluation above) may
	 * advance the snapshot "segmateSync" value, so we're best off setting the
	 * shared-snapshot-ready value here. This will dispatch to the writer gang
	 * and force it to set its snapshot; we'll then be able to serialize the
	 * same snapshot version (see qdSerializeDtxContextInfo() below).
	 */
	if (queryDesc->extended_query)
	{
		verify_shared_snapshot_ready();
	}

	cdbdisp_dispatchX(queryDesc, planRequiresTxn, cancelOnError);
}

/*
 * Special for sending SET commands that change GUC variables, so they go to all
 * gangs, both reader and writer
 *
 * Can not dispatch SET commands to busy reader gangs (allocated by cursors) directly because another
 * command is already in progress.
 * Cursors only allocate reader gangs, so primary writer and idle reader gangs can be dispatched to.
 */
void
CdbDispatchSetCommand(const char *strCommand, bool cancelOnError)
{
	CdbDispatcherState *ds;
	DispatchCommandQueryParms *pQueryParms;
	Gang *primaryGang;
	char	   *queryText;
	int		queryTextLength;
	ListCell   *le;
	ErrorData *qeError = NULL;

	elog((Debug_print_full_dtm ? LOG : DEBUG5),
		 "CdbDispatchSetCommand for command = '%s'",
		 strCommand);

	pQueryParms = cdbdisp_buildCommandQueryParms(strCommand, DF_NONE);

	ds = cdbdisp_makeDispatcherState(false);

	queryText = buildGpQueryString(pQueryParms, &queryTextLength);

	primaryGang = AllocateGang(ds, GANGTYPE_PRIMARY_WRITER, cdbcomponent_getCdbComponentsList());

	/* put all idle segment to a gang so QD can send SET command to them */
	AllocateGang(ds, GANGTYPE_PRIMARY_READER, formIdleSegmentIdList());
	
	cdbdisp_makeDispatchResults(ds, list_length(ds->allocatedGangs), cancelOnError);
	cdbdisp_makeDispatchParams (ds, list_length(ds->allocatedGangs), queryText, queryTextLength);

	foreach(le, ds->allocatedGangs)
	{
		Gang	   *rg = lfirst(le);

		cdbdisp_dispatchToGang(ds, rg, -1);
	}
	addToGxactTwophaseSegments(primaryGang);

	/*
	 * No need for two-phase commit, so no need to call
	 * addToGxactTwophaseSegments.
	 */

	cdbdisp_waitDispatchFinish(ds);

	cdbdisp_checkDispatchResult(ds, DISPATCH_WAIT_NONE);

	cdbdisp_getDispatchResults(ds, &qeError);

	cdbdisp_destroyDispatcherState(ds);

	/*
	 * For named portal (like CURSOR), SET command will not be
	 * dispatched. Meanwhile such gang should not be reused because
	 * it's guc was not set.
	 */
	cdbdisp_markNamedPortalGangsDestroyed();

	if (qeError)
	{
		ReThrowError(qeError);
	}
}

/*
 * CdbDispatchCommand:
 *
 * Execute plain command on all primary writer QEs.
 * If one or more QEs got error, throw a Error.
 *
 * -flags:
 *  Is the combination of DF_NEED_TWO_PHASE, DF_WITH_SNAPSHOT,DF_CANCEL_ON_ERROR
 */
void
CdbDispatchCommand(const char *strCommand,
				   int flags,
				   CdbPgResults *cdb_pgresults)
{
	return CdbDispatchCommandToSegments(strCommand,
										flags,
										cdbcomponent_getCdbComponentsList(),
										cdb_pgresults);
}

/*
 * Like CdbDispatchCommand, but sends the command only to the
 * specified segments.
 */
void
CdbDispatchCommandToSegments(const char *strCommand,
							 int flags,
							 List *segments,
							 CdbPgResults *cdb_pgresults)
{
	DispatchCommandQueryParms *pQueryParms;
	bool needTwoPhase = flags & DF_NEED_TWO_PHASE;

	if (needTwoPhase)
		setupTwoPhaseTransaction();

	elogif((Debug_print_full_dtm || log_min_messages <= DEBUG5), LOG,
		   "CdbDispatchCommand: %s (needTwoPhase = %s)",
		   strCommand, (needTwoPhase ? "true" : "false"));

	pQueryParms = cdbdisp_buildCommandQueryParms(strCommand, flags);

	return cdbdisp_dispatchCommandInternal(pQueryParms,
										   flags,
										   segments,
										   cdb_pgresults);
}

/*
 * CdbDispatchUtilityStatement
 *
 * Dispatch an already parsed statement to all primary writer QEs, wait until
 * all QEs finished successfully. If one or more QEs got error,
 * throw an Error.
 *
 * -flags:
 *      Is the combination of DF_NEED_TWO_PHASE, DF_WITH_SNAPSHOT,DF_CANCEL_ON_ERROR
 *
 * -cdb_pgresults:
 *      Indicate whether return the pg_result for each QE connection.
 *
 */
void
CdbDispatchUtilityStatement(struct Node *stmt,
							int flags,
							List *oid_assignments,
							CdbPgResults *cdb_pgresults)
{
	DispatchCommandQueryParms *pQueryParms;
	bool needTwoPhase = flags & DF_NEED_TWO_PHASE;

	if (needTwoPhase)
		setupTwoPhaseTransaction();

	elogif((Debug_print_full_dtm || log_min_messages <= DEBUG5), LOG,
		   "CdbDispatchUtilityStatement: %s (needTwoPhase = %s)",
		   debug_query_string, (needTwoPhase ? "true" : "false"));

	pQueryParms = cdbdisp_buildUtilityQueryParms(stmt, flags, oid_assignments);

	return cdbdisp_dispatchCommandInternal(pQueryParms,
										   flags,
										   cdbcomponent_getCdbComponentsList(),
										   cdb_pgresults);
}

static void
cdbdisp_dispatchCommandInternal(DispatchCommandQueryParms *pQueryParms,
                                int flags,
								List *segments,
                                CdbPgResults *cdb_pgresults)
{
	CdbDispatcherState *ds;
	Gang *primaryGang;
	CdbDispatchResults *pr;
	ErrorData *qeError = NULL;
	char *queryText;
	int queryTextLength;

	/*
	 * Dispatch the command.
	 */
	ds = cdbdisp_makeDispatcherState(false);

	queryText = buildGpQueryString(pQueryParms, &queryTextLength);

	/*
	 * Allocate a primary QE for every available segDB in the system.
	 */
	primaryGang = AllocateGang(ds, GANGTYPE_PRIMARY_WRITER, segments);
	Assert(primaryGang);

	cdbdisp_makeDispatchResults(ds, 1, flags & DF_CANCEL_ON_ERROR);
	cdbdisp_makeDispatchParams (ds, 1, queryText, queryTextLength);

	cdbdisp_dispatchToGang(ds, primaryGang, -1);

	if ((flags & DF_NEED_TWO_PHASE) != 0 || isDtxExplicitBegin())
		addToGxactTwophaseSegments(primaryGang);

	cdbdisp_waitDispatchFinish(ds);

	cdbdisp_checkDispatchResult(ds, DISPATCH_WAIT_NONE);

	pr = cdbdisp_getDispatchResults(ds, &qeError);

	if (qeError)
	{
		cdbdisp_destroyDispatcherState(ds);
		ReThrowError(qeError);
	}

	cdbdisp_returnResults(pr, cdb_pgresults);

	cdbdisp_destroyDispatcherState(ds);
}

static DispatchCommandQueryParms *
cdbdisp_buildCommandQueryParms(const char *strCommand, int flags)
{
	bool needTwoPhase = flags & DF_NEED_TWO_PHASE;
	bool withSnapshot = flags & DF_WITH_SNAPSHOT;
	DispatchCommandQueryParms *pQueryParms;

	pQueryParms = palloc0(sizeof(*pQueryParms));
	pQueryParms->strCommand = strCommand;
	pQueryParms->serializedQuerytree = NULL;
	pQueryParms->serializedQuerytreelen = 0;
	pQueryParms->serializedQueryDispatchDesc = NULL;
	pQueryParms->serializedQueryDispatchDesclen = 0;

	/*
	 * Serialize a version of our DTX Context Info
	 */
	pQueryParms->serializedDtxContextInfo =
		qdSerializeDtxContextInfo(&pQueryParms->serializedDtxContextInfolen,
								  withSnapshot, false,
								  mppTxnOptions(needTwoPhase),
								  "cdbdisp_dispatchCommandInternal");
    pQueryParms->serializedDtxTS = palloc0(DtxTimeStamp_SerializeSize());
    DtxTimeStamp_Serialize(pQueryParms->serializedDtxTS, &global_tmp_timestamp);
    pQueryParms->serializedDtxTSlen = DtxTimeStamp_SerializeSize();
	return pQueryParms;
}

static DispatchCommandQueryParms *
cdbdisp_buildUtilityQueryParms(struct Node *stmt,
				int flags,
				List *oid_assignments)
{
	char *serializedQuerytree = NULL;
	char *serializedQueryDispatchDesc = NULL;
	int serializedQuerytree_len = 0;
	int serializedQueryDispatchDesc_len = 0;
	bool needTwoPhase = flags & DF_NEED_TWO_PHASE;
	bool withSnapshot = flags & DF_WITH_SNAPSHOT;
	QueryDispatchDesc *qddesc;
	Query *q;
	DispatchCommandQueryParms *pQueryParms;

	Assert(stmt != NULL);
	Assert(stmt->type < 1000);
	Assert(stmt->type > 0);

	q = makeNode(Query);

	q->querySource = QSRC_ORIGINAL;
	q->commandType = CMD_UTILITY;
	q->utilityStmt = stmt;

	/*
	 * We must set q->canSetTag = true.  False would be used to hide a command
	 * introduced by rule expansion which is not allowed to return its
	 * completion status in the command tag (PQcmdStatus/PQcmdTuples). For
	 * example, if the original unexpanded command was SELECT, the status
	 * should come back as "SELECT n" and should not reflect other commands
	 * inserted by rewrite rules.  True means we want the status.
	 */
	q->canSetTag = true;

	/*
	 * serialized the stmt tree, and create the sql statement: mppexec ....
	 */
	serializedQuerytree = serializeNode((Node *) q, &serializedQuerytree_len,
										NULL /* uncompressed_size */ );

	Assert(serializedQuerytree != NULL);

	if (oid_assignments)
	{
		qddesc = makeNode(QueryDispatchDesc);
		qddesc->oidAssignments = oid_assignments;

		serializedQueryDispatchDesc = serializeNode((Node *) qddesc, &serializedQueryDispatchDesc_len,
													NULL /* uncompressed_size */ );
	}

	pQueryParms = palloc0(sizeof(*pQueryParms));
	pQueryParms->strCommand = debug_query_string;
	pQueryParms->serializedQuerytree = serializedQuerytree;
	pQueryParms->serializedQuerytreelen = serializedQuerytree_len;
	pQueryParms->serializedQueryDispatchDesc = serializedQueryDispatchDesc;
	pQueryParms->serializedQueryDispatchDesclen = serializedQueryDispatchDesc_len;

	/*
	 * Serialize a version of our DTX Context Info
	 */
	pQueryParms->serializedDtxContextInfo =
		qdSerializeDtxContextInfo(&pQueryParms->serializedDtxContextInfolen,
								  withSnapshot, false,
								  mppTxnOptions(needTwoPhase),
								  "cdbdisp_dispatchCommandInternal");

    pQueryParms->serializedDtxTS = palloc0(DtxTimeStamp_SerializeSize());
    DtxTimeStamp_Serialize(pQueryParms->serializedDtxTS, &global_tmp_timestamp);
    pQueryParms->serializedDtxTSlen = DtxTimeStamp_SerializeSize();
	return pQueryParms;
}

static DispatchCommandQueryParms *
cdbdisp_buildPlanQueryParms(struct QueryDesc *queryDesc,
							bool planRequiresTxn)
{
	char	   *splan,
			   *sddesc,
			   *sparams;

	int			splan_len,
				splan_len_uncompressed,
				sddesc_len,
				sparams_len,
				rootIdx;

	rootIdx = RootSliceIndex(queryDesc->estate);

#ifdef USE_ASSERT_CHECKING
	SliceTable *sliceTbl = queryDesc->estate->es_sliceTable;

	Assert(rootIdx == 0 ||
		   (rootIdx > sliceTbl->nMotions
			&& rootIdx <= sliceTbl->nMotions + sliceTbl->nInitPlans));
#endif

	DispatchCommandQueryParms *pQueryParms = (DispatchCommandQueryParms *) palloc0(sizeof(*pQueryParms));

	/*
	 * serialized plan tree. Note that we're called for a single slice tree
	 * (corresponding to an initPlan or the main plan), so the parameters are
	 * fixed and we can include them in the prefix.
	 */
	splan = serializeNode((Node *) queryDesc->plannedstmt, &splan_len, &splan_len_uncompressed);

	uint64		plan_size_in_kb = ((uint64) splan_len_uncompressed) / (uint64) 1024;

	elog(((gp_log_gang >= GPVARS_VERBOSITY_TERSE) ? LOG : DEBUG1),
		 "Query plan size to dispatch: " UINT64_FORMAT "KB", plan_size_in_kb);

	if (0 < gp_max_plan_size && plan_size_in_kb > gp_max_plan_size)
	{
		ereport(ERROR,
				(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				 (errmsg("Query plan size limit exceeded, current size: "
						 UINT64_FORMAT "KB, max allowed size: %dKB",
						 plan_size_in_kb, gp_max_plan_size),
				  errhint("Size controlled by gp_max_plan_size"))));
	}

	Assert(splan != NULL && splan_len > 0 && splan_len_uncompressed > 0);

	if (queryDesc->params != NULL && queryDesc->params->numParams > 0)
	{
		sparams = serializeParamListInfo(queryDesc->params, &sparams_len);
	}
	else
	{
		sparams = NULL;
		sparams_len = 0;
	}

	sddesc = serializeNode((Node *) queryDesc->ddesc, &sddesc_len, NULL /* uncompressed_size */ );

	pQueryParms->strCommand = queryDesc->sourceText;
	pQueryParms->serializedQuerytree = NULL;
	pQueryParms->serializedQuerytreelen = 0;
	pQueryParms->serializedPlantree = splan;
	pQueryParms->serializedPlantreelen = splan_len;
	pQueryParms->serializedParams = sparams;
	pQueryParms->serializedParamslen = sparams_len;
	pQueryParms->serializedQueryDispatchDesc = sddesc;
	pQueryParms->serializedQueryDispatchDesclen = sddesc_len;

	/*
	 * Serialize a version of our snapshot, and generate our transction
	 * isolations. We generally want Plan based dispatch to be in a global
	 * transaction. The executor gets to decide if the special circumstances
	 * exist which allow us to dispatch without starting a global xact.
	 */
	pQueryParms->serializedDtxContextInfo =
		qdSerializeDtxContextInfo(&pQueryParms->serializedDtxContextInfolen,
								  true /* wantSnapshot */ ,
								  queryDesc->extended_query,
								  mppTxnOptions(planRequiresTxn),
								  "cdbdisp_buildPlanQueryParms");

    pQueryParms->serializedDtxTS = palloc0(DtxTimeStamp_SerializeSize());
    DtxTimeStamp_Serialize(pQueryParms->serializedDtxTS, &global_tmp_timestamp);
    pQueryParms->serializedDtxTSlen = DtxTimeStamp_SerializeSize();

	return pQueryParms;
}

/*
 * Three Helper functions for cdbdisp_dispatchX:
 *
 * Used to figure out the dispatch order for the sliceTable by
 * counting the number of dependent child slices for each slice; and
 * then sorting based on the count (all indepenedent slices get
 * dispatched first, then the slice above them and so on).
 *
 * fillSliceVector: figure out the number of slices we're dispatching,
 * and order them.
 *
 * count_dependent_children(): walk tree counting up children.
 *
 * compare_slice_order(): comparison function for qsort(): order the
 * slices by the number of dependent children. Empty slices are
 * sorted last (to make this work with initPlans).
 *
 */
static int
compare_slice_order(const void *aa, const void *bb)
{
	SliceVec   *a = (SliceVec *) aa;
	SliceVec   *b = (SliceVec *) bb;

	if (a->slice == NULL)
		return 1;
	if (b->slice == NULL)
		return -1;

	/*
	 * Put the slice not going to dispatch in the last
	 */
	if (a->slice->primaryGang == NULL)
	{
		Assert(a->slice->gangType == GANGTYPE_UNALLOCATED);
		return 1;
	}
	if (b->slice->primaryGang == NULL)
	{
		Assert(b->slice->gangType == GANGTYPE_UNALLOCATED);
		return -1;
	}

	/*
	 * sort the writer gang slice first, because he sets the shared snapshot
	 */
	if (a->slice->primaryGang->gang_id == 1)
	{
		Assert(b->slice->primaryGang->gang_id != 1);
		return -1;
	}
	if (b->slice->primaryGang->gang_id == 1)
	{
		return 1;
	}

	/* sort slice with larger size first because it has a bigger chance to contain writers */
	if (a->slice->primaryGang->size > b->slice->primaryGang->size)
		return -1;

	if (a->slice->primaryGang->size < b->slice->primaryGang->size)
		return 1;

	if (a->children == b->children)
		return 0;
	else if (a->children > b->children)
		return 1;
	else
		return -1;
}

/*
 * Quick and dirty bit mask operations
 */
static void
mark_bit(char *bits, int nth)
{
	int			nthbyte = nth >> 3;
	char		nthbit = 1 << (nth & 7);

	bits[nthbyte] |= nthbit;
}

static void
or_bits(char *dest, char *src, int n)
{
	int			i;

	for (i = 0; i < n; i++)
		dest[i] |= src[i];
}

static int
count_bits(char *bits, int nbyte)
{
	int			i;
	int			nbit = 0;

	int			bitcount[] =
	{
		0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4
	};

	for (i = 0; i < nbyte; i++)
	{
		nbit += bitcount[bits[i] & 0x0F];
		nbit += bitcount[(bits[i] >> 4) & 0x0F];
	}

	return nbit;
}

/*
 * We use a bitmask to count the dep. childrens.
 * Because of input sharing, the slices now are DAG. We cannot simply go down the
 * tree and add up number of children, which will return too big number.
 */
static int
markbit_dep_children(SliceTable *sliceTable, int sliceIdx,
					 SliceVec *sliceVec, int bitmasklen, char *bits)
{
	ListCell   *sublist;
	Slice	   *slice = (Slice *) list_nth(sliceTable->slices, sliceIdx);

	foreach(sublist, slice->children)
	{
		int			childIndex = lfirst_int(sublist);
		char	   *newbits = palloc0(bitmasklen);

		markbit_dep_children(sliceTable, childIndex,
							 sliceVec, bitmasklen, newbits);
		or_bits(bits, newbits, bitmasklen);
		mark_bit(bits, childIndex);
		pfree(newbits);
	}

	sliceVec[sliceIdx].sliceIndex = sliceIdx;
	sliceVec[sliceIdx].children = count_bits(bits, bitmasklen);
	sliceVec[sliceIdx].slice = slice;

	return sliceVec[sliceIdx].children;
}

/*
 * Count how many dependent childrens and fill in the sliceVector of dependent childrens.
 */
static int
count_dependent_children(SliceTable *sliceTable, int sliceIndex,
						 SliceVec *sliceVector, int len)
{
	int			ret = 0;
	int			bitmasklen = (len + 7) >> 3;
	char	   *bitmask = palloc0(bitmasklen);

	ret = markbit_dep_children(sliceTable, sliceIndex, sliceVector, bitmasklen, bitmask);
	pfree(bitmask);

	return ret;
}

static int
fillSliceVector(SliceTable *sliceTbl, int rootIdx,
				SliceVec *sliceVector, int nTotalSlices)
{
	int			top_count;

	/*
	 * count doesn't include top slice add 1, note that sliceVector would be
	 * modified in place by count_dependent_children.
	 */
	top_count = 1 + count_dependent_children(sliceTbl, rootIdx, sliceVector, nTotalSlices);

	qsort(sliceVector, nTotalSlices, sizeof(SliceVec), compare_slice_order);

	return top_count;
}

/*
 * Build a query string to be dispatched to QE.
 */
static char *
buildGpQueryString(DispatchCommandQueryParms *pQueryParms,
				   int *finalLen)
{
	const char *command = pQueryParms->strCommand;
	int			command_len;
	const char *querytree = pQueryParms->serializedQuerytree;
	int			querytree_len = pQueryParms->serializedQuerytreelen;
	const char *plantree = pQueryParms->serializedPlantree;
	int			plantree_len = pQueryParms->serializedPlantreelen;
	const char *params = pQueryParms->serializedParams;
	int			params_len = pQueryParms->serializedParamslen;
	const char *sddesc = pQueryParms->serializedQueryDispatchDesc;
	int			sddesc_len = pQueryParms->serializedQueryDispatchDesclen;
	const char *dtxContextInfo = pQueryParms->serializedDtxContextInfo;
	int			dtxContextInfo_len = pQueryParms->serializedDtxContextInfolen;
	int64		currentStatementStartTimestamp = GetCurrentStatementStartTimestamp();
	Oid			sessionUserId = GetSessionUserId();
	Oid			outerUserId = GetOuterUserId();
	Oid			currentUserId = GetUserId();
	int32		numsegments = getgpsegmentCount();
	StringInfoData resgroupInfo;
    char       *serializedDtxTS = pQueryParms->serializedDtxTS;
    int         serializedDtxTSlen = pQueryParms->serializedDtxTSlen;

	int			tmp,
				len;
	uint32		n32;
	int			total_query_len;
	char	   *shared_query,
			   *pos;
	MemoryContext oldContext;

	/*
	 * Must allocate query text within DispatcherContext,
	 */
	Assert(DispatcherContext);
	oldContext = MemoryContextSwitchTo(DispatcherContext);

	/*
	 * If either querytree or plantree is set then the query string is not so
	 * important, dispatch a truncated version to increase the performance.
	 *
	 * Here we only need to determine the truncated size, the actual work is
	 * done later when copying it to the result buffer.
	 */
	if (querytree || plantree)
		command_len = strnlen(command, QUERY_STRING_TRUNCATE_SIZE - 1) + 1;
	else
		command_len = strlen(command) + 1;

	initStringInfo(&resgroupInfo);
	if (IsResGroupActivated())
		SerializeResGroupInfo(&resgroupInfo);

	total_query_len = 1 /* 'M' */ +
		sizeof(len) /* message length */ +
		sizeof(gp_command_count) +
		sizeof(sessionUserId) /* sessionUserIsSuper */ +
		sizeof(outerUserId) /* outerUserIsSuper */ +
		sizeof(currentUserId) +
		sizeof(n32) * 2 /* currentStatementStartTimestamp */ +
		sizeof(command_len) +
		sizeof(querytree_len) +
		sizeof(plantree_len) +
		sizeof(params_len) +
		sizeof(sddesc_len) +
		sizeof(dtxContextInfo_len) +
		dtxContextInfo_len +
		command_len +
		querytree_len +
		plantree_len +
		params_len +
		sddesc_len +
		sizeof(numsegments) +
		sizeof(resgroupInfo.len) +
		resgroupInfo.len + 
        serializedDtxTSlen + 
        sizeof(serializedDtxTSlen);

	shared_query = palloc0(total_query_len);

	pos = shared_query;

	*pos++ = 'M';

	pos += 4;					/* placeholder for message length */

	tmp = htonl(gp_command_count);
	memcpy(pos, &tmp, sizeof(gp_command_count));
	pos += sizeof(gp_command_count);

	tmp = htonl(sessionUserId);
	memcpy(pos, &tmp, sizeof(sessionUserId));
	pos += sizeof(sessionUserId);

	tmp = htonl(outerUserId);
	memcpy(pos, &tmp, sizeof(outerUserId));
	pos += sizeof(outerUserId);

	tmp = htonl(currentUserId);
	memcpy(pos, &tmp, sizeof(currentUserId));
	pos += sizeof(currentUserId);

	/*
	 * High order half first, since we're doing MSB-first
	 */
	n32 = (uint32) (currentStatementStartTimestamp >> 32);
	n32 = htonl(n32);
	memcpy(pos, &n32, sizeof(n32));
	pos += sizeof(n32);

	/*
	 * Now the low order half
	 */
	n32 = (uint32) currentStatementStartTimestamp;
	n32 = htonl(n32);
	memcpy(pos, &n32, sizeof(n32));
	pos += sizeof(n32);

	tmp = htonl(command_len);
	memcpy(pos, &tmp, sizeof(command_len));
	pos += sizeof(command_len);

	tmp = htonl(querytree_len);
	memcpy(pos, &tmp, sizeof(querytree_len));
	pos += sizeof(querytree_len);

	tmp = htonl(plantree_len);
	memcpy(pos, &tmp, sizeof(plantree_len));
	pos += sizeof(plantree_len);

	tmp = htonl(params_len);
	memcpy(pos, &tmp, sizeof(params_len));
	pos += sizeof(params_len);

	tmp = htonl(sddesc_len);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	tmp = htonl(dtxContextInfo_len);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	if (dtxContextInfo_len > 0)
	{
		memcpy(pos, dtxContextInfo, dtxContextInfo_len);
		pos += dtxContextInfo_len;
	}

    tmp = htonl(serializedDtxTSlen);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

    if (serializedDtxTSlen > 0)
    {
        memcpy(pos, serializedDtxTS, serializedDtxTSlen);
	    pos += serializedDtxTSlen;
    }

	memcpy(pos, command, command_len);
	/* If command is truncated we need to set the terminating '\0' manually */
	pos[command_len - 1] = '\0';
	pos += command_len;

	if (querytree_len > 0)
	{
		memcpy(pos, querytree, querytree_len);
		pos += querytree_len;
	}

	if (plantree_len > 0)
	{
		memcpy(pos, plantree, plantree_len);
		pos += plantree_len;
	}

	if (params_len > 0)
	{
		memcpy(pos, params, params_len);
		pos += params_len;
	}

	if (sddesc_len > 0)
	{
		memcpy(pos, sddesc, sddesc_len);
		pos += sddesc_len;
	}

	tmp = htonl(numsegments);
	memcpy(pos, &tmp, sizeof(numsegments));
	pos += sizeof(numsegments);

	tmp = htonl(resgroupInfo.len);
	memcpy(pos, &tmp, sizeof(resgroupInfo.len));
	pos += sizeof(resgroupInfo.len);

	if (resgroupInfo.len > 0)
	{
		memcpy(pos, resgroupInfo.data, resgroupInfo.len);
		pos += resgroupInfo.len;
	}

	len = pos - shared_query - 1;

	/*
	 * fill in length placeholder
	 */
	tmp = htonl(len);
	memcpy(shared_query + 1, &tmp, sizeof(len));

	Assert(len + 1 == total_query_len);

	if (finalLen)
		*finalLen = len + 1;

	MemoryContextSwitchTo(oldContext);

	return shared_query;
}

/*
 * This function is used for dispatching sliced plans
 */
static void
cdbdisp_dispatchX(QueryDesc* queryDesc,
					bool planRequiresTxn,
					bool cancelOnError)
{
	SliceVec   *sliceVector = NULL;
	int			nSlices = 1;	/* slices this dispatch cares about */
	int			nTotalSlices = 1;	/* total slices in sliceTbl */

	int			iSlice;
	int			rootIdx;
	char	   *queryText = NULL;
	int			queryTextLength = 0;
	struct SliceTable *sliceTbl;
	struct EState *estate;
	CdbDispatcherState *ds;
	ErrorData *qeError = NULL;
	DispatchCommandQueryParms *pQueryParms;

	if (log_dispatch_stats)
		ResetUsage();

	estate = queryDesc->estate;
	sliceTbl = estate->es_sliceTable;
	Assert(sliceTbl != NULL);

	rootIdx = RootSliceIndex(estate);
	Assert(rootIdx == 0 ||
		   (rootIdx > sliceTbl->nMotions &&
			rootIdx <= sliceTbl->nMotions + sliceTbl->nInitPlans));


	ds = cdbdisp_makeDispatcherState(queryDesc->extended_query);

	/*
	 * Since we intend to execute the plan, inventory the slice tree,
	 * allocate gangs, and associate them with slices.
	 *
	 * On return, gangs have been allocated and CDBProcess lists have
	 * been filled in in the slice table.)
	 * 
	 * Notice: This must be done before cdbdisp_buildPlanQueryParms
	 */
	AssignGangs(ds, queryDesc);

	/*
	 * Traverse the slice tree in sliceTbl rooted at rootIdx and build a
	 * vector of slice indexes specifying the order of [potential] dispatch.
	 */
	nTotalSlices = list_length(sliceTbl->slices);
	sliceVector = palloc0(nTotalSlices * sizeof(SliceVec));
	nSlices = fillSliceVector(sliceTbl, rootIdx, sliceVector, nTotalSlices);

	pQueryParms = cdbdisp_buildPlanQueryParms(queryDesc, planRequiresTxn);
	queryText = buildGpQueryString(pQueryParms, &queryTextLength);

	/*
	 * Allocate result array with enough slots for QEs of primary gangs.
	 */
	cdbdisp_makeDispatchResults(ds, nTotalSlices, cancelOnError);
	cdbdisp_makeDispatchParams(ds, nTotalSlices, queryText, queryTextLength);

	cdb_total_plans++;
	cdb_total_slices += nSlices;
	if (nSlices > cdb_max_slices)
		cdb_max_slices = nSlices;

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];

		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG,
						(errmsg("duration to start of dispatch send (root %d): %s ms",
								rootIdx, msec_str)));
				break;
		}
	}

	for (iSlice = 0; iSlice < nSlices; iSlice++)
	{
		Gang	   *primaryGang = NULL;
		Slice	   *slice = NULL;
		int			si = -1;

		Assert(sliceVector != NULL);

		slice = sliceVector[iSlice].slice;
		si = slice->sliceIndex;

		/*
		 * Is this a slice we should dispatch?
		 */
		if (slice && slice->gangType == GANGTYPE_UNALLOCATED)
		{
			Assert(slice->primaryGang == NULL);

			/*
			 * Most slices are dispatched, however, in many cases the root
			 * runs only on the QD and is not dispatched to the QEs.
			 */
			continue;
		}

		primaryGang = slice->primaryGang;
		Assert(primaryGang != NULL);

		if (Test_print_direct_dispatch_info)
			elog(INFO, "(slice %d) Dispatch command to %s", slice->sliceIndex,
				 		segmentsToContentStr(slice->directDispatch.isDirectDispatch ?
											slice->directDispatch.contentIds : slice->segments));

		/*
		 * Bail out if already got an error or cancellation request.
		 */
		if (cancelOnError)
		{
			if (ds->primaryResults->errcode)
				break;
			if (InterruptPending)
				break;
		}
		SIMPLE_FAULT_INJECTOR("before_one_slice_dispatched");

		cdbdisp_dispatchToGang(ds, primaryGang, si);
		if (planRequiresTxn || isDtxExplicitBegin())
			addToGxactTwophaseSegments(primaryGang);

		SIMPLE_FAULT_INJECTOR("after_one_slice_dispatched");
	}

	pfree(sliceVector);

	cdbdisp_waitDispatchFinish(ds);

	/*
	 * If bailed before completely dispatched, stop QEs and throw error.
	 */
	if (iSlice < nSlices)
	{
		elog(Debug_cancel_print ? LOG : DEBUG2,
			 "Plan dispatch canceled; dispatched %d of %d slices",
			 iSlice, nSlices);

		/*
		 * Cancel any QEs still running, and wait for them to terminate.
		 */
		cdbdisp_cancelDispatch(ds);

		/*
		 * Check and free the results of all gangs. If any QE had an error,
		 * report it and exit via PG_THROW.
		 */
		cdbdisp_getDispatchResults(ds, &qeError);

		cdbdisp_destroyDispatcherState(ds);

		if (qeError)
			ReThrowError(qeError);

		/*
		 * Wasn't an error, must have been an interrupt.
		 */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Strange! Not an interrupt either.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg_internal("unable to dispatch plan")));
	}

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];

		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG,
						(errmsg("duration to dispatch out (root %d): %s ms",
								rootIdx, msec_str)));
				break;
		}
	}

	estate->dispatcherState = ds;
}

/*
 * Serialization of query parameters (ParamListInfos).
 *
 * When a query is dispatched from QD to QE, we also need to dispatch any
 * query parameters, contained in the ParamListInfo struct. We need to
 * serialize ParamListInfo, but there are a few complications:
 *
 * - ParamListInfo is not a Node type, so we cannot use the usual
 * nodeToStringBinary() function directly. We turn the array of
 * ParamExternDatas into a List of SerializedParamExternData nodes,
 * which we can then pass to nodeToStringBinary().
 *
 * - The paramFetch callback, which could be used in this process to fetch
 * parameter values on-demand, cannot be used in a different process.
 * Therefore, fetch all parameters before serializing them. When
 * deserializing, leave the callbacks NULL.
 *
 * - In order to deserialize correctly, the receiver needs the typlen and
 * typbyval information for each datatype. The receiver has access to the
 * catalogs, so it could look them up, but for the sake of simplicity and
 * robustness in the receiver, we include that information in
 * SerializedParamExternData.
 *
 * - RECORD types. Type information of transient record is kept only in
 * backend private memory, indexed by typmod. The recipient will not know
 * what a record type's typmod means. And record types can also be nested.
 * Because of that, if there are any RECORD, we include a copy of the whole
 * transient record type cache.
 *
 * If there are no record types involved, we dispatch a list of
 * SerializedParamListInfos, i.e.
 *
 * List<SerializedParamListInfo>
 *
 * With record types, we dispatch:
 *
 * List(List<TupleDescNode>, List<SerializedParamListInfo>)
 *
 * XXX: Sending *all* record types can be quite bulky, but ATM there is no
 * easy way to extract just the needed record types.
 */
static char *
serializeParamListInfo(ParamListInfo paramLI, int *len_p)
{
	int			i;
	List	   *sparams;
	bool		found_records = false;

	/* Construct a list of SerializedParamExternData */
	sparams = NIL;
	for (i = 0; i < paramLI->numParams; i++)
	{
		ParamExternData *prm = &paramLI->params[i];
		SerializedParamExternData *sprm;

		/*
		 * First, use paramFetch to fetch any "lazy" parameters. (The callback
		 * function is of no use in the QE.)
		 */
		if (paramLI->paramFetch && !OidIsValid(prm->ptype))
			(*paramLI->paramFetch) (paramLI, i + 1);

		sprm = makeNode(SerializedParamExternData);

		sprm->value = prm->value;
		sprm->isnull = prm->isnull;
		sprm->pflags = prm->pflags;
		sprm->ptype = prm->ptype;

		if (OidIsValid(prm->ptype))
		{
			get_typlenbyval(prm->ptype, &sprm->plen, &sprm->pbyval);

			if (prm->ptype == RECORDOID && !prm->isnull)
			{
				/*
				 * Note: We don't want to use lookup_rowtype_tupdesc_copy here, because
				 * it copies defaults and constraints too. We don't want those.
				 */
				found_records = true;
			}
		}
		else
		{
			sprm->plen = 0;
			sprm->pbyval = true;
		}

		sparams = lappend(sparams, sprm);
	}

	/*
	 * If there were any record types, include the transient record type cache.
	 */
	if (found_records)
		sparams = lcons(build_tuple_node_list(0), sparams);

	return nodeToBinaryStringFast(sparams, len_p);
}

ParamListInfo
deserializeParamListInfo(const char *str, int slen)
{
	List	   *sparams;
	ListCell   *lc;
	TupleRemapper *remapper;
	ParamListInfo paramLI;
	int			numParams;
	int			iparam;

	sparams = (List *) readNodeFromBinaryString(str, slen);
	if (!IsA(sparams, List))
		elog(ERROR, "could not deserialize query parameters");

	if (!sparams)
		return NULL;;

	/*
	 * If a transient record type cache was included, load it into
	 * a TupleRemapper.
	 */
	if (IsA(linitial(sparams), List))
	{
		List *typelist = (List *) linitial(sparams);
		sparams = list_delete_first(sparams);

		remapper = CreateTupleRemapper();
		TRHandleTypeLists(remapper, typelist);
	}
	else
		remapper = NULL;

	/*
	 * Build a new ParamListInfo.
	 */
	numParams = list_length(sparams);

	paramLI = palloc(offsetof(ParamListInfoData, params) + numParams * sizeof(ParamExternData));
	/* this clears the callback fields, among others */
	memset(paramLI, 0, offsetof(ParamListInfoData, params));
	paramLI->numParams = numParams;

	/*
	 * Read the ParamExternDatas
	 */
	iparam = 0;
	foreach(lc, sparams)
	{
		SerializedParamExternData *sprm = (SerializedParamExternData *) lfirst(lc);
		ParamExternData *prm = &paramLI->params[iparam];

		if (!IsA(sprm, SerializedParamExternData))
			elog(ERROR, "could not deserialize query parameters");

		prm->ptype = sprm->ptype;
		prm->isnull = sprm->isnull;
		prm->pflags = sprm->pflags;

		/* If remapping record types is needed, do it. */
		if (remapper && prm->ptype != InvalidOid)
			prm->value = TRRemapDatum(remapper, sprm->ptype, sprm->value);
		else
			prm->value = sprm->value;

		iparam++;
	}

	return paramLI;
}

/*
 * CdbDispatchCopyStart allocate a writer gang and
 * dispatch the COPY command to segments.
 *
 * In COPY protocol, after a COPY command is dispatched, a response
 * to this will be a PGresult object bearing a status code of
 * PGRES_COPY_OUT or PGRES_COPY_IN, then client can use APIs like
 * PQputCopyData/PQgetCopyData to copy in/out data.
 *
 * cdbdisp_checkDispatchResult() will block until all connections
 * has issued a PGRES_COPY_OUT/PGRES_COPY_IN PGresult response.
 */
void
CdbDispatchCopyStart(struct CdbCopy *cdbCopy, Node *stmt, int flags)
{
	DispatchCommandQueryParms *pQueryParms;
	char *queryText;
	int queryTextLength;
	CdbDispatcherState *ds;
	Gang *primaryGang;
	ErrorData *error = NULL;
	bool needTwoPhase = flags & DF_NEED_TWO_PHASE;

	if (needTwoPhase)
		setupTwoPhaseTransaction();

	elogif((Debug_print_full_dtm || log_min_messages <= DEBUG5), LOG,
		   "CdbDispatchCopyStart: %s (needTwoPhase = %s)",
		   debug_query_string, (needTwoPhase ? "true" : "false"));

	pQueryParms = cdbdisp_buildUtilityQueryParms(stmt, flags, NULL);

	/*
	 * Dispatch the command.
	 */
	ds = cdbdisp_makeDispatcherState(false);

	queryText = buildGpQueryString(pQueryParms, &queryTextLength);

	/*
	 * Allocate a primary QE for every available segDB in the system.
	 */
	primaryGang = AllocateGang(ds, GANGTYPE_PRIMARY_WRITER, cdbCopy->seglist);
	Assert(primaryGang);

	cdbdisp_makeDispatchResults(ds, 1, flags & DF_CANCEL_ON_ERROR);
	cdbdisp_makeDispatchParams (ds, 1, queryText, queryTextLength);

	cdbdisp_dispatchToGang(ds, primaryGang, -1);
	if ((flags & DF_NEED_TWO_PHASE) != 0 || isDtxExplicitBegin())
		addToGxactTwophaseSegments(primaryGang);

	cdbdisp_waitDispatchFinish(ds);

	cdbdisp_checkDispatchResult(ds, DISPATCH_WAIT_NONE);

	if (!cdbdisp_getDispatchResults(ds, &error))
	{
		Assert(error);
		cdbdisp_destroyDispatcherState(ds);
		ReThrowError(error);
	}

	/*
	 * Notice: Do not call cdbdisp_finishCommand to destroy dispatcher state,
	 * following PQputCopyData/PQgetCopyData will be called on those connections
	 */
	cdbCopy->dispatcherState = ds;
}

void
CdbDispatchCopyEnd(struct CdbCopy *cdbCopy)
{
	CdbDispatcherState *ds;

	ds = cdbCopy->dispatcherState;
	cdbCopy->dispatcherState = NULL;
	cdbdisp_destroyDispatcherState(ds);
}

/*
 * Helper function only used by CdbDispatchSetCommand()
 *
 * Return a List of segment id who has idle segment dbs, the list
 * may contain duplicated segment id. eg, if segment 0 has two
 * idle segment dbs in freelist, the list looks like 0 -> 0.
 */
static List *
formIdleSegmentIdList(void)
{
	CdbComponentDatabases	*cdbs;
	List					*segments = NIL;
	int						i, j;

	cdbs = cdbcomponent_getCdbComponents();

	if (cdbs->segment_db_info != NULL)
	{
		for (i = 0; i < cdbs->total_segment_dbs; i++)
		{
			CdbComponentDatabaseInfo *cdi = &cdbs->segment_db_info[i];
			for (j = 0; j < cdi->numIdleQEs; j++)
				segments = lappend_int(segments, cdi->config->segindex);
		}
	}

	return segments;
}
