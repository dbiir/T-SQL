/*-------------------------------------------------------------------------
 *
 * createplan.c
 *	  Routines to create the desired plan for processing a query.
 *	  Planning is complete, we just need to convert the selected
 *	  Path into a Plan.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/createplan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "catalog/pg_exttable.h"
#include "access/skey.h"
#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "executor/execHHashagg.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/planpartition.h"
#include "optimizer/predtest.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_clause.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"	/* ordering_oper_opid */
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/uri.h"

#include "cdb/cdbllize.h"		/* pull_up_Flow() */
#include "cdb/cdbmutate.h"
#include "cdb/cdbpath.h"		/* cdbpath_rows() */
#include "cdb/cdbpathtoplan.h"	/* cdbpathtoplan_create_flow() etc. */
#include "cdb/cdbpullup.h"		/* cdbpullup_targetlist() */
#include "cdb/cdbsetop.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "tdb/storage_param.h"

static Plan *create_subplan(PlannerInfo *root, Path *best_path);		/* CDB */
static Plan *create_scan_plan(PlannerInfo *root, Path *best_path);
static List *build_path_tlist(PlannerInfo *root, Path *path);
static bool use_physical_tlist(PlannerInfo *root, RelOptInfo *rel);
static void disuse_physical_tlist(PlannerInfo *root, Plan *plan, Path *path);
static Plan *create_gating_plan(PlannerInfo *root, Plan *plan, List *quals);
static Plan *create_join_plan(PlannerInfo *root, JoinPath *best_path);
static Plan *create_append_plan(PlannerInfo *root, AppendPath *best_path);
static Plan *create_merge_append_plan(PlannerInfo *root, MergeAppendPath *best_path);
static Result *create_result_plan(PlannerInfo *root, ResultPath *best_path);
static Material *create_material_plan(PlannerInfo *root, MaterialPath *best_path);
static Plan *create_unique_plan(PlannerInfo *root, UniquePath *best_path);
static Plan *create_motion_plan(PlannerInfo *root, CdbMotionPath *path);
static SeqScan *create_seqscan_plan(PlannerInfo *root, Path *best_path,
					List *tlist, List *scan_clauses);
static ExternalScan *create_externalscan_plan(PlannerInfo *root, Path *best_path,
						 List *tlist, List *scan_clauses);
static Scan *create_indexscan_plan(PlannerInfo *root, IndexPath *best_path,
					  List *tlist, List *scan_clauses, bool indexonly);
static BitmapHeapScan *create_bitmap_scan_plan(PlannerInfo *root,
						BitmapHeapPath *best_path,
						List *tlist, List *scan_clauses);
static Plan *create_bitmap_subplan(PlannerInfo *root, Path *bitmapqual,
					  List **qual, List **indexqual, List **indexECs);
static TidScan *create_tidscan_plan(PlannerInfo *root, TidPath *best_path,
					List *tlist, List *scan_clauses);
static SubqueryScan *create_subqueryscan_plan(PlannerInfo *root, Path *best_path,
						 List *tlist, List *scan_clauses);
static FunctionScan *create_functionscan_plan(PlannerInfo *root, Path *best_path,
						 List *tlist, List *scan_clauses);
static TableFunctionScan *create_tablefunction_plan(PlannerInfo *root,
						  Path *best_path,
						  List *tlist,
						  List *scan_clauses);
static ValuesScan *create_valuesscan_plan(PlannerInfo *root, Path *best_path,
					   List *tlist, List *scan_clauses);
static SubqueryScan *create_ctescan_plan(PlannerInfo *root, Path *best_path,
					List *tlist, List *scan_clauses);
static WorkTableScan *create_worktablescan_plan(PlannerInfo *root, Path *best_path,
						  List *tlist, List *scan_clauses);
static ForeignScan *create_foreignscan_plan(PlannerInfo *root, ForeignPath *best_path,
						List *tlist, List *scan_clauses);
static NestLoop *create_nestloop_plan(PlannerInfo *root, NestPath *best_path,
					 Plan *outer_plan, Plan *inner_plan);
static MergeJoin *create_mergejoin_plan(PlannerInfo *root, MergePath *best_path,
					  Plan *outer_plan, Plan *inner_plan);
static HashJoin *create_hashjoin_plan(PlannerInfo *root, HashPath *best_path,
					 Plan *outer_plan, Plan *inner_plan);
static Node *replace_nestloop_params(PlannerInfo *root, Node *expr);
static Node *replace_nestloop_params_mutator(Node *node, PlannerInfo *root);
static void process_subquery_nestloop_params(PlannerInfo *root,
								 List *subplan_params);
static List *fix_indexqual_references(PlannerInfo *root, IndexPath *index_path);
static List *fix_indexorderby_references(PlannerInfo *root, IndexPath *index_path);
static Node *fix_indexqual_operand(Node *node, IndexOptInfo *index, int indexcol);
static List *get_switched_clauses(List *clauses, Relids outerrelids);
static List *order_qual_clauses(PlannerInfo *root, List *clauses);
static void copy_path_costsize(PlannerInfo *root, Plan *dest, Path *src);
static void copy_plan_costsize(Plan *dest, Plan *src);
static SeqScan *make_seqscan(List *qptlist, List *qpqual, Index scanrelid);
static ExternalScan *make_externalscan(List *qptlist,
				  List *qpqual,
				  Index scanrelid,
				  List *filenames,
				  char *fmtoptstring,
				  bool istext,
				  bool ismasteronly,
				  int rejectlimit,
				  bool rejectlimitinrows,
				  bool logerrors,
				  int encoding);
static IndexScan *make_indexscan(List *qptlist, List *qpqual, Index scanrelid,
			   Oid indexid, List *indexqual, List *indexqualorig,
			   List *indexorderby, List *indexorderbyorig,
			   ScanDirection indexscandir);
static IndexOnlyScan *make_indexonlyscan(List *qptlist, List *qpqual,
				   Index scanrelid, Oid indexid,
				   List *indexqual, List *indexqualorig,
				   List *indexorderby,
				   List *indextlist,
				   ScanDirection indexscandir);
static BitmapIndexScan *make_bitmap_indexscan(Index scanrelid, Oid indexid,
					  List *indexqual,
					  List *indexqualorig);
static BitmapHeapScan *make_bitmap_heapscan(List *qptlist,
					 List *qpqual,
					 Plan *lefttree,
					 List *bitmapqualorig,
					 Index scanrelid);
static TidScan *make_tidscan(List *qptlist, List *qpqual, Index scanrelid,
			 List *tidquals);
static FunctionScan *make_functionscan(List *qptlist, List *qpqual,
				  Index scanrelid, List *functions, bool funcordinality);
static TableFunctionScan *make_tablefunction(List *qptlist, List *qpqual,
				   Plan *subplan, Index scanrelid, RangeTblFunction *function);
static ValuesScan *make_valuesscan(List *qptlist, List *qpqual,
				Index scanrelid, List *values_lists);
static CteScan *make_ctescan(List *qptlist, List *qpqual,
			 Index scanrelid, int ctePlanId, int cteParam);
static WorkTableScan *make_worktablescan(List *qptlist, List *qpqual,
				   Index scanrelid, int wtParam);
static BitmapAnd *make_bitmap_and(List *bitmapplans);
static BitmapOr *make_bitmap_or(List *bitmapplans);
static List *flatten_grouping_list(List *groupcls);
static void adjust_modifytable_flow(PlannerInfo *root, ModifyTable *node, List *is_split_updates);
static Plan *prepare_sort_from_pathkeys(PlannerInfo *root,
						   Plan *lefttree, List *pathkeys,
						   Relids relids,
						   const AttrNumber *reqColIdx,
						   bool adjust_tlist_in_place,
						   int *p_numsortkeys,
						   AttrNumber **p_sortColIdx,
						   Oid **p_sortOperators,
						   Oid **p_collations,
						   bool **p_nullsFirst, bool add_keys_to_targetlist);
static EquivalenceMember *find_ec_member_for_tle(EquivalenceClass *ec,
					   TargetEntry *tle,
					   Relids relids);

static Motion *cdbpathtoplan_create_motion_plan(PlannerInfo *root,
								 CdbMotionPath *path,
								 Plan *subplan);

/*
 * GPDB_92_MERGE_FIXME: The following functions have been removed in PG 9.2
 * But GPDB codes are still using them, so keep them here.
 */
static int
add_sort_column(AttrNumber colIdx, Oid sortOp, Oid coll, bool nulls_first,
				int numCols, AttrNumber *sortColIdx,
				Oid *sortOperators, Oid *collations, bool *nullsFirst);

/*
 * create_plan
 *	  Creates the access plan for a query by recursively processing the
 *	  desired tree of pathnodes, starting at the node 'best_path'.  For
 *	  every pathnode found, we create a corresponding plan node containing
 *	  appropriate id, target list, and qualification information.
 *
 *	  The tlists and quals in the plan tree are still in planner format,
 *	  ie, Vars still correspond to the parser's numbering.  This will be
 *	  fixed later by setrefs.c.
 *
 *	  best_path is the best access path
 *
 *	  Returns a Plan tree.
 */
Plan *
create_plan(PlannerInfo *root, Path *path)
{
	Plan	   *plan;

	/* plan_params should not be in use in current query level */
	Assert(root->plan_params == NIL);

	/* Modify path to support unique rowid operation for subquery preds. */
	if (root->join_info_list)
		cdbpath_dedup_fixup(root, path);

	/* Generate the Plan tree. */
	plan = create_subplan(root, path);

	/* Decorate the top node of the plan with a Flow node. */
	plan->flow = cdbpathtoplan_create_flow(root,
										   path->locus,
										   path->parent ? path->parent->relids
										   : NULL,
										   plan);
	return plan;
}	/* create_plan */


/*
 * create_subplan
 */
static Plan *
create_subplan(PlannerInfo *root, Path *best_path)
{
	Plan	   *plan;

	/* Initialize this module's private workspace in PlannerInfo */
	root->curOuterRels = NULL;
	root->curOuterParams = NIL;

	/* Recursively process the path tree */
	plan = create_plan_recurse(root, best_path);

	/* Check we successfully assigned all NestLoopParams to plan nodes */
	if (root->curOuterParams != NIL)
		elog(ERROR, "failed to assign all NestLoopParams to plan nodes");

	/*
	 * Reset plan_params to ensure param IDs used for nestloop params are not
	 * re-used later
	 */
	root->plan_params = NIL;

	return plan;
}

/*
 * create_plan_recurse
 *	  Recursive guts of create_plan().
 */
Plan *
create_plan_recurse(PlannerInfo *root, Path *best_path)
{
	Plan	   *plan;

	/* Guard against stack overflow due to overly complex plans */
	check_stack_depth();

	switch (best_path->pathtype)
	{
		case T_SeqScan:
		case T_IndexScan:
		case T_ExternalScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFunctionScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_ForeignScan:
			plan = create_scan_plan(root, best_path);
			break;
		case T_HashJoin:
		case T_MergeJoin:
		case T_NestLoop:
			plan = create_join_plan(root,
									(JoinPath *) best_path);
			break;
		case T_Append:
			plan = create_append_plan(root,
									  (AppendPath *) best_path);
			break;
		case T_MergeAppend:
			plan = create_merge_append_plan(root,
											(MergeAppendPath *) best_path);
			break;
		case T_Result:
			plan = (Plan *) create_result_plan(root,
											   (ResultPath *) best_path);
			break;
		case T_Material:
			plan = (Plan *) create_material_plan(root,
												 (MaterialPath *) best_path);
			break;
		case T_Unique:
			plan = create_unique_plan(root,
									  (UniquePath *) best_path);
			break;
		case T_Motion:
			plan = create_motion_plan(root, (CdbMotionPath *) best_path);
			break;
		case T_PartitionSelector:
			plan = create_partition_selector_plan(root, (PartitionSelectorPath *) best_path);
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) best_path->pathtype);
			plan = NULL;		/* keep compiler quiet */
			break;
	}

	if (CdbPathLocus_IsPartitioned(best_path->locus) ||
		CdbPathLocus_IsSegmentGeneral(best_path->locus) ||
		CdbPathLocus_IsReplicated(best_path->locus))
		plan->dispatch = DISPATCH_PARALLEL;

	return plan;
}

/*
 * create_scan_plan
 *	 Create a scan plan for the parent relation of 'best_path'.
 */
static Plan *
create_scan_plan(PlannerInfo *root, Path *best_path)
{
	RelOptInfo *rel = best_path->parent;
	List	   *tlist;
	List	   *scan_clauses;
	Plan	   *plan;

	/*
	 * For table scans, rather than using the relation targetlist (which is
	 * only those Vars actually needed by the query), we prefer to generate a
	 * tlist containing all Vars in order.  This will allow the executor to
	 * optimize away projection of the table tuples, if possible.  (Note that
	 * planner.c may replace the tlist we generate here, forcing projection to
	 * occur.)
	 */
	if (use_physical_tlist(root, rel))
	{
		if (best_path->pathtype == T_IndexOnlyScan)
		{
			/* For index-only scan, the preferred tlist is the index's */
			tlist = copyObject(((IndexPath *) best_path)->indexinfo->indextlist);
		}
		else
		{
			tlist = build_physical_tlist(root, rel);
			/* if fail because of dropped cols, use regular method */
			if (tlist == NIL)
				tlist = build_path_tlist(root, best_path);
		}
	}
	else
	{
		tlist = build_path_tlist(root, best_path);
	}

	/*
	 * Extract the relevant restriction clauses from the parent relation. The
	 * executor must apply all these restrictions during the scan, except for
	 * pseudoconstants which we'll take care of below.
	 */
	scan_clauses = rel->baserestrictinfo;

	/*
	 * If this is a parameterized scan, we also need to enforce all the join
	 * clauses available from the outer relation(s).
	 *
	 * For paranoia's sake, don't modify the stored baserestrictinfo list.
	 */
	if (best_path->param_info)
		scan_clauses = list_concat(list_copy(scan_clauses),
								   best_path->param_info->ppi_clauses);

	switch (best_path->pathtype)
	{
		case T_SeqScan:
			plan = (Plan *) create_seqscan_plan(root,
												best_path,
												tlist,
												scan_clauses);
			break;

		case T_ExternalScan:
			plan = (Plan *) create_externalscan_plan(root,
													 best_path,
													 tlist,
													 scan_clauses);
			break;

		case T_IndexScan:
			plan = (Plan *) create_indexscan_plan(root,
												  (IndexPath *) best_path,
												  tlist,
												  scan_clauses,
												  false);
			break;

		case T_IndexOnlyScan:
			plan = (Plan *) create_indexscan_plan(root,
												  (IndexPath *) best_path,
												  tlist,
												  scan_clauses,
												  true);
			break;

		case T_BitmapHeapScan:
			plan = (Plan *) create_bitmap_scan_plan(root,
												(BitmapHeapPath *) best_path,
													tlist,
													scan_clauses);
			break;

		case T_TidScan:
			plan = (Plan *) create_tidscan_plan(root,
												(TidPath *) best_path,
												tlist,
												scan_clauses);
			break;

		case T_SubqueryScan:
			plan = (Plan *) create_subqueryscan_plan(root,
													 best_path,
													 tlist,
													 scan_clauses);
			break;

		case T_FunctionScan:
			plan = (Plan *) create_functionscan_plan(root,
													 best_path,
													 tlist,
													 scan_clauses);
			break;

		case T_TableFunctionScan:
			plan = (Plan *) create_tablefunction_plan(root,
													  best_path,
													  tlist,
													  scan_clauses);
			break;

		case T_ValuesScan:
			plan = (Plan *) create_valuesscan_plan(root,
												   best_path,
												   tlist,
												   scan_clauses);
			break;

		case T_CteScan:
			plan = (Plan *) create_ctescan_plan(root,
												best_path,
												tlist,
												scan_clauses);
			break;

		case T_WorkTableScan:
			plan = (Plan *) create_worktablescan_plan(root,
													  best_path,
													  tlist,
													  scan_clauses);
			break;

		case T_ForeignScan:
			plan = (Plan *) create_foreignscan_plan(root,
													(ForeignPath *) best_path,
													tlist,
													scan_clauses);
			break;

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) best_path->pathtype);
			plan = NULL;		/* keep compiler quiet */
			break;
	}

	/* Decorate the top node of the plan with a Flow node. */
	plan->flow = cdbpathtoplan_create_flow(root,
										   best_path->locus,
								best_path->parent ? best_path->parent->relids
										   : NULL,
										   plan);

	/**
	 * If plan has a flow node, ensure all entries of hashExpr
	 * are in the targetlist.
	 */
	if (plan->flow && plan->flow->hashExprs)
	{
		plan->targetlist = add_to_flat_tlist_junk(plan->targetlist, plan->flow->hashExprs, true /* resjunk */ );
	}

	/*
	 * If there are any pseudoconstant clauses attached to this node, insert a
	 * gating Result node that evaluates the pseudoconstants as one-time
	 * quals.
	 */
	if (root->hasPseudoConstantQuals)
		plan = create_gating_plan(root, plan, scan_clauses);

	return plan;
}

/*
 * Build a target list (ie, a list of TargetEntry) for the Path's output.
 */
static List *
build_path_tlist(PlannerInfo *root, Path *path)
{
	RelOptInfo *rel = path->parent;
	List	   *tlist = NIL;
	int			resno = 1;
	ListCell   *v;

	foreach(v, rel->reltargetlist)
	{
		/* Do we really need to copy here?	Not sure */
		Node	   *node = (Node *) copyObject(lfirst(v));

		/*
		 * If it's a parameterized path, there might be lateral references in
		 * the tlist, which need to be replaced with Params.  There's no need
		 * to remake the TargetEntry nodes, so apply this to each list item
		 * separately.
		 */
		if (path->param_info)
			node = replace_nestloop_params(root, node);

		tlist = lappend(tlist, makeTargetEntry((Expr *) node,
											   resno,
											   NULL,
											   false));
		resno++;
	}
	return tlist;
}

/*
 * use_physical_tlist
 *		Decide whether to use a tlist matching relation structure,
 *		rather than only those Vars actually referenced.
 */
static bool
use_physical_tlist(PlannerInfo *root, RelOptInfo *rel)
{
	RangeTblEntry *rte;
	int			i;
	ListCell   *lc;

	/*
	 * We can do this for real relation scans, subquery scans, function scans,
	 * values scans, and CTE scans (but not for, eg, joins).
	 */
	if (rel->rtekind != RTE_RELATION &&
		rel->rtekind != RTE_SUBQUERY &&
		rel->rtekind != RTE_FUNCTION &&
		rel->rtekind != RTE_VALUES &&
		rel->rtekind != RTE_TABLEFUNCTION &&
		rel->rtekind != RTE_CTE)
		return false;

	/*
	 * Can't do it with inheritance cases either (mainly because Append
	 * doesn't project).
	 */
	if (rel->reloptkind != RELOPT_BASEREL)
		return false;

	/*
	 * Can't do it if any system columns or whole-row Vars are requested.
	 * (This could possibly be fixed but would take some fragile assumptions
	 * in setrefs.c, I think.)
	 */
	for (i = rel->min_attr; i <= 0; i++)
	{
		if (!bms_is_empty(rel->attr_needed[i - rel->min_attr]))
			return false;
	}

	/*
	 * Can't do it if the rel is required to emit any placeholder expressions,
	 * either.
	 */
	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(lc);

		if (bms_nonempty_difference(phinfo->ph_needed, rel->relids) &&
			bms_is_subset(phinfo->ph_eval_at, rel->relids))
			return false;
	}

	/* CDB: Don't use physical tlist if rel has pseudo columns. */
	rte = rt_fetch(rel->relid, root->parse->rtable);
	if (rte->pseudocols)
		return false;

	return true;
}

/*
 * disuse_physical_tlist
 *		Switch a plan node back to emitting only Vars actually referenced.
 *
 * If the plan node immediately above a scan would prefer to get only
 * needed Vars and not a physical tlist, it must call this routine to
 * undo the decision made by use_physical_tlist().  Currently, Hash, Sort,
 * and Material nodes want this, so they don't have to store useless columns.
 * We need to ensure that all vars referenced in Flow node, if any, are added
 * to the targetlist as resjunk.
 */
static void
disuse_physical_tlist(PlannerInfo *root, Plan *plan, Path *path)
{
	/* Only need to undo it for path types handled by create_scan_plan() */
	switch (path->pathtype)
	{
		case T_SeqScan:
		case T_ExternalScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_ForeignScan:
			plan->targetlist = build_path_tlist(root, path);

			/**
			 * If plan has a flow node, ensure all entries of hashExpr
			 * are in the targetlist.
			 */
			if (plan->flow && plan->flow->hashExprs)
			{
				plan->targetlist = add_to_flat_tlist_junk(plan->targetlist, plan->flow->hashExprs, true /* resjunk */);
			}
			break;
		default:
			break;
	}
}

/*
 * create_gating_plan
 *	  Deal with pseudoconstant qual clauses
 *
 * If the node's quals list includes any pseudoconstant quals, put them
 * into a gating Result node atop the already-built plan.  Otherwise,
 * return the plan as-is.
 *
 * Note that we don't change cost or size estimates when doing gating.
 * The costs of qual eval were already folded into the plan's startup cost.
 * Leaving the size alone amounts to assuming that the gating qual will
 * succeed, which is the conservative estimate for planning upper queries.
 * We certainly don't want to assume the output size is zero (unless the
 * gating qual is actually constant FALSE, and that case is dealt with in
 * clausesel.c).  Interpolating between the two cases is silly, because
 * it doesn't reflect what will really happen at runtime, and besides which
 * in most cases we have only a very bad idea of the probability of the gating
 * qual being true.
 */
static Plan *
create_gating_plan(PlannerInfo *root, Plan *plan, List *quals)
{
	List	   *pseudoconstants;

	/* Sort into desirable execution order while still in RestrictInfo form */
	quals = order_qual_clauses(root, quals);

	/* Pull out any pseudoconstant quals from the RestrictInfo list */
	pseudoconstants = extract_actual_clauses(quals, true);

	if (!pseudoconstants)
		return plan;

	return (Plan *) make_result(root,
								plan->targetlist,
								(Node *) pseudoconstants,
								plan);
}

/*
 * create_join_plan
 *	  Create a join plan for 'best_path' and (recursively) plans for its
 *	  inner and outer paths.
 */
static Plan *
create_join_plan(PlannerInfo *root, JoinPath *best_path)
{
	Plan	   *outer_plan;
	Plan	   *inner_plan;
	Plan	   *plan;
	Relids		saveOuterRels = root->curOuterRels;
	bool		partition_selector_created;
	List	   *partSelectors;

	/*
	 * Try to inject Partition Selectors.
	 */
	partition_selector_created =
		inject_partition_selectors_for_join(root,
											best_path,
											&partSelectors);

	outer_plan = create_plan_recurse(root, best_path->outerjoinpath);

	/* For a nestloop, include outer relids in curOuterRels for inner side */
	if (best_path->path.pathtype == T_NestLoop)
		root->curOuterRels = bms_union(root->curOuterRels,
									   best_path->outerjoinpath->parent->relids);

	inner_plan = create_plan_recurse(root, best_path->innerjoinpath);

	switch (best_path->path.pathtype)
	{
		case T_MergeJoin:
			plan = (Plan *) create_mergejoin_plan(root,
												  (MergePath *) best_path,
												  outer_plan,
												  inner_plan);
			break;
		case T_HashJoin:
			plan = (Plan *) create_hashjoin_plan(root,
												 (HashPath *) best_path,
												 outer_plan,
												 inner_plan);
			break;
		case T_NestLoop:
			/* Restore curOuterRels */
			bms_free(root->curOuterRels);
			root->curOuterRels = saveOuterRels;

			plan = (Plan *) create_nestloop_plan(root,
												 (NestPath *) best_path,
												 outer_plan,
												 inner_plan);
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) best_path->path.pathtype);
			plan = NULL;		/* keep compiler quiet */
			break;
	}

	/*
	 * If we injected a partition selector to the inner side, we must evaluate
	 * the inner side before the outer side, so that the partition selector
	 * can influence the execution of the outer side.
	 */
	Assert(plan->type == best_path->path.pathtype);
	if (partition_selector_created)
		((Join *) plan)->prefetch_inner = true;

	/*
	 * A motion deadlock can also happen when outer and joinqual both contain
	 * motions.  It is not easy to check for joinqual here, so we set the
	 * prefetch_joinqual mark only according to outer motion, and check for
	 * joinqual later in the executor.
	 *
	 * See ExecPrefetchJoinQual() for details.
	 */
	if (best_path->outerjoinpath &&
		best_path->outerjoinpath->motionHazard)
		((Join *) plan)->prefetch_joinqual = true;

	plan->flow = cdbpathtoplan_create_flow(root,
			best_path->path.locus,
			best_path->path.parent ? best_path->path.parent->relids
					: NULL,
					  plan);

	/**
	 * If plan has a flow node, ensure all entries of hashExpr
	 * are in the targetlist.
	 */
	if (plan->flow && plan->flow->hashExprs)
	{
		plan->targetlist = add_to_flat_tlist_junk(plan->targetlist, plan->flow->hashExprs, true /* resjunk */ );
	}

	/*
	 * If there are any pseudoconstant clauses attached to this node, insert a
	 * gating Result node that evaluates the pseudoconstants as one-time
	 * quals.
	 */
	if (root->hasPseudoConstantQuals)
		plan = create_gating_plan(root, plan, best_path->joinrestrictinfo);

#ifdef NOT_USED

	/*
	 * * Expensive function pullups may have pulled local predicates * into
	 * this path node.  Put them in the qpqual of the plan node. * JMH,
	 * 6/15/92
	 */
	if (get_loc_restrictinfo(best_path) != NIL)
		set_qpqual((Plan) plan,
				   list_concat(get_qpqual((Plan) plan),
					   get_actual_clauses(get_loc_restrictinfo(best_path))));
#endif

	return plan;
}

/*
 * create_append_plan
 *	  Create an Append plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan *
create_append_plan(PlannerInfo *root, AppendPath *best_path)
{
	Append	   *plan;
	List	   *tlist = build_path_tlist(root, &best_path->path);
	List	   *subplans = NIL;
	ListCell   *subpaths;

	/*
	 * The subpaths list could be empty, if every child was proven empty by
	 * constraint exclusion.  In that case generate a dummy plan that returns
	 * no rows.
	 *
	 * Note that an AppendPath with no members is also generated in certain
	 * cases where there was no appending construct at all, but we know the
	 * relation is empty (see set_dummy_rel_pathlist).
	 */
	if (best_path->subpaths == NIL)
	{
		/* Generate a Result plan with constant-FALSE gating qual */
		return (Plan *) make_result(root,
									tlist,
									(Node *) list_make1(makeBoolConst(false,
																	  false)),
									NULL);
	}

	/* Build the plan for each child */
	foreach(subpaths, best_path->subpaths)
	{
		Path	   *subpath = (Path *) lfirst(subpaths);

		subplans = lappend(subplans, create_plan_recurse(root, subpath));
	}

	/*
	 * XXX ideally, if there's just one child, we'd not bother to generate an
	 * Append node but just return the single child.  At the moment this does
	 * not work because the varno of the child scan plan won't match the
	 * parent-rel Vars it'll be asked to emit.
	 */

	plan = make_append(subplans, tlist);

	return (Plan *) plan;
}

/*
 * create_merge_append_plan
 *	  Create a MergeAppend plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan *
create_merge_append_plan(PlannerInfo *root, MergeAppendPath *best_path)
{
	MergeAppend *node = makeNode(MergeAppend);
	Plan	   *plan = &node->plan;
	List	   *tlist = build_path_tlist(root, &best_path->path);
	List	   *pathkeys = best_path->path.pathkeys;
	List	   *subplans = NIL;
	ListCell   *subpaths;

	/*
	 * We don't have the actual creation of the MergeAppend node split out
	 * into a separate make_xxx function.  This is because we want to run
	 * prepare_sort_from_pathkeys on it before we do so on the individual
	 * child plans, to make cross-checking the sort info easier.
	 */
	copy_path_costsize(root, plan, (Path *) best_path);
	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;

	/* Compute sort column info, and adjust MergeAppend's tlist as needed */
	(void) prepare_sort_from_pathkeys(root, plan, pathkeys,
									  best_path->path.parent->relids,
									  NULL,
									  true,
									  &node->numCols,
									  &node->sortColIdx,
									  &node->sortOperators,
									  &node->collations,
									  &node->nullsFirst,
									  true);

	/*
	 * Now prepare the child plans.  We must apply prepare_sort_from_pathkeys
	 * even to subplans that don't need an explicit sort, to make sure they
	 * are returning the same sort key columns the MergeAppend expects.
	 */
	foreach(subpaths, best_path->subpaths)
	{
		Path	   *subpath = (Path *) lfirst(subpaths);
		Plan	   *subplan;
		int			numsortkeys;
		AttrNumber *sortColIdx;
		Oid		   *sortOperators;
		Oid		   *collations;
		bool	   *nullsFirst;

		/* Build the child plan */
		subplan = create_plan_recurse(root, subpath);

		/* Compute sort column info, and adjust subplan's tlist as needed */
		subplan = prepare_sort_from_pathkeys(root, subplan, pathkeys,
											 subpath->parent->relids,
											 node->sortColIdx,
											 false,
											 &numsortkeys,
											 &sortColIdx,
											 &sortOperators,
											 &collations,
											 &nullsFirst,
											 true);

		/*
		 * Check that we got the same sort key information.  We just Assert
		 * that the sortops match, since those depend only on the pathkeys;
		 * but it seems like a good idea to check the sort column numbers
		 * explicitly, to ensure the tlists really do match up.
		 */
		Assert(numsortkeys == node->numCols);
		if (memcmp(sortColIdx, node->sortColIdx,
				   numsortkeys * sizeof(AttrNumber)) != 0)
			elog(ERROR, "MergeAppend child's targetlist doesn't match MergeAppend");
		Assert(memcmp(sortOperators, node->sortOperators,
					  numsortkeys * sizeof(Oid)) == 0);
		Assert(memcmp(collations, node->collations,
					  numsortkeys * sizeof(Oid)) == 0);
		Assert(memcmp(nullsFirst, node->nullsFirst,
					  numsortkeys * sizeof(bool)) == 0);

		/* Now, insert a Sort node if subplan isn't sufficiently ordered */
		if (!pathkeys_contained_in(pathkeys, subpath->pathkeys))
			subplan = (Plan *) make_sort(root, subplan, numsortkeys,
										 sortColIdx, sortOperators,
										 collations, nullsFirst,
										 best_path->limit_tuples);

		subplans = lappend(subplans, subplan);
	}

	node->mergeplans = subplans;

	return (Plan *) node;
}

/*
 * create_result_plan
 *	  Create a Result plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Result *
create_result_plan(PlannerInfo *root, ResultPath *best_path)
{
	List	   *tlist;
	List	   *quals;

	/* The tlist will be installed later, since we have no RelOptInfo */
	Assert(best_path->path.parent == NULL);
	tlist = NIL;

	/* best_path->quals is just bare clauses */

	quals = order_qual_clauses(root, best_path->quals);

	return make_result(root, tlist, (Node *) quals, NULL);
}

/*
 * create_material_plan
 *	  Create a Material plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Material *
create_material_plan(PlannerInfo *root, MaterialPath *best_path)
{
	Material   *plan;
	Plan	   *subplan;

	subplan = create_plan_recurse(root, best_path->subpath);

	/* We don't want any excess columns in the materialized tuples */
	disuse_physical_tlist(root, subplan, best_path->subpath);

	plan = make_material(subplan);

	plan->cdb_strict = best_path->cdb_strict;
	plan->cdb_shield_child_from_rescans = best_path->cdb_shield_child_from_rescans;

	copy_path_costsize(root, &plan->plan, (Path *) best_path);

	return plan;
}

/*
 * create_unique_plan
 *	  Create a Unique plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan *
create_unique_plan(PlannerInfo *root, UniquePath *best_path)
{
	Plan	   *plan;
	Plan	   *subplan;
	List	   *in_operators;
	List	   *uniq_exprs;
	List	   *newtlist;
	int			nextresno;
	bool		newitems;
	int			numGroupCols;
	AttrNumber *groupColIdx;
	int			groupColPos;
	ListCell   *l;

	subplan = create_plan_recurse(root, best_path->subpath);

	/* Return naked subplan if we don't need to do any actual unique-ifying */
	if (best_path->umethod == UNIQUE_PATH_NOOP)
		return subplan;

	/*
	 * As constructed, the subplan has a "flat" tlist containing just the Vars
	 * needed here and at upper levels.  The values we are supposed to
	 * unique-ify may be expressions in these variables.  We have to add any
	 * such expressions to the subplan's tlist.
	 *
	 * The subplan may have a "physical" tlist if it is a simple scan plan. If
	 * we're going to sort, this should be reduced to the regular tlist, so
	 * that we don't sort more data than we need to.  For hashing, the tlist
	 * should be left as-is if we don't need to add any expressions; but if we
	 * do have to add expressions, then a projection step will be needed at
	 * runtime anyway, so we may as well remove unneeded items. Therefore
	 * newtlist starts from build_path_tlist() not just a copy of the
	 * subplan's tlist; and we don't install it into the subplan unless we are
	 * sorting or stuff has to be added.
	 */
	in_operators = best_path->in_operators;
	uniq_exprs = best_path->uniq_exprs;

	/* initialize modified subplan tlist as just the "required" vars */
	newtlist = build_path_tlist(root, &best_path->path);
	nextresno = list_length(newtlist) + 1;
	newitems = false;

	foreach(l, uniq_exprs)
	{
		Node	   *uniqexpr = lfirst(l);
		TargetEntry *tle;

		tle = tlist_member(uniqexpr, newtlist);
		if (!tle)
		{
			tle = makeTargetEntry((Expr *) uniqexpr,
								  nextresno,
								  NULL,
								  false);
			newtlist = lappend(newtlist, tle);
			nextresno++;
			newitems = true;
		}
	}

	if (newitems || best_path->umethod == UNIQUE_PATH_SORT)
	{
		/*
		 * If the top plan node can't do projections and its existing target
		 * list isn't already what we need, we need to add a Result node to
		 * help it along.
		 */
		subplan = plan_pushdown_tlist(root, subplan, newtlist);
	}

	/*
	 * Build control information showing which subplan output columns are to
	 * be examined by the grouping step.  Unfortunately we can't merge this
	 * with the previous loop, since we didn't then know which version of the
	 * subplan tlist we'd end up using.
	 */
	newtlist = subplan->targetlist;
	numGroupCols = list_length(uniq_exprs);
	groupColIdx = (AttrNumber *) palloc(numGroupCols * sizeof(AttrNumber));

	groupColPos = 0;
	foreach(l, uniq_exprs)
	{
		Node	   *uniqexpr = lfirst(l);
		TargetEntry *tle;

		tle = tlist_member(uniqexpr, newtlist);
		if (!tle)				/* shouldn't happen */
			elog(ERROR, "failed to find unique expression in subplan tlist");
		groupColIdx[groupColPos++] = tle->resno;
	}

	if (best_path->umethod == UNIQUE_PATH_HASH)
	{
		long		numGroups;
		Oid		   *groupOperators;

		numGroups = (long) Min(best_path->path.rows, (double) LONG_MAX);

		/*
		 * Get the hashable equality operators for the Agg node to use.
		 * Normally these are the same as the IN clause operators, but if
		 * those are cross-type operators then the equality operators are the
		 * ones for the IN clause operators' RHS datatype.
		 */
		groupOperators = (Oid *) palloc(numGroupCols * sizeof(Oid));
		groupColPos = 0;
		foreach(l, in_operators)
		{
			Oid			in_oper = lfirst_oid(l);
			Oid			eq_oper;

			if (!get_compatible_hash_operators(in_oper, NULL, &eq_oper))
				elog(ERROR, "could not find compatible hash operator for operator %u",
					 in_oper);
			groupOperators[groupColPos++] = eq_oper;
		}

		/*
		 * Since the Agg node is going to project anyway, we can give it the
		 * minimum output tlist, without any stuff we might have added to the
		 * subplan tlist.
		 */
		plan = (Plan *) make_agg(root,
								 build_path_tlist(root, &best_path->path),
								 NIL,
								 AGG_HASHED,
								 NULL,
								 false, /* streaming */
								 numGroupCols,
								 groupColIdx,
								 groupOperators,
								 numGroups,
								 0, /* num_nullcols */
								 0, /* input_grouping */
								 0, /* grouping */
								 0, /* rollup_gs_times */
								 subplan);
	}
	else
	{
		List	   *sortList = NIL;

		/* Create an ORDER BY list to sort the input compatibly */
		groupColPos = 0;
		foreach(l, in_operators)
		{
			Oid			in_oper = lfirst_oid(l);
			Oid			sortop;
			Oid			eqop;
			TargetEntry *tle;
			SortGroupClause *sortcl;

			sortop = get_ordering_op_for_equality_op(in_oper, false);
			if (!OidIsValid(sortop))	/* shouldn't happen */
				elog(ERROR, "could not find ordering operator for equality operator %u",
					 in_oper);

			/*
			 * The Unique node will need equality operators.  Normally these
			 * are the same as the IN clause operators, but if those are
			 * cross-type operators then the equality operators are the ones
			 * for the IN clause operators' RHS datatype.
			 */
			eqop = get_equality_op_for_ordering_op(sortop, NULL);
			if (!OidIsValid(eqop))		/* shouldn't happen */
				elog(ERROR, "could not find equality operator for ordering operator %u",
					 sortop);

			tle = get_tle_by_resno(subplan->targetlist,
								   groupColIdx[groupColPos]);
			Assert(tle != NULL);

			sortcl = makeNode(SortGroupClause);
			sortcl->tleSortGroupRef = assignSortGroupRef(tle,
														 subplan->targetlist);
			sortcl->eqop = eqop;
			sortcl->sortop = sortop;
			sortcl->nulls_first = false;
			sortcl->hashable = false;	/* no need to make this accurate */
			sortList = lappend(sortList, sortcl);
			groupColPos++;
		}
		plan = (Plan *) make_sort_from_sortclauses(root, sortList, subplan);
		plan = (Plan *) make_unique(plan, sortList);
	}

	/* Adjust output size estimate (other fields should be OK already) */
	plan->plan_rows = best_path->path.rows;

	return plan;
}


/*
 * create_motion_plan
 */
Plan *
create_motion_plan(PlannerInfo *root, CdbMotionPath *path)
{
	Motion	   *motion;
	Path	   *subpath = path->subpath;
	Plan	   *subplan;
	Relids		save_curOuterRels = root->curOuterRels;
	List	   *save_curOuterParams = root->curOuterParams;

	/*
	 * singleQE-->entry:  Elide the motion.  The subplan will run in the same
	 * process with its parent: either the qDisp (if it is a top slice) or a
	 * singleton gang on the entry db (otherwise).
	 */
	if (CdbPathLocus_IsEntry(path->path.locus) &&
		CdbPathLocus_IsSingleQE(subpath->locus))
	{
		/* Push the MotionPath's locus and pathkeys down onto subpath. */
		subpath->locus = path->path.locus;
		subpath->pathkeys = path->path.pathkeys;

		subplan = create_subplan(root, subpath);

		root->curOuterRels = save_curOuterRels;
		root->curOuterParams = save_curOuterParams;

		return subplan;
	}

	subplan = create_subplan(root, subpath);

	/* Only the needed columns should be projected from base rel. */
	disuse_physical_tlist(root, subplan, subpath);

	/* Add motion operator. */
	motion = cdbpathtoplan_create_motion_plan(root, path, subplan);

	copy_path_costsize(root, &motion->plan, (Path *) path);

	root->curOuterRels = save_curOuterRels;
	root->curOuterParams = save_curOuterParams;

	return (Plan *) motion;
}	/* create_motion_plan */


/*****************************************************************************
 *
 *	BASE-RELATION SCAN METHODS
 *
 *****************************************************************************/


/*
 * create_seqscan_plan
 *	 Returns a seqscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SeqScan *
create_seqscan_plan(PlannerInfo *root, Path *best_path,
					List *tlist, List *scan_clauses)
{
	SeqScan    *scan_plan;
	Index		scan_relid = best_path->parent->relid;

	/* it should be a base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->parent->rtekind == RTE_RELATION);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
	}

	scan_plan = make_seqscan(tlist,
							 scan_clauses,
							 scan_relid);

	copy_path_costsize(root, &scan_plan->plan, best_path);

	return scan_plan;
}

/*
 * create_externalscan_plan
 *	 Returns an externalscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 *
 *	 The external plan also includes the data format specification and file
 *	 location specification. Here is where we do the mapping of external file
 *	 to segment database and add it to the plan (or bail out of the mapping
 *	 rules are broken)
 *
 *	 Mapping rules
 *	 -------------
 *	 - 'file' protocol: each location (URI of local file) gets mapped to one
 *						and one only primary segdb.
 *	 - 'http' protocol: each location (URI of http server) gets mapped to one
 *						and one only primary segdb.
 *	 - 'gpfdist' and 'gpfdists' protocols: all locations (URI of gpfdist(s) client) are mapped
 *						to all primary segdbs. If there are less URIs than
 *						segdbs (usually the case) the URIs are duplicated
 *						so that there will be one for each segdb. However, if
 *						the GUC variable gp_external_max_segs is set to a num
 *						less than (total segdbs/total URIs) then we make sure
 *						that no URI gets mapped to more than this GUC number by
 *						skipping some segdbs randomly.
 *	 - 'exec' protocol: all segdbs get mapped to execute the command (this is
 *						soon to be changed though).
 */
static ExternalScan *
create_externalscan_plan(PlannerInfo *root, Path *best_path,
						 List *tlist, List *scan_clauses)
{
	ExternalScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;
	RelOptInfo *rel = best_path->parent;
	List	   *filenames;
	bool		ismasteronly = false;
	bool		islimitinrows = false;
	int			rejectlimit = -1;
	bool		logerrors = false;
	ExtTableEntry *ext = rel->extEntry;

	/* it should be an external rel... */
	Assert(scan_relid > 0);
	Assert(rel->rtekind == RTE_RELATION);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	Assert(ext->execlocations != NIL);

	if (ext->rejectlimit != -1)
	{
		/*
		 * single row error handling is requested, make sure reject limit and
		 * error table (if requested) are valid.
		 *
		 * NOTE: this should never happen unless somebody modified the catalog
		 * manually. We are just being pedantic here.
		 */
		VerifyRejectLimit(ext->rejectlimittype, ext->rejectlimit);
	}

	/* assign Uris to segments. */
	filenames = create_external_scan_uri_list(ext, &ismasteronly);

	/* data format description */
	Assert(ext->fmtopts);

	/* single row error handling */
	if (ext->rejectlimit != -1)
	{
		islimitinrows = (ext->rejectlimittype == 'r' ? true : false);
		rejectlimit = ext->rejectlimit;
		logerrors = ext->logerrors;
	}

	scan_plan = make_externalscan(tlist,
								  scan_clauses,
								  scan_relid,
								  filenames,
								  ext->fmtopts,
								  ext->fmtcode,
								  ismasteronly,
								  rejectlimit,
								  islimitinrows,
								  logerrors,
								  ext->encoding);

	copy_path_costsize(root, &scan_plan->scan.plan, best_path);

	return scan_plan;
}

List *
create_external_scan_uri_list(ExtTableEntry *ext, bool *ismasteronly)
{
	ListCell   *c;
	List	   *modifiedloclist = NIL;
	int			i;
	CdbComponentDatabases *db_info;
	int			total_primaries;
	char	  **segdb_file_map;

	/* various processing flags */
	bool		using_execute = false;	/* true if EXECUTE is used */
	bool		using_location; /* true if LOCATION is used */
	bool		found_candidate = false;
	bool		found_match = false;
	bool		done = false;
	List	   *filenames;

	/* gpfdist(s) or EXECUTE specific variables */
	int			total_to_skip = 0;
	int			max_participants_allowed = 0;
	int			num_segs_participating = 0;
	bool	   *skip_map = NULL;
	bool		should_skip_randomly = false;

	Uri		   *uri;
	char	   *on_clause;

	*ismasteronly = false;

	/* is this an EXECUTE table or a LOCATION (URI) table */
	if (ext->command)
	{
		using_execute = true;
		using_location = false;
	}
	else
	{
		using_execute = false;
		using_location = true;
	}

	/* is this an EXECUTE table or a LOCATION (URI) table */
	if (ext->command && !gp_external_enable_exec)
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_CONFIGURED),	/* any better errcode? */
				 errmsg("using external tables with OS level commands (EXECUTE clause) is disabled"),
				 errhint("To enable set gp_external_enable_exec=on.")));
	}

	/* various validations */
	if (ext->iswritable)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot read from a WRITABLE external table"),
				 errhint("Create the table as READABLE instead.")));

	/*
	 * take a peek at the first URI so we know which protocol we'll deal with
	 */
	if (!using_execute)
	{
		char	   *first_uri_str;

		first_uri_str = strVal(linitial(ext->urilocations));
		uri = ParseExternalTableUri(first_uri_str);
	}
	else
		uri = NULL;

	/* get the ON clause information, and restrict 'ON MASTER' to custom
	 * protocols only */
	on_clause = (char *) strVal(linitial(ext->execlocations));
	if ((strcmp(on_clause, "MASTER_ONLY") == 0)
		&& using_location && (uri->protocol != URI_CUSTOM)) {
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				errmsg("\'ON MASTER\' is not supported by this protocol yet")));
	}

	/* get the total valid primary segdb count */
	db_info = cdbcomponent_getCdbComponents();
	total_primaries = 0;
	for (i = 0; i < db_info->total_segment_dbs; i++)
	{
		CdbComponentDatabaseInfo *p = &db_info->segment_db_info[i];

		if (SEGMENT_IS_ACTIVE_PRIMARY(p))
			total_primaries++;
	}

	/*
	 * initialize a file-to-segdb mapping. segdb_file_map string array indexes
	 * segindex and the entries are the external file path is assigned to this
	 * segment database. For example if segdb_file_map[2] has "/tmp/emp.1" then
	 * this file is assigned to primary segdb 2. if an entry has NULL then
	 * that segdb isn't assigned any file.
	 */
	segdb_file_map = (char **) palloc0(total_primaries * sizeof(char *));

	/*
	 * Now we do the actual assignment of work to the segment databases (where
	 * work is either a URI to open or a command to execute). Due to the big
	 * differences between the different protocols we handle each one
	 * separately. Unfortunately this means some code duplication, but keeping
	 * this separation makes the code much more understandable and (even) more
	 * maintainable.
	 *
	 * Outline of the following code blocks (from simplest to most complex):
	 * (only one of these will get executed for a statement)
	 *
	 * 1) segment mapping for tables with LOCATION http:// or file:// .
	 *
	 * These two protocols are very similar in that they enforce a
	 * 1-URI:1-segdb relationship. The only difference between them is that
	 * file:// URI must be assigned to a segdb on a host that is local to that
	 * URI.
	 *
	 * 2) segment mapping for tables with LOCATION gpfdist(s):// or custom
	 * protocol
	 *
	 * This protocol is more complicated - in here we usually duplicate the
	 * user supplied gpfdist(s):// URIs until there is one available to every
	 * segdb. However, in some cases (as determined by gp_external_max_segs
	 * GUC) we don't want to use *all* segdbs but instead figure out how many
	 * and pick them randomly (this is mainly for better performance and
	 * resource mgmt).
	 *
	 * 3) segment mapping for tables with EXECUTE 'cmd' ON.
	 *
	 * In here we don't have URI's. We have a single command string and a
	 * specification of the segdb granularity it should get executed on (the
	 * ON clause). Depending on the ON clause specification we could go many
	 * different ways, for example: assign the command to all segdb, or one
	 * command per host, or assign to 5 random segments, etc...
	 */

	/* (1) */
	if (using_location && (uri->protocol == URI_FILE || uri->protocol == URI_HTTP))
	{
		/*
		 * extract file path and name from URI strings and assign them a
		 * primary segdb
		 */
		foreach(c, ext->urilocations)
		{
			const char *uri_str = (char *) strVal(lfirst(c));

			uri = ParseExternalTableUri(uri_str);

			found_candidate = false;
			found_match = false;

			/*
			 * look through our segment database list and try to find a
			 * database that can handle this uri.
			 */
			for (i = 0; i < db_info->total_segment_dbs && !found_match; i++)
			{
				CdbComponentDatabaseInfo *p = &db_info->segment_db_info[i];
				int segind = p->config->segindex;

				/*
				 * Assign mapping of external file to this segdb only if:
				 * 1) This segdb is a valid primary.
				 * 2) An external file wasn't already assigned to it.
				 * 3) If 'file' protocol, host of segdb and file must be
				 *    the same.
				 *
				 * This logic also guarantees that file that appears first in
				 * the external location list for the same host gets assigned
				 * the segdb with the lowest index for this host.
				 */
				if (SEGMENT_IS_ACTIVE_PRIMARY(p))
				{
					if (uri->protocol == URI_FILE)
					{
						if (pg_strcasecmp(uri->hostname, p->config->hostname) != 0 && pg_strcasecmp(uri->hostname, p->config->address) != 0)
							continue;
					}

					/* a valid primary segdb exist on this host */
					found_candidate = true;

					if (segdb_file_map[segind] == NULL)
					{
						/* segdb not taken yet. assign this URI to this segdb */
						segdb_file_map[segind] = pstrdup(uri_str);
						found_match = true;
					}

					/*
					 * too bad. this segdb already has an external source
					 * assigned
					 */
				}
			}

			/*
			 * We failed to find a segdb for this URI.
			 */
			if (!found_match)
			{
				if (uri->protocol == URI_FILE)
				{
					if (found_candidate)
					{
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								 errmsg("could not assign a segment database for \"%s\"",
										uri_str),
								 errdetail("There are more external files than primary segment databases on host \"%s\"",
										   uri->hostname)));
					}
					else
					{
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								 errmsg("could not assign a segment database for \"%s\"",
										uri_str),
								 errdetail("There isn't a valid primary segment database on host \"%s\"",
										   uri->hostname)));
					}
				}
				else	/* HTTP */
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("could not assign a segment database for \"%s\"",
									uri_str),
							 errdetail("There are more URIs than total primary segment databases")));
				}
			}
		}


	}
	/* (2) */
	else if (using_location && (uri->protocol == URI_GPFDIST ||
							   uri->protocol == URI_GPFDISTS ||
							   uri->protocol == URI_CUSTOM))
	{
		if ((strcmp(on_clause, "MASTER_ONLY") == 0) && (uri->protocol == URI_CUSTOM))
		{
			const char *uri_str = strVal(linitial(ext->urilocations));
			segdb_file_map[0] = pstrdup(uri_str);
			*ismasteronly = true;
		}
		else
		{
			/*
			 * Re-write the location list for GPFDIST or GPFDISTS before mapping to segments.
			 *
			 * If we happen to be dealing with URI's with the 'gpfdist' (or 'gpfdists') protocol
			 * we do an extra step here.
			 *
			 * (*) We modify the urilocationlist so that every
			 * primary segdb will get a URI (therefore we duplicate the existing
			 * URI's until the list is of size = total_primaries).
			 * Example: 2 URIs, 7 total segdbs.
			 * Original LocationList: URI1->URI2
			 * Modified LocationList: URI1->URI2->URI1->URI2->URI1->URI2->URI1
			 *
			 * (**) We also make sure that we don't allocate more segdbs than
			 * (# of URIs x gp_external_max_segs).
			 * Example: 2 URIs, 7 total segdbs, gp_external_max_segs = 3
			 * Original LocationList: URI1->URI2
			 * Modified LocationList: URI1->URI2->URI1->URI2->URI1->URI2 (6 total).
			 *
			 * (***) In that case that we need to allocate only a subset of primary
			 * segdbs and not all we then also create a random map of segments to skip.
			 * Using the previous example a we create a map of 7 entries and need to
			 * randomly select 1 segdb to skip (7 - 6 = 1). so it may look like this:
			 * [F F T F F F F] - in which case we know to skip the 3rd segment only.
			 */

			/* total num of segs that will participate in the external operation */
			num_segs_participating = total_primaries;

			/* max num of segs that are allowed to participate in the operation */
			if ((uri->protocol == URI_GPFDIST) || (uri->protocol == URI_GPFDISTS))
			{
				max_participants_allowed = list_length(ext->urilocations) *
					gp_external_max_segs;
			}
			else
			{
				/*
				 * for custom protocol, set max_participants_allowed to
				 * num_segs_participating so that assignment to segments will use
				 * all available segments
				 */
				max_participants_allowed = num_segs_participating;
			}

			elog(DEBUG5,
				 "num_segs_participating = %d. max_participants_allowed = %d. number of URIs = %d",
				 num_segs_participating, max_participants_allowed, list_length(ext->urilocations));

			/* see (**) above */
			if (num_segs_participating > max_participants_allowed)
			{
				total_to_skip = num_segs_participating - max_participants_allowed;
				num_segs_participating = max_participants_allowed;
				should_skip_randomly = true;

				elog(NOTICE, "External scan %s will utilize %d out "
					 "of %d segment databases",
					 (uri->protocol == URI_GPFDIST ? "from gpfdist(s) server" : "using custom protocol"),
					 num_segs_participating,
					 total_primaries);
			}

			if (list_length(ext->urilocations) > num_segs_participating)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("there are more external files (URLs) than primary segments that can read them"),
						 errdetail("Found %d URLs and %d primary segments.",
								   list_length(ext->urilocations),
								   num_segs_participating)));

			/*
			 * restart location list and fill in new list until number of
			 * locations equals the number of segments participating in this
			 * action (see (*) above for more details).
			 */
			while (!done)
			{
				foreach(c, ext->urilocations)
				{
					char	   *uri_str = (char *) strVal(lfirst(c));

					/* append to a list of Value nodes, size nelems */
					modifiedloclist = lappend(modifiedloclist, makeString(pstrdup(uri_str)));

					if (list_length(modifiedloclist) == num_segs_participating)
					{
						done = true;
						break;
					}

					if (list_length(modifiedloclist) > num_segs_participating)
					{
						elog(ERROR, "External scan location list failed building distribution.");
					}
				}
			}

			/* See (***) above for details */
			if (should_skip_randomly)
				skip_map = makeRandomSegMap(total_primaries, total_to_skip);

			/*
			 * assign each URI from the new location list a primary segdb
			 */
			foreach(c, modifiedloclist)
			{
				const char *uri_str = strVal(lfirst(c));

				uri = ParseExternalTableUri(uri_str);

				found_candidate = false;
				found_match = false;

				/*
				 * look through our segment database list and try to find a
				 * database that can handle this uri.
				 */
				for (i = 0; i < db_info->total_segment_dbs && !found_match; i++)
				{
					CdbComponentDatabaseInfo *p = &db_info->segment_db_info[i];
					int			segind = p->config->segindex;

					/*
					 * Assign mapping of external file to this segdb only if:
					 * 1) This segdb is a valid primary.
					 * 2) An external file wasn't already assigned to it.
					 */
					if (SEGMENT_IS_ACTIVE_PRIMARY(p))
					{
						/*
						 * skip this segdb if skip_map for this seg index tells us
						 * to skip it (set to 'true').
						 */
						if (should_skip_randomly)
						{
							Assert(segind < total_primaries);

							if (skip_map[segind])
								continue;	/* skip it */
						}

						/* a valid primary segdb exist on this host */
						found_candidate = true;

						if (segdb_file_map[segind] == NULL)
						{
							/* segdb not taken yet. assign this URI to this segdb */
							segdb_file_map[segind] = pstrdup(uri_str);
							found_match = true;
						}

						/*
						 * too bad. this segdb already has an external source
						 * assigned
						 */
					}
				}

				/* We failed to find a segdb for this gpfdist(s) URI */
				if (!found_match)
				{
					/* should never happen */
					elog(LOG,
						 "external tables gpfdist(s) allocation error. "
						 "total_primaries: %d, num_segs_participating %d "
						 "max_participants_allowed %d, total_to_skip %d",
						 total_primaries, num_segs_participating,
						 max_participants_allowed, total_to_skip);

					elog(ERROR,
						 "internal error in createplan for external tables when trying to assign segments for gpfdist(s)");
				}
			}
		}
	}
	/* (3) */
	else if (using_execute)
	{
		const char *command = ext->command;
		const char *prefix = "execute:";
		char	   *prefixed_command;

		/* build the command string for the executor - 'execute:command' */
		StringInfo	buf = makeStringInfo();

		appendStringInfo(buf, "%s%s", prefix, command);
		prefixed_command = pstrdup(buf->data);

		pfree(buf->data);
		pfree(buf);
		buf = NULL;

		/*
		 * Now we handle each one of the ON locations separately:
		 *
		 * 1) all segs
		 * 2) one per host
		 * 3) all segs on host <foo>
		 * 4) seg <n> only
		 * 5) <n> random segs
		 * 6) master only
		 */
		if (strcmp(on_clause, "ALL_SEGMENTS") == 0)
		{
			/* all segments get a copy of the command to execute */

			for (i = 0; i < db_info->total_segment_dbs; i++)
			{
				CdbComponentDatabaseInfo *p = &db_info->segment_db_info[i];
				int			segind = p->config->segindex;

				if (SEGMENT_IS_ACTIVE_PRIMARY(p))
					segdb_file_map[segind] = pstrdup(prefixed_command);
			}

		}
		else if (strcmp(on_clause, "PER_HOST") == 0)
		{
			/* 1 seg per host */

			List	   *visited_hosts = NIL;
			ListCell   *lc;

			for (i = 0; i < db_info->total_segment_dbs; i++)
			{
				CdbComponentDatabaseInfo *p = &db_info->segment_db_info[i];
				int			segind = p->config->segindex;

				if (SEGMENT_IS_ACTIVE_PRIMARY(p))
				{
					bool		host_taken = false;

					foreach(lc, visited_hosts)
					{
						const char *hostname = strVal(lfirst(lc));

						if (pg_strcasecmp(hostname, p->config->hostname) == 0)
						{
							host_taken = true;
							break;
						}
					}

					/*
					 * if not assigned to a seg on this host before - do it
					 * now and add this hostname to the list so that we don't
					 * use segs on this host again.
					 */
					if (!host_taken)
					{
						segdb_file_map[segind] = pstrdup(prefixed_command);
						visited_hosts = lappend(visited_hosts,
										   makeString(pstrdup(p->config->hostname)));
					}
				}
			}
		}
		else if (strncmp(on_clause, "HOST:", strlen("HOST:")) == 0)
		{
			/* all segs on the specified host get copy of the command */
			char	   *hostname = on_clause + strlen("HOST:");
			bool		match_found = false;

			for (i = 0; i < db_info->total_segment_dbs; i++)
			{
				CdbComponentDatabaseInfo *p = &db_info->segment_db_info[i];
				int			segind = p->config->segindex;

				if (SEGMENT_IS_ACTIVE_PRIMARY(p) &&
					pg_strcasecmp(hostname, p->config->hostname) == 0)
				{
					segdb_file_map[segind] = pstrdup(prefixed_command);
					match_found = true;
				}
			}

			if (!match_found)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("could not assign a segment database for command \"%s\")",
								command),
						 errdetail("No valid primary segment was found in the requested host name \"%s\".",
								hostname)));
		}
		else if (strncmp(on_clause, "SEGMENT_ID:", strlen("SEGMENT_ID:")) == 0)
		{
			/* 1 seg with specified id gets a copy of the command */
			int			target_segid = atoi(on_clause + strlen("SEGMENT_ID:"));
			bool		match_found = false;

			for (i = 0; i < db_info->total_segment_dbs; i++)
			{
				CdbComponentDatabaseInfo *p = &db_info->segment_db_info[i];
				int			segind = p->config->segindex;

				if (SEGMENT_IS_ACTIVE_PRIMARY(p) && segind == target_segid)
				{
					segdb_file_map[segind] = pstrdup(prefixed_command);
					match_found = true;
				}
			}

			if (!match_found)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("could not assign a segment database for command \"%s\"",
								command),
						 errdetail("The requested segment id %d is not a valid primary segment or doesn't exist in the database",
								   target_segid)));
		}
		else if (strncmp(on_clause, "TOTAL_SEGS:", strlen("TOTAL_SEGS:")) == 0)
		{
			/* total n segments selected randomly */

			int			num_segs_to_use = atoi(on_clause + strlen("TOTAL_SEGS:"));

			if (num_segs_to_use > total_primaries)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("table defined with EXECUTE ON %d but there are only %d valid primary segments in the database",
								num_segs_to_use, total_primaries)));

			total_to_skip = total_primaries - num_segs_to_use;
			skip_map = makeRandomSegMap(total_primaries, total_to_skip);

			for (i = 0; i < db_info->total_segment_dbs; i++)
			{
				CdbComponentDatabaseInfo *p = &db_info->segment_db_info[i];
				int			segind = p->config->segindex;

				if (SEGMENT_IS_ACTIVE_PRIMARY(p))
				{
					Assert(segind < total_primaries);
					if (skip_map[segind])
						continue;		/* skip it */

					segdb_file_map[segind] = pstrdup(prefixed_command);
				}
			}
		}
		else if (strcmp(on_clause, "MASTER_ONLY") == 0)
		{
			/*
			 * store the command in first array entry and indicate that it is
			 * meant for the master segment (not seg o).
			 */
			segdb_file_map[0] = pstrdup(prefixed_command);
			*ismasteronly = true;
		}
		else
		{
			elog(ERROR, "Internal error in createplan for external tables: got invalid ON clause code %s",
				 on_clause);
		}
	}
	else
	{
		/* should never get here */
		elog(ERROR, "Internal error in createplan for external tables");
	}

	/*
	 * convert array map to a list so it can be serialized as part of the plan
	 */
	filenames = NIL;
	for (i = 0; i < total_primaries; i++)
	{
		if (segdb_file_map[i] != NULL)
			filenames = lappend(filenames, makeString(segdb_file_map[i]));
		else
		{
			/* no file for this segdb. add a null entry */
			Value	   *n = makeNode(Value);

			n->type = T_Null;
			filenames = lappend(filenames, n);
		}
	}

	return filenames;
}


/*
 * create_indexscan_plan
 *	  Returns an indexscan plan for the base relation scanned by 'best_path'
 *	  with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 *
 * We use this for both plain IndexScans and IndexOnlyScans, because the
 * qual preprocessing work is the same for both.  Note that the caller tells
 * us which to build --- we don't look at best_path->path.pathtype, because
 * create_bitmap_subplan needs to be able to override the prior decision.
 */
static Scan *
create_indexscan_plan(PlannerInfo *root,
					  IndexPath *best_path,
					  List *tlist,
					  List *scan_clauses,
					  bool indexonly)
{
	Scan	   *scan_plan;
	List	   *indexquals = best_path->indexquals;
	List	   *indexorderbys = best_path->indexorderbys;
	Index		baserelid = best_path->path.parent->relid;
	Oid			indexoid = best_path->indexinfo->indexoid;
	List	   *qpqual;
	List	   *stripped_indexquals;
	List	   *fixed_indexquals;
	List	   *fixed_indexorderbys;
	ListCell   *l;

	/* it should be a base rel... */
	Assert(baserelid > 0);
	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	/*
	 * Build "stripped" indexquals structure (no RestrictInfos) to pass to
	 * executor as indexqualorig
	 */
	stripped_indexquals = get_actual_clauses(indexquals);

	/*
	 * The executor needs a copy with the indexkey on the left of each clause
	 * and with index Vars substituted for table ones.
	 */
	fixed_indexquals = fix_indexqual_references(root, best_path);

	/*
	 * Likewise fix up index attr references in the ORDER BY expressions.
	 */
	fixed_indexorderbys = fix_indexorderby_references(root, best_path);

	/*
	 * The qpqual list must contain all restrictions not automatically handled
	 * by the index, other than pseudoconstant clauses which will be handled
	 * by a separate gating plan node.  All the predicates in the indexquals
	 * will be checked (either by the index itself, or by nodeIndexscan.c),
	 * but if there are any "special" operators involved then they must be
	 * included in qpqual.  The upshot is that qpqual must contain
	 * scan_clauses minus whatever appears in indexquals.
	 *
	 * In normal cases simple pointer equality checks will be enough to spot
	 * duplicate RestrictInfos, so we try that first.
	 *
	 * Another common case is that a scan_clauses entry is generated from the
	 * same EquivalenceClass as some indexqual, and is therefore redundant
	 * with it, though not equal.  (This happens when indxpath.c prefers a
	 * different derived equality than what generate_join_implied_equalities
	 * picked for a parameterized scan's ppi_clauses.)
	 *
	 * In some situations (particularly with OR'd index conditions) we may
	 * have scan_clauses that are not equal to, but are logically implied by,
	 * the index quals; so we also try a predicate_implied_by() check to see
	 * if we can discard quals that way.  (predicate_implied_by assumes its
	 * first input contains only immutable functions, so we have to check
	 * that.)
	 *
	 * We can also discard quals that are implied by a partial index's
	 * predicate, but only in a plain SELECT; when scanning a target relation
	 * of UPDATE/DELETE/SELECT FOR UPDATE, we must leave such quals in the
	 * plan so that they'll be properly rechecked by EvalPlanQual testing.
	 */
	qpqual = NIL;
	foreach(l, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		Assert(IsA(rinfo, RestrictInfo));
		if (rinfo->pseudoconstant)
			continue;			/* we may drop pseudoconstants here */
		if (list_member_ptr(indexquals, rinfo))
			continue;			/* simple duplicate */
		if (is_redundant_derived_clause(rinfo, indexquals))
			continue;			/* derived from same EquivalenceClass */
		if (!contain_mutable_functions((Node *) rinfo->clause))
		{
			List	   *clausel = list_make1(rinfo->clause);

			if (predicate_implied_by(clausel, indexquals))
				continue;		/* provably implied by indexquals */
			if (best_path->indexinfo->indpred)
			{
				if (baserelid != root->parse->resultRelation &&
					get_plan_rowmark(root->rowMarks, baserelid) == NULL)
					if (predicate_implied_by(clausel,
											 best_path->indexinfo->indpred))
						continue;		/* implied by index predicate */
			}
		}
		qpqual = lappend(qpqual, rinfo);
	}

	/* Sort clauses into best execution order */
	qpqual = order_qual_clauses(root, qpqual);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	qpqual = extract_actual_clauses(qpqual, false);

	/*
	 * We have to replace any outer-relation variables with nestloop params in
	 * the indexqualorig, qpqual, and indexorderbyorig expressions.  A bit
	 * annoying to have to do this separately from the processing in
	 * fix_indexqual_references --- rethink this when generalizing the inner
	 * indexscan support.  But note we can't really do this earlier because
	 * it'd break the comparisons to predicates above ... (or would it?  Those
	 * wouldn't have outer refs)
	 */
	if (best_path->path.param_info)
	{
		stripped_indexquals = (List *)
			replace_nestloop_params(root, (Node *) stripped_indexquals);
		qpqual = (List *)
			replace_nestloop_params(root, (Node *) qpqual);
		indexorderbys = (List *)
			replace_nestloop_params(root, (Node *) indexorderbys);
	}

	/* Finally ready to build the plan node */
	if (indexonly)
		scan_plan = (Scan *) make_indexonlyscan(tlist,
												qpqual,
												baserelid,
												indexoid,
												fixed_indexquals,
												stripped_indexquals,
												fixed_indexorderbys,
												best_path->indexinfo->indextlist,
												best_path->indexscandir);
	else
		scan_plan = (Scan *) make_indexscan(tlist,
											qpqual,
											baserelid,
											indexoid,
											fixed_indexquals,
											stripped_indexquals,
											fixed_indexorderbys,
											indexorderbys,
											best_path->indexscandir);

	copy_path_costsize(root, &scan_plan->plan, &best_path->path);

	return scan_plan;
}

/*
 * create_bitmap_scan_plan
 *	  Returns a bitmap scan plan for the base relation scanned by 'best_path'
 *	  with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static BitmapHeapScan *
create_bitmap_scan_plan(PlannerInfo *root,
						BitmapHeapPath *best_path,
						List *tlist,
						List *scan_clauses)
{
	Index		baserelid = best_path->path.parent->relid;
	Plan	   *bitmapqualplan;
	List	   *bitmapqualorig;
	List	   *indexquals;
	List	   *indexECs;
	List	   *qpqual;
	ListCell   *l;
	BitmapHeapScan *scan_plan;

	/* it should be a base rel... */
	Assert(baserelid > 0);
	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	/* Process the bitmapqual tree into a Plan tree and qual lists */
	bitmapqualplan = create_bitmap_subplan(root, best_path->bitmapqual,
										   &bitmapqualorig, &indexquals,
										   &indexECs);

	/*
	 * The qpqual list must contain all restrictions not automatically handled
	 * by the index, other than pseudoconstant clauses which will be handled
	 * by a separate gating plan node.  All the predicates in the indexquals
	 * will be checked (either by the index itself, or by
	 * nodeBitmapHeapscan.c), but if there are any "special" operators
	 * involved then they must be added to qpqual.  The upshot is that qpqual
	 * must contain scan_clauses minus whatever appears in indexquals.
	 *
	 * This loop is similar to the comparable code in create_indexscan_plan(),
	 * but with some differences because it has to compare the scan clauses to
	 * stripped (no RestrictInfos) indexquals.  See comments there for more
	 * info.
	 *
	 * In normal cases simple equal() checks will be enough to spot duplicate
	 * clauses, so we try that first.  We next see if the scan clause is
	 * redundant with any top-level indexqual by virtue of being generated
	 * from the same EC.  After that, try predicate_implied_by().
	 *
	 * Unlike create_indexscan_plan(), we need take no special thought here
	 * for partial index predicates; this is because the predicate conditions
	 * are already listed in bitmapqualorig and indexquals.  Bitmap scans have
	 * to do it that way because predicate conditions need to be rechecked if
	 * the scan becomes lossy, so they have to be included in bitmapqualorig.
	 */
	qpqual = NIL;
	foreach(l, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
		Node	   *clause = (Node *) rinfo->clause;

		Assert(IsA(rinfo, RestrictInfo));
		if (rinfo->pseudoconstant)
			continue;			/* we may drop pseudoconstants here */
		if (list_member(indexquals, clause))
			continue;			/* simple duplicate */
		if (rinfo->parent_ec && list_member_ptr(indexECs, rinfo->parent_ec))
			continue;			/* derived from same EquivalenceClass */
		if (!contain_mutable_functions(clause))
		{
			List	   *clausel = list_make1(clause);

			if (predicate_implied_by(clausel, indexquals))
				continue;		/* provably implied by indexquals */
		}
		qpqual = lappend(qpqual, rinfo);
	}

	/* Sort clauses into best execution order */
	qpqual = order_qual_clauses(root, qpqual);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	qpqual = extract_actual_clauses(qpqual, false);

	/*
	 * When dealing with special operators, we will at this point have
	 * duplicate clauses in qpqual and bitmapqualorig.  We may as well drop
	 * 'em from bitmapqualorig, since there's no point in making the tests
	 * twice.
	 */
	bitmapqualorig = list_difference_ptr(bitmapqualorig, qpqual);

	/*
	 * We have to replace any outer-relation variables with nestloop params in
	 * the qpqual and bitmapqualorig expressions.  (This was already done for
	 * expressions attached to plan nodes in the bitmapqualplan tree.)
	 */
	if (best_path->path.param_info)
	{
		qpqual = (List *)
			replace_nestloop_params(root, (Node *) qpqual);
		bitmapqualorig = (List *)
			replace_nestloop_params(root, (Node *) bitmapqualorig);
	}

	/* Finally ready to build the plan node */
	scan_plan = make_bitmap_heapscan(tlist,
									 qpqual,
									 bitmapqualplan,
									 bitmapqualorig,
									 baserelid);

	copy_path_costsize(root, &scan_plan->scan.plan, &best_path->path);

	return scan_plan;
}

/*
 * Given a bitmapqual tree, generate the Plan tree that implements it
 *
 * As byproducts, we also return in *qual and *indexqual the qual lists
 * (in implicit-AND form, without RestrictInfos) describing the original index
 * conditions and the generated indexqual conditions.  (These are the same in
 * simple cases, but when special index operators are involved, the former
 * list includes the special conditions while the latter includes the actual
 * indexable conditions derived from them.)  Both lists include partial-index
 * predicates, because we have to recheck predicates as well as index
 * conditions if the bitmap scan becomes lossy.
 *
 * In addition, we return a list of EquivalenceClass pointers for all the
 * top-level indexquals that were possibly-redundantly derived from ECs.
 * This allows removal of scan_clauses that are redundant with such quals.
 * (We do not attempt to detect such redundancies for quals that are within
 * OR subtrees.  This could be done in a less hacky way if we returned the
 * indexquals in RestrictInfo form, but that would be slower and still pretty
 * messy, since we'd have to build new RestrictInfos in many cases.)
 */
static Plan *
create_bitmap_subplan(PlannerInfo *root, Path *bitmapqual,
					  List **qual, List **indexqual, List **indexECs)
{
	Plan	   *plan;

	if (IsA(bitmapqual, BitmapAndPath))
	{
		BitmapAndPath *apath = (BitmapAndPath *) bitmapqual;
		List	   *subplans = NIL;
		List	   *subquals = NIL;
		List	   *subindexquals = NIL;
		List	   *subindexECs = NIL;
		ListCell   *l;

		/*
		 * There may well be redundant quals among the subplans, since a
		 * top-level WHERE qual might have gotten used to form several
		 * different index quals.  We don't try exceedingly hard to eliminate
		 * redundancies, but we do eliminate obvious duplicates by using
		 * list_concat_unique.
		 */
		foreach(l, apath->bitmapquals)
		{
			Plan	   *subplan;
			List	   *subqual;
			List	   *subindexqual;
			List	   *subindexEC;

			subplan = create_bitmap_subplan(root, (Path *) lfirst(l),
											&subqual, &subindexqual,
											&subindexEC);
			subplans = lappend(subplans, subplan);
			subquals = list_concat_unique(subquals, subqual);
			subindexquals = list_concat_unique(subindexquals, subindexqual);
			/* Duplicates in indexECs aren't worth getting rid of */
			subindexECs = list_concat(subindexECs, subindexEC);
		}
		plan = (Plan *) make_bitmap_and(subplans);
		plan->startup_cost = apath->path.startup_cost;
		plan->total_cost = apath->path.total_cost;
		plan->plan_rows =
			clamp_row_est(apath->bitmapselectivity * apath->path.parent->tuples);
		plan->plan_width = 0;	/* meaningless */
		*qual = subquals;
		*indexqual = subindexquals;
		*indexECs = subindexECs;
	}
	else if (IsA(bitmapqual, BitmapOrPath))
	{
		BitmapOrPath *opath = (BitmapOrPath *) bitmapqual;
		List	   *subplans = NIL;
		List	   *subquals = NIL;
		List	   *subindexquals = NIL;
		bool		const_true_subqual = false;
		bool		const_true_subindexqual = false;
		ListCell   *l;

		/*
		 * Here, we only detect qual-free subplans.  A qual-free subplan would
		 * cause us to generate "... OR true ..."  which we may as well reduce
		 * to just "true".  We do not try to eliminate redundant subclauses
		 * because (a) it's not as likely as in the AND case, and (b) we might
		 * well be working with hundreds or even thousands of OR conditions,
		 * perhaps from a long IN list.  The performance of list_append_unique
		 * would be unacceptable.
		 */
		foreach(l, opath->bitmapquals)
		{
			Plan	   *subplan;
			List	   *subqual;
			List	   *subindexqual;
			List	   *subindexEC;

			subplan = create_bitmap_subplan(root, (Path *) lfirst(l),
											&subqual, &subindexqual,
											&subindexEC);
			subplans = lappend(subplans, subplan);
			if (subqual == NIL)
				const_true_subqual = true;
			else if (!const_true_subqual)
				subquals = lappend(subquals,
								   make_ands_explicit(subqual));
			if (subindexqual == NIL)
				const_true_subindexqual = true;
			else if (!const_true_subindexqual)
				subindexquals = lappend(subindexquals,
										make_ands_explicit(subindexqual));
		}

		/*
		 * In the presence of ScalarArrayOpExpr quals, we might have built
		 * BitmapOrPaths with just one subpath; don't add an OR step.
		 */
		if (list_length(subplans) == 1)
		{
			plan = (Plan *) linitial(subplans);
		}
		else
		{
			plan = (Plan *) make_bitmap_or(subplans);
			plan->startup_cost = opath->path.startup_cost;
			plan->total_cost = opath->path.total_cost;
			plan->plan_rows =
				clamp_row_est(opath->bitmapselectivity * opath->path.parent->tuples);
			plan->plan_width = 0;		/* meaningless */
		}

		/*
		 * If there were constant-TRUE subquals, the OR reduces to constant
		 * TRUE.  Also, avoid generating one-element ORs, which could happen
		 * due to redundancy elimination or ScalarArrayOpExpr quals.
		 */
		if (const_true_subqual)
			*qual = NIL;
		else if (list_length(subquals) <= 1)
			*qual = subquals;
		else
			*qual = list_make1(make_orclause(subquals));
		if (const_true_subindexqual)
			*indexqual = NIL;
		else if (list_length(subindexquals) <= 1)
			*indexqual = subindexquals;
		else
			*indexqual = list_make1(make_orclause(subindexquals));
		*indexECs = NIL;
	}
	else if (IsA(bitmapqual, IndexPath))
	{
		IndexPath  *ipath = (IndexPath *) bitmapqual;
		IndexScan  *iscan;
		List	   *subindexECs;
		ListCell   *l;

		/* Use the regular indexscan plan build machinery... */
		iscan = (IndexScan *) create_indexscan_plan(root, ipath,
													NIL, NIL, false);
		Assert(IsA(iscan, IndexScan));
		/* then convert to a bitmap indexscan */
		plan = (Plan *) make_bitmap_indexscan(iscan->scan.scanrelid,
											  iscan->indexid,
											  iscan->indexqual,
											  iscan->indexqualorig);
		plan->startup_cost = 0.0;
		plan->total_cost = ipath->indextotalcost;
		plan->plan_rows =
			clamp_row_est(ipath->indexselectivity * ipath->path.parent->tuples);
		plan->plan_width = 0;	/* meaningless */
		*qual = get_actual_clauses(ipath->indexclauses);
		*indexqual = get_actual_clauses(ipath->indexquals);
		foreach(l, ipath->indexinfo->indpred)
		{
			Expr	   *pred = (Expr *) lfirst(l);

			/*
			 * We know that the index predicate must have been implied by the
			 * query condition as a whole, but it may or may not be implied by
			 * the conditions that got pushed into the bitmapqual.  Avoid
			 * generating redundant conditions.
			 */
			if (!predicate_implied_by(list_make1(pred), ipath->indexclauses))
			{
				*qual = lappend(*qual, pred);
				*indexqual = lappend(*indexqual, pred);
			}
		}
		subindexECs = NIL;
		foreach(l, ipath->indexquals)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

			if (rinfo->parent_ec)
				subindexECs = lappend(subindexECs, rinfo->parent_ec);
		}
		*indexECs = subindexECs;
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d", nodeTag(bitmapqual));
		plan = NULL;			/* keep compiler quiet */
	}

	return plan;
}

/*
 * create_tidscan_plan
 *	 Returns a tidscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static TidScan *
create_tidscan_plan(PlannerInfo *root, TidPath *best_path,
					List *tlist, List *scan_clauses)
{
	TidScan    *scan_plan;
	Index		scan_relid = best_path->path.parent->relid;
	List	   *tidquals = best_path->tidquals;
	List	   *ortidquals;

	/* it should be a base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->path.param_info)
	{
		tidquals = (List *)
			replace_nestloop_params(root, (Node *) tidquals);
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
	}

	/*
	 * Remove any clauses that are TID quals.  This is a bit tricky since the
	 * tidquals list has implicit OR semantics.
	 *
	 * In the case of CURRENT OF, however, we do want the CurrentOfExpr to
	 * reside in both the tidlist and the qual, as CurrentOfExpr is effectively
	 * a ctid, gp_segment_id, and tableoid qual. Constant folding will
	 * finish up this qual rewriting to ensure what we dispatch is a sane interpretation
	 * of CURRENT OF behavior.
	 */
	if (!(list_length(scan_clauses) == 1 && IsA(linitial(scan_clauses), CurrentOfExpr)))
	{
		ortidquals = tidquals;
		if (list_length(ortidquals) > 1)
			ortidquals = list_make1(make_orclause(ortidquals));
		scan_clauses = list_difference(scan_clauses, ortidquals);
	}

	scan_plan = make_tidscan(tlist,
							 scan_clauses,
							 scan_relid,
							 tidquals);

	copy_path_costsize(root, &scan_plan->scan.plan, &best_path->path);

	return scan_plan;
}

/*
 * create_subqueryscan_plan
 *	 Returns a subqueryscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SubqueryScan *
create_subqueryscan_plan(PlannerInfo *root, Path *best_path,
						 List *tlist, List *scan_clauses)
{
	SubqueryScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;

	/* it should be a subquery base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->parent->rtekind == RTE_SUBQUERY);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
		process_subquery_nestloop_params(root,
										 best_path->parent->subplan_params);
	}

	scan_plan = make_subqueryscan(tlist,
								  scan_clauses,
								  scan_relid,
								  best_path->parent->subplan);

	copy_path_costsize(root, &scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_functionscan_plan
 *	 Returns a functionscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static FunctionScan *
create_functionscan_plan(PlannerInfo *root, Path *best_path,
						 List *tlist, List *scan_clauses)
{
	FunctionScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;
	RangeTblEntry *rte;
	List	   *functions;

	/* it should be a function base rel... */
	Assert(scan_relid > 0);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(rte->rtekind == RTE_FUNCTION);
	functions = rte->functions;

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
		/* The function expressions could contain nestloop params, too */
		functions = (List *) replace_nestloop_params(root, (Node *) functions);
	}

	scan_plan = make_functionscan(tlist, scan_clauses, scan_relid,
								  functions, rte->funcordinality);

	copy_path_costsize(root, &scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_tablefunction_plan
 *	 Returns a TableFunction plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static TableFunctionScan *
create_tablefunction_plan(PlannerInfo *root,
						  Path *best_path,
						  List *tlist,
						  List *scan_clauses)
{
	TableFunctionScan *tablefunc;
	Plan	   *subplan = best_path->parent->subplan;
	Index		scan_relid = best_path->parent->relid;
	RangeTblEntry *rte;
	RangeTblFunction *rtf;

	/* it should be a function base rel... */
	Assert(scan_relid > 0);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(best_path->parent->rtekind == RTE_TABLEFUNCTION);
	Assert(list_length(rte->functions) == 1);
	rtf = linitial(rte->functions);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the TableFunctionScan plan */
	tablefunc = make_tablefunction(tlist, scan_clauses, subplan, scan_relid, rtf);

	/* Cost is determined largely by the cost of the underlying subplan */
	copy_plan_costsize(&tablefunc->scan.plan, subplan);

	copy_path_costsize(root, &tablefunc->scan.plan, best_path);

	return tablefunc;
}

/*
 * create_valuesscan_plan
 *	 Returns a valuesscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static ValuesScan *
create_valuesscan_plan(PlannerInfo *root, Path *best_path,
					   List *tlist, List *scan_clauses)
{
	ValuesScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;
	RangeTblEntry *rte;
	List	   *values_lists;

	/* it should be a values base rel... */
	Assert(scan_relid > 0);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(rte->rtekind == RTE_VALUES);
	values_lists = rte->values_lists;

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
		/* The values lists could contain nestloop params, too */
		values_lists = (List *)
			replace_nestloop_params(root, (Node *) values_lists);
	}

	scan_plan = make_valuesscan(tlist, scan_clauses, scan_relid,
								values_lists);

	copy_path_costsize(root, &scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_ctescan_plan
 *	 Returns a ctescan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SubqueryScan *
create_ctescan_plan(PlannerInfo *root, Path *best_path,
					List *tlist, List *scan_clauses)
{
	Index		scan_relid = best_path->parent->relid;
	SubqueryScan *scan_plan;

	Assert(best_path->parent->rtekind == RTE_CTE);

	Assert(scan_relid > 0);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
	}

	scan_plan = make_subqueryscan(tlist,
								  scan_clauses,
								  scan_relid,
								  best_path->parent->subplan);

	copy_path_costsize(root, &scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_worktablescan_plan
 *	 Returns a worktablescan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static WorkTableScan *
create_worktablescan_plan(PlannerInfo *root, Path *best_path,
						  List *tlist, List *scan_clauses)
{
	WorkTableScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;
	RangeTblEntry *rte;
	Index		levelsup;
	PlannerInfo *cteroot;

	Assert(scan_relid > 0);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(rte->rtekind == RTE_CTE);
	Assert(rte->self_reference);

	/*
	 * We need to find the worktable param ID, which is in the plan level
	 * that's processing the recursive UNION, which is one level *below* where
	 * the CTE comes from.
	 */
	levelsup = rte->ctelevelsup;
	if (levelsup == 0)			/* shouldn't happen */
		elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	levelsup--;
	cteroot = root;
	while (levelsup-- > 0)
	{
		cteroot = cteroot->parent_root;
		if (!cteroot)			/* shouldn't happen */
			elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	}
	if (cteroot->wt_param_id < 0)		/* shouldn't happen */
		elog(ERROR, "could not find param ID for CTE \"%s\"", rte->ctename);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
	}

	scan_plan = make_worktablescan(tlist, scan_clauses, scan_relid,
								   cteroot->wt_param_id);

	copy_path_costsize(root, &scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_foreignscan_plan
 *	 Returns a foreignscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static ForeignScan *
create_foreignscan_plan(PlannerInfo *root, ForeignPath *best_path,
						List *tlist, List *scan_clauses)
{
	ForeignScan *scan_plan;
	RelOptInfo *rel = best_path->path.parent;
	Index		scan_relid = rel->relid;
	RangeTblEntry *rte;
	Bitmapset  *attrs_used = NULL;
	ListCell   *lc;
	int			i;

	/* it should be a base rel... */
	Assert(scan_relid > 0);
	Assert(rel->rtekind == RTE_RELATION);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(rte->rtekind == RTE_RELATION);

	/*
	 * Sort clauses into best execution order.  We do this first since the FDW
	 * might have more info than we do and wish to adjust the ordering.
	 */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/*
	 * Let the FDW perform its processing on the restriction clauses and
	 * generate the plan node.  Note that the FDW might remove restriction
	 * clauses that it intends to execute remotely, or even add more (if it
	 * has selected some join clauses for remote use but also wants them
	 * rechecked locally).
	 */
	scan_plan = rel->fdwroutine->GetForeignPlan(root, rel, rte->relid,
												best_path,
												tlist, scan_clauses);

	/* Copy cost data from Path to Plan; no need to make FDW do this */
	copy_path_costsize(root, &scan_plan->scan.plan, &best_path->path);

	/*
	 * Replace any outer-relation variables with nestloop params in the qual
	 * and fdw_exprs expressions.  We do this last so that the FDW doesn't
	 * have to be involved.  (Note that parts of fdw_exprs could have come
	 * from join clauses, so doing this beforehand on the scan_clauses
	 * wouldn't work.)
	 */
	if (best_path->path.param_info)
	{
		scan_plan->scan.plan.qual = (List *)
			replace_nestloop_params(root, (Node *) scan_plan->scan.plan.qual);
		scan_plan->fdw_exprs = (List *)
			replace_nestloop_params(root, (Node *) scan_plan->fdw_exprs);
	}

	/*
	 * Detect whether any system columns are requested from rel.  This is a
	 * bit of a kluge and might go away someday, so we intentionally leave it
	 * out of the API presented to FDWs.
	 *
	 * First, examine all the attributes needed for joins or final output.
	 * Note: we must look at reltargetlist, not the attr_needed data, because
	 * attr_needed isn't computed for inheritance child rels.
	 */
	pull_varattnos((Node *) rel->reltargetlist, rel->relid, &attrs_used);

	/* Add all the attributes used by restriction clauses. */
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, rel->relid, &attrs_used);
	}

	/* Now, are any system columns requested from rel? */
	scan_plan->fsSystemCol = false;
	for (i = FirstLowInvalidHeapAttributeNumber + 1; i < 0; i++)
	{
		if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
		{
			scan_plan->fsSystemCol = true;
			break;
		}
	}

	bms_free(attrs_used);

	return scan_plan;
}

static Expr *
remove_isnotfalse_expr(Expr *expr)
{
	if (IsA(expr, BooleanTest))
	{
		BooleanTest *bt = (BooleanTest *) expr;

		if (bt->booltesttype == IS_NOT_FALSE)
		{
			return bt->arg;
		}
	}
	return expr;
}

/*
 * remove_isnotfalse
 *	  Given a list of joinclauses, extract the bare clauses, removing any IS_NOT_FALSE
 *	  additions. The original data structure is not touched; a modified list is returned
 */
static List *
remove_isnotfalse(List *clauses)
{
	List	   *t_list = NIL;
	ListCell   *l;

	foreach(l, clauses)
	{
		Node	   *node = (Node *) lfirst(l);

		if (IsA(node, Expr) ||IsA(node, BooleanTest))
		{
			Expr	   *expr = (Expr *) node;

			expr = remove_isnotfalse_expr(expr);
			t_list = lappend(t_list, expr);
		}
		else if (IsA(node, RestrictInfo))
		{
			RestrictInfo *restrictinfo = (RestrictInfo *) node;
			Expr	   *rclause = restrictinfo->clause;

			rclause = remove_isnotfalse_expr(rclause);
			t_list = lappend(t_list, rclause);
		}
		else
		{
			t_list = lappend(t_list, node);
		}
	}
	return t_list;
}


/*****************************************************************************
 *
 *	JOIN METHODS
 *
 *****************************************************************************/

static NestLoop *
create_nestloop_plan(PlannerInfo *root,
					 NestPath *best_path,
					 Plan *outer_plan,
					 Plan *inner_plan)
{
	NestLoop   *join_plan;
	List	   *tlist = build_path_tlist(root, &best_path->path);
	List	   *joinrestrictclauses = best_path->joinrestrictinfo;
	List	   *joinclauses;
	List	   *otherclauses;
	Relids		outerrelids;
	List	   *nestParams;
	ListCell   *cell;
	ListCell   *prev;
	ListCell   *next;

	bool		prefetch = false;

	/*
	 * MPP-1459: subqueries are resolved after our deadlock checks in
	 * pathnode.c; so we have to check here to make sure that we catch all
	 * motion deadlocks.
	 *
	 * MPP-1487: if there is already a materialize node here, we don't want to
	 * insert another one. :-)
	 *
	 * NOTE: materialize_finished_plan() does *almost* what we want -- except
	 * we aren't finished.
	 */
	if (best_path->innerjoinpath->motionHazard ||
		!best_path->innerjoinpath->rescannable)
	{
		Plan	   *p;
		Material   *mat;

		p = inner_plan;
		while (IsA(p, PartitionSelector))
			p = p->lefttree;
		if (IsA(p, Material))
		{
			mat = (Material *) p;
		}
		else
		{
			Path		matpath;	/* dummy for cost fixup */

			/* Set cost data */
			cost_material(&matpath,
						  root,
						  inner_plan->startup_cost,
						  inner_plan->total_cost,
						  inner_plan->plan_rows,
						  inner_plan->plan_width);

			mat = make_material(inner_plan);

			mat->plan.startup_cost = matpath.startup_cost;
			mat->plan.total_cost = matpath.total_cost;
			mat->plan.plan_rows = inner_plan->plan_rows;
			mat->plan.plan_width = inner_plan->plan_width;

			inner_plan = (Plan *) mat;
		}

		/*
		 * MPP-1657: Even if there is already a materialize here, we
		 * may need to update its strictness.
		 */
		if (best_path->outerjoinpath->motionHazard)
		{
			mat->cdb_strict = true;
			prefetch = true;
		}
	}

#if  0
	/*
	 * If the inner path is a nestloop inner indexscan, it might be using some
	 * of the join quals as index quals, in which case we don't have to check
	 * them again at the join node.  Remove any join quals that are redundant.
	 */
	joinrestrictclauses =
		select_nonredundant_join_clauses(root,
										 joinrestrictclauses,
										 best_path->innerjoinpath);
#endif

	/* Sort join qual clauses into best execution order */
	joinrestrictclauses = order_qual_clauses(root, joinrestrictclauses);

	/* Get the join qual clauses (in plain expression form) */
	/* Any pseudoconstant clauses are ignored here */
	if (IS_OUTER_JOIN(best_path->jointype))
	{
		extract_actual_join_clauses(joinrestrictclauses,
									best_path->path.parent->relids,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(joinrestrictclauses, false);
		otherclauses = NIL;
	}

	if (best_path->jointype == JOIN_LASJ_NOTIN)
	{
		joinclauses = remove_isnotfalse(joinclauses);
	}

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->path.param_info)
	{
		joinclauses = (List *)
			replace_nestloop_params(root, (Node *) joinclauses);
		otherclauses = (List *)
			replace_nestloop_params(root, (Node *) otherclauses);
	}

	/*
	 * Identify any nestloop parameters that should be supplied by this join
	 * node, and move them from root->curOuterParams to the nestParams list.
	 */
	outerrelids = best_path->outerjoinpath->parent->relids;
	nestParams = NIL;
	prev = NULL;
	for (cell = list_head(root->curOuterParams); cell; cell = next)
	{
		NestLoopParam *nlp = (NestLoopParam *) lfirst(cell);

		next = lnext(cell);
		if (IsA(nlp->paramval, Var) &&
			bms_is_member(nlp->paramval->varno, outerrelids))
		{
			root->curOuterParams = list_delete_cell(root->curOuterParams,
													cell, prev);
			nestParams = lappend(nestParams, nlp);
		}
		else if (IsA(nlp->paramval, PlaceHolderVar) &&
				 bms_overlap(((PlaceHolderVar *) nlp->paramval)->phrels,
							 outerrelids) &&
				 bms_is_subset(find_placeholder_info(root,
											(PlaceHolderVar *) nlp->paramval,
													 false)->ph_eval_at,
							   outerrelids))
		{
			root->curOuterParams = list_delete_cell(root->curOuterParams,
													cell, prev);
			nestParams = lappend(nestParams, nlp);
		}
		else
			prev = cell;
	}

	join_plan = make_nestloop(tlist,
							  joinclauses,
							  otherclauses,
							  nestParams,
							  outer_plan,
							  inner_plan,
							  best_path->jointype);

	copy_path_costsize(root, &join_plan->join.plan, &best_path->path);

	if (IsA(best_path->innerjoinpath, MaterialPath))
	{
		MaterialPath *mp = (MaterialPath *) best_path->innerjoinpath;

		if (mp->cdb_strict)
			prefetch = true;
	}

	if (prefetch)
		join_plan->join.prefetch_inner = true;

	/*
	 * A motion deadlock can also happen when outer and joinqual both contain
	 * motions.  It is not easy to check for joinqual here, so we set the
	 * prefetch_joinqual mark only according to outer motion, and check for
	 * joinqual later in the executor.
	 *
	 * See ExecPrefetchJoinQual() for details.
	 */
	if (best_path->outerjoinpath &&
		best_path->outerjoinpath->motionHazard)
		join_plan->join.prefetch_joinqual = true;

	return join_plan;
}

static MergeJoin *
create_mergejoin_plan(PlannerInfo *root,
					  MergePath *best_path,
					  Plan *outer_plan,
					  Plan *inner_plan)
{
	List	   *tlist = build_path_tlist(root, &best_path->jpath.path);
	List	   *joinclauses;
	List	   *otherclauses;
	List	   *mergeclauses;
	Sort	   *sort;
	bool		prefetch = false;
	bool		set_mat_cdb_strict = false;
	List	   *outerpathkeys;
	List	   *innerpathkeys;
	int			nClauses;
	Oid		   *mergefamilies;
	Oid		   *mergecollations;
	int		   *mergestrategies;
	bool	   *mergenullsfirst;
	PathKey    *opathkey;
	EquivalenceClass *opeclass;
	MergeJoin  *join_plan;
	int			i;
	ListCell   *lc;
	ListCell   *lop;
	ListCell   *lip;

	/* Sort join qual clauses into best execution order */
	/* NB: do NOT reorder the mergeclauses */
	joinclauses = order_qual_clauses(root, best_path->jpath.joinrestrictinfo);

	/* Get the join qual clauses (in plain expression form) */
	/* Any pseudoconstant clauses are ignored here */
	if (IS_OUTER_JOIN(best_path->jpath.jointype))
	{
		extract_actual_join_clauses(joinclauses,
									best_path->jpath.path.parent->relids,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(joinclauses, false);
		otherclauses = NIL;
	}

	/*
	 * Remove the mergeclauses from the list of join qual clauses, leaving the
	 * list of quals that must be checked as qpquals.
	 */
	mergeclauses = get_actual_clauses(best_path->path_mergeclauses);
	joinclauses = list_difference(joinclauses, mergeclauses);

	/*
	 * Replace any outer-relation variables with nestloop params.  There
	 * should not be any in the mergeclauses.
	 */
	if (best_path->jpath.path.param_info)
	{
		joinclauses = (List *)
			replace_nestloop_params(root, (Node *) joinclauses);
		otherclauses = (List *)
			replace_nestloop_params(root, (Node *) otherclauses);
	}

	/*
	 * Rearrange mergeclauses, if needed, so that the outer variable is always
	 * on the left; mark the mergeclause restrictinfos with correct
	 * outer_is_left status.
	 */
	mergeclauses = get_switched_clauses(best_path->path_mergeclauses,
							 best_path->jpath.outerjoinpath->parent->relids);

	/*
	 * Create explicit sort nodes for the outer and inner paths if necessary.
	 * Make sure there are no excess columns in the inputs if sorting.
	 */
	if (best_path->outersortkeys)
	{
		disuse_physical_tlist(root, outer_plan, best_path->jpath.outerjoinpath);
		sort =
			make_sort_from_pathkeys(root,
									outer_plan,
									best_path->outersortkeys,
									-1.0,
									true);
		if (sort)
			outer_plan = (Plan *) sort;
		outerpathkeys = best_path->outersortkeys;
	}
	else
		outerpathkeys = best_path->jpath.outerjoinpath->pathkeys;

	if (best_path->innersortkeys)
	{
		disuse_physical_tlist(root, inner_plan, best_path->jpath.innerjoinpath);
		sort =
			make_sort_from_pathkeys(root,
									inner_plan,
									best_path->innersortkeys,
									-1.0,
									true);
		if (sort)
			inner_plan = (Plan *) sort;
		innerpathkeys = best_path->innersortkeys;
	}
	else
		innerpathkeys = best_path->jpath.innerjoinpath->pathkeys;

	/*
	 * MPP-3300: very similar to the nested-loop join motion deadlock cases. But we may have already
	 * put some slackening operators below (e.g. a sort).
	 *
	 * We need some kind of strict slackening operator (something which consumes all of its
	 * input before producing a row of output) for our inner. And we need to prefetch that side
	 * first.
	 *
	 * See motion_sanity_walker() for details on how a deadlock may occur.
	 */
	if (best_path->jpath.outerjoinpath->motionHazard && best_path->jpath.innerjoinpath->motionHazard)
	{
		prefetch = true;
		if (!IsA(inner_plan, Sort))
		{
			if (!IsA(inner_plan, Material))
				best_path->materialize_inner = true;
			set_mat_cdb_strict = true;
		}
	}

	/*
	 * If specified, add a materialize node to shield the inner plan from the
	 * need to handle mark/restore.
	 */
	if (best_path->materialize_inner)
	{
		Plan	   *matplan = (Plan *) make_material(inner_plan);

		Assert(!IsA(inner_plan, Material));

		/*
		 * We assume the materialize will not spill to disk, and therefore
		 * charge just cpu_operator_cost per tuple.  (Keep this estimate in
		 * sync with final_cost_mergejoin.)
		 */
		copy_plan_costsize(matplan, inner_plan);
		matplan->total_cost += cpu_operator_cost * matplan->plan_rows;

		inner_plan = matplan;
	}

	if (set_mat_cdb_strict)
		((Material *) inner_plan)->cdb_strict = true;

	/*
	 * Compute the opfamily/collation/strategy/nullsfirst arrays needed by the
	 * executor.  The information is in the pathkeys for the two inputs, but
	 * we need to be careful about the possibility of mergeclauses sharing a
	 * pathkey, as well as the possibility that the inner pathkeys are not in
	 * an order matching the mergeclauses.
	 */
	nClauses = list_length(mergeclauses);
	Assert(nClauses == list_length(best_path->path_mergeclauses));
	mergefamilies = (Oid *) palloc(nClauses * sizeof(Oid));
	mergecollations = (Oid *) palloc(nClauses * sizeof(Oid));
	mergestrategies = (int *) palloc(nClauses * sizeof(int));
	mergenullsfirst = (bool *) palloc(nClauses * sizeof(bool));

	opathkey = NULL;
	opeclass = NULL;
	lop = list_head(outerpathkeys);
	lip = list_head(innerpathkeys);
	i = 0;
	foreach(lc, best_path->path_mergeclauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		EquivalenceClass *oeclass;
		EquivalenceClass *ieclass;
		PathKey    *ipathkey = NULL;
		EquivalenceClass *ipeclass = NULL;
		bool		first_inner_match = false;

		/* fetch outer/inner eclass from mergeclause */
		Assert(IsA(rinfo, RestrictInfo));
		if (rinfo->outer_is_left)
		{
			oeclass = rinfo->left_ec;
			ieclass = rinfo->right_ec;
		}
		else
		{
			oeclass = rinfo->right_ec;
			ieclass = rinfo->left_ec;
		}
		Assert(oeclass != NULL);
		Assert(ieclass != NULL);

		/*
		 * We must identify the pathkey elements associated with this clause
		 * by matching the eclasses (which should give a unique match, since
		 * the pathkey lists should be canonical).  In typical cases the merge
		 * clauses are one-to-one with the pathkeys, but when dealing with
		 * partially redundant query conditions, things are more complicated.
		 *
		 * lop and lip reference the first as-yet-unmatched pathkey elements.
		 * If they're NULL then all pathkey elements have been matched.
		 *
		 * The ordering of the outer pathkeys should match the mergeclauses,
		 * by construction (see find_mergeclauses_for_outer_pathkeys()). There
		 * could be more than one mergeclause for the same outer pathkey, but
		 * no pathkey may be entirely skipped over.
		 */
		if (oeclass != opeclass)	/* multiple matches are not interesting */
		{
			/* doesn't match the current opathkey, so must match the next */
			if (lop == NULL)
				elog(ERROR, "outer pathkeys do not match mergeclauses");
			opathkey = (PathKey *) lfirst(lop);
			opeclass = opathkey->pk_eclass;
			lop = lnext(lop);
			if (oeclass != opeclass)
				elog(ERROR, "outer pathkeys do not match mergeclauses");
		}

		/*
		 * The inner pathkeys likewise should not have skipped-over keys, but
		 * it's possible for a mergeclause to reference some earlier inner
		 * pathkey if we had redundant pathkeys.  For example we might have
		 * mergeclauses like "o.a = i.x AND o.b = i.y AND o.c = i.x".  The
		 * implied inner ordering is then "ORDER BY x, y, x", but the pathkey
		 * mechanism drops the second sort by x as redundant, and this code
		 * must cope.
		 *
		 * It's also possible for the implied inner-rel ordering to be like
		 * "ORDER BY x, y, x DESC".  We still drop the second instance of x as
		 * redundant; but this means that the sort ordering of a redundant
		 * inner pathkey should not be considered significant.  So we must
		 * detect whether this is the first clause matching an inner pathkey.
		 */
		if (lip)
		{
			ipathkey = (PathKey *) lfirst(lip);
			ipeclass = ipathkey->pk_eclass;
			if (ieclass == ipeclass)
			{
				/* successful first match to this inner pathkey */
				lip = lnext(lip);
				first_inner_match = true;
			}
		}
		if (!first_inner_match)
		{
			/* redundant clause ... must match something before lip */
			ListCell   *l2;

			foreach(l2, innerpathkeys)
			{
				if (l2 == lip)
					break;
				ipathkey = (PathKey *) lfirst(l2);
				ipeclass = ipathkey->pk_eclass;
				if (ieclass == ipeclass)
					break;
			}
			if (ieclass != ipeclass)
				elog(ERROR, "inner pathkeys do not match mergeclauses");
		}

		/*
		 * The pathkeys should always match each other as to opfamily and
		 * collation (which affect equality), but if we're considering a
		 * redundant inner pathkey, its sort ordering might not match.  In
		 * such cases we may ignore the inner pathkey's sort ordering and use
		 * the outer's.  (In effect, we're lying to the executor about the
		 * sort direction of this inner column, but it does not matter since
		 * the run-time row comparisons would only reach this column when
		 * there's equality for the earlier column containing the same eclass.
		 * There could be only one value in this column for the range of inner
		 * rows having a given value in the earlier column, so it does not
		 * matter which way we imagine this column to be ordered.)  But a
		 * non-redundant inner pathkey had better match outer's ordering too.
		 */
		if (opathkey->pk_opfamily != ipathkey->pk_opfamily ||
			opathkey->pk_eclass->ec_collation != ipathkey->pk_eclass->ec_collation)
			elog(ERROR, "left and right pathkeys do not match in mergejoin");
		if (first_inner_match &&
			(opathkey->pk_strategy != ipathkey->pk_strategy ||
			 opathkey->pk_nulls_first != ipathkey->pk_nulls_first))
			elog(ERROR, "left and right pathkeys do not match in mergejoin");

		/* OK, save info for executor */
		mergefamilies[i] = opathkey->pk_opfamily;
		mergecollations[i] = opathkey->pk_eclass->ec_collation;
		mergestrategies[i] = opathkey->pk_strategy;
		mergenullsfirst[i] = opathkey->pk_nulls_first;
		i++;
	}

	/*
	 * Note: it is not an error if we have additional pathkey elements (i.e.,
	 * lop or lip isn't NULL here).  The input paths might be better-sorted
	 * than we need for the current mergejoin.
	 */

	/*
	 * Now we can build the mergejoin node.
	 */
	join_plan = make_mergejoin(tlist,
							   joinclauses,
							   otherclauses,
							   mergeclauses,
							   mergefamilies,
							   mergecollations,
							   mergestrategies,
							   mergenullsfirst,
							   outer_plan,
							   inner_plan,
							   best_path->jpath.jointype);

	join_plan->join.prefetch_inner = prefetch;

	/*
	 * A motion deadlock can also happen when outer and joinqual both contain
	 * motions.  It is not easy to check for joinqual here, so we set the
	 * prefetch_joinqual mark only according to outer motion, and check for
	 * joinqual later in the executor.
	 *
	 * See ExecPrefetchJoinQual() for details.
	 */
	if (best_path->jpath.outerjoinpath &&
		best_path->jpath.outerjoinpath->motionHazard)
		join_plan->join.prefetch_joinqual = true;
	/*
	 * If inner motion is not under a Material or Sort node then there could
	 * also be motion deadlock between inner and joinqual in mergejoin.
	 */
	if (best_path->jpath.innerjoinpath &&
		best_path->jpath.innerjoinpath->motionHazard)
		join_plan->join.prefetch_joinqual = true;

	/* Costs of sort and material steps are included in path cost already */
	copy_path_costsize(root, &join_plan->join.plan, &best_path->jpath.path);

	return join_plan;
}

static HashJoin *
create_hashjoin_plan(PlannerInfo *root,
					 HashPath *best_path,
					 Plan *outer_plan,
					 Plan *inner_plan)
{
	List	   *tlist = build_path_tlist(root, &best_path->jpath.path);
	List	   *joinclauses;
	List	   *otherclauses;
	List	   *hashclauses;
	Oid			skewTable = InvalidOid;
	AttrNumber	skewColumn = InvalidAttrNumber;
	bool		skewInherit = false;
	Oid			skewColType = InvalidOid;
	int32		skewColTypmod = -1;
	HashJoin   *join_plan;
	Hash	   *hash_plan;

	/* Sort join qual clauses into best execution order */
	joinclauses = order_qual_clauses(root, best_path->jpath.joinrestrictinfo);
	/* There's no point in sorting the hash clauses ... */

	/* Get the join qual clauses (in plain expression form) */
	/* Any pseudoconstant clauses are ignored here */
	if (IS_OUTER_JOIN(best_path->jpath.jointype))
	{
		extract_actual_join_clauses(joinclauses,
									best_path->jpath.path.parent->relids,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(joinclauses, false);
		otherclauses = NIL;
	}

	/*
	 * Remove the hashclauses from the list of join qual clauses, leaving the
	 * list of quals that must be checked as qpquals.
	 */
	hashclauses = get_actual_clauses(best_path->path_hashclauses);
	joinclauses = list_difference(joinclauses, hashclauses);

	/*
	 * Replace any outer-relation variables with nestloop params.  There
	 * should not be any in the hashclauses.
	 */
	if (best_path->jpath.path.param_info)
	{
		joinclauses = (List *)
			replace_nestloop_params(root, (Node *) joinclauses);
		otherclauses = (List *)
			replace_nestloop_params(root, (Node *) otherclauses);
	}

	/*
	 * Rearrange hashclauses, if needed, so that the outer variable is always
	 * on the left.
	 */
	hashclauses = get_switched_clauses(best_path->path_hashclauses,
							 best_path->jpath.outerjoinpath->parent->relids);

	/*
	 * We don't want any excess columns in the hashed tuples, or in the outer
	 * either!
	 */
	disuse_physical_tlist(root, inner_plan, best_path->jpath.innerjoinpath);
	if (outer_plan)
		disuse_physical_tlist(root, outer_plan, best_path->jpath.outerjoinpath);

	/* If we expect batching, suppress excess columns in outer tuples too */
	if (best_path->num_batches > 1)
		disuse_physical_tlist(root, outer_plan, best_path->jpath.outerjoinpath);

	/*
	 * If there is a single join clause and we can identify the outer variable
	 * as a simple column reference, supply its identity for possible use in
	 * skew optimization.  (Note: in principle we could do skew optimization
	 * with multiple join clauses, but we'd have to be able to determine the
	 * most common combinations of outer values, which we don't currently have
	 * enough stats for.)
	 */
	if (list_length(hashclauses) == 1)
	{
		OpExpr	   *clause = (OpExpr *) linitial(hashclauses);
		Node	   *node;

		Assert(is_opclause(clause));
		node = (Node *) linitial(clause->args);
		if (IsA(node, RelabelType))
			node = (Node *) ((RelabelType *) node)->arg;
		if (IsA(node, Var))
		{
			Var		   *var = (Var *) node;
			RangeTblEntry *rte;

			rte = root->simple_rte_array[var->varno];
			if (rte->rtekind == RTE_RELATION)
			{
				skewTable = rte->relid;
				skewColumn = var->varattno;
				skewInherit = rte->inh;
				skewColType = var->vartype;
				skewColTypmod = var->vartypmod;
			}
		}
	}

	/*
	 * Build the hash node and hash join node.
	 */
	hash_plan = make_hash(inner_plan,
						  skewTable,
						  skewColumn,
						  skewInherit,
						  skewColType,
						  skewColTypmod);
	join_plan = make_hashjoin(tlist,
							  joinclauses,
							  otherclauses,
							  hashclauses,
							  NIL, /* hashqualclauses */
							  outer_plan,
							  (Plan *) hash_plan,
							  best_path->jpath.jointype);

	/*
	 * MPP-4635.  best_path->jpath.outerjoinpath may be NULL.
	 * From the comment, it is adaptive nestloop join may cause this.
	 */
	/*
	 * MPP-4165: we need to descend left-first if *either* of the
	 * subplans have any motion.
	 */
	/*
	 * MPP-3300: unify motion-deadlock prevention for all join types.
	 * This allows us to undo the MPP-989 changes in nodeHashjoin.c
	 * (allowing us to check the outer for rows before building the
	 * hash-table).
	 */
	if (best_path->jpath.outerjoinpath == NULL ||
		best_path->jpath.outerjoinpath->motionHazard ||
		best_path->jpath.innerjoinpath->motionHazard)
	{
		join_plan->join.prefetch_inner = true;
	}

	/*
	 * A motion deadlock can also happen when outer and joinqual both contain
	 * motions.  It is not easy to check for joinqual here, so we set the
	 * prefetch_joinqual mark only according to outer motion, and check for
	 * joinqual later in the executor.
	 *
	 * See ExecPrefetchJoinQual() for details.
	 */
	if (best_path->jpath.outerjoinpath &&
		best_path->jpath.outerjoinpath->motionHazard)
		join_plan->join.prefetch_joinqual = true;

	copy_path_costsize(root, &join_plan->join.plan, &best_path->jpath.path);

	return join_plan;
}


/*****************************************************************************
 *
 *	SUPPORTING ROUTINES
 *
 *****************************************************************************/

/*
 * replace_nestloop_params
 *	  Replace outer-relation Vars and PlaceHolderVars in the given expression
 *	  with nestloop Params
 *
 * All Vars and PlaceHolderVars belonging to the relation(s) identified by
 * root->curOuterRels are replaced by Params, and entries are added to
 * root->curOuterParams if not already present.
 */
static Node *
replace_nestloop_params(PlannerInfo *root, Node *expr)
{
	/* No setup needed for tree walk, so away we go */
	return replace_nestloop_params_mutator(expr, root);
}

static Node *
replace_nestloop_params_mutator(Node *node, PlannerInfo *root)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;
		Param	   *param;
		NestLoopParam *nlp;
		ListCell   *lc;

		/* Upper-level Vars should be long gone at this point */
		Assert(var->varlevelsup == 0);
		/* If not to be replaced, we can just return the Var unmodified */
		if (!bms_is_member(var->varno, root->curOuterRels))
			return node;
		/* Create a Param representing the Var */
		param = assign_nestloop_param_var(root, var);
		/* Is this param already listed in root->curOuterParams? */
		foreach(lc, root->curOuterParams)
		{
			nlp = (NestLoopParam *) lfirst(lc);
			if (nlp->paramno == param->paramid)
			{
				Assert(equal(var, nlp->paramval));
				/* Present, so we can just return the Param */
				return (Node *) param;
			}
		}
		/* No, so add it */
		nlp = makeNode(NestLoopParam);
		nlp->paramno = param->paramid;
		nlp->paramval = var;
		root->curOuterParams = lappend(root->curOuterParams, nlp);
		/* And return the replacement Param */
		return (Node *) param;
	}
	if (IsA(node, PlaceHolderVar))
	{
		PlaceHolderVar *phv = (PlaceHolderVar *) node;
		Param	   *param;
		NestLoopParam *nlp;
		ListCell   *lc;

		/* Upper-level PlaceHolderVars should be long gone at this point */
		Assert(phv->phlevelsup == 0);

		/*
		 * Check whether we need to replace the PHV.  We use bms_overlap as a
		 * cheap/quick test to see if the PHV might be evaluated in the outer
		 * rels, and then grab its PlaceHolderInfo to tell for sure.
		 */
		if (!bms_overlap(phv->phrels, root->curOuterRels) ||
		  !bms_is_subset(find_placeholder_info(root, phv, false)->ph_eval_at,
						 root->curOuterRels))
		{
			/*
			 * We can't replace the whole PHV, but we might still need to
			 * replace Vars or PHVs within its expression, in case it ends up
			 * actually getting evaluated here.  (It might get evaluated in
			 * this plan node, or some child node; in the latter case we don't
			 * really need to process the expression here, but we haven't got
			 * enough info to tell if that's the case.)  Flat-copy the PHV
			 * node and then recurse on its expression.
			 *
			 * Note that after doing this, we might have different
			 * representations of the contents of the same PHV in different
			 * parts of the plan tree.  This is OK because equal() will just
			 * match on phid/phlevelsup, so setrefs.c will still recognize an
			 * upper-level reference to a lower-level copy of the same PHV.
			 */
			PlaceHolderVar *newphv = makeNode(PlaceHolderVar);

			memcpy(newphv, phv, sizeof(PlaceHolderVar));
			newphv->phexpr = (Expr *)
				replace_nestloop_params_mutator((Node *) phv->phexpr,
												root);
			return (Node *) newphv;
		}
		/* Create a Param representing the PlaceHolderVar */
		param = assign_nestloop_param_placeholdervar(root, phv);
		/* Is this param already listed in root->curOuterParams? */
		foreach(lc, root->curOuterParams)
		{
			nlp = (NestLoopParam *) lfirst(lc);
			if (nlp->paramno == param->paramid)
			{
				Assert(equal(phv, nlp->paramval));
				/* Present, so we can just return the Param */
				return (Node *) param;
			}
		}
		/* No, so add it */
		nlp = makeNode(NestLoopParam);
		nlp->paramno = param->paramid;
		nlp->paramval = (Var *) phv;
		root->curOuterParams = lappend(root->curOuterParams, nlp);
		/* And return the replacement Param */
		return (Node *) param;
	}
	return expression_tree_mutator(node,
								   replace_nestloop_params_mutator,
								   (void *) root);
}

/*
 * process_subquery_nestloop_params
 *	  Handle params of a parameterized subquery that need to be fed
 *	  from an outer nestloop.
 *
 * Currently, that would be *all* params that a subquery in FROM has demanded
 * from the current query level, since they must be LATERAL references.
 *
 * The subplan's references to the outer variables are already represented
 * as PARAM_EXEC Params, so we need not modify the subplan here.  What we
 * do need to do is add entries to root->curOuterParams to signal the parent
 * nestloop plan node that it must provide these values.
 */
static void
process_subquery_nestloop_params(PlannerInfo *root, List *subplan_params)
{
	ListCell   *ppl;

	foreach(ppl, subplan_params)
	{
		PlannerParamItem *pitem = (PlannerParamItem *) lfirst(ppl);

		if (IsA(pitem->item, Var))
		{
			Var		   *var = (Var *) pitem->item;
			NestLoopParam *nlp;
			ListCell   *lc;

			/* If not from a nestloop outer rel, complain */
			if (!bms_is_member(var->varno, root->curOuterRels))
				elog(ERROR, "non-LATERAL parameter required by subquery");
			/* Is this param already listed in root->curOuterParams? */
			foreach(lc, root->curOuterParams)
			{
				nlp = (NestLoopParam *) lfirst(lc);
				if (nlp->paramno == pitem->paramId)
				{
					Assert(equal(var, nlp->paramval));
					/* Present, so nothing to do */
					break;
				}
			}
			if (lc == NULL)
			{
				/* No, so add it */
				nlp = makeNode(NestLoopParam);
				nlp->paramno = pitem->paramId;
				nlp->paramval = copyObject(var);
				root->curOuterParams = lappend(root->curOuterParams, nlp);
			}
		}
		else if (IsA(pitem->item, PlaceHolderVar))
		{
			PlaceHolderVar *phv = (PlaceHolderVar *) pitem->item;
			NestLoopParam *nlp;
			ListCell   *lc;

			/* If not from a nestloop outer rel, complain */
			if (!bms_is_subset(find_placeholder_info(root, phv, false)->ph_eval_at,
							   root->curOuterRels))
				elog(ERROR, "non-LATERAL parameter required by subquery");
			/* Is this param already listed in root->curOuterParams? */
			foreach(lc, root->curOuterParams)
			{
				nlp = (NestLoopParam *) lfirst(lc);
				if (nlp->paramno == pitem->paramId)
				{
					Assert(equal(phv, nlp->paramval));
					/* Present, so nothing to do */
					break;
				}
			}
			if (lc == NULL)
			{
				/* No, so add it */
				nlp = makeNode(NestLoopParam);
				nlp->paramno = pitem->paramId;
				nlp->paramval = copyObject(phv);
				root->curOuterParams = lappend(root->curOuterParams, nlp);
			}
		}
		else
			elog(ERROR, "unexpected type of subquery parameter");
	}
}

/*
 * fix_indexqual_references
 *	  Adjust indexqual clauses to the form the executor's indexqual
 *	  machinery needs.
 *
 * We have four tasks here:
 *	* Remove RestrictInfo nodes from the input clauses.
 *	* Replace any outer-relation Var or PHV nodes with nestloop Params.
 *	  (XXX eventually, that responsibility should go elsewhere?)
 *	* Index keys must be represented by Var nodes with varattno set to the
 *	  index's attribute number, not the attribute number in the original rel.
 *	* If the index key is on the right, commute the clause to put it on the
 *	  left.
 *
 * The result is a modified copy of the path's indexquals list --- the
 * original is not changed.  Note also that the copy shares no substructure
 * with the original; this is needed in case there is a subplan in it (we need
 * two separate copies of the subplan tree, or things will go awry).
 */
static List *
fix_indexqual_references(PlannerInfo *root, IndexPath *index_path)
{
	IndexOptInfo *index = index_path->indexinfo;
	List	   *fixed_indexquals;
	ListCell   *lcc,
			   *lci;

	fixed_indexquals = NIL;

	forboth(lcc, index_path->indexquals, lci, index_path->indexqualcols)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lcc);
		int			indexcol = lfirst_int(lci);
		Node	   *clause;

		Assert(IsA(rinfo, RestrictInfo));

		/*
		 * Replace any outer-relation variables with nestloop params.
		 *
		 * This also makes a copy of the clause, so it's safe to modify it
		 * in-place below.
		 */
		clause = replace_nestloop_params(root, (Node *) rinfo->clause);

		if (IsA(clause, OpExpr))
		{
			OpExpr	   *op = (OpExpr *) clause;

			if (list_length(op->args) != 2)
				elog(ERROR, "indexqual clause is not binary opclause");

			/*
			 * Check to see if the indexkey is on the right; if so, commute
			 * the clause.  The indexkey should be the side that refers to
			 * (only) the base relation.
			 */
			if (!bms_equal(rinfo->left_relids, index->rel->relids))
				CommuteOpExpr(op);

			/*
			 * Now replace the indexkey expression with an index Var.
			 */
			linitial(op->args) = fix_indexqual_operand(linitial(op->args),
													   index,
													   indexcol);
		}
		else if (IsA(clause, RowCompareExpr))
		{
			RowCompareExpr *rc = (RowCompareExpr *) clause;
			Expr	   *newrc;
			List	   *indexcolnos;
			bool		var_on_left;
			ListCell   *lca,
					   *lcai;

			/*
			 * Re-discover which index columns are used in the rowcompare.
			 */
			newrc = adjust_rowcompare_for_index(rc,
												index,
												indexcol,
												&indexcolnos,
												&var_on_left);

			/*
			 * Trouble if adjust_rowcompare_for_index thought the
			 * RowCompareExpr didn't match the index as-is; the clause should
			 * have gone through that routine already.
			 */
			if (newrc != (Expr *) rc)
				elog(ERROR, "inconsistent results from adjust_rowcompare_for_index");

			/*
			 * Check to see if the indexkey is on the right; if so, commute
			 * the clause.
			 */
			if (!var_on_left)
				CommuteRowCompareExpr(rc);

			/*
			 * Now replace the indexkey expressions with index Vars.
			 */
			Assert(list_length(rc->largs) == list_length(indexcolnos));
			forboth(lca, rc->largs, lcai, indexcolnos)
			{
				lfirst(lca) = fix_indexqual_operand(lfirst(lca),
													index,
													lfirst_int(lcai));
			}
		}
		else if (IsA(clause, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;

			/* Never need to commute... */

			/* Replace the indexkey expression with an index Var. */
			linitial(saop->args) = fix_indexqual_operand(linitial(saop->args),
														 index,
														 indexcol);
		}
		else if (IsA(clause, NullTest))
		{
			NullTest   *nt = (NullTest *) clause;

			/* Replace the indexkey expression with an index Var. */
			nt->arg = (Expr *) fix_indexqual_operand((Node *) nt->arg,
													 index,
													 indexcol);
		}
		else
			elog(ERROR, "unsupported indexqual type: %d",
				 (int) nodeTag(clause));

		fixed_indexquals = lappend(fixed_indexquals, clause);
	}

	return fixed_indexquals;
}

/*
 * fix_indexorderby_references
 *	  Adjust indexorderby clauses to the form the executor's index
 *	  machinery needs.
 *
 * This is a simplified version of fix_indexqual_references.  The input does
 * not have RestrictInfo nodes, and we assume that indxpath.c already
 * commuted the clauses to put the index keys on the left.  Also, we don't
 * bother to support any cases except simple OpExprs, since nothing else
 * is allowed for ordering operators.
 */
static List *
fix_indexorderby_references(PlannerInfo *root, IndexPath *index_path)
{
	IndexOptInfo *index = index_path->indexinfo;
	List	   *fixed_indexorderbys;
	ListCell   *lcc,
			   *lci;

	fixed_indexorderbys = NIL;

	forboth(lcc, index_path->indexorderbys, lci, index_path->indexorderbycols)
	{
		Node	   *clause = (Node *) lfirst(lcc);
		int			indexcol = lfirst_int(lci);

		/*
		 * Replace any outer-relation variables with nestloop params.
		 *
		 * This also makes a copy of the clause, so it's safe to modify it
		 * in-place below.
		 */
		clause = replace_nestloop_params(root, clause);

		if (IsA(clause, OpExpr))
		{
			OpExpr	   *op = (OpExpr *) clause;

			if (list_length(op->args) != 2)
				elog(ERROR, "indexorderby clause is not binary opclause");

			/*
			 * Now replace the indexkey expression with an index Var.
			 */
			linitial(op->args) = fix_indexqual_operand(linitial(op->args),
													   index,
													   indexcol);
		}
		else
			elog(ERROR, "unsupported indexorderby type: %d",
				 (int) nodeTag(clause));

		fixed_indexorderbys = lappend(fixed_indexorderbys, clause);
	}

	return fixed_indexorderbys;
}

/*
 * fix_indexqual_operand
 *	  Convert an indexqual expression to a Var referencing the index column.
 *
 * We represent index keys by Var nodes having varno == INDEX_VAR and varattno
 * equal to the index's attribute number (index column position).
 *
 * Most of the code here is just for sanity cross-checking that the given
 * expression actually matches the index column it's claimed to.
 */
static Node *
fix_indexqual_operand(Node *node, IndexOptInfo *index, int indexcol)
{
	Var		   *result;
	int			pos;
	ListCell   *indexpr_item;

	/*
	 * Remove any binary-compatible relabeling of the indexkey
	 */
	if (IsA(node, RelabelType))
		node = (Node *) ((RelabelType *) node)->arg;

	Assert(indexcol >= 0 && indexcol < index->ncolumns);

	if (index->indexkeys[indexcol] != 0)
	{
		/* It's a simple index column */
		if (IsA(node, Var) &&
			((Var *) node)->varno == index->rel->relid &&
			((Var *) node)->varattno == index->indexkeys[indexcol])
		{
			result = (Var *) copyObject(node);
			result->varno = INDEX_VAR;
			result->varattno = indexcol + 1;
			return (Node *) result;
		}
		else
			elog(ERROR, "index key does not match expected index column");
	}

	/* It's an index expression, so find and cross-check the expression */
	indexpr_item = list_head(index->indexprs);
	for (pos = 0; pos < index->ncolumns; pos++)
	{
		if (index->indexkeys[pos] == 0)
		{
			if (indexpr_item == NULL)
				elog(ERROR, "too few entries in indexprs list");
			if (pos == indexcol)
			{
				Node	   *indexkey;

				indexkey = (Node *) lfirst(indexpr_item);
				if (indexkey && IsA(indexkey, RelabelType))
					indexkey = (Node *) ((RelabelType *) indexkey)->arg;
				if (equal(node, indexkey))
				{
					result = makeVar(INDEX_VAR, indexcol + 1,
									 exprType(lfirst(indexpr_item)), -1,
									 exprCollation(lfirst(indexpr_item)),
									 0);
					return (Node *) result;
				}
				else
					elog(ERROR, "index key does not match expected index column");
			}
			indexpr_item = lnext(indexpr_item);
		}
	}

	/* Ooops... */
	elog(ERROR, "index key does not match expected index column");
	return NULL;				/* keep compiler quiet */
}

/*
 * get_switched_clauses
 *	  Given a list of merge or hash joinclauses (as RestrictInfo nodes),
 *	  extract the bare clauses, and rearrange the elements within the
 *	  clauses, if needed, so the outer join variable is on the left and
 *	  the inner is on the right.  The original clause data structure is not
 *	  touched; a modified list is returned.  We do, however, set the transient
 *	  outer_is_left field in each RestrictInfo to show which side was which.
 */
static List *
get_switched_clauses(List *clauses, Relids outerrelids)
{
	List	   *t_list = NIL;
	ListCell   *l;

	foreach(l, clauses)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);

		Expr	   *rclause = restrictinfo->clause;
		OpExpr	   *clause;

		/**
		 * If this is a IS NOT FALSE boolean test, we can peek underneath.
		 */
		if (IsA(rclause, BooleanTest))
		{
			BooleanTest *bt = (BooleanTest *) rclause;

			if (bt->booltesttype == IS_NOT_FALSE)
			{
				rclause = bt->arg;
			}
		}

		Assert(is_opclause(rclause));
		clause = (OpExpr *) rclause;
		if (bms_is_subset(restrictinfo->right_relids, outerrelids))
		{
			/*
			 * Duplicate just enough of the structure to allow commuting the
			 * clause without changing the original list.  Could use
			 * copyObject, but a complete deep copy is overkill.
			 */
			OpExpr	   *temp = makeNode(OpExpr);

			temp->opno = clause->opno;
			temp->opfuncid = InvalidOid;
			temp->opresulttype = clause->opresulttype;
			temp->opretset = clause->opretset;
			temp->opcollid = clause->opcollid;
			temp->inputcollid = clause->inputcollid;
			temp->args = list_copy(clause->args);
			temp->location = clause->location;
			/* Commute it --- note this modifies the temp node in-place. */
			CommuteOpExpr(temp);
			t_list = lappend(t_list, temp);
			restrictinfo->outer_is_left = false;
		}
		else
		{
			Assert(bms_is_subset(restrictinfo->left_relids, outerrelids));
			t_list = lappend(t_list, clause);
			restrictinfo->outer_is_left = true;
		}
	}
	return t_list;
}

/*
 * order_qual_clauses
 *		Given a list of qual clauses that will all be evaluated at the same
 *		plan node, sort the list into the order we want to check the quals
 *		in at runtime.
 *
 * Ideally the order should be driven by a combination of execution cost and
 * selectivity, but it's not immediately clear how to account for both,
 * and given the uncertainty of the estimates the reliability of the decisions
 * would be doubtful anyway.  So we just order by estimated per-tuple cost,
 * being careful not to change the order when (as is often the case) the
 * estimates are identical.
 *
 * Although this will work on either bare clauses or RestrictInfos, it's
 * much faster to apply it to RestrictInfos, since it can re-use cost
 * information that is cached in RestrictInfos.
 *
 * Note: some callers pass lists that contain entries that will later be
 * removed; this is the easiest way to let this routine see RestrictInfos
 * instead of bare clauses.  It's OK because we only sort by cost, but
 * a cost/selectivity combination would likely do the wrong thing.
 */
static List *
order_qual_clauses(PlannerInfo *root, List *clauses)
{
	typedef struct
	{
		Node	   *clause;
		Cost		cost;
	} QualItem;
	int			nitems = list_length(clauses);
	QualItem   *items;
	ListCell   *lc;
	int			i;
	List	   *result;

	/* No need to work hard for 0 or 1 clause */
	if (nitems <= 1)
		return clauses;

	/*
	 * Collect the items and costs into an array.  This is to avoid repeated
	 * cost_qual_eval work if the inputs aren't RestrictInfos.
	 */
	items = (QualItem *) palloc(nitems * sizeof(QualItem));
	i = 0;
	foreach(lc, clauses)
	{
		Node	   *clause = (Node *) lfirst(lc);
		QualCost	qcost;

		cost_qual_eval_node(&qcost, clause, root);
		items[i].clause = clause;
		items[i].cost = qcost.per_tuple;
		i++;
	}

	/*
	 * Sort.  We don't use qsort() because it's not guaranteed stable for
	 * equal keys.  The expected number of entries is small enough that a
	 * simple insertion sort should be good enough.
	 */
	for (i = 1; i < nitems; i++)
	{
		QualItem	newitem = items[i];
		int			j;

		/* insert newitem into the already-sorted subarray */
		for (j = i; j > 0; j--)
		{
			if (newitem.cost >= items[j - 1].cost)
				break;
			items[j] = items[j - 1];
		}
		items[j] = newitem;
	}

	/* Convert back to a list */
	result = NIL;
	for (i = 0; i < nitems; i++)
		result = lappend(result, items[i].clause);

	return result;
}

/*
 * Copy cost and size info from a Path node to the Plan node created from it.
 * The executor usually won't use this info, but it's needed by EXPLAIN.
 */
static void
copy_path_costsize(PlannerInfo *root, Plan *dest, Path *src)
{
	if (src)
	{
		dest->startup_cost = src->startup_cost;
		dest->total_cost = src->total_cost;
		dest->plan_rows = src->rows;
		dest->plan_width = src->parent->width;
	}
	else
	{
		dest->startup_cost = 0;
		dest->total_cost = 0;
		dest->plan_rows = 0;
		dest->plan_width = 0;
	}
}

/*
 * Copy cost and size info from a lower plan node to an inserted node.
 * (Most callers alter the info after copying it.)
 */
static void
copy_plan_costsize(Plan *dest, Plan *src)
{
	if (src)
	{
		dest->startup_cost = src->startup_cost;
		dest->total_cost = src->total_cost;
		dest->plan_rows = src->plan_rows;
		dest->plan_width = src->plan_width;
	}
	else
	{
		dest->startup_cost = 0;
		dest->total_cost = 0;
		dest->plan_rows = 0;
		dest->plan_width = 0;
	}
}


/*****************************************************************************
 *
 *	PLAN NODE BUILDING ROUTINES
 *
 * Some of these are exported because they are called to build plan nodes
 * in contexts where we're not deriving the plan node from a path node.
 *
 *****************************************************************************/

static SeqScan *
make_seqscan(List *qptlist,
			 List *qpqual,
			 Index scanrelid)
{
	SeqScan    *node = makeNode(SeqScan);
	Plan	   *plan = &node->plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scanrelid = scanrelid;

	return node;
}

static ExternalScan *
make_externalscan(List *qptlist,
				  List *qpqual,
				  Index scanrelid,
				  List *urilist,
				  char *fmtoptstring,
				  char fmttype,
				  bool ismasteronly,
				  int rejectlimit,
				  bool rejectlimitinrows,
				  bool logerrors,
				  int encoding)
{
	ExternalScan *node = makeNode(ExternalScan);
	Plan	   *plan = &node->scan.plan;
	static uint32 scancounter = 0;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;

	/* external specifictions */
	node->uriList = urilist;
	node->fmtOptString = fmtoptstring;
	node->fmtType = fmttype;
	node->isMasterOnly = ismasteronly;
	node->rejLimit = rejectlimit;
	node->rejLimitInRows = rejectlimitinrows;
	node->logErrors = logerrors;
	node->encoding = encoding;
	node->scancounter = scancounter++;

	return node;
}

static IndexScan *
make_indexscan(List *qptlist,
			   List *qpqual,
			   Index scanrelid,
			   Oid indexid,
			   List *indexqual,
			   List *indexqualorig,
			   List *indexorderby,
			   List *indexorderbyorig,
			   ScanDirection indexscandir)
{
	IndexScan  *node = makeNode(IndexScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->indexid = indexid;
	node->indexqual = indexqual;
	node->indexqualorig = indexqualorig;
	node->indexorderby = indexorderby;
	node->indexorderbyorig = indexorderbyorig;
	node->indexorderdir = indexscandir;

	return node;
}

static IndexOnlyScan *
make_indexonlyscan(List *qptlist,
				   List *qpqual,
				   Index scanrelid,
				   Oid indexid,
				   List *indexqual,
				   List *indexqualorig,
				   List *indexorderby,
				   List *indextlist,
				   ScanDirection indexscandir)
{
	IndexOnlyScan *node = makeNode(IndexOnlyScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->indexid = indexid;
	node->indexqual = indexqual;
	node->indexqualorig = indexqualorig;
	node->indexorderby = indexorderby;
	node->indextlist = indextlist;
	node->indexorderdir = indexscandir;

	return node;
}

static BitmapIndexScan *
make_bitmap_indexscan(Index scanrelid,
					  Oid indexid,
					  List *indexqual,
					  List *indexqualorig)
{
	BitmapIndexScan *node = makeNode(BitmapIndexScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = NIL;		/* not used */
	plan->qual = NIL;			/* not used */
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->indexid = indexid;
	node->indexqual = indexqual;
	node->indexqualorig = indexqualorig;

	return node;
}

static BitmapHeapScan *
make_bitmap_heapscan(List *qptlist,
					 List *qpqual,
					 Plan *lefttree,
					 List *bitmapqualorig,
					 Index scanrelid)
{
	BitmapHeapScan *node = makeNode(BitmapHeapScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->bitmapqualorig = bitmapqualorig;

	return node;
}

static TidScan *
make_tidscan(List *qptlist,
			 List *qpqual,
			 Index scanrelid,
			 List *tidquals)
{
	TidScan    *node = makeNode(TidScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->tidquals = tidquals;

	return node;
}

SubqueryScan *
make_subqueryscan(List *qptlist,
				  List *qpqual,
				  Index scanrelid,
				  Plan *subplan)
{
	SubqueryScan *node = makeNode(SubqueryScan);
	Plan	   *plan = &node->scan.plan;

	/*
	 * Cost is figured here for the convenience of prepunion.c.  Note this is
	 * only correct for the case where qpqual is empty; otherwise caller
	 * should overwrite cost with a better estimate.
	 */
	copy_plan_costsize(plan, subplan);
	plan->total_cost += cpu_tuple_cost * subplan->plan_rows;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	plan->extParam = bms_copy(subplan->extParam);
	plan->allParam = bms_copy(subplan->allParam);

	/*
	 * Note that, in most scan nodes, scanrelid refers to an entry in the rtable of the
	 * containing plan; in a subqueryscan node, the containing plan is the higher
	 * level plan!
	 */
	node->scan.scanrelid = scanrelid;

	node->subplan = subplan;

	return node;
}

static FunctionScan *
make_functionscan(List *qptlist,
				  List *qpqual,
				  Index scanrelid,
				  List *functions,
				  bool funcordinality)
{
	FunctionScan *node = makeNode(FunctionScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->functions = functions;
	node->funcordinality = funcordinality;

	return node;
}

static TableFunctionScan *
make_tablefunction(List *qptlist, List *qpqual, Plan *subplan,
				   Index scanrelid, RangeTblFunction *function)
{
	TableFunctionScan *node = makeNode(TableFunctionScan);
	Plan	   *plan = &node->scan.plan;

	copy_plan_costsize(plan, subplan);  /* only care about copying size */

	/* FIXME: fix costing */
	plan->startup_cost  = subplan->startup_cost;
	plan->total_cost    = subplan->total_cost;
	plan->total_cost   += 2 * plan->plan_rows;

	plan->qual			= qpqual;
	plan->targetlist	= qptlist;
	plan->righttree		= NULL;

	/* Fill in information for the subplan */
	plan->lefttree		 = subplan;
	node->scan.scanrelid = scanrelid;
	node->function = function;

	return node;
}

static ValuesScan *
make_valuesscan(List *qptlist,
				List *qpqual,
				Index scanrelid,
				List *values_lists)
{
	ValuesScan *node = makeNode(ValuesScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->values_lists = values_lists;

	return node;
}

static pg_attribute_unused() CteScan *
make_ctescan(List *qptlist,
			 List *qpqual,
			 Index scanrelid,
			 int ctePlanId,
			 int cteParam)
{
	CteScan    *node = makeNode(CteScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->ctePlanId = ctePlanId;
	node->cteParam = cteParam;

	return node;
}

static WorkTableScan *
make_worktablescan(List *qptlist,
				   List *qpqual,
				   Index scanrelid,
				   int wtParam)
{
	WorkTableScan *node = makeNode(WorkTableScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->wtParam = wtParam;

	return node;
}

ForeignScan *
make_foreignscan(List *qptlist,
				 List *qpqual,
				 Index scanrelid,
				 List *fdw_exprs,
				 List *fdw_private)
{
	ForeignScan *node = makeNode(ForeignScan);
	Plan	   *plan = &node->scan.plan;

	/* cost will be filled in by create_foreignscan_plan */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->fdw_exprs = fdw_exprs;
	node->fdw_private = fdw_private;
	/* fsSystemCol will be filled in by create_foreignscan_plan */
	node->fsSystemCol = false;

	return node;
}

Append *
make_append(List *appendplans, List *tlist)
{
	Append	   *node = makeNode(Append);
	Plan	   *plan = &node->plan;
	double		total_size;
	ListCell   *subnode;

	/*
	 * Compute cost as sum of subplan costs.  We charge nothing extra for the
	 * Append itself, which perhaps is too optimistic, but since it doesn't do
	 * any selection or projection, it is a pretty cheap node.
	 *
	 * If you change this, see also create_append_path().  Also, the size
	 * calculations should match set_append_rel_pathlist().  It'd be better
	 * not to duplicate all this logic, but some callers of this function
	 * aren't working from an appendrel or AppendPath, so there's noplace to
	 * copy the data from.
	 */
	plan->startup_cost = 0;
	plan->total_cost = 0;
	plan->plan_rows = 0;
	total_size = 0;
	foreach(subnode, appendplans)
	{
		Plan	   *subplan = (Plan *) lfirst(subnode);

		if (subnode == list_head(appendplans))	/* first node? */
			plan->startup_cost = subplan->startup_cost;
		plan->total_cost += subplan->total_cost;
		plan->plan_rows += subplan->plan_rows;
		total_size += subplan->plan_width * subplan->plan_rows;
	}
	/* GPDB_84_MERGE_FIXME: ensure this math is okay compared to before the
	 * merge */
	if (plan->plan_rows > 0)
		plan->plan_width = rint(total_size / plan->plan_rows);
	else
		plan->plan_width = 0;

	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->appendplans = appendplans;

	return node;
}

RecursiveUnion *
make_recursive_union(List *tlist,
					 Plan *lefttree,
					 Plan *righttree,
					 int wtParam,
					 List *distinctList,
					 long numGroups)
{
	RecursiveUnion *node = makeNode(RecursiveUnion);
	Plan	   *plan = &node->plan;
	int			numCols = list_length(distinctList);

	cost_recursive_union(plan, lefttree, righttree);

	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	node->wtParam = wtParam;

	/*
	 * convert SortGroupClause list into arrays of attr indexes and equality
	 * operators, as wanted by executor
	 */
	node->numCols = numCols;
	if (numCols > 0)
	{
		int			keyno = 0;
		AttrNumber *dupColIdx;
		Oid		   *dupOperators;
		ListCell   *slitem;

		dupColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numCols);
		dupOperators = (Oid *) palloc(sizeof(Oid) * numCols);

		foreach(slitem, distinctList)
		{
			SortGroupClause *sortcl = (SortGroupClause *) lfirst(slitem);
			TargetEntry *tle = get_sortgroupclause_tle(sortcl,
													   plan->targetlist);

			dupColIdx[keyno] = tle->resno;
			dupOperators[keyno] = sortcl->eqop;
			Assert(OidIsValid(dupOperators[keyno]));
			keyno++;
		}
		node->dupColIdx = dupColIdx;
		node->dupOperators = dupOperators;
	}
	node->numGroups = numGroups;

	return node;
}

static BitmapAnd *
make_bitmap_and(List *bitmapplans)
{
	BitmapAnd  *node = makeNode(BitmapAnd);
	Plan	   *plan = &node->plan;

	/* cost should be inserted by caller */
	plan->targetlist = NIL;
	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->bitmapplans = bitmapplans;

	return node;
}

static BitmapOr *
make_bitmap_or(List *bitmapplans)
{
	BitmapOr   *node = makeNode(BitmapOr);
	Plan	   *plan = &node->plan;

	/* cost should be inserted by caller */
	plan->targetlist = NIL;
	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->bitmapplans = bitmapplans;

	return node;
}

NestLoop *
make_nestloop(List *tlist,
			  List *joinclauses,
			  List *otherclauses,
			  List *nestParams,
			  Plan *lefttree,
			  Plan *righttree,
			  JoinType jointype)
{
	NestLoop   *node = makeNode(NestLoop);
	Plan	   *plan = &node->join.plan;

	/* cost should be inserted by caller */
	plan->targetlist = tlist;
	plan->qual = otherclauses;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	node->join.jointype = jointype;
	node->join.joinqual = joinclauses;
	node->nestParams = nestParams;

	return node;
}

HashJoin *
make_hashjoin(List *tlist,
			  List *joinclauses,
			  List *otherclauses,
			  List *hashclauses,
			  List *hashqualclauses,
			  Plan *lefttree,
			  Plan *righttree,
			  JoinType jointype)
{
	HashJoin   *node = makeNode(HashJoin);
	Plan	   *plan = &node->join.plan;

	/* cost should be inserted by caller */
	plan->targetlist = tlist;
	plan->qual = otherclauses;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	node->hashclauses = hashclauses;
	node->hashqualclauses = hashqualclauses;
	node->join.jointype = jointype;
	node->join.joinqual = joinclauses;

	return node;
}

Hash *
make_hash(Plan *lefttree,
		  Oid skewTable,
		  AttrNumber skewColumn,
		  bool skewInherit,
		  Oid skewColType,
		  int32 skewColTypmod)
{
	Hash	   *node = makeNode(Hash);
	Plan	   *plan = &node->plan;

	copy_plan_costsize(plan, lefttree);

	/*
	 * For plausibility, make startup & total costs equal total cost of input
	 * plan; this only affects EXPLAIN display not decisions.
	 */
	plan->startup_cost = plan->total_cost;
	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	node->skewTable = skewTable;
	node->skewColumn = skewColumn;
	node->skewInherit = skewInherit;
	node->skewColType = skewColType;
	node->skewColTypmod = skewColTypmod;

	node->rescannable = false;	/* CDB (unused for now) */

	return node;
}

MergeJoin *
make_mergejoin(List *tlist,
			   List *joinclauses,
			   List *otherclauses,
			   List *mergeclauses,
			   Oid *mergefamilies,
			   Oid *mergecollations,
			   int *mergestrategies,
			   bool *mergenullsfirst,
			   Plan *lefttree,
			   Plan *righttree,
			   JoinType jointype)
{
	MergeJoin  *node = makeNode(MergeJoin);
	Plan	   *plan = &node->join.plan;

	/* cost should be inserted by caller */
	plan->targetlist = tlist;
	plan->qual = otherclauses;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	node->mergeclauses = mergeclauses;
	node->mergeFamilies = mergefamilies;
	node->mergeCollations = mergecollations;
	node->mergeStrategies = mergestrategies;
	node->mergeNullsFirst = mergenullsfirst;
	node->join.jointype = jointype;
	node->join.joinqual = joinclauses;

	return node;
}

/*
 * make_sort --- basic routine to build a Sort plan node
 *
 * Caller must have built the sortColIdx, sortOperators, collations, and
 * nullsFirst arrays already.
 * limit_tuples is as for cost_sort (in particular, pass -1 if no limit)
 */
Sort *
make_sort(PlannerInfo *root, Plan *lefttree, int numCols,
		  AttrNumber *sortColIdx, Oid *sortOperators,
		  Oid *collations, bool *nullsFirst,
		  double limit_tuples)
{
	Sort	   *node = makeNode(Sort);
	Plan	   *plan = &node->plan;

	copy_plan_costsize(plan, lefttree); /* only care about copying size */
	plan = add_sort_cost(root, plan, limit_tuples);

	plan->targetlist = cdbpullup_targetlist(lefttree,
				 cdbpullup_exprHasSubplanRef((Expr *) lefttree->targetlist));
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	node->numCols = numCols;
	node->sortColIdx = sortColIdx;
	node->sortOperators = sortOperators;
	node->collations = collations;
	node->nullsFirst = nullsFirst;

	Assert(sortColIdx[0] != 0);

	node->noduplicates = false; /* CDB */

	node->share_type = SHARE_NOTSHARED;
	node->share_id = SHARE_ID_NOT_SHARED;
	node->driver_slice = -1;
	node->nsharer = 0;
	node->nsharer_xslice = 0;

	return node;
}

/*
 * add_sort_cost --- basic routine to accumulate Sort cost into a
 * plan node representing the input cost.
 *
 * Unused arguments (e.g., sortColIdx and sortOperators arrays) are
 * included to allow for future improvements to sort costing.  Note
 * that root may be NULL (e.g. when called outside make_sort).
 */
Plan *
add_sort_cost(PlannerInfo *root, Plan *input, double limit_tuples)
{
	Path		sort_path;		/* dummy for result of cost_sort */

	cost_sort(&sort_path, root, NIL,
			  input->total_cost,
			  input->plan_rows,
			  input->plan_width,
			  0.0,
			  work_mem,
			  limit_tuples);
	input->startup_cost = sort_path.startup_cost;
	input->total_cost = sort_path.total_cost;

	return input;
}

/*
 * prepare_sort_from_pathkeys
 *	  Prepare to sort according to given pathkeys
 *
 * This is used to set up for both Sort and MergeAppend nodes.  It calculates
 * the executor's representation of the sort key information, and adjusts the
 * plan targetlist if needed to add resjunk sort columns.
 *
 * Input parameters:
 *	  'lefttree' is the plan node which yields input tuples
 *	  'pathkeys' is the list of pathkeys by which the result is to be sorted
 *	  'relids' identifies the child relation being sorted, if any
 *	  'reqColIdx' is NULL or an array of required sort key column numbers
 *	  'adjust_tlist_in_place' is TRUE if lefttree must be modified in-place
 *
 * We must convert the pathkey information into arrays of sort key column
 * numbers, sort operator OIDs, collation OIDs, and nulls-first flags,
 * which is the representation the executor wants.  These are returned into
 * the output parameters *p_numsortkeys etc.
 *
 * When looking for matches to an EquivalenceClass's members, we will only
 * consider child EC members if they match 'relids'.  This protects against
 * possible incorrect matches to child expressions that contain no Vars.
 *
 * If reqColIdx isn't NULL then it contains sort key column numbers that
 * we should match.  This is used when making child plans for a MergeAppend;
 * it's an error if we can't match the columns.
 *
 * If the pathkeys include expressions that aren't simple Vars, we will
 * usually need to add resjunk items to the input plan's targetlist to
 * compute these expressions, since the Sort/MergeAppend node itself won't
 * do any such calculations.  If the input plan type isn't one that can do
 * projections, this means adding a Result node just to do the projection.
 * However, the caller can pass adjust_tlist_in_place = TRUE to force the
 * lefttree tlist to be modified in-place regardless of whether the node type
 * can project --- we use this for fixing the tlist of MergeAppend itself.
 *
 * Returns the node which is to be the input to the Sort (either lefttree,
 * or a Result stacked atop lefttree).
 */
static Plan *
prepare_sort_from_pathkeys(PlannerInfo *root, Plan *lefttree, List *pathkeys,
						   Relids relids,
						   const AttrNumber *reqColIdx,
						   bool adjust_tlist_in_place,
						   int *p_numsortkeys,
						   AttrNumber **p_sortColIdx,
						   Oid **p_sortOperators,
						   Oid **p_collations,
						   bool **p_nullsFirst,
						   bool add_keys_to_targetlist)
{
	List	   *tlist = lefttree->targetlist;
	ListCell   *i;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;

	/*
	 * We will need at most list_length(pathkeys) sort columns; possibly less
	 */
	numsortkeys = list_length(pathkeys);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
	collations = (Oid *) palloc(numsortkeys * sizeof(Oid));
	nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

	numsortkeys = 0;

	foreach(i, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(i);
		EquivalenceClass *ec = pathkey->pk_eclass;
		EquivalenceMember *em;
		TargetEntry *tle = NULL;
		Oid			pk_datatype = InvalidOid;
		Oid			sortop;
		ListCell   *j;

		if (ec->ec_has_volatile)
		{
			/*
			 * If the pathkey's EquivalenceClass is volatile, then it must
			 * have come from an ORDER BY clause, and we have to match it to
			 * that same targetlist entry.
			 */
			if (ec->ec_sortref == 0)	/* can't happen */
				elog(ERROR, "volatile EquivalenceClass has no sortref");
			tle = get_sortgroupref_tle(ec->ec_sortref, tlist);
			Assert(tle);
			Assert(list_length(ec->ec_members) == 1);
			pk_datatype = ((EquivalenceMember *) linitial(ec->ec_members))->em_datatype;
		}
		else if (reqColIdx != NULL)
		{
			/*
			 * If we are given a sort column number to match, only consider
			 * the single TLE at that position.  It's possible that there is
			 * no such TLE, in which case fall through and generate a resjunk
			 * targetentry (we assume this must have happened in the parent
			 * plan as well).  If there is a TLE but it doesn't match the
			 * pathkey's EC, we do the same, which is probably the wrong thing
			 * but we'll leave it to caller to complain about the mismatch.
			 */
			tle = get_tle_by_resno(tlist, reqColIdx[numsortkeys]);
			if (tle)
			{
				em = find_ec_member_for_tle(ec, tle, relids);
				if (em)
				{
					/* found expr at right place in tlist */
					pk_datatype = em->em_datatype;
				}
				else
					tle = NULL;
			}
		}
		else
		{
			/*
			 * Otherwise, we can sort by any non-constant expression listed in
			 * the pathkey's EquivalenceClass.  For now, we take the first
			 * tlist item found in the EC. If there's no match, we'll generate
			 * a resjunk entry using the first EC member that is an expression
			 * in the input's vars.  (The non-const restriction only matters
			 * if the EC is below_outer_join; but if it isn't, it won't
			 * contain consts anyway, else we'd have discarded the pathkey as
			 * redundant.)
			 *
			 * XXX if we have a choice, is there any way of figuring out which
			 * might be cheapest to execute?  (For example, int4lt is likely
			 * much cheaper to execute than numericlt, but both might appear
			 * in the same equivalence class...)  Not clear that we ever will
			 * have an interesting choice in practice, so it may not matter.
			 */
			foreach(j, tlist)
			{
				tle = (TargetEntry *) lfirst(j);
				em = find_ec_member_for_tle(ec, tle, relids);
				if (em)
				{
					/* found expr already in tlist */
					pk_datatype = em->em_datatype;
					break;
				}
				tle = NULL;
			}
		}

		if (!tle)
		{
			/*
			 * No matching tlist item; look for a computable expression. Note
			 * that we treat Aggrefs as if they were variables; this is
			 * necessary when attempting to sort the output from an Agg node
			 * for use in a WindowFunc (since grouping_planner will have
			 * treated the Aggrefs as variables, too).
			 */
			Expr	   *sortexpr = NULL;

			if (!add_keys_to_targetlist)
				break;

			foreach(j, ec->ec_members)
			{
				EquivalenceMember *em = (EquivalenceMember *) lfirst(j);
				List	   *exprvars;
				ListCell   *k;

				/*
				 * We shouldn't be trying to sort by an equivalence class that
				 * contains a constant, so no need to consider such cases any
				 * further.
				 */
				if (em->em_is_const)
					continue;

				/*
				 * Ignore child members unless they match the rel being
				 * sorted.
				 */
				if (em->em_is_child &&
					!bms_equal(em->em_relids, relids))
					continue;

				sortexpr = em->em_expr;
				exprvars = pull_var_clause((Node *) sortexpr,
										   PVC_INCLUDE_AGGREGATES,
										   PVC_INCLUDE_PLACEHOLDERS);
				foreach(k, exprvars)
				{
					if (!tlist_member_ignore_relabel(lfirst(k), tlist))
						break;
				}
				list_free(exprvars);
				if (!k)
				{
					pk_datatype = em->em_datatype;
					break;		/* found usable expression */
				}
			}
			if (!j)
				elog(ERROR, "could not find pathkey item to sort");

			/*
			 * Do we need to insert a Result node?
			 */
			if (!adjust_tlist_in_place &&
				!is_projection_capable_plan(lefttree))
			{
				/* copy needed so we don't modify input's tlist below */
				tlist = copyObject(tlist);
				lefttree = (Plan *) make_result(root, tlist, NULL,
												lefttree);
				if (lefttree->lefttree->flow)
					lefttree->flow = pull_up_Flow(lefttree, lefttree->lefttree);
			}

			/* Don't bother testing is_projection_capable_plan again */
			adjust_tlist_in_place = true;

			/*
			 * Add resjunk entry to input's tlist
			 */
			tle = makeTargetEntry(sortexpr,
								  list_length(tlist) + 1,
								  NULL,
								  true);
			tlist = lappend(tlist, tle);
			lefttree->targetlist = tlist;		/* just in case NIL before */
		}

		/*
		 * Look up the correct sort operator from the PathKey's slightly
		 * abstracted representation.
		 */
		sortop = get_opfamily_member(pathkey->pk_opfamily,
									 pk_datatype,
									 pk_datatype,
									 pathkey->pk_strategy);
		if (!OidIsValid(sortop))	/* should not happen */
			elog(ERROR, "could not find member %d(%u,%u) of opfamily %u",
				 pathkey->pk_strategy, pk_datatype, pk_datatype,
				 pathkey->pk_opfamily);

		/* Add the column to the sort arrays */
		sortColIdx[numsortkeys] = tle->resno;
		sortOperators[numsortkeys] = sortop;
		collations[numsortkeys] = ec->ec_collation;
		nullsFirst[numsortkeys] = pathkey->pk_nulls_first;
		numsortkeys++;
	}

	/* Return results */
	*p_numsortkeys = numsortkeys;
	*p_sortColIdx = sortColIdx;
	*p_sortOperators = sortOperators;
	*p_collations = collations;
	*p_nullsFirst = nullsFirst;

	return lefttree;
}

/*
 * find_ec_member_for_tle
 *		Locate an EquivalenceClass member matching the given TLE, if any
 *
 * Child EC members are ignored unless they match 'relids'.
 */
static EquivalenceMember *
find_ec_member_for_tle(EquivalenceClass *ec,
					   TargetEntry *tle,
					   Relids relids)
{
	Expr	   *tlexpr;
	ListCell   *lc;

	/* We ignore binary-compatible relabeling on both ends */
	tlexpr = tle->expr;
	while (tlexpr && IsA(tlexpr, RelabelType))
		tlexpr = ((RelabelType *) tlexpr)->arg;

	foreach(lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
		Expr	   *emexpr;

		/*
		 * We shouldn't be trying to sort by an equivalence class that
		 * contains a constant, so no need to consider such cases any further.
		 */
		if (em->em_is_const)
			continue;

		/*
		 * Ignore child members unless they match the rel being sorted.
		 */
		if (em->em_is_child &&
			!bms_equal(em->em_relids, relids))
			continue;

		/* Match if same expression (after stripping relabel) */
		emexpr = em->em_expr;
		while (emexpr && IsA(emexpr, RelabelType))
			emexpr = ((RelabelType *) emexpr)->arg;

		if (equal(emexpr, tlexpr))
			return em;
	}

	return NULL;
}

/*
 * make_sort_from_pathkeys
 *	  Create sort plan to sort according to given pathkeys
 *
 *	  'lefttree' is the node which yields input tuples
 *	  'pathkeys' is the list of pathkeys by which the result is to be sorted
 *	  'limit_tuples' is the bound on the number of output tuples;
 *				-1 if no bound
 *	  'add_keys_to_targetlist' is true if it is ok to append to the subplan's
 *				targetlist or insert a Result node atop the subplan to
 *				evaluate sort key exprs that are not already present in the
 *				subplan's tlist.
 */
Sort *
make_sort_from_pathkeys(PlannerInfo *root, Plan *lefttree, List *pathkeys,
						double limit_tuples, bool add_keys_to_targetlist)
{
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;

	/* Compute sort column info, and adjust lefttree as needed */
	lefttree = prepare_sort_from_pathkeys(root, lefttree, pathkeys,
										  NULL,
										  NULL,
										  false,
										  &numsortkeys,
										  &sortColIdx,
										  &sortOperators,
										  &collations,
										  &nullsFirst,
										  add_keys_to_targetlist);

	if (lefttree == NULL)
	{
		Assert(!add_keys_to_targetlist);
		return NULL;
	}

	/* Now build the Sort node */
	return make_sort(root, lefttree, numsortkeys,
					 sortColIdx, sortOperators, collations,
					 nullsFirst, limit_tuples);
}

/*
 * make_sort_from_sortclauses
 *	  Create sort plan to sort according to given sortclauses
 *
 *	  'sortcls' is a list of SortGroupClauses
 *	  'lefttree' is the node which yields input tuples
 */
Sort *
make_sort_from_sortclauses(PlannerInfo *root, List *sortcls, Plan *lefttree)
{
	List	   *sub_tlist = lefttree->targetlist;
	ListCell   *l;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;

	/* Convert list-ish representation to arrays wanted by executor */
	numsortkeys = list_length(sortcls);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
	collations = (Oid *) palloc(numsortkeys * sizeof(Oid));
	nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

	numsortkeys = 0;
	foreach(l, sortcls)
	{
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(l);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, sub_tlist);

		sortColIdx[numsortkeys] = tle->resno;
		sortOperators[numsortkeys] = sortcl->sortop;
		collations[numsortkeys] = exprCollation((Node *) tle->expr);
		nullsFirst[numsortkeys] = sortcl->nulls_first;
		numsortkeys++;
	}

	return make_sort(root, lefttree, numsortkeys,
					 sortColIdx, sortOperators, collations,
					 nullsFirst, -1.0);
}

/*
 * make_sort_from_groupcols
 *	  Create sort plan to sort based on grouping columns
 *
 * 'groupcls' is the list of SortGroupClauses
 * 'grpColIdx' gives the column numbers to use
 * 'appendGrouping' represents whether to append a Grouping
 *	  as the last sort key, used for grouping extension.
 *
 * This might look like it could be merged with make_sort_from_sortclauses,
 * but presently we *must* use the grpColIdx[] array to locate sort columns,
 * because the child plan's tlist is not marked with ressortgroupref info
 * appropriate to the grouping node.  So, only the sort ordering info
 * is used from the SortGroupClause entries.
 */
Sort *
make_sort_from_groupcols(PlannerInfo *root,
						 List *groupcls,
						 AttrNumber *grpColIdx,
						 bool appendGrouping,
						 Plan *lefttree)
{
	List	   *sub_tlist = lefttree->targetlist;
	ListCell   *l;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;
	List	   *flat_groupcls;

	/*
	 * We will need at most list_length(groupcls) sort columns; possibly less
	 */
	numsortkeys = num_distcols_in_grouplist(groupcls);

	if (appendGrouping)
		numsortkeys++;

	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
	collations = (Oid *) palloc(numsortkeys * sizeof(Oid));
	nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

	numsortkeys = 0;

	flat_groupcls = flatten_grouping_list(groupcls);

	foreach(l, flat_groupcls)
	{
		SortGroupClause *grpcl = (SortGroupClause *) lfirst(l);
		TargetEntry *tle = get_tle_by_resno(sub_tlist, grpColIdx[numsortkeys]);

		if (!tle)
			elog(ERROR, "could not retrieve tle for sort-from-groupcols");

		sortColIdx[numsortkeys] = tle->resno;
		sortOperators[numsortkeys] = grpcl->sortop;
		collations[numsortkeys] = exprCollation((Node *) tle->expr);
		nullsFirst[numsortkeys] = grpcl->nulls_first;
		numsortkeys++;
	}

	if (appendGrouping)
	{
		Oid			lt_opr;

		/* Grouping will be the last entry in grpColIdx */
		TargetEntry *tle = get_tle_by_resno(sub_tlist, grpColIdx[numsortkeys]);

		if (tle->resname == NULL)
			tle->resname = "grouping";

		get_sort_group_operators(exprType((Node *) tle->expr),
								 true, false, false,
								 &lt_opr, NULL, NULL, NULL);

		numsortkeys = add_sort_column(tle->resno, lt_opr, InvalidOid, false,
									  numsortkeys, sortColIdx, sortOperators, collations, nullsFirst);
	}


	Assert(numsortkeys > 0);

	return make_sort(root, lefttree, numsortkeys,
					 sortColIdx, sortOperators, collations,
					 nullsFirst, -1.0);
}

/*
 * Reconstruct a new list of GroupClause based on the given grpCols.
 *
 * The original grouping clauses may contain grouping extensions. This function
 * extract the raw grouping attributes and construct a list of GroupClauses
 * that contains only ordinary grouping.
 */
List *
reconstruct_group_clause(List *orig_groupClause, List *tlist, AttrNumber *grpColIdx, int numcols)
{
	List	   *flat_groupcls;
	List	   *new_groupClause = NIL;
	int			grpno;

	flat_groupcls = flatten_grouping_list(orig_groupClause);
	for (grpno = 0; grpno < numcols; grpno++)
	{
		ListCell   *lc = NULL;
		TargetEntry *te;
		SortGroupClause *gc = NULL;

		te = get_tle_by_resno(tlist, grpColIdx[grpno]);

		foreach(lc, flat_groupcls)
		{
			gc = (SortGroupClause *) lfirst(lc);

			if (gc->tleSortGroupRef == te->ressortgroupref)
				break;
		}
		if (lc != NULL)
			new_groupClause = lappend(new_groupClause, gc);
	}

	return new_groupClause;
}

/* --------------------------------------------------------------------
 * make_motion -- creates a Motion node.
 * Caller must have built the pHashDefn, pFixedDefn,
 * and pSortDefn structs already.
 * useExecutorVarFormat is true if make_motion is called after setrefs
 * This call only make a motion node, without filling in flow info
 * After calling this function, caller need to call add_slice_to_motion
 * --------------------------------------------------------------------
 */
Motion *
make_motion(PlannerInfo *root, Plan *lefttree,
			int numSortCols, AttrNumber *sortColIdx,
			Oid *sortOperators, Oid *collations, bool *nullsFirst,
			bool useExecutorVarFormat)
{
    Motion *node = makeNode(Motion);
    Plan   *plan = &node->plan;

	Assert(lefttree);
	Assert(!IsA(lefttree, Motion));

	plan->startup_cost = lefttree->startup_cost;
	plan->total_cost = lefttree->total_cost;
	plan->plan_rows = lefttree->plan_rows;
	plan->plan_width = lefttree->plan_width;

	plan->targetlist = cdbpullup_targetlist(lefttree, useExecutorVarFormat);
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	plan->dispatch = DISPATCH_PARALLEL;

	node->numSortCols = numSortCols;
	node->sortColIdx = sortColIdx;
	node->sortOperators = sortOperators;
	node->collations = collations;
	node->nullsFirst = nullsFirst;

#ifdef USE_ASSERT_CHECKING
	/*
	 * If the child node was a Sort, then surely the order the caller gave us
	 * must match that of the underlying sort.
	 */
	if (numSortCols > 0 && IsA(lefttree, Sort))
	{
		Sort	   *childsort = (Sort *) lefttree;
		Assert(childsort->numCols >= node->numSortCols);
		Assert(memcmp(childsort->sortColIdx, node->sortColIdx, node->numSortCols * sizeof(AttrNumber)) == 0);
		Assert(memcmp(childsort->sortOperators, node->sortOperators, node->numSortCols * sizeof(Oid)) == 0);
		Assert(memcmp(childsort->nullsFirst, node->nullsFirst, node->numSortCols * sizeof(bool)) == 0);
	}
#endif

	node->sendSorted = (numSortCols > 0);

	plan->extParam = bms_copy(lefttree->extParam);
	plan->allParam = bms_copy(lefttree->allParam);

	plan->flow = NULL;

	return node;
}

Material *
make_material(Plan *lefttree)
{
	Material   *node = makeNode(Material);
	Plan	   *plan = &node->plan;

	/* cost should be inserted by caller */
	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	node->cdb_strict = false;
	node->share_type = SHARE_NOTSHARED;
	node->share_id = SHARE_ID_NOT_SHARED;
	node->driver_slice = -1;
	node->nsharer = 0;
	node->nsharer_xslice = 0;

	return node;
}

/*
 * materialize_finished_plan: stick a Material node atop a completed plan
 *
 * There are a couple of places where we want to attach a Material node
 * after completion of subquery_planner().  This currently requires hackery.
 * Since subquery_planner has already run SS_finalize_plan on the subplan
 * tree, we have to kluge up parameter lists for the Material node.
 * Possibly this could be fixed by postponing SS_finalize_plan processing
 * until setrefs.c is run?
 */
Plan *
materialize_finished_plan(PlannerInfo *root, Plan *subplan)
{
	Plan	   *matplan;
	Path		matpath;		/* dummy for result of cost_material */

	matplan = (Plan *) make_material(subplan);

	/* Set cost data */
	cost_material(&matpath,
				  root,
				  subplan->startup_cost,
				  subplan->total_cost,
				  subplan->plan_rows,
				  subplan->plan_width);
	matplan->startup_cost = matpath.startup_cost;
	matplan->total_cost = matpath.total_cost;
	matplan->plan_rows = subplan->plan_rows;
	matplan->plan_width = subplan->plan_width;

	/* MPP -- propagate dispatch method and flow */
	matplan->dispatch = subplan->dispatch;
	matplan->flow = copyObject(subplan->flow);

	/* parameter kluge --- see comments above */
	matplan->extParam = bms_copy(subplan->extParam);
	matplan->allParam = bms_copy(subplan->allParam);

	return matplan;
}

Agg *
make_agg(PlannerInfo *root, List *tlist, List *qual,
		 AggStrategy aggstrategy, const AggClauseCosts *aggcosts,
		 bool streaming,
		 int numGroupCols, AttrNumber *grpColIdx, Oid *grpOperators,
		 long numGroups, int num_nullcols,
		 uint64 input_grouping, uint64 grouping,
		 int rollupGSTimes,
		 Plan *lefttree)
{
	Agg		   *node = makeNode(Agg);
	Plan	   *plan = &node->plan;

	node->aggstrategy = aggstrategy;
	node->numCols = numGroupCols;
	node->grpColIdx = grpColIdx;
	node->grpOperators = grpOperators;
	node->numGroups = numGroups;
	if (aggcosts)
		node->transSpace = aggcosts->transitionSpace;
	else
		node->transSpace = 0;
	node->numNullCols = num_nullcols;
	node->inputGrouping = input_grouping;
	node->grouping = grouping;
	node->inputHasGrouping = false;
	node->rollupGSTimes = rollupGSTimes;
	node->lastAgg = false;
	node->streaming = streaming;
	node->aggParams = NULL;		/* SS_finalize_plan() will fill this */

	copy_plan_costsize(plan, lefttree); /* only care about copying size */

	add_agg_cost(root, plan, tlist, qual, aggstrategy, streaming,
				 numGroupCols, numGroups, aggcosts);

	plan->qual = qual;
	plan->targetlist = tlist;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	plan->extParam = bms_copy(lefttree->extParam);
	plan->allParam = bms_copy(lefttree->allParam);

	return node;
}

/*
 * add_agg_cost -- basic routine to accumulate Agg cost into a
 * plan node representing the input cost.
 *
 * Note that root may be NULL (e.g., when called from * outside make_agg).
 */
Plan *
add_agg_cost(PlannerInfo *root, Plan *plan,
			 List *tlist, List *qual,
			 AggStrategy aggstrategy,
			 bool streaming,
			 int numGroupCols,
			 long numGroups,
			 const AggClauseCosts *aggcosts)
{
	Path		agg_path;		/* dummy for result of cost_agg */
	QualCost	qual_cost;
	HashAggTableSizes hash_info;
	double entrywidth;

    /* Solution for MPP-11942
     * Before this fix, we calculated the width from the sub_tlist which
     * only contains a subset of the tlist. For example, for the query
     * select a, min(b), max(b), min(c), max(c) from s group by a;
     * the sub_tlist has the columns {a,b,c} while the tlist
     * is {a, min(b), max(b), min(c), max(c)}. Therefore, the plan_width
     * for the above query is calculated to be 12 instead of 20.
     *
     * In this fix, we calculate the actual row width from the tlist.
     */

    plan->plan_width = get_row_width(tlist);

	if (aggstrategy == AGG_HASHED)
	{
		AggClauseCosts dummy_aggcosts;

		/* Like in cost_agg, use all-zero per-aggregate costs if NULL is passed */
		if (aggcosts == NULL)
		{
			Assert(aggstrategy == AGG_HASHED);
			MemSet(&dummy_aggcosts, 0, sizeof(AggClauseCosts));
			aggcosts = &dummy_aggcosts;
		}

		/* The following estimate is very rough but good enough for planning. */
		entrywidth = agg_hash_entrywidth(aggcosts->numAggs,
								   sizeof(HeapTupleData) + sizeof(HeapTupleHeaderData) + plan->plan_width,
								   aggcosts->transitionSpace);
		if (!calcHashAggTableSizes(global_work_mem(root),
								   numGroups,
								   entrywidth,
								   true,
								   &hash_info))
		{
			elog(ERROR, "Planner committed to impossible hash aggregate.");
		}

		cost_agg(&agg_path, root,
				 aggstrategy, aggcosts,
				 numGroupCols, numGroups,
				 plan->startup_cost,
				 plan->total_cost,
				 plan->plan_rows, hash_info.workmem_per_entry,
				 hash_info.nbatches, hash_info.hashentry_width, streaming);
	}
	else
		cost_agg(&agg_path, root,
				 aggstrategy, aggcosts,
				 numGroupCols, numGroups,
				 plan->startup_cost,
				 plan->total_cost,
				 plan->plan_rows, 0.0, 0.0,
				 0.0, false);

	plan->startup_cost = agg_path.startup_cost;
	plan->total_cost = agg_path.total_cost;

	/*
	 * We will produce a single output tuple if not grouping, and a tuple per
	 * group otherwise.
	 */
	if (aggstrategy == AGG_PLAIN)
		plan->plan_rows = 1;
	else
		plan->plan_rows = numGroups;

	/*
	 * We also need to account for the cost of evaluation of the qual (ie, the
	 * HAVING clause) and the tlist.  Note that cost_qual_eval doesn't charge
	 * anything for Aggref nodes; this is okay since they are really
	 * comparable to Vars.
	 *
	 * See notes in add_tlist_costs_to_plan about why only make_agg,
	 * make_windowagg and make_group worry about tlist eval cost.
	 */
	if (qual)
	{
		cost_qual_eval(&qual_cost, qual, root);
		plan->startup_cost += qual_cost.startup;
		plan->total_cost += qual_cost.startup;
		plan->total_cost += qual_cost.per_tuple * plan->plan_rows;
	}
	add_tlist_costs_to_plan(root, plan, tlist);

	return plan;
}

WindowAgg *
make_windowagg(PlannerInfo *root, List *tlist,
			   List *windowFuncs, Index winref,
			   int partNumCols, AttrNumber *partColIdx, Oid *partOperators,
			   int ordNumCols, AttrNumber *ordColIdx, Oid *ordOperators,
			   AttrNumber firstOrderCol, Oid firstOrderCmpOperator, bool firstOrderNullsFirst,
			   int frameOptions, Node *startOffset, Node *endOffset,
			   Plan *lefttree)
{
	WindowAgg  *node = makeNode(WindowAgg);
	Plan	   *plan = &node->plan;
	Path		windowagg_path; /* dummy for result of cost_windowagg */

	node->winref = winref;
	node->partNumCols = partNumCols;
	node->partColIdx = partColIdx;
	node->partOperators = partOperators;
	node->ordNumCols = ordNumCols;
	node->ordColIdx = ordColIdx;
	node->ordOperators = ordOperators;
	node->firstOrderCol = firstOrderCol;
	node->firstOrderCmpOperator= firstOrderCmpOperator;
	node->firstOrderNullsFirst= firstOrderNullsFirst;
	node->frameOptions = frameOptions;
	node->startOffset = startOffset;
	node->endOffset = endOffset;

	copy_plan_costsize(plan, lefttree); /* only care about copying size */
	cost_windowagg(&windowagg_path, root,
				   windowFuncs, partNumCols, ordNumCols,
				   lefttree->startup_cost,
				   lefttree->total_cost,
				   lefttree->plan_rows);
	plan->startup_cost = windowagg_path.startup_cost;
	plan->total_cost = windowagg_path.total_cost;

	/*
	 * We also need to account for the cost of evaluation of the tlist.
	 *
	 * See notes in add_tlist_costs_to_plan about why only make_agg,
	 * make_windowagg and make_group worry about tlist eval cost.
	 */
	add_tlist_costs_to_plan(root, plan, tlist);

	plan->targetlist = tlist;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	/* WindowAgg nodes never have a qual clause */
	plan->qual = NIL;

	if (lefttree->flow)
		plan->flow = pull_up_Flow(plan, lefttree);

	return node;
}

/*
 * distinctList is a list of SortGroupClauses, identifying the targetlist items
 * that should be considered by the Unique filter.  The input path must
 * already be sorted accordingly.
 */
Unique *
make_unique(Plan *lefttree, List *distinctList)
{
	Unique	   *node = makeNode(Unique);
	Plan	   *plan = &node->plan;
	int			numCols = list_length(distinctList);
	int			keyno = 0;
	AttrNumber *uniqColIdx;
	Oid		   *uniqOperators;
	ListCell   *slitem;

	copy_plan_costsize(plan, lefttree);

	/*
	 * Charge one cpu_operator_cost per comparison per input tuple. We assume
	 * all columns get compared at most of the tuples.  (XXX probably this is
	 * an overestimate.)
	 */
	plan->total_cost += cpu_operator_cost * plan->plan_rows * numCols;

	/*
	 * plan->plan_rows is left as a copy of the input subplan's plan_rows; ie,
	 * we assume the filter removes nothing.  The caller must alter this if he
	 * has a better idea.
	 */

	plan->targetlist = cdbpullup_targetlist(lefttree,
				 cdbpullup_exprHasSubplanRef((Expr *) lefttree->targetlist));
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	/*
	 * convert SortGroupClause list into arrays of attr indexes and equality
	 * operators, as wanted by executor
	 */
	Assert(numCols > 0);
	uniqColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numCols);
	uniqOperators = (Oid *) palloc(sizeof(Oid) * numCols);

	foreach(slitem, distinctList)
	{
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(slitem);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, plan->targetlist);

		uniqColIdx[keyno] = tle->resno;
		uniqOperators[keyno] = sortcl->eqop;
		Assert(OidIsValid(uniqOperators[keyno]));
		keyno++;
	}

	node->numCols = numCols;
	node->uniqColIdx = uniqColIdx;
	node->uniqOperators = uniqOperators;

	/* CDB */	/* pass DISTINCT to sort */
	if (IsA(lefttree, Sort) && gp_enable_sort_distinct)
	{
		Sort	   *pSort = (Sort *) lefttree;

		pSort->noduplicates = true;
	}

	return node;
}

/*
 * distinctList is a list of SortGroupClauses, identifying the targetlist
 * items that should be considered by the SetOp filter.  The input path must
 * already be sorted accordingly.
 */
SetOp *
make_setop(SetOpCmd cmd, SetOpStrategy strategy, Plan *lefttree,
		   List *distinctList, AttrNumber flagColIdx, int firstFlag,
		   long numGroups, double outputRows)
{
	SetOp	   *node = makeNode(SetOp);
	Plan	   *plan = &node->plan;
	int			numCols = list_length(distinctList);
	int			keyno = 0;
	AttrNumber *dupColIdx;
	Oid		   *dupOperators;
	ListCell   *slitem;

	copy_plan_costsize(plan, lefttree);
	plan->plan_rows = outputRows;

	/*
	 * Charge one cpu_operator_cost per comparison per input tuple. We assume
	 * all columns get compared at most of the tuples.
	 */
	plan->total_cost += cpu_operator_cost * lefttree->plan_rows * numCols;

	plan->targetlist = cdbpullup_targetlist(lefttree,
				 cdbpullup_exprHasSubplanRef((Expr *) lefttree->targetlist));
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	/*
	 * convert SortGroupClause list into arrays of attr indexes and equality
	 * operators, as wanted by executor
	 */
	Assert(numCols > 0);
	dupColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numCols);
	dupOperators = (Oid *) palloc(sizeof(Oid) * numCols);

	foreach(slitem, distinctList)
	{
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(slitem);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, plan->targetlist);

		dupColIdx[keyno] = tle->resno;
		dupOperators[keyno] = sortcl->eqop;
		Assert(OidIsValid(dupOperators[keyno]));
		keyno++;
	}

	node->cmd = cmd;
	node->strategy = strategy;
	node->numCols = numCols;
	node->dupColIdx = dupColIdx;
	node->dupOperators = dupOperators;
	node->flagColIdx = flagColIdx;
	node->firstFlag = firstFlag;
	node->numGroups = numGroups;

	return node;
}

/*
 * make_lockrows
 *	  Build a LockRows plan node
 */
LockRows *
make_lockrows(Plan *lefttree, List *rowMarks, int epqParam)
{
	LockRows   *node = makeNode(LockRows);
	Plan	   *plan = &node->plan;

	copy_plan_costsize(plan, lefttree);

	/* charge cpu_tuple_cost to reflect locking costs (underestimate?) */
	plan->total_cost += cpu_tuple_cost * plan->plan_rows;

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	node->rowMarks = rowMarks;
	node->epqParam = epqParam;

	return node;
}

/*
 * Note: offset_est and count_est are passed in to save having to repeat
 * work already done to estimate the values of the limitOffset and limitCount
 * expressions.  Their values are as returned by preprocess_limit (0 means
 * "not relevant", -1 means "couldn't estimate").  Keep the code below in sync
 * with that function!
 */
Limit *
make_limit(Plan *lefttree, Node *limitOffset, Node *limitCount,
		   int64 offset_est, int64 count_est)
{
	Limit	   *node = makeNode(Limit);
	Plan	   *plan = &node->plan;

	copy_plan_costsize(plan, lefttree);

	/*
	 * Adjust the output rows count and costs according to the offset/limit.
	 * This is only a cosmetic issue if we are at top level, but if we are
	 * building a subquery then it's important to report correct info to the
	 * outer planner.
	 *
	 * When the offset or count couldn't be estimated, use 10% of the
	 * estimated number of rows emitted from the subplan.
	 */
	if (offset_est != 0)
	{
		double		offset_rows;

		if (offset_est > 0)
			offset_rows = (double) offset_est;
		else
			offset_rows = clamp_row_est(lefttree->plan_rows * 0.10);
		if (offset_rows > plan->plan_rows)
			offset_rows = plan->plan_rows;
		if (plan->plan_rows > 0)
			plan->startup_cost +=
				(plan->total_cost - plan->startup_cost)
				* offset_rows / plan->plan_rows;
		plan->plan_rows -= offset_rows;
		if (plan->plan_rows < 1)
			plan->plan_rows = 1;
	}

	if (count_est != 0)
	{
		double		count_rows;

		if (count_est > 0)
			count_rows = (double) count_est;
		else
			count_rows = clamp_row_est(lefttree->plan_rows * 0.10);
		if (count_rows > plan->plan_rows)
			count_rows = plan->plan_rows;
		if (plan->plan_rows > 0)
			plan->total_cost = plan->startup_cost +
				(plan->total_cost - plan->startup_cost)
				* count_rows / plan->plan_rows;
		plan->plan_rows = count_rows;
		if (plan->plan_rows < 1)
			plan->plan_rows = 1;
	}

	plan->targetlist = cdbpullup_targetlist(lefttree,
				 cdbpullup_exprHasSubplanRef((Expr *) lefttree->targetlist));
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	node->limitOffset = limitOffset;
	node->limitCount = limitCount;

	return node;
}

/*
 * make_result
 *	  Build a Result plan node
 *
 * If we have a subplan, assume that any evaluation costs for the gating qual
 * were already factored into the subplan's startup cost, and just copy the
 * subplan cost.  If there's no subplan, we should include the qual eval
 * cost.  In either case, tlist eval cost is not to be included here.
 */
Result *
make_result(PlannerInfo *root,
			List *tlist,
			Node *resconstantqual,
			Plan *subplan)
{
	Result	   *node = makeNode(Result);
	Plan	   *plan = &node->plan;

	if (subplan)
		copy_plan_costsize(plan, subplan);
	else
	{
		plan->startup_cost = 0;
		plan->total_cost = cpu_tuple_cost;
		plan->plan_rows = 1;	/* wrong if we have a set-valued function? */
		plan->plan_width = 0;	/* XXX is it worth being smarter? */
		if (resconstantqual)
		{
			QualCost	qual_cost;

			cost_qual_eval(&qual_cost, (List *) resconstantqual, root);
			/* resconstantqual is evaluated once at startup */
			plan->startup_cost += qual_cost.startup + qual_cost.per_tuple;
			plan->total_cost += qual_cost.startup + qual_cost.per_tuple;
		}
	}

	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = subplan;
	plan->righttree = NULL;
	node->resconstantqual = resconstantqual;

	node->numHashFilterCols = 0;
	node->hashFilterColIdx = NULL;
	node->hashFilterFuncs = NULL;

	return node;
}

/*
 * make_repeat
 *	  Build a Repeat plan node
 */
Repeat *
make_repeat(List *tlist,
			List *qual,
			Expr *repeatCountExpr,
			uint64 grouping,
			Plan *subplan)
{
	Repeat	   *node = makeNode(Repeat);
	Plan	   *plan = &node->plan;

	Assert(subplan != NULL);
	copy_plan_costsize(plan, subplan);

	plan->targetlist = tlist;
	plan->qual = qual;
	plan->lefttree = subplan;
	plan->righttree = NULL;

	node->repeatCountExpr = repeatCountExpr;
	node->grouping = grouping;

	return node;
}

/*
 * make_modifytable
 *	  Build a ModifyTable plan node
 *
 * Currently, we don't charge anything extra for the actual table modification
 * work, nor for the WITH CHECK OPTIONS or RETURNING expressions if any.  It
 * would only be window dressing, since these are always top-level nodes and
 * there is no way for the costs to change any higher-level planning choices.
 * But we might want to make it look better sometime.
 */
ModifyTable *
make_modifytable(PlannerInfo *root,
				 CmdType operation, bool canSetTag,
				 List *resultRelations, List *subplans,
				 List *withCheckOptionLists, List *returningLists,
				 List *is_split_updates,
				 List *rowMarks, int epqParam)
{
	ModifyTable *node = makeNode(ModifyTable);
	Plan       *plan = &node->plan;
	double      total_size;
	List       *fdw_private_list;
	ListCell   *subnode;
	ListCell   *lc;
	int         i;

	Assert(list_length(resultRelations) == list_length(subplans));
	Assert(withCheckOptionLists == NIL ||
		   list_length(resultRelations) == list_length(withCheckOptionLists));
	Assert(returningLists == NIL ||
		   list_length(resultRelations) == list_length(returningLists));

	node->plan.lefttree = NULL;
	node->plan.righttree = NULL;
	node->plan.qual = NIL;
	/* setrefs.c will fill in the targetlist, if needed */
	node->plan.targetlist = NIL;

	node->operation = operation;
	node->canSetTag = canSetTag;
	node->resultRelations = resultRelations;
	node->resultRelIndex = -1;  /* will be set correctly in setrefs.c */
	node->plans = subplans;
	node->withCheckOptionLists = withCheckOptionLists;
	node->returningLists = returningLists;
	node->rowMarks = rowMarks;
	node->epqParam = epqParam;
	node->action_col_idxes = NIL;
	node->ctid_col_idxes = NIL;
	node->oid_col_idxes = NIL;

	adjust_modifytable_flow(root, node, is_split_updates);

	/*
	 * Compute cost as sum of subplan costs.
	 */
	plan->startup_cost = 0;
	plan->total_cost = 0;
	plan->plan_rows = 0;
	total_size = 0;
	foreach(subnode, subplans)
	{
		Plan	   *subplan = (Plan *) lfirst(subnode);

		if (subnode == list_head(subplans))		/* first node? */
			plan->startup_cost = subplan->startup_cost;
		plan->total_cost += subplan->total_cost;
		plan->plan_rows += subplan->plan_rows;
		total_size += subplan->plan_width * subplan->plan_rows;
	}
	if (plan->plan_rows > 0)
		plan->plan_width = rint(total_size / plan->plan_rows);
	else
		plan->plan_width = 0;

	/*
     * For each result relation that is a foreign table, allow the FDW to
     * construct private plan data, and accumulate it all into a list.
     */
	fdw_private_list = NIL;
	i = 0;
	foreach(lc, resultRelations)
	{
		Index       rti = lfirst_int(lc);
		FdwRoutine *fdwroutine;
		List       *fdw_private;

		/*
		 * If possible, we want to get the FdwRoutine from our RelOptInfo for
		 * the table.  But sometimes we don't have a RelOptInfo and must get
		 * it the hard way.  (In INSERT, the target relation is not scanned,
		 * so it's not a baserel; and there are also corner cases for
		 * updatable views where the target rel isn't a baserel.)
		 */
		if (rti < root->simple_rel_array_size &&
			root->simple_rel_array[rti] != NULL)
		{
			RelOptInfo *resultRel = root->simple_rel_array[rti];

			fdwroutine = resultRel->fdwroutine;
		}
		else
		{
			RangeTblEntry *rte = planner_rt_fetch(rti, root);

			Assert(rte->rtekind == RTE_RELATION);
			if (rte->relkind == RELKIND_FOREIGN_TABLE)
				fdwroutine = GetFdwRoutineByRelId(rte->relid);
			else
				fdwroutine = NULL;
		}

		if (fdwroutine != NULL &&
			fdwroutine->PlanForeignModify != NULL)
			fdw_private = fdwroutine->PlanForeignModify(root, node, rti, i);
		else
			fdw_private = NIL;
		fdw_private_list = lappend(fdw_private_list, fdw_private);
		i++;
	}
	node->fdwPrivLists = fdw_private_list;

	return node;
}

/*
 * Set the Flow in a ModifyTable and its children correctly.
 *
 * The input to a ModifyTable node must be distributed according to the
 * DISTRIBUTED BY of the target table. Adjust the Flows of the child
 * plans for that. Also set the Flow of the ModifyTable node itself.
 */
static void
adjust_modifytable_flow(PlannerInfo *root, ModifyTable *node, List *is_split_updates)
{
	/*
	 * The input plans must be distributed correctly.
	 */
	ListCell   *lcr,
			   *lcp,
			   *lci;
	bool		all_subplans_entry = true,
				all_subplans_replicated = true;
	int			numsegments = -1;

	if (node->operation == CMD_INSERT)
	{
		forboth(lcr, node->resultRelations, lcp, node->plans)
		{
			int			rti = lfirst_int(lcr);
			Plan	   *subplan = (Plan *) lfirst(lcp);
			RangeTblEntry *rte = rt_fetch(rti, root->parse->rtable);
			List	   *hashExprs = NIL;
			List	   *hashOpfamilies = NIL;
			GpPolicy   *targetPolicy;
			GpPolicyType targetPolicyType;

			Assert(rte->rtekind == RTE_RELATION);

			targetPolicy = GpPolicyFetch(rte->relid);
			targetPolicyType = targetPolicy->ptype;

			numsegments = Max(targetPolicy->numsegments, numsegments);

			if (targetPolicyType == POLICYTYPE_PARTITIONED)
			{
				all_subplans_entry = false;
				all_subplans_replicated = false;

				/*
				 * A query to reach here: INSERT INTO t1 VALUES(1).
				 * There is no need to add a motion from General, we could
				 * simply put General on the same segments with target table.
				 */
				/* FIXME: also do this for other targetPolicyType? */
				/* FIXME: also do this for all the subplans */
				if (subplan->flow->locustype == CdbLocusType_General)
				{
					subplan->flow->numsegments = targetPolicy->numsegments;
				}

				if (gp_enable_fast_sri && IsA(subplan, Result))
					sri_optimize_for_result(root, subplan, rte,
											&targetPolicy, &hashExprs, &hashOpfamilies);
				if (!hashExprs)
				{
					hashExprs = getExprListFromTargetList(subplan->targetlist,
														  targetPolicy->nattrs,
														  targetPolicy->attrs,
														  false);
					hashOpfamilies = NIL;
					for (int i = 0; i < targetPolicy->nattrs; i++)
					{
						hashOpfamilies = lappend_oid(hashOpfamilies,
													 get_opclass_family(targetPolicy->opclasses[i]));
					}
				}
				Relation rel = relation_open(rte->relid, NoLock);
				if (enable_range_distribution && RelationIsRocksDB(rel))
                {
                    if (!rangePlan(subplan, false, false, hashExprs, hashOpfamilies, numsegments))
					{   
                        ereport(ERROR, (errcode(ERRCODE_GP_FEATURE_NOT_YET),
									errmsg("Cannot parallelize that INSERT yet")));
                    }
                }
                else
                {
				    if (!repartitionPlan(subplan, false, false, hashExprs, hashOpfamilies, numsegments))
					    ereport(ERROR,
							(errcode(ERRCODE_GP_FEATURE_NOT_YET),
							 errmsg("cannot parallelize that INSERT yet")));
                }

                relation_close(rel, NoLock);
			}
			else if (targetPolicyType == POLICYTYPE_ENTRY)
			{
				/* Master-only table */

				all_subplans_replicated = false;

				/* All's well if query result is already on the QD. */
				if (!(subplan->flow->flotype == FLOW_SINGLETON &&
					  subplan->flow->segindex < 0))
				{
					/*
					 * Query result needs to be brought back to the QD.
					 * Ask for motion to a single QE.  Later, apply_motion
					 * will override that to bring it to the QD instead.
					 */
					if (!focusPlan(subplan, false, false))
						ereport(ERROR,
								(errcode(ERRCODE_GP_FEATURE_NOT_YET),
								 errmsg("cannot parallelize that INSERT yet")));
				}
			}
			else if (targetPolicyType == POLICYTYPE_REPLICATED)
			{
				Assert(subplan->flow->flotype != FLOW_REPLICATED);

				all_subplans_entry = false;

				/*
				 * CdbLocusType_SegmentGeneral is only used by replicated table
				 * right now, so if both input and target are replicated table,
				 * no need to add a motion.
				 *
				 * Also, to expand a replicated table to new segments, gpexpand
				 * force a data reorganization by a query like:
				 * CREATE TABLE tmp_tab AS SELECT * FROM source_table DISTRIBUTED REPLICATED
				 * Obviously, tmp_tab in new segments can't get data if we don't
				 * add a broadcast here. 
				 */
				if (optimizer_replicated_table_insert &&
					subplan->flow->flotype == FLOW_SINGLETON &&
					subplan->flow->locustype == CdbLocusType_SegmentGeneral &&
					!contain_volatile_functions((Node *)subplan->targetlist))
				{
					if (subplan->flow->numsegments >= targetPolicy->numsegments)
					{
						/*
						 * A query to reach here:
						 *     INSERT INTO d1 SELECT * FROM d1;
						 * There is no need to add a motion from General, we
						 * could simply put General on the same segments with
						 * target table.
						 */
						subplan->flow->numsegments = targetPolicy->numsegments;
						continue;
					}

					/*
					 * Otherwise a broadcast motion is needed otherwise d2 will
					 * only have data on segment 0.
					 *
					 * A query to reach here:
					 *     INSERT INTO d2 SELECT * FROM d1;
					 */
				}

				/* plan's data are available on all segment, no motion needed */
				if (optimizer_replicated_table_insert &&
					subplan->flow->flotype == FLOW_SINGLETON &&
					subplan->flow->locustype == CdbLocusType_General &&
					!contain_volatile_functions((Node *)subplan->targetlist))
				{
					subplan->dispatch = DISPATCH_PARALLEL;
					if (subplan->flow->numsegments >= targetPolicy->numsegments)
					{
						/*
						 * A query to reach here: INSERT INTO d1 VALUES(1).
						 * There is no need to add a motion from General, we
						 * could simply put General on the same segments with
						 * target table.
						 */
						subplan->flow->numsegments = targetPolicy->numsegments;
					}
					else
					{
						/* FIXME: is here reachable? */
					}
					continue;
				}

				if (!broadcastPlan(subplan, false, false, targetPolicy->numsegments))
					ereport(ERROR,
							(errcode(ERRCODE_GP_FEATURE_NOT_YET),
							 errmsg("cannot parallelize that INSERT yet")));

			}
			else
				elog(ERROR, "unrecognized policy type %u", targetPolicyType);
		}
	}
	else if (node->operation == CMD_UPDATE || node->operation == CMD_DELETE)
	{
		forthree(lcr, node->resultRelations, lcp, node->plans, lci, is_split_updates)
		{
			int			rti = lfirst_int(lcr);
			Plan	   *subplan = (Plan *) lfirst(lcp);
			bool		is_split_update = lfirst_int(lci) ? true : false;
			RangeTblEntry *rte = rt_fetch(rti, root->parse->rtable);
			GpPolicy   *targetPolicy;
			GpPolicyType targetPolicyType;

			Assert(rti > 0);
			Assert(rte->rtekind == RTE_RELATION);

			targetPolicy = GpPolicyFetch(rte->relid);
			targetPolicyType = targetPolicy->ptype;

			numsegments = Max(targetPolicy->numsegments, numsegments);

			if (targetPolicyType == POLICYTYPE_PARTITIONED)
			{
				all_subplans_entry = false;
				all_subplans_replicated = false;

				/*
				 * If any of the distribution key columns are being changed,
				 * the UPDATE might move tuples from one segment to another.
				 * Create a Split Update node to deal with that.
				 *
				 * If the input is a dummy plan that cannot return any rows,
				 * e.g. because the input was eliminated by constraint
				 * exclusion, we can skip it.
				 */
				if (is_split_update && !is_dummy_plan(subplan))
				{
					List	   *hashExprs;
					List	   *hashOpfamilies;
					int			i;
					Plan	*new_subplan;

					Assert(node->operation == CMD_UPDATE);

					if (Gp_role == GP_ROLE_UTILITY)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot update distribution key columns in utility mode")));
					}

					new_subplan = (Plan *) make_splitupdate(root, (ModifyTable *) node, subplan, rte);

					hashExprs = getExprListFromTargetList(new_subplan->targetlist,
														  targetPolicy->nattrs,
														  targetPolicy->attrs,
														  false);
					hashOpfamilies = NIL;
					for (i = 0; i < targetPolicy->nattrs; i++)
					{
						hashOpfamilies = lappend_oid(hashOpfamilies,
													 get_opclass_family(targetPolicy->opclasses[i]));
					}
					if (!repartitionPlan(new_subplan, false, false,
										 hashExprs, hashOpfamilies,
										 targetPolicy->numsegments))
						ereport(ERROR, (errcode(ERRCODE_GP_FEATURE_NOT_YET),
										errmsg("Cannot parallelize that UPDATE yet")));

					lcp->data.ptr_value = new_subplan;
				}
				else
				{
					node->action_col_idxes = lappend_int(node->action_col_idxes, -1);
					node->ctid_col_idxes = lappend_int(node->ctid_col_idxes, -1);
					node->oid_col_idxes = lappend_int(node->oid_col_idxes, 0);
					request_explicit_motion(subplan, rti, root->glob->finalrtable);
				}
			}
			else if (targetPolicyType == POLICYTYPE_ENTRY)
			{
				all_subplans_replicated = false;

				/* Master-only table */
				if (subplan->flow->flotype == FLOW_PARTITIONED ||
					subplan->flow->flotype == FLOW_REPLICATED ||
					(subplan->flow->flotype == FLOW_SINGLETON && subplan->flow->segindex != -1))
				{
					/*
					 * target table is master-only but flow is
					 * distributed: add a GatherMotion on top
					 */

					/* create a shallow copy of the plan flow */
					Flow	   *flow = subplan->flow;

					subplan->flow = (Flow *) palloc(sizeof(Flow));
					*(subplan->flow) = *flow;

					/* save original flow information */
					subplan->flow->flow_before_req_move = flow;

					/* request a GatherMotion node */
					subplan->flow->req_move = MOVEMENT_FOCUS;
					subplan->flow->hashExprs = NIL;
					subplan->flow->hashOpfamilies = NIL;
					subplan->flow->segindex = 0;
				}
				else
				{
					/*
					 * Source is, presumably, a dispatcher singleton.
					 */
					subplan->flow->req_move = MOVEMENT_NONE;
				}
				node->action_col_idxes = lappend_int(node->action_col_idxes, -1);
				node->ctid_col_idxes = lappend_int(node->ctid_col_idxes, -1);
				node->oid_col_idxes = lappend_int(node->oid_col_idxes, 0);
			}
			else if (targetPolicyType == POLICYTYPE_REPLICATED)
			{
				node->action_col_idxes = lappend_int(node->action_col_idxes, -1);
				node->ctid_col_idxes = lappend_int(node->ctid_col_idxes, -1);
				node->oid_col_idxes = lappend_int(node->oid_col_idxes, 0);
				all_subplans_entry = false;
			}
			else
				elog(ERROR, "unrecognized policy type %u", targetPolicyType);
		}
	}

	Assert(numsegments >= 0);

	/*
	 * Set the distribution of the ModifyTable node itself. If there is only
	 * one subplan, or all the subplans have a compatible distribution, then
	 * we could mark the ModifyTable with the same distribution key. However,
	 * currently, because a ModifyTable node can only be at the top of the
	 * plan, it won't make any difference to the overall plan.
	 *
	 * GPDB_90_MERGE_FIXME: I've hacked a basic implementation of the above for
	 * the case where all the subplans are POLICYTYPE_ENTRY, but it seems like
	 * there should be a more general way to do this.
	 */
	if (all_subplans_entry)
	{
		mark_plan_entry((Plan *) node);
		((Plan *) node)->flow->numsegments = numsegments;
	}
	else if (all_subplans_replicated)
	{
		mark_plan_replicated((Plan *) node, numsegments);
	}
	else
	{
		mark_plan_strewn((Plan *) node, numsegments);

		if (list_length(node->plans) == 1)
		{
			node->plan.directDispatch = ((Plan *) linitial(node->plans))->directDispatch;
		}
	}
}

/*
 * is_projection_capable_plan
 *		Check whether a given Plan node is able to do projection.
 */
bool
is_projection_capable_plan(Plan *plan)
{
	/* Most plan types can project, so just list the ones that can't */
	switch (nodeTag(plan))
	{
		case T_Hash:
		case T_Material:
		case T_Sort:
		case T_Unique:
		case T_SetOp:
		case T_LockRows:
		case T_Limit:
		case T_ModifyTable:
		case T_Append:
		case T_MergeAppend:
		case T_RecursiveUnion:
		case T_Motion:
		case T_ShareInputScan:
			return false;
		default:
			break;
	}
	return true;
}


/*
 * plan_pushdown_tlist
 *
 * If the given Plan node does projection, the same node is returned after
 * replacing its targetlist with the given targetlist.
 *
 * Otherwise, returns a Result node with the given targetlist, inserted atop
 * the given plan.
 */
Plan *
plan_pushdown_tlist(PlannerInfo *root, Plan *plan, List *tlist)
{
	bool		need_result;

	if (!is_projection_capable_plan(plan) &&
		!tlist_same_exprs(tlist, plan->targetlist))
	{
		need_result = true;
	}
	else
		need_result = false;

	if (!need_result)
	{
		/* Fix up annotation of plan's distribution and ordering properties. */
		if (plan->flow)
			plan->flow = pull_up_Flow((Plan *) make_result(root, tlist, NULL, plan),
									  plan);

		/* Install the new targetlist. */
		plan->targetlist = tlist;
	}
	else
	{
		Plan	   *subplan = plan;

		/* Insert a Result node to evaluate the targetlist. */
		plan = (Plan *) make_result(root, tlist, NULL, subplan);

		/* Propagate the subplan's distribution. */
		if (subplan->flow)
			plan->flow = pull_up_Flow(plan, subplan);
	}
	return plan;
}	/* plan_pushdown_tlist */

/*
 * Return true if there is the same tleSortGroupRef in an entry in glist
 * as the tleSortGroupRef in gc.
 */
static bool
groupcol_in_list(SortGroupClause *gc, List *glist)
{
	bool		found = false;
	ListCell   *lc;

	foreach(lc, glist)
	{
		SortGroupClause *entry = (SortGroupClause *) lfirst(lc);

		Assert(IsA(entry, SortGroupClause));
		if (gc->tleSortGroupRef == entry->tleSortGroupRef)
		{
			found = true;
			break;
		}
	}
	return found;
}


/*
 * Construct a list of GroupClauses from the transformed GROUP BY clause.
 * This list of GroupClauses has unique tleSortGroupRefs.
 */
static List *
flatten_grouping_list(List *groupcls)
{
	List	   *result = NIL;
	ListCell   *gc;

	foreach(gc, groupcls)
	{
		Node	   *node = (Node *) lfirst(gc);

		if (node == NULL)
			continue;

		Assert(IsA(node, GroupingClause) ||
			   IsA(node, SortGroupClause) ||
			   IsA(node, List));

		if (IsA(node, GroupingClause))
		{
			List	   *groupsets = ((GroupingClause *) node)->groupsets;
			List	   *flatten_groupsets =
			flatten_grouping_list(groupsets);
			ListCell   *lc;

			foreach(lc, flatten_groupsets)
			{
				SortGroupClause *flatten_gc = (SortGroupClause *) lfirst(lc);

				Assert(IsA(flatten_gc, SortGroupClause));

				if (!groupcol_in_list(flatten_gc, result))
					result = lappend(result, flatten_gc);
			}

		}
		else if (IsA(node, List))
		{
			List	   *flatten_groupsets =
			flatten_grouping_list((List *) node);
			ListCell   *lc;

			foreach(lc, flatten_groupsets)
			{
				SortGroupClause *flatten_gc = (SortGroupClause *) lfirst(lc);

				Assert(IsA(flatten_gc, SortGroupClause));

				if (!groupcol_in_list(flatten_gc, result))
					result = lappend(result, flatten_gc);
			}
		}
		else
		{
			Assert(IsA(node, SortGroupClause));

			if (!groupcol_in_list((SortGroupClause *) node, result))
				result = lappend(result, node);
		}
	}

	return result;
}

/*
 * add_sort_column --- utility subroutine for building sort info arrays
 *
 * We need this routine because the same column might be selected more than
 * once as a sort key column; if so, the extra mentions are redundant.
 *
 * Caller is assumed to have allocated the arrays large enough for the
 * max possible number of columns.	Return value is the new column count.
 */
static int
add_sort_column(AttrNumber colIdx, Oid sortOp, Oid coll, bool nulls_first,
				int numCols, AttrNumber *sortColIdx,
				Oid *sortOperators, Oid *collations, bool *nullsFirst)
{
	int			i;

	Assert(OidIsValid(sortOp));

	for (i = 0; i < numCols; i++)
	{
		/*
		 * Note: we check sortOp because it's conceivable that "ORDER BY foo
		 * USING <, foo USING <<<" is not redundant, if <<< distinguishes
		 * values that < considers equal.  We need not check nulls_first
		 * however because a lower-order column with the same sortop but
		 * opposite nulls direction is redundant.
		 *
		 * We could probably consider sort keys with the same sortop and
		 * different collations to be redundant too, but for the moment treat
		 * them as not redundant.  This will be needed if we ever support
		 * collations with different notions of equality.
		 */
		if (sortColIdx[i] == colIdx &&
			sortOperators[numCols] == sortOp &&
			collations[numCols] == coll)
		{
			/* Already sorting by this col, so extra sort key is useless */
			return numCols;
		}
	}

	/* Add the column */
	sortColIdx[numCols] = colIdx;
	sortOperators[numCols] = sortOp;
	collations[numCols] = coll;
	nullsFirst[numCols] = nulls_first;
	return numCols + 1;
}




/*
 * cdbpathtoplan_create_motion_plan
 */
static Motion *
cdbpathtoplan_create_motion_plan(PlannerInfo *root,
								 CdbMotionPath *path,
								 Plan *subplan)
{
	Motion	   *motion = NULL;
	Path	   *subpath = path->subpath;
	int			numsegments;

	numsegments = CdbPathLocus_NumSegments(path->path.locus);

	/* Send all tuples to a single process? */
	if (CdbPathLocus_IsBottleneck(path->path.locus))
	{
		if (path->path.pathkeys)
		{
			Plan	   *prep;
			int			numSortCols;
			AttrNumber *sortColIdx;
			Oid		   *sortOperators;
			Oid		   *collations;
			bool		*nullsFirst;

			/*
			 * Build sort key info to define our Merge Receive keys.
			 */
			prep = prepare_sort_from_pathkeys(root,
											  subplan,
											  path->path.pathkeys,
											  subpath->parent->relids,
											  NULL,
											  false,
											  &numSortCols,
											  &sortColIdx,
											  &sortOperators,
											  &collations,
											  &nullsFirst,
											  true /* add_keys_to_targetlist */);

			if (prep)
			{
				/*
				 * Create a Merge Receive to preserve ordering.
				 *
				 * prepare_sort_from_pathkeys() might return a Result node, if
				 * one would needs to be inserted above the Sort. We don't
				 * create an actual Sort node here, the input is already
				 * ordered, but use the Result node, if any, as the input to
				 * the Motion node. (I'm not sure if that is possible with
				 * Gather Motion nodes. Since the input is already ordered,
				 * presumably the target list already contains the expressions
				 * for the key columns. But better safe than sorry.)
				 */
				subplan = prep;
				motion = make_sorted_union_motion(root, subplan, numSortCols, sortColIdx, sortOperators, collations,
												  nullsFirst, false, numsegments);
			}
			else
			{
				/* Degenerate ordering... build unordered Union Receive */
				motion = make_union_motion(subplan, false, numsegments);
			}
		}

		/* Unordered Union Receive */
		else
			motion = make_union_motion(subplan, false, numsegments);
	}

	/* Send all of the tuples to all of the QEs in gang above... */
	else if (CdbPathLocus_IsReplicated(path->path.locus))
		motion = make_broadcast_motion(subplan,
									   false	/* useExecutorVarFormat */,
									   numsegments
			);

	/* Hashed redistribution to all QEs in gang above... */
	else if (CdbPathLocus_IsHashed(path->path.locus) ||
			 CdbPathLocus_IsHashedOJ(path->path.locus))
	{
		List	   *hashExprs;
		List	   *hashOpfamilies;

		cdbpathlocus_get_distkey_exprs(path->path.locus,
									   path->path.parent->relids,
									   subplan->targetlist,
									   &hashExprs, &hashOpfamilies);
		if (!hashExprs)
			elog(ERROR, "could not find hash distribution key expressions in target list");

		/**
         * If there are subplans in the hashExpr, push it down to lower level.
         */
		if (contain_subplans((Node *) hashExprs))
		{
			/* make a Result node to do the projection if necessary */
			if (!is_projection_capable_plan(subplan))
			{
				List	   *tlist = copyObject(subplan->targetlist);

				subplan = (Plan *) make_result(root, tlist, NULL, subplan);
			}
			subplan->targetlist = add_to_flat_tlist_junk(subplan->targetlist,
														 hashExprs,
														 true /* resjunk */);
        }
        motion = make_hashed_motion(subplan,
									hashExprs,
									hashOpfamilies,
                                    false /* useExecutorVarFormat */,
									numsegments);
    }
    else
        Insist(0);

    /*
     * Decorate the subplan with a Flow node telling the plan slicer
     * what kind of gang will be needed to execute the subplan.
     */
    subplan->flow = cdbpathtoplan_create_flow(root,
                                              subpath->locus,
                                              subpath->parent
                                                ? subpath->parent->relids
                                                : NULL,
                                              subplan);

	return motion;
}								/* cdbpathtoplan_create_motion_plan */
