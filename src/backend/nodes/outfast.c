/*-------------------------------------------------------------------------
 *
 * outfast.c
 *	  Fast serialization functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * NOTES
 *	  Every node type that can appear in an Greenplum Database serialized query or plan
 *    tree must have an output function defined here.
 *
 * 	  There *MUST* be a one-to-one correspondence between this routine
 *    and readfast.c.  If not, you will likely crash the system.
 *
 *     By design, the only user of these routines is the function
 *     serializeNode in cdbsrlz.c.  Other callers beware.
 *
 *    Like readfast.c, this file borrows the definitions of most functions
 *    from outfuncs.c.
 *
 * 	  Rather than serialize to a (somewhat human-readable) string, these
 *    routines create a binary serialization via a simple depth-first walk
 *    of the tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "utils/datum.h"
#include "catalog/heap.h"
#include "cdb/cdbgang.h"
#include "utils/workfile_mgr.h"
#include "parser/parsetree.h"


/*
 * Macros to simplify output of different kinds of fields.	Use these
 * wherever possible to reduce the chance for silly typos.	Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/*
 * Write the label for the node type.  nodelabel is accepted for
 * compatibility with outfuncs.c, but is ignored
 */
#define WRITE_NODE_TYPE(nodelabel) \
	{ int16 nt =nodeTag(node); appendBinaryStringInfo(str, (const char *)&nt, sizeof(int16)); }

/* Write an integer field  */
#define WRITE_INT_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int)); }

/* Write an integer field  */
#define WRITE_INT8_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int8)); }

/* Write an integer field  */
#define WRITE_INT16_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int16)); }

/* Write an unsigned integer field */
#define WRITE_UINT_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int))

/* Write an uint64 field */
#define WRITE_UINT64_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(uint64))

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(Oid))

/* Write a long-integer field */
#define WRITE_LONG_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(long))

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) \
	appendBinaryStringInfo(str, &node->fldname, 1)

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) \
	{ int16 en=node->fldname; appendBinaryStringInfo(str, (const char *)&en, sizeof(int16)); }

/* Write a float field --- the format is accepted but ignored (for compat with outfuncs.c)  */
#define WRITE_FLOAT_FIELD(fldname,format) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(double))

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
	{ \
		char b = node->fldname ? 1 : 0; \
		appendBinaryStringInfo(str, (const char *)&b, 1); }

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname) \
	{ int slen = node->fldname != NULL ? strlen(node->fldname) : 0; \
		appendBinaryStringInfo(str, (const char *)&slen, sizeof(int)); \
		if (slen>0) appendBinaryStringInfo(str, node->fldname, strlen(node->fldname));}

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int)); }

/* Write a Node field */
#define WRITE_NODE_FIELD(fldname) \
	(_outNode(str, node->fldname))

/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
	 _outBitmapset(str, node->fldname)

/* Write a binary field */
#define WRITE_BINARY_FIELD(fldname, sz) \
{ appendBinaryStringInfo(str, (const char *) &node->fldname, (sz)); }

/* Write a bytea field */
#define WRITE_BYTEA_FIELD(fldname) \
	(_outDatum(str, PointerGetDatum(node->fldname), -1, false))

/* Write a dummy field -- value not displayable or copyable */
#define WRITE_DUMMY_FIELD(fldname) \
	{ /*int * dummy = 0; appendBinaryStringInfo(str,(const char *)&dummy, sizeof(int *)) ;*/ }

	/* Read an integer array */
#define WRITE_INT_ARRAY(fldname, count, Type) \
	if ( count > 0 ) \
	{ \
		int i; \
		for(i = 0; i < count; i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(Type)); \
		} \
	}

/* Write a boolean array  */
#define WRITE_BOOL_ARRAY(fldname, count) \
	if ( count > 0 ) \
	{ \
		int i; \
		for(i = 0; i < count; i++) \
		{ \
			char b = node->fldname[i] ? 1 : 0;								\
			appendBinaryStringInfo(str, (const char *)&b, 1); \
		} \
	}

/* Write an Trasnaction ID array  */
#define WRITE_XID_ARRAY(fldname, count) \
	if ( count > 0 ) \
	{ \
		int i; \
		for(i = 0; i < count; i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(TransactionId)); \
		} \
	}

/* Write an Oid array  */
#define WRITE_OID_ARRAY(fldname, count) \
	if ( count > 0 ) \
	{ \
		int i; \
		for(i = 0; i < count; i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(Oid)); \
		} \
	}

static void _outNode(StringInfo str, void *obj);

static void
_outList(StringInfo str, List *node)
{
	ListCell   *lc;

	if (node == NULL)
	{
		int16 tg = 0;
		appendBinaryStringInfo(str, (const char *)&tg, sizeof(int16));
		return;
	}

	WRITE_NODE_TYPE("");
    WRITE_INT_FIELD(length);

	foreach(lc, node)
	{

		if (IsA(node, List))
		{
			_outNode(str, lfirst(lc));
		}
		else if (IsA(node, IntList))
		{
			int n = lfirst_int(lc);
			appendBinaryStringInfo(str, (const char *)&n, sizeof(int));
		}
		else if (IsA(node, OidList))
		{
			Oid n = lfirst_oid(lc);
			appendBinaryStringInfo(str, (const char *)&n, sizeof(Oid));
		}
	}
}

/*
 * _outBitmapset -
 *	   converts a bitmap set of integers
 *
 * Currently bitmapsets do not appear in any node type that is stored in
 * rules, so there is no support in readfast.c for reading this format.
 */
static void
_outBitmapset(StringInfo str, Bitmapset *bms)
{
	int i;
	int nwords = 0;
	if (bms) nwords = bms->nwords;
	appendBinaryStringInfo(str, (char *)&nwords, sizeof(int));
	for (i = 0; i < nwords; i++)
	{
		appendBinaryStringInfo(str, (char *)&bms->words[i], sizeof(bitmapword));
	}

}

/*
 * Print the value of a Datum given its type.
 */
static void
_outDatum(StringInfo str, Datum value, int typlen, bool typbyval)
{
	Size		length;
	char	   *s;

	if (typbyval)
	{
		s = (char *) (&value);
		appendBinaryStringInfo(str, s, sizeof(Datum));
	}
	else
	{
		s = (char *) DatumGetPointer(value);
		if (!PointerIsValid(s))
		{
			length = 0;
			appendBinaryStringInfo(str, (char *)&length, sizeof(Size));
		}
		else
		{
			length = datumGetSize(value, typbyval, typlen);
			appendBinaryStringInfo(str, (char *)&length, sizeof(Size));
			appendBinaryStringInfo(str, s, length);
		}
	}
}

#define COMPILING_BINARY_FUNCS
#include "outfuncs.c"

/*
 *	Stuff from plannodes.h
 */

/*
 * print the basic stuff of all nodes that inherit from Plan
 */
static void
_outPlanInfo(StringInfo str, const Plan *node)
{
	WRITE_INT_FIELD(plan_node_id);

	WRITE_FLOAT_FIELD(startup_cost, "%.2f");
	WRITE_FLOAT_FIELD(total_cost, "%.2f");
	WRITE_FLOAT_FIELD(plan_rows, "%.0f");
	WRITE_INT_FIELD(plan_width);

	WRITE_NODE_FIELD(targetlist);
	WRITE_NODE_FIELD(qual);

	WRITE_BITMAPSET_FIELD(extParam);
	WRITE_BITMAPSET_FIELD(allParam);

	WRITE_NODE_FIELD(flow);
	WRITE_INT_FIELD(dispatch);
	WRITE_BOOL_FIELD(directDispatch.isDirectDispatch);
	WRITE_NODE_FIELD(directDispatch.contentIds);
	WRITE_OID_FIELD(directDispatch.rangeid);

	WRITE_INT_FIELD(nMotionNodes);
	WRITE_INT_FIELD(nInitPlans);

	WRITE_NODE_FIELD(sliceTable);

    WRITE_NODE_FIELD(lefttree);
    WRITE_NODE_FIELD(righttree);
    WRITE_NODE_FIELD(initPlan);

	WRITE_UINT64_FIELD(operatorMemKB);
}

static void
_outResult(StringInfo str, const Result *node)
{
	WRITE_NODE_TYPE("RESULT");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(resconstantqual);

	WRITE_INT_FIELD(numHashFilterCols);
	WRITE_INT_ARRAY(hashFilterColIdx, node->numHashFilterCols, AttrNumber);
	WRITE_OID_ARRAY(hashFilterFuncs, node->numHashFilterCols);
}

static void
_outRecursiveUnion(StringInfo str, RecursiveUnion *node)
{
	WRITE_NODE_TYPE("RECURSIVEUNION");

	_outPlanInfo(str, (Plan *) node);

	WRITE_INT_FIELD(wtParam);
	WRITE_INT_FIELD(numCols);

	WRITE_INT_ARRAY(dupColIdx, node->numCols, AttrNumber);
	WRITE_OID_ARRAY(dupOperators, node->numCols);

	WRITE_LONG_FIELD(numGroups);
}

static void
_outPlannedStmt(StringInfo str, PlannedStmt *node)
{
	WRITE_NODE_TYPE("PLANNEDSTMT");

	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_ENUM_FIELD(planGen, PlanGenerator);
	WRITE_BOOL_FIELD(hasReturning);
	WRITE_BOOL_FIELD(hasModifyingCTE);
	WRITE_BOOL_FIELD(canSetTag);
	WRITE_BOOL_FIELD(transientPlan);
	WRITE_BOOL_FIELD(oneoffPlan);
	WRITE_BOOL_FIELD(simplyUpdatable);
	WRITE_NODE_FIELD(planTree);
	WRITE_NODE_FIELD(rtable);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(utilityStmt);
	WRITE_NODE_FIELD(subplans);
	WRITE_BITMAPSET_FIELD(rewindPlanIDs);

	WRITE_NODE_FIELD(result_partitions);
	WRITE_NODE_FIELD(result_aosegnos);
	WRITE_NODE_FIELD(queryPartOids);
	WRITE_NODE_FIELD(queryPartsMetadata);
	WRITE_NODE_FIELD(numSelectorsPerScanId);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(relationOids);
	/*
	 * Don't serialize invalItems. The TIDs of the invalidated items wouldn't
	 * make sense in segments.
	 */
	WRITE_INT_FIELD(nParamExec);
	WRITE_INT_FIELD(nMotionNodes);
	WRITE_INT_FIELD(nInitPlans);
	WRITE_NODE_FIELD(intoPolicy);
	WRITE_UINT64_FIELD(query_mem);
	WRITE_NODE_FIELD(intoClause);
	WRITE_NODE_FIELD(copyIntoClause);
	WRITE_INT8_FIELD(metricsQueryType);
}

static void
outLogicalIndexInfo(StringInfo str, const LogicalIndexInfo *node)
{
	WRITE_OID_FIELD(logicalIndexOid);
	WRITE_INT_FIELD(nColumns);
	WRITE_INT_ARRAY(indexKeys, node->nColumns, AttrNumber);
	WRITE_NODE_FIELD(indPred);
	WRITE_NODE_FIELD(indExprs);
	WRITE_BOOL_FIELD(indIsUnique);
	WRITE_ENUM_FIELD(indType, LogicalIndexType);
	WRITE_NODE_FIELD(partCons);
	WRITE_NODE_FIELD(defaultLevels);
}

static void
_outCopyStmt(StringInfo str, CopyStmt *node)
{
	WRITE_NODE_TYPE("COPYSTMT");
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(attlist);
	WRITE_BOOL_FIELD(is_from);
	WRITE_BOOL_FIELD(is_program);
	WRITE_BOOL_FIELD(skip_ext_partition);
	WRITE_STRING_FIELD(filename);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(sreh);
	WRITE_NODE_FIELD(partitions);
	WRITE_NODE_FIELD(ao_segnos);
}

static void
_outSubqueryScan(StringInfo str, SubqueryScan *node)
{
	WRITE_NODE_TYPE("SUBQUERYSCAN");

	_outScanInfo(str, (Scan *) node);

	WRITE_NODE_FIELD(subplan);
}

static void
_outMergeJoin(StringInfo str, MergeJoin *node)
{
	int			numCols;

	WRITE_NODE_TYPE("MERGEJOIN");

	_outJoinPlanInfo(str, (Join *) node);

	WRITE_NODE_FIELD(mergeclauses);

	numCols = list_length(node->mergeclauses);

	WRITE_OID_ARRAY(mergeFamilies, numCols);
	WRITE_OID_ARRAY(mergeCollations, numCols);
	WRITE_INT_ARRAY(mergeStrategies, numCols, int);
	WRITE_BOOL_ARRAY(mergeNullsFirst, numCols);

	WRITE_BOOL_FIELD(unique_outer);
}

static void
_outAgg(StringInfo str, Agg *node)
{

	WRITE_NODE_TYPE("AGG");

	_outPlanInfo(str, (Plan *) node);

	WRITE_ENUM_FIELD(aggstrategy, AggStrategy);
	WRITE_INT_FIELD(numCols);

	WRITE_INT_ARRAY(grpColIdx, node->numCols, AttrNumber);
	WRITE_OID_ARRAY(grpOperators, node->numCols);

	WRITE_LONG_FIELD(numGroups);
	WRITE_INT_FIELD(transSpace);

	WRITE_INT_FIELD(numNullCols);
	WRITE_UINT64_FIELD(inputGrouping);
	WRITE_UINT64_FIELD(grouping);
	WRITE_BOOL_FIELD(inputHasGrouping);
	WRITE_INT_FIELD(rollupGSTimes);
	WRITE_BOOL_FIELD(lastAgg);
	WRITE_BOOL_FIELD(streaming);
}

static void
_outWindowAgg(StringInfo str, WindowAgg *node)
{
	WRITE_NODE_TYPE("WINDOWAGG");

	_outPlanInfo(str, (Plan *) node);

	WRITE_UINT_FIELD(winref);
	WRITE_INT_FIELD(partNumCols);
	WRITE_INT_ARRAY(partColIdx, node->partNumCols, AttrNumber);
	WRITE_OID_ARRAY(partOperators, node->partNumCols);

	WRITE_INT_FIELD(ordNumCols);

	WRITE_INT_ARRAY(ordColIdx, node->ordNumCols, AttrNumber);
	WRITE_OID_ARRAY(ordOperators, node->ordNumCols);
	WRITE_INT_FIELD(firstOrderCol);
	WRITE_OID_FIELD(firstOrderCmpOperator);
	WRITE_BOOL_FIELD(firstOrderNullsFirst);
	WRITE_INT_FIELD(frameOptions);
	WRITE_NODE_FIELD(startOffset);
	WRITE_NODE_FIELD(endOffset);
}

static void
_outSort(StringInfo str, Sort *node)
{

	WRITE_NODE_TYPE("SORT");

	_outPlanInfo(str, (Plan *) node);

	WRITE_INT_FIELD(numCols);
	WRITE_INT_ARRAY(sortColIdx, node->numCols, AttrNumber);
	WRITE_OID_ARRAY(sortOperators, node->numCols);
	WRITE_OID_ARRAY(collations, node->numCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numCols);

    /* CDB */
    WRITE_BOOL_FIELD(noduplicates);

	WRITE_ENUM_FIELD(share_type, ShareType);
	WRITE_INT_FIELD(share_id);
	WRITE_INT_FIELD(driver_slice);
	WRITE_INT_FIELD(nsharer);
	WRITE_INT_FIELD(nsharer_xslice);
}

static void
_outMergeAppend(StringInfo str, MergeAppend *node)
{
	WRITE_NODE_TYPE("MERGEAPPEND");

	_outPlanInfo(str, (Plan *) node);

	WRITE_NODE_FIELD(mergeplans);

	WRITE_INT_FIELD(numCols);

	WRITE_INT_ARRAY(sortColIdx, node->numCols, AttrNumber);
	WRITE_OID_ARRAY(sortOperators, node->numCols);
	WRITE_OID_ARRAY(collations, node->numCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numCols);
}

static void
_outUnique(StringInfo str, Unique *node)
{

	WRITE_NODE_TYPE("UNIQUE");

	_outPlanInfo(str, (Plan *) node);

	WRITE_INT_FIELD(numCols);
	WRITE_INT_ARRAY(uniqColIdx, node->numCols, AttrNumber);
	WRITE_OID_ARRAY(uniqOperators, node->numCols);
}

static void
_outSetOp(StringInfo str, SetOp *node)
{

	WRITE_NODE_TYPE("SETOP");

	_outPlanInfo(str, (Plan *) node);

	WRITE_ENUM_FIELD(cmd, SetOpCmd);
	WRITE_ENUM_FIELD(strategy, SetOpStrategy);
	WRITE_INT_FIELD(numCols);
	WRITE_INT_ARRAY(dupColIdx, node->numCols, AttrNumber);
	WRITE_OID_ARRAY(dupOperators, node->numCols);

	WRITE_INT_FIELD(flagColIdx);
	WRITE_INT_FIELD(firstFlag);
	WRITE_LONG_FIELD(numGroups);
}

static void
_outMotion(StringInfo str, Motion *node)
{
	WRITE_NODE_TYPE("MOTION");

	WRITE_INT_FIELD(motionID);
	WRITE_ENUM_FIELD(motionType, MotionType);
	WRITE_BOOL_FIELD(sendSorted);

	WRITE_NODE_FIELD(hashExprs);
	WRITE_OID_ARRAY(hashFuncs, list_length(node->hashExprs));

	WRITE_INT_FIELD(isBroadcast);

	WRITE_INT_FIELD(numSortCols);
	WRITE_INT_ARRAY(sortColIdx, node->numSortCols, AttrNumber);
	WRITE_OID_ARRAY(sortOperators, node->numSortCols);
	WRITE_OID_ARRAY(collations, node->numSortCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numSortCols);

	WRITE_INT_FIELD(segidColIdx);

	_outPlanInfo(str, (Plan *) node);
}

/*****************************************************************************
 *
 *	Stuff from primnodes.h.
 *
 *****************************************************************************/

static void
_outConst(StringInfo str, Const *node)
{
	WRITE_NODE_TYPE("CONST");

	WRITE_OID_FIELD(consttype);
	WRITE_INT_FIELD(consttypmod);
	WRITE_OID_FIELD(constcollid);
	WRITE_INT_FIELD(constlen);
	WRITE_BOOL_FIELD(constbyval);
	WRITE_BOOL_FIELD(constisnull);
	WRITE_LOCATION_FIELD(location);

	if (!node->constisnull)
		_outDatum(str, node->constvalue, node->constlen, node->constbyval);
}

static void
_outBoolExpr(StringInfo str, BoolExpr *node)
{
	WRITE_NODE_TYPE("BOOLEXPR");
	WRITE_ENUM_FIELD(boolop, BoolExprType);

	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCurrentOfExpr(StringInfo str, CurrentOfExpr *node)
{
	WRITE_NODE_TYPE("CURRENTOFEXPR");

	WRITE_UINT_FIELD(cvarno);
	WRITE_STRING_FIELD(cursor_name);
	WRITE_INT_FIELD(cursor_param);
	WRITE_OID_FIELD(target_relid);
}

static void
_outJoinExpr(StringInfo str, JoinExpr *node)
{
	WRITE_NODE_TYPE("JOINEXPR");

	WRITE_ENUM_FIELD(jointype, JoinType);
	WRITE_BOOL_FIELD(isNatural);
	WRITE_NODE_FIELD(larg);
	WRITE_NODE_FIELD(rarg);
	WRITE_NODE_FIELD(usingClause);
	WRITE_NODE_FIELD(quals);
	WRITE_NODE_FIELD(alias);
	WRITE_INT_FIELD(rtindex);
}

/*****************************************************************************
 *
 *	Stuff from relation.h.
 *
 *****************************************************************************/

static void
_outIndexOptInfo(StringInfo str, IndexOptInfo *node)
{
	WRITE_NODE_TYPE("INDEXOPTINFO");

	/* NB: this isn't a complete set of fields */
	WRITE_OID_FIELD(indexoid);
	/* Do NOT print rel field, else infinite recursion */
	WRITE_UINT_FIELD(pages);
	WRITE_FLOAT_FIELD(tuples, "%.0f");
	WRITE_INT_FIELD(ncolumns);

	WRITE_OID_ARRAY(opfamily, node->ncolumns);
	WRITE_INT_ARRAY(indexkeys, node->ncolumns, int);
	WRITE_OID_ARRAY(sortopfamily, node->ncolumns);

	WRITE_BOOL_FIELD(reverse_sort);
	WRITE_BOOL_FIELD(nulls_first);

    WRITE_OID_FIELD(relam);
	WRITE_OID_FIELD(amcostestimate);
	WRITE_NODE_FIELD(indexprs);
	WRITE_NODE_FIELD(indpred);
	WRITE_BOOL_FIELD(predOK);
	WRITE_BOOL_FIELD(unique);
	WRITE_BOOL_FIELD(hypothetical);

	WRITE_BOOL_FIELD(amoptionalkey);
}

/*****************************************************************************
 *
 *	Stuff from parsenodes.h.
 *
 *****************************************************************************/

static void
_outCreateExtensionStmt(StringInfo str, CreateExtensionStmt *node)
{
	WRITE_NODE_TYPE("CREATEEXTENSIONSTMT");
	WRITE_STRING_FIELD(extname);
	WRITE_BOOL_FIELD(if_not_exists);
	WRITE_NODE_FIELD(options);
	WRITE_ENUM_FIELD(create_ext_state, CreateExtensionState);
}

static void
_outCreateStmt_common(StringInfo str, CreateStmt *node)
{
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(tableElts);
	WRITE_NODE_FIELD(inhRelations);
	WRITE_NODE_FIELD(inhOids);
	WRITE_INT_FIELD(parentOidCount);
	WRITE_NODE_FIELD(ofTypename);
	WRITE_NODE_FIELD(constraints);

	WRITE_NODE_FIELD(options);
	WRITE_ENUM_FIELD(oncommit, OnCommitAction);
	WRITE_STRING_FIELD(tablespacename);
	WRITE_BOOL_FIELD(if_not_exists);

	WRITE_NODE_FIELD(distributedBy);
	WRITE_CHAR_FIELD(relKind);
	WRITE_CHAR_FIELD(relStorage);
	WRITE_OID_FIELD(relRangeID); 
	WRITE_INT_FIELD(replicanum);
	int replica_num = node->replicanum;
	WRITE_INT_ARRAY(replica_segid, replica_num, uint32);
	/* postCreate - for analysis, QD only */
	/* deferredStmts - for analysis, QD only */
	WRITE_BOOL_FIELD(is_part_child);
	WRITE_BOOL_FIELD(is_part_parent);
	WRITE_BOOL_FIELD(is_add_part);
	WRITE_BOOL_FIELD(is_split_part);
	WRITE_OID_FIELD(ownerid);
	WRITE_BOOL_FIELD(buildAoBlkdir);
	WRITE_NODE_FIELD(attr_encodings);
}

static void
_outCreateStmt(StringInfo str, CreateStmt *node)
{
	WRITE_NODE_TYPE("CREATESTMT");

	_outCreateStmt_common(str, node);
}

static void
_outCreateForeignTableStmt(StringInfo str, CreateForeignTableStmt *node)
{
	WRITE_NODE_TYPE("CREATEFOREIGNTABLESTMT");

	_outCreateStmt_common(str, &node->base);

	WRITE_STRING_FIELD(servername);
	WRITE_NODE_FIELD(options);
}

static void
_outPartitionSpec(StringInfo str, PartitionSpec *node)
{
	WRITE_NODE_TYPE("PARTITIONSPEC");
	WRITE_NODE_FIELD(partElem);
	WRITE_NODE_FIELD(subSpec);
	WRITE_BOOL_FIELD(istemplate);
	WRITE_LOCATION_FIELD(location);
	WRITE_NODE_FIELD(enc_clauses);
}

static void
_outPartitionBoundSpec(StringInfo str, PartitionBoundSpec *node)
{
	WRITE_NODE_TYPE("PARTITIONBOUNDSPEC");
	WRITE_NODE_FIELD(partStart);
	WRITE_NODE_FIELD(partEnd);
	WRITE_NODE_FIELD(partEvery);
	WRITE_LOCATION_FIELD(location);
}

static void
_outPartition(StringInfo str, Partition *node)
{
	WRITE_NODE_TYPE("PARTITION");

	WRITE_OID_FIELD(partid);
	WRITE_OID_FIELD(parrelid);
	WRITE_CHAR_FIELD(parkind);
	WRITE_INT_FIELD(parlevel);
	WRITE_BOOL_FIELD(paristemplate);
	WRITE_BINARY_FIELD(parnatts, sizeof(int16));
	WRITE_INT_ARRAY(paratts, node->parnatts, int16);
	WRITE_OID_ARRAY(parclass, node->parnatts);
}

static void
_outPartitionRule(StringInfo str, PartitionRule *node)
{
	WRITE_NODE_TYPE("PARTITIONRULE");

	WRITE_OID_FIELD(parruleid);
	WRITE_OID_FIELD(paroid);
	WRITE_OID_FIELD(parchildrelid);
	WRITE_OID_FIELD(parparentoid);
	WRITE_BOOL_FIELD(parisdefault);
	WRITE_STRING_FIELD(parname);
	WRITE_NODE_FIELD(parrangestart);
	WRITE_BOOL_FIELD(parrangestartincl);
	WRITE_NODE_FIELD(parrangeend);
	WRITE_BOOL_FIELD(parrangeendincl);
	WRITE_NODE_FIELD(parrangeevery);
	WRITE_NODE_FIELD(parlistvalues);
	WRITE_BINARY_FIELD(parruleord, sizeof(int16));
	WRITE_NODE_FIELD(parreloptions);
	WRITE_OID_FIELD(partemplatespaceId);
	WRITE_NODE_FIELD(children);
}

static void
_outAlterPartitionCmd(StringInfo str, AlterPartitionCmd *node)
{
	WRITE_NODE_TYPE("ALTERPARTITIONCMD");

	WRITE_NODE_FIELD(partid);
	WRITE_NODE_FIELD(arg1);
	WRITE_NODE_FIELD(arg2);
}

static void
_outCreateDomainStmt(StringInfo str, CreateDomainStmt *node)
{
	WRITE_NODE_TYPE("CREATEDOMAINSTMT");
	WRITE_NODE_FIELD(domainname);
	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(constraints);
}

static void
_outAlterDomainStmt(StringInfo str, AlterDomainStmt *node)
{
	WRITE_NODE_TYPE("ALTERDOMAINSTMT");
	WRITE_CHAR_FIELD(subtype);
	WRITE_NODE_FIELD(typeName);
	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(def);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAlterDefaultPrivilegesStmt(StringInfo str, AlterDefaultPrivilegesStmt *node)
{
	WRITE_NODE_TYPE("ALTERDEFAULTPRIVILEGESSTMT");
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(action);
}

static void
_outQuery(StringInfo str, Query *node)
{
	WRITE_NODE_TYPE("QUERY");

	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_ENUM_FIELD(querySource, QuerySource);
	WRITE_BOOL_FIELD(canSetTag);

	WRITE_NODE_FIELD(utilityStmt);
	WRITE_INT_FIELD(resultRelation);
	WRITE_BOOL_FIELD(hasAggs);
	WRITE_BOOL_FIELD(hasWindowFuncs);
	WRITE_BOOL_FIELD(hasSubLinks);
	WRITE_BOOL_FIELD(hasDynamicFunctions);
	WRITE_BOOL_FIELD(hasFuncsWithExecRestrictions);
	WRITE_BOOL_FIELD(hasDistinctOn);
	WRITE_BOOL_FIELD(hasRecursive);
	WRITE_BOOL_FIELD(hasModifyingCTE);
	WRITE_BOOL_FIELD(hasForUpdate);
	WRITE_NODE_FIELD(cteList);
	WRITE_NODE_FIELD(rtable);
	WRITE_NODE_FIELD(jointree);
	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(returningList);
	WRITE_NODE_FIELD(groupClause);
	WRITE_NODE_FIELD(havingQual);
	WRITE_NODE_FIELD(windowClause);
	WRITE_NODE_FIELD(distinctClause);
	WRITE_NODE_FIELD(sortClause);
	WRITE_NODE_FIELD(scatterClause);
	WRITE_BOOL_FIELD(isTableValueSelect);
	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(limitCount);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(setOperations);
	WRITE_NODE_FIELD(constraintDeps);
	WRITE_BOOL_FIELD(parentStmtType);

	/* Don't serialize policy */
}

static void
_outAExpr(StringInfo str, A_Expr *node)
{
	WRITE_NODE_TYPE("AEXPR");
	WRITE_ENUM_FIELD(kind, A_Expr_Kind);

	switch (node->kind)
	{
		case AEXPR_OP:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_AND:

			break;
		case AEXPR_OR:

			break;
		case AEXPR_NOT:

			break;
		case AEXPR_OP_ANY:

			WRITE_NODE_FIELD(name);

			break;
		case AEXPR_OP_ALL:

			WRITE_NODE_FIELD(name);

			break;
		case AEXPR_DISTINCT:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NULLIF:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_OF:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_IN:

			WRITE_NODE_FIELD(name);
			break;
		default:

			break;
	}

	WRITE_NODE_FIELD(lexpr);
	WRITE_NODE_FIELD(rexpr);
	WRITE_LOCATION_FIELD(location);
}

static void
_outValue(StringInfo str, Value *value)
{

	int16 vt = value->type;
	appendBinaryStringInfo(str, (const char *)&vt, sizeof(int16));
	switch (value->type)
	{
		case T_Integer:
			appendBinaryStringInfo(str, (const char *)&value->val.ival, sizeof(long));
			break;
		case T_Float:
		case T_String:
		case T_BitString:
			{
				int slen = (value->val.str != NULL ? strlen(value->val.str) : 0);
				appendBinaryStringInfo(str, (const char *)&slen, sizeof(int));
				if (slen > 0)
					appendBinaryStringInfo(str, value->val.str, slen);
			}
			break;
		case T_Null:
			/* nothing to do */
			break;
		default:
			elog(ERROR, "unrecognized node type: %d", (int) value->type);
			break;
	}
}

static void
_outAConst(StringInfo str, A_Const *node)
{
	WRITE_NODE_TYPE("A_CONST");

	_outValue(str, &(node->val));
	WRITE_LOCATION_FIELD(location);  /*CDB*/

}

static void
_outConstraint(StringInfo str, Constraint *node)
{
	WRITE_NODE_TYPE("CONSTRAINT");

	WRITE_STRING_FIELD(conname);
	WRITE_BOOL_FIELD(deferrable);
	WRITE_BOOL_FIELD(initdeferred);
	WRITE_LOCATION_FIELD(location);

	WRITE_ENUM_FIELD(contype,ConstrType);

	switch (node->contype)
	{
		case CONSTR_PRIMARY:
		case CONSTR_UNIQUE:
			WRITE_NODE_FIELD(keys);
			WRITE_NODE_FIELD(options);
			WRITE_STRING_FIELD(indexspace);
			/* access_method and where_clause not currently used */
			break;

		case CONSTR_CHECK:
			/*
			 * GPDB: need dispatch skip_validation and is_no_inherit for statement like:
			 * ALTER DOMAIN things ADD CONSTRAINT meow CHECK (VALUE < 11) NOT VALID;
			 * ALTER TABLE constraint_rename_test ADD CONSTRAINT con2 CHECK NO INHERIT (b > 0);
			 */
			WRITE_BOOL_FIELD(skip_validation);
			WRITE_BOOL_FIELD(is_no_inherit);
		case CONSTR_DEFAULT:
			WRITE_NODE_FIELD(raw_expr);
			WRITE_STRING_FIELD(cooked_expr);
			break;

		case CONSTR_EXCLUSION:
			WRITE_NODE_FIELD(exclusions);
			WRITE_NODE_FIELD(options);
			WRITE_STRING_FIELD(indexspace);
			WRITE_STRING_FIELD(access_method);
			WRITE_NODE_FIELD(where_clause);
			break;

		case CONSTR_FOREIGN:
			WRITE_NODE_FIELD(pktable);
			WRITE_NODE_FIELD(fk_attrs);
			WRITE_NODE_FIELD(pk_attrs);
			WRITE_CHAR_FIELD(fk_matchtype);
			WRITE_CHAR_FIELD(fk_upd_action);
			WRITE_CHAR_FIELD(fk_del_action);
			WRITE_BOOL_FIELD(skip_validation);
			WRITE_BOOL_FIELD(initially_valid);
			WRITE_OID_FIELD(trig1Oid);
			WRITE_OID_FIELD(trig2Oid);
			WRITE_OID_FIELD(trig3Oid);
			WRITE_OID_FIELD(trig4Oid);
			break;

		case CONSTR_NULL:
		case CONSTR_NOTNULL:
		case CONSTR_ATTR_DEFERRABLE:
		case CONSTR_ATTR_NOT_DEFERRABLE:
		case CONSTR_ATTR_DEFERRED:
		case CONSTR_ATTR_IMMEDIATE:
			break;

		default:
			elog(WARNING,"serialization doesn't know what to do with this constraint");
			break;
	}
}

static void
_outCreateQueueStmt(StringInfo str, CreateQueueStmt *node)
{
	WRITE_NODE_TYPE("CREATEQUEUESTMT");

	WRITE_STRING_FIELD(queue);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outAlterQueueStmt(StringInfo str, AlterQueueStmt *node)
{
	WRITE_NODE_TYPE("ALTERQUEUESTMT");

	WRITE_STRING_FIELD(queue);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outCreateResourceGroupStmt(StringInfo str, CreateResourceGroupStmt *node)
{
	WRITE_NODE_TYPE("CREATERESOURCEGROUPSTMT");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outAlterResourceGroupStmt(StringInfo str, AlterResourceGroupStmt *node)
{
	WRITE_NODE_TYPE("ALTERRESOURCEGROUPSTMT");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

/*
 * Support for serializing TupleDescs and ParamListInfos.
 *
 * TupleDescs and ParamListInfos are not Nodes as such, but if you wrap them
 * in TupleDescNode and ParamListInfoNode structs, we allow serializing them.
 */
static void
_outTupleDescNode(StringInfo str, TupleDescNode *node)
{
	int			i;

	Assert(node->tuple->tdtypeid == RECORDOID);

	WRITE_NODE_TYPE("TUPLEDESCNODE");
	WRITE_INT_FIELD(natts);
	WRITE_INT_FIELD(tuple->natts);

	for (i = 0; i < node->tuple->natts; i++)
		appendBinaryStringInfo(str, node->tuple->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);

	Assert(node->tuple->constr == NULL);

	WRITE_OID_FIELD(tuple->tdtypeid);
	WRITE_INT_FIELD(tuple->tdtypmod);
	WRITE_BOOL_FIELD(tuple->tdhasoid);
	WRITE_INT_FIELD(tuple->tdrefcount);
}

static void
_outSerializedParamExternData(StringInfo str, SerializedParamExternData *node)
{
	WRITE_NODE_TYPE("SERIALIZEDPARAMEXTERNDATA");

	WRITE_BOOL_FIELD(isnull);
	WRITE_INT16_FIELD(pflags);
	WRITE_OID_FIELD(ptype);
	WRITE_INT16_FIELD(plen);
	WRITE_BOOL_FIELD(pbyval);

	if (!node->isnull)
		_outDatum(str, node->value, node->plen, node->pbyval);
}

static void
_outCookedConstraint(StringInfo str, CookedConstraint *node)
{
	WRITE_NODE_TYPE("COOKEDCONSTRAINT");

	WRITE_ENUM_FIELD(contype,ConstrType);
	WRITE_STRING_FIELD(name);
	WRITE_INT_FIELD(attnum);
	WRITE_NODE_FIELD(expr);
	WRITE_BOOL_FIELD(is_local);
	WRITE_INT_FIELD(inhcount);
}

static void
_outAlterEnumStmt(StringInfo str, AlterEnumStmt *node)
{
	WRITE_NODE_TYPE("ALTERENUMSTMT");

	WRITE_NODE_FIELD(typeName);
	WRITE_STRING_FIELD(newVal);
	WRITE_STRING_FIELD(newValNeighbor);
	WRITE_BOOL_FIELD(newValIsAfter);
	WRITE_BOOL_FIELD(skipIfExists);
}

static void
_outCreateFdwStmt(StringInfo str, CreateFdwStmt *node)
{
	WRITE_NODE_TYPE("CREATEFDWSTMT");

	WRITE_STRING_FIELD(fdwname);
	WRITE_NODE_FIELD(func_options);
	WRITE_NODE_FIELD(options);
}

static void
_outAlterFdwStmt(StringInfo str, AlterFdwStmt *node)
{
	WRITE_NODE_TYPE("ALTERFDWSTMT");

	WRITE_STRING_FIELD(fdwname);
	WRITE_NODE_FIELD(func_options);
	WRITE_NODE_FIELD(options);
}

static void
_outCreateForeignServerStmt(StringInfo str, CreateForeignServerStmt *node)
{
	WRITE_NODE_TYPE("CREATEFOREIGNSERVERSTMT");

	WRITE_STRING_FIELD(servername);
	WRITE_STRING_FIELD(servertype);
	WRITE_STRING_FIELD(version);
	WRITE_STRING_FIELD(fdwname);
	WRITE_NODE_FIELD(options);
}

static void
_outAlterForeignServerStmt(StringInfo str, AlterForeignServerStmt *node)
{
	WRITE_NODE_TYPE("ALTERFOREIGNSERVERSTMT");

	WRITE_STRING_FIELD(servername);
	WRITE_STRING_FIELD(version);
	WRITE_NODE_FIELD(options);
	WRITE_BOOL_FIELD(has_version);
}

static void
_outCreateUserMappingStmt(StringInfo str, CreateUserMappingStmt *node)
{
	WRITE_NODE_TYPE("CREATEUSERMAPPINGSTMT");

	WRITE_STRING_FIELD(username);
	WRITE_STRING_FIELD(servername);
	WRITE_NODE_FIELD(options);
}

static void
_outAlterUserMappingStmt(StringInfo str, AlterUserMappingStmt *node)
{
	WRITE_NODE_TYPE("ALTERUSERMAPPINGSTMT");

	WRITE_STRING_FIELD(username);
	WRITE_STRING_FIELD(servername);
	WRITE_NODE_FIELD(options);
}

static void
_outDropUserMappingStmt(StringInfo str, DropUserMappingStmt *node)
{
	WRITE_NODE_TYPE("DROPUSERMAPPINGSTMT");

	WRITE_STRING_FIELD(username);
	WRITE_STRING_FIELD(servername);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAccessPriv(StringInfo str, AccessPriv *node)
{
	WRITE_NODE_TYPE("ACCESSPRIV");

	WRITE_STRING_FIELD(priv_name);
	WRITE_NODE_FIELD(cols);
}

static void
_outGpPolicy(StringInfo str, GpPolicy *node)
{
	WRITE_NODE_TYPE("GPPOLICY");

	WRITE_ENUM_FIELD(ptype, GpPolicyType);
	WRITE_INT_FIELD(numsegments);
	WRITE_INT_FIELD(nattrs);
	WRITE_INT_ARRAY(attrs, node->nattrs, AttrNumber);
	WRITE_INT_ARRAY(opclasses, node->nattrs, Oid);
}

static void
_outAlterTableMoveAllStmt(StringInfo str, AlterTableMoveAllStmt *node)
{
	WRITE_NODE_TYPE("ALTERTABLESPACEMOVESTMT");

	WRITE_STRING_FIELD(orig_tablespacename);
	WRITE_ENUM_FIELD(objtype, ObjectType);
	WRITE_NODE_FIELD(roles);
	WRITE_STRING_FIELD(new_tablespacename);
	WRITE_BOOL_FIELD(nowait);
}

static void
_outAlterTableSpaceOptionsStmt(StringInfo str, AlterTableSpaceOptionsStmt *node)
{
	WRITE_NODE_TYPE("ALTERTABLESPACEOPTIONS");

	WRITE_STRING_FIELD(tablespacename);
	WRITE_NODE_FIELD(options);
	WRITE_BOOL_FIELD(isReset);
}

/*
 * _outNode -
 *	  converts a Node into binary string and append it to 'str'
 */
static void
_outNode(StringInfo str, void *obj)
{
	if (obj == NULL)
	{
		int16 tg = 0;
		appendBinaryStringInfo(str, (const char *)&tg, sizeof(int16));
	}
	else if (IsA(obj, List) ||IsA(obj, IntList) || IsA(obj, OidList))
		_outList(str, obj);
	else if (IsA(obj, Integer) ||
			 IsA(obj, Float) ||
			 IsA(obj, String) ||
			 IsA(obj, Null) ||
			 IsA(obj, BitString))
	{
		_outValue(str, obj);
	}
	else
	{
		switch (nodeTag(obj))
		{
			case T_PlannedStmt:
				_outPlannedStmt(str,obj);
				break;
			case T_QueryDispatchDesc:
				_outQueryDispatchDesc(str,obj);
				break;
			case T_OidAssignment:
				_outOidAssignment(str,obj);
				break;
			case T_Plan:
				_outPlan(str, obj);
				break;
			case T_Result:
				_outResult(str, obj);
				break;
			case T_Repeat:
				_outRepeat(str, obj);
				break;
			case T_ModifyTable:
				_outModifyTable(str, obj);
				break;
			case T_Append:
				_outAppend(str, obj);
				break;
			case T_MergeAppend:
				_outMergeAppend(str, obj);
				break;
			case T_Sequence:
				_outSequence(str, obj);
				break;
			case T_RecursiveUnion:
				_outRecursiveUnion(str, obj);
				break;
			case T_BitmapAnd:
				_outBitmapAnd(str, obj);
				break;
			case T_BitmapOr:
				_outBitmapOr(str, obj);
				break;
			case T_Scan:
				_outScan(str, obj);
				break;
			case T_SeqScan:
				_outSeqScan(str, obj);
				break;
			case T_DynamicSeqScan:
				_outDynamicSeqScan(str, obj);
				break;
			case T_CteScan:
				_outCteScan(str, obj);
				break;
			case T_WorkTableScan:
				_outWorkTableScan(str, obj);
				break;
			case T_ForeignScan:
				_outForeignScan(str, obj);
				break;
			case T_ExternalScan:
				_outExternalScan(str, obj);
				break;
			case T_IndexScan:
				_outIndexScan(str, obj);
				break;
			case T_IndexOnlyScan:
				_outIndexOnlyScan(str, obj);
				break;
			case T_DynamicIndexScan:
				_outDynamicIndexScan(str, obj);
				break;
			case T_BitmapIndexScan:
				_outBitmapIndexScan(str, obj);
				break;
			case T_DynamicBitmapIndexScan:
				_outDynamicBitmapIndexScan(str, obj);
				break;
			case T_BitmapHeapScan:
				_outBitmapHeapScan(str, obj);
				break;
			case T_DynamicBitmapHeapScan:
				_outDynamicBitmapHeapScan(str, obj);
				break;
			case T_TidScan:
				_outTidScan(str, obj);
				break;
			case T_SubqueryScan:
				_outSubqueryScan(str, obj);
				break;
			case T_FunctionScan:
				_outFunctionScan(str, obj);
				break;
			case T_ValuesScan:
				_outValuesScan(str, obj);
				break;
			case T_Join:
				_outJoin(str, obj);
				break;
			case T_NestLoop:
				_outNestLoop(str, obj);
				break;
			case T_MergeJoin:
				_outMergeJoin(str, obj);
				break;
			case T_HashJoin:
				_outHashJoin(str, obj);
				break;
			case T_Agg:
				_outAgg(str, obj);
				break;
			case T_WindowAgg:
				_outWindowAgg(str, obj);
				break;
			case T_TableFunctionScan:
				_outTableFunctionScan(str, obj);
				break;
			case T_Material:
				_outMaterial(str, obj);
				break;
			case T_ShareInputScan:
				_outShareInputScan(str, obj);
				break;
			case T_Sort:
				_outSort(str, obj);
				break;
			case T_Unique:
				_outUnique(str, obj);
				break;
			case T_SetOp:
				_outSetOp(str, obj);
				break;
			case T_LockRows:
				_outLockRows(str, obj);
				break;
			case T_Limit:
				_outLimit(str, obj);
				break;
			case T_NestLoopParam:
				_outNestLoopParam(str, obj);
				break;
			case T_PlanRowMark:
				_outPlanRowMark(str, obj);
				break;
			case T_Hash:
				_outHash(str, obj);
				break;
			case T_Motion:
				_outMotion(str, obj);
				break;
			case T_DML:
				_outDML(str, obj);
				break;
			case T_SplitUpdate:
				_outSplitUpdate(str, obj);
				break;
			case T_RowTrigger:
				_outRowTrigger(str, obj);
				break;
			case T_AssertOp:
				_outAssertOp(str, obj);
				break;
			case T_PartitionSelector:
				_outPartitionSelector(str, obj);
				break;
			case T_Alias:
				_outAlias(str, obj);
				break;
			case T_Temporal:
				_outTemporal(str, obj);
				break;
			case T_RangeVar:
				_outRangeVar(str, obj);
				break;
			case T_IntoClause:
				_outIntoClause(str, obj);
				break;
			case T_CopyIntoClause:
				_outCopyIntoClause(str, obj);
				break;
			case T_Var:
				_outVar(str, obj);
				break;
			case T_Const:
				_outConst(str, obj);
				break;
			case T_Param:
				_outParam(str, obj);
				break;
			case T_Aggref:
				_outAggref(str, obj);
				break;
			case T_WindowFunc:
				_outWindowFunc(str, obj);
				break;
			case T_ArrayRef:
				_outArrayRef(str, obj);
				break;
			case T_FuncExpr:
				_outFuncExpr(str, obj);
				break;
			case T_NamedArgExpr:
				_outNamedArgExpr(str, obj);
				break;
			case T_OpExpr:
				_outOpExpr(str, obj);
				break;
			case T_DistinctExpr:
				_outDistinctExpr(str, obj);
				break;
			case T_ScalarArrayOpExpr:
				_outScalarArrayOpExpr(str, obj);
				break;
			case T_BoolExpr:
				_outBoolExpr(str, obj);
				break;
			case T_SubLink:
				_outSubLink(str, obj);
				break;
			case T_SubPlan:
				_outSubPlan(str, obj);
				break;
			case T_AlternativeSubPlan:
				_outAlternativeSubPlan(str, obj);
				break;
			case T_FieldSelect:
				_outFieldSelect(str, obj);
				break;
			case T_FieldStore:
				_outFieldStore(str, obj);
				break;
			case T_RelabelType:
				_outRelabelType(str, obj);
				break;
			case T_CoerceViaIO:
				_outCoerceViaIO(str, obj);
				break;
			case T_ArrayCoerceExpr:
				_outArrayCoerceExpr(str, obj);
				break;
			case T_ConvertRowtypeExpr:
				_outConvertRowtypeExpr(str, obj);
				break;
			case T_CollateExpr:
				_outCollateExpr(str, obj);
				break;
			case T_CaseExpr:
				_outCaseExpr(str, obj);
				break;
			case T_CaseWhen:
				_outCaseWhen(str, obj);
				break;
			case T_CaseTestExpr:
				_outCaseTestExpr(str, obj);
				break;
			case T_ArrayExpr:
				_outArrayExpr(str, obj);
				break;
			case T_RowExpr:
				_outRowExpr(str, obj);
				break;
			case T_RowCompareExpr:
				_outRowCompareExpr(str, obj);
				break;
			case T_CoalesceExpr:
				_outCoalesceExpr(str, obj);
				break;
			case T_MinMaxExpr:
				_outMinMaxExpr(str, obj);
				break;
			case T_NullIfExpr:
				_outNullIfExpr(str, obj);
				break;
			case T_NullTest:
				_outNullTest(str, obj);
				break;
			case T_BooleanTest:
				_outBooleanTest(str, obj);
				break;
			case T_XmlExpr:
				_outXmlExpr(str, obj);
				break;
			case T_CoerceToDomain:
				_outCoerceToDomain(str, obj);
				break;
			case T_CoerceToDomainValue:
				_outCoerceToDomainValue(str, obj);
				break;
			case T_SetToDefault:
				_outSetToDefault(str, obj);
				break;
			case T_CurrentOfExpr:
				_outCurrentOfExpr(str, obj);
				break;
			case T_TargetEntry:
				_outTargetEntry(str, obj);
				break;
			case T_RangeTblRef:
				_outRangeTblRef(str, obj);
				break;
			case T_RangeTblFunction:
				_outRangeTblFunction(str, obj);
				break;
			case T_JoinExpr:
				_outJoinExpr(str, obj);
				break;
			case T_FromExpr:
				_outFromExpr(str, obj);
				break;
			case T_Flow:
				_outFlow(str, obj);
				break;

			case T_Path:
				_outPath(str, obj);
				break;
			case T_IndexPath:
				_outIndexPath(str, obj);
				break;
			case T_BitmapHeapPath:
				_outBitmapHeapPath(str, obj);
				break;
			case T_BitmapAndPath:
				_outBitmapAndPath(str, obj);
				break;
			case T_BitmapOrPath:
				_outBitmapOrPath(str, obj);
				break;
			case T_TidPath:
				_outTidPath(str, obj);
				break;
			case T_ForeignPath:
				_outForeignPath(str, obj);
				break;
			case T_AppendPath:
				_outAppendPath(str, obj);
				break;
			case T_MergeAppendPath:
				_outMergeAppendPath(str, obj);
				break;
			case T_AppendOnlyPath:
				_outAppendOnlyPath(str, obj);
				break;
			case T_AOCSPath:
				_outAOCSPath(str, obj);
				break;
			case T_ResultPath:
				_outResultPath(str, obj);
				break;
			case T_MaterialPath:
				_outMaterialPath(str, obj);
				break;
			case T_UniquePath:
				_outUniquePath(str, obj);
				break;
			case T_NestPath:
				_outNestPath(str, obj);
				break;
			case T_MergePath:
				_outMergePath(str, obj);
				break;
			case T_HashPath:
				_outHashPath(str, obj);
				break;
            case T_CdbMotionPath:
                _outCdbMotionPath(str, obj);
                break;
			case T_PlannerInfo:
				_outPlannerInfo(str, obj);
				break;
			case T_PlannerParamItem:
				_outPlannerParamItem(str, obj);
				break;
			case T_RelOptInfo:
				_outRelOptInfo(str, obj);
				break;
			case T_IndexOptInfo:
				_outIndexOptInfo(str, obj);
				break;
			case T_PathKey:
				_outPathKey(str, obj);
				break;
			case T_ParamPathInfo:
				_outParamPathInfo(str, obj);
				break;
			case T_RestrictInfo:
				_outRestrictInfo(str, obj);
				break;
			case T_SpecialJoinInfo:
				_outSpecialJoinInfo(str, obj);
				break;
			case T_LateralJoinInfo:
				_outLateralJoinInfo(str, obj);
				break;
			case T_AppendRelInfo:
				_outAppendRelInfo(str, obj);
				break;
			case T_CreateExtensionStmt:
				_outCreateExtensionStmt(str, obj);
				break;


			case T_GrantStmt:
				_outGrantStmt(str, obj);
				break;
			case T_AccessPriv:
				_outAccessPriv(str, obj);
				break;
			case T_PrivGrantee:
				_outPrivGrantee(str, obj);
				break;
			case T_FuncWithArgs:
				_outFuncWithArgs(str, obj);
				break;
			case T_GrantRoleStmt:
				_outGrantRoleStmt(str, obj);
				break;
			case T_LockStmt:
				_outLockStmt(str, obj);
				break;

			case T_CreateStmt:
				_outCreateStmt(str, obj);
				break;
			case T_CreateForeignTableStmt:
				_outCreateForeignTableStmt(str, obj);
				break;
			case T_ColumnReferenceStorageDirective:
				_outColumnReferenceStorageDirective(str, obj);
				break;
			case T_PartitionBy:
				_outPartitionBy(str, obj);
				break;
			case T_PartitionElem:
				_outPartitionElem(str, obj);
				break;
			case T_PartitionRangeItem:
				_outPartitionRangeItem(str, obj);
				break;
			case T_PartitionBoundSpec:
				_outPartitionBoundSpec(str, obj);
				break;
			case T_PartitionSpec:
				_outPartitionSpec(str, obj);
				break;
			case T_PartitionValuesSpec:
				_outPartitionValuesSpec(str, obj);
				break;
			case T_ExpandStmtSpec:
				_outExpandStmtSpec(str, obj);
				break;
			case T_Partition:
				_outPartition(str, obj);
				break;
			case T_PartitionRule:
				_outPartitionRule(str, obj);
				break;
			case T_PartitionNode:
				_outPartitionNode(str, obj);
				break;
			case T_PgPartRule:
				_outPgPartRule(str, obj);
				break;

			case T_SegfileMapNode:
				_outSegfileMapNode(str, obj);
				break;

			case T_ExtTableTypeDesc:
				_outExtTableTypeDesc(str, obj);
				break;
            case T_CreateExternalStmt:
				_outCreateExternalStmt(str, obj);
				break;

			case T_IndexStmt:
				_outIndexStmt(str, obj);
				break;
			case T_ReindexStmt:
				_outReindexStmt(str, obj);
				break;

			case T_ConstraintsSetStmt:
				_outConstraintsSetStmt(str, obj);
				break;

			case T_CreateFunctionStmt:
				_outCreateFunctionStmt(str, obj);
				break;
			case T_FunctionParameter:
				_outFunctionParameter(str, obj);
				break;
			case T_AlterFunctionStmt:
				_outAlterFunctionStmt(str, obj);
				break;

			case T_DefineStmt:
				_outDefineStmt(str,obj);
				break;

			case T_CompositeTypeStmt:
				_outCompositeTypeStmt(str,obj);
				break;
			case T_CreateEnumStmt:
				_outCreateEnumStmt(str,obj);
				break;
			case T_CreateRangeStmt:
				_outCreateRangeStmt(str, obj);
				break;
			case T_AlterEnumStmt:
				_outAlterEnumStmt(str, obj);
				break;

			case T_CreateCastStmt:
				_outCreateCastStmt(str,obj);
				break;
			case T_CreateOpClassStmt:
				_outCreateOpClassStmt(str,obj);
				break;
			case T_CreateOpClassItem:
				_outCreateOpClassItem(str,obj);
				break;
			case T_CreateOpFamilyStmt:
				_outCreateOpFamilyStmt(str,obj);
				break;
			case T_AlterOpFamilyStmt:
				_outAlterOpFamilyStmt(str,obj);
				break;
			case T_CreateConversionStmt:
				_outCreateConversionStmt(str,obj);
				break;


			case T_ViewStmt:
				_outViewStmt(str, obj);
				break;
			case T_RuleStmt:
				_outRuleStmt(str, obj);
				break;
			case T_DropStmt:
				_outDropStmt(str, obj);
				break;
			case T_DropOwnedStmt:
				_outDropOwnedStmt(str, obj);
				break;
			case T_ReassignOwnedStmt:
				_outReassignOwnedStmt(str, obj);
				break;
			case T_TruncateStmt:
				_outTruncateStmt(str, obj);
				break;
			case T_ReplicaIdentityStmt:
				_outReplicaIdentityStmt(str, obj);
				break;
			case T_AlterTableStmt:
				_outAlterTableStmt(str, obj);
				break;
			case T_AlterTableCmd:
				_outAlterTableCmd(str, obj);
				break;
			case T_SetDistributionCmd:
				_outSetDistributionCmd(str, obj);
				break;
			case T_InheritPartitionCmd:
				_outInheritPartitionCmd(str, obj);
				break;

			case T_AlterPartitionCmd:
				_outAlterPartitionCmd(str, obj);
				break;
			case T_AlterPartitionId:
				_outAlterPartitionId(str, obj);
				break;

			case T_CreateRoleStmt:
				_outCreateRoleStmt(str, obj);
				break;
			case T_DropRoleStmt:
				_outDropRoleStmt(str, obj);
				break;
			case T_AlterRoleStmt:
				_outAlterRoleStmt(str, obj);
				break;
			case T_AlterRoleSetStmt:
				_outAlterRoleSetStmt(str, obj);
				break;

			case T_AlterObjectSchemaStmt:
				_outAlterObjectSchemaStmt(str, obj);
				break;

			case T_AlterOwnerStmt:
				_outAlterOwnerStmt(str, obj);
				break;

			case T_RenameStmt:
				_outRenameStmt(str, obj);
				break;

			case T_CreateSeqStmt:
				_outCreateSeqStmt(str, obj);
				break;
			case T_AlterSeqStmt:
				_outAlterSeqStmt(str, obj);
				break;
			case T_ClusterStmt:
				_outClusterStmt(str, obj);
				break;
			case T_CreatedbStmt:
				_outCreatedbStmt(str, obj);
				break;
			case T_DropdbStmt:
				_outDropdbStmt(str, obj);
				break;
			case T_CreateDomainStmt:
				_outCreateDomainStmt(str, obj);
				break;
			case T_AlterDomainStmt:
				_outAlterDomainStmt(str, obj);
				break;
			case T_AlterDefaultPrivilegesStmt:
				_outAlterDefaultPrivilegesStmt(str, obj);
				break;

			case T_TransactionStmt:
				_outTransactionStmt(str, obj);
				break;

			case T_NotifyStmt:
				_outNotifyStmt(str, obj);
				break;
			case T_DeclareCursorStmt:
				_outDeclareCursorStmt(str, obj);
				break;
			case T_SingleRowErrorDesc:
				_outSingleRowErrorDesc(str, obj);
				break;
			case T_CopyStmt:
				_outCopyStmt(str, obj);
				break;
			case T_SelectStmt:
				_outSelectStmt(str, obj);
				break;
			case T_InsertStmt:
				_outInsertStmt(str, obj);
				break;
			case T_DeleteStmt:
				_outDeleteStmt(str, obj);
				break;
			case T_UpdateStmt:
				_outUpdateStmt(str, obj);
				break;
			case T_ColumnDef:
				_outColumnDef(str, obj);
				break;
			case T_TypeName:
				_outTypeName(str, obj);
				break;
			case T_SortBy:
				_outSortBy(str, obj);
				break;
			case T_TypeCast:
				_outTypeCast(str, obj);
				break;
			case T_CollateClause:
				_outCollateClause(str, obj);
				break;
			case T_IndexElem:
				_outIndexElem(str, obj);
				break;
			case T_Query:
				_outQuery(str, obj);
				break;
			case T_WithCheckOption:
				_outWithCheckOption(str, obj);
				break;
			case T_SortGroupClause:
				_outSortGroupClause(str, obj);
				break;
			case T_GroupingClause:
				_outGroupingClause(str, obj);
				break;
			case T_GroupingFunc:
				_outGroupingFunc(str, obj);
				break;
			case T_Grouping:
				_outGrouping(str, obj);
				break;
			case T_GroupId:
				_outGroupId(str, obj);
				break;
			case T_WindowClause:
				_outWindowClause(str, obj);
				break;
			case T_RowMarkClause:
				_outRowMarkClause(str, obj);
				break;
			case T_WithClause:
				_outWithClause(str, obj);
				break;
			case T_CommonTableExpr:
				_outCommonTableExpr(str, obj);
				break;
			case T_SetOperationStmt:
				_outSetOperationStmt(str, obj);
				break;
			case T_RangeTblEntry:
				_outRangeTblEntry(str, obj);
				break;
			case T_A_Expr:
				_outAExpr(str, obj);
				break;
			case T_ColumnRef:
				_outColumnRef(str, obj);
				break;
			case T_ParamRef:
				_outParamRef(str, obj);
				break;
			case T_A_Const:
				_outAConst(str, obj);
				break;
			case T_A_Star:
				_outA_Star(str, obj);
				break;
			case T_A_Indices:
				_outA_Indices(str, obj);
				break;
			case T_A_Indirection:
				_outA_Indirection(str, obj);
				break;
			case T_A_ArrayExpr:
				_outA_ArrayExpr(str,obj);
				break;
			case T_ResTarget:
				_outResTarget(str, obj);
				break;
			case T_Constraint:
				_outConstraint(str, obj);
				break;
			case T_FuncCall:
				_outFuncCall(str, obj);
				break;
			case T_DefElem:
				_outDefElem(str, obj);
				break;
			case T_TableLikeClause:
				_outTableLikeClause(str, obj);
				break;
			case T_LockingClause:
				_outLockingClause(str, obj);
				break;
			case T_XmlSerialize:
				_outXmlSerialize(str, obj);
				break;

			case T_CreateSchemaStmt:
				_outCreateSchemaStmt(str, obj);
				break;
			case T_CreatePLangStmt:
				_outCreatePLangStmt(str, obj);
				break;
			case T_VacuumStmt:
				_outVacuumStmt(str, obj);
				break;
			case T_CdbProcess:
				_outCdbProcess(str, obj);
				break;
			case T_Slice:
				_outSlice(str, obj);
				break;
			case T_SliceTable:
				_outSliceTable(str, obj);
				break;
			case T_CursorPosInfo:
				_outCursorPosInfo(str, obj);
				break;
			case T_VariableSetStmt:
				_outVariableSetStmt(str, obj);
				break;

			case T_DMLActionExpr:
				_outDMLActionExpr(str, obj);
				break;

			case T_PartSelectedExpr:
				_outPartSelectedExpr(str, obj);
				break;

			case T_PartDefaultExpr:
				_outPartDefaultExpr(str, obj);
				break;

			case T_PartBoundExpr:
				_outPartBoundExpr(str, obj);
				break;

			case T_PartBoundInclusionExpr:
				_outPartBoundInclusionExpr(str, obj);
				break;

			case T_PartBoundOpenExpr:
				_outPartBoundOpenExpr(str, obj);
				break;

			case T_PartListRuleExpr:
				_outPartListRuleExpr(str, obj);
				break;

			case T_PartListNullTestExpr:
				_outPartListNullTestExpr(str, obj);
				break;

			case T_CreateTrigStmt:
				_outCreateTrigStmt(str, obj);
				break;

			case T_CreateTableSpaceStmt:
				_outCreateTableSpaceStmt(str, obj);
				break;

			case T_DropTableSpaceStmt:
				_outDropTableSpaceStmt(str, obj);
				break;

			case T_CreateQueueStmt:
				_outCreateQueueStmt(str, obj);
				break;
			case T_AlterQueueStmt:
				_outAlterQueueStmt(str, obj);
				break;
			case T_DropQueueStmt:
				_outDropQueueStmt(str, obj);
				break;

			case T_CreateResourceGroupStmt:
				_outCreateResourceGroupStmt(str, obj);
				break;
			case T_DropResourceGroupStmt:
				_outDropResourceGroupStmt(str, obj);
				break;
			case T_AlterResourceGroupStmt:
				_outAlterResourceGroupStmt(str, obj);
				break;

            case T_CommentStmt:
                _outCommentStmt(str, obj);
                break;
			case T_TableValueExpr:
				_outTableValueExpr(str, obj);
                break;
			case T_DenyLoginInterval:
				_outDenyLoginInterval(str, obj);
				break;
			case T_DenyLoginPoint:
				_outDenyLoginPoint(str, obj);
				break;

			case T_AlterTypeStmt:
				_outAlterTypeStmt(str, obj);
				break;
			case T_AlterExtensionStmt:
				_outAlterExtensionStmt(str, obj);
				break;
			case T_AlterExtensionContentsStmt:
				_outAlterExtensionContentsStmt(str, obj);
				break;

			case T_TupleDescNode:
				_outTupleDescNode(str, obj);
				break;
			case T_SerializedParamExternData:
				_outSerializedParamExternData(str, obj);
				break;

			case T_AlterTSConfigurationStmt:
				_outAlterTSConfigurationStmt(str, obj);
				break;
			case T_AlterTSDictionaryStmt:
				_outAlterTSDictionaryStmt(str, obj);
				break;
			case T_PlaceHolderVar:
				_outPlaceHolderVar(str, obj);
				break;
			case T_PlaceHolderInfo:
				_outPlaceHolderInfo(str, obj);
				break;
			case T_MinMaxAggInfo:
				_outMinMaxAggInfo(str, obj);
				break;

			case T_CookedConstraint:
				_outCookedConstraint(str, obj);
				break;

			case T_DropUserMappingStmt:
				_outDropUserMappingStmt(str, obj);
				break;
			case T_AlterUserMappingStmt:
				_outAlterUserMappingStmt(str, obj);
				break;
			case T_CreateUserMappingStmt:
				_outCreateUserMappingStmt(str, obj);
				break;
			case T_AlterForeignServerStmt:
				_outAlterForeignServerStmt(str, obj);
				break;
			case T_CreateForeignServerStmt:
				_outCreateForeignServerStmt(str, obj);
				break;
			case T_AlterFdwStmt:
				_outAlterFdwStmt(str, obj);
				break;
			case T_CreateFdwStmt:
				_outCreateFdwStmt(str, obj);
				break;
			case T_GpPolicy:
				_outGpPolicy(str, obj);
				break;
			case T_DistributedBy:
				_outDistributedBy(str, obj);
				break;
			case T_AlterTableMoveAllStmt:
				_outAlterTableMoveAllStmt(str, obj);
				break;
			case T_AlterTableSpaceOptionsStmt:
				_outAlterTableSpaceOptionsStmt(str, obj);
				break;
			default:
				elog(ERROR, "could not serialize unrecognized node type: %d",
						 (int) nodeTag(obj));
				break;
		}
	}
}

/*
 * nodeToBinaryStringFast -
 *	   returns a binary representation of the Node as a palloc'd string
 */
char *
nodeToBinaryStringFast(void *obj, int *length)
{
	StringInfoData str;
	int16 tg = (int16) 0xDEAD;

	/* see stringinfo.h for an explanation of this maneuver */
	initStringInfoOfSize(&str, 4096);

	_outNode(&str, obj);

	/* Add something special at the end that we can check in readfast.c */
	appendBinaryStringInfo(&str, (const char *)&tg, sizeof(int16));

	*length = str.len;
	return str.data;
}
