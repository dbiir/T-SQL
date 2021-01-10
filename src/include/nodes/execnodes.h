/*-------------------------------------------------------------------------
 *
 * execnodes.h
 *	  definitions for executor state nodes
 *
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/execnodes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECNODES_H
#define EXECNODES_H

#include "access/genam.h"
#include "access/heapam.h"
#include "executor/instrument.h"
#include "nodes/params.h"
#include "nodes/plannodes.h"
#include "utils/reltrigger.h"
#include "utils/sortsupport.h"
#include "utils/tuplestore.h"
#include "nodes/parsenodes.h"

#include "gpmon/gpmon.h"                /* gpmon_packet_t */

#include "tdb/tdbkvam.h"

/*
 * partition selector ids start from 1. Sometimes we use 0 to initialize variables
 */
#define InvalidPartitionSelectorId  0

struct CdbExplain_ShowStatCtx;          /* private, in "cdb/cdbexplain.c" */
struct ChunkTransportState;             /* #include "cdb/cdbinterconnect.h" */
struct StringInfoData;                  /* #include "lib/stringinfo.h" */
struct MemTupleBinding;
struct MemTupleData;
struct HeapScanDescData;
struct FileScanDescData;
struct SliceTable;

/* ----------------
 *	  IndexInfo information
 *
 *		this struct holds the information needed to construct new index
 *		entries for a particular index.  Used for both index_build and
 *		retail creation of index entries.
 *
 *		NumIndexAttrs		number of columns in this index
 *		KeyAttrNumbers		underlying-rel attribute numbers used as keys
 *							(zeroes indicate expressions)
 *		Expressions			expr trees for expression entries, or NIL if none
 *		ExpressionsState	exec state for expressions, or NIL if none
 *		Predicate			partial-index predicate, or NIL if none
 *		PredicateState		exec state for predicate, or NIL if none
 *		ExclusionOps		Per-column exclusion operators, or NULL if none
 *		ExclusionProcs		Underlying function OIDs for ExclusionOps
 *		ExclusionStrats		Opclass strategy numbers for ExclusionOps
 *		Unique				is it a unique index?
 *		ReadyForInserts		is it valid for inserts?
 *		Concurrent			are we doing a concurrent index build?
 *		BrokenHotChain		did we detect any broken HOT chains?
 *
 * ii_Concurrent and ii_BrokenHotChain are used only during index build;
 * they're conventionally set to false otherwise.
 * ----------------
 */
typedef struct IndexInfo
{
	NodeTag		type;
	int			ii_NumIndexAttrs;
	AttrNumber	ii_KeyAttrNumbers[INDEX_MAX_KEYS];
	List	   *ii_Expressions; /* list of Expr */
	List	   *ii_ExpressionsState;	/* list of ExprState */
	List	   *ii_Predicate;	/* list of Expr */
	List	   *ii_PredicateState;		/* list of ExprState */
	Oid		   *ii_ExclusionOps;	/* array with one entry per column */
	Oid		   *ii_ExclusionProcs;		/* array with one entry per column */
	uint16	   *ii_ExclusionStrats;		/* array with one entry per column */
	bool		ii_Unique;
	bool		ii_ReadyForInserts;
	bool		ii_Concurrent;
	bool		ii_BrokenHotChain;
	Oid			ii_Am;
} IndexInfo;

/* ----------------
 *	  ExprContext_CB
 *
 *		List of callbacks to be called at ExprContext shutdown.
 * ----------------
 */
typedef void (*ExprContextCallbackFunction) (Datum arg);

typedef struct ExprContext_CB
{
	struct ExprContext_CB *next;
	ExprContextCallbackFunction function;
	Datum		arg;
} ExprContext_CB;

/* ----------------
 *	  ExprContext
 *
 *		This class holds the "current context" information
 *		needed to evaluate expressions for doing tuple qualifications
 *		and tuple projections.  For example, if an expression refers
 *		to an attribute in the current inner tuple then we need to know
 *		what the current inner tuple is and so we look at the expression
 *		context.
 *
 *	There are two memory contexts associated with an ExprContext:
 *	* ecxt_per_query_memory is a query-lifespan context, typically the same
 *	  context the ExprContext node itself is allocated in.  This context
 *	  can be used for purposes such as storing function call cache info.
 *	* ecxt_per_tuple_memory is a short-term context for expression results.
 *	  As the name suggests, it will typically be reset once per tuple,
 *	  before we begin to evaluate expressions for that tuple.  Each
 *	  ExprContext normally has its very own per-tuple memory context.
 *
 *	CurrentMemoryContext should be set to ecxt_per_tuple_memory before
 *	calling ExecEvalExpr() --- see ExecEvalExprSwitchContext().
 * ----------------
 */
typedef struct ExprContext
{
	NodeTag		type;

	/* Tuples that Var nodes in expression may refer to */
	TupleTableSlot *ecxt_scantuple;
	TupleTableSlot *ecxt_innertuple;
	TupleTableSlot *ecxt_outertuple;

	/* Memory contexts for expression evaluation --- see notes above */
	MemoryContext ecxt_per_query_memory;
	MemoryContext ecxt_per_tuple_memory;

	/* Values to substitute for Param nodes in expression */
	ParamExecData *ecxt_param_exec_vals;		/* for PARAM_EXEC params */
	ParamListInfo ecxt_param_list_info; /* for other param types */

	/*
	 * Values to substitute for Aggref nodes in the expressions of an Agg
	 * node, or for WindowFunc nodes within a WindowAgg node.
	 */
	Datum	   *ecxt_aggvalues; /* precomputed values for aggs/windowfuncs */
	bool	   *ecxt_aggnulls;	/* null flags for aggs/windowfuncs */

	/* Value to substitute for CaseTestExpr nodes in expression */
	Datum		caseValue_datum;
	bool		caseValue_isNull;

	/* Value to substitute for CoerceToDomainValue nodes in expression */
	Datum		domainValue_datum;
	bool		domainValue_isNull;

	/* Link to containing EState (NULL if a standalone ExprContext) */
	struct EState *ecxt_estate;

	/* Functions to call back when ExprContext is shut down or rescanned */
	ExprContext_CB *ecxt_callbacks;

	/* Representing the final grouping and group_id for a tuple
	 * in a grouping extension query. */
	uint64      grouping;
	uint32      group_id;
} ExprContext;

/*
 * Set-result status returned by ExecEvalExpr()
 */
typedef enum
{
	ExprSingleResult,			/* expression does not return a set */
	ExprMultipleResult,			/* this result is an element of a set */
	ExprEndResult				/* there are no more elements in the set */
} ExprDoneCond;

/*
 * Return modes for functions returning sets.  Note values must be chosen
 * as separate bits so that a bitmask can be formed to indicate supported
 * modes.  SFRM_Materialize_Random and SFRM_Materialize_Preferred are
 * auxiliary flags about SFRM_Materialize mode, rather than separate modes.
 */
typedef enum
{
	SFRM_ValuePerCall = 0x01,	/* one value returned per call */
	SFRM_Materialize = 0x02,	/* result set instantiated in Tuplestore */
	SFRM_Materialize_Random = 0x04,		/* Tuplestore needs randomAccess */
	SFRM_Materialize_Preferred = 0x08	/* caller prefers Tuplestore */
} SetFunctionReturnMode;

/*
 * When calling a function that might return a set (multiple rows),
 * a node of this type is passed as fcinfo->resultinfo to allow
 * return status to be passed back.  A function returning set should
 * raise an error if no such resultinfo is provided.
 */
typedef struct ReturnSetInfo
{
	NodeTag		type;
	/* values set by caller: */
	ExprContext *econtext;		/* context function is being called in */
	TupleDesc	expectedDesc;	/* tuple descriptor expected by caller */
	int			allowedModes;	/* bitmask: return modes caller can handle */
	/* result status from function (but pre-initialized by caller): */
	SetFunctionReturnMode returnMode;	/* actual return mode */
	ExprDoneCond isDone;		/* status for ValuePerCall mode */
	/* fields filled by function in Materialize return mode: */
	Tuplestorestate *setResult; /* holds the complete returned tuple set */
	TupleDesc	setDesc;		/* actual descriptor for returned tuples */
} ReturnSetInfo;

/* ----------------
 *		ProjectionInfo node information
 *
 *		This is all the information needed to perform projections ---
 *		that is, form new tuples by evaluation of targetlist expressions.
 *		Nodes which need to do projections create one of these.
 *
 *		ExecProject() evaluates the tlist, forms a tuple, and stores it
 *		in the given slot.  Note that the result will be a "virtual" tuple
 *		unless ExecMaterializeSlot() is then called to force it to be
 *		converted to a physical tuple.  The slot must have a tupledesc
 *		that matches the output of the tlist!
 *
 *		The planner very often produces tlists that consist entirely of
 *		simple Var references (lower levels of a plan tree almost always
 *		look like that).  And top-level tlists are often mostly Vars too.
 *		We therefore optimize execution of simple-Var tlist entries.
 *		The pi_targetlist list actually contains only the tlist entries that
 *		aren't simple Vars, while those that are Vars are processed using the
 *		varSlotOffsets/varNumbers/varOutputCols arrays.
 *
 *		The lastXXXVar fields are used to optimize fetching of fields from
 *		input tuples: they let us do a slot_getsomeattrs() call to ensure
 *		that all needed attributes are extracted in one pass.
 *
 *		targetlist		target list for projection (non-Var expressions only)
 *		exprContext		expression context in which to evaluate targetlist
 *		slot			slot to place projection result in
 *		itemIsDone		workspace array for ExecProject
 *		directMap		true if varOutputCols[] is an identity map
 *		numSimpleVars	number of simple Vars found in original tlist
 *		varSlotOffsets	array indicating which slot each simple Var is from
 *		varNumbers		array containing input attr numbers of simple Vars
 *		varOutputCols	array containing output attr numbers of simple Vars
 *		lastInnerVar	highest attnum from inner tuple slot (0 if none)
 *		lastOuterVar	highest attnum from outer tuple slot (0 if none)
 *		lastScanVar		highest attnum from scan tuple slot (0 if none)
 * ----------------
 */
typedef struct ProjectionInfo
{
	NodeTag		type;
	List	   *pi_targetlist;
	ExprContext *pi_exprContext;
	TupleTableSlot *pi_slot;
	ExprDoneCond *pi_itemIsDone;
	bool		pi_directMap;
	int			pi_numSimpleVars;
	int		   *pi_varSlotOffsets;
	int		   *pi_varNumbers;
	int		   *pi_varOutputCols;
	int			pi_lastInnerVar;
	int			pi_lastOuterVar;
	int			pi_lastScanVar;
} ProjectionInfo;

/* ----------------
 *	  JunkFilter
 *
 *	  This class is used to store information regarding junk attributes.
 *	  A junk attribute is an attribute in a tuple that is needed only for
 *	  storing intermediate information in the executor, and does not belong
 *	  in emitted tuples.  For example, when we do an UPDATE query,
 *	  the planner adds a "junk" entry to the targetlist so that the tuples
 *	  returned to ExecutePlan() contain an extra attribute: the ctid of
 *	  the tuple to be updated.  This is needed to do the update, but we
 *	  don't want the ctid to be part of the stored new tuple!  So, we
 *	  apply a "junk filter" to remove the junk attributes and form the
 *	  real output tuple.  The junkfilter code also provides routines to
 *	  extract the values of the junk attribute(s) from the input tuple.
 *
 *	  targetList:		the original target list (including junk attributes).
 *	  cleanTupType:		the tuple descriptor for the "clean" tuple (with
 *						junk attributes removed).
 *	  cleanMap:			A map with the correspondence between the non-junk
 *						attribute numbers of the "original" tuple and the
 *						attribute numbers of the "clean" tuple.
 *	  resultSlot:		tuple slot used to hold cleaned tuple.
 *	  junkAttNo:		not used by junkfilter code.  Can be used by caller
 *						to remember the attno of a specific junk attribute
 *						(nodeModifyTable.c keeps the "ctid" or "wholerow"
 *						attno here).
 * ----------------
 */
typedef struct JunkFilter
{
	NodeTag		type;
	List	   *jf_targetList;
	TupleDesc	jf_cleanTupType;
	AttrNumber *jf_cleanMap;
	TupleTableSlot *jf_resultSlot;
	AttrNumber	jf_junkAttNo;
} JunkFilter;

typedef void *RelationUpdateDesc;
typedef void *RelationDeleteDesc;

/* ----------------
 *	  ResultRelInfo information
 *
 *		Whenever we update an existing relation, we have to
 *		update indices on the relation, and perhaps also fire triggers.
 *		The ResultRelInfo class is used to hold all the information needed
 *		about a result relation, including indices.. -cim 10/15/89
 *
 *		RangeTableIndex			result relation's range table index
 *		RelationDesc			relation descriptor for result relation
 *		NumIndices				# of indices existing on result relation
 *		IndexRelationDescs		array of relation descriptors for indices
 *		IndexRelationInfo		array of key/attr info for indices
 *		TrigDesc				triggers to be fired, if any
 *		TrigFunctions			cached lookup info for trigger functions
 *		TrigWhenExprs			array of trigger WHEN expr states
 *		TrigInstrument			optional runtime measurements for triggers
 *		FdwRoutine				FDW callback functions, if foreign table
 *		FdwState				available to save private state of FDW
 *		WithCheckOptions		list of WithCheckOption's for views
 *		WithCheckOptionExprs	list of WithCheckOption expr states
 *		ConstraintExprs			array of constraint-checking expr states
 *		junkFilter				for removing junk attributes from tuples
 *		projectReturning		for computing a RETURNING list
 *		tupdesc_match			???
 *		mt_bind					???
 *		aoInsertDesc			context for appendonly relation buffered INSERT.
 *		aoDeleteDesc			context for appendonly relation buffered DELETE.
 *		ao_segno				the AO segfile we inserted into.
 *		extinsertDesc			???
 *		aosegno					???
 *		aoprocessed				???
 *		partInsertMap			map input attrno to target attrno
 *		partSlot				TupleTableSlot for the target part relation
 *		resultSlot          	TupleTableSlot for the target relation
 * ----------------
 */
typedef struct ResultRelInfo
{
	NodeTag		type;
	Index		ri_RangeTableIndex;
	Relation	ri_RelationDesc;
	int			ri_NumIndices;
	RelationPtr ri_IndexRelationDescs;
	IndexInfo **ri_IndexRelationInfo;
	TriggerDesc *ri_TrigDesc;
	FmgrInfo   *ri_TrigFunctions;
	List	  **ri_TrigWhenExprs;
	Instrumentation *ri_TrigInstrument;
	struct FdwRoutine *ri_FdwRoutine;
	void	   *ri_FdwState;
	List	   *ri_WithCheckOptions;
	List	   *ri_WithCheckOptionExprs;
	List	  **ri_ConstraintExprs;
	JunkFilter *ri_junkFilter;
	AttrNumber  ri_segid_attno; /* gpdb: attribute number of "gp_segment_id" */
	ProjectionInfo *ri_projectReturning;
	int			tupdesc_match;
	struct MemTupleBinding *mt_bind;

	struct AppendOnlyInsertDescData *ri_aoInsertDesc;
	struct AOCSInsertDescData *ri_aocsInsertDesc;
	struct ExternalInsertDescData *ri_extInsertDesc;
	struct KVEngineInsertDescData *ri_kvInsertDesc;

	RelationDeleteDesc ri_deleteDesc;
	RelationUpdateDesc ri_updateDesc;

	int			ri_aosegno;
	uint64		ri_aoprocessed; /* tuples added/deleted for AO */
	struct AttrMap *ri_partInsertMap;
	TupleTableSlot *ri_resultSlot;
	/* Parent relation in checkPartitionUpdate */
	Relation	ri_PartitionParent;
	/* tupdesc_match for checkPartitionUpdate */
	int			ri_PartCheckTupDescMatch;
	/* Attribute map in checkPartitionUpdate */
	struct AttrMap *ri_PartCheckMap;

	/*
	 * Hash table of sub-ResultRelInfos, one for each active partition.
	 * Keyed by partition's OID.
	 */
	HTAB	   *ri_partition_hash;

	/*
	 * cdb: CopyFrom() buffers the tuples and flush them all together
	 * in CopyFromInsertBatch()
	 * For partition table, each child partition needs a buffer array
	 * to hold the tuples.
	 */
	BulkInsertState biState;
	int			nBufferedTuples;
	HeapTuple	*bufferedTuples;
	Size		bufferedTuplesSize;
} ResultRelInfo;

typedef struct ShareNodeEntry
{
	NodeTag		type;

	Node	   *sharePlan;
	Node	   *shareState;
	int			refcount; /* reference count to guard from too-eager-free risk */
} ShareNodeEntry;

/*
 * PartitionAccessMethods
 *    Defines the lookup access methods for partitions, one for each level.
 */
typedef struct PartitionAccessMethods
{
	/* Number of partition levels */
	int			partLevels;

	/* Access methods, one for each level */
	void	  **amstate;

	/* Memory context for access methods */
	MemoryContext part_cxt;
} PartitionAccessMethods;

typedef struct PartitionState
{
	NodeTag		type;

	AttrNumber	max_partition_attr;
	PartitionAccessMethods *accessMethods;
} PartitionState;

/*
 * PartitionMetadata
 *   Defines the metadata for partitions.
 */
typedef struct PartitionMetadata
{
	PartitionNode *partsAndRules;
	PartitionAccessMethods *accessMethods;
} PartitionMetadata;

/*
 * PartOidEntry
 *   Defines an entry in the shared partOid hash table.
 */
typedef struct PartOidEntry
{
	/* oid of an individual leaf partition */
	Oid			partOid;

	/* list of partition selectors that produced the above part oid */
	List	   *selectorList;
} PartOidEntry;

/*
 * DynamicTableScanInfo
 *   Encapsulate the information that is needed to maintain the pid indexes
 * for all dynamic table scans in a plan.
 */
typedef struct DynamicTableScanInfo
{
	/*
	 * The total number of unique dynamic table scans in the plan.
	 */
	int			numScans;

	/*
	 * List containing the number of partition selectors for every scan id.
	 * Element #i in the list corresponds to scan id i
	 */
	List	   *numSelectorsPerScanId;

	/*
	 * An array of pid indexes, one for each unique dynamic table scans.
	 * Each of these pid indexes maintains unique pids that are involved
	 * in the scan.
	 */
	HTAB	  **pidIndexes;

	/*
	 * An array of Oids, for the current partition being scanned in each
	 *  dynamic scan. (XXX: Currently only used to pass the oid from
	 * DynamicBitmapHeapScan to DynamicBitmapIndexScans.
	 */
	Oid		   *curRelOids;

	/*
	 * Partitioning metadata for all relevant partition tables.
	 */
	List	   *partsMetadata;
} DynamicTableScanInfo;

/*
 * Number of pids used when initializing the pid-index hash table for each dynamic
 * table scan.
 */
#define INITIAL_NUM_PIDS 1000

/*
 * The initial estimate size for dynamic table scan pid-index array, and the
 * default incremental number when the array is out of space.
 */
#define NUM_PID_INDEXES_ADDED 10

/* ----------------
 *	  EState information
 *
 * Master working state for an Executor invocation
 * ----------------
 */
typedef struct EState
{
	NodeTag		type;

	/* Basic state for all query types: */
	ScanDirection es_direction; /* current scan direction */
	Snapshot	es_snapshot;	/* time qual to use */
	Snapshot	es_crosscheck_snapshot; /* crosscheck time qual for RI */
	List	   *es_range_table; /* List of RangeTblEntry */
	PlannedStmt *es_plannedstmt;	/* link to top of plan tree */

	JunkFilter *es_junkFilter;	/* top-level junk filter, if any */

	/* If query can insert/delete tuples, the command ID to mark them with */
	CommandId	es_output_cid;

	/* Info about target table(s) for insert/update/delete queries: */
	ResultRelInfo *es_result_relations; /* array of ResultRelInfos */
	int			es_num_result_relations;		/* length of array */
	ResultRelInfo *es_result_relation_info;		/* currently active array elt */

	/* Stuff used for firing triggers: */
	List	   *es_trig_target_relations;		/* trigger-only ResultRelInfos */

	/* partitioning info for target relation */
	PartitionNode *es_result_partitions;

	/* AO fileseg info for target relation */
	List	   *es_result_aosegnos;

	TupleTableSlot *es_trig_tuple_slot; /* for trigger output tuples */
	TupleTableSlot *es_trig_oldtup_slot;		/* for TriggerEnabled */
	TupleTableSlot *es_trig_newtup_slot;		/* for TriggerEnabled */

	/* Parameter info: */
	ParamListInfo es_param_list_info;	/* values of external params */
	ParamExecData *es_param_exec_vals;	/* values of internal params */

	/* Other working state: */
	MemoryContext es_query_cxt; /* per-query context in which EState lives */

	List	   *es_tupleTable;	/* List of TupleTableSlots */

	List	   *es_rowMarks;	/* List of ExecRowMarks */

	uint64		es_processed;	/* # of tuples processed */
	Oid			es_lastoid;		/* last oid processed (by INSERT) */

	int			es_top_eflags;	/* eflags passed to ExecutorStart */
	int			es_instrument;	/* OR of InstrumentOption flags */
	bool		es_finished;	/* true when ExecutorFinish is done */

	List	   *es_exprcontexts;	/* List of ExprContexts within EState */

	List	   *es_subplanstates;		/* List of PlanState for SubPlans */

	List	   *es_auxmodifytables;		/* List of secondary ModifyTableStates */

	/*
	 * this ExprContext is for per-output-tuple operations, such as constraint
	 * checks and index-value computations.  It will be reset for each output
	 * tuple.  Note that it will be created only if needed.
	 */
	ExprContext *es_per_tuple_exprcontext;

	/*
	 * These fields are for re-evaluating plan quals when an updated tuple is
	 * substituted in READ COMMITTED mode.  es_epqTuple[] contains tuples that
	 * scan plan nodes should return instead of whatever they'd normally
	 * return, or NULL if nothing to return; es_epqTupleSet[] is true if a
	 * particular array entry is valid; and es_epqScanDone[] is state to
	 * remember if the tuple has been returned already.  Arrays are of size
	 * list_length(es_range_table) and are indexed by scan node scanrelid - 1.
	 */
	HeapTuple  *es_epqTuple;	/* array of EPQ substitute tuples */
	bool	   *es_epqTupleSet; /* true if EPQ tuple is provided */
	bool	   *es_epqScanDone; /* true if EPQ tuple has been fetched */

	/* Additions for MPP plan slicing. */
	struct SliceTable *es_sliceTable;

	/* Current positions of cursors used in CURRENT OF expressions */
	List	   *es_cursorPositions;

	/* Data structure for node sharing */
	List	  **es_sharenode;

	int			active_recv_id;
	void	   *motionlayer_context;  /* Motion Layer state */
	struct ChunkTransportState *interconnect_context; /* Interconnect state */

	/* MPP used resources */
	bool		es_interconnect_is_setup;   /* is interconnect set-up?    */

	bool		es_got_eos;			/* was end-of-stream recieved? */

	bool		cancelUnfinished;	/* when we're cleaning up, we need to make sure that we know it */

	/* results from qExec processes */
	struct CdbDispatcherState *dispatcherState;

	/* CDB: EXPLAIN ANALYZE statistics */
	struct CdbExplain_ShowStatCtx  *showstatctx;

	/* CDB: partitioning state info */
	PartitionState *es_partition_state;

	/*
	 * The slice number for the current node that is
	 * being processed. During the tree traversal,
	 * this value is set by Motion and InitPlan nodes.
	 *
	 * currentSliceIdInPlan and currentExecutingSliceId
	 * are basically the same, except for InitPlan nodes.
	 * For InitPlan nodes, the nodes in the top slice have
	 * an assigned slice id in the plan, while the executing
	 * slice id for these nodes is the root slice id.
	 */
	int			currentSliceIdInPlan;
	int			currentExecutingSliceId;

	/*
	 * This is >0, if we're processing a subplan.
	 * This is used to determine whether we could eager free
	 * the Material node on top of Broadcast inside a subplan
	 * (for supporting correlated subqueries). The Material
	 * node can be eager-free'ed only when this value is 0.
	 */
	int			currentSubplanLevel;

	/*
	 * The root slice id for this EState.
	 */
	int			rootSliceId;

	/*
	 * Information relevant to dynamic table scans.
	 */
	DynamicTableScanInfo *dynamicTableScanInfo;

	/* Should the executor skip past the alien plan nodes */
	bool eliminateAliens;
} EState;

struct PlanState;
struct MotionState;

extern struct MotionState *getMotionState(struct PlanState *ps, int sliceIndex);
extern int LocallyExecutingSliceIndex(EState *estate);
extern int PrimaryWriterSliceIndex(EState *estate);
extern int RootSliceIndex(EState *estate);
#ifdef USE_ASSERT_CHECKING
extern void SliceLeafMotionStateAreValid(struct MotionState *ms);
#endif

/*
 * ExecRowMark -
 *	   runtime representation of FOR [KEY] UPDATE/SHARE clauses
 *
 * When doing UPDATE, DELETE, or SELECT FOR [KEY] UPDATE/SHARE, we should have an
 * ExecRowMark for each non-target relation in the query (except inheritance
 * parent RTEs, which can be ignored at runtime).  See PlanRowMark for details
 * about most of the fields.  In addition to fields directly derived from
 * PlanRowMark, we store curCtid, which is used by the WHERE CURRENT OF code.
 *
 * EState->es_rowMarks is a list of these structs.
 */
typedef struct ExecRowMark
{
	Relation	relation;		/* opened and suitably locked relation */
	Index		rti;			/* its range table index */
	Index		prti;			/* parent range table index, if child */
	Index		rowmarkId;		/* unique identifier for resjunk columns */
	RowMarkType markType;		/* see enum in nodes/plannodes.h */
	bool		noWait;			/* NOWAIT option */
	ItemPointerData curCtid;	/* ctid of currently locked tuple, if any */
} ExecRowMark;

/*
 * ExecAuxRowMark -
 *	   additional runtime representation of FOR [KEY] UPDATE/SHARE clauses
 *
 * Each LockRows and ModifyTable node keeps a list of the rowmarks it needs to
 * deal with.  In addition to a pointer to the related entry in es_rowMarks,
 * this struct carries the column number(s) of the resjunk columns associated
 * with the rowmark (see comments for PlanRowMark for more detail).  In the
 * case of ModifyTable, there has to be a separate ExecAuxRowMark list for
 * each child plan, because the resjunk columns could be at different physical
 * column positions in different subplans.
 */
typedef struct ExecAuxRowMark
{
	ExecRowMark *rowmark;		/* related entry in es_rowMarks */
	AttrNumber	ctidAttNo;		/* resno of ctid junk attribute, if any */
	AttrNumber	toidAttNo;		/* resno of tableoid junk attribute, if any */
	AttrNumber	wholeAttNo;		/* resno of whole-row junk attribute, if any */
} ExecAuxRowMark;


/* ----------------------------------------------------------------
 *				 Tuple Hash Tables
 *
 * All-in-memory tuple hash tables are used for a number of purposes.
 *
 * Note: tab_hash_funcs are for the key datatype(s) stored in the table,
 * and tab_eq_funcs are non-cross-type equality operators for those types.
 * Normally these are the only functions used, but FindTupleHashEntry()
 * supports searching a hashtable using cross-data-type hashing.  For that,
 * the caller must supply hash functions for the LHS datatype as well as
 * the cross-type equality operators to use.  in_hash_funcs and cur_eq_funcs
 * are set to point to the caller's function arrays while doing such a search.
 * During LookupTupleHashEntry(), they point to tab_hash_funcs and
 * tab_eq_funcs respectively.
 * ----------------------------------------------------------------
 */
typedef struct TupleHashEntryData *TupleHashEntry;
typedef struct TupleHashTableData *TupleHashTable;

typedef struct TupleHashEntryData
{
	/* firstTuple must be the first field in this struct! */
	struct MemTupleData *firstTuple;	/* copy of first tuple in this group */
	/* there may be additional data beyond the end of this struct */
} TupleHashEntryData;			/* VARIABLE LENGTH STRUCT */

typedef struct TupleHashTableData
{
	HTAB	   *hashtab;		/* underlying dynahash table */
	int			numCols;		/* number of columns in lookup key */
	AttrNumber *keyColIdx;		/* attr numbers of key columns */
	FmgrInfo   *tab_hash_funcs; /* hash functions for table datatype(s) */
	FmgrInfo   *tab_eq_funcs;	/* equality functions for table datatype(s) */
	MemoryContext tablecxt;		/* memory context containing table */
	MemoryContext tempcxt;		/* context for function evaluations */
	Size		entrysize;		/* actual size to make each hash entry */
	TupleTableSlot *tableslot;	/* slot for referencing table entries */
	/* The following fields are set transiently for each table search: */
	TupleTableSlot *inputslot;	/* current input tuple's slot */
	FmgrInfo   *in_hash_funcs;	/* hash functions for input datatype(s) */
	FmgrInfo   *cur_eq_funcs;	/* equality functions for input vs. table */
}	TupleHashTableData;

typedef HASH_SEQ_STATUS TupleHashIterator;

/*
 * Use InitTupleHashIterator/TermTupleHashIterator for a read/write scan.
 * Use ResetTupleHashIterator if the table can be frozen (in this case no
 * explicit scan termination is needed).
 */
#define InitTupleHashIterator(htable, iter) \
	hash_seq_init(iter, (htable)->hashtab)
#define TermTupleHashIterator(iter) \
	hash_seq_term(iter)
#define ResetTupleHashIterator(htable, iter) \
	do { \
		hash_freeze((htable)->hashtab); \
		hash_seq_init(iter, (htable)->hashtab); \
	} while (0)
#define ScanTupleHashTable(iter) \
	((TupleHashEntry) hash_seq_search(iter))

/* Abstraction of different memory management calls */
typedef struct MemoryManagerContainer
{
	void *manager; /* memory manager instance */
	void *(*alloc)(void *manager, Size len);
	void (*free)(void *manager, void *pointer);
	/*
	 * If existing space is too small, the realloced space is how many
	 * times of the existing one.
	 */
	int realloc_ratio;
} MemoryManagerContainer;

/* ----------------------------------------------------------------
 *				 Expression State Trees
 *
 * Each executable expression tree has a parallel ExprState tree.
 *
 * Unlike PlanState, there is not an exact one-for-one correspondence between
 * ExprState node types and Expr node types.  Many Expr node types have no
 * need for node-type-specific run-time state, and so they can use plain
 * ExprState or GenericExprState as their associated ExprState node type.
 * ----------------------------------------------------------------
 */

/* ----------------
 *		ExprState node
 *
 * ExprState is the common superclass for all ExprState-type nodes.
 *
 * It can also be instantiated directly for leaf Expr nodes that need no
 * local run-time state (such as Var, Const, or Param).
 *
 * To save on dispatch overhead, each ExprState node contains a function
 * pointer to the routine to execute to evaluate the node.
 * ----------------
 */

typedef struct ExprState ExprState;

typedef Datum (*ExprStateEvalFunc) (ExprState *expression,
												ExprContext *econtext,
												bool *isNull,
												ExprDoneCond *isDone);

struct ExprState
{
	NodeTag		type;
	Expr	   *expr;			/* associated Expr node */
	ExprStateEvalFunc evalfunc; /* routine to run to execute node */
};

/* ----------------
 *		GenericExprState node
 *
 * This is used for Expr node types that need no local run-time state,
 * but have one child Expr node.
 * ----------------
 */
typedef struct GenericExprState
{
	ExprState	xprstate;
	ExprState  *arg;			/* state of my child node */
} GenericExprState;

/* ----------------
 *		WholeRowVarExprState node
 * ----------------
 */
typedef struct WholeRowVarExprState
{
	ExprState	xprstate;
	struct PlanState *parent;	/* parent PlanState, or NULL if none */
	TupleDesc	wrv_tupdesc;	/* descriptor for resulting tuples */
	JunkFilter *wrv_junkFilter; /* JunkFilter to remove resjunk cols */
} WholeRowVarExprState;

/* ----------------
 *		AggrefExprState node
 * ----------------
 */
typedef struct AggrefExprState
{
	ExprState	xprstate;
	List	   *aggdirectargs;	/* states of direct-argument expressions */
	List	   *args;			/* states of aggregated-argument expressions */
	ExprState  *aggfilter;		/* state of FILTER expression, if any */
	int			aggno;			/* ID number for agg within its plan node */
} AggrefExprState;

/*
 * ----------------
 *		GroupingFuncExprState node
 * ----------------
 */
typedef struct GroupingFuncExprState
{
	ExprState  xprstate;
	List          *args;
	int        ngrpcols;   /* number of unique grouping attributes */
} GroupingFuncExprState;

/* ----------------
 *		WindowFuncExprState node
 * ----------------
 */
typedef struct WindowFuncExprState
{
	ExprState	xprstate;
	List	   *args;			/* states of argument expressions */
	ExprState  *aggfilter;		/* FILTER expression */
	int			wfuncno;		/* ID number for wfunc within its plan node */
} WindowFuncExprState;

/* ----------------
 *		ArrayRefExprState node
 *
 * Note: array types can be fixed-length (typlen > 0), but only when the
 * element type is itself fixed-length.  Otherwise they are varlena structures
 * and have typlen = -1.  In any case, an array type is never pass-by-value.
 * ----------------
 */
typedef struct ArrayRefExprState
{
	ExprState	xprstate;
	List	   *refupperindexpr;	/* states for child nodes */
	List	   *reflowerindexpr;
	ExprState  *refexpr;
	ExprState  *refassgnexpr;
	int16		refattrlength;	/* typlen of array type */
	int16		refelemlength;	/* typlen of the array element type */
	bool		refelembyval;	/* is the element type pass-by-value? */
	char		refelemalign;	/* typalign of the element type */
} ArrayRefExprState;

/* ----------------
 *		FuncExprState node
 *
 * Although named for FuncExpr, this is also used for OpExpr, DistinctExpr,
 * and NullIf nodes; be careful to check what xprstate.expr is actually
 * pointing at!
 * ----------------
 */
typedef struct FuncExprState
{
	ExprState	xprstate;
	List	   *args;			/* states of argument expressions */

	/*
	 * Function manager's lookup info for the target function.  If func.fn_oid
	 * is InvalidOid, we haven't initialized it yet (nor any of the following
	 * fields).
	 */
	FmgrInfo	func;

	/*
	 * For a set-returning function (SRF) that returns a tuplestore, we keep
	 * the tuplestore here and dole out the result rows one at a time. The
	 * slot holds the row currently being returned.
	 */
	Tuplestorestate *funcResultStore;
	TupleTableSlot *funcResultSlot;

	/*
	 * In some cases we need to compute a tuple descriptor for the function's
	 * output.  If so, it's stored here.
	 */
	TupleDesc	funcResultDesc;
	bool		funcReturnsTuple;		/* valid when funcResultDesc isn't
										 * NULL */

	/*
	 * setArgsValid is true when we are evaluating a set-returning function
	 * that uses value-per-call mode and we are in the middle of a call
	 * series; we want to pass the same argument values to the function again
	 * (and again, until it returns ExprEndResult).  This indicates that
	 * fcinfo_data already contains valid argument data.
	 */
	bool		setArgsValid;

	/*
	 * Flag to remember whether we found a set-valued argument to the
	 * function. This causes the function result to be a set as well. Valid
	 * only when setArgsValid is true or funcResultStore isn't NULL.
	 */
	bool		setHasSetArg;	/* some argument returns a set */

	/*
	 * Flag to remember whether we have registered a shutdown callback for
	 * this FuncExprState.  We do so only if funcResultStore or setArgsValid
	 * has been set at least once (since all the callback is for is to release
	 * the tuplestore or clear setArgsValid).
	 */
	bool		shutdown_reg;	/* a shutdown callback is registered */

	/*
	 * Call parameter structure for the function.  This has been initialized
	 * (by InitFunctionCallInfoData) if func.fn_oid is valid.  It also saves
	 * argument values between calls, when setArgsValid is true.
	 */
	FunctionCallInfoData fcinfo_data;

	/* Fast Path */
	ExprState  *fp_arg[2];
	Datum		fp_datum[2];
	bool		fp_null[2];
} FuncExprState;

/* ----------------
 *		ScalarArrayOpExprState node
 *
 * This is a FuncExprState plus some additional data.
 * ----------------
 */
typedef struct ScalarArrayOpExprState
{
	FuncExprState fxprstate;
	/* Cached info about array element type */
	Oid			element_type;
	int16		typlen;
	bool		typbyval;
	char		typalign;

	/* Fast path x in ('A', 'B', 'C') */
	int			fp_n;
	int		   *fp_len;
	Datum	   *fp_datum;
} ScalarArrayOpExprState;

/* ----------------
 *		BoolExprState node
 * ----------------
 */
typedef struct BoolExprState
{
	ExprState	xprstate;
	List	   *args;			/* states of argument expression(s) */
} BoolExprState;

/* ----------------
 *		PartSelectedExprState node
 * ----------------
 */
typedef struct PartSelectedExprState
{
	ExprState	xprstate;
} PartSelectedExprState;

/* ----------------
 *		PartDefaultExprState node
 * ----------------
 */
typedef struct PartDefaultExprState
{
	ExprState	xprstate;

	/* PartitionSelectorState where expression evaluator can look for rules */
	struct PartitionSelectorState *selector;
} PartDefaultExprState;

/* ----------------
 *		PartBoundExprState node
 * ----------------
 */
typedef struct PartBoundExprState
{
	ExprState	xprstate;

	/* PartitionSelectorState where expression evaluator can look for rules */
	struct PartitionSelectorState *selector;
} PartBoundExprState;

/* ----------------
 *		PartBoundInclusionExprState node
 * ----------------
 */
typedef struct PartBoundInclusionExprState
{
	ExprState	xprstate;

	/* PartitionSelectorState where expression evaluator can look for rules */
	struct PartitionSelectorState *selector;
} PartBoundInclusionExprState;

/* ----------------
 *		PartBoundOpenExprState node
 * ----------------
 */
typedef struct PartBoundOpenExprState
{
	ExprState	xprstate;

	/* PartitionSelectorState where expression evaluator can look for rules */
	struct PartitionSelectorState *selector;
} PartBoundOpenExprState;

/* ----------------
 *		PartListRuleExprState node
 * ----------------
 */
typedef struct PartListRuleExprState
{
	ExprState	xprstate;

	/* PartitionSelectorState where expression evaluator can look for rules */
	struct PartitionSelectorState *selector;
} PartListRuleExprState;

/* ----------------
 *		PartListNullTestExprState node
 * ----------------
 */
typedef struct PartListNullTestExprState
{
	ExprState	xprstate;

	/* PartitionSelectorState where expression evaluator can look for rules */
	struct PartitionSelectorState *selector;
} PartListNullTestExprState;

/* ----------------
 *		SubPlanState node
 * ----------------
 */
typedef struct SubPlanState
{
	ExprState	xprstate;
	struct PlanState *planstate;	/* subselect plan's state tree */
	ExprState  *testexpr;		/* state of combining expression */
	List	   *args;			/* states of argument expression(s) */
	struct MemTupleData *curTuple; /* copy of most recent tuple from subplan */
	Datum		curArray;		/* most recent array from ARRAY() subplan */
	/* these are used when hashing the subselect's output: */
	ProjectionInfo *projLeft;	/* for projecting lefthand exprs */
	ProjectionInfo *projRight;	/* for projecting subselect output */
	TupleHashTable hashtable;	/* hash table for no-nulls subselect rows */
	TupleHashTable hashnulls;	/* hash table for rows with null(s) */
	bool		havehashrows;	/* TRUE if hashtable is not empty */
	bool		havenullrows;	/* TRUE if hashnulls is not empty */
	MemoryContext hashtablecxt; /* memory context containing hash tables */
	MemoryContext hashtempcxt;	/* temp memory context for hash tables */
	ExprContext *innerecontext; /* econtext for computing inner tuples */
	AttrNumber *keyColIdx;		/* control data for hash tables */
	FmgrInfo   *tab_hash_funcs; /* hash functions for table datatype(s) */
	FmgrInfo   *tab_eq_funcs;	/* equality functions for table datatype(s) */
	FmgrInfo   *lhs_hash_funcs; /* hash functions for lefthand datatype(s) */
	FmgrInfo   *cur_eq_funcs;	/* equality functions for LHS vs. table */
} SubPlanState;

/* ----------------
 *		AlternativeSubPlanState node
 * ----------------
 */
typedef struct AlternativeSubPlanState
{
	ExprState	xprstate;
	List	   *subplans;		/* states of alternative subplans */
	int			active;			/* list index of the one we're using */
} AlternativeSubPlanState;

/* ----------------
 *		FieldSelectState node
 * ----------------
 */
typedef struct FieldSelectState
{
	ExprState	xprstate;
	ExprState  *arg;			/* input expression */
	TupleDesc	argdesc;		/* tupdesc for most recent input */
} FieldSelectState;

/* ----------------
 *		FieldStoreState node
 * ----------------
 */
typedef struct FieldStoreState
{
	ExprState	xprstate;
	ExprState  *arg;			/* input tuple value */
	List	   *newvals;		/* new value(s) for field(s) */
	TupleDesc	argdesc;		/* tupdesc for most recent input */
} FieldStoreState;

/* ----------------
 *		CoerceViaIOState node
 * ----------------
 */
typedef struct CoerceViaIOState
{
	ExprState	xprstate;
	ExprState  *arg;			/* input expression */
	FmgrInfo	outfunc;		/* lookup info for source output function */
	FmgrInfo	infunc;			/* lookup info for result input function */
	Oid			intypioparam;	/* argument needed for input function */
} CoerceViaIOState;

/* ----------------
 *		ArrayCoerceExprState node
 * ----------------
 */
typedef struct ArrayCoerceExprState
{
	ExprState	xprstate;
	ExprState  *arg;			/* input array value */
	Oid			resultelemtype; /* element type of result array */
	FmgrInfo	elemfunc;		/* lookup info for element coercion function */
	/* use struct pointer to avoid including array.h here */
	struct ArrayMapState *amstate;		/* workspace for array_map */
} ArrayCoerceExprState;

/* ----------------
 *		ConvertRowtypeExprState node
 * ----------------
 */
typedef struct ConvertRowtypeExprState
{
	ExprState	xprstate;
	ExprState  *arg;			/* input tuple value */
	TupleDesc	indesc;			/* tupdesc for source rowtype */
	TupleDesc	outdesc;		/* tupdesc for result rowtype */
	/* use "struct" so we needn't include tupconvert.h here */
	struct TupleConversionMap *map;
	bool		initialized;
} ConvertRowtypeExprState;

/* ----------------
 *		CaseExprState node
 * ----------------
 */
typedef struct CaseExprState
{
	ExprState	xprstate;
	ExprState  *arg;			/* implicit equality comparison argument */
	List	   *args;			/* the arguments (list of WHEN clauses) */
	ExprState  *defresult;		/* the default result (ELSE clause) */
} CaseExprState;

/* ----------------
 *		CaseWhenState node
 * ----------------
 */
typedef struct CaseWhenState
{
	ExprState	xprstate;
	ExprState  *expr;			/* condition expression */
	ExprState  *result;			/* substitution result */
} CaseWhenState;

/* ----------------
 *		ArrayExprState node
 *
 * Note: ARRAY[] expressions always produce varlena arrays, never fixed-length
 * arrays.
 * ----------------
 */
typedef struct ArrayExprState
{
	ExprState	xprstate;
	List	   *elements;		/* states for child nodes */
	int16		elemlength;		/* typlen of the array element type */
	bool		elembyval;		/* is the element type pass-by-value? */
	char		elemalign;		/* typalign of the element type */
} ArrayExprState;

/* ----------------
 *		RowExprState node
 * ----------------
 */
typedef struct RowExprState
{
	ExprState	xprstate;
	List	   *args;			/* the arguments */
	TupleDesc	tupdesc;		/* descriptor for result tuples */
} RowExprState;

/* ----------------
 *		RowCompareExprState node
 * ----------------
 */
typedef struct RowCompareExprState
{
	ExprState	xprstate;
	List	   *largs;			/* the left-hand input arguments */
	List	   *rargs;			/* the right-hand input arguments */
	FmgrInfo   *funcs;			/* array of comparison function info */
	Oid		   *collations;		/* array of collations to use */
} RowCompareExprState;

/* ----------------
 *		CoalesceExprState node
 * ----------------
 */
typedef struct CoalesceExprState
{
	ExprState	xprstate;
	List	   *args;			/* the arguments */
} CoalesceExprState;

/* ----------------
 *		MinMaxExprState node
 * ----------------
 */
typedef struct MinMaxExprState
{
	ExprState	xprstate;
	List	   *args;			/* the arguments */
	FmgrInfo	cfunc;			/* lookup info for comparison func */
} MinMaxExprState;

/* ----------------
 *		XmlExprState node
 * ----------------
 */
typedef struct XmlExprState
{
	ExprState	xprstate;
	List	   *named_args;		/* ExprStates for named arguments */
	List	   *args;			/* ExprStates for other arguments */
} XmlExprState;

/* ----------------
 *		NullTestState node
 * ----------------
 */
typedef struct NullTestState
{
	ExprState	xprstate;
	ExprState  *arg;			/* input expression */
	/* used only if input is of composite type: */
	TupleDesc	argdesc;		/* tupdesc for most recent input */
} NullTestState;

/* ----------------
 *		CoerceToDomainState node
 * ----------------
 */
typedef struct CoerceToDomainState
{
	ExprState	xprstate;
	ExprState  *arg;			/* input expression */
	/* Cached list of constraints that need to be checked */
	List	   *constraints;	/* list of DomainConstraintState nodes */
} CoerceToDomainState;

/*
 * DomainConstraintState - one item to check during CoerceToDomain
 *
 * Note: this is just a Node, and not an ExprState, because it has no
 * corresponding Expr to link to.  Nonetheless it is part of an ExprState
 * tree, so we give it a name following the xxxState convention.
 */
typedef enum DomainConstraintType
{
	DOM_CONSTRAINT_NOTNULL,
	DOM_CONSTRAINT_CHECK
} DomainConstraintType;

typedef struct DomainConstraintState
{
	NodeTag		type;
	DomainConstraintType constrainttype;		/* constraint type */
	char	   *name;			/* name of constraint (for error msgs) */
	ExprState  *check_expr;		/* for CHECK, a boolean expression */
} DomainConstraintState;


/* ----------------------------------------------------------------
 *				 Executor State Trees
 *
 * An executing query has a PlanState tree paralleling the Plan tree
 * that describes the plan.
 * ----------------------------------------------------------------
 */

/* ----------------
 *		PlanState node
 *
 * We never actually instantiate any PlanState nodes; this is just the common
 * abstract superclass for all PlanState-type nodes.
 * ----------------
 */
typedef struct PlanState
{
	NodeTag		type;

	Plan	   *plan;			/* associated Plan node */

	EState	   *state;			/* at execution time, states of individual
								 * nodes point to one EState for the whole
								 * top-level plan */

	bool		fHadSentGpmon;

	/*
	 * Common structural data for all Plan types.  These links to subsidiary
	 * state trees parallel links in the associated plan tree (except for the
	 * subPlan list, which does not exist in the plan tree).
	 */
	List	   *targetlist;		/* target list to be computed at this node */
	List	   *qual;			/* implicitly-ANDed qual conditions */
	struct PlanState *lefttree; /* input plan tree(s) */
	struct PlanState *righttree;
	List	   *initPlan;		/* Init SubPlanState nodes (un-correlated expr
								 * subselects) */
	List	   *subPlan;		/* SubPlanState nodes in my expressions */

	/*
	 * State for management of parameter-change-driven rescanning
	 */
	Bitmapset  *chgParam;		/* set of IDs of changed Params */

	/*
	 * Other run-time state needed by most if not all node types.
	 */
	TupleTableSlot *ps_ResultTupleSlot; /* slot for my result tuples */
	ExprContext *ps_ExprContext;	/* node's expression-evaluation context */
	ProjectionInfo *ps_ProjInfo;	/* info for doing tuple projection */

	/*
	 * EXPLAIN ANALYZE statistics collection
	 */
	Instrumentation *instrument;     /* runtime stats for this node */
	struct StringInfoData  *cdbexplainbuf;  /* EXPLAIN ANALYZE report buf */
	void      (*cdbexplainfun)(struct PlanState *planstate, struct StringInfoData *buf);
	/* callback before ExecutorEnd */

	/*
	 * GpMon packet
	 */
	int		gpmon_plan_tick;
	gpmon_packet_t gpmon_pkt;
	bool		fHadSentNodeStart;

	bool		squelched;		/* has ExecSquelchNode() been called already? */

	/* MemoryAccount to use for recording the memory usage of different plan nodes. */
	MemoryAccountIdType memoryAccountId;
} PlanState;

/* Gpperfmon helper functions defined in execGpmon.c */
extern void CheckSendPlanStateGpmonPkt(PlanState *ps);
extern void EndPlanStateGpmonPkt(PlanState *ps);
extern void InitPlanNodeGpmonPkt(Plan* plan, gpmon_packet_t *gpmon_pkt, EState *estate);

extern uint64 PlanStateOperatorMemKB(const PlanState *ps);

static inline void Gpmon_Incr_Rows_Out(gpmon_packet_t *pkt)
{
    ++pkt->u.qexec.rowsout;
}

/* ----------------
 *	these are defined to avoid confusion problems with "left"
 *	and "right" and "inner" and "outer".  The convention is that
 *	the "left" plan is the "outer" plan and the "right" plan is
 *	the inner plan, but these make the code more readable.
 * ----------------
 */
#define innerPlanState(node)		(((PlanState *)(node))->righttree)
#define outerPlanState(node)		(((PlanState *)(node))->lefttree)

/* Macros for inline access to certain instrumentation counters */
#define InstrCountFiltered1(node, delta) \
	do { \
		if (((PlanState *)(node))->instrument) \
			((PlanState *)(node))->instrument->nfiltered1 += (delta); \
	} while(0)
#define InstrCountFiltered2(node, delta) \
	do { \
		if (((PlanState *)(node))->instrument) \
			((PlanState *)(node))->instrument->nfiltered2 += (delta); \
	} while(0)

/*
 * EPQState is state for executing an EvalPlanQual recheck on a candidate
 * tuple in ModifyTable or LockRows.  The estate and planstate fields are
 * NULL if inactive.
 */
typedef struct EPQState
{
	EState	   *estate;			/* subsidiary EState */
	PlanState  *planstate;		/* plan state tree ready to be executed */
	TupleTableSlot *origslot;	/* original output tuple to be rechecked */
	Plan	   *plan;			/* plan tree to be executed */
	List	   *arowMarks;		/* ExecAuxRowMarks (non-locking only) */
	int			epqParam;		/* ID of Param to force scan node re-eval */
} EPQState;


/* ----------------
 *	 ResultState information
 * ----------------
 */
typedef struct ResultState
{
	PlanState	ps;				/* its first field is NodeTag */
	ExprState  *resconstantqual;
	bool		inputFullyConsumed;		/* are we done? */
	bool		rs_checkqual;	/* do we need to check the qual? */
	bool		isSRF;			/* state flag for processing set-valued
								 * functions in targetlist */
	ExprDoneCond lastSRFCond;	/* Applicable only if isSRF is true.
								 * Represents the last done flag */

	struct CdbHash *hashFilter;
} ResultState;

/* ----------------
 *	 RepeatState information
 * ----------------
 */
typedef struct RepeatState
{
	PlanState	ps;				/* its first field is NodeTag */

	bool		repeat_done;	/* are we done? */
	TupleTableSlot *slot;		/* The current tuple */
	int			repeat_count;	/* The number of repeats for the current tuple */
	ExprState  *expr_state;		/* The state to evaluate the expression */
} RepeatState;

/* ----------------
 *	 ModifyTableState information
 * ----------------
 */
typedef struct ModifyTableState
{
	PlanState	ps;				/* its first field is NodeTag */
	CmdType		operation;		/* INSERT, UPDATE, or DELETE */
	bool		canSetTag;		/* do we set the command tag/es_processed? */
	bool		mt_done;		/* are we done? */
	PlanState **mt_plans;		/* subplans (one per target rel) */
	int			mt_nplans;		/* number of plans in the array */
	int			mt_whichplan;	/* which one is being executed (0..n-1) */
	ResultRelInfo *resultRelInfo;		/* per-subplan target relations */
	List	  **mt_arowmarks;	/* per-subplan ExecAuxRowMark lists */
	EPQState	mt_epqstate;	/* for evaluating EvalPlanQual rechecks */
	bool		fireBSTriggers; /* do we need to fire stmt triggers? */
	AttrNumber		*mt_action_col_idxes;
	AttrNumber		*mt_ctid_col_idxes;
	AttrNumber		*mt_oid_col_idxes;
} ModifyTableState;

/* ----------------
 *	 AppendState information
 *
 *		nplans			how many plans are in the array
 *		whichplan		which plan is being executed (0 .. n-1)
 * ----------------
 */
typedef struct AppendState
{
	PlanState	ps;				/* its first field is NodeTag */
	PlanState **appendplans;	/* array of PlanStates for my inputs */
	int			eflags;			/* used to initialize each subplan */
	int			as_nplans;
	int			as_whichplan;
} AppendState;

/*
 * SequenceState
 */
typedef struct SequenceState
{
	PlanState	ps;
	PlanState **subplans;
	int			numSubplans;

	/*
	 * True if no subplan has been executed.
	 */
	bool		initState;
} SequenceState;

/* ----------------
 *	 MergeAppendState information
 *
 *		nplans			how many plans are in the array
 *		nkeys			number of sort key columns
 *		sortkeys		sort keys in SortSupport representation
 *		slots			current output tuple of each subplan
 *		heap			heap of active tuples
 *		initialized		true if we have fetched first tuple from each subplan
 * ----------------
 */
typedef struct MergeAppendState
{
	PlanState	ps;				/* its first field is NodeTag */
	PlanState **mergeplans;		/* array of PlanStates for my inputs */
	int			ms_nplans;
	int			ms_nkeys;
	SortSupport ms_sortkeys;	/* array of length ms_nkeys */
	TupleTableSlot **ms_slots;	/* array of length ms_nplans */
	struct binaryheap *ms_heap; /* binary heap of slot indices */
	bool		ms_initialized; /* are subplans started? */
} MergeAppendState;

/* ----------------
 *	 RecursiveUnionState information
 *
 *		RecursiveUnionState is used for performing a recursive union.
 *
 *		recursing			T when we're done scanning the non-recursive term
 *		intermediate_empty	T if intermediate_table is currently empty
 *		working_table		working table (to be scanned by recursive term)
 *		intermediate_table	current recursive output (next generation of WT)
 * ----------------
 */
typedef struct RecursiveUnionState
{
	PlanState	ps;				/* its first field is NodeTag */
	bool		recursing;
	bool		intermediate_empty;
	Tuplestorestate *working_table;
	Tuplestorestate *intermediate_table;

	/* Remaining fields are unused in UNION ALL case */
	FmgrInfo   *eqfunctions;	/* per-grouping-field equality fns */
	FmgrInfo   *hashfunctions;	/* per-grouping-field hash fns */
	MemoryContext tempContext;	/* short-term context for comparisons */
	TupleHashTable hashtable;	/* hash table for tuples already seen */
	MemoryContext tableContext; /* memory context containing hash table */
} RecursiveUnionState;

/* ----------------
 *	 BitmapAndState information
 * ----------------
 */
typedef struct BitmapAndState
{
	PlanState	ps;				/* its first field is NodeTag */
	PlanState **bitmapplans;	/* array of PlanStates for my inputs */
	int			nplans;			/* number of input plans */
	Node	   *bitmap;			/* output stream bitmap */
} BitmapAndState;

/* ----------------
 *	 BitmapOrState information
 * ----------------
 */
typedef struct BitmapOrState
{
	PlanState	ps;				/* its first field is NodeTag */
	PlanState **bitmapplans;	/* array of PlanStates for my inputs */
	int			nplans;			/* number of input plans */
	Node	   *bitmap;			/* output bitmap */
} BitmapOrState;

/* ----------------------------------------------------------------
 *				 Scan State Information
 * ----------------------------------------------------------------
 */

/* What stage the scan node is currently
 *
 * 	SCAN_INIT: we are initializing the scan state
 * 	SCAN_SCAN: all initializations for reading tuples are done
 * 		and we are either reading tuples, or ready to read tuples
 * 	SCAN_DONE: we are done with all relations/partitions, but
 * 		the scan state is still valid for a ReScan (i.e., we
 * 		haven't destroyed our scan state yet)
 * 	SCAN_END: we are completely done. We cannot ReScan, without
 * 		redoing the whole initialization phase again.
 */
typedef enum
{
	SCAN_INIT,
	SCAN_SCAN,
	SCAN_DONE,
	SCAN_END
} ScanStatus;

/* ----------------
 *	 ScanState information
 *
 *		ScanState extends PlanState for node types that represent
 *		scans of an underlying relation.  It can also be used for nodes
 *		that scan the output of an underlying plan node --- in that case,
 *		only ScanTupleSlot is actually useful, and it refers to the tuple
 *		retrieved from the subplan.
 *
 *		currentRelation    relation being scanned (NULL if none)
 *		ScanTupleSlot	   pointer to slot in tuple table holding scan tuple
 * ----------------
 */
typedef struct ScanState
{
	PlanState	ps;				/* its first field is NodeTag */
	Relation	ss_currentRelation;
	TupleTableSlot *ss_ScanTupleSlot;
} ScanState;

/*
 * SeqScanState
 *   State data for scanning heap/AO/AOCS table.
 */
typedef struct SeqScanState
{
	ScanState ss;
	struct HeapScanDescData *ss_currentScanDesc_heap;
	struct AppendOnlyScanDescData *ss_currentScanDesc_ao;
	struct AOCSScanDescData *ss_currentScanDesc_aocs;
    struct KVEngineScanDescData* rks_ScanDesc;
	/* extra state for AOCS scans */
	bool	   *ss_aocs_proj;
	int			ss_aocs_ncol;
} SeqScanState;

/*
 * These structs store information about index quals that don't have simple
 * constant right-hand sides.  See comments for ExecIndexBuildScanKeys()
 * for discussion.
 */
typedef struct
{
	ScanKey		scan_key;		/* scankey to put value into */
	ExprState  *key_expr;		/* expr to evaluate to get value */
	bool		key_toastable;	/* is expr's result a toastable datatype? */
} IndexRuntimeKeyInfo;

typedef struct
{
	ScanKey		scan_key;		/* scankey to put value into */
	ExprState  *array_expr;		/* expr to evaluate to get array value */
	int			next_elem;		/* next array element to use */
	int			num_elems;		/* number of elems in current array value */
	Datum	   *elem_values;	/* array of num_elems Datums */
	bool	   *elem_nulls;		/* array of num_elems is-null flags */
} IndexArrayKeyInfo;

/* ----------------
 *	 IndexScanState information
 *
 *		indexqualorig	   execution state for indexqualorig expressions
 *		ScanKeys		   Skey structures for index quals
 *		NumScanKeys		   number of ScanKeys
 *		OrderByKeys		   Skey structures for index ordering operators
 *		NumOrderByKeys	   number of OrderByKeys
 *		RuntimeKeys		   info about Skeys that must be evaluated at runtime
 *		NumRuntimeKeys	   number of RuntimeKeys
 *		RuntimeKeysReady   true if runtime Skeys have been computed
 *		RuntimeContext	   expr context for evaling runtime Skeys
 *		RelationDesc	   index relation descriptor
 *		ScanDesc		   index scan descriptor
 * ----------------
 */
typedef struct IndexScanState
{
	ScanState	ss;				/* its first field is NodeTag */
	List	   *indexqualorig;
	ScanKey		iss_ScanKeys;
	int			iss_NumScanKeys;
	ScanKey		iss_OrderByKeys;
	int			iss_NumOrderByKeys;
	IndexRuntimeKeyInfo *iss_RuntimeKeys;
	int			iss_NumRuntimeKeys;
	bool		iss_RuntimeKeysReady;
	ExprContext *iss_RuntimeContext;
	Relation	iss_RelationDesc;
	IndexScanDesc iss_ScanDesc;
    KVEngineScanDesc iss_KVScanDesc;
	/*
	 * tableOid is the oid of the partition or relation on which our current
	 * index relation is defined.
	 */
	Oid			tableOid;
} IndexScanState;

/*
 * DynamicIndexScanState
 */
typedef struct DynamicIndexScanState
{
	ScanState	ss;

	int			scan_state; /* the stage of scanning */

	int			eflags;
	IndexScanState *indexScanState;
	List	   *tuptable;
	ExprContext *outer_exprContext;

	/*
	* Partition id index that mantains all unique partition ids for the
	* DynamicIndexScan.
	*/
	HTAB *pidxIndex;

	/*
	* Status of the part to retrieve (result of the sequential search in a hash table).
	*/
	HASH_SEQ_STATUS pidxStatus;

	/* Like DynamicTableScanState, this flag is required to handle error condition.
	 * This flag prevent ExecEndDynamicIndexScan from calling hash_seq_term() or
	 * a NULL hash table. */
	bool shouldCallHashSeqTerm;

	/*
	 * We will create a new copy of logicalIndexInfo in this memory context for
	 * each partition. This memory context will be reset per-partition to free
	 * up previous partition's logicalIndexInfo memory
	 */
	MemoryContext partitionMemoryContext;

	/* The partition oid for which the current varnos are mapped */
	Oid columnLayoutOid;
} DynamicIndexScanState;

/* ----------------
 *	 IndexOnlyScanState information
 *
 *		indexqual		   execution state for indexqual expressions
 *		ScanKeys		   Skey structures for index quals
 *		NumScanKeys		   number of ScanKeys
 *		OrderByKeys		   Skey structures for index ordering operators
 *		NumOrderByKeys	   number of OrderByKeys
 *		RuntimeKeys		   info about Skeys that must be evaluated at runtime
 *		NumRuntimeKeys	   number of RuntimeKeys
 *		RuntimeKeysReady   true if runtime Skeys have been computed
 *		RuntimeContext	   expr context for evaling runtime Skeys
 *		RelationDesc	   index relation descriptor
 *		ScanDesc		   index scan descriptor
 *		VMBuffer		   buffer in use for visibility map testing, if any
 *		HeapFetches		   number of tuples we were forced to fetch from heap
 * ----------------
 */
typedef struct IndexOnlyScanState
{
	ScanState	ss;				/* its first field is NodeTag */
	List	   *indexqual;
	ScanKey		ioss_ScanKeys;
	int			ioss_NumScanKeys;
	ScanKey		ioss_OrderByKeys;
	int			ioss_NumOrderByKeys;
	IndexRuntimeKeyInfo *ioss_RuntimeKeys;
	int			ioss_NumRuntimeKeys;
	bool		ioss_RuntimeKeysReady;
	ExprContext *ioss_RuntimeContext;
	Relation	ioss_RelationDesc;
	IndexScanDesc ioss_ScanDesc;
	KVEngineScanDesc ioss_KVScanDesc;
	Buffer		ioss_VMBuffer;
	long		ioss_HeapFetches;
} IndexOnlyScanState;

/* ----------------
 *	 BitmapIndexScanState information
 *
 *		result			   bitmap to return output into, or NULL
 *		ScanKeys		   Skey structures for index quals
 *		NumScanKeys		   number of ScanKeys
 *		RuntimeKeys		   info about Skeys that must be evaluated at runtime
 *		NumRuntimeKeys	   number of RuntimeKeys
 *		ArrayKeys		   info about Skeys that come from ScalarArrayOpExprs
 *		NumArrayKeys	   number of ArrayKeys
 *		RuntimeKeysReady   true if runtime Skeys have been computed
 *		RuntimeContext	   expr context for evaling runtime Skeys
 *		RelationDesc	   index relation descriptor
 *		ScanDesc		   index scan descriptor
 * ----------------
 */
typedef struct BitmapIndexScanState
{
	ScanState	ss;				/* its first field is NodeTag */
	Node       *biss_result;	/* output bitmap */
	ScanKey		biss_ScanKeys;
	int			biss_NumScanKeys;
	IndexRuntimeKeyInfo *biss_RuntimeKeys;
	int			biss_NumRuntimeKeys;
	IndexArrayKeyInfo *biss_ArrayKeys;
	int			biss_NumArrayKeys;
	bool		biss_RuntimeKeysReady;
	ExprContext *biss_RuntimeContext;
	Relation	biss_RelationDesc;
	IndexScanDesc biss_ScanDesc;
} BitmapIndexScanState;

/*
 * DynamicBitmapIndexScanState
 */
typedef struct DynamicBitmapIndexScanState
{
	ScanState	ss;

	int			eflags;
	BitmapIndexScanState *bitmapIndexScanState;
	ExprContext *outer_exprContext;

	/*
	 * We will create a new copy of logicalIndexInfo in this memory context for
	 * each partition. This memory context will be reset per-partition to free
	 * up previous partition's logicalIndexInfo memory
	 */
	MemoryContext partitionMemoryContext;

	/* The partition oid for which the current varnos are mapped */
	Oid			columnLayoutOid;
} DynamicBitmapIndexScanState;

/* ----------------
 *	 BitmapHeapScanState information
 *
 *		bitmapqualorig	   execution state for bitmapqualorig expressions
 *		tbm				   bitmap obtained from child index scan(s)
 *		tbmiterator		   iterator for scanning current pages
 *		tbmres			   current-page data
 *		exact_pages		   total number of exact pages retrieved
 *		lossy_pages		   total number of lossy pages retrieved
 *		prefetch_iterator  iterator for prefetching ahead of current page
 *		prefetch_pages	   # pages prefetch iterator is ahead of current
 *		prefetch_target    target prefetch distance
 * ----------------
 */
typedef struct BitmapHeapScanState
{
	ScanState	ss;				/* its first field is NodeTag */

	struct HeapScanDescData *bhs_currentScanDesc_heap;
	struct AppendOnlyFetchDescData *bhs_currentAOFetchDesc;
	struct AOCSFetchDescData *bhs_currentAOCSFetchDesc;
	struct AOCSFetchDescData *bhs_currentAOCSLossyFetchDesc;
    struct KVEngineScanDescData* rks_ScanDesc;
	List	   *bitmapqualorig;
	Node	   *tbm;
	GenericBMIterator *tbmiterator;
	TBMIterateResult *tbmres;
	long		exact_pages;
	long		lossy_pages;
	GenericBMIterator *prefetch_iterator;
	int			prefetch_pages;
	int			prefetch_target;

	/* These are used by AO/AOCS scans, to work with lossy bitmap pages */
	bool		baos_gotpage;
	int			baos_cindex;
	bool		baos_lossy;
	int			baos_ntuples;

} BitmapHeapScanState;

typedef struct DynamicBitmapHeapScanState
{
	ScanState	ss;				/* its first field is NodeTag */

	int			scan_state; /* the stage of scanning */

	int			eflags;
	BitmapHeapScanState *bhsState;


	/*
	 * Pid index that maintains all unique partition pids for this dynamic
	 * table scan to scan.
	 */
	HTAB	   *pidIndex;

	/*
	 * The status of sequentially scan the pid index.
	 */
	HASH_SEQ_STATUS pidStatus;

	/*
	 * Should we call hash_seq_term()? This is required
	 * to handle error condition, where we are required to explicitly
	 * call hash_seq_term(). Also, if we don't have any partition, this
	 * flag should prevent ExecEndDynamicSeqScan from calling
	 * hash_seq_term() on a NULL hash table.
	 */
	bool		shouldCallHashSeqTerm;

	/*
	 * The first partition requires initialization of expression states,
	 * such as qual and targetlist, regardless of whether we need to re-map varattno
	 */
	bool		firstPartition;
	/*
	 * lastRelOid is the last relation that corresponds to the
	 * varattno mapping of qual and target list. Each time we open a new partition, we will
	 * compare the last relation with current relation by using varattnos_map()
	 * and then convert the varattno to the new varattno
	 */
	Oid			lastRelOid;

	/*
	 * scanrelid is the RTE index for this scan node. It will be used to select
	 * varno whose varattno will be remapped, if necessary
	 */
	Index		scanrelid;

	/*
	 * This memory context will be reset per-partition to free
	 * up previous partition's memory
	 */
	MemoryContext partitionMemoryContext;

} DynamicBitmapHeapScanState;

/* ----------------
 *	 TidScanState information
 *
 *		isCurrentOf    scan has a CurrentOfExpr qual
 *		NumTids		   number of tids in this scan
 *		TidPtr		   index of currently fetched tid
 *		TidList		   evaluated item pointers (array of size NumTids)
 * ----------------
 */
typedef struct TidScanState
{
	ScanState	ss;				/* its first field is NodeTag */
	List	   *tss_tidquals;	/* list of ExprState nodes */
	bool		tss_isCurrentOf;
	int			tss_NumTids;
	int			tss_TidPtr;
	int			tss_MarkTidPtr;
	ItemPointerData *tss_TidList;
	HeapTupleData tss_htup;
} TidScanState;

/* ----------------
 *	 SubqueryScanState information
 *
 *		SubqueryScanState is used for scanning a sub-query in the range table.
 *		ScanTupleSlot references the current output tuple of the sub-query.
 * ----------------
 */
typedef struct SubqueryScanState
{
	ScanState	ss;				/* its first field is NodeTag */
	PlanState  *subplan;
	bool		cdb_want_ctid;	/* true => ctid is referenced in targetlist */
	ItemPointerData cdb_fake_ctid;
} SubqueryScanState;

/* ----------------
 *	 FunctionScanState information
 *
 *		Function nodes are used to scan the results of a
 *		function appearing in FROM (typically a function returning set).
 *
 *		eflags				node's capability flags
 *		ordinality			is this scan WITH ORDINALITY?
 *		simple				true if we have 1 function and no ordinality
 *		ordinal				current ordinal column value
 *		nfuncs				number of functions being executed
 *		funcstates			per-function execution states (private in
 *							nodeFunctionscan.c)
 *		cdb_want_ctid		true => ctid is referenced in targetlist
 *		cdb_fake_ctid
 *		cdb_mark_ctid
 *		argcontext			memory context to evaluate function arguments in
 * ----------------
 */
struct FunctionScanPerFuncState;

typedef struct FunctionScanState
{
	ScanState	ss;				/* its first field is NodeTag */
	int			eflags;
	bool		ordinality;
	bool		simple;
	int64		ordinal;
	int			nfuncs;
	struct FunctionScanPerFuncState *funcstates;		/* array of length
														 * nfuncs */
	bool		cdb_want_ctid;
	ItemPointerData cdb_fake_ctid;
	ItemPointerData cdb_mark_ctid;
	MemoryContext argcontext;

	bool		delayEagerFree;		/* is is safe to free memory used by this node,
									 * when this node has outputted its last row? */
} FunctionScanState;


/* ----------------
 * TableFunctionState information
 *
 *   Table Function nodes are used to scan the results of a table function
 *   operating over a table as input.
 * ----------------
 */
typedef struct TableFunctionState
{
	ScanState	ss;				/* Table Function is a Scan */
	struct AnyTableData *inputscan;		/* subquery scan data */
	TupleDesc	resultdesc;		/* Function Result descriptor */
	HeapTupleData tuple;		/* Returned tuple */
	FuncExprState *fcache;		/* Function Call Cache */
	FunctionCallInfoData fcinfo;	/* Function Call Context */
	ReturnSetInfo rsinfo;		/* Resultset Context */
	bool		is_rowtype;		/* Function returns records */
	bool		is_firstcall;
	bytea	   *userdata;		/* bytea given by describe func */
} TableFunctionState;


/* ----------------
 *	 ValuesScanState information
 *
 *		ValuesScan nodes are used to scan the results of a VALUES list
 *
 *		rowcontext			per-expression-list context
 *		exprlists			array of expression lists being evaluated
 *		array_len			size of array
 *		curr_idx			current array index (0-based)
 *		marked_idx			marked position (for mark/restore)
 *
 *	Note: ss.ps.ps_ExprContext is used to evaluate any qual or projection
 *	expressions attached to the node.  We create a second ExprContext,
 *	rowcontext, in which to build the executor expression state for each
 *	Values sublist.  Resetting this context lets us get rid of expression
 *	state for each row, avoiding major memory leakage over a long values list.
 * ----------------
 */
typedef struct ValuesScanState
{
	ScanState	ss;				/* its first field is NodeTag */
	ExprContext *rowcontext;
	List	  **exprlists;
	int			array_len;
	int			curr_idx;
	int			marked_idx;
	bool		cdb_want_ctid;	/* true => ctid is referenced in targetlist */
} ValuesScanState;

/* ----------------
 *	 CteScanState information
 *
 *		CteScan nodes are used to scan a CommonTableExpr query.
 *
 * Multiple CteScan nodes can read out from the same CTE query.  We use
 * a tuplestore to hold rows that have been read from the CTE query but
 * not yet consumed by all readers.
 * ----------------
 */
typedef struct CteScanState
{
	ScanState	ss;				/* its first field is NodeTag */
	int			eflags;			/* capability flags to pass to tuplestore */
	int			readptr;		/* index of my tuplestore read pointer */
	PlanState  *cteplanstate;	/* PlanState for the CTE query itself */
	/* Link to the "leader" CteScanState (possibly this same node) */
	struct CteScanState *leader;
	/* The remaining fields are only valid in the "leader" CteScanState */
	Tuplestorestate *cte_table; /* rows already read from the CTE query */
	bool		eof_cte;		/* reached end of CTE query? */
} CteScanState;

/* ----------------
 *	 WorkTableScanState information
 *
 *		WorkTableScan nodes are used to scan the work table created by
 *		a RecursiveUnion node.  We locate the RecursiveUnion node
 *		during executor startup.
 * ----------------
 */
typedef struct WorkTableScanState
{
	ScanState	ss;				/* its first field is NodeTag */
	RecursiveUnionState *rustate;
} WorkTableScanState;

/* ----------------
 *	 ForeignScanState information
 *
 *		ForeignScan nodes are used to scan foreign-data tables.
 * ----------------
 */
typedef struct ForeignScanState
{
	ScanState	ss;				/* its first field is NodeTag */
	/* use struct pointer to avoid including fdwapi.h here */
	struct FdwRoutine *fdwroutine;
	void	   *fdw_state;		/* foreign-data wrapper can keep state here */
} ForeignScanState;

/* ----------------
 *         ExternalScanState information
 *
 *	 ExternalScan nodes are used to scan external tables
 *
 *	 ess_ScanDesc                the state of the file data scan
 * ----------------
 */
typedef struct ExternalScanState
{
	ScanState	ss;
	struct FileScanDescData *ess_ScanDesc;
	bool		cdb_want_ctid;
	ItemPointerData cdb_fake_ctid;
} ExternalScanState;

/*
 * DynamicSeqScanState
 */
typedef struct DynamicSeqScanState
{
	ScanState	ss;

	int			scan_state; /* the stage of scanning */

	int			eflags;
	SeqScanState *seqScanState;

	/*
	 * Pid index that maintains all unique partition pids for this dynamic
	 * table scan to scan.
	 */
	HTAB	   *pidIndex;

	/*
	 * The status of sequentially scan the pid index.
	 */
	HASH_SEQ_STATUS pidStatus;

	/*
	 * Should we call hash_seq_term()? This is required
	 * to handle error condition, where we are required to explicitly
	 * call hash_seq_term(). Also, if we don't have any partition, this
	 * flag should prevent ExecEndDynamicSeqScan from calling
	 * hash_seq_term() on a NULL hash table.
	 */
	bool		shouldCallHashSeqTerm;

	/*
	 * The first partition requires initialization of expression states,
	 * such as qual and targetlist, regardless of whether we need to re-map varattno
	 */
	bool		firstPartition;
	/*
	 * lastRelOid is the last relation that corresponds to the
	 * varattno mapping of qual and target list. Each time we open a new partition, we will
	 * compare the last relation with current relation by using varattnos_map()
	 * and then convert the varattno to the new varattno
	 */
	Oid			lastRelOid;

	/*
	 * scanrelid is the RTE index for this scan node. It will be used to select
	 * varno whose varattno will be remapped, if necessary
	 */
	Index		scanrelid;

	/*
	 * This memory context will be reset per-partition to free
	 * up previous partition's memory
	 */
	MemoryContext partitionMemoryContext;


} DynamicSeqScanState;

/* ----------------------------------------------------------------
 *				 Join State Information
 * ----------------------------------------------------------------
 */

/* ----------------
 *	 JoinState information
 *
 *		Superclass for state nodes of join plans.
 * ----------------
 */
typedef struct JoinState
{
	PlanState	ps;
	JoinType	jointype;
	List	   *joinqual;		/* JOIN quals (in addition to ps.qual) */
} JoinState;

/* ----------------
 *	 NestLoopState information
 *
 *		NeedNewOuter	   true if need new outer tuple on next call
 *		MatchedOuter	   true if found a join match for current outer tuple
 *		NullInnerTupleSlot prepared null tuple for left outer joins
 * ----------------
 */
typedef struct NestLoopState
{
	JoinState	js;				/* its first field is NodeTag */
	bool		nl_NeedNewOuter;
	bool		nl_MatchedOuter;
	bool		shared_outer;
	bool		prefetch_inner;
	bool		prefetch_joinqual;
	bool		reset_inner; /*CDB-OLAP*/
	bool		require_inner_reset; /*CDB-OLAP*/

	TupleTableSlot *nl_NullInnerTupleSlot;

	List	   *nl_InnerJoinKeys;        /* list of ExprState nodes */
	List	   *nl_OuterJoinKeys;        /* list of ExprState nodes */
	bool		nl_innerSideScanned;      /* set to true once we've scanned all inner tuples the first time */
	bool		nl_qualResultForNull;     /* the value of the join condition when one of the sides contains a NULL */
} NestLoopState;

/* ----------------
 *	 MergeJoinState information
 *
 *		NumClauses		   number of mergejoinable join clauses
 *		Clauses			   info for each mergejoinable clause
 *		JoinState		   current state of ExecMergeJoin state machine
 *		ExtraMarks		   true to issue extra Mark operations on inner scan
 *		ConstFalseJoin	   true if we have a constant-false joinqual
 *		FillOuter		   true if should emit unjoined outer tuples anyway
 *		FillInner		   true if should emit unjoined inner tuples anyway
 *		MatchedOuter	   true if found a join match for current outer tuple
 *		MatchedInner	   true if found a join match for current inner tuple
 *		OuterTupleSlot	   slot in tuple table for cur outer tuple
 *		InnerTupleSlot	   slot in tuple table for cur inner tuple
 *		MarkedTupleSlot    slot in tuple table for marked tuple
 *		NullOuterTupleSlot prepared null tuple for right outer joins
 *		NullInnerTupleSlot prepared null tuple for left outer joins
 *		OuterEContext	   workspace for computing outer tuple's join values
 *		InnerEContext	   workspace for computing inner tuple's join values
 * ----------------
 */
/* private in nodeMergejoin.c: */
typedef struct MergeJoinClauseData *MergeJoinClause;

typedef struct MergeJoinState
{
	JoinState	js;				/* its first field is NodeTag */
	int			mj_NumClauses;
	MergeJoinClause mj_Clauses; /* array of length mj_NumClauses */
	int			mj_JoinState;
	bool		mj_ExtraMarks;
	bool		mj_ConstFalseJoin;
	bool		mj_FillOuter;
	bool		mj_FillInner;
	bool		mj_MatchedOuter;
	bool		mj_MatchedInner;
	TupleTableSlot *mj_OuterTupleSlot;
	TupleTableSlot *mj_InnerTupleSlot;
	TupleTableSlot *mj_MarkedTupleSlot;
	TupleTableSlot *mj_NullOuterTupleSlot;
	TupleTableSlot *mj_NullInnerTupleSlot;
	ExprContext *mj_OuterEContext;
	ExprContext *mj_InnerEContext;
	bool		prefetch_inner; /* MPP-3300 */
	bool		prefetch_joinqual;
} MergeJoinState;

/* ----------------
 *	 HashJoinState information
 *
 *		hashclauses				original form of the hashjoin condition
 *		hj_OuterHashKeys		the outer hash keys in the hashjoin condition
 *		hj_InnerHashKeys		the inner hash keys in the hashjoin condition
 *		hj_HashOperators		the join operators in the hashjoin condition
 *		hj_HashTable			hash table for the hashjoin
 *								(NULL if table not built yet)
 *		hj_CurHashValue			hash value for current outer tuple
 *		hj_CurBucketNo			regular bucket# for current outer tuple
 *		hj_CurSkewBucketNo		skew bucket# for current outer tuple
 *		hj_CurTuple				last inner tuple matched to current outer
 *								tuple, or NULL if starting search
 *								(hj_CurXXX variables are undefined if
 *								OuterTupleSlot is empty!)
 *		hj_OuterTupleSlot		tuple slot for outer tuples
 *		hj_HashTupleSlot		tuple slot for inner (hashed) tuples
 *		hj_NullOuterTupleSlot	prepared null tuple for right/full outer joins
 *		hj_NullInnerTupleSlot	prepared null tuple for left/full outer joins
 *		hj_FirstOuterTupleSlot	first tuple retrieved from outer plan
 *		hj_JoinState			current state of ExecHashJoin state machine
 *		hj_MatchedOuter			true if found a join match for current outer
 *		hj_OuterNotEmpty		true if outer relation known not empty
 *		hj_nonequijoin			true to force hash table to keep nulls
 * ----------------
 */

/* these structs are defined in executor/hashjoin.h: */
typedef struct HashJoinTupleData *HashJoinTuple;
typedef struct HashJoinTableData *HashJoinTable;

typedef struct HashJoinState
{
	JoinState	js;				/* its first field is NodeTag */
	List	   *hashclauses;	/* list of ExprState nodes */
	List	   *hashqualclauses;	/* CDB: list of ExprState nodes (match) */
	List	   *hj_OuterHashKeys;		/* list of ExprState nodes */
	List	   *hj_InnerHashKeys;		/* list of ExprState nodes */
	List	   *hj_HashOperators;		/* list of operator OIDs */
	HashJoinTable hj_HashTable;
	uint32		hj_CurHashValue;
	int			hj_CurBucketNo;
	int			hj_CurSkewBucketNo;
	HashJoinTuple hj_CurTuple;
	TupleTableSlot *hj_OuterTupleSlot;
	TupleTableSlot *hj_HashTupleSlot;
	TupleTableSlot *hj_NullOuterTupleSlot;
	TupleTableSlot *hj_NullInnerTupleSlot;
	TupleTableSlot *hj_FirstOuterTupleSlot;
	int			hj_JoinState;
	bool		hj_MatchedOuter;
	bool		hj_OuterNotEmpty;
	bool		hj_InnerEmpty;  /* set to true if inner side is empty */
	bool		prefetch_inner;
	bool		prefetch_joinqual;
	bool		hj_nonequijoin;

	/* set if the operator created workfiles */
	bool workfiles_created;
	bool reuse_hashtable; /* Do we need to preserve hash table to support rescan */
} HashJoinState;


/* ----------------------------------------------------------------
 *				 Materialization State Information
 * ----------------------------------------------------------------
 */

/* ----------------
 *         Generic tuplestore structure
 *	 used to communicate between ShareInputScan nodes,
 *	 Materialize and Sort
 *
 * ----------------
 */
typedef union GenericTupStore
{
	struct NTupleStore        *matstore;     /* Used by Materialize */
	void	   *sortstore;	/* Used by Sort */
} GenericTupStore;

/* ----------------
 *	 MaterialState information
 *
 *		materialize nodes are used to materialize the results
 *		of a subplan into a temporary file.
 *
 *		ss.ss_ScanTupleSlot refers to output of underlying plan.
 * ----------------
 */
typedef struct MaterialState
{
	ScanState	ss;				/* its first field is NodeTag */
	int			eflags;			/* capability flags to pass to tuplestore */
	bool		eof_underlying; /* reached end of underlying plan? */
	bool		ts_destroyed;	/* called destroy tuple store? */

	bool		delayEagerFree;		/* is is safe to free memory used by this node,
									 * when this node has outputted its last row? */

	GenericTupStore *ts_state;	/* private state of tuplestore.c */
	void	   *ts_pos;
	void	   *ts_markpos;
	void	   *share_lk_ctxt;
} MaterialState;

/* ----------------
 *	  ShareInputScanState information
 *
 *		State of each scanner of the ShareInput node
 * ----------------
 */
typedef struct ShareInputScanState
{
	ScanState	ss;
	/*
	 * Depends on share_type, we should have a tuplestore_state, tuplestore_pos
	 * or tuplesort_state, tuplesort_pos
	 */
	GenericTupStore *ts_state;
	void	   *ts_pos;
	void	   *ts_markpos;

	void	   *share_lk_ctxt;
	bool		freed; /* is this node already freed? */
} ShareInputScanState;

/* XXX Should move into buf file */
extern void *shareinput_reader_waitready(int share_id, PlanGenerator planGen);
extern void *shareinput_writer_notifyready(int share_id, int nsharer_xslice_notify_ready, PlanGenerator planGen);
extern void shareinput_reader_notifydone(void *, int share_id);
extern void shareinput_writer_waitdone(void *, int share_id, int nsharer_xslice_wait_done);
extern void shareinput_create_bufname_prefix(char* p, int size, int share_id);

/* ----------------
 *	 SortState information
 * ----------------
 */
typedef struct SortState
{
	ScanState	ss;				/* its first field is NodeTag */
	bool		randomAccess;	/* need random access to sort output? */
	bool		bounded;		/* is the result set bounded? */
	int64		bound;			/* if bounded, how many tuples are needed */
	bool		sort_Done;		/* sort completed yet? */
	bool		bounded_Done;	/* value of bounded we did the sort with */
	int64		bound_Done;		/* value of bound we did the sort with */
	GenericTupStore *tuplesortstate; /* private state of tuplesort.c */
	bool		noduplicates;	/* true if discard duplicate rows */

	bool		delayEagerFree;		/* is is safe to free memory used by this node,
									 * when this node has outputted its last row? */

	void	   *share_lk_ctxt;

} SortState;

/* ---------------------
 *	AggState information
 *
 *	ss.ss_ScanTupleSlot refers to output of underlying plan.
 *
 *	Note: ss.ps.ps_ExprContext contains ecxt_aggvalues and
 *	ecxt_aggnulls arrays, which hold the computed agg values for the current
 *	input group during evaluation of an Agg node's output tuple(s).  We
 *	create a second ExprContext, tmpcontext, in which to evaluate input
 *	expressions and run the aggregate transition functions.
 * -------------------------
 */
/* these structs are private in nodeAgg.c: */
typedef struct AggStatePerAggData *AggStatePerAgg;
typedef struct AggStatePerGroupData *AggStatePerGroup;

typedef enum HashAggStatus
{
	HASHAGG_BEFORE_FIRST_PASS,
	HASHAGG_IN_A_PASS,
	HASHAGG_BETWEEN_PASSES,
	HASHAGG_STREAMING,
	HASHAGG_END_OF_PASSES
} HashAggStatus;

typedef struct AggState
{
	ScanState	ss;				/* its first field is NodeTag */
	List	   *aggs;			/* all Aggref nodes in targetlist & quals */
	int			numaggs;		/* length of list (could be zero!) */
	FmgrInfo   *eqfunctions;	/* per-grouping-field equality fns */
	FmgrInfo   *hashfunctions;	/* per-grouping-field hash fns */
	AggStatePerAgg peragg;		/* per-Aggref information */
	MemoryContext aggcontext;	/* memory context for long-lived data */
	ExprContext *tmpcontext;	/* econtext for input expressions */
	AggStatePerAgg curperagg;	/* identifies currently active aggregate */
	bool		agg_done;		/* indicates completion of Agg scan */
	bool        has_partial_agg;/* indicate if a partial aggregate result
								 * has been calculated in the previous call.
								 */

	/* these fields are used in AGG_PLAIN and AGG_SORTED modes: */
	AggStatePerGroup pergroup;	/* per-Aggref-per-group working state */
	struct MemTupleData *grp_firstTuple; /* copy of first tuple of current group */
	/* these fields are used in AGG_HASHED mode: */
	TupleHashTable hashtable;	/* hash table with one entry per group */
	TupleTableSlot *hashslot;	/* slot for loading hash table */
	List	   *hash_needed;	/* list of columns needed in hash table */
	TupleHashIterator hashiter; /* for iterating through hash table */

	/* MPP */
	struct HashAggTable *hhashtable;
	HashAggStatus hashaggstatus;
	MemoryManagerContainer mem_manager;

	/* ROLLUP */
	AggStatePerGroup perpassthru; /* per-Aggref-per-pass-through-tuple working state */

	/*
	 * The following are used to define how to modify input tuples to
	 * satisfy the rollup level of this Agg node.
	 */
	int			num_attrs;	/* number of grouping attributes for the Agg node */
	Datum	   *replValues;
	bool	   *replIsnull;
	bool	   *doReplace;

	/*
	 * Most executor nodes in GPDB don't support SRFs in target lists, the
	 * planner tries to insulate them from SRFs by adding Result nodes. But
	 * Agg needs to handle them, because a Result can't evaluate Aggrefs,
	 * which an Agg's target list usually has.
	 */
	bool		ps_TupFromTlist;
} AggState;

/* ----------------
 *	WindowAggState information
 * ----------------
 */
/* these structs are private in nodeWindowAgg.c: */
typedef struct WindowStatePerFuncData *WindowStatePerFunc;
typedef struct WindowStatePerAggData *WindowStatePerAgg;

typedef struct WindowAggState
{
	ScanState	ss;				/* its first field is NodeTag */

	/* these fields are filled in by ExecInitExpr: */
	List	   *funcs;			/* all WindowFunc nodes in targetlist */
	int			numfuncs;		/* total number of window functions */
	int			numaggs;		/* number that are plain aggregates */

	WindowStatePerFunc perfunc; /* per-window-function information */
	WindowStatePerAgg peragg;	/* per-plain-aggregate information */
	FmgrInfo   *partEqfunctions;	/* equality funcs for partition columns */
	FmgrInfo   *ordEqfunctions; /* equality funcs for ordering columns */
	Tuplestorestate *buffer;	/* stores rows of current partition */
	int			current_ptr;	/* read pointer # for current */
	int64		spooled_rows;	/* total # of rows in buffer */
	int64		currentpos;		/* position of current row in partition */
	int64		frameheadpos;	/* current frame head position */
	int64		frametailpos;	/* current frame tail position */
	/* use struct pointer to avoid including windowapi.h here */
	struct WindowObjectData *agg_winobj;		/* winobj for aggregate
												 * fetches */
	int64		aggregatedbase; /* start row for current aggregates */
	int64		aggregatedupto; /* rows before this one are aggregated */

	int			frameOptions;	/* frame_clause options, see WindowDef */
	ExprState  *startOffset;	/* expression for starting bound offset */
	ExprState  *endOffset;		/* expression for ending bound offset */
	Datum		startOffsetValue;		/* result of startOffset evaluation */
	Datum		endOffsetValue; /* result of endOffset evaluation */

	FmgrInfo	ordCmpFunction;	/* btree cmp function for first ORDER BY col */
	bool		ordReverse;		/* is the first ORDER BY col reversed? */
	bool		start_offset_valid;	/* is startOffsetValue valid for current row? */
	bool		end_offset_valid;	/* is endOffsetValue valid for current row? */

	ExprState  *startBound;		/* expression for RANGE starting boundary */
	ExprState  *endBound;		/* expression for RANGE ending boundary */
	Datum		startBoundValue;
	bool		startBoundIsNull;
	Datum		endBoundValue;
	bool		endBoundIsNull;
	int16		boundTypeLen;
	bool		boundTypeByVal;

	ExprState  *startOffsetIsNegative; /* expression to test if startOffset is negative */
	ExprState  *endOffsetIsNegative; /* expression to test if startOffset is negative */

	/*
	 * In GPDB, we support RANGE/ROWS start/end expressions to contain
	 * variables. You lose on some optimizations in that case, so we use
	 * these flags to indicate if they don't contain any variables, to allow
	 * those optimizations in the usual case that they don't.
	 */
	bool		start_offset_var_free;
	bool		end_offset_var_free;

	MemoryContext partcontext;	/* context for partition-lifespan data */
	MemoryContext aggcontext;	/* shared context for aggregate working data */
	MemoryContext curaggcontext;	/* current aggregate's working data */
	ExprContext *tmpcontext;	/* short-term evaluation context */

	bool		all_first;		/* true if the scan is starting */
	bool		all_done;		/* true if the scan is finished */
	bool		partition_spooled;		/* true if all tuples in current
										 * partition have been spooled into
										 * tuplestore */
	bool		more_partitions;/* true if there's more partitions after this
								 * one */
	bool		framehead_valid;/* true if frameheadpos is known up to date
								 * for current row */
	bool		frametail_valid;/* true if frametailpos is known up to date
								 * for current row */

	TupleTableSlot *first_part_slot;	/* first tuple of current or next
										 * partition */

	/* temporary slots for tuples fetched back from tuplestore */
	TupleTableSlot *agg_row_slot;
	TupleTableSlot *temp_slot_1;
	TupleTableSlot *temp_slot_2;

	/*
	 * Most executor nodes in GPDB don't support SRFs in target lists, the
	 * planner tries to insulate them from SRFs by adding Result nodes. But
	 * WindowAgg needs to handle them, because a Result can't evaluate
	 * WindowFunc, which an WindowAgg's target list usually has.
	 * This is the same logic as for AggState.
	 */
	bool		ps_TupFromTlist;
} WindowAggState;

/* ----------------
 *	 UniqueState information
 *
 *		Unique nodes are used "on top of" sort nodes to discard
 *		duplicate tuples returned from the sort phase.  Basically
 *		all it does is compare the current tuple from the subplan
 *		with the previously fetched tuple (stored in its result slot).
 *		If the two are identical in all interesting fields, then
 *		we just fetch another tuple from the sort and try again.
 * ----------------
 */
typedef struct UniqueState
{
	PlanState	ps;				/* its first field is NodeTag */
	FmgrInfo   *eqfunctions;	/* per-field lookup data for equality fns */
	MemoryContext tempContext;	/* short-term context for comparisons */
} UniqueState;

/* ----------------
 *	 HashState information
 * ----------------
 */
typedef struct HashState
{
	PlanState	ps;				/* its first field is NodeTag */
	HashJoinTable hashtable;	/* hash table for the hashjoin */
	List	   *hashkeys;		/* list of ExprState nodes */
	bool		hs_keepnull;	/* Keep nulls */
	bool		hs_quit_if_hashkeys_null;	/* quit building hash table if hashkeys are all null */
	bool		hs_hashkeys_null;	/* found an instance wherein hashkeys are all null */
	/* hashkeys is same as parent's hj_InnerHashKeys */
} HashState;

/* ----------------
 *	 SetOpState information
 *
 *		Even in "sorted" mode, SetOp nodes are more complex than a simple
 *		Unique, since we have to count how many duplicates to return.  But
 *		we also support hashing, so this is really more like a cut-down
 *		form of Agg.
 * ----------------
 */
/* this struct is private in nodeSetOp.c: */
typedef struct SetOpStatePerGroupData *SetOpStatePerGroup;

typedef struct SetOpState
{
	PlanState	ps;				/* its first field is NodeTag */
	FmgrInfo   *eqfunctions;	/* per-grouping-field equality fns */
	FmgrInfo   *hashfunctions;	/* per-grouping-field hash fns */
	bool		setop_done;		/* indicates completion of output scan */
	long		numOutput;		/* number of dups left to output */
	MemoryContext tempContext;	/* short-term context for comparisons */
	/* these fields are used in SETOP_SORTED mode: */
	SetOpStatePerGroup pergroup;	/* per-group working state */
	HeapTuple	grp_firstTuple; /* copy of first tuple of current group */
	/* these fields are used in SETOP_HASHED mode: */
	TupleHashTable hashtable;	/* hash table with one entry per group */
	MemoryContext tableContext; /* memory context containing hash table */
	bool		table_filled;	/* hash table filled yet? */
	TupleHashIterator hashiter; /* for iterating through hash table */
} SetOpState;

/* ----------------
 *	 LockRowsState information
 *
 *		LockRows nodes are used to enforce FOR [KEY] UPDATE/SHARE locking.
 * ----------------
 */
typedef struct LockRowsState
{
	PlanState	ps;				/* its first field is NodeTag */
	List	   *lr_arowMarks;	/* List of ExecAuxRowMarks */
	EPQState	lr_epqstate;	/* for evaluating EvalPlanQual rechecks */
} LockRowsState;

/* ----------------
 *	 LimitState information
 *
 *		Limit nodes are used to enforce LIMIT/OFFSET clauses.
 *		They just select the desired subrange of their subplan's output.
 *
 * offset is the number of initial tuples to skip (0 does nothing).
 * count is the number of tuples to return after skipping the offset tuples.
 * If no limit count was specified, count is undefined and noCount is true.
 * When lstate == LIMIT_INITIAL, offset/count/noCount haven't been set yet.
 * ----------------
 */
typedef enum
{
	LIMIT_INITIAL,				/* initial state for LIMIT node */
	LIMIT_RESCAN,				/* rescan after recomputing parameters */
	LIMIT_EMPTY,				/* there are no returnable rows */
	LIMIT_INWINDOW,				/* have returned a row in the window */
	LIMIT_SUBPLANEOF,			/* at EOF of subplan (within window) */
	LIMIT_WINDOWEND,			/* stepped off end of window */
	LIMIT_WINDOWSTART			/* stepped off beginning of window */
} LimitStateCond;

typedef struct LimitState
{
	PlanState	ps;				/* its first field is NodeTag */
	ExprState  *limitOffset;	/* OFFSET parameter, or NULL if none */
	ExprState  *limitCount;		/* COUNT parameter, or NULL if none */
	int64		offset;			/* current OFFSET value */
	int64		count;			/* current COUNT, if any */
	bool		noCount;		/* if true, ignore count */
	LimitStateCond lstate;		/* state machine status, as above */
	int64		position;		/* 1-based index of last tuple returned */
	TupleTableSlot *subSlot;	/* tuple last obtained from subplan */
} LimitState;

/*
 * DML Operations
 */

/*
 * ExecNode for DML.
 * This operator contains a Plannode in PlanState.
 * The Plannode contains indexes to the resjunk columns
 * needed for deciding the action (Insert/Delete), the table oid
 * and the tuple ctid.
 */
typedef struct DMLState
{
	PlanState	ps;
	JunkFilter *junkfilter;			/* filter that removes junk and dropped attributes */
	TupleTableSlot *cleanedUpSlot;	/* holds 'final' tuple which matches the target relation schema */
	AttrNumber	segid_attno;		/* attribute number of "gp_segment_id" */
} DMLState;

/*
 * ExecNode for Split.
 * This operator contains a Plannode in PlanState.
 * The Plannode contains indexes to the ctid, insert, delete, resjunk columns
 * needed for adding the action (Insert/Delete).
 * A MemoryContext and TupleTableSlot are maintained to keep the INSERT
 * tuple until requested.
 */
typedef struct SplitUpdateState
{
	PlanState	ps;
	bool		processInsert;	/* flag that specifies the operator's next
								 * action. */
	TupleTableSlot *insertTuple;	/* tuple to Insert */
	TupleTableSlot *deleteTuple;	/* tuple to Delete */
} SplitUpdateState;

/*
 * ExecNode for AssertOp.
 * This operator contains a Plannode that contains the expressions
 * to execute.
 */
typedef struct AssertOpState
{
	PlanState	ps;
} AssertOpState;

/*
 * ExecNode for RowTrigger.
 * This operator contains a Plannode that contains the triggers
 * to execute.
 */
typedef struct RowTriggerState
{
	PlanState	ps;
	TupleTableSlot *newTuple;	/* stores new values */
	TupleTableSlot *oldTuple;	/* stores old values */
	TupleTableSlot *triggerTuple;		/* stores returned values by the
										 * trigger */

} RowTriggerState;


typedef enum MotionStateType
{
	MOTIONSTATE_NONE,			/* The motion state is not decided, or non
								 * active in a slice (neither send nor recv) */
	MOTIONSTATE_SEND,			/* The motion is sender */
	MOTIONSTATE_RECV,			/* The motion is recver */
} MotionStateType;

/* ----------------
 *         MotionState information
 * ----------------
 */
typedef struct MotionState
{
	PlanState	ps;				/* its first field is NodeTag */
	MotionStateType mstype;		/* Motion state type */
	bool		stopRequested;	/* set when we want transfer to stop */

	/* For motion send */
	bool		sentEndOfStream;	/* set when end-of-stream has successfully been sent */
	List	   *hashExprs;		/* state struct used for evaluating the hash expressions */
	struct CdbHash *cdbhash;	/* hash api object */

	/* For Motion recv */
	int			routeIdNext;	/* for a sorted motion node, the routeId to get next (same as
								 * the routeId last returned ) */
	bool		tupleheapReady; /* for a sorted motion node, false until we have a tuple from
								 * each source segindex */

	/* For sorted Motion recv */
	struct MotionMKHeapContext *tupleheap_mk;		/* data structure for match merge in sorted motion node */

	struct binaryheap *tupleheap;
	struct CdbTupleHeapInfo *tupleheap_entries;
	struct CdbMergeComparatorContext *tupleheap_cxt;

	/* The following can be used for debugging, usage stats, etc.  */
	int			numTuplesFromChild;	/* Number of tuples received from child */
	int			numTuplesToAMS;		/* Number of tuples from child that were sent to AMS */
	int			numTuplesFromAMS;	/* Number of tuples received from AMS */
	int			numTuplesToParent;	/* Number of tuples either from child or AMS that were sent to parent */

	struct timeval otherTime;   /* time accumulator used in sending motion node to keep track of time
								 * spent getting the next tuple (not sending). this could mean time spent
								 * in another motion node receiving. */

	struct timeval motionTime;  /* time accumulator for time spent in motion node.  For sending motion node
								 * it is just the amount of time actually sending the tuple thru the
								 * interconnect.  For receiving motion node, it is the time spent waiting
								 * and processing of the next incoming tuple.
								 */

	Oid		   *outputFunArray;	/* output functions for each column (debug only) */

	int			numInputSegs;	/* the number of segments on the sending slice */
	bool		isExplictGatherMotion;
} MotionState;

/*zx
 * ExecNode for PartitionSelector.
 * This operator contains a Plannode in PlanState.
 */
typedef struct PartitionSelectorState
{
	PlanState ps;                                       /* its first field is NodeTag */
	PartitionNode *rootPartitionNode;                   /* PartitionNode for root table */
	PartitionAccessMethods *accessMethods;              /* Access method for partition */
	struct PartitionRule **levelPartRules; 				/* accepted partitions for all levels */
	List *levelEqExprStates;                            /* ExprState for equality expressions for all levels */
	List *levelExprStateLists;                          /* ExprState list for general expressions for all levels */
	List *residualPredicateExprStateList;               /* ExprState list for evaluating residual predicate */
	ExprState *propagationExprState;                    /* ExprState for evaluating propagation expression */

	TupleDesc	partTabDesc;
	TupleTableSlot *partTabSlot;
	ProjectionInfo *partTabProj;
} PartitionSelectorState;

#endif   /* EXECNODES_H */
