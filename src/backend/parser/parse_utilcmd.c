/*-------------------------------------------------------------------------
 *
 * parse_utilcmd.c
 *	  Perform parse analysis work for various utility commands
 *
 * Formerly we did this work during parse_analyze() in analyze.c.  However
 * that is fairly unsafe in the presence of querytree caching, since any
 * database state that we depend on in making the transformations might be
 * obsolete by the time the utility command is executed; and utility commands
 * have no infrastructure for holding locks or rechecking plan validity.
 * Hence these functions are now called at the start of execution of their
 * respective utility commands.
 *
 * NOTE: in general we must avoid scribbling on the passed-in raw parse
 * tree, since it might be in a plan cache.  The simplest solution is
 * a quick copyObject() call before manipulating the query tree.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/backend/parser/parse_utilcmd.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/pg_compression.h"
#include "catalog/pg_constraint.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_encoding.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_partition.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "parser/parser.h"
#include "rewrite/rewriteManip.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "cdb/cdbhash.h"
#include "cdb/cdbpartition.h"
#include "cdb/partitionselection.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "utils/guc.h"
#include "utils/tqual.h"

/* State shared by transformCreateSchemaStmt and its subroutines */
typedef struct
{
	const char *stmtType;		/* "CREATE SCHEMA" or "ALTER SCHEMA" */
	char	   *schemaname;		/* name of schema */
	char	   *authid;			/* owner of schema */
	List	   *sequences;		/* CREATE SEQUENCE items */
	List	   *tables;			/* CREATE TABLE items */
	List	   *views;			/* CREATE VIEW items */
	List	   *indexes;		/* CREATE INDEX items */
	List	   *triggers;		/* CREATE TRIGGER items */
	List	   *grants;			/* GRANT items */
} CreateSchemaStmtContext;


static void transformColumnDefinition(CreateStmtContext *cxt,
						  ColumnDef *column);
static void transformTableConstraint(CreateStmtContext *cxt,
						 Constraint *constraint);
static void transformTableLikeClause(CreateStmtContext *cxt,
						 TableLikeClause *table_like_clause,
						 bool forceBareCol, CreateStmt *stmt, List **stenc);
static void transformOfType(CreateStmtContext *cxt,
				TypeName *ofTypename);
static IndexStmt *generateClonedIndexStmt(CreateStmtContext *cxt,
						Relation source_idx,
						const AttrNumber *attmap, int attmap_length);
static List *get_collation(Oid collation, Oid actual_datatype);
static List *get_opclass(Oid opclass, Oid actual_datatype);
static void transformIndexConstraints(CreateStmtContext *cxt, bool mayDefer);
static IndexStmt *transformIndexConstraint(Constraint *constraint,
						 CreateStmtContext *cxt);
static void transformFKConstraints(CreateStmtContext *cxt,
					   bool skipValidation,
					   bool isAddConstraint);
static void transformConstraintAttrs(CreateStmtContext *cxt,
						 List *constraintList);
static void transformColumnType(CreateStmtContext *cxt, ColumnDef *column);
static void setSchemaName(char *context_schema, char **stmt_schema_name);

static DistributedBy *getLikeDistributionPolicy(TableLikeClause *e);
static bool co_explicitly_disabled(List *opts);
static DistributedBy *transformDistributedBy(CreateStmtContext *cxt,
					   DistributedBy *distributedBy,
					   DistributedBy *likeDistributedBy,
					   bool bQuiet);
static List *transformAttributeEncoding(List *stenc, CreateStmt *stmt,
										CreateStmtContext *cxt);
static bool encodings_overlap(List *a, List *b, bool test_conflicts);

static AlterTableCmd *transformAlterTable_all_PartitionStmt(ParseState *pstate,
									  AlterTableStmt *stmt,
									  CreateStmtContext *pCxt,
									  AlterTableCmd *cmd);

static IdentifiedBy *transformIdentifiedBy(CreateStmtContext *cxt);
static bool is_rocksdb(List *opts);
static bool is_rocksdb_global(void);
static List *add_extra_rocksdb_option(List *opts);
static bool have_primary_key(List *elts);
static ColumnDef *makeMyColumnDef(void);
static Constraint *makeMyConstraint(void);

/*
 * transformCreateStmt -
 *	  parse analysis for CREATE TABLE
 *
 * Returns a List of utility commands to be done in sequence.  One of these
 * will be the transformed CreateStmt, but there may be additional actions
 * to be done before and after the actual DefineRelation() call.
 *
 * SQL allows constraints to be scattered all over, so thumb through
 * the columns and collect all constraints into one place.
 * If there are any implied indices (e.g. UNIQUE or PRIMARY KEY)
 * then expand those into multiple IndexStmt blocks.
 *	  - thomas 1997-12-02
 */
List *
transformCreateStmt(CreateStmt *stmt, const char *queryString, bool createPartition)
{
	ParseState *pstate;
	CreateStmtContext cxt;
	List	   *result;
	List	   *save_alist;
	List	   *save_root_partition_alist = NIL;
	ListCell   *elements;
	Oid			namespaceid;
	Oid			existing_relid;
	DistributedBy *likeDistributedBy = NULL;
	bool		bQuiet = false;		/* shut up transformDistributedBy messages */
	List	   *stenc = NIL;		/* column reference storage encoding clauses */

 	/*
	 * We don't normally care much about the memory consumption of parsing,
	 * because any memory leaked is leaked into MessageContext which is
	 * reset between each command. But if a table is heavily partitioned,
	 * the CREATE TABLE statement can be expanded into hundreds or even
	 * thousands of CreateStmts, so the leaks start to add up. To reduce
	 * the memory consumption, we use a temporary memory context that's
	 * destroyed after processing the CreateStmt for some parts of the
	 * processing.
	 */
	cxt.tempCtx =
		AllocSetContextCreate(CurrentMemoryContext,
							  "CreateStmt analyze context",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * We must not scribble on the passed-in CreateStmt, so copy it.  (This is
	 * overkill, but easy.)
	 */
	stmt = (CreateStmt *) copyObject(stmt);

	/*
	 * Look up the creation namespace.  This also checks permissions on the
	 * target namespace, locks it against concurrent drops, checks for a
	 * preexisting relation in that namespace with the same name, and updates
	 * stmt->relation->relpersistence if the select namespace is temporary.
	 */
	namespaceid =
		RangeVarGetAndCheckCreationNamespace(stmt->relation, NoLock,
											 &existing_relid);

	/*
	 * If the relation already exists and the user specified "IF NOT EXISTS",
	 * bail out with a NOTICE.
	 */
	if (stmt->if_not_exists && OidIsValid(existing_relid))
	{
		ereport(NOTICE,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists, skipping",
						stmt->relation->relname)));
		return NIL;
	}

	/*
	 * If the target relation name isn't schema-qualified, make it so. This
	 * prevents some corner cases in which added-on rewritten commands might
	 * think they should apply to other relations that have the same name and
	 * are earlier in the search path.  But a local temp table is effectively
	 * specified to be in pg_temp, so no need for anything extra in that case.
	 */
	if (stmt->relation->schemaname == NULL &&
	    stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
		stmt->relation->schemaname = get_namespace_name(namespaceid);

	/* Set up pstate and CreateStmtContext */
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	cxt.pstate = pstate;
	if (IsA(stmt, CreateForeignTableStmt))
	{
		cxt.stmtType = "CREATE FOREIGN TABLE";
		cxt.isforeign = true;
	}
	else
	{
		cxt.stmtType = "CREATE TABLE";
		cxt.isforeign = false;
	}
	cxt.relation = stmt->relation;
	cxt.rel = NULL;
	cxt.inhRelations = stmt->inhRelations;
	cxt.isalter = false;
	cxt.iscreatepart = createPartition;
	cxt.issplitpart = stmt->is_split_part;
	cxt.columns = NIL;
	cxt.ckconstraints = NIL;
	cxt.fkconstraints = NIL;
	cxt.ixconstraints = NIL;
	cxt.inh_indexes = NIL;
	cxt.blist = NIL;
	cxt.alist = NIL;
	cxt.dlist = NIL; /* for deferred analysis requiring the created table */
	cxt.pkey = NULL;
	cxt.hasoids = interpretOidsOption(stmt->options, true);

	Assert(!stmt->ofTypename || !stmt->inhRelations);	/* grammar enforces */

	if (stmt->ofTypename)
		transformOfType(&cxt, stmt->ofTypename);

	/* Disallow inheritance in combination with partitioning. */
	if (stmt->inhRelations && (stmt->partitionBy || stmt->is_part_child))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot mix inheritance with partitioning")));

	/* Disallow inheritance for CO table */
	if (stmt->inhRelations && is_aocs(stmt->options))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("INHERITS clause cannot be used with column oriented tables")));

	/*
	 * GPDB_91_MERGE_FIXME: Previous gpdb does not allow create
	 * partition on temp table. Let's follow this at this moment
	 * although we do do not understand why even we know temp
	 * partition table seems to be not practical. Previous gpdb
	 * does not have this issue since in make_child_node()
	 * child_tab_name->istemp is not assigned and it's default
	 * value is false.
	 */
	if ((stmt->partitionBy || stmt->is_part_child) &&
	    stmt->relation->relpersistence == RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot create partition inherited from temporary relation")));

	/**
	 * RocksDB table should be regular and permanent.
	 */
	if ((stmt->relation->relpersistence != RELPERSISTENCE_PERMANENT ||
		 stmt->relKind != RELKIND_RELATION) &&
		is_rocksdb(stmt->options))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("rocksdb storage engine is only available for regular and permanent table")));

	/* Only on top-most partitioned tables. */
	if (stmt->partitionBy && !stmt->is_part_child)
		fixCreateStmtForPartitionedTable(stmt);

	/* 
	 * Rafferty Yu
	 * If store_mode is set to rocksdb, create rocksdb table by defauly. 
	 * Add extra rocksdb options if necessary.
	 */
	if (is_rocksdb_global() &&
	    !is_rocksdb(stmt->options) &&
	    stmt->relation->relpersistence ==RELPERSISTENCE_PERMANENT &&
		stmt->relKind == RELKIND_RELATION)
	{
		stmt->options = add_extra_rocksdb_option(stmt->options);
	}

	if (is_rocksdb_global() && !have_primary_key(stmt->tableElts))
	{
		if (stmt->tableElts)
		{
                        stmt->tableElts = lappend(stmt->tableElts, makeMyColumnDef());
			//((ColumnDef *)stmt->tableElts->head->data.ptr_value)->constraints =
			//    lappend(((ColumnDef *)stmt->tableElts->head->data.ptr_value)->constraints,         makeMyConstraint());
			
		}
		else
		{
			stmt->tableElts = list_make1(makeMyColumnDef());
		}
	}

	/*
	 * Run through each primary element in the table creation clause. Separate
	 * column defs from constraints, and do preliminary analysis.
	 */
	foreach(elements, stmt->tableElts)
	{
		Node	   *element = lfirst(elements);

		switch (nodeTag(element))
		{
			case T_ColumnDef:
				transformColumnDefinition(&cxt, (ColumnDef *) element);
				break;

			case T_Constraint:
				transformTableConstraint(&cxt, (Constraint *) element);
				break;

			case T_TableLikeClause:
				{
					bool            isBeginning = (cxt.columns == NIL);

					transformTableLikeClause(&cxt, (TableLikeClause *) element, false, stmt, &stenc);

					if (Gp_role == GP_ROLE_DISPATCH && isBeginning &&
						stmt->distributedBy == NULL &&
						stmt->inhRelations == NIL)
					{
						likeDistributedBy = getLikeDistributionPolicy((TableLikeClause*) element);
					}
				}
				break;

			case T_ColumnReferenceStorageDirective:
				/* processed below in transformAttributeEncoding() */
				stenc = lappend(stenc, element);
				break;

			default:
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(element));
				break;
		}
	}

	/*
	 * transformIndexConstraints wants cxt.alist to contain only index
	 * statements, so transfer anything we already have into save_alist.
	 */
	save_alist = cxt.alist;
	cxt.alist = NIL;

	Assert(stmt->constraints == NIL);

	/*
	 * Postprocess constraints that give rise to index definitions.
	 */
	if (!stmt->is_part_child || stmt->is_split_part || stmt->is_add_part)
		transformIndexConstraints(&cxt, stmt->is_add_part || stmt->is_split_part);

	/*
	 * Rocksdb must have primary key.
	 * Check if there is primary key definition in the statement, and transfer
	 * the primary key into identified key for rocksdb. stmt.identifiedBy is
	 * allocated and initialized here.
	 */
	if (is_rocksdb(stmt->options))
	{
		IdentifiedBy *identBy = transformIdentifiedBy(&cxt);
		if (identBy == NULL)
		{
			ereport(ERROR,
				    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				     errmsg("primary key must be defined for rocksdb table")));
		}
		else
		{
			stmt->identifiedBy = identBy;
		}
	}

	/*
	 * Carry any deferred analysis statements forward.  Added for MPP-13750
	 * but should also apply to the similar case involving simple inheritance.
	 */
	if (cxt.dlist)
	{
		stmt->deferredStmts = list_concat(stmt->deferredStmts, cxt.dlist);
		cxt.dlist = NIL;
	}

	/*
	 * Postprocess foreign-key constraints.
	 * But don't cascade FK constraints to parts, yet.
	 */
	if (!stmt->is_part_child)
		transformFKConstraints(&cxt, true, false);

	/*-----------
	 * Analyze attribute encoding clauses.
	 *
	 * Partitioning configurations may have things like:
	 *
	 * CREATE TABLE ...
	 *  ( a int ENCODING (...))
	 * WITH (appendonly=true, orientation=column)
	 * PARTITION BY ...
	 * (PARTITION ... WITH (appendonly=false));
	 *
	 * We don't want to throw an error when we try to apply the ENCODING clause
	 * to the partition which the user wants to be non-AO. Just ignore it
	 * instead.
	 *-----------
	 */
	if (!is_aocs(stmt->options) && stmt->is_part_child)
	{
		if (co_explicitly_disabled(stmt->options) || !stenc)
			stmt->attr_encodings = NIL;
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ENCODING clause only supported with column oriented partitioned tables")));
		}
	}
	else
		stmt->attr_encodings = transformAttributeEncoding(stenc, stmt, &cxt);

	/*
	 * Postprocess Greenplum Database distribution columns
	 */
	/* silence distro messages for partitions */
	if (stmt->is_part_child)
		bQuiet = true;
	else if (stmt->partitionBy)
	{
		PartitionBy *partitionBy = (PartitionBy *) stmt->partitionBy;

		/* be very quiet if set subpartn template */
		if (partitionBy->partQuiet == PART_VERBO_NOPARTNAME)
			bQuiet = true;
		/* quiet for partitions of depth > 0 */
		else if (partitionBy->partDepth != 0 &&
				 partitionBy->partQuiet != PART_VERBO_NORMAL)
			bQuiet = true;
	}

	/*
	 * Transform DISTRIBUTED BY (or constuct a default one, if not given
	 *  explicitly). Not for foreign tables, though.
	 */
	if (stmt->relKind == RELKIND_RELATION)
	{
		int			numsegments = -1;

		AssertImply(stmt->is_part_parent,
					stmt->distributedBy == NULL);
		AssertImply(stmt->is_part_child,
					stmt->distributedBy != NULL);

		/*
		 * We want children have the same numsegments with parent.  As
		 * transformDistributedBy() always set numsegments to DEFAULT, does
		 * this meet our expectation?  No, because DEFAULT does not always
		 * equal to DEFAULT itself.  When DEFAULT is set to RANDOM a different
		 * value is returned each time.
		 *
		 * So we have to save the parent numsegments here.
		 */
		if (stmt->is_part_child)
			numsegments = stmt->distributedBy->numsegments;

		stmt->distributedBy = transformDistributedBy(&cxt, stmt->distributedBy,
							   likeDistributedBy, bQuiet);

		/*
		 * And forcely set it on children after transformDistributedBy().
		 */
		if (stmt->is_part_child)
			stmt->distributedBy->numsegments = numsegments;
	}

	if (stmt->partitionBy != NULL &&
		stmt->distributedBy &&
		stmt->distributedBy->ptype == POLICYTYPE_REPLICATED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("PARTITION BY clause cannot be used with DISTRIBUTED REPLICATED clause")));

	/*
	 * Save the alist for root partitions before transformPartitionBy adds the
	 * child create statements.
	 */
	if (stmt->partitionBy && !stmt->is_part_child)
	{
		save_root_partition_alist = cxt.alist;
		cxt.alist = NIL;
	}

	/*
	 * Process table partitioning clause
	 */
	transformPartitionBy(&cxt, stmt, stmt->partitionBy);

	/*
	 * Output results.
	 */
	stmt->tableElts = cxt.columns;
	stmt->constraints = cxt.ckconstraints;

	result = lappend(cxt.blist, stmt);
	result = list_concat(result, cxt.alist);
	if (stmt->partitionBy && !stmt->is_part_child)
		result = list_concat(result, save_root_partition_alist);
	result = list_concat(result, save_alist);

	MemoryContextDelete(cxt.tempCtx);

	return result;
}

/*
 * transformColumnDefinition -
 *		transform a single ColumnDef within CREATE TABLE
 *		Also used in ALTER TABLE ADD COLUMN
 */
static void
transformColumnDefinition(CreateStmtContext *cxt, ColumnDef *column)
{
	bool		is_serial;
	bool		saw_nullable;
	bool		saw_default;
	Constraint *constraint;
	ListCell   *clist;

	cxt->columns = lappend(cxt->columns, column);

	/* Check for SERIAL pseudo-types */
	is_serial = false;
	if (column->typeName
		&& list_length(column->typeName->names) == 1
		&& !column->typeName->pct_type)
	{
		char	   *typname = strVal(linitial(column->typeName->names));

		if (strcmp(typname, "smallserial") == 0 ||
			strcmp(typname, "serial2") == 0)
		{
			is_serial = true;
			column->typeName->names = NIL;
			column->typeName->typeOid = INT2OID;
		}
		else if (strcmp(typname, "serial") == 0 ||
				 strcmp(typname, "serial4") == 0)
		{
			is_serial = true;
			column->typeName->names = NIL;
			column->typeName->typeOid = INT4OID;
		}
		else if (strcmp(typname, "bigserial") == 0 ||
				 strcmp(typname, "serial8") == 0)
		{
			is_serial = true;
			column->typeName->names = NIL;
			column->typeName->typeOid = INT8OID;
		}

		/*
		 * We have to reject "serial[]" explicitly, because once we've set
		 * typeid, LookupTypeName won't notice arrayBounds.  We don't need any
		 * special coding for serial(typmod) though.
		 */
		if (is_serial && column->typeName->arrayBounds != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("array of serial is not implemented"),
					 parser_errposition(cxt->pstate,
										column->typeName->location)));
	}

	/* Do necessary work on the column type declaration */
	if (column->typeName)
		transformColumnType(cxt, column);

	/* Special actions for SERIAL pseudo-types */
	if (is_serial)
	{
		Oid			snamespaceid;
		char	   *snamespace;
		char	   *sname;
		char	   *qstring;
		A_Const    *snamenode;
		TypeCast   *castnode;
		FuncCall   *funccallnode;
		CreateSeqStmt *seqstmt;
		AlterSeqStmt *altseqstmt;
		List	   *attnamelist;

		/*
		 * Determine namespace and name to use for the sequence.
		 *
		 * Although we use ChooseRelationName, it's not guaranteed that the
		 * selected sequence name won't conflict; given sufficiently long
		 * field names, two different serial columns in the same table could
		 * be assigned the same sequence name, and we'd not notice since we
		 * aren't creating the sequence quite yet.  In practice this seems
		 * quite unlikely to be a problem, especially since few people would
		 * need two serial columns in one table.
		 */
		if (cxt->rel)
			snamespaceid = RelationGetNamespace(cxt->rel);
		else
		{
			snamespaceid = RangeVarGetCreationNamespace(cxt->relation);
			RangeVarAdjustRelationPersistence(cxt->relation, snamespaceid);
		}
		snamespace = get_namespace_name(snamespaceid);
		sname = ChooseRelationName(cxt->relation->relname,
								   column->colname,
								   "seq",
								   snamespaceid);

		ereport(DEBUG1,
				(errmsg("%s will create implicit sequence \"%s\" for serial column \"%s.%s\"",
						cxt->stmtType, sname,
						cxt->relation->relname, column->colname)));

		/*
		 * Build a CREATE SEQUENCE command to create the sequence object, and
		 * add it to the list of things to be done before this CREATE/ALTER
		 * TABLE.
		 */
		seqstmt = makeNode(CreateSeqStmt);
		seqstmt->sequence = makeRangeVar(snamespace, sname, -1);
		seqstmt->sequence->relpersistence = cxt->relation->relpersistence;
		seqstmt->options = NIL;

		/*
		 * If this is ALTER ADD COLUMN, make sure the sequence will be owned
		 * by the table's owner.  The current user might be someone else
		 * (perhaps a superuser, or someone who's only a member of the owning
		 * role), but the SEQUENCE OWNED BY mechanisms will bleat unless table
		 * and sequence have exactly the same owning role.
		 */
		if (cxt->rel)
			seqstmt->ownerId = cxt->rel->rd_rel->relowner;
		else
			seqstmt->ownerId = InvalidOid;

		cxt->blist = lappend(cxt->blist, seqstmt);

		/*
		 * Build an ALTER SEQUENCE ... OWNED BY command to mark the sequence
		 * as owned by this column, and add it to the list of things to be
		 * done after this CREATE/ALTER TABLE.
		 */
		altseqstmt = makeNode(AlterSeqStmt);
		altseqstmt->sequence = makeRangeVar(snamespace, sname, -1);
		attnamelist = list_make3(makeString(snamespace),
								 makeString(cxt->relation->relname),
								 makeString(column->colname));
		altseqstmt->options = list_make1(makeDefElem("owned_by",
													 (Node *) attnamelist));

		cxt->alist = lappend(cxt->alist, altseqstmt);

		/*
		 * Create appropriate constraints for SERIAL.  We do this in full,
		 * rather than shortcutting, so that we will detect any conflicting
		 * constraints the user wrote (like a different DEFAULT).
		 *
		 * Create an expression tree representing the function call
		 * nextval('sequencename').  We cannot reduce the raw tree to cooked
		 * form until after the sequence is created, but there's no need to do
		 * so.
		 */
		qstring = quote_qualified_identifier(snamespace, sname);
		snamenode = makeNode(A_Const);
		snamenode->val.type = T_String;
		snamenode->val.val.str = qstring;
		snamenode->location = -1;
		castnode = makeNode(TypeCast);
		castnode->typeName = SystemTypeName("regclass");
		castnode->arg = (Node *) snamenode;
		castnode->location = -1;
		funccallnode = makeFuncCall(SystemFuncName("nextval"),
									list_make1(castnode),
									-1);
		constraint = makeNode(Constraint);
		constraint->contype = CONSTR_DEFAULT;
		constraint->location = -1;
		constraint->raw_expr = (Node *) funccallnode;
		constraint->cooked_expr = NULL;
		column->constraints = lappend(column->constraints, constraint);

		constraint = makeNode(Constraint);
		constraint->contype = CONSTR_NOTNULL;
		constraint->location = -1;
		column->constraints = lappend(column->constraints, constraint);
	}

	/* Process column constraints, if any... */
	transformConstraintAttrs(cxt, column->constraints);

	saw_nullable = false;
	saw_default = false;

	foreach(clist, column->constraints)
	{
		constraint = lfirst(clist);
		Assert(IsA(constraint, Constraint));

		switch (constraint->contype)
		{
			case CONSTR_NULL:
				if (saw_nullable && column->is_not_null)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting NULL/NOT NULL declarations for column \"%s\" of table \"%s\"",
									column->colname, cxt->relation->relname),
							 parser_errposition(cxt->pstate,
												constraint->location)));
				column->is_not_null = FALSE;
				saw_nullable = true;
				break;

			case CONSTR_NOTNULL:
				if (saw_nullable && !column->is_not_null)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting NULL/NOT NULL declarations for column \"%s\" of table \"%s\"",
									column->colname, cxt->relation->relname),
							 parser_errposition(cxt->pstate,
												constraint->location)));
				column->is_not_null = TRUE;
				saw_nullable = true;
				break;

			case CONSTR_DEFAULT:
				if (saw_default)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("multiple default values specified for column \"%s\" of table \"%s\"",
									column->colname, cxt->relation->relname),
							 parser_errposition(cxt->pstate,
												constraint->location)));
				column->raw_default = constraint->raw_expr;
				Assert(constraint->cooked_expr == NULL);
				saw_default = true;
				break;

			case CONSTR_CHECK:
				if (cxt->isforeign)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("constraints are not supported on foreign tables"),
							 parser_errposition(cxt->pstate,
												constraint->location)));
				cxt->ckconstraints = lappend(cxt->ckconstraints, constraint);
				break;

			case CONSTR_PRIMARY:
			case CONSTR_UNIQUE:
				if (cxt->isforeign)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("constraints are not supported on foreign tables"),
							 parser_errposition(cxt->pstate,
												constraint->location)));
				if (constraint->keys == NIL)
					constraint->keys = list_make1(makeString(column->colname));
				cxt->ixconstraints = lappend(cxt->ixconstraints, constraint);
				break;

			case CONSTR_EXCLUSION:
				/* grammar does not allow EXCLUDE as a column constraint */
				elog(ERROR, "column exclusion constraints are not supported");
				break;

			case CONSTR_FOREIGN:
				if (cxt->isforeign)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("constraints are not supported on foreign tables"),
							 parser_errposition(cxt->pstate,
												constraint->location)));

				/*
				 * Fill in the current attribute's name and throw it into the
				 * list of FK constraints to be processed later.
				 */
				constraint->fk_attrs = list_make1(makeString(column->colname));
				cxt->fkconstraints = lappend(cxt->fkconstraints, constraint);
				break;

			case CONSTR_ATTR_DEFERRABLE:
			case CONSTR_ATTR_NOT_DEFERRABLE:
			case CONSTR_ATTR_DEFERRED:
			case CONSTR_ATTR_IMMEDIATE:
				/* transformConstraintAttrs took care of these */
				break;

			default:
				elog(ERROR, "unrecognized constraint type: %d",
					 constraint->contype);
				break;
		}
	}

	/*
	 * If needed, generate ALTER FOREIGN TABLE ALTER COLUMN statement to add
	 * per-column foreign data wrapper options to this column after creation.
	 */
	if (column->fdwoptions != NIL)
	{
		AlterTableStmt *stmt;
		AlterTableCmd *cmd;

		cmd = makeNode(AlterTableCmd);
		cmd->subtype = AT_AlterColumnGenericOptions;
		cmd->name = column->colname;
		cmd->def = (Node *) column->fdwoptions;
		cmd->behavior = DROP_RESTRICT;
		cmd->missing_ok = false;

		stmt = makeNode(AlterTableStmt);
		stmt->relation = cxt->relation;
		stmt->cmds = NIL;
		stmt->relkind = OBJECT_FOREIGN_TABLE;
		stmt->cmds = lappend(stmt->cmds, cmd);

		cxt->alist = lappend(cxt->alist, stmt);
	}
}

/*
 * transformTableConstraint
 *		transform a Constraint node within CREATE TABLE or ALTER TABLE
 */
static void
transformTableConstraint(CreateStmtContext *cxt, Constraint *constraint)
{
	if (cxt->isforeign)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("constraints are not supported on foreign tables"),
				 parser_errposition(cxt->pstate,
									constraint->location)));

	if (constraint->contype == CONSTR_EXCLUSION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("GPDB does not support exclusion constraints.")));

	switch (constraint->contype)
	{
		case CONSTR_PRIMARY:
		case CONSTR_UNIQUE:
		case CONSTR_EXCLUSION:
			cxt->ixconstraints = lappend(cxt->ixconstraints, constraint);
			break;

		case CONSTR_CHECK:
			cxt->ckconstraints = lappend(cxt->ckconstraints, constraint);
			break;

		case CONSTR_FOREIGN:
			cxt->fkconstraints = lappend(cxt->fkconstraints, constraint);
			break;

		case CONSTR_NULL:
		case CONSTR_NOTNULL:
		case CONSTR_DEFAULT:
		case CONSTR_ATTR_DEFERRABLE:
		case CONSTR_ATTR_NOT_DEFERRABLE:
		case CONSTR_ATTR_DEFERRED:
		case CONSTR_ATTR_IMMEDIATE:
			elog(ERROR, "invalid context for constraint type %d",
				 constraint->contype);
			break;

		default:
			elog(ERROR, "unrecognized constraint type: %d",
				 constraint->contype);
			break;
	}
}

/*
 * transformTableLikeClause
 *
 * Change the LIKE <srctable> portion of a CREATE TABLE statement into
 * column definitions which recreate the user defined column portions of
 * <srctable>.
 *
 * GPDB: if forceBareCol is true we disallow inheriting any indexes/constr/defaults.
 */
static void
transformTableLikeClause(CreateStmtContext *cxt, TableLikeClause *table_like_clause,
						 bool forceBareCol, CreateStmt *stmt, List **stenc)
{
	AttrNumber	parent_attno;
	Relation	relation;
	TupleDesc	tupleDesc;
	TupleConstr *constr;
	AttrNumber *attmap;
	AclResult	aclresult;
	char	   *comment;
	ParseCallbackState pcbstate;
	MemoryContext oldcontext;

	setup_parser_errposition_callback(&pcbstate, cxt->pstate,
									  table_like_clause->relation->location);

	/* we could support LIKE in many cases, but worry about it another day */
	if (cxt->isforeign)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			   errmsg("LIKE is not supported for creating foreign tables")));

	relation = relation_openrv(table_like_clause->relation, AccessShareLock);

	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_VIEW &&
		relation->rd_rel->relkind != RELKIND_MATVIEW &&
		relation->rd_rel->relkind != RELKIND_COMPOSITE_TYPE &&
		relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table, view, materialized view, composite type, or foreign table",
						RelationGetRelationName(relation))));

	cancel_parser_errposition_callback(&pcbstate);

	/*
	 * Check for privileges
	 */
	if (relation->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
	{
		aclresult = pg_type_aclcheck(relation->rd_rel->reltype, GetUserId(),
									 ACL_USAGE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TYPE,
						   RelationGetRelationName(relation));
	}
	else
	{
		aclresult = pg_class_aclcheck(RelationGetRelid(relation), GetUserId(),
									  ACL_SELECT);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_CLASS,
						   RelationGetRelationName(relation));
	}

	tupleDesc = RelationGetDescr(relation);
	constr = tupleDesc->constr;

	if (forceBareCol && table_like_clause->options != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("LIKE INCLUDING may not be used with this kind of relation")));

	/*
	 * Initialize column number map for map_variable_attnos().  We need this
	 * since dropped columns in the source table aren't copied, so the new
	 * table can have different column numbers.
	 */
	attmap = (AttrNumber *) palloc0(sizeof(AttrNumber) * tupleDesc->natts);

	/*
	 * Insert the copied attributes into the cxt for the new table definition.
	 */
	for (parent_attno = 1; parent_attno <= tupleDesc->natts;
		 parent_attno++)
	{
		Form_pg_attribute attribute = tupleDesc->attrs[parent_attno - 1];
		char	   *attributeName = NameStr(attribute->attname);
		ColumnDef  *def;

		/*
		 * Ignore dropped columns in the parent.  attmap entry is left zero.
		 */
		if (attribute->attisdropped)
			continue;

		/*
		 * Create a new column, which is marked as NOT inherited.
		 *
		 * For constraints, ONLY the NOT NULL constraint is inherited by the
		 * new column definition per SQL99.
		 */
		def = makeNode(ColumnDef);
		def->colname = pstrdup(attributeName);
		def->typeName = makeTypeNameFromOid(attribute->atttypid,
											attribute->atttypmod);
		def->inhcount = 0;
		def->is_local = true;
		def->is_not_null = (forceBareCol ? false : attribute->attnotnull);
		def->is_from_type = false;
		def->storage = 0;
		def->raw_default = NULL;
		def->cooked_default = NULL;
		def->collClause = NULL;
		def->collOid = attribute->attcollation;
		def->constraints = NIL;
		def->location = -1;

		/*
		 * Add to column list
		 */
		cxt->columns = lappend(cxt->columns, def);

		attmap[parent_attno - 1] = list_length(cxt->columns);

		/*
		 * Copy default, if present and the default has been requested
		 */
		if (attribute->atthasdef &&
			(table_like_clause->options & CREATE_TABLE_LIKE_DEFAULTS))
		{
			Node	   *this_default = NULL;
			AttrDefault *attrdef;
			int			i;

			/* Find default in constraint structure */
			Assert(constr != NULL);
			attrdef = constr->defval;
			for (i = 0; i < constr->num_defval; i++)
			{
				if (attrdef[i].adnum == parent_attno)
				{
					this_default = stringToNode(attrdef[i].adbin);
					break;
				}
			}
			Assert(this_default != NULL);

			/*
			 * If default expr could contain any vars, we'd need to fix 'em,
			 * but it can't; so default is ready to apply to child.
			 */

			def->cooked_default = this_default;
		}

		/* Likewise, copy storage if requested */
		if (table_like_clause->options & CREATE_TABLE_LIKE_STORAGE)
			def->storage = attribute->attstorage;
		else
			def->storage = 0;

		/* Likewise, copy comment if requested */
		if ((table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS) &&
			(comment = GetComment(attribute->attrelid,
								  RelationRelationId,
								  attribute->attnum)) != NULL)
		{
			CommentStmt *stmt = makeNode(CommentStmt);

			stmt->objtype = OBJECT_COLUMN;
			stmt->objname = list_make3(makeString(cxt->relation->schemaname),
									   makeString(cxt->relation->relname),
									   makeString(def->colname));
			stmt->objargs = NIL;
			stmt->comment = comment;

			cxt->alist = lappend(cxt->alist, stmt);
		}
	}

	/*
	 * Copy CHECK constraints if requested, being careful to adjust attribute
	 * numbers so they match the child.
	 */
	if ((table_like_clause->options & CREATE_TABLE_LIKE_CONSTRAINTS) &&
		tupleDesc->constr)
	{
		int			ccnum;

		for (ccnum = 0; ccnum < tupleDesc->constr->num_check; ccnum++)
		{
			char	   *ccname = tupleDesc->constr->check[ccnum].ccname;
			char	   *ccbin = tupleDesc->constr->check[ccnum].ccbin;
			Constraint *n = makeNode(Constraint);
			Node	   *ccbin_node;
			bool		found_whole_row;

			ccbin_node = map_variable_attnos(stringToNode(ccbin),
											 1, 0,
											 attmap, tupleDesc->natts,
											 &found_whole_row);

			/*
			 * We reject whole-row variables because the whole point of LIKE
			 * is that the new table's rowtype might later diverge from the
			 * parent's.  So, while translation might be possible right now,
			 * it wouldn't be possible to guarantee it would work in future.
			 */
			if (found_whole_row)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert whole-row table reference"),
						 errdetail("Constraint \"%s\" contains a whole-row reference to table \"%s\".",
								   ccname,
								   RelationGetRelationName(relation))));

			n->contype = CONSTR_CHECK;
			n->location = -1;
			n->conname = pstrdup(ccname);
			n->raw_expr = NULL;
			n->cooked_expr = nodeToString(ccbin_node);
			cxt->ckconstraints = lappend(cxt->ckconstraints, n);

			/* Copy comment on constraint */
			if ((table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS) &&
				(comment = GetComment(get_relation_constraint_oid(RelationGetRelid(relation),
														  n->conname, false),
									  ConstraintRelationId,
									  0)) != NULL)
			{
				CommentStmt *stmt = makeNode(CommentStmt);

				stmt->objtype = OBJECT_CONSTRAINT;
				stmt->objname = list_make3(makeString(cxt->relation->schemaname),
										   makeString(cxt->relation->relname),
										   makeString(n->conname));
				stmt->objargs = NIL;
				stmt->comment = comment;

				cxt->alist = lappend(cxt->alist, stmt);
			}
		}
	}

	/*
	 * Likewise, copy indexes if requested
	 */
	if ((table_like_clause->options & CREATE_TABLE_LIKE_INDEXES) &&
		relation->rd_rel->relhasindex)
	{
		List	   *parent_indexes;
		ListCell   *l;

		parent_indexes = RelationGetIndexList(relation);

		foreach(l, parent_indexes)
		{
			Oid			parent_index_oid = lfirst_oid(l);
			Relation	parent_index;
			IndexStmt  *index_stmt;

			parent_index = index_open(parent_index_oid, AccessShareLock);

			/* Build CREATE INDEX statement to recreate the parent_index */
			index_stmt = generateClonedIndexStmt(cxt, parent_index,
												 attmap, tupleDesc->natts);

			/* Copy comment on index, if requested */
			if (table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS)
			{
				comment = GetComment(parent_index_oid, RelationRelationId, 0);

				/*
				 * We make use of IndexStmt's idxcomment option, so as not to
				 * need to know now what name the index will have.
				 */
				index_stmt->idxcomment = comment;
			}

			/* Save it in the inh_indexes list for the time being */
			cxt->inh_indexes = lappend(cxt->inh_indexes, index_stmt);

			index_close(parent_index, AccessShareLock);
		}
	}

	/*
	 * If STORAGE is included, we need to copy over the table storage params
	 * as well as the attribute encodings.
	 */
	if (stmt && table_like_clause->options & CREATE_TABLE_LIKE_STORAGE)
	{
		/*
		 * As we are modifying the utility statement we must make sure these
		 * DefElem allocations can survive outside of this context.
		 */
		oldcontext = MemoryContextSwitchTo(CurTransactionContext);

		if (relation->rd_appendonly)
		{
			Form_pg_appendonly ao = relation->rd_appendonly;

			stmt->options = lappend(stmt->options, makeDefElem("appendonly", (Node *) makeString(pstrdup("true"))));
			if (ao->columnstore)
				stmt->options = lappend(stmt->options, makeDefElem("orientation", (Node *) makeString(pstrdup("column"))));
			stmt->options = lappend(stmt->options, makeDefElem("checksum", (Node *) makeInteger(ao->checksum)));
			stmt->options = lappend(stmt->options, makeDefElem("compresslevel", (Node *) makeInteger(ao->compresslevel)));
			if (strlen(NameStr(ao->compresstype)) > 0)
				stmt->options = lappend(stmt->options, makeDefElem("compresstype", (Node *) makeString(pstrdup(NameStr(ao->compresstype)))));
		}

		/*
		 * Set the attribute encodings.
		 */
		*stenc = list_union(*stenc, rel_get_column_encodings(relation));
		MemoryContextSwitchTo(oldcontext);
	}

	/*
	 * Close the parent rel, but keep our AccessShareLock on it until xact
	 * commit.  That will prevent someone else from deleting or ALTERing the
	 * parent before the child is committed.
	 */
	heap_close(relation, NoLock);
}

static void
transformOfType(CreateStmtContext *cxt, TypeName *ofTypename)
{
	HeapTuple	tuple;
	TupleDesc	tupdesc;
	int			i;
	Oid			ofTypeId;

	AssertArg(ofTypename);

	tuple = typenameType(NULL, ofTypename, NULL);
	check_of_type(tuple);
	ofTypeId = HeapTupleGetOid(tuple);
	ofTypename->typeOid = ofTypeId;		/* cached for later */

	tupdesc = lookup_rowtype_tupdesc(ofTypeId, -1);
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = tupdesc->attrs[i];
		ColumnDef  *n;

		if (attr->attisdropped)
			continue;

		n = makeNode(ColumnDef);
		n->colname = pstrdup(NameStr(attr->attname));
		n->typeName = makeTypeNameFromOid(attr->atttypid, attr->atttypmod);
		n->inhcount = 0;
		n->is_local = true;
		n->is_not_null = false;
		n->is_from_type = true;
		n->storage = 0;
		n->raw_default = NULL;
		n->cooked_default = NULL;
		n->collClause = NULL;
		n->collOid = attr->attcollation;
		n->constraints = NIL;
		n->location = -1;
		cxt->columns = lappend(cxt->columns, n);
	}
	DecrTupleDescRefCount(tupdesc);

	ReleaseSysCache(tuple);
}

/*
 * Generate an IndexStmt node using information from an already existing index
 * "source_idx".  Attribute numbers should be adjusted according to attmap.
 */
static IndexStmt *
generateClonedIndexStmt(CreateStmtContext *cxt, Relation source_idx,
						const AttrNumber *attmap, int attmap_length)
{
	Oid			source_relid = RelationGetRelid(source_idx);
	Form_pg_attribute *attrs = RelationGetDescr(source_idx)->attrs;
	HeapTuple	ht_idxrel;
	HeapTuple	ht_idx;
	Form_pg_class idxrelrec;
	Form_pg_index idxrec;
	Form_pg_am	amrec;
	oidvector  *indcollation;
	oidvector  *indclass;
	IndexStmt  *index;
	List	   *indexprs;
	ListCell   *indexpr_item;
	Oid			indrelid;
	Oid			constraintId = InvalidOid;
	int			keyno;
	Oid			keycoltype;
	Datum		datum;
	bool		isnull;

	/*
	 * Fetch pg_class tuple of source index.  We can't use the copy in the
	 * relcache entry because it doesn't include optional fields.
	 */
	ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(source_relid));
	if (!HeapTupleIsValid(ht_idxrel))
		elog(ERROR, "cache lookup failed for relation %u", source_relid);
	idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);

	/* Fetch pg_index tuple for source index from relcache entry */
	ht_idx = source_idx->rd_indextuple;
	idxrec = (Form_pg_index) GETSTRUCT(ht_idx);
	indrelid = idxrec->indrelid;

	/* Fetch pg_am tuple for source index from relcache entry */
	amrec = source_idx->rd_am;

	/* Extract indcollation from the pg_index tuple */
	datum = SysCacheGetAttr(INDEXRELID, ht_idx,
							Anum_pg_index_indcollation, &isnull);
	Assert(!isnull);
	indcollation = (oidvector *) DatumGetPointer(datum);

	/* Extract indclass from the pg_index tuple */
	datum = SysCacheGetAttr(INDEXRELID, ht_idx,
							Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	indclass = (oidvector *) DatumGetPointer(datum);

	/* Begin building the IndexStmt */
	index = makeNode(IndexStmt);
	index->relation = cxt->relation;
	index->accessMethod = pstrdup(NameStr(amrec->amname));
	if (OidIsValid(idxrelrec->reltablespace))
		index->tableSpace = get_tablespace_name(idxrelrec->reltablespace);
	else
		index->tableSpace = NULL;
	index->excludeOpNames = NIL;
	index->idxcomment = NULL;
	index->indexOid = InvalidOid;
	index->oldNode = InvalidOid;
	index->unique = idxrec->indisunique;
	index->primary = idxrec->indisprimary;
	index->concurrent = false;
	index->is_split_part = cxt->issplitpart;

	/*
	 * We don't try to preserve the name of the source index; instead, just
	 * let DefineIndex() choose a reasonable name.  (If we tried to preserve
	 * the name, we'd get duplicate-relation-name failures unless the source
	 * table was in a different schema.)
	 */
	index->idxname = NULL;

	/*
	 * If the index is marked PRIMARY or has an exclusion condition, it's
	 * certainly from a constraint; else, if it's not marked UNIQUE, it
	 * certainly isn't.  If it is or might be from a constraint, we have to
	 * fetch the pg_constraint record.
	 */
	if (index->primary || index->unique || idxrec->indisexclusion)
	{
		constraintId = get_index_constraint(source_relid);

		if (OidIsValid(constraintId))
		{
			HeapTuple	ht_constr;
			Form_pg_constraint conrec;

			ht_constr = SearchSysCache1(CONSTROID,
										ObjectIdGetDatum(constraintId));
			if (!HeapTupleIsValid(ht_constr))
				elog(ERROR, "cache lookup failed for constraint %u",
					 constraintId);
			conrec = (Form_pg_constraint) GETSTRUCT(ht_constr);

			index->isconstraint = true;
			index->deferrable = conrec->condeferrable;
			index->initdeferred = conrec->condeferred;

			/* If it's an exclusion constraint, we need the operator names */
			if (idxrec->indisexclusion)
			{
				Datum	   *elems;
				int			nElems;
				int			i;

				Assert(conrec->contype == CONSTRAINT_EXCLUSION);
				/* Extract operator OIDs from the pg_constraint tuple */
				datum = SysCacheGetAttr(CONSTROID, ht_constr,
										Anum_pg_constraint_conexclop,
										&isnull);
				if (isnull)
					elog(ERROR, "null conexclop for constraint %u",
						 constraintId);

				deconstruct_array(DatumGetArrayTypeP(datum),
								  OIDOID, sizeof(Oid), true, 'i',
								  &elems, NULL, &nElems);

				for (i = 0; i < nElems; i++)
				{
					Oid			operid = DatumGetObjectId(elems[i]);
					HeapTuple	opertup;
					Form_pg_operator operform;
					char	   *oprname;
					char	   *nspname;
					List	   *namelist;

					opertup = SearchSysCache1(OPEROID,
											  ObjectIdGetDatum(operid));
					if (!HeapTupleIsValid(opertup))
						elog(ERROR, "cache lookup failed for operator %u",
							 operid);
					operform = (Form_pg_operator) GETSTRUCT(opertup);
					oprname = pstrdup(NameStr(operform->oprname));
					/* For simplicity we always schema-qualify the op name */
					nspname = get_namespace_name(operform->oprnamespace);
					namelist = list_make2(makeString(nspname),
										  makeString(oprname));
					index->excludeOpNames = lappend(index->excludeOpNames,
													namelist);
					ReleaseSysCache(opertup);
				}
			}

			ReleaseSysCache(ht_constr);
		}
		else
			index->isconstraint = false;
	}
	else
		index->isconstraint = false;

	/*
	 * GPDB: If we are splitting a partition, or creating a new child
	 * partition, set the parents of the relation in the index statement.
	 */
	if (cxt->issplitpart || cxt->iscreatepart)
	{
		index->parentIndexId = source_relid;
		index->parentConstraintId = constraintId;
	}

	/* Get the index expressions, if any */
	datum = SysCacheGetAttr(INDEXRELID, ht_idx,
							Anum_pg_index_indexprs, &isnull);
	if (!isnull)
	{
		char	   *exprsString;

		exprsString = TextDatumGetCString(datum);
		indexprs = (List *) stringToNode(exprsString);
	}
	else
		indexprs = NIL;

	/* Build the list of IndexElem */
	index->indexParams = NIL;

	indexpr_item = list_head(indexprs);
	for (keyno = 0; keyno < idxrec->indnatts; keyno++)
	{
		IndexElem  *iparam;
		AttrNumber	attnum = idxrec->indkey.values[keyno];
		int16		opt = source_idx->rd_indoption[keyno];

		iparam = makeNode(IndexElem);

		if (AttributeNumberIsValid(attnum))
		{
			/* Simple index column */
			char	   *attname;

			attname = get_relid_attribute_name(indrelid, attnum);
			keycoltype = get_atttype(indrelid, attnum);

			iparam->name = attname;
			iparam->expr = NULL;
		}
		else
		{
			/* Expressional index */
			Node	   *indexkey;
			bool		found_whole_row;

			if (indexpr_item == NULL)
				elog(ERROR, "too few entries in indexprs list");
			indexkey = (Node *) lfirst(indexpr_item);
			indexpr_item = lnext(indexpr_item);

			/* Adjust Vars to match new table's column numbering */
			indexkey = map_variable_attnos(indexkey,
										   1, 0,
										   attmap, attmap_length,
										   &found_whole_row);

			/* As in transformTableLikeClause, reject whole-row variables */
			if (found_whole_row)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert whole-row table reference"),
						 errdetail("Index \"%s\" contains a whole-row table reference.",
								   RelationGetRelationName(source_idx))));

			iparam->name = NULL;
			iparam->expr = indexkey;

			keycoltype = exprType(indexkey);
		}

		/* Copy the original index column name */
		iparam->indexcolname = pstrdup(NameStr(attrs[keyno]->attname));

		/* Add the collation name, if non-default */
		iparam->collation = get_collation(indcollation->values[keyno], keycoltype);

		/* Add the operator class name, if non-default */
		iparam->opclass = get_opclass(indclass->values[keyno], keycoltype);

		iparam->ordering = SORTBY_DEFAULT;
		iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;

		/* Adjust options if necessary */
		if (amrec->amcanorder)
		{
			/*
			 * If it supports sort ordering, copy DESC and NULLS opts. Don't
			 * set non-default settings unnecessarily, though, so as to
			 * improve the chance of recognizing equivalence to constraint
			 * indexes.
			 */
			if (opt & INDOPTION_DESC)
			{
				iparam->ordering = SORTBY_DESC;
				if ((opt & INDOPTION_NULLS_FIRST) == 0)
					iparam->nulls_ordering = SORTBY_NULLS_LAST;
			}
			else
			{
				if (opt & INDOPTION_NULLS_FIRST)
					iparam->nulls_ordering = SORTBY_NULLS_FIRST;
			}
		}

		index->indexParams = lappend(index->indexParams, iparam);
	}

	/* Copy reloptions if any */
	datum = SysCacheGetAttr(RELOID, ht_idxrel,
							Anum_pg_class_reloptions, &isnull);
	if (!isnull)
		index->options = untransformRelOptions(datum);

	/* If it's a partial index, decompile and append the predicate */
	datum = SysCacheGetAttr(INDEXRELID, ht_idx,
							Anum_pg_index_indpred, &isnull);
	if (!isnull)
	{
		char	   *pred_str;
		Node	   *pred_tree;
		bool		found_whole_row;

		/* Convert text string to node tree */
		pred_str = TextDatumGetCString(datum);
		pred_tree = (Node *) stringToNode(pred_str);

		/* Adjust Vars to match new table's column numbering */
		pred_tree = map_variable_attnos(pred_tree,
										1, 0,
										attmap, attmap_length,
										&found_whole_row);

		/* As in transformTableLikeClause, reject whole-row variables */
		if (found_whole_row)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot convert whole-row table reference"),
			  errdetail("Index \"%s\" contains a whole-row table reference.",
						RelationGetRelationName(source_idx))));

		index->whereClause = pred_tree;
		/* Adjust attribute numbers */
		change_varattnos_of_a_node(index->whereClause, attmap);
	}

	/* Clean up */
	ReleaseSysCache(ht_idxrel);

	return index;
}

/*
 * get_collation		- fetch qualified name of a collation
 *
 * If collation is InvalidOid or is the default for the given actual_datatype,
 * then the return value is NIL.
 */
static List *
get_collation(Oid collation, Oid actual_datatype)
{
	List	   *result;
	HeapTuple	ht_coll;
	Form_pg_collation coll_rec;
	char	   *nsp_name;
	char	   *coll_name;

	if (!OidIsValid(collation))
		return NIL;				/* easy case */
	if (collation == get_typcollation(actual_datatype))
		return NIL;				/* just let it default */

	ht_coll = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
	if (!HeapTupleIsValid(ht_coll))
		elog(ERROR, "cache lookup failed for collation %u", collation);
	coll_rec = (Form_pg_collation) GETSTRUCT(ht_coll);

	/* For simplicity, we always schema-qualify the name */
	nsp_name = get_namespace_name(coll_rec->collnamespace);
	coll_name = pstrdup(NameStr(coll_rec->collname));
	result = list_make2(makeString(nsp_name), makeString(coll_name));

	ReleaseSysCache(ht_coll);
	return result;
}

/*
 * get_opclass			- fetch qualified name of an index operator class
 *
 * If the opclass is the default for the given actual_datatype, then
 * the return value is NIL.
 */
static List *
get_opclass(Oid opclass, Oid actual_datatype)
{
	List	   *result = NIL;
	HeapTuple	ht_opc;
	Form_pg_opclass opc_rec;

	ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
	if (!HeapTupleIsValid(ht_opc))
		elog(ERROR, "cache lookup failed for opclass %u", opclass);
	opc_rec = (Form_pg_opclass) GETSTRUCT(ht_opc);

	if (GetDefaultOpClass(actual_datatype, opc_rec->opcmethod) != opclass)
	{
		/* For simplicity, we always schema-qualify the name */
		char	   *nsp_name = get_namespace_name(opc_rec->opcnamespace);
		char	   *opc_name = pstrdup(NameStr(opc_rec->opcname));

		result = list_make2(makeString(nsp_name), makeString(opc_name));
	}

	ReleaseSysCache(ht_opc);
	return result;
}

List *
transformCreateExternalStmt(CreateExternalStmt *stmt, const char *queryString)
{
	ParseState *pstate;
	CreateStmtContext cxt;
	List	   *result;
	ListCell   *elements;
	DistributedBy *likeDistributedBy = NULL;
	bool	    bQuiet = false;	/* shut up transformDistributedBy messages */
	bool		iswritable = false;

	/* Set up pstate */
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	memset(&cxt, 0, sizeof(CreateStmtContext));

	/*
	 * Create a temporary context in order to confine memory leaks due
	 * to expansions within a short lived context
	 */
	cxt.tempCtx = AllocSetContextCreate(CurrentMemoryContext,
							  "CreateExteranlStmt analyze context",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * There exist transformations that might write on the passed on stmt.
	 * Create a copy of it to both protect from (un)intentional writes and be
	 * a bit more explicit of the intended ownership.
	 */
	stmt = (CreateExternalStmt *)copyObject(stmt);

	cxt.pstate = pstate;
	cxt.stmtType = "CREATE EXTERNAL TABLE";
	cxt.relation = stmt->relation;
	cxt.inhRelations = NIL;
	cxt.hasoids = false;
	cxt.isalter = false;
	cxt.iscreatepart = false;
	cxt.columns = NIL;
	cxt.ckconstraints = NIL;
	cxt.fkconstraints = NIL;
	cxt.ixconstraints = NIL;
	cxt.pkey = NULL;
	cxt.rel = NULL;

	cxt.blist = NIL;
	cxt.alist = NIL;

	iswritable = stmt->iswritable;

	/*
	 * Run through each primary element in the table creation clause. Separate
	 * column defs from constraints, and do preliminary analysis.
	 */
	foreach(elements, stmt->tableElts)
	{
		Node	   *element = lfirst(elements);

		switch (nodeTag(element))
		{
			case T_ColumnDef:
				transformColumnDefinition(&cxt, (ColumnDef *) element);
				break;

			case T_Constraint:
				/* should never happen. If it does fix gram.y */
				elog(ERROR, "node type %d not supported for external tables",
					 (int) nodeTag(element));
				break;

			case T_TableLikeClause:
				{
					/* LIKE */
					bool	isBeginning = (cxt.columns == NIL);

					transformTableLikeClause(&cxt, (TableLikeClause *) element, true, NULL, NULL);

					if (Gp_role == GP_ROLE_DISPATCH && isBeginning &&
						stmt->distributedBy == NULL &&
						iswritable /* dont bother if readable table */)
					{
						likeDistributedBy = getLikeDistributionPolicy((TableLikeClause *) element);
					}
				}
				break;

			default:
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(element));
				break;
		}
	}

	/*
	 * Forbid LOG ERRORS and ON MASTER combination.
	 */
	if (stmt->exttypedesc->exttabletype == EXTTBL_TYPE_EXECUTE)
	{
		ListCell   *exec_location_opt;

		foreach(exec_location_opt, stmt->exttypedesc->on_clause)
		{
			DefElem    *defel = (DefElem *) lfirst(exec_location_opt);

			if (strcmp(defel->defname, "master") == 0)
			{
				SingleRowErrorDesc *srehDesc = (SingleRowErrorDesc *)stmt->sreh;

				if(srehDesc && srehDesc->into_file)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("External web table with ON MASTER clause "
									"cannot use LOG ERRORS feature.")));
			}
		}
	}

	/*
	 * Handle DISTRIBUTED BY clause, if any.
	 *
	 * For writeable external tables, by default we distribute RANDOMLY, or
	 * by the distribution key of the LIKE table if exists. However, if
	 * DISTRIBUTED BY was specified we use it by calling the regular
	 * transformDistributedBy and handle it like we would for non external
	 * tables.
	 *
	 * For readable external tables, don't create a policy row at all.
	 * Non-EXECUTE type external tables are implicitly randomly distributed.
	 * EXECUTE type external tables encapsulate similar information in the
	 * "ON <segment spec>" clause, which is stored in pg_exttable.location.
	 */
	if (iswritable)
	{
		if (stmt->distributedBy == NULL && likeDistributedBy == NULL)
		{
			/*
			 * defaults to DISTRIBUTED RANDOMLY irrespective of the
			 * gp_create_table_random_default_distribution guc.
			 */
			stmt->distributedBy = makeNode(DistributedBy);
			stmt->distributedBy->ptype = POLICYTYPE_PARTITIONED;
			stmt->distributedBy->keyCols = NIL;
			stmt->distributedBy->numsegments = GP_POLICY_DEFAULT_NUMSEGMENTS();
		}
		else
		{
			/* regular DISTRIBUTED BY transformation */
			stmt->distributedBy = transformDistributedBy(&cxt, stmt->distributedBy,
								   (DistributedBy *)likeDistributedBy, bQuiet);
			if (stmt->distributedBy->ptype == POLICYTYPE_REPLICATED)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("external tables can't have DISTRIBUTED REPLICATED clause")));
		}
	}
	else if (stmt->distributedBy != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("readable external tables can\'t specify a DISTRIBUTED BY clause")));

	Assert(cxt.ckconstraints == NIL);
	Assert(cxt.fkconstraints == NIL);
	Assert(cxt.ixconstraints == NIL);

	/*
	 * Output results.
	 */
	stmt->tableElts = cxt.columns;

	result = lappend(cxt.blist, stmt);
	result = list_concat(result, cxt.alist);

	MemoryContextDelete(cxt.tempCtx);

	return result;
}



/*
 * Process a DISTRIBUTED BY clause.
 *
 * If no DISTRIBUTED BY was given, this deduces a suitable default based on
 * various things.
 *
 * NOTE: We cannot form a GpPolicy object yet, because we don't know the
 * attribute numbers the columns will get. With inheritance, the table might
 * inherit more columns from a parent table, which are not visible in the
 * CreateStmt.
 */
static DistributedBy *
transformDistributedBy(CreateStmtContext *cxt,
					   DistributedBy *distributedBy,
					   DistributedBy *likeDistributedBy,
					   bool bQuiet)
{
	ListCell	*keys = NULL;
	List		*distrkeys = NIL;
	ListCell   *lc;
	int			numsegments;

	/*
	 * utility mode creates can't have a policy.  Only the QD can have policies
	 */
	if (Gp_role != GP_ROLE_DISPATCH && !IsBinaryUpgrade)
		return NULL;

	if (distributedBy && distributedBy->numsegments > 0)
		/* If numsegments is set in DISTRIBUTED BY use the specified value */
		numsegments = distributedBy->numsegments;
	else
		/* Otherwise use DEFAULT as numsegments */
		numsegments = GP_POLICY_DEFAULT_NUMSEGMENTS();

	/* Explictly specified distributed randomly, no futher check needed */
	if (distributedBy &&
		(distributedBy->ptype == POLICYTYPE_PARTITIONED && distributedBy->keyCols == NIL))
	{
		distributedBy->numsegments = numsegments;
		return distributedBy;
	}

	/* Check replicated policy */
	if (distributedBy && distributedBy->ptype == POLICYTYPE_REPLICATED)
	{
		if (cxt->inhRelations != NIL)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("INHERITS clause cannot be used with DISTRIBUTED REPLICATED clause")));

		distributedBy->numsegments = numsegments;
		return distributedBy;
	}

	if (distributedBy)
		distrkeys = distributedBy->keyCols;

	/*
	 * If distributedBy is NIL, the user did not explicitly say what he
	 * wanted for a distribution policy.  So, we need to assign one.
	 */
	if (distrkeys == NIL)
	{
		/*
		 * If we have a PRIMARY KEY or UNIQUE constraints, derive the distribution key
		 * from them.
		 *
		 * The distribution key chosen to be the largest common subset of columns, across
		 * all the PRIMARY KEY / UNIQUE constraints.
		 */
		/* begin with the PRIMARY KEY, if any */
		if (cxt->pkey != NULL)
		{
			IndexStmt  *index = cxt->pkey;
			List	   *indexParams;
			ListCell   *ip;

			Assert(index->indexParams != NULL);
			indexParams = index->indexParams;

			foreach(ip, indexParams)
			{
				IndexElem  *iparam = lfirst(ip);

				if (iparam && iparam->name != 0)
				{
					IndexElem *distrkey = makeNode(IndexElem);

					distrkey->name = iparam->name;
					distrkey->opclass = NULL;

					distrkeys = lappend(distrkeys, distrkey);
				}
			}
		}

		/* walk through all UNIQUE constraints next. */
		foreach(lc, cxt->ixconstraints)
		{
			Constraint *constraint = (Constraint *) lfirst(lc);
			ListCell   *ip;
			List	   *new_distrkeys = NIL;

			if (constraint->contype != CONSTR_UNIQUE)
				continue;

			if (distrkeys)
			{
				/*
				 * We saw a PRIMARY KEY or UNIQUE constraint already. Find
				 * the columns that are present in the key chosen so far,
				 * and this constraint.
				 */
				foreach(ip, constraint->keys)
				{
					Value	   *v = lfirst(ip);
					ListCell   *dkcell;

					foreach(dkcell, distrkeys)
					{
						IndexElem  *dk = (IndexElem *) lfirst(dkcell);

						if (strcmp(dk->name, strVal(v)) == 0)
						{
							new_distrkeys = lappend(new_distrkeys, dk);
							break;
						}
					}
				}

				/* If there were no common columns, we're out of luck. */
				if (new_distrkeys == NIL)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("UNIQUE or PRIMARY KEY definitions are incompatible with each other"),
							 errhint("When there are multiple PRIMARY KEY / UNIQUE constraints, they must have at least one column in common.")));
			}
			else
			{
				/*
				 * No distribution key chosen yet. Use this key as is.
				 */
				new_distrkeys = NIL;
				foreach(ip, constraint->keys)
				{
					Value	   *v = lfirst(ip);
					IndexElem  *dk = makeNode(IndexElem);

					dk->name = strVal(v);
					dk->opclass = NULL;

					new_distrkeys = lappend(new_distrkeys, dk);
				}
			}

			distrkeys = new_distrkeys;
		}
	}

	/*
	 * If new table INHERITS from one or more parent tables, check parents.
	 */
	if (cxt->inhRelations != NIL)
	{
		ListCell   *entry;

		foreach(entry, cxt->inhRelations)
		{
			RangeVar   *parent = (RangeVar *) lfirst(entry);
			GpPolicy   *parentPolicy;
			Relation	parentrel;

			parentrel = heap_openrv(parent, AccessShareLock);
			parentPolicy = parentrel->rd_cdbpolicy;

			/*
			 * Partitioned child must have partitioned parents. During binary
			 * upgrade we allow to skip this check since that runs against a
			 * segment in utility mode and the distribution policy isn't stored
			 * in the segments.
			 */
			if ((parentPolicy == NULL ||
					parentPolicy->ptype == POLICYTYPE_ENTRY) &&
					!IsBinaryUpgrade)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot inherit from catalog table \"%s\" to create table \"%s\"",
								parent->relname, cxt->relation->relname),
						 errdetail("An inheritance hierarchy cannot contain a mixture of distributed and non-distributed tables.")));
			}

			if ((parentPolicy == NULL ||
					GpPolicyIsReplicated(parentPolicy)) &&
					!IsBinaryUpgrade)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot inherit from replicated table \"%s\" to create table \"%s\"",
								parent->relname, cxt->relation->relname),
						 errdetail("An inheritance hierarchy cannot contain a mixture of distributed and non-distributed tables.")));
			}

			/*
			 * If we still don't know what distribution to use, and this
			 * is an inherited table, set the distribution based on the
			 * parent (or one of the parents)
			 */
			if (distrkeys == NIL && parentPolicy->nattrs >= 0)
			{
				if (!bQuiet)
					ereport(NOTICE,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("table has parent, setting distribution columns to match parent table")));

				distributedBy = make_distributedby_for_rel(parentrel);
				heap_close(parentrel, AccessShareLock);

				distributedBy->numsegments = numsegments;
				return distributedBy;
			}
			heap_close(parentrel, AccessShareLock);
		}
	}

	if (distrkeys == NIL && likeDistributedBy != NULL)
	{
		if (!bQuiet)
			ereport(NOTICE,
					(errmsg("table doesn't have 'DISTRIBUTED BY' clause, defaulting to distribution columns from LIKE table")));

		if (likeDistributedBy->ptype == POLICYTYPE_PARTITIONED &&
			likeDistributedBy->keyCols == NIL)
		{
			distributedBy = makeNode(DistributedBy);
			distributedBy->ptype = POLICYTYPE_PARTITIONED;
			distributedBy->numsegments = numsegments;
			return distributedBy;
		}
		else if (likeDistributedBy->ptype == POLICYTYPE_REPLICATED)
		{
			distributedBy = makeNode(DistributedBy);
			distributedBy->ptype = POLICYTYPE_REPLICATED;
			distributedBy->numsegments = numsegments;
			return distributedBy;
		}

		distrkeys = likeDistributedBy->keyCols;
	}

	if (gp_create_table_random_default_distribution && NIL == distrkeys)
	{
		Assert(NULL == likeDistributedBy);

		if (!bQuiet)
		{
			ereport(NOTICE,
				(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
				 errmsg("using default RANDOM distribution since no distribution was specified"),
				 errhint("Consider including the 'DISTRIBUTED BY' clause to determine the distribution of rows.")));
		}

		distributedBy = makeNode(DistributedBy);
		distributedBy->ptype = POLICYTYPE_PARTITIONED;
		distributedBy->numsegments = numsegments;
		return distributedBy;
	}
	else if (distrkeys == NIL)
	{
		/*
		 * if we get here, we haven't a clue what to use for the distribution columns.
		 * table has one or more attributes and there is still no distribution
		 * key. pick a default one. the winner is the first attribute that is
		 * an Greenplum Database-hashable data type.
		 */

		ListCell   *columns;

		if (cxt->inhRelations)
		{
			bool		found = false;
			/* try inherited tables */
			ListCell   *inher;

			foreach(inher, cxt->inhRelations)
			{
				RangeVar   *inh = (RangeVar *) lfirst(inher);
				Relation	rel;
				int			count;

				Assert(IsA(inh, RangeVar));
				rel = heap_openrv(inh, AccessShareLock);
				if (rel->rd_rel->relkind != RELKIND_RELATION)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("inherited relation \"%s\" is not a table",
									inh->relname)));
				for (count = 0; count < rel->rd_att->natts; count++)
				{
					Form_pg_attribute inhattr = rel->rd_att->attrs[count];
					Oid typeOid = inhattr->atttypid;

					if (inhattr->attisdropped)
						continue;
					if (cdb_default_distribution_opclass_for_type(typeOid) != InvalidOid)
					{
						char	   *inhname = NameStr(inhattr->attname);
						IndexElem  *ielem;

						ielem = makeNode(IndexElem);
						ielem->name = inhname;
						ielem->opclass = NULL;

						distrkeys = list_make1(ielem);
						if (!bQuiet)
							ereport(NOTICE,
								(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
								 errmsg("Table doesn't have 'DISTRIBUTED BY' clause -- Using column "
										"named '%s' from parent table as the Greenplum Database data distribution key for this "
										"table. ", inhname),
								 errhint("The 'DISTRIBUTED BY' clause determines the distribution of data."
								 		 " Make sure column(s) chosen are the optimal data distribution key to minimize skew.")));
						found = true;
						break;
					}
				}
				heap_close(rel, NoLock);

				if (distrkeys != NIL)
					break;
			}

		}

		if (distrkeys == NIL)
		{
			foreach(columns, cxt->columns)
			{
				ColumnDef  *column = (ColumnDef *) lfirst(columns);
				Oid			typeOid;

				typeOid = typenameTypeId(NULL, column->typeName);

				/*
				 * If we can hash this type, this column will be our default
				 * key.
				 */
				if (cdb_default_distribution_opclass_for_type(typeOid))
				{
					IndexElem  *ielem = makeNode(IndexElem);

					ielem->name = column->colname;
					ielem->opclass = NULL;		/* or should we explicitly set the opclass we just looked up? */

					distrkeys = list_make1(ielem);
					if (!bQuiet)
						ereport(NOTICE,
							(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
							 errmsg("Table doesn't have 'DISTRIBUTED BY' clause -- Using column "
									"named '%s' as the Greenplum Database data distribution key for this "
									"table. ", column->colname),
							 errhint("The 'DISTRIBUTED BY' clause determines the distribution of data."
							 		 " Make sure column(s) chosen are the optimal data distribution key to minimize skew.")));
					break;
				}
			}
		}

		if (distrkeys == NIL)
		{
			/*
			 * There was no eligible distribution column to default to. This table
			 * will be partitioned on an empty distribution key list. In other words,
			 * tuples coming into the system will be randomly assigned a bucket.
			 */
			if (!bQuiet)
				ereport(NOTICE,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("Table doesn't have 'DISTRIBUTED BY' clause, and no column type is suitable for a distribution key. Creating a NULL policy entry.")));

			distributedBy = makeNode(DistributedBy);
			distributedBy->ptype = POLICYTYPE_PARTITIONED;
			distributedBy->numsegments = numsegments;
			return distributedBy;
		}
	}
	else
	{
		/*
		 * We have a DISTRIBUTED BY column list, either specified by the user
		 * or defaulted to a primary key or unique column. Process it now.
		 */
		foreach(keys, distrkeys)
		{
			IndexElem  *ielem = (IndexElem *) lfirst(keys);
			char	   *colname = ielem->name;
			bool		found = false;
			ListCell   *columns;

			if (cxt->inhRelations)
			{
				/* try inherited tables */
				ListCell   *inher;

				foreach(inher, cxt->inhRelations)
				{
					RangeVar   *inh = (RangeVar *) lfirst(inher);
					Relation	rel;
					int			count;

					Assert(IsA(inh, RangeVar));
					rel = heap_openrv(inh, AccessShareLock);
					if (rel->rd_rel->relkind != RELKIND_RELATION)
						ereport(ERROR,
								(errcode(ERRCODE_WRONG_OBJECT_TYPE),
								 errmsg("inherited relation \"%s\" is not a table",
										inh->relname)));
					for (count = 0; count < rel->rd_att->natts; count++)
					{
						Form_pg_attribute inhattr = rel->rd_att->attrs[count];
						char	   *inhname = NameStr(inhattr->attname);

						if (inhattr->attisdropped)
							continue;
						if (strcmp(colname, inhname) == 0)
						{
							found = true;

							break;
						}
					}
					heap_close(rel, NoLock);
					if (found)
						elog(DEBUG1, "DISTRIBUTED BY clause refers to columns of inherited table");

					if (found)
						break;
				}
			}

			if (!found)
			{
				foreach(columns, cxt->columns)
				{
					ColumnDef *column = (ColumnDef *) lfirst(columns);
					Assert(IsA(column, ColumnDef));

					if (strcmp(column->colname, colname) == 0)
					{
						found = true;
						break;
					}
				}
			}

			/*
			 * In the ALTER TABLE case, don't complain about index keys
			 * not created in the command; they may well exist already.
			 * DefineIndex will complain about them if not, and will also
			 * take care of marking them NOT NULL.
			 */
			if (!found && !cxt->isalter)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" named in 'DISTRIBUTED BY' clause does not exist",
								colname)));
		}
	}

	/*
	 * Ok, we have decided on the distribution key columns now, and have the column
	 * names in 'distrkeys'. Perform last cross-checks between UNIQUE and PRIMARY KEY
	 * constraints and the chosen distribution key. (These tests should always pass,
	 * if the distribution key was derived from the PRIMARY KEY or UNIQUE constraints,
	 * but it doesn't hurt to check even in those cases.)
	 */
	if (cxt && cxt->pkey)
	{
		/* The distribution key must be a subset of the primary key */
		IndexStmt  *index = cxt->pkey;
		ListCell   *dk;

		foreach(dk, distrkeys)
		{
			char	   *distcolname = strVal(lfirst(dk));
			ListCell   *ip;
			bool		found = false;

			foreach(ip, index->indexParams)
			{
				IndexElem  *iparam = lfirst(ip);

				if (!iparam->name)
					elog(ERROR, "PRIMARY KEY on an expression index not supported");

				if (strcmp(iparam->name, distcolname) == 0)
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("PRIMARY KEY and DISTRIBUTED BY definitions are incompatible"),
						 errhint("When there is both a PRIMARY KEY and a DISTRIBUTED BY clause, the DISTRIBUTED BY clause must be a subset of the PRIMARY KEY.")));
			}
		}
	}

	/* Make sure distribution columns match any UNIQUE and PRIMARY KEY constraints. */
	foreach (lc, cxt->ixconstraints)
	{
		Constraint *constraint = (Constraint *) lfirst(lc);
		ListCell   *dk;

		if (constraint->contype != CONSTR_PRIMARY &&
			constraint->contype != CONSTR_UNIQUE)
			continue;

		foreach(dk, distrkeys)
		{
			char	   *distcolname = strVal(lfirst(dk));
			ListCell   *ip;
			bool		found = false;

			foreach (ip, constraint->keys)
			{
				IndexElem  *iparam = lfirst(ip);

				if (!iparam->name)
					elog(ERROR, "UNIQUE constraint on an expression index not supported");

				if (strcmp(iparam->name, distcolname) == 0)
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("UNIQUE constraint and DISTRIBUTED BY definitions are incompatible"),
						 errhint("When there is both a UNIQUE constraint and a DISTRIBUTED BY clause, the DISTRIBUTED BY clause must be a subset of the UNIQUE constraint.")));
			}
		}
	}

	/* Form the resulting Distributed By clause */
	distributedBy = makeNode(DistributedBy);
	distributedBy->ptype = POLICYTYPE_PARTITIONED;
	distributedBy->keyCols = distrkeys;
	distributedBy->numsegments = numsegments;

	return distributedBy;
}

/*
 * Given a DistributedBy clause, construct a GpPolicy for it.
 */
GpPolicy *
getPolicyForDistributedBy(DistributedBy *distributedBy, TupleDesc tupdesc)
{
	List	   *policykeys;
	List	   *policyopclasses;
	ListCell   *lc;

	if (!distributedBy)
		return NULL; /* XXX or should we complain? */

	switch(distributedBy->ptype)
	{
		case POLICYTYPE_PARTITIONED:
			/* Look up the attribute numbers for each column */
			policykeys = NIL;
			policyopclasses = NIL;
			foreach(lc, distributedBy->keyCols)
			{
				IndexElem  *ielem = (IndexElem *) lfirst(lc);
				char	   *colname = ielem->name;
				int			i;
				bool		found = false;

				for (i = 0; i < tupdesc->natts; i++)
				{
					Form_pg_attribute attr = tupdesc->attrs[i];

					if (strcmp(colname, NameStr(attr->attname)) == 0)
					{
						Oid			opclass;

						opclass = cdb_get_opclass_for_column_def(ielem->opclass, attr->atttypid);

						policykeys = lappend_int(policykeys, attr->attnum);
						policyopclasses = lappend_oid(policyopclasses, opclass);
						found = true;
					}
				}
				if (!found)
					elog(ERROR, "could not find DISTRIBUTED BY column \"%s\"", colname);
			}

			return createHashPartitionedPolicy(policykeys,
											   policyopclasses,
											   distributedBy->numsegments);;

		case POLICYTYPE_ENTRY:
			elog(ERROR, "unexpected entry distribution policy");
			return NULL;

		case POLICYTYPE_REPLICATED:
			return createReplicatedGpPolicy(distributedBy->numsegments);
	}
	elog(ERROR, "unrecognized policy type %d", distributedBy->ptype);
	return NULL;
}

/*
 * Add any missing encoding attributes (compresstype = none,
 * blocksize=...).  The column specific encoding attributes supported
 * today are compresstype, compresslevel and blocksize.  Refer to
 * pg_compression.c for more info.
 */
static List *
fillin_encoding(List *list)
{
	bool foundCompressType = false;
	bool foundCompressTypeNone = false;
	char *cmplevel = NULL;
	bool foundBlockSize = false;
	char *arg;
	List *retList = list_copy(list);
	ListCell *lc;
	DefElem *el;
	const StdRdOptions *ao_opts = currentAOStorageOptions();

	foreach(lc, list)
	{
		el = lfirst(lc);

		if (pg_strcasecmp("compresstype", el->defname) == 0)
		{
			foundCompressType = true;
			arg = defGetString(el);
			if (pg_strcasecmp("none", arg) == 0)
				foundCompressTypeNone = true;
		}
		else if (pg_strcasecmp("compresslevel", el->defname) == 0)
		{
			cmplevel = defGetString(el);
		}
		else if (pg_strcasecmp("blocksize", el->defname) == 0)
			foundBlockSize = true;
	}

	if (foundCompressType == false && cmplevel == NULL)
	{
		/* No compression option specified, use current defaults. */
		arg = ao_opts->compresstype[0] ?
				pstrdup(ao_opts->compresstype) : "none";
		el = makeDefElem("compresstype", (Node *) makeString(arg));
		retList = lappend(retList, el);
		el = makeDefElem("compresslevel",
						 (Node *) makeInteger(ao_opts->compresslevel));
		retList = lappend(retList, el);
	}
	else if (foundCompressType == false && cmplevel)
	{
		if (strcmp(cmplevel, "0") == 0)
		{
			/*
			 * User wants to disable compression by specifying
			 * compresslevel=0.
			 */
			el = makeDefElem("compresstype", (Node *) makeString("none"));
			retList = lappend(retList, el);
		}
		else
		{
			/*
			 * User wants to enable compression by specifying non-zero
			 * compresslevel.  Therefore, choose default compresstype
			 * if configured, otherwise use zlib.
			 */
			if (ao_opts->compresstype[0] &&
				strcmp(ao_opts->compresstype, "none") != 0)
			{
				arg = pstrdup(ao_opts->compresstype);
			}
			else
			{
				arg = AO_DEFAULT_COMPRESSTYPE;
			}
			el = makeDefElem("compresstype", (Node *) makeString(arg));
			retList = lappend(retList, el);
		}
	}
	else if (foundCompressType && cmplevel == NULL)
	{
		if (foundCompressTypeNone)
		{
			/*
			 * User wants to disable compression by specifying
			 * compresstype=none.
			 */
			el = makeDefElem("compresslevel", (Node *) makeInteger(0));
			retList = lappend(retList, el);
		}
		else
		{
			/*
			 * Valid compresstype specified.  Use default
			 * compresslevel if it's non-zero, otherwise use 1.
			 */
			el = makeDefElem("compresslevel",
							 (Node *) makeInteger(ao_opts->compresslevel > 0 ?
												  ao_opts->compresslevel : 1));
			retList = lappend(retList, el);
		}
	}
	if (foundBlockSize == false)
	{
		el = makeDefElem("blocksize", (Node *) makeInteger(ao_opts->blocksize));
		retList = lappend(retList, el);
	}
	return retList;
}

/*
 * transformIndexConstraints
 *		Handle UNIQUE, PRIMARY KEY, EXCLUDE constraints, which create indexes.
 *		We also merge in any index definitions arising from
 *		LIKE ... INCLUDING INDEXES.
 */
static void
transformIndexConstraints(CreateStmtContext *cxt, bool mayDefer)
{
	IndexStmt  *index;
	List	   *indexlist = NIL;
	ListCell   *lc;

	/*
	 * Run through the constraints that need to generate an index. For PRIMARY
	 * KEY, mark each column as NOT NULL and create an index. For UNIQUE or
	 * EXCLUDE, create an index as for PRIMARY KEY, but do not insist on NOT
	 * NULL.
	 */
	foreach(lc, cxt->ixconstraints)
	{
		Constraint *constraint = (Constraint *) lfirst(lc);

		Assert(IsA(constraint, Constraint));
		if(constraint->contype == CONSTR_EXCLUSION)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("GPDB does not support exclusion constraints.")));

		Assert(constraint->contype == CONSTR_PRIMARY ||
			   constraint->contype == CONSTR_UNIQUE);

		index = transformIndexConstraint(constraint, cxt);

		indexlist = lappend(indexlist, index);
	}

	/* Add in any indexes defined by LIKE ... INCLUDING INDEXES */
	foreach(lc, cxt->inh_indexes)
	{
		index = (IndexStmt *) lfirst(lc);

		if (index->primary)
		{
			if (cxt->pkey != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("multiple primary keys for table \"%s\" are not allowed",
								cxt->relation->relname)));
			cxt->pkey = index;
		}

		indexlist = lappend(indexlist, index);
	}

	/*
	 * Scan the index list and remove any redundant index specifications. This
	 * can happen if, for instance, the user writes UNIQUE PRIMARY KEY. A
	 * strict reading of SQL would suggest raising an error instead, but that
	 * strikes me as too anal-retentive. - tgl 2001-02-14
	 *
	 * XXX in ALTER TABLE case, it'd be nice to look for duplicate
	 * pre-existing indexes, too.
	 */
	Assert(cxt->alist == NIL);
	if (cxt->pkey != NULL)
	{
		/* Make sure we keep the PKEY index in preference to others... */
		cxt->alist = list_make1(cxt->pkey);
	}

	foreach(lc, indexlist)
	{
		bool		keep = true;
		bool		defer = false;
		ListCell   *k;

		index = lfirst(lc);

		/* if it's pkey, it's already in cxt->alist */
		if (index == cxt->pkey)
			continue;

		foreach(k, cxt->alist)
		{
			IndexStmt  *priorindex = lfirst(k);

			if (equal(index->indexParams, priorindex->indexParams) &&
				equal(index->whereClause, priorindex->whereClause) &&
				equal(index->excludeOpNames, priorindex->excludeOpNames) &&
				strcmp(index->accessMethod, priorindex->accessMethod) == 0 &&
				index->deferrable == priorindex->deferrable &&
				index->initdeferred == priorindex->initdeferred)
			{
				priorindex->unique |= index->unique;

				/*
				 * If the prior index is as yet unnamed, and this one is
				 * named, then transfer the name to the prior index. This
				 * ensures that if we have named and unnamed constraints,
				 * we'll use (at least one of) the names for the index.
				 */
				if (priorindex->idxname == NULL)
					priorindex->idxname = index->idxname;
				keep = false;
				break;
			}
		}
		
		defer = index->whereClause != NULL;
		if ( !defer )
		{
			ListCell *j;
			foreach(j, index->indexParams)
			{
				IndexElem *elt = (IndexElem*)lfirst(j);
				Assert(IsA(elt, IndexElem));
				
				if (elt->expr != NULL)
				{
					defer = true;
					break;
				}
			}
		}

		if (keep)
		{
			if (defer && mayDefer)
			{
				/* An index on an expression with a WHERE clause or for an 
				 * inheritance child will cause a trip through parse_analyze.  
				 * If we do that before creating the table, it will fail, so 
				 * we put it on a list for later.
				 */
			
				ereport(DEBUG1,
						(errmsg("deferring index creation for table \"%s\"",
								cxt->relation->relname)
						 ));
				cxt->dlist = lappend(cxt->dlist, index);
			}
			else
			{
				cxt->alist = lappend(cxt->alist, index);
			}
		}
	}
}

/*
 * transformIndexConstraint
 *		Transform one UNIQUE, PRIMARY KEY, or EXCLUDE constraint for
 *		transformIndexConstraints.
 */
static IndexStmt *
transformIndexConstraint(Constraint *constraint, CreateStmtContext *cxt)
{
	Assert(constraint->contype !=  CONSTR_EXCLUSION);

	IndexStmt  *index;
	ListCell   *lc;

	index = makeNode(IndexStmt);

	index->unique = (constraint->contype != CONSTR_EXCLUSION);
	index->primary = (constraint->contype == CONSTR_PRIMARY);
	if (index->primary)
	{
		if (cxt->pkey != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
			 errmsg("multiple primary keys for table \"%s\" are not allowed",
					cxt->relation->relname),
					 parser_errposition(cxt->pstate, constraint->location)));
		cxt->pkey = index;

		/*
		 * In ALTER TABLE case, a primary index might already exist, but
		 * DefineIndex will check for it.
		 */
	}
	index->isconstraint = true;
	index->deferrable = constraint->deferrable;
	index->initdeferred = constraint->initdeferred;

	if (constraint->conname != NULL)
		index->idxname = pstrdup(constraint->conname);
	else
		index->idxname = NULL;	/* DefineIndex will choose name */

	index->relation = cxt->relation;
	index->accessMethod = constraint->access_method ? constraint->access_method : DEFAULT_INDEX_TYPE;
	index->options = constraint->options;
	index->tableSpace = constraint->indexspace;
	index->whereClause = constraint->where_clause;
	index->indexParams = NIL;
	index->excludeOpNames = NIL;
	index->idxcomment = NULL;
	index->indexOid = InvalidOid;
	index->oldNode = InvalidOid;
	index->concurrent = false;

	/*
	 * If it's ALTER TABLE ADD CONSTRAINT USING INDEX, look up the index and
	 * verify it's usable, then extract the implied column name list.  (We
	 * will not actually need the column name list at runtime, but we need it
	 * now to check for duplicate column entries below.)
	 */
	if (constraint->indexname != NULL)
	{
		char	   *index_name = constraint->indexname;
		Relation	heap_rel = cxt->rel;
		Oid			index_oid;
		Relation	index_rel;
		Form_pg_index index_form;
		oidvector  *indclass;
		Datum		indclassDatum;
		bool		isnull;
		int			i;

		/* Grammar should not allow this with explicit column list */
		Assert(constraint->keys == NIL);

		/* Grammar should only allow PRIMARY and UNIQUE constraints */
		Assert(constraint->contype == CONSTR_PRIMARY ||
			   constraint->contype == CONSTR_UNIQUE);

		/* Must be ALTER, not CREATE, but grammar doesn't enforce that */
		if (!cxt->isalter)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot use an existing index in CREATE TABLE"),
					 parser_errposition(cxt->pstate, constraint->location)));

		/* Look for the index in the same schema as the table */
		index_oid = get_relname_relid(index_name, RelationGetNamespace(heap_rel));

		if (!OidIsValid(index_oid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("index \"%s\" does not exist", index_name),
					 parser_errposition(cxt->pstate, constraint->location)));

		/* Open the index (this will throw an error if it is not an index) */
		index_rel = index_open(index_oid, AccessShareLock);
		index_form = index_rel->rd_index;

		/* Check that it does not have an associated constraint already */
		if (OidIsValid(get_index_constraint(index_oid)))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			   errmsg("index \"%s\" is already associated with a constraint",
					  index_name),
					 parser_errposition(cxt->pstate, constraint->location)));

		/* Perform validity checks on the index */
		if (index_form->indrelid != RelationGetRelid(heap_rel))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("index \"%s\" does not belong to table \"%s\"",
							index_name, RelationGetRelationName(heap_rel)),
					 parser_errposition(cxt->pstate, constraint->location)));

		if (!IndexIsValid(index_form))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("index \"%s\" is not valid", index_name),
					 parser_errposition(cxt->pstate, constraint->location)));

		if (!index_form->indisunique)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a unique index", index_name),
					 errdetail("Cannot create a primary key or unique constraint using such an index."),
					 parser_errposition(cxt->pstate, constraint->location)));

		if (RelationGetIndexExpressions(index_rel) != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("index \"%s\" contains expressions", index_name),
					 errdetail("Cannot create a primary key or unique constraint using such an index."),
					 parser_errposition(cxt->pstate, constraint->location)));

		if (RelationGetIndexPredicate(index_rel) != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a partial index", index_name),
					 errdetail("Cannot create a primary key or unique constraint using such an index."),
					 parser_errposition(cxt->pstate, constraint->location)));

		/*
		 * It's probably unsafe to change a deferred index to non-deferred. (A
		 * non-constraint index couldn't be deferred anyway, so this case
		 * should never occur; no need to sweat, but let's check it.)
		 */
		if (!index_form->indimmediate && !constraint->deferrable)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a deferrable index", index_name),
					 errdetail("Cannot create a non-deferrable constraint using a deferrable index."),
					 parser_errposition(cxt->pstate, constraint->location)));

		/*
		 * Insist on it being a btree.  That's the only kind that supports
		 * uniqueness at the moment anyway; but we must have an index that
		 * exactly matches what you'd get from plain ADD CONSTRAINT syntax,
		 * else dump and reload will produce a different index (breaking
		 * pg_upgrade in particular).
		 */
		if (index_rel->rd_rel->relam != get_am_oid(DEFAULT_INDEX_TYPE, false))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("index \"%s\" is not a btree", index_name),
					 parser_errposition(cxt->pstate, constraint->location)));

		/* Must get indclass the hard way */
		indclassDatum = SysCacheGetAttr(INDEXRELID, index_rel->rd_indextuple,
										Anum_pg_index_indclass, &isnull);
		Assert(!isnull);
		indclass = (oidvector *) DatumGetPointer(indclassDatum);

		for (i = 0; i < index_form->indnatts; i++)
		{
			int16		attnum = index_form->indkey.values[i];
			Form_pg_attribute attform;
			char	   *attname;
			Oid			defopclass;

			/*
			 * We shouldn't see attnum == 0 here, since we already rejected
			 * expression indexes.  If we do, SystemAttributeDefinition will
			 * throw an error.
			 */
			if (attnum > 0)
			{
				Assert(attnum <= heap_rel->rd_att->natts);
				attform = heap_rel->rd_att->attrs[attnum - 1];
			}
			else
				attform = SystemAttributeDefinition(attnum,
											   heap_rel->rd_rel->relhasoids);
			attname = pstrdup(NameStr(attform->attname));

			/*
			 * Insist on default opclass and sort options.  While the index
			 * would still work as a constraint with non-default settings, it
			 * might not provide exactly the same uniqueness semantics as
			 * you'd get from a normally-created constraint; and there's also
			 * the dump/reload problem mentioned above.
			 */
			defopclass = GetDefaultOpClass(attform->atttypid,
										   index_rel->rd_rel->relam);
			if (indclass->values[i] != defopclass ||
				index_rel->rd_indoption[i] != 0)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("index \"%s\" does not have default sorting behavior", index_name),
						 errdetail("Cannot create a primary key or unique constraint using such an index."),
					 parser_errposition(cxt->pstate, constraint->location)));

			constraint->keys = lappend(constraint->keys, makeString(attname));
		}

		/* Close the index relation but keep the lock */
		relation_close(index_rel, NoLock);

		index->indexOid = index_oid;
	}

	/*
	 * If it's an EXCLUDE constraint, the grammar returns a list of pairs of
	 * IndexElems and operator names.  We have to break that apart into
	 * separate lists.
	 */
	if (constraint->contype == CONSTR_EXCLUSION)
	{
		foreach(lc, constraint->exclusions)
		{
			List	   *pair = (List *) lfirst(lc);
			IndexElem  *elem;
			List	   *opname;

			Assert(list_length(pair) == 2);
			elem = (IndexElem *) linitial(pair);
			Assert(IsA(elem, IndexElem));
			opname = (List *) lsecond(pair);
			Assert(IsA(opname, List));

			index->indexParams = lappend(index->indexParams, elem);
			index->excludeOpNames = lappend(index->excludeOpNames, opname);
		}

		return index;
	}

	/*
	 * For UNIQUE and PRIMARY KEY, we just have a list of column names.
	 *
	 * Make sure referenced keys exist.  If we are making a PRIMARY KEY index,
	 * also make sure they are NOT NULL, if possible. (Although we could leave
	 * it to DefineIndex to mark the columns NOT NULL, it's more efficient to
	 * get it right the first time.)
	 */
	foreach(lc, constraint->keys)
	{
		char	   *key = strVal(lfirst(lc));
		bool		found = false;
		ColumnDef  *column = NULL;
		ListCell   *columns;
		IndexElem  *iparam;

		foreach(columns, cxt->columns)
		{
			column = (ColumnDef *) lfirst(columns);
			Assert(IsA(column, ColumnDef));
			if (strcmp(column->colname, key) == 0)
			{
				found = true;
				break;
			}
		}
		if (found)
		{
			/* found column in the new table; force it to be NOT NULL */
			if (constraint->contype == CONSTR_PRIMARY)
				column->is_not_null = TRUE;
		}
		else if (SystemAttributeByName(key, cxt->hasoids) != NULL)
		{
			/*
			 * column will be a system column in the new table, so accept it.
			 * System columns can't ever be null, so no need to worry about
			 * PRIMARY/NOT NULL constraint.
			 */
			found = true;
		}
		else if (cxt->inhRelations)
		{
			/* try inherited tables */
			ListCell   *inher;

			foreach(inher, cxt->inhRelations)
			{
				RangeVar   *inh = (RangeVar *) lfirst(inher);
				Relation	rel;
				int			count;

				Assert(IsA(inh, RangeVar));
				rel = heap_openrv(inh, AccessShareLock);
				if (rel->rd_rel->relkind != RELKIND_RELATION)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						   errmsg("inherited relation \"%s\" is not a table",
								  inh->relname)));
				for (count = 0; count < rel->rd_att->natts; count++)
				{
					Form_pg_attribute inhattr = rel->rd_att->attrs[count];
					char	   *inhname = NameStr(inhattr->attname);

					if (inhattr->attisdropped)
						continue;
					if (strcmp(key, inhname) == 0)
					{
						found = true;

						/*
						 * We currently have no easy way to force an inherited
						 * column to be NOT NULL at creation, if its parent
						 * wasn't so already. We leave it to DefineIndex to
						 * fix things up in this case.
						 */
						break;
					}
				}
				heap_close(rel, NoLock);
				if (found)
					break;
			}
		}

		/*
		 * In the ALTER TABLE case, don't complain about index keys not
		 * created in the command; they may well exist already. DefineIndex
		 * will complain about them if not, and will also take care of marking
		 * them NOT NULL.
		 */
		if (!found && !cxt->isalter)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" named in key does not exist", key),
					 parser_errposition(cxt->pstate, constraint->location)));

		/* Check for PRIMARY KEY(foo, foo) */
		foreach(columns, index->indexParams)
		{
			iparam = (IndexElem *) lfirst(columns);
			if (iparam->name && strcmp(key, iparam->name) == 0)
			{
				if (index->primary)
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_COLUMN),
							 errmsg("column \"%s\" appears twice in primary key constraint",
									key),
					 parser_errposition(cxt->pstate, constraint->location)));
				else
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_COLUMN),
					errmsg("column \"%s\" appears twice in unique constraint",
						   key),
					 parser_errposition(cxt->pstate, constraint->location)));
			}
		}

		/* OK, add it to the index definition */
		iparam = makeNode(IndexElem);
		iparam->name = pstrdup(key);
		iparam->expr = NULL;
		iparam->indexcolname = NULL;
		iparam->collation = NIL;
		iparam->opclass = NIL;
		iparam->ordering = SORTBY_DEFAULT;
		iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;
		index->indexParams = lappend(index->indexParams, iparam);
	}

	return index;
}

/*
 * transformFKConstraints
 *		handle FOREIGN KEY constraints
 */
static void
transformFKConstraints(CreateStmtContext *cxt,
					   bool skipValidation, bool isAddConstraint)
{
	ListCell   *fkclist;

	if (cxt->fkconstraints == NIL)
		return;

	/*
	 * If CREATE TABLE or adding a column with NULL default, we can safely
	 * skip validation of FK constraints, and nonetheless mark them valid.
	 * (This will override any user-supplied NOT VALID flag.)
	 */
	if (skipValidation)
	{
		foreach(fkclist, cxt->fkconstraints)
		{
			Constraint *constraint = (Constraint *) lfirst(fkclist);

			constraint->skip_validation = true;
			constraint->initially_valid = true;
		}
	}

	/*
	 * For CREATE TABLE or ALTER TABLE ADD COLUMN, gin up an ALTER TABLE ADD
	 * CONSTRAINT command to execute after the basic command is complete. (If
	 * called from ADD CONSTRAINT, that routine will add the FK constraints to
	 * its own subcommand list.)
	 *
	 * Note: the ADD CONSTRAINT command must also execute after any index
	 * creation commands.  Thus, this should run after
	 * transformIndexConstraints, so that the CREATE INDEX commands are
	 * already in cxt->alist.
	 */
	if (!isAddConstraint)
	{
		AlterTableStmt *alterstmt = makeNode(AlterTableStmt);

		alterstmt->relation = cxt->relation;
		alterstmt->cmds = NIL;
		alterstmt->relkind = OBJECT_TABLE;

		foreach(fkclist, cxt->fkconstraints)
		{
			Constraint *constraint = (Constraint *) lfirst(fkclist);
			AlterTableCmd *altercmd = makeNode(AlterTableCmd);

			altercmd->subtype = AT_ProcessedConstraint;
			altercmd->name = NULL;
			altercmd->def = (Node *) constraint;
			alterstmt->cmds = lappend(alterstmt->cmds, altercmd);
		}

		cxt->alist = lappend(cxt->alist, alterstmt);
	}
}

/*
 * transformIndexStmt - parse analysis for CREATE INDEX and ALTER TABLE
 *
 * Note: this is a no-op for an index not using either index expressions or
 * a predicate expression.  There are several code paths that create indexes
 * without bothering to call this, because they know they don't have any
 * such expressions to deal with.
 *
 * To avoid race conditions, it's important that this function rely only on
 * the passed-in relid (and not on stmt->relation) to determine the target
 * relation.
 */
IndexStmt *
transformIndexStmt(Oid relid, IndexStmt *stmt, const char *queryString)
{
	ParseState *pstate;
	RangeTblEntry *rte;
	ListCell   *l;
	Relation	rel;
	LOCKMODE	lockmode;

	/*
	 * We must not scribble on the passed-in IndexStmt, so copy it.  (This is
	 * overkill, but easy.)
	 */
	stmt = (IndexStmt *) copyObject(stmt);

	/*
	 * Open the parent table with appropriate locking.	We must do this
	 * because addRangeTableEntry() would acquire only AccessShareLock,
	 * leaving DefineIndex() needing to do a lock upgrade with consequent risk
	 * of deadlock.  Make sure this stays in sync with the type of lock
	 * DefineIndex() wants. If we are being called by ALTER TABLE, we will
	 * already hold a higher lock.
	 */
	lockmode = stmt->concurrent ? ShareUpdateExclusiveLock : ShareLock;
	rel = heap_openrv(stmt->relation, lockmode);

	/* Set up pstate */
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	/*
	 * Put the parent table into the rtable so that the expressions can refer
	 * to its fields without qualification.  Caller is responsible for locking
	 * relation, but we still need to open it.
	 */
	rte = addRangeTableEntryForRelation(pstate, rel, NULL, false, true);

	/* no to join list, yes to namespaces */
	addRTEtoQuery(pstate, rte, false, true, true);

	/* take care of the where clause */
	if (stmt->whereClause)
	{
		stmt->whereClause = transformWhereClause(pstate,
												 stmt->whereClause,
												 EXPR_KIND_INDEX_PREDICATE,
												 "WHERE");
		/* we have to fix its collations too */
		assign_expr_collations(pstate, stmt->whereClause);
	}

	/* take care of any index expressions */
	foreach(l, stmt->indexParams)
	{
		IndexElem  *ielem = (IndexElem *) lfirst(l);

		if (ielem->expr)
		{
			/* Extract preliminary index col name before transforming expr */
			if (ielem->indexcolname == NULL)
				ielem->indexcolname = FigureIndexColname(ielem->expr);

			/* Now do parse transformation of the expression */
			ielem->expr = transformExpr(pstate, ielem->expr,
										EXPR_KIND_INDEX_EXPRESSION);

			/* We have to fix its collations too */
			assign_expr_collations(pstate, ielem->expr);

			/*
			 * transformExpr() should have already rejected subqueries,
			 * aggregates, and window functions, based on the EXPR_KIND_ for
			 * an index expression.
			 *
			 * Also reject expressions returning sets; this is for consistency
			 * with what transformWhereClause() checks for the predicate.
			 * DefineIndex() will make more checks.
			 */
			if (expression_returns_set(ielem->expr))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("index expression cannot return a set")));
		}
	}

	/*
	 * Check that only the base rel is mentioned.  (This should be dead code
	 * now that add_missing_from is history.)
	 */
	if (list_length(pstate->p_rtable) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("index expressions and predicates can refer only to the table being indexed")));

	free_parsestate(pstate);

	/*
	 * Close relation. Unless this is a CREATE INDEX
	 * for a partitioned table, and we're processing a partition. In that
	 * case, we want to release the lock on the partition early, so that
	 * you don't run out of space in the lock manager if there are a lot
	 * of partitions. Holding the lock on the parent table should be
	 * enough.
	 */
	if (!rel_needs_long_lock(RelationGetRelid(rel)))
		heap_close(rel, lockmode);
	else
		heap_close(rel, NoLock);

	return stmt;
}


/*
 * transformRuleStmt -
 *	  transform a CREATE RULE Statement. The action is a list of parse
 *	  trees which is transformed into a list of query trees, and we also
 *	  transform the WHERE clause if any.
 *
 * actions and whereClause are output parameters that receive the
 * transformed results.
 *
 * Note that we must not scribble on the passed-in RuleStmt, so we do
 * copyObject() on the actions and WHERE clause.
 */
void
transformRuleStmt(RuleStmt *stmt, const char *queryString,
				  List **actions, Node **whereClause)
{
	Relation	rel;
	ParseState *pstate;
	RangeTblEntry *oldrte;
	RangeTblEntry *newrte;

	/*
	 * To avoid deadlock, make sure the first thing we do is grab
	 * AccessExclusiveLock on the target relation.  This will be needed by
	 * DefineQueryRewrite(), and we don't want to grab a lesser lock
	 * beforehand.
	 */
	rel = heap_openrv(stmt->relation, AccessExclusiveLock);

	if (rel->rd_rel->relkind == RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("rules on materialized views are not supported")));

	/* Set up pstate */
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	/*
	 * NOTE: 'OLD' must always have a varno equal to 1 and 'NEW' equal to 2.
	 * Set up their RTEs in the main pstate for use in parsing the rule
	 * qualification.
	 */
	oldrte = addRangeTableEntryForRelation(pstate, rel,
										   makeAlias("old", NIL),
										   false, false);
	newrte = addRangeTableEntryForRelation(pstate, rel,
										   makeAlias("new", NIL),
										   false, false);
	/* Must override addRangeTableEntry's default access-check flags */
	oldrte->requiredPerms = 0;
	newrte->requiredPerms = 0;

	/*
	 * They must be in the namespace too for lookup purposes, but only add the
	 * one(s) that are relevant for the current kind of rule.  In an UPDATE
	 * rule, quals must refer to OLD.field or NEW.field to be unambiguous, but
	 * there's no need to be so picky for INSERT & DELETE.  We do not add them
	 * to the joinlist.
	 */
	switch (stmt->event)
	{
		case CMD_SELECT:
			addRTEtoQuery(pstate, oldrte, false, true, true);
			break;
		case CMD_UPDATE:
			addRTEtoQuery(pstate, oldrte, false, true, true);
			addRTEtoQuery(pstate, newrte, false, true, true);
			break;
		case CMD_INSERT:
			addRTEtoQuery(pstate, newrte, false, true, true);
			break;
		case CMD_DELETE:
			addRTEtoQuery(pstate, oldrte, false, true, true);
			break;
		default:
			elog(ERROR, "unrecognized event type: %d",
				 (int) stmt->event);
			break;
	}

	/* take care of the where clause */
	*whereClause = transformWhereClause(pstate,
									  (Node *) copyObject(stmt->whereClause),
										EXPR_KIND_WHERE,
										"WHERE");
	/* we have to fix its collations too */
	assign_expr_collations(pstate, *whereClause);

	/* this is probably dead code without add_missing_from: */
	if (list_length(pstate->p_rtable) != 2)		/* naughty, naughty... */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("rule WHERE condition cannot contain references to other relations")));

	/*
	 * 'instead nothing' rules with a qualification need a query rangetable so
	 * the rewrite handler can add the negated rule qualification to the
	 * original query. We create a query with the new command type CMD_NOTHING
	 * here that is treated specially by the rewrite system.
	 */
	if (stmt->actions == NIL)
	{
		Query	   *nothing_qry = makeNode(Query);

		nothing_qry->commandType = CMD_NOTHING;
		nothing_qry->rtable = pstate->p_rtable;
		nothing_qry->jointree = makeFromExpr(NIL, NULL);		/* no join wanted */

		*actions = list_make1(nothing_qry);
	}
	else
	{
		ListCell   *l;
		List	   *newactions = NIL;

		/*
		 * transform each statement, like parse_sub_analyze()
		 */
		foreach(l, stmt->actions)
		{
			Node	   *action = (Node *) lfirst(l);
			ParseState *sub_pstate = make_parsestate(NULL);
			Query	   *sub_qry,
					   *top_subqry;
			bool		has_old,
						has_new;

			/*
			 * Since outer ParseState isn't parent of inner, have to pass down
			 * the query text by hand.
			 */
			sub_pstate->p_sourcetext = queryString;

			/*
			 * Set up OLD/NEW in the rtable for this statement.  The entries
			 * are added only to relnamespace, not varnamespace, because we
			 * don't want them to be referred to by unqualified field names
			 * nor "*" in the rule actions.  We decide later whether to put
			 * them in the joinlist.
			 */
			oldrte = addRangeTableEntryForRelation(sub_pstate, rel,
												   makeAlias("old", NIL),
												   false, false);
			newrte = addRangeTableEntryForRelation(sub_pstate, rel,
												   makeAlias("new", NIL),
												   false, false);
			oldrte->requiredPerms = 0;
			newrte->requiredPerms = 0;
			addRTEtoQuery(sub_pstate, oldrte, false, true, false);
			addRTEtoQuery(sub_pstate, newrte, false, true, false);

			/* Transform the rule action statement */
			top_subqry = transformStmt(sub_pstate,
									   (Node *) copyObject(action));

			/*
			 * We cannot support utility-statement actions (eg NOTIFY) with
			 * nonempty rule WHERE conditions, because there's no way to make
			 * the utility action execute conditionally.
			 */
			if (top_subqry->commandType == CMD_UTILITY &&
				*whereClause != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("rules with WHERE conditions can only have SELECT, INSERT, UPDATE, or DELETE actions")));

			/*
			 * If the action is INSERT...SELECT, OLD/NEW have been pushed down
			 * into the SELECT, and that's what we need to look at. (Ugly
			 * kluge ... try to fix this when we redesign querytrees.)
			 */
			sub_qry = getInsertSelectQuery(top_subqry, NULL);

			/*
			 * If the sub_qry is a setop, we cannot attach any qualifications
			 * to it, because the planner won't notice them.  This could
			 * perhaps be relaxed someday, but for now, we may as well reject
			 * such a rule immediately.
			 */
			if (sub_qry->setOperations != NULL && *whereClause != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")));

			/*
			 * Validate action's use of OLD/NEW, qual too
			 */
			has_old =
				rangeTableEntry_used((Node *) sub_qry, PRS2_OLD_VARNO, 0) ||
				rangeTableEntry_used(*whereClause, PRS2_OLD_VARNO, 0);
			has_new =
				rangeTableEntry_used((Node *) sub_qry, PRS2_NEW_VARNO, 0) ||
				rangeTableEntry_used(*whereClause, PRS2_NEW_VARNO, 0);

			switch (stmt->event)
			{
				case CMD_SELECT:
					if (has_old)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("ON SELECT rule cannot use OLD")));
					if (has_new)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("ON SELECT rule cannot use NEW")));
					break;
				case CMD_UPDATE:
					/* both are OK */
					break;
				case CMD_INSERT:
					if (has_old)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("ON INSERT rule cannot use OLD")));
					break;
				case CMD_DELETE:
					if (has_new)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("ON DELETE rule cannot use NEW")));
					break;
				default:
					elog(ERROR, "unrecognized event type: %d",
						 (int) stmt->event);
					break;
			}

			/*
			 * OLD/NEW are not allowed in WITH queries, because they would
			 * amount to outer references for the WITH, which we disallow.
			 * However, they were already in the outer rangetable when we
			 * analyzed the query, so we have to check.
			 *
			 * Note that in the INSERT...SELECT case, we need to examine the
			 * CTE lists of both top_subqry and sub_qry.
			 *
			 * Note that we aren't digging into the body of the query looking
			 * for WITHs in nested sub-SELECTs.  A WITH down there can
			 * legitimately refer to OLD/NEW, because it'd be an
			 * indirect-correlated outer reference.
			 */
			if (rangeTableEntry_used((Node *) top_subqry->cteList,
									 PRS2_OLD_VARNO, 0) ||
				rangeTableEntry_used((Node *) sub_qry->cteList,
									 PRS2_OLD_VARNO, 0))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot refer to OLD within WITH query")));
			if (rangeTableEntry_used((Node *) top_subqry->cteList,
									 PRS2_NEW_VARNO, 0) ||
				rangeTableEntry_used((Node *) sub_qry->cteList,
									 PRS2_NEW_VARNO, 0))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot refer to NEW within WITH query")));

			/*
			 * For efficiency's sake, add OLD to the rule action's jointree
			 * only if it was actually referenced in the statement or qual.
			 *
			 * For INSERT, NEW is not really a relation (only a reference to
			 * the to-be-inserted tuple) and should never be added to the
			 * jointree.
			 *
			 * For UPDATE, we treat NEW as being another kind of reference to
			 * OLD, because it represents references to *transformed* tuples
			 * of the existing relation.  It would be wrong to enter NEW
			 * separately in the jointree, since that would cause a double
			 * join of the updated relation.  It's also wrong to fail to make
			 * a jointree entry if only NEW and not OLD is mentioned.
			 */
			if (has_old || (has_new && stmt->event == CMD_UPDATE))
			{
				/*
				 * If sub_qry is a setop, manipulating its jointree will do no
				 * good at all, because the jointree is dummy. (This should be
				 * a can't-happen case because of prior tests.)
				 */
				if (sub_qry->setOperations != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")));
				/* hack so we can use addRTEtoQuery() */
				sub_pstate->p_rtable = sub_qry->rtable;
				sub_pstate->p_joinlist = sub_qry->jointree->fromlist;
				addRTEtoQuery(sub_pstate, oldrte, true, false, false);
				sub_qry->jointree->fromlist = sub_pstate->p_joinlist;
			}

			newactions = lappend(newactions, top_subqry);

			free_parsestate(sub_pstate);
		}

		*actions = newactions;
	}

	free_parsestate(pstate);

	/* Close relation, but keep the exclusive lock */
	heap_close(rel, NoLock);
}


/*
 * transformAlterTableStmt -
 *		parse analysis for ALTER TABLE
 *
 * Returns a List of utility commands to be done in sequence.  One of these
 * will be the transformed AlterTableStmt, but there may be additional actions
 * to be done before and after the actual AlterTable() call.
 *
 * To avoid race conditions, it's important that this function rely only on
 * the passed-in relid (and not on stmt->relation) to determine the target
 * relation.
 */
List *
transformAlterTableStmt(Oid relid, AlterTableStmt *stmt,
						const char *queryString)
{
	Relation	rel;
	ParseState *pstate;
	CreateStmtContext cxt;
	List	   *result;
	List	   *save_alist;
	ListCell   *lcmd,
			   *l;
	List	   *newcmds = NIL;
	bool		skipValidation = true;
	AlterTableCmd *newcmd;

	/*
	 * We must not scribble on the passed-in AlterTableStmt, so copy it. (This
	 * is overkill, but easy.)
	 */
	stmt = (AlterTableStmt *) copyObject(stmt);

	/* Caller is responsible for locking the relation */
	/* GPDB_94_MERGE_FIXME: this function used to be responsible, and we had some
	 * more complicated logic here for partitions:
	 *
	 * In GPDB, we release the lock early if this command is part of a
	 * partitioned CREATE TABLE.
	 */
	rel = relation_open(relid, NoLock);

	/* Set up pstate and CreateStmtContext */
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	cxt.pstate = pstate;
	if (stmt->relkind == OBJECT_FOREIGN_TABLE)
	{
		cxt.stmtType = "ALTER FOREIGN TABLE";
		cxt.isforeign = true;
	}
	else
	{
		cxt.stmtType = "ALTER TABLE";
		cxt.isforeign = false;
	}
	cxt.relation = stmt->relation;
	cxt.rel = rel;
	cxt.inhRelations = NIL;
	cxt.isalter = true;
	cxt.iscreatepart = false;
	cxt.hasoids = false;		/* need not be right */
	cxt.columns = NIL;
	cxt.ckconstraints = NIL;
	cxt.fkconstraints = NIL;
	cxt.ixconstraints = NIL;
	cxt.inh_indexes = NIL;
	cxt.blist = NIL;
	cxt.alist = NIL;
	cxt.dlist = NIL; /* used by transformCreateStmt, not here */
	cxt.pkey = NULL;

	/*
	 * The only subtypes that currently require parse transformation handling
	 * are ADD COLUMN and ADD CONSTRAINT.  These largely re-use code from
	 * CREATE TABLE.
	 * And ALTER TABLE ... <operator> PARTITION ...
	 */
	foreach(lcmd, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

		switch (cmd->subtype)
		{
			case AT_AddColumn:
			case AT_AddColumnToView:
				{
					ColumnDef  *def = (ColumnDef *) cmd->def;

					Assert(IsA(def, ColumnDef));

					/*
					 * Adding a column with a primary key or unique constraint
					 * is not supported in GPDB.
					 */
					if (Gp_role == GP_ROLE_DISPATCH)
					{
						ListCell *c;
						foreach(c, def->constraints)
						{
							Constraint *cons = (Constraint *) lfirst(c);
							if (cons->contype == CONSTR_PRIMARY)
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("cannot add column with primary key constraint")));
							if (cons->contype == CONSTR_UNIQUE)
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("cannot add column with unique constraint")));
						}
					}
					transformColumnDefinition(&cxt, def);

					/*
					 * If the column has a non-null default, we can't skip
					 * validation of foreign keys.
					 */
					if (def->raw_default != NULL)
						skipValidation = false;

					/*
					 * All constraints are processed in other ways. Remove the
					 * original list
					 */
					def->constraints = NIL;

					newcmds = lappend(newcmds, cmd);
					break;
				}
			case AT_AddConstraint:

				/*
				 * The original AddConstraint cmd node doesn't go to newcmds
				 */
				if (IsA(cmd->def, Constraint))
				{
					transformTableConstraint(&cxt, (Constraint *) cmd->def);
					if (((Constraint *) cmd->def)->contype == CONSTR_FOREIGN)
					{
						/* GPDB: always skip validation of foreign keys */
						skipValidation = true;
					}
				}
				else
					elog(ERROR, "unrecognized node type: %d",
						 (int) nodeTag(cmd->def));
				break;

			case AT_ProcessedConstraint:

				/*
				 * Already-transformed ADD CONSTRAINT, so just make it look
				 * like the standard case.
				 */
				cmd->subtype = AT_AddConstraint;
				newcmds = lappend(newcmds, cmd);
				break;

				/* CDB: Partitioned Tables */
            case AT_PartAlter:			/* Alter */
            case AT_PartAdd:			/* Add */
            case AT_PartDrop:			/* Drop */
            case AT_PartExchange:		/* Exchange */
            case AT_PartRename:			/* Rename */
            case AT_PartSetTemplate:	/* Set Subpartition Template */
            case AT_PartSplit:			/* Split */
            case AT_PartTruncate:		/* Truncate */
				cmd = transformAlterTable_all_PartitionStmt(
					pstate,
					stmt,
					&cxt,
					cmd);

				newcmds = lappend(newcmds, cmd);
				break;

			case AT_PartAddInternal:	/* Add partition, as part of CREATE TABLE */
				cxt.iscreatepart = true;
				newcmds = lappend(newcmds, cmd);
				break;

			default:
				newcmds = lappend(newcmds, cmd);
				break;
		}
	}

	/*
	 * transformIndexConstraints wants cxt.alist to contain only index
	 * statements, so transfer anything we already have into save_alist
	 * immediately.
	 */
	save_alist = cxt.alist;
	cxt.alist = NIL;

	/* Postprocess index and FK constraints */
	transformIndexConstraints(&cxt, false);

	transformFKConstraints(&cxt, skipValidation, true);

	/*
	 * Push any index-creation commands into the ALTER, so that they can be
	 * scheduled nicely by tablecmds.c.  Note that tablecmds.c assumes that
	 * the IndexStmt attached to an AT_AddIndex or AT_AddIndexConstraint
	 * subcommand has already been through transformIndexStmt.
	 */
	foreach(l, cxt.alist)
	{
		IndexStmt  *idxstmt = (IndexStmt *) lfirst(l);

		Assert(IsA(idxstmt, IndexStmt));
		idxstmt = transformIndexStmt(relid, idxstmt, queryString);
		newcmd = makeNode(AlterTableCmd);
		newcmd->subtype = OidIsValid(idxstmt->indexOid) ? AT_AddIndexConstraint : AT_AddIndex;
		newcmd->def = (Node *) idxstmt;
		newcmds = lappend(newcmds, newcmd);
	}
	cxt.alist = NIL;

	/* Append any CHECK or FK constraints to the commands list */
	foreach(l, cxt.ckconstraints)
	{
		newcmd = makeNode(AlterTableCmd);
		newcmd->subtype = AT_AddConstraint;
		newcmd->def = (Node *) lfirst(l);
		newcmds = lappend(newcmds, newcmd);
	}
	foreach(l, cxt.fkconstraints)
	{
		newcmd = makeNode(AlterTableCmd);
		newcmd->subtype = AT_AddConstraint;
		newcmd->def = (Node *) lfirst(l);
		newcmds = lappend(newcmds, newcmd);
	}

	/*
	 * Close rel
	 *
	 * If this is part of a CREATE TABLE of a partitioned table, creating
	 * the partitions, we release the lock immediately, however. We hold
	 * a lock on the parent table, and no-one can see the partitions yet,
	 * so the lock on each partition isn't strictly required. Creating a
	 * massively partitioned table could otherwise require holding a lot
	 * of locks, running out of shared memory in the lock manager.
	 */
	if (cxt.iscreatepart)
		relation_close(rel, AccessExclusiveLock);
	else
		relation_close(rel, NoLock);

	/*
	 * Output results.
	 */
	stmt->cmds = newcmds;

	result = lappend(cxt.blist, stmt);
	result = list_concat(result, cxt.alist);
	result = list_concat(result, save_alist);

	return result;
}


/*
 * Preprocess a list of column constraint clauses
 * to attach constraint attributes to their primary constraint nodes
 * and detect inconsistent/misplaced constraint attributes.
 *
 * NOTE: currently, attributes are only supported for FOREIGN KEY, UNIQUE,
 * EXCLUSION, and PRIMARY KEY constraints, but someday they ought to be
 * supported for other constraint types.
 */
static void
transformConstraintAttrs(CreateStmtContext *cxt, List *constraintList)
{
	Constraint *lastprimarycon = NULL;
	bool		saw_deferrability = false;
	bool		saw_initially = false;
	ListCell   *clist;

#define SUPPORTS_ATTRS(node)				\
	((node) != NULL &&						\
	 ((node)->contype == CONSTR_PRIMARY ||	\
	  (node)->contype == CONSTR_UNIQUE ||	\
	  (node)->contype == CONSTR_EXCLUSION || \
	  (node)->contype == CONSTR_FOREIGN))

	foreach(clist, constraintList)
	{
		Constraint *con = (Constraint *) lfirst(clist);

		if (!IsA(con, Constraint))
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(con));
		switch (con->contype)
		{
			case CONSTR_ATTR_DEFERRABLE:
				if (!SUPPORTS_ATTRS(lastprimarycon))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("misplaced DEFERRABLE clause"),
							 parser_errposition(cxt->pstate, con->location)));
				if (saw_deferrability)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed"),
							 parser_errposition(cxt->pstate, con->location)));
				saw_deferrability = true;
				lastprimarycon->deferrable = true;
				break;

			case CONSTR_ATTR_NOT_DEFERRABLE:
				if (!SUPPORTS_ATTRS(lastprimarycon))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("misplaced NOT DEFERRABLE clause"),
							 parser_errposition(cxt->pstate, con->location)));
				if (saw_deferrability)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed"),
							 parser_errposition(cxt->pstate, con->location)));
				saw_deferrability = true;
				lastprimarycon->deferrable = false;
				if (saw_initially &&
					lastprimarycon->initdeferred)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
							 parser_errposition(cxt->pstate, con->location)));
				break;

			case CONSTR_ATTR_DEFERRED:
				if (!SUPPORTS_ATTRS(lastprimarycon))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("misplaced INITIALLY DEFERRED clause"),
							 parser_errposition(cxt->pstate, con->location)));
				if (saw_initially)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed"),
							 parser_errposition(cxt->pstate, con->location)));
				saw_initially = true;
				lastprimarycon->initdeferred = true;

				/*
				 * If only INITIALLY DEFERRED appears, assume DEFERRABLE
				 */
				if (!saw_deferrability)
					lastprimarycon->deferrable = true;
				else if (!lastprimarycon->deferrable)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
							 parser_errposition(cxt->pstate, con->location)));
				break;

			case CONSTR_ATTR_IMMEDIATE:
				if (!SUPPORTS_ATTRS(lastprimarycon))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("misplaced INITIALLY IMMEDIATE clause"),
							 parser_errposition(cxt->pstate, con->location)));
				if (saw_initially)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed"),
							 parser_errposition(cxt->pstate, con->location)));
				saw_initially = true;
				lastprimarycon->initdeferred = false;
				break;

			default:
				/* Otherwise it's not an attribute */
				lastprimarycon = con;
				/* reset flags for new primary node */
				saw_deferrability = false;
				saw_initially = false;
				break;
		}
	}
}

/*
 * Special handling of type definition for a column
 */
static void
transformColumnType(CreateStmtContext *cxt, ColumnDef *column)
{
	/*
	 * All we really need to do here is verify that the type is valid,
	 * including any collation spec that might be present.
	 */
	Type		ctype = typenameType(cxt->pstate, column->typeName, NULL);

	if (column->collClause)
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(ctype);

		LookupCollation(cxt->pstate,
						column->collClause->collname,
						column->collClause->location);
		/* Complain if COLLATE is applied to an uncollatable type */
		if (!OidIsValid(typtup->typcollation))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("collations are not supported by type %s",
							format_type_be(HeapTupleGetOid(ctype))),
					 parser_errposition(cxt->pstate,
										column->collClause->location)));
	}

	ReleaseSysCache(ctype);
}


/*
 * transformCreateSchemaStmt -
 *	  analyzes the CREATE SCHEMA statement
 *
 * Split the schema element list into individual commands and place
 * them in the result list in an order such that there are no forward
 * references (e.g. GRANT to a table created later in the list). Note
 * that the logic we use for determining forward references is
 * presently quite incomplete.
 *
 * SQL also allows constraints to make forward references, so thumb through
 * the table columns and move forward references to a posterior alter-table
 * command.
 *
 * The result is a list of parse nodes that still need to be analyzed ---
 * but we can't analyze the later commands until we've executed the earlier
 * ones, because of possible inter-object references.
 *
 * Note: this breaks the rules a little bit by modifying schema-name fields
 * within passed-in structs.  However, the transformation would be the same
 * if done over, so it should be all right to scribble on the input to this
 * extent.
 */
List *
transformCreateSchemaStmt(CreateSchemaStmt *stmt)
{
	CreateSchemaStmtContext cxt;
	List	   *result;
	ListCell   *elements;

	cxt.stmtType = "CREATE SCHEMA";
	cxt.schemaname = stmt->schemaname;
	cxt.authid = stmt->authid;
	cxt.sequences = NIL;
	cxt.tables = NIL;
	cxt.views = NIL;
	cxt.indexes = NIL;
	cxt.triggers = NIL;
	cxt.grants = NIL;

	/*
	 * Run through each schema element in the schema element list. Separate
	 * statements by type, and do preliminary analysis.
	 */
	foreach(elements, stmt->schemaElts)
	{
		Node	   *element = lfirst(elements);

		switch (nodeTag(element))
		{
			case T_CreateSeqStmt:
				{
					CreateSeqStmt *elp = (CreateSeqStmt *) element;

					setSchemaName(cxt.schemaname, &elp->sequence->schemaname);
					cxt.sequences = lappend(cxt.sequences, element);
				}
				break;

			case T_CreateStmt:
				{
					CreateStmt *elp = (CreateStmt *) element;

					setSchemaName(cxt.schemaname, &elp->relation->schemaname);

					/*
					 * XXX todo: deal with constraints
					 */
					cxt.tables = lappend(cxt.tables, element);
				}
				break;

			case T_ViewStmt:
				{
					ViewStmt   *elp = (ViewStmt *) element;

					setSchemaName(cxt.schemaname, &elp->view->schemaname);

					/*
					 * XXX todo: deal with references between views
					 */
					cxt.views = lappend(cxt.views, element);
				}
				break;

			case T_IndexStmt:
				{
					IndexStmt  *elp = (IndexStmt *) element;

					setSchemaName(cxt.schemaname, &elp->relation->schemaname);
					cxt.indexes = lappend(cxt.indexes, element);
				}
				break;

			case T_CreateTrigStmt:
				{
					CreateTrigStmt *elp = (CreateTrigStmt *) element;

					setSchemaName(cxt.schemaname, &elp->relation->schemaname);
					cxt.triggers = lappend(cxt.triggers, element);
				}
				break;

			case T_GrantStmt:
				cxt.grants = lappend(cxt.grants, element);
				break;

			default:
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(element));
		}
	}

	result = NIL;
	result = list_concat(result, cxt.sequences);
	result = list_concat(result, cxt.tables);
	result = list_concat(result, cxt.views);
	result = list_concat(result, cxt.indexes);
	result = list_concat(result, cxt.triggers);
	result = list_concat(result, cxt.grants);

	return result;
}

/*
 * setSchemaName
 *		Set or check schema name in an element of a CREATE SCHEMA command
 */
static void
setSchemaName(char *context_schema, char **stmt_schema_name)
{
	if (*stmt_schema_name == NULL)
		*stmt_schema_name = context_schema;
	else if (strcmp(context_schema, *stmt_schema_name) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_SCHEMA_DEFINITION),
				 errmsg("CREATE specifies a schema (%s) "
						"different from the one being created (%s)",
						*stmt_schema_name, context_schema)));
}

/*
 * getLikeDistributionPolicy
 *
 * For Greenplum Database distributed tables, default to
 * the same distribution as the first LIKE table, unless
 * we also have INHERITS
 */
static DistributedBy *
getLikeDistributionPolicy(TableLikeClause *e)
{
	DistributedBy *likeDistributedBy = NULL;
	Relation	rel;

	rel = relation_openrv(e->relation, AccessShareLock);

	if (rel->rd_cdbpolicy != NULL && rel->rd_cdbpolicy->ptype != POLICYTYPE_ENTRY)
	{
		likeDistributedBy = make_distributedby_for_rel(rel);
	}

	relation_close(rel, AccessShareLock);

	return likeDistributedBy;
}

/*
 * Transform and validate the actual encoding clauses.
 *
 * We need tell the underlying system that these are AO/CO tables too,
 * hence the concatenation of the extra elements.
 */
List *
transformStorageEncodingClause(List *options)
{
	Datum d;
	ListCell *lc;
	DefElem *dl;
	foreach(lc, options)
	{
		dl = (DefElem *) lfirst(lc);
		if (pg_strncasecmp(dl->defname, SOPT_CHECKSUM, strlen(SOPT_CHECKSUM)) == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"%s\" is not a column specific option",
							SOPT_CHECKSUM)));
		}
	}
	List *extra = list_make2(makeDefElem("appendonly",
										 (Node *)makeString("true")),
							 makeDefElem("orientation",
										 (Node *)makeString("column")));

	/* add defaults for missing values */
	options = fillin_encoding(options);

	/*
	 * The following two statements validate that the encoding clause is well
	 * formed.
	 */
	d = transformRelOptions(PointerGetDatum(NULL),
									  list_concat(extra, options),
									  NULL, NULL,
									  true, false);
	(void)heap_reloptions(RELKIND_RELATION, d, true);

	return options;
}

/*
 * Validate the sanity of column reference storage clauses.
 *
 * 1. Ensure that we only refer to columns that exist.
 * 2. Ensure that each column is referenced either zero times or once.
 */
static void
validateColumnStorageEncodingClauses(List *stenc, CreateStmt *stmt)
{
	ListCell *lc;
	struct HTAB *ht = NULL;
	struct colent {
		char colname[NAMEDATALEN];
		int count;
	} *ce = NULL;

	if (!stenc)
		return;

	/* Generate a hash table for all the columns */
	foreach(lc, stmt->tableElts)
	{
		Node *n = lfirst(lc);

		if (IsA(n, ColumnDef))
		{
			ColumnDef *c = (ColumnDef *)n;
			char *colname;
			bool found = false;
			size_t n = NAMEDATALEN - 1 < strlen(c->colname) ?
							NAMEDATALEN - 1 : strlen(c->colname);

			colname = palloc0(NAMEDATALEN);
			MemSet(colname, 0, NAMEDATALEN);
			memcpy(colname, c->colname, n);
			colname[n] = '\0';

			if (!ht)
			{
				HASHCTL  cacheInfo;
				int      cacheFlags;

				memset(&cacheInfo, 0, sizeof(cacheInfo));
				cacheInfo.keysize = NAMEDATALEN;
				cacheInfo.entrysize = sizeof(*ce);
				cacheFlags = HASH_ELEM;

				ht = hash_create("column info cache",
								 list_length(stmt->tableElts),
								 &cacheInfo, cacheFlags);
			}

			ce = hash_search(ht, colname, HASH_ENTER, &found);

			/*
			 * The user specified a duplicate column name. We check duplicate
			 * column names VERY late (under MergeAttributes(), which is called
			 * by DefineRelation(). For the specific case here, it is safe to
			 * call out that this is a duplicate. We don't need to delay until
			 * we look at inheritance.
			 */
			if (found)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" duplicated",
								colname)));
				
			}
			ce->count = 0;
		}
	}

	/*
	 * If the table has no columns -- usually in the partitioning case -- then
	 * we can short circuit.
	 */
	if (!ht)
		return;

	/*
	 * All column reference storage directives without the DEFAULT
	 * clause should refer to real columns.
	 */
	foreach(lc, stenc)
	{
		ColumnReferenceStorageDirective *c = lfirst(lc);

		Insist(IsA(c, ColumnReferenceStorageDirective));

		if (c->deflt)
			continue;
		else
		{
			bool found = false;
			char colname[NAMEDATALEN];
			size_t collen = strlen(c->column);
			size_t n = NAMEDATALEN - 1 < collen ? NAMEDATALEN - 1 : collen;
			MemSet(colname, 0, NAMEDATALEN);
			memcpy(colname, c->column, n);
			colname[n] = '\0';

			ce = hash_search(ht, colname, HASH_FIND, &found);

			if (!found)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("column \"%s\" does not exist", colname)));

			ce->count++;

			if (ce->count > 1)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("column \"%s\" referenced in more than one COLUMN ENCODING clause",
								colname)));
		}
	}

	hash_destroy(ht);
}

/*
 * Find the column reference storage encoding clause for `column'.
 *
 * This is called by transformAttributeEncoding() in a loop but stenc should be
 * quite small in practice.
 */
static ColumnReferenceStorageDirective *
find_crsd(char *column, List *stenc)
{
	ListCell *lc;

	foreach(lc, stenc)
	{
		ColumnReferenceStorageDirective *c = lfirst(lc);

		if (c->deflt == false && strcmp(column, c->column) == 0)
			return c;
	}
	return NULL;
}


List *
TypeNameGetStorageDirective(TypeName *typname)
{
	Relation	rel;
	ScanKeyData scankey;
	SysScanDesc sscan;
	HeapTuple	tuple;
	Oid			typid;
	List	   *out = NIL;

	typid = typenameTypeId(NULL, typname);

	rel = heap_open(TypeEncodingRelationId, AccessShareLock);

	/* SELECT typoptions FROM pg_type_encoding where typid = :1 */
	ScanKeyInit(&scankey,
				Anum_pg_type_encoding_typid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(typid));
	sscan = systable_beginscan(rel, TypeEncodingTypidIndexId,
							   true, NULL, 1, &scankey);
	tuple = systable_getnext(sscan);
	if (HeapTupleIsValid(tuple))
	{
		Datum options;
		bool isnull;

		options = heap_getattr(tuple,
							   Anum_pg_type_encoding_typoptions,
							   RelationGetDescr(rel),
							   &isnull);

		Insist(!isnull);

		out = untransformRelOptions(options);
	}

	systable_endscan(sscan);
	heap_close(rel, AccessShareLock);

	return out;
}

/*
 * Make a default column storage directive from a WITH clause
 * Ignore options in the WITH clause that don't appear in 
 * storage_directives for column-level compression.
 */
List *
form_default_storage_directive(List *enc)
{
	List *out = NIL;
	ListCell *lc;

	foreach(lc, enc)
	{
		DefElem *el = lfirst(lc);

		if (!el->defname)
			out = lappend(out, copyObject(el));

		if (pg_strcasecmp("appendonly", el->defname) == 0)
			continue;
		if (pg_strcasecmp("orientation", el->defname) == 0)
			continue;
		if (pg_strcasecmp("oids", el->defname) == 0)
			continue;
		if (pg_strcasecmp("fillfactor", el->defname) == 0)
			continue;
		if (pg_strcasecmp("tablename", el->defname) == 0)
			continue;
		/* checksum is not a column specific attribute. */
		if (pg_strcasecmp("checksum", el->defname) == 0)
			continue;
		out = lappend(out, copyObject(el));
	}
	return out;
}

static List *
transformAttributeEncoding(List *stenc, CreateStmt *stmt, CreateStmtContext *cxt)
{
	ListCell *lc;
	bool found_enc = stenc != NIL;
	bool can_enc = is_aocs(stmt->options);
	ColumnReferenceStorageDirective *deflt = NULL;
	List *newenc = NIL;
	List *tmpenc;
	MemoryContext oldCtx;

#define UNSUPPORTED_ORIENTATION_ERROR() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("ENCODING clause only supported with column oriented tables")))

	/* We only support the attribute encoding clause on AOCS tables */
	if (stenc && !can_enc)
		UNSUPPORTED_ORIENTATION_ERROR();

	/* Use the temporary context to avoid leaving behind so much garbage. */
	oldCtx = MemoryContextSwitchTo(cxt->tempCtx);

	/* get the default clause, if there is one. */
	foreach(lc, stenc)
	{
		ColumnReferenceStorageDirective *c = lfirst(lc);
		Insist(IsA(c, ColumnReferenceStorageDirective));

		if (c->deflt)
		{
			/*
			 * Some quick validation: there should only be one default
			 * clause
			 */
			if (deflt)
				elog(ERROR, "only one default column encoding may be specified");

			deflt = copyObject(c);
			deflt->encoding = transformStorageEncodingClause(deflt->encoding);

			/*
			 * The default encoding and the with clause better not
			 * try and set the same options!
			 */
			if (encodings_overlap(stmt->options, deflt->encoding, false))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("DEFAULT COLUMN ENCODING clause cannot override values set in WITH clause")));
		}
	}

	/*
	 * If no default has been specified, we might create one out of the
	 * WITH clause.
	 */
	tmpenc = form_default_storage_directive(stmt->options);

	if (tmpenc)
	{
		deflt = makeNode(ColumnReferenceStorageDirective);
		deflt->deflt = true;
		deflt->encoding = transformStorageEncodingClause(tmpenc);
	}

	/*
	 * Loop over all columns. If a column has a column reference storage clause
	 * -- i.e., COLUMN name ENCODING () -- apply that. Otherwise, apply the
	 * default.
	 */
	foreach(lc, cxt->columns)
	{
		ColumnDef *d = (ColumnDef *) lfirst(lc);
		ColumnReferenceStorageDirective *c;

		Insist(IsA(d, ColumnDef));

		c = makeNode(ColumnReferenceStorageDirective);
		c->column = pstrdup(d->colname);

		/*
		 * Find a storage encoding for this column, in this order:
		 *
		 * 1. An explicit encoding clause in the ColumnDef
		 * 2. A column reference storage directive for this column
		 * 3. A default column encoding in the statement
		 * 4. A default for the type.
		 */
		if (d->encoding)
		{
			found_enc = true;
			c->encoding = transformStorageEncodingClause(d->encoding);
		}
		else
		{
			ColumnReferenceStorageDirective *s = find_crsd(c->column, stenc);

			if (s)
				c->encoding = transformStorageEncodingClause(s->encoding);
			else
			{
				if (deflt)
					c->encoding = copyObject(deflt->encoding);
				else
				{
					List	   *te;

					if (d->typeName)
						te = TypeNameGetStorageDirective(d->typeName);
					else
						te = NIL;

					if (te)
						c->encoding = copyObject(te);
					else
						c->encoding = default_column_encoding_clause();
				}
			}
		}
		newenc = lappend(newenc, c);
	}

	/* Check again incase we expanded a some column encoding clauses */
	if (!can_enc)
	{
		if (found_enc)
			UNSUPPORTED_ORIENTATION_ERROR();
		else
			newenc = NULL;
	}

	validateColumnStorageEncodingClauses(newenc, stmt);

	/* copy the result out of the temporary memory context */
	MemoryContextSwitchTo(oldCtx);
	newenc = copyObject(newenc);

	return newenc;
}

/*
 * Tells the caller if CO is explicitly disabled, to handle cases where we
 * want to ignore encoding clauses in partition expansion.
 *
 * This is an ugly special case that backup expects to work and since we've got
 * tonnes of dumps out there and the possibility that users have learned this
 * grammar from them, we must continue to support it.
 */
static bool
co_explicitly_disabled(List *opts)
{
	ListCell *lc;

	foreach(lc, opts)
	{
		DefElem *el = lfirst(lc);
		char *arg = NULL;

		/* Argument will be a Value */
		if (!el->arg)
		{
			continue;
		}

		arg = defGetString(el);
		bool result = false;
		if (pg_strcasecmp("appendonly", el->defname) == 0 &&
			pg_strcasecmp("false", arg) == 0)
		{
			result = true;
		}
		else if (pg_strcasecmp("orientation", el->defname) == 0 &&
				 pg_strcasecmp("column", arg) != 0)
		{
			result = true;
		}

		if (result)
		{
			return true;
		}
	}
	return false;
}

/*
 * Tell the caller whether appendonly=true and orientation=column
 * have been specified.
 */
bool
is_aocs(List *opts)
{
	bool found_ao = false;
	bool found_cs = false;
	bool aovalue = false;
	bool csvalue = false;

	ListCell *lc;

	foreach(lc, opts)
	{
		DefElem *el = lfirst(lc);
		char *arg = NULL;

		/* Argument will be a Value */
		if (!el->arg)
		{
			continue;
		}

		arg = defGetString(el);

		if (pg_strcasecmp("appendonly", el->defname) == 0)
		{
			found_ao = true;
			if (!parse_bool(arg, &aovalue))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for option \"appendonly\"")));
		}
		else if (pg_strcasecmp("orientation", el->defname) == 0)
		{
			found_cs = true;
			csvalue = pg_strcasecmp("column", arg) == 0;
		}
	}
	if (!found_ao)
		aovalue = isDefaultAO();
	if (!found_cs)
		csvalue = isDefaultAOCS();
	return (aovalue && csvalue);
}

/*
 * See if two encodings attempt to see the same parameters. If test_conflicts is
 * true, allow setting the same value, but the setting must be identical.
 */
static bool
encodings_overlap(List *a, List *b, bool test_conflicts)
{
	ListCell *lca;

	foreach(lca, a)
	{
		ListCell *lcb;
		DefElem *ela = lfirst(lca);

		foreach(lcb, b)
		{
			DefElem *elb = lfirst(lcb);

			if (pg_strcasecmp(ela->defname, elb->defname) == 0)
			{
				if (test_conflicts)
				{
					if (!ela->arg && !elb->arg)
						return true;
					else if (!ela->arg || !elb->arg)
					{
						/* skip */
					}
					else
					{
						char *ela_str = defGetString(ela);
						char *elb_str = defGetString(elb);

						if (pg_strcasecmp(ela_str,elb_str) != 0)
							return true;
					}
				}
				else
					return true;
			}
		}
	}
	return false;
}

/*
 * transformAlterTable_all_PartitionStmt -
 *	transform an Alter Table Statement for some Partition operation
 */
static AlterTableCmd *
transformAlterTable_all_PartitionStmt(
		ParseState *pstate,
		AlterTableStmt *stmt,
		CreateStmtContext *pCxt,
		AlterTableCmd *cmd)
{
	AlterPartitionCmd 	*pc   	   = (AlterPartitionCmd *) cmd->def;
	AlterPartitionCmd 	*pci  	   = pc;
	AlterPartitionId  	*pid  	   = (AlterPartitionId *)pci->partid;
	AlterTableCmd 		*atc1 	   = cmd;
	RangeVar 			*rv   	   = stmt->relation;
	PartitionNode 		*pNode 	   = NULL;
	PartitionNode 		*prevNode  = NULL;
	int 			 	 partDepth = 0;
	Oid 			 	 par_oid   = InvalidOid;
	StringInfoData   sid1, sid2;

	if (atc1->subtype == AT_PartAlter)
	{
		PgPartRule* 	 prule = NULL;
		char 			*lrelname;
		Relation 		 rel   = heap_openrv(rv, AccessShareLock);

		initStringInfo(&sid1);
		initStringInfo(&sid2);

		appendStringInfo(&sid1, "relation \"%s\"",
						 RelationGetRelationName(rel));

		lrelname = sid1.data;

		pNode = RelationBuildPartitionDesc(rel, false);

		if (!pNode)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("\"%s\" is not partitioned",
							lrelname)));

		/* Processes nested ALTER (if it exists) */
		while (1)
		{
			AlterPartitionId  	*pid2 = NULL;

			if (atc1->subtype != AT_PartAlter)
			{
				rv = makeRangeVar(
						get_namespace_name(
								RelationGetNamespace(rel)),
						pstrdup(RelationGetRelationName(rel)), -1);
				heap_close(rel, AccessShareLock);
				rel = NULL;
				break;
			}

			pid2 = (AlterPartitionId *)pci->partid;

			if (pid2 && (pid2->idtype == AT_AP_IDValue))
			{
				List *vallist = (List *)pid2->partiddef;
				pid2->partiddef =
						(Node *)transformExpressionList(
							pstate, vallist,
							EXPR_KIND_PARTITION_EXPRESSION);
			}

			partDepth++;

			if (!pNode)
				prule = NULL;
			else
				prule = get_part_rule1(rel,
									   pid2,
									   false, true,
									   NULL,
									   pNode,
									   sid1.data, NULL);

			if (prule && prule->topRule &&
				prule->topRule->children)
			{
				prevNode = pNode;
				pNode = prule->topRule->children;
				par_oid = RelationGetRelid(rel);

				/*
				 * Don't hold a long lock -- lock on the master is
				 * sufficient
				 */
				heap_close(rel, AccessShareLock);
				rel = heap_open(prule->topRule->parchildrelid,
								AccessShareLock);

				appendStringInfo(&sid2, "partition%s of %s",
								 prule->partIdStr, sid1.data);
				resetStringInfo(&sid1);
				appendStringInfo(&sid1, "%s", sid2.data);
				resetStringInfo(&sid2);
			}
			else
			{
				prevNode = pNode;
				pNode = NULL;
			}

			atc1 = (AlterTableCmd *)pci->arg1;
			pci = (AlterPartitionCmd *)atc1->def;
		} /* end while */
		if (rel)
			/* No need to hold onto the lock -- see above */
			heap_close(rel, AccessShareLock);
	} /* end if alter */

	switch (atc1->subtype)
	{
		case AT_PartAdd:				/* Add */
		case AT_PartSetTemplate:		/* Set Subpartn Template */
		case AT_PartDrop:				/* Drop */
		case AT_PartExchange:			/* Exchange */
		case AT_PartRename:				/* Rename */
		case AT_PartTruncate:			/* Truncate */
		case AT_PartSplit:				/* Split */
			/* MPP-4011: get right pid for FOR(value) */
			pid  	   = (AlterPartitionId *)pci->partid;
			if (pid && (pid->idtype == AT_AP_IDValue))
			{
				List *vallist = (List *)pid->partiddef;
				pid->partiddef =
						(Node *)transformExpressionList(
							pstate, vallist,
							EXPR_KIND_PARTITION_EXPRESSION);
			}
	break;
		default:
			break;
	}
	/* transform boundary specifications at execute time */
	return cmd;
} /* end transformAlterTable_all_PartitionStmt */

static IdentifiedBy *
transformIdentifiedBy(CreateStmtContext *cxt)
{
	IndexStmt  *index = cxt->pkey;
	List	   *indexParams;
	ListCell   *ip;
	List       *identKeys = NIL;
	IdentifiedBy *result = NULL;
	int numkeys = 0;

	if (index == NULL)
	{
		return NULL;
	}

	/* 
	 * CreateStmt.identifiedBy is not initilized in grammer parser (gram.y),
	 * so we make it here.
	 */
	result = palloc0(sizeof(IdentifiedBy));

	Assert(index->indexParams != NULL);
	indexParams = index->indexParams;

	foreach(ip, indexParams)
	{
		IndexElem  *iparam = lfirst(ip);

		if (iparam && iparam->name != 0)
		{
			identKeys = lappend(identKeys, (Node *) makeString(iparam->name));
			numkeys++;
		}
	}

	result->type = T_IdentifiedBy;
	result->numkeys = numkeys;
	result->keys = identKeys;
	return result;
}

static bool
is_rocksdb(List *opts)
{
	bool is_rocksdb = false;

	ListCell *lc;

	foreach(lc, opts)
	{
		DefElem *el = lfirst(lc);
		char *arg = NULL;

		/* Argument should be a Value */
		if (!el->arg)
		{
			continue;
		}

		arg = defGetString(el);

		if (pg_strcasecmp(SOPT_STORAGE_ENGINE, el->defname) == 0 &&
			pg_strcasecmp(ROCKSDB_STORAGE_ENGINE, arg) == 0)
		{
			is_rocksdb = true;
			break;
		}
	}
	
	return is_rocksdb;
}

static bool
is_rocksdb_global(void)
{
	return store_mode == STORE_MODE_ROCKSDB? true : false;
}

static List *
add_extra_rocksdb_option(List *opts)
{
	return lappend(opts, makeDefElem("storage_engine", (Node *)makeString("rocksdb")));
}

static bool
have_primary_key(List *elts)
{
	ListCell *elt;
	ListCell *con;
	ColumnDef * col;
	Constraint *constraint;

	foreach(elt, elts)
	{
		col = lfirst(elt);
		foreach(con, col->constraints)
		{
			constraint = lfirst(con);
			if (constraint->contype == CONSTR_PRIMARY)
			    return true;
		}
	}
	return false;
}

static ColumnDef *
makeMyColumnDef(void)
{
	ColumnDef *column = makeNode(ColumnDef);
	column->type = T_ColumnDef;
	column->colname = "inner_primary_key";
	column->typeName = makeTypeName("serial");
	column->inhcount = 0;
	column->is_local = true;
	column->is_not_null = false;
	column->is_from_type = false;
	column->attnum = 0;
	column->storage = 0;
	column->raw_default = (Node *) 0;
	column->cooked_default = (Node *) 0;
	column->collClause = (CollateClause *) 0;
	column->collOid = 0;
	column->constraints = list_make1(makeMyConstraint());
	column->encoding = NIL;
	column->fdwoptions = NIL;
	column->location = -1;
	return column;
}

static Constraint *
makeMyConstraint(void)
{
	Constraint *constraint = makeNode(Constraint);
	constraint->type = T_Constraint;
	constraint->contype = CONSTR_PRIMARY;
	constraint->conname = NULL;
	constraint->deferrable = false;
	constraint->initdeferred = false;
	constraint->location = -1;
	constraint->is_no_inherit = false;
	constraint->raw_expr = NULL;
	constraint->cooked_expr = NULL;
	constraint->keys = NIL;
	constraint->exclusions = NIL;
	constraint->options = NIL;
	constraint->indexname = NULL;
	constraint->indexspace = NULL;
	constraint->access_method = NULL;
	constraint->where_clause = NULL;
	constraint->pktable = NULL;
	constraint->fk_attrs = NIL;
	constraint->pk_attrs = NIL;
	constraint->fk_matchtype = 0;
	constraint->fk_upd_action = 0;
	constraint->fk_del_action = 0;
	constraint->old_conpfeqop = NIL;
	constraint->old_pktable_oid = 0;
	constraint->skip_validation = false;
	constraint->initially_valid = 0;
	constraint->trig1Oid = 0;
	constraint->trig2Oid = 0;
	constraint->trig3Oid = 0;
	constraint->trig4Oid = 0;
	return constraint;
}
