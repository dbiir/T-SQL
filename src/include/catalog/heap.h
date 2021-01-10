/*-------------------------------------------------------------------------
 *
 * heap.h
 *	  prototypes for functions in backend/catalog/heap.c
 *
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/heap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HEAP_H
#define HEAP_H

#include "parser/parse_node.h"
#include "catalog/indexing.h"


typedef struct RawColumnDefault
{
	AttrNumber	attnum;			/* attribute to attach default to */
	Node	   *raw_default;	/* default value (untransformed parse tree) */
} RawColumnDefault;

typedef struct CookedConstraint
{
	/*
	 * In PostgreSQL, this struct is only during CREATE TABLE processing, but
	 * in GPDB, we create these in the QD and dispatch pre-built
	 * CookedConstraints to the QE nodes, in the CreateStmt. That's why we
	 * need to have a node tag and copy/out/read function support for this
	 * in GPDB.
	 */
	NodeTag		type;
	ConstrType	contype;		/* CONSTR_DEFAULT or CONSTR_CHECK */
	char	   *name;			/* name, or NULL if none */
	AttrNumber	attnum;			/* which attr (only for DEFAULT) */
	Node	   *expr;			/* transformed default or check expr */
	bool		skip_validation;	/* skip validation? (only for CHECK) */
	bool		is_local;		/* constraint has local (non-inherited) def */
	int			inhcount;		/* number of times constraint is inherited */
	/*
	 * Remember to update copy/out/read functions if new fields are added
	 * here!
	 */
	bool		is_no_inherit;	/* constraint has local def and cannot be
								 * inherited */
} CookedConstraint;

extern Relation heap_create(const char *relname,
			Oid relnamespace,
			Oid reltablespace,
			Oid relid,
			Oid relfilenode,
			TupleDesc tupDesc,
			Oid relam,
			char relkind,
			char relpersistence,
			char relstorage,
			bool shared_relation,
			bool mapped_relation,
			bool allow_system_table_mods);

extern Oid heap_create_with_catalog(const char *relname,
									Oid relnamespace,
									Oid reltablespace,
									Oid relid,
									Oid reltypeid,
									Oid reloftypeid,
									Oid ownerid,
									TupleDesc tupdesc,
									List *cooked_constraints,
									Oid relam,
									char relkind,
									char relpersistence,
									char relstorage,
									bool shared_relation,
									bool mapped_relation,
									bool oidislocal,
									int oidinhcount,
									OnCommitAction oncommit,
									const struct GpPolicy *policy,    /* MPP */
									Datum reloptions,
									bool use_user_acl,
									bool allow_system_table_mods,
									bool is_internal,
									bool valid_opts,
									bool is_part_child,
									bool is_part_parent);				/* RocksDB */

extern void heap_create_init_fork(Relation rel);

extern void heap_drop_with_catalog(Oid relid);

extern void heap_truncate(List *relids);

extern void heap_truncate_one_rel(Relation rel);

extern void heap_truncate_check_FKs(List *relations, bool tempTables);

extern List *heap_truncate_find_FKs(List *relationIds);

extern void InsertPgAttributeTuple(Relation pg_attribute_rel,
					   Form_pg_attribute new_attribute,
					   CatalogIndexState indstate);

extern void InsertPgClassTuple(Relation pg_class_desc,
				   Relation new_rel_desc,
				   Oid new_rel_oid,
				   Datum relacl,
				   Datum reloptions);

extern List *AddRelationNewConstraints(Relation rel,
						  List *newColDefaults,
						  List *newConstraints,
						  bool allow_merge,
						  bool is_local,
						  bool is_internal);
extern List *AddRelationConstraints(Relation rel,
						  List *rawColDefaults,
						  List *constraints);

extern void StoreAttrDefault(Relation rel, AttrNumber attnum,
				 Node *expr, bool is_internal);

extern Node *cookDefault(ParseState *pstate,
			Node *raw_default,
			Oid atttypid,
			int32 atttypmod,
			char *attname);

extern void DeleteRelationTuple(Oid relid);
extern void DeleteAttributeTuples(Oid relid);
extern void DeleteSystemAttributeTuples(Oid relid);
extern void RemoveAttributeById(Oid relid, AttrNumber attnum);
extern void RemoveAttrDefault(Oid relid, AttrNumber attnum,
				  DropBehavior behavior, bool complain, bool internal);
extern void RemoveAttrDefaultById(Oid attrdefId);
extern void RemoveStatistics(Oid relid, AttrNumber attnum);

extern Form_pg_attribute SystemAttributeDefinition(AttrNumber attno,
						  bool relhasoids);

extern Form_pg_attribute SystemAttributeByName(const char *attname,
					  bool relhasoids);

extern void CheckAttributeNamesTypes(TupleDesc tupdesc, char relkind,
						 bool allow_system_table_mods);

extern void CheckAttributeType(const char *attname,
				   Oid atttypid, Oid attcollation,
				   List *containing_rowtypes,
				   bool allow_system_table_mods);
extern void SetRelationNumChecks(Relation rel, int numchecks);

/* MPP-6929: metadata tracking */
extern void MetaTrackAddObject(Oid		classid, 
							   Oid		objoid, 
							   Oid		relowner,
							   char*	actionname,
							   char*	subtype);
extern void MetaTrackUpdObject(Oid		classid, 
							   Oid		objoid, 
							   Oid		relowner,
							   char*	actionname,
							   char*	subtype);
extern void MetaTrackDropObject(Oid		classid, 
								Oid		objoid);

#define MetaTrackValidRelkind(relkind) \
		(((relkind) == RELKIND_RELATION) \
		|| ((relkind) == RELKIND_INDEX) \
		|| ((relkind) == RELKIND_SEQUENCE) \
		|| ((relkind) == RELKIND_VIEW)) 

extern bool should_have_valid_relfrozenxid(char relkind,
										   char relstorage,
										   bool is_partition_parent);
#endif   /* HEAP_H */
