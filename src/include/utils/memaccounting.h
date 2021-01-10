/*-------------------------------------------------------------------------
 *
 * memaccounting.h
 *	  This file contains declarations for memory instrumentation utility
 *	  functions.
 *
 * Portions Copyright (c) 2013, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/utils/meminstrumentation.h,v 1.00 2013/03/22 14:57:00 rahmaf2 Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef MEMACCOUNTING_H
#define MEMACCOUNTING_H

#include "lib/stringinfo.h"             /* StringInfo */

struct MemoryContextData;

/* Macros to define the level of memory accounting to show in EXPLAIN ANALYZE */
#define EXPLAIN_MEMORY_VERBOSITY_SUPPRESS  0 /* Suppress memory reporting in explain analyze */
#define EXPLAIN_MEMORY_VERBOSITY_SUMMARY  1 /* Summary of memory usage for each owner in explain analyze */
#define EXPLAIN_MEMORY_VERBOSITY_DETAIL  2 /* Detail memory accounting tree for each slice in explain analyze */
#define EXPLAIN_MEMORY_VERBOSITY_DEBUG  3 /* Detail memory accounting tree with every executor having its own account
										   * for each slice in explain analyze */

/*
 * MemoryAccount is a private data structure for recording memory usage by
 * memory managers, MemoryAccountExplain is a publicly exposed structure used
 * for copying out the interesting parts of a MemoryAccount for usage in
 * EXPLAIN {ANALYZE} commands.
 */
typedef struct MemoryAccountExplain {
	double allocated;
	double freed;
	double peak;
} MemoryAccountExplain;

/*
 * What level of details of the memory accounting information to show during EXPLAIN ANALYZE?
 */
extern int explain_memory_verbosity;

/*
 * Unique run id for memory profiling. May be just a start timestamp for a batch of queries such as TPCH
 */
extern char* memory_profiler_run_id;

/*
 * Dataset ID. Determined by the external script. One example could be, 1: TPCH, 2: TPCDS etc.
 */
extern char* memory_profiler_dataset_id;

/*
 * Which query of the query suite is running currently. E.g., query 21 of TPCH
 */
extern char* memory_profiler_query_id;

/*
 * Scale factor of TPCH/TPCDS etc.
 */
extern int memory_profiler_dataset_size;


/*
 * Each memory account can assume one of the following memory
 * owner types
 */
typedef enum MemoryOwnerType
{
	/*
	 * Undefined represents invalid account. We explicitly start from 0
	 * as we use the long living accounts' enumeration to index into the
	 * long living accounts array.
	 */
	MEMORY_OWNER_TYPE_Undefined = 0,

	/* Long-living accounts that survive reset */
	MEMORY_OWNER_TYPE_START_LONG_LIVING,
	MEMORY_OWNER_TYPE_LogicalRoot = MEMORY_OWNER_TYPE_START_LONG_LIVING,
	MEMORY_OWNER_TYPE_SharedChunkHeader,
	MEMORY_OWNER_TYPE_Rollover,
	MEMORY_OWNER_TYPE_MemAccount,
	MEMORY_OWNER_TYPE_Exec_AlienShared,
	MEMORY_OWNER_TYPE_Exec_RelinquishedPool,
	MEMORY_OWNER_TYPE_END_LONG_LIVING = MEMORY_OWNER_TYPE_Exec_RelinquishedPool,
	/* End of long-living accounts */

	/* Short-living accounts */
	MEMORY_OWNER_TYPE_START_SHORT_LIVING,
	MEMORY_OWNER_TYPE_Top = MEMORY_OWNER_TYPE_START_SHORT_LIVING,
	MEMORY_OWNER_TYPE_MainEntry,
	MEMORY_OWNER_TYPE_Parser,
	MEMORY_OWNER_TYPE_Planner,
	MEMORY_OWNER_TYPE_PlannerHook,
	MEMORY_OWNER_TYPE_Optimizer,
	MEMORY_OWNER_TYPE_Dispatcher,
	MEMORY_OWNER_TYPE_Serializer,
	MEMORY_OWNER_TYPE_Deserializer,

	MEMORY_OWNER_TYPE_EXECUTOR_START,
	MEMORY_OWNER_TYPE_EXECUTOR = MEMORY_OWNER_TYPE_EXECUTOR_START,
	MEMORY_OWNER_TYPE_Exec_Result,
	MEMORY_OWNER_TYPE_Exec_Append,
	MEMORY_OWNER_TYPE_Exec_Sequence,
	MEMORY_OWNER_TYPE_Exec_MergeAppend,
	MEMORY_OWNER_TYPE_Exec_BitmapAnd,
	MEMORY_OWNER_TYPE_Exec_BitmapOr,
	MEMORY_OWNER_TYPE_Exec_SeqScan,
	MEMORY_OWNER_TYPE_Exec_DynamicSeqScan,
	MEMORY_OWNER_TYPE_Exec_ExternalScan,
	MEMORY_OWNER_TYPE_Exec_IndexScan,
	MEMORY_OWNER_TYPE_Exec_IndexOnlyScan,
	MEMORY_OWNER_TYPE_Exec_DynamicIndexScan,
	MEMORY_OWNER_TYPE_Exec_BitmapIndexScan,
	MEMORY_OWNER_TYPE_Exec_DynamicBitmapIndexScan,
	MEMORY_OWNER_TYPE_Exec_BitmapHeapScan,
	MEMORY_OWNER_TYPE_Exec_DynamicBitmapHeapScan,
	MEMORY_OWNER_TYPE_Exec_TidScan,
	MEMORY_OWNER_TYPE_Exec_SubqueryScan,
	MEMORY_OWNER_TYPE_Exec_FunctionScan,
	MEMORY_OWNER_TYPE_Exec_TableFunctionScan,
	MEMORY_OWNER_TYPE_Exec_ValuesScan,
	MEMORY_OWNER_TYPE_Exec_NestLoop,
	MEMORY_OWNER_TYPE_Exec_MergeJoin,
	MEMORY_OWNER_TYPE_Exec_HashJoin,
	MEMORY_OWNER_TYPE_Exec_Material,
	MEMORY_OWNER_TYPE_Exec_Sort,
	MEMORY_OWNER_TYPE_Exec_Agg,
	MEMORY_OWNER_TYPE_Exec_Unique,
	MEMORY_OWNER_TYPE_Exec_Hash,
	MEMORY_OWNER_TYPE_Exec_SetOp,
	MEMORY_OWNER_TYPE_Exec_Limit,
	MEMORY_OWNER_TYPE_Exec_Motion,
	MEMORY_OWNER_TYPE_Exec_ShareInputScan,
	MEMORY_OWNER_TYPE_Exec_WindowAgg,
	MEMORY_OWNER_TYPE_Exec_Repeat,
	MEMORY_OWNER_TYPE_Exec_ModifyTable,
	MEMORY_OWNER_TYPE_Exec_LockRows,
	MEMORY_OWNER_TYPE_Exec_DML,
	MEMORY_OWNER_TYPE_Exec_SplitUpdate,
	MEMORY_OWNER_TYPE_Exec_RowTrigger,
	MEMORY_OWNER_TYPE_Exec_AssertOp,
	MEMORY_OWNER_TYPE_Exec_PartitionSelector,
	MEMORY_OWNER_TYPE_Exec_RecursiveUnion,
	MEMORY_OWNER_TYPE_Exec_CteScan,
	MEMORY_OWNER_TYPE_Exec_WorkTableScan,
	MEMORY_OWNER_TYPE_Exec_ForeignScan,
	MEMORY_OWNER_TYPE_Exec_NestedExecutor,
	MEMORY_OWNER_TYPE_EXECUTOR_END = MEMORY_OWNER_TYPE_Exec_NestedExecutor,
	MEMORY_OWNER_TYPE_END_SHORT_LIVING = MEMORY_OWNER_TYPE_EXECUTOR_END
} MemoryOwnerType;

/****
 * The following are constants to define additional memory stats
 * (in addition to memory accounts) during CSV dump of memory balance
 */
/* vmem reserved from memprot.c */
#define MEMORY_STAT_TYPE_VMEM_RESERVED -1
/* Peak memory observed from inside memory accounting among all allocations */
#define MEMORY_STAT_TYPE_MEMORY_ACCOUNTING_PEAK -2
/***************************************************************************/

typedef uint64 MemoryAccountIdType;

extern MemoryAccountIdType ActiveMemoryAccountId;

/*
 * START_MEMORY_ACCOUNT would switch to the specified newMemoryAccount,
 * saving the oldActiveMemoryAccount. Must be paired with END_MEMORY_ACCOUNT
 */
#define START_MEMORY_ACCOUNT(newMemoryAccountId)  \
	do { \
		MemoryAccountIdType oldActiveMemoryAccountId = ActiveMemoryAccountId; \
		ActiveMemoryAccountId = newMemoryAccountId;

/*
 * END_MEMORY_ACCOUNT would restore the previous memory account that was
 * active at the time of START_MEMORY_ACCCOUNT call
 */
#define END_MEMORY_ACCOUNT()  \
		ActiveMemoryAccountId = oldActiveMemoryAccountId;\
	} while (0);



extern MemoryAccountIdType
MemoryAccounting_CreateAccount(long maxLimit, enum MemoryOwnerType ownerType);

extern MemoryAccountIdType
MemoryAccounting_SwitchAccount(MemoryAccountIdType desiredAccountId);

extern size_t
MemoryAccounting_SizeOfAccountInBytes(void);

extern void
MemoryAccounting_Reset(void);

extern uint32
MemoryAccounting_Serialize(StringInfoData* buffer);

extern uint64
MemoryAccounting_GetAccountPeakBalance(MemoryAccountIdType memoryAccountId);

extern uint64
MemoryAccounting_GetAccountCurrentBalance(MemoryAccountIdType memoryAccountId);

extern uint64
MemoryAccounting_GetGlobalPeak(void);

extern void
MemoryAccounting_CombinedAccountArrayToExplain(void *accountArrayBytes,
											  MemoryAccountIdType accountCount,
											  void *es);

extern void
MemoryAccounting_SaveToFile(int currentSliceId);

extern void
MemoryAccounting_SaveToLog(void);

extern void
MemoryAccounting_PrettyPrint(void);

extern uint64
MemoryAccounting_DeclareDone(void);

extern uint64
MemoryAccounting_RequestQuotaIncrease(void);

extern MemoryAccountExplain *
MemoryAccounting_ExplainCurrentOptimizerAccountInfo(void);

extern MemoryAccountIdType
MemoryAccounting_CreateMainExecutor(void);

extern MemoryAccountIdType
MemoryAccounting_GetOrCreateNestedExecutorAccount(void);

extern MemoryAccountIdType
MemoryAccounting_GetOrCreateOptimizerAccount(void);

/*
 * MemoryAccounting_GetOrCreatePlannerAccount creates a memory account for
 * query planning. If the planner is a direct child of the 'X_NestedExecutor'
 * account, the planner account will also be assigned to 'X_NestedExecutor'.
 *
 * The planner account will always be created if the explain_memory_verbosity
 * guc is set to 'debug' or above.
 */
extern MemoryAccountIdType
MemoryAccounting_GetOrCreatePlannerAccount(void);

extern bool
MemoryAccounting_IsUnderNestedExecutor(void);

extern bool
MemoryAccounting_IsMainExecutorCreated(void);

/*
 * MemoryAccounting_CreateExecutorMemoryAccount will create the main executor
 * account. Any subsequent calls to this function will assign the executor to
 * the 'X_NestedExecutor' account.
 *
 * If the explain_memory_verbosity guc is set to 'debug' or above, all
 * executors will be given its own memory account.
 */
static inline MemoryAccountIdType
MemoryAccounting_CreateExecutorMemoryAccount(void)
{
	if (explain_memory_verbosity >= EXPLAIN_MEMORY_VERBOSITY_DEBUG)
		return MemoryAccounting_CreateAccount(0, MEMORY_OWNER_TYPE_EXECUTOR);
	else
		if (MemoryAccounting_IsMainExecutorCreated())
			return MemoryAccounting_GetOrCreateNestedExecutorAccount();
		else
			return MemoryAccounting_CreateMainExecutor();
}


#endif   /* MEMACCOUNTING_H */
