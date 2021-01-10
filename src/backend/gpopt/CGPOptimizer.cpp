//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum, Inc.
//
//	@filename:
//		CGPOptimizer.cpp
//
//	@doc:
//		Entry point to GP optimizer
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/CGPOptimizer.h"
#include "gpopt/utils/COptTasks.h"

// the following headers are needed to reference optimizer library initializers
#include "naucrates/init.h"
#include "gpopt/init.h"
#include "gpos/_api.h"
#include "gpopt/gpdbwrappers.h"

#include "naucrates/exception.h"
#include "utils/guc.h"

extern MemoryContext MessageContext;

//---------------------------------------------------------------------------
//	@function:
//		CGPOptimizer::PlstmtOptimize
//
//	@doc:
//		Optimize given query using GP optimizer
//
//---------------------------------------------------------------------------
PlannedStmt *
CGPOptimizer::GPOPTOptimizedPlan
	(
	Query *query,
	bool *had_unexpected_failure // output : set to true if optimizer unexpectedly failed to produce plan
	)
{
	SOptContext gpopt_context;
	PlannedStmt* plStmt = NULL;

	*had_unexpected_failure = false;

	GPOS_TRY
	{
		plStmt = COptTasks::GPOPTOptimizedPlan(query, &gpopt_context);
		// clean up context
		gpopt_context.Free(gpopt_context.epinQuery, gpopt_context.epinPlStmt);
	}
	GPOS_CATCH_EX(ex)
	{
		// clone the error message before context free.
		CHAR* serialized_error_msg = gpopt_context.CloneErrorMsg(MessageContext);
		// clean up context
		gpopt_context.Free(gpopt_context.epinQuery, gpopt_context.epinPlStmt);

		// Special handler for a few common user-facing errors. In particular,
		// we want to use the correct error code for these, in case an application
		// tries to do something smart with them. Also, ERRCODE_INTERNAL_ERROR
		// is handled specially in elog.c, and we don't want that for "normal"
		// application errors.
		if (GPOS_MATCH_EX(ex, gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLNotNullViolation))
		{
			errstart(ERROR, ex.Filename(), ex.Line(), NULL, TEXTDOMAIN);
			errfinish(errcode(ERRCODE_NOT_NULL_VIOLATION),
				  errmsg("%s", serialized_error_msg));
		}

		else if (GPOS_MATCH_EX(ex, gpdxl::ExmaDXL, gpdxl::ExmiOptimizerError) ||
			 gpopt_context.m_should_error_out)
		{
			Assert(NULL != serialized_error_msg);
			errstart(ERROR, ex.Filename(), ex.Line(), NULL, TEXTDOMAIN);
			errfinish(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("%s", serialized_error_msg));
		}
		else if (GPOS_MATCH_EX(ex, gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError))
		{
			PG_RE_THROW();
		}
		else if (GPOS_MATCH_EX(ex, gpdxl::ExmaDXL, gpdxl::ExmiNoAvailableMemory))
		{
			errstart(ERROR, ex.Filename(), ex.Line(), NULL, TEXTDOMAIN);
			errfinish(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("no available memory to allocate string buffer"));
		}
		else if (GPOS_MATCH_EX(ex, gpdxl::ExmaDXL, gpdxl::ExmiInvalidComparisonTypeCode))
		{
			errstart(ERROR, ex.Filename(), ex.Line(), NULL, TEXTDOMAIN);
			errfinish(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("invalid comparison type code. Valid values are Eq, NEq, LT, LEq, GT, GEq."));
		}

		// Failed to produce a plan, but it wasn't an error that should
		// be propagated to the user. Log the failure if needed, and
		// return without a plan. The caller should fall back to the
		// Postgres planner.

		if (optimizer_trace_fallback)
		{
			errstart(INFO, ex.Filename(), ex.Line(), NULL, TEXTDOMAIN);
			errfinish(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("GPORCA failed to produce a plan, falling back to planner"),
				  serialized_error_msg ? errdetail("%s", serialized_error_msg) : 0);
		}

		*had_unexpected_failure = gpopt_context.m_is_unexpected_failure;

		if (serialized_error_msg)
			pfree(serialized_error_msg);
	}
	GPOS_CATCH_END;
	return plStmt;
}


//---------------------------------------------------------------------------
//	@function:
//		CGPOptimizer::SerializeDXLPlan
//
//	@doc:
//		Serialize planned statement into DXL
//
//---------------------------------------------------------------------------
char *
CGPOptimizer::SerializeDXLPlan
	(
	Query *query
	)
{
	GPOS_TRY;
	{
		return COptTasks::Optimize(query);
	}
	GPOS_CATCH_EX(ex);
	{
		errstart(ERROR, ex.Filename(), ex.Line(), NULL, TEXTDOMAIN);
		errfinish(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("optimizer failed to produce plan"));
	}
	GPOS_CATCH_END;
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		InitGPOPT()
//
//	@doc:
//		Initialize GPTOPT and dependent libraries
//
//---------------------------------------------------------------------------
void
CGPOptimizer::InitGPOPT ()
{
  // Use GPORCA's default allocators
  void *(*gpos_alloc)(size_t) = NULL;
  void (*gpos_free)(void *) = NULL;
  if (optimizer_use_gpdb_allocators)
  {
	gpos_alloc = gpdb::OptimizerAlloc;
	gpos_free = gpdb::OptimizerFree;
  }
  struct gpos_init_params params =
	{gpos_alloc, gpos_free, gpdb::IsAbortRequested};
  gpos_init(&params);
  gpdxl_init();
  gpopt_init();
}

//---------------------------------------------------------------------------
//	@function:
//		TerminateGPOPT()
//
//	@doc:
//		Terminate GPOPT and dependent libraries
//
//---------------------------------------------------------------------------
void
CGPOptimizer::TerminateGPOPT ()
{
  gpopt_terminate();
  gpdxl_terminate();
  gpos_terminate();
}

//---------------------------------------------------------------------------
//	@function:
//		GPOPTOptimizedPlan
//
//	@doc:
//		Expose GP optimizer API to C files
//
//---------------------------------------------------------------------------
extern "C"
{
PlannedStmt *GPOPTOptimizedPlan
	(
	Query *query,
	bool *had_unexpected_failure
	)
{
	return CGPOptimizer::GPOPTOptimizedPlan(query, had_unexpected_failure);
}
}

//---------------------------------------------------------------------------
//	@function:
//		SerializeDXLPlan
//
//	@doc:
//		Serialize planned statement to DXL
//
//---------------------------------------------------------------------------
extern "C"
{
char *SerializeDXLPlan
	(
	Query *query
	)
{
	return CGPOptimizer::SerializeDXLPlan(query);
}
}

//---------------------------------------------------------------------------
//	@function:
//		InitGPOPT()
//
//	@doc:
//		Initialize GPTOPT and dependent libraries
//
//---------------------------------------------------------------------------
extern "C"
{
void InitGPOPT ()
{
	return CGPOptimizer::InitGPOPT();
}
}

//---------------------------------------------------------------------------
//	@function:
//		TerminateGPOPT()
//
//	@doc:
//		Terminate GPOPT and dependent libraries
//
//---------------------------------------------------------------------------
extern "C"
{
void TerminateGPOPT ()
{
	return CGPOptimizer::TerminateGPOPT();
}
}

// EOF
