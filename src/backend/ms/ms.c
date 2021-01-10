/*-------------------------------------------------------------------------
 *
 * ms.c
 *	  Process under QD postmaster polls the segments on a periodic basis
 *    or at the behest of QEs.
 *
 * Maintains an array in shared memory containing the state of each segment.
 *
 * Portions Copyright (c) 2005-2010, Greenplum Inc.
 * Portions Copyright (c) 2011, EMC Corp.
 * Portions Copyright (c) 2012-2019 Pivotal Software, Inc.
 * Portions Copyright (c) 2019-present Tencent [hongyaozhao], Inc.
 *
 * IDENTIFICATION
 *	    src/backend/ms/ms.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/gp_segment_config.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "cdb/cdbvars.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"

#include "cdb/cdbfts.h"
#include "postmaster/fts.h"
#include "postmaster/ftsprobe.h"

#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/pmsignal.h"			/* PostmasterIsAlive */
#include "storage/sinvaladt.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "catalog/catalog.h"

#include "catalog/gp_configuration_history.h"
#include "catalog/gp_segment_config.h"

#include "storage/backendid.h"

#include "executor/spi.h"
#include "tcop/tcopprot.h" /* quickdie() */
#include "postmaster/startup.h"
#include "tdb/session_processor.h"
#include "tdb/mssender.h"
#include "tdb/ms.h"
#include "tdb/storage_param.h"
/*
 * CONSTANTS
 */
/* maximum number of segments */
#define MAX_NUM_OF_SEGMENTS  32768

bool am_mshandler = false;
bool am_msprobe = false;

int ms_heartbeat_interval = 20;
static volatile pid_t *shmMSProbePID;
#define GpConfigHistoryRelName    "gp_configuration_history"


/*
 * STATIC VARIABLES
 */

static bool skipFtsProbe = false;

static volatile bool shutdown_requested = false;
static volatile bool probe_requested = false;
static volatile sig_atomic_t got_SIGHUP = false;

//static char *probeDatabase = "postgres";

/*
 * FUNCTION PROTOTYPES
 */

#ifdef EXEC_BACKEND
static pid_t MSprobe_forkexec(void);
#endif
//NON_EXEC_STATIC void MSMain(int argc, char *argv[]);
static void MSLoop(void);

static CdbComponentDatabases *readCdbComponentInfoAndUpdateStatus(MemoryContext);

/*=========================================================================
 * HELPER FUNCTIONS
 */
/* SIGHUP: set flag to reload config file */
static void
sigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

/* SIGINT: set flag to indicate a FTS scan is requested */
static void
sigIntHandler(SIGNAL_ARGS)
{
	probe_requested = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

bool
MSStartRule(Datum main_arg)
{
	/* we only start fts probe on master when -E is specified */
	if (IsUnderMasterDispatchMode())
		return true;

	return false;
}

void
MSProbeShmemInit(void)
{
	if (IsUnderPostmaster)
		return;

	shmMSProbePID = (volatile pid_t*)ShmemAlloc(sizeof(*shmMSProbePID));
	*shmMSProbePID = 0;
}

pid_t
MSProbePID(void)
{
	return *shmMSProbePID;
}

/*
 * MSProbeMain
 */
void
MSMain(Datum main_arg)
{
	IsUnderPostmaster = true;
	am_msprobe = true;
	*shmMSProbePID = MyProcPid;

	/*
	 * reread postgresql.conf if requested
	 */
	pqsignal(SIGHUP, sigHupHandler);
	pqsignal(SIGINT, sigIntHandler);
	/*
	 * CDB: Catch program error signals.
	 *
	 * Save our main thread-id for comparison during signals.
	 */
	main_tid = pthread_self();

#ifdef SIGSEGV
	pqsignal(SIGSEGV, CdbProgramErrorHandler);
#endif

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(DB_FOR_COMMON_ACCESS, NULL);

	/* [hongyaozhao] Init storage mq. */
	if (IsUnderPostmaster && !am_startup)
    {
		SessionInitMQ();
    }

	/* main loop */
	MSLoop();

	/* One iteration done, go away */
	proc_exit(0);
}

/*
 * Populate cdb_component_dbs object by reading from catalog.  Use
 * probeContext instead of current memory context because current
 * context will be destroyed by CommitTransactionCommand().
 */
static
CdbComponentDatabases *readCdbComponentInfoAndUpdateStatus(MemoryContext probeContext)
{
	int i;
	CdbComponentDatabases *cdbs = cdbcomponent_getCdbComponents();

	for (i=0; i < cdbs->total_segment_dbs; i++)
	{
		CdbComponentDatabaseInfo *segInfo = &cdbs->segment_db_info[i];
		uint8	segStatus = 0;

		if (!SEGMENT_IS_ALIVE(segInfo))
			FTS_STATUS_SET_DOWN(segStatus);

		ftsProbeInfo->status[segInfo->config->dbid] = segStatus;
	}

	/*
	 * Initialize fts_stausVersion after populating the config details in
	 * shared memory for the first time after FTS startup.
	 */
	if (ftsProbeInfo->status_version == 0)
	{
		ftsProbeInfo->status_version++;
		//writeGpSegConfigToFTSFiles();
	}

	return cdbs;
}

static
void MSLoop()
{
	bool	updated_probe_state = false;
	MemoryContext probeContext = NULL, oldContext = NULL;
	time_t elapsed,	probe_start_time, timeout;
	CdbComponentDatabases *cdbs = NULL;

	probeContext = AllocSetContextCreate(TopMemoryContext,
										 "MSProbeMemCtxt",
										 ALLOCSET_DEFAULT_INITSIZE,	/* always have some memory */
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);
	while (true)
	{
		int			rc;
		/* no need to live on if postmaster has died */
		if (!PostmasterIsAlive())
			exit(1);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		CHECK_FOR_INTERRUPTS();

		SIMPLE_FAULT_INJECTOR("ftsLoop_before_probe");

		probe_start_time = time(NULL);

		SpinLockAcquire(&ftsProbeInfo->lock);
		ftsProbeInfo->start_count++;
		SpinLockRelease(&ftsProbeInfo->lock);

		/* Need a transaction to access the catalogs */
		StartTransactionCommand();

		cdbs = readCdbComponentInfoAndUpdateStatus(probeContext);
		ssm_statistics->seg_count = cdbs->total_segments;
		/* close the transaction we started above */
		CommitTransactionCommand();

		/* Reset this as we are performing the probe */
		probe_requested = false;
		skipFtsProbe = false;

		if (SIMPLE_FAULT_INJECTOR("fts_probe") == FaultInjectorTypeSkip)
			skipFtsProbe = true;

		if (skipFtsProbe)
		{
			elogif(gp_log_fts >= GPVARS_VERBOSITY_VERBOSE, LOG,
				   "skipping MS probes due to %s", "fts_probe fault");
		}
		else
		{
			elogif(gp_log_fts == GPVARS_VERBOSITY_DEBUG, LOG,
				   "FTS: starting %s scan with %d segments and %d contents",
				   (probe_requested ? "full " : ""),
				   cdbs->total_segment_dbs,
				   cdbs->total_segments);
			/*
			* We probe in a special context, some of the heap access
			* stuff palloc()s internally
			*/
			oldContext = MemoryContextSwitchTo(probeContext);

			//updated_probe_state = MSWalRepMessageSegments(cdbs);

			MemoryContextSwitchTo(oldContext);

			/* free any pallocs we made inside probeSegments() */
			MemoryContextReset(probeContext);

			/* Bump the version if configuration was updated. */
			if (updated_probe_state)
				ftsProbeInfo->status_version++;
		}

		/* free current components info and free ip addr caches */
		cdbcomponent_destroyCdbComponents();


		SIMPLE_FAULT_INJECTOR("ftsLoop_after_probe");

		/* Notify any waiting backends about probe cycle completion. */
		SpinLockAcquire(&ftsProbeInfo->lock);
		ftsProbeInfo->done_count = ftsProbeInfo->start_count;
		SpinLockRelease(&ftsProbeInfo->lock);

		/* check if we need to sleep before starting next iteration */

		elapsed = time(NULL) - probe_start_time;
		timeout = elapsed >= gp_fts_probe_interval ? 0 :
							gp_fts_probe_interval - elapsed;

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   timeout * 1000L);

		SIMPLE_FAULT_INJECTOR("ftsLoop_after_latch");

		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	} /* end server loop */

	return;
}
