/*-------------------------------------------------------------------------
 *
 * cdbvars.h
 *	  definitions for Greenplum-specific global variables
 *
 * Portions Copyright (c) 2003-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbvars.h
 *
 * NOTES
 *	  See src/backend/utils/misc/guc_gp.c for variable external specification.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBVARS_H
#define CDBVARS_H

#include "access/xlogdefs.h"  /*XLogRecPtr*/
#include "catalog/gp_segment_config.h" /* MASTER_CONTENT_ID */

/*
 * ----- Declarations of Greenplum-specific global variables ------
 */

#ifdef sparc
#define TUPLE_CHUNK_ALIGN	4
#else
#define TUPLE_CHUNK_ALIGN	1
#endif

#ifndef PRIO_MAX
#define PRIO_MAX 20
#endif
/*
 * Parameters gp_session_role and gp_role
 *
 * The run-time parameters (GUC variables) gp_session_role and
 * gp_role report and provide control over the role assumed by a
 * postgres process.
 *
 * Valid  roles are the following:
 *
 *	dispatch	The process acts as a parallel SQL dispatcher.
 *	execute		The process acts as a parallel SQL executor.
 *	utility		The process acts as a simple SQL engine.
 *
 * Both parameters are initialized to the same value at connection
 * time and are local to the backend process resulting from the
 * connection.	The default is dispatch which is the normal setting
 * for a user of Greenplum connecting to a node of  the Greenplum cluster.
 * Neither parameter appears in the configuration file.
 *
 * gp_session_role
 *
 * - does not affect the operation of the backend, and
 * - does not change during the lifetime of PostgreSQL session.
 *
 * gp_role
 *
 * - determines the operating role of the backend, and
 * - may be changed by a superuser via the SET command.
 *
 * The connection time value of gp_session_role used by a
 * libpq-based client application can be specified using the
 * environment variable PGOPTIONS.	For example, this is how to
 * invoke psql on database test as a utility:
 *
 *	PGOPTIONS='-c gp_session_role=utility' psql test
 *
 * Alternatively, libpq-based applications can be modified to set
 * the value directly via an argument to one of the libpq functions
 * PQconnectdb, PQsetdbLogin or PQconnectStart.
 *
 * Don't try to set gp_role this way.  At the time options are
 * processed it is unknown whether the user is a superuser, so
 * the attempt will be rejected.
 *
 * ----------
 *
 * Greenplum Developers can access the values of these parameters via the
 * global variables Gp_session_role and Gp_role of type
 * GpRoleValue. For example
 *
 *	#include "cdb/cdbvars.h"
 *
 *	switch ( Gp_role  )
 *	{
 *		case GP_ROLE_DISPATCH:
 *			... Act like a query dispatcher ...
 *			break;
 *		case GP_ROLE_EXECUTE:
 *			... Act like a query executor ...
 *			break;
 *		case GP_ROLE_UTILITY:
 *			... Act like an unmodified PostgreSQL backend. ...
 *			break;
 *		default:
 *			... Won't happen ..
 *			break;
 *	}
 *
 * You can also modify Gp_role (even if the session doesn't have
 * superuser privileges) by setting it to one of the three valid
 * values, however this must be well documented to avoid
 * disagreements between modules.  Don't modify the value  of
 * Gp_session_role.
 *
 */

typedef enum
{
	GP_ROLE_UTILITY = 0,		/* Operating as a simple database engine */
	GP_ROLE_DISPATCH,			/* Operating as the parallel query dispatcher */
	GP_ROLE_DISPATCH_ASS,		/* Operating as the assist parallel query dispatcher */
	GP_ROLE_EXECUTE,			/* Operating as a parallel query executor */
	GP_ROLE_UNDEFINED			/* Should never see this role in use */
} GpRoleValue;


extern GpRoleValue Gp_session_role;	/* GUC var - server startup mode.  */
extern char *gp_session_role_string;	/* Use by guc.c as staging area for
										 * value. */

extern GpRoleValue Gp_role;	/* GUC var - server operating mode.  */
extern char *gp_role_string;	/* Use by guc.c as staging area for value. */

extern bool gp_reraise_signal; /* try to force a core dump ?*/

extern bool gp_set_proc_affinity; /* try to bind postmaster to a processor */

/* Parameter Gp_is_writer
 *
 * This run_time parameter indicates whether session is a qExec that is a
 * "writer" process.  Only writers are allowed to write changes to the database
 * and currently there is only one writer per group of segmates.
 *
 * It defaults to false, but may be specified as true using a connect option.
 * This should only ever be set by a QD connecting to a QE, rather than
 * directly.
 */
extern bool Gp_is_writer;

/* Parameter gp_session_id
 *
 * This run time parameter indicates a unique id to identify a particular user
 * session throughout the entire Greenplum array.
 */
extern int gp_session_id;

/* The Hostname where this segment's QD is located. This variable is NULL for the QD itself */
extern char * qdHostname;

/* The Postmaster listener port for the QD.  This variable is 0 for the QD itself.*/
extern int qdPostmasterPort;

/*How many gangs to keep around from stmt to stmt.*/
extern int			gp_cached_gang_threshold;

/*
 * gp_reject_percent_threshold
 *
 * When reject limit in single row error handling is specified in PERCENT,
 * we don't want to start calculating the percent of rows rejected right
 * from the 1st row, since for example, if the second row is bad this will
 * mean that 50% of the data is bad even if all the following rows are valid.
 * Therefore we want to keep track of count of bad rows but start calculating
 * the percent at a later stage in the game. By default we'll start the
 * calculation after 100 rows were processed, but this GUC can be set for
 * some other number if it makes more sense in certain situations.
 */
extern int			gp_reject_percent_threshold;

/*
 * For use while debugging DTM issues: alter MVCC semantics such that
 * "invisible" rows are returned.
 */
extern bool           gp_select_invisible;

/*
 * Used to set the maximum length of the current query which is displayed
 * when the user queries pg_stat_activty table.
 */
extern int			pgstat_track_activity_query_size;

/*
 * In order to facilitate testing of reader-gang/writer-gang synchronization
 * it is very handy to slow down the writer (opens important race-window).
 */
extern bool           gp_enable_slow_writer_testmode;

/*
 * MPP-6926: Resource Queues on by default
 */
#define GP_DEFAULT_RESOURCE_QUEUE_NAME "pg_default"

/* Parameter gp_debug_pgproc
 *
 * This run-time parameter requests to print out detailed info relevant to
 * PGPROC.
 */
extern bool gp_debug_pgproc;

/* Parameter debug_print_prelim_plan
 *
 * This run-time parameter is closely related to the PostgreSQL parameter
 * debug_print_plan which, if true, causes the final plan to display on the
 * server log prior to execution.  This parameter, if true, causes the
 * preliminary plan (from the optimizer prior to cdbparallelize) to display
 * on the log.
 */
extern bool Debug_print_prelim_plan;

/* Parameter debug_print_slice_table
 *
 * This run-time parameter, if true, causes the slice table for a plan to
 * display on the server log.  On a QD this occurs prior to sending the
 * slice table to any QE.  On a QE it occurs just prior to execution of
 * the plan slice.
 */
extern bool Debug_print_slice_table;

extern bool Debug_resource_group;

extern bool Debug_burn_xids;

/*
 * gp_external_enable_exec
 *
 * when set to 'true' external tables that were defined with an EXECUTE
 * clause (will execute OS level commands) are allowed to be SELECTed from.
 * When set to 'false' trying to SELECT from such a table will result in an
 * error. Default is 'true'
 */
extern bool gp_external_enable_exec;

/* gp_external_max_segs
 *
 * The maximum number of segment databases that will get assigned to
 * connect to a specific gpfdist (or gpfdists) client (1 gpfdist:// URI). The limit
 * is good to have as there is a number of segdbs that as we increase
 * it won't add to performance but may actually decrease it, while
 * occupying unneeded resources. By having a limit we can control our
 * server resources better while not harming performance.
 */
extern int gp_external_max_segs;

/*
 * This option determines whether curl verifies the authenticity of the
 * gpfdist's certificate.
 */
extern bool verify_gpfdists_cert;

/*
 * gp_command_count
 *
 * This GUC is 0 at the beginning of a client session and
 * increments for each command received from the client.
 */
extern int gp_command_count;

/*
 * gp_safefswritesize
 *
 * should only be set to <N bytes> on file systems that the so called
 * 'torn-write' problem may occur. This guc should be set for the minimum
 * write size that is always safe on this particular file system. The side
 * effect of increasing this value for torn write protection is that single row
 * INSERTs into append only tables will use <N bytes> of space and therefore
 * also slow down SELECTs and become strongly discouraged.
 *
 * On mature file systems where torn write may not occur this GUC should be
 * set to 0 (the default).
 */
extern int gp_safefswritesize;

/*
 * Gp_write_shared_snapshot
 *
 * The value of this variable is actually meaningless. We use it simply
 * in order to have a mechanism of dispatching a SET query that will
 * allocate writer gangs that will write the shared snapshot and make
 * it available for the reader gangs. This is currently only needed
 * before DECLARE'ing a cursor, since a cursor query uses only reader
 * gangs. But should be used for other things if it makes sense to do so.
 */
extern bool Gp_write_shared_snapshot;

extern int gp_fts_transition_retries;
extern int gp_fts_transition_timeout;

/*
 * If number of subtransactions within a transaction exceed this limit,
 * then a warning is given to the user.
 */
extern int32 gp_subtrans_warn_limit;

extern const char *role_to_string(GpRoleValue role);

extern int	gp_segment_connect_timeout; /* GUC var - timeout specifier for gang creation */
extern int	gp_snapshotadd_timeout; /* GUC var - timeout specifier for snapshot-creation wait */

extern int	gp_fts_probe_retries; /* GUC var - specifies probe number of retries for FTS */
extern int	gp_fts_probe_timeout; /* GUC var - specifies probe timeout for FTS */
extern int	gp_fts_probe_interval; /* GUC var - specifies polling interval for FTS */
extern int gp_fts_mark_mirror_down_grace_period;

extern int gp_gang_creation_retry_count; /* How many retries ? */
extern int gp_gang_creation_retry_timer; /* How long between retries */

/*
 * Parameter Gp_max_packet_size
 *
 * The run-time parameter gp_max_packet_size controls the largest packet
 * size that the interconnect will transmit.  In general, the interconnect
 * tries to squeeze as many tuplechunks as possible into a packet without going
 * above this size limit.
 *
 * The largest tuple chunk size that the system will form can be derived
 * from this with:
 *		MAX_CHUNK_SIZE = Gp_max_packet_size - PACKET_HEADER_SIZE - TUPLE_CHUNK_HEADER_SIZE
 *
 *
 */
extern int	Gp_max_packet_size;	/* GUC var */

#define DEFAULT_PACKET_SIZE 8192
#define MIN_PACKET_SIZE 512
#define MAX_PACKET_SIZE 65507 /* Max payload for IPv4/UDP (subtract 20 more for IPv6 without extensions) */

/*
 * Support for multiple "types" of interconnect
 */
typedef enum GpVars_Interconnect_Type
{
	INTERCONNECT_TYPE_TCP = 0,
	INTERCONNECT_TYPE_UDPIFC,
} GpVars_Interconnect_Type;

extern int Gp_interconnect_type;

typedef enum GpVars_Interconnect_Method
{
	INTERCONNECT_FC_METHOD_CAPACITY = 0,
	INTERCONNECT_FC_METHOD_LOSS = 2,
} GpVars_Interconnect_Method;

extern int Gp_interconnect_fc_method;

/*
 * Parameter Gp_interconnect_queue_depth
 *
 * The run-time parameter Gp_interconnect_queue_depth controls the
 * number of outstanding messages allowed to accumulated on a
 * 'connection' before the peer will start to backoff.
 *
 * This guc is specific to the UDP-interconnect.
 *
 */
extern int	Gp_interconnect_queue_depth;

/*
 * Parameter Gp_interconnect_snd_queue_depth
 *
 * The run-time parameter Gp_interconnect_snd_queue_depth controls the
 * average number of outstanding messages on the sender side
 *
 * This guc is specific to the UDP-interconnect.
 *
 */
extern int	Gp_interconnect_snd_queue_depth;
extern int	Gp_interconnect_timer_period;
extern int	Gp_interconnect_timer_checking_period;
extern int	Gp_interconnect_default_rtt;
extern int	Gp_interconnect_min_rto;
extern int  Gp_interconnect_transmit_timeout;
extern int	Gp_interconnect_min_retries_before_timeout;
extern int	Gp_interconnect_debug_retry_interval;

/* UDP recv buf size in KB.  For testing */
extern int 	Gp_udp_bufsize_k;

/*
 * Parameter gp_interconnect_aggressive_retry
 *
 * The run-time parameter gp_interconnect_aggressive_retry controls the
 * activation of the application-level retry (which acts much faster than the OS-level
 * TCP retries); In most cases this should stay enabled.
 */
extern bool gp_interconnect_aggressive_retry; /* fast-track app-level retry */

/*
 * Parameter gp_interconnect_full_crc
 *
 * Perform a full CRC on UDP-packets as they depart and arrive.
 */
extern bool gp_interconnect_full_crc;

/*
 * Parameter gp_interconnect_log_stats
 *
 * Emit interconnect statistics at log-level, instead of debug1
 */
extern bool gp_interconnect_log_stats;

extern bool gp_interconnect_cache_future_packets;

#define UNDEF_SEGMENT -2

/*
 * Parameter interconnect_setup_timeout
 *
 * The run-time parameter (GUC variable) interconnect_setup_timeout is used
 * during SetupInterconnect() to timeout the operation.  This value is in
 * seconds.  Setting it to zero effectively disables the timeout.
 */
extern int	interconnect_setup_timeout;

#ifdef USE_ASSERT_CHECKING
/*
 * UDP-IC Test hooks (for fault injection).
 */
extern int gp_udpic_dropseg; /* specifies which segments to apply the
							  * following two gucs -- if set to
							  * UNDEF_SEGMENT, all segments apply
							  * them. */
extern int gp_udpic_dropxmit_percent;
extern int gp_udpic_dropacks_percent;
extern int gp_udpic_fault_inject_percent;
extern int gp_udpic_fault_inject_bitmap;
extern int gp_udpic_network_disable_ipv6;
#endif

/*
 * Each slice table has a unique ID (certain commands like "vacuum
 * analyze" run many many slice-tables for each gp_command_id). This
 * gets passed around as part of the slice-table (not used as a guc!).
 */
extern uint32 gp_interconnect_id;

/* --------------------------------------------------------------------------------------------------
 * Logging
 */

typedef enum GpVars_Verbosity
{
	GPVARS_VERBOSITY_UNDEFINED = 0,
    GPVARS_VERBOSITY_OFF,
	GPVARS_VERBOSITY_TERSE,
	GPVARS_VERBOSITY_VERBOSE,
    GPVARS_VERBOSITY_DEBUG,
} GpVars_Verbosity;

/* Enable single-slice single-row inserts. */
extern bool gp_enable_fast_sri;

/* Enable single-mirror pair dispatch. */
extern bool gp_enable_direct_dispatch;

/* Name of pseudo-function to access any table as if it was randomly distributed. */
#define GP_DIST_RANDOM_NAME "GP_DIST_RANDOM"

/*
 * gp_log_gang
 *
 * Should creation, reallocation and cleanup of gangs of QE processes be logged?
 * "OFF"     -> only errors are logged
 * "TERSE"   -> terse logging of routine events, e.g. creation of new qExecs
 * "VERBOSE" -> gang allocation per command is logged
 * "DEBUG"   -> additional events are logged at severity level DEBUG1 to DEBUG5
 *
 * The messages that are enabled by the TERSE and VERBOSE settings are
 * written with a severity level of LOG.
 */
extern int gp_log_gang;

/*
 * gp_log_fts
 *
 * What kind of messages should be logged by the fault-prober
 * "OFF"     -> only errors are logged
 * "TERSE"   -> terse logging of routine events
 * "VERBOSE" -> more messages
 * "DEBUG"   -> additional events are logged at severity level DEBUG1 to DEBUG5
 *
 * The messages that are enabled by the TERSE and VERBOSE settings are
 * written with a severity level of LOG.
 */
extern int gp_log_fts;

/*
 * gp_log_interconnect
 *
 * Should connections between internal processes be logged?  (qDisp/qExec/etc)
 * "OFF"     -> connection errors are logged
 * "TERSE"   -> terse logging of routine events, e.g. successful connections
 * "VERBOSE" -> most interconnect setup events are logged
 * "DEBUG"   -> additional events are logged at severity level DEBUG1 to DEBUG5.
 *
 * The messages that are enabled by the TERSE and VERBOSE settings are
 * written with a severity level of LOG.
 */
extern int gp_log_interconnect;

/* --------------------------------------------------------------------------------------------------
 * Greenplum Optimizer GUCs
 */

/*
 * "gp_motion_cost_per_row"
 *
 * If >0, the planner uses this value -- instead of 2 * "cpu_tuple_cost" --
 * for Motion operator cost estimation.
 */
extern double   gp_motion_cost_per_row;

/*
 * "gp_segments_for_planner"
 *
 * If >0, number of segment dbs for the planner to assume in its cost and size
 * estimates.  If 0, estimates are based on the actual number of segment dbs.
 */
extern int      gp_segments_for_planner;

/*
 * Enable/disable the special optimization of MIN/MAX aggregates as
 * Index Scan with limit.
 */
extern bool gp_enable_minmax_optimization;

/*
 * "gp_enable_multiphase_agg"
 *
 * Unlike some other enable... vars, gp_enable_multiphase_agg is not cost based.
 * When set to false, the planner will not use multi-phase aggregation.
 */
extern bool gp_enable_multiphase_agg;

/*
 * Perform a post-planning scan of the final plan looking for motion deadlocks:
 * emit verbose messages about any found.
 */
extern bool gp_enable_motion_deadlock_sanity;

/*
 * Adjust selectivity for nulltests atop of outer joins;
 * Special casing prominent use case to work around lack of (NOT) IN subqueries
 */
extern bool gp_adjust_selectivity_for_outerjoins;

/*
 * Target density for hash-node (HJ).
 */
extern int gp_hashjoin_tuples_per_bucket;
extern int gp_hashagg_groups_per_bucket;

/*
 * Damping of selectivities of clauses which pertain to the same base
 * relation; compensates for undetected correlation
 */
extern bool gp_selectivity_damping_for_scans;

/*
 * Damping of selectivities of clauses in joins
 */
extern bool gp_selectivity_damping_for_joins;

/*
 * Damping factor for selecticity damping
 */
extern double gp_selectivity_damping_factor;

/*
 * Sort selectivities by significance before applying
 * damping (ON by default)
 */
extern bool gp_selectivity_damping_sigsort;


/* ----- Experimental Features ----- */

/*
 * "gp_enable_agg_distinct"
 *
 * May Greenplum redistribute on the argument of a lone aggregate distinct in
 * order to use 2-phase aggregation?
 *
 * The code does uses planner estimates to decide whether to use this feature,
 * when enabled.
 */
extern bool gp_enable_agg_distinct;

/*
 * "gp_enable_agg_distinct_pruning"
 *
 * May Greenplum use grouping in the first phases of 3-phase aggregation to
 * prune values from DISTINCT-qualified aggregate function arguments?
 *
 * The code uses planner estimates to decide whether to use this feature,
 * when enabled.  See, however, gp_eager_dqa_pruning.
 */
extern bool gp_enable_dqa_pruning;

/*
 * "gp_eager_agg_distinct_pruning"
 *
 * Should Greenplum bias planner estimates so as to favor the use of grouping
 * in the first phases of 3-phase aggregation to prune values from DISTINCT-
 * qualified aggregate function arguments?
 *
 * Note that this has effect only when gp_enable_dqa_pruning it true.  It
 * provided to facilitate testing and is not a tuning parameter.
 */
extern bool gp_eager_dqa_pruning;

/*
 * "gp_eager_one_phase_agg"
 *
 * Should Greenplum bias planner estimates so as to favor the use of one
 * phase aggregation?
 *
 * It is provided to facilitate testing and is not a tuning parameter.
 */
extern bool gp_eager_one_phase_agg;

/*
 * "gp_eager_two_phase_agg"
 *
 * Should Greenplum bias planner estimates so as to favor the use of two
 * phase aggregation?
 *
 * It is provided to facilitate testing and is not a tuning parameter.
 */
extern bool gp_eager_two_phase_agg;

/*
 * "gp_enable_groupext_distinct_pruning"
 *
 * Should Greenplum bias planner estimates so as to favor the use of
 * grouping in the first phases of 3-phase aggregation to prune values
 * from DISTINCT-qualified aggregate function arguments on a grouping
 * extension query?
 */
extern bool gp_enable_groupext_distinct_pruning;

/*
 * "gp_enable_groupext_distinct_gather"
 *
 * Should Greenplum bias planner estimates so as to favor the use of
 * gathering motion to gather the data into a single node to compute
 * DISTINCT-qualified aggregates on a grouping extension query?
 */
extern bool gp_enable_groupext_distinct_gather;

/*
 * "gp_distinct_grouping_sets_threshold"
 *
 * The planner will treat gp_enable_groupext_distinct_pruning as 'off'
 * when the number of grouping sets that have been rewritten based
 * on the multi-phrase aggregation exceeds the threshold value here divided by
 * the number of distinct-qualified aggregates.
 */
extern int gp_distinct_grouping_sets_threshold;

/* May Greenplum apply Unique operator (and possibly a Sort) in parallel prior
 * to the collocation motion for a Unique operator?  The idea is to reduce
 * the number of rows moving over the interconnect.
 *
 * The code uses planner estimates for this.  If enabled, the tactic is used
 * only if it is estimated to be cheaper that a 1-phase approach.  However,
 * see gp_eqger_preunique.
 */
extern bool gp_enable_preunique;

/* If gp_enable_preunique is true, then  apply the associated optimzation
 * in an "eager" fashion.  In effect, this setting overrides the cost-
 * based decision whether to use a 2-phase approach to duplicate removal.
 */
extern bool gp_eager_preunique;

/* May Greenplum dump statistics for all segments as a huge ugly string
 * during EXPLAIN ANALYZE?
 *
 */
extern bool gp_enable_explain_allstat;

/* May Greenplum restrict ORDER BY sorts to the first N rows if the ORDER BY
 * is wrapped by a LIMIT clause (where N=OFFSET+LIMIT)?
 *
 * The code does not currently use planner estimates for this.  If enabled,
 * the tactic is used whenever possible.
 */
extern bool gp_enable_sort_limit;

/* May Greenplum discard duplicate rows in sort if it is is wrapped by a
 * DISTINCT clause (unique aggregation operator)?
 *
 * The code does not currently use planner estimates for this.  If enabled,
 * the tactic is used whenever possible.
 */
extern bool gp_enable_sort_distinct;

/* Greenplum MK Sort */
extern bool gp_enable_mk_sort;
extern bool gp_enable_motion_mk_sort;

#ifdef USE_ASSERT_CHECKING
extern bool gp_mk_sort_check;
#endif

extern bool trace_sort;

/* Generic Greenplum sort flag for testing.
 *
 *
 */
extern int gp_sort_flags;

/* If Greenplum is discarding duplicate rows in sort, switch back to
 * standard sort if the number of distinct values exceeds max_distinct.
 * (If the number of distinct values is too large the behavior of the
 * insertion sort is inferior to the heapsort)
 */
extern int gp_sort_max_distinct;

/**
 * Enable dynamic pruning of partitions based on join condition.
 */
extern bool gp_dynamic_partition_pruning;

/* Sharing of plan fragments for common table expressions */
extern bool gp_cte_sharing;
/* Enable RECURSIVE clauses in common table expressions */
extern bool gp_recursive_cte;

/* Priority for the segworkers relative to the postmaster's priority */
extern int gp_segworker_relative_priority;

/*  Max size of dispatched plans; 0 if no limit */
extern int gp_max_plan_size;

/* If we use two stage hashagg, we can stream the bottom half */
extern bool gp_hashagg_streambottom;

/* The default number of batches to use when the hybrid hashed aggregation
 * algorithm (re-)spills in-memory groups to disk.
 */
extern int gp_hashagg_default_nbatches;

/* Get statistics for partitioned parent from a child */
extern bool 	gp_statistics_pullup_from_child_partition;

/* Extract numdistinct from foreign key relationship */
extern bool		gp_statistics_use_fkeys;

/* Analyze tools */
extern int gp_motion_slice_noop;

/* Disable setting of hint-bits while reading db pages */
extern bool gp_disable_tuple_hints;

/* Enable gpmon */
extern bool gp_enable_gpperfmon;
extern int gp_gpperfmon_send_interval;
extern bool gp_enable_query_metrics;
extern int gp_instrument_shmem_size;

extern bool dml_ignore_target_partition_check;

/* gpmon alert level, control log alert level used by gpperfmon */
typedef enum 
{
	GPPERFMON_LOG_ALERT_LEVEL_NONE,
	GPPERFMON_LOG_ALERT_LEVEL_WARNING,
	GPPERFMON_LOG_ALERT_LEVEL_ERROR,
	GPPERFMON_LOG_ALERT_LEVEL_FATAL,
	GPPERFMON_LOG_ALERT_LEVEL_PANIC
} GpperfmonLogAlertLevel;
extern int gpperfmon_log_alert_level;


extern int gp_workfile_limit_per_segment;
extern int gp_workfile_limit_per_query;
extern int gp_workfile_limit_files_per_query;
extern int gp_workfile_caching_loglevel;
extern int gp_sessionstate_loglevel;
extern int gp_workfile_bytes_to_checksum;

extern bool coredump_on_memerror;

/*
 * Autostats feature, whether or not to to automatically run ANALYZE after 
 * insert/delete/update/ctas or after ctas/copy/insert in case the target
 * table has no statistics
 */
typedef enum
{
	GP_AUTOSTATS_NONE = 0,			/* Autostats is switched off */
	GP_AUTOSTATS_ON_CHANGE,			/* Autostats is enabled on change
									 * (insert/delete/update/ctas) */
	GP_AUTOSTATS_ON_NO_STATS,		/* Autostats is enabled on ctas or copy or
									 * insert if no stats are present */
} GpAutoStatsModeValue;

extern int	gp_autostats_mode;
extern int	gp_autostats_mode_in_functions;
extern int	gp_autostats_on_change_threshold;
extern bool	log_autostats;


/* --------------------------------------------------------------------------------------------------
 * Server debugging
 */

/*
 * gp_debug_linger (integer)
 *
 * Upon an error with severity FATAL and error code ERRCODE_INTERNAL_ERROR,
 * errfinish() will sleep() for the specified number of seconds before
 * termination, to let the user attach a debugger.
 */
extern int  gp_debug_linger;

/* --------------------------------------------------------------------------------------------------
 * Non-GUC globals
 */

#define UNSET_SLICE_ID -1
extern int	currentSliceId;

extern int cdb_total_plans;
/* Enable ading the cost for walking the chain in the hash join. */
extern bool gp_cost_hashjoin_chainwalk;

extern int cdb_total_slices;
extern int cdb_max_slices;

typedef struct GpId
{
	int32		dbid;			/* the dbid of this database */
	int32		segindex;		/* content indicator: -1 for entry database,
								 * 0, ..., n-1 for segment database *
								 * a primary and its mirror have the same segIndex */
} GpId;

/* --------------------------------------------------------------------------------------------------
 * Global variable declaration for the data for the single row of gp_id table
 */
extern GpId GpIdentity;
extern int get_dbid_string_length(void);
#define UNINITIALIZED_GP_IDENTITY_VALUE (-10000)
#define IS_QUERY_DISPATCHER() (GpIdentity.segindex == MASTER_CONTENT_ID)

/* Stores the listener port that this process uses to listen for incoming
 * Interconnect connections from other Motion nodes.
 */
extern uint32 Gp_listener_port;

/*
 * Thread-safe routine to write to the log
 */
extern void write_log(const char *fmt,...) __attribute__((format(printf, 1, 2)));


extern void increment_command_count(void);

/* default to RANDOM distribution for CREATE TABLE without DISTRIBUTED BY */
extern bool gp_create_table_random_default_distribution;

/* Functions in guc_gp.c to lookup values in enum GUCs */
extern GpperfmonLogAlertLevel lookup_loglevel_by_name(const char *name);
extern const char * lookup_autostats_mode_by_value(GpAutoStatsModeValue val);

#endif   /* CDBVARS_H */
