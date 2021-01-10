/*
 *	pg_upgrade.c
 *
 *	main source file
 *
 *	Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *	Portions Copyright (c) 2016-Present, Pivotal Software Inc
 *	contrib/pg_upgrade/pg_upgrade.c
 */

/*
 *	To simplify the upgrade process, we force certain system values to be
 *	identical between old and new clusters:
 *
 *	We control all assignments of pg_class.oid (and relfilenode) so toast
 *	oids are the same between old and new clusters.  This is important
 *	because toast oids are stored as toast pointers in user tables.
 *
 *	While pg_class.oid and pg_class.relfilenode are initially the same
 *	in a cluster, they can diverge due to CLUSTER, REINDEX, or VACUUM
 *	FULL.  In the new cluster, pg_class.oid and pg_class.relfilenode will
 *	be the same and will match the old pg_class.oid value.  Because of
 *	this, old/new pg_class.relfilenode values will not match if CLUSTER,
 *	REINDEX, or VACUUM FULL have been performed in the old cluster.
 *
 *	We control all assignments of pg_type.oid because these oids are stored
 *	in user composite type values.
 *
 *	We control all assignments of pg_enum.oid because these oids are stored
 *	in user tables as enum values.
 *
 *	We control all assignments of pg_authid.oid because these oids are stored
 *	in pg_largeobject_metadata.
 */



#include "postgres_fe.h"

#include "pg_upgrade.h"

#ifdef HAVE_LANGINFO_H
#include <langinfo.h>
#endif

static void prepare_new_cluster(void);
static void prepare_new_databases(void);
static void create_new_objects(void);
static void copy_clog_xlog_xid(void);
static void set_frozenxids(bool minmxid_only);
static void freeze_master_data(void);
static void reset_system_identifier(void);
static void setup(char *argv0, bool *live_check);
static void cleanup(void);
static void	get_restricted_token(const char *progname);

static void copy_subdir_files(char *subdir);

#ifdef WIN32
static int	CreateRestrictedProcess(char *cmd, PROCESS_INFORMATION *processInfo, const char *progname);
#endif

ClusterInfo old_cluster,
			new_cluster;
OSInfo		os_info;

char	   *output_files[] = {
	SERVER_LOG_FILE,
#ifdef WIN32
	/* unique file for pg_ctl start */
	SERVER_START_LOG_FILE,
#endif
	UTILITY_LOG_FILE,
	INTERNAL_LOG_FILE,
	NULL
};

#ifdef WIN32
static char *restrict_env;
#endif

/* This is the database used by pg_dumpall to restore global tables */
#define GLOBAL_DUMP_DB	"postgres"

ClusterInfo old_cluster,
			new_cluster;
OSInfo		os_info;

int
main(int argc, char **argv)
{
	char	   *sequence_script_file_name = NULL;
	char	   *analyze_script_file_name = NULL;
	char	   *deletion_script_file_name = NULL;
	bool		live_check = false;

	/* Ensure that all files created by pg_upgrade are non-world-readable */
	umask(S_IRWXG | S_IRWXO);

	parseCommandLine(argc, argv);

	get_restricted_token(os_info.progname);

	adjust_data_dir(&old_cluster);
	adjust_data_dir(&new_cluster);

	setup(argv[0], &live_check);

	report_progress(NULL, CHECK, "Checking cluster compatibility");
	output_check_banner(live_check);

	check_cluster_versions();

	get_sock_dir(&old_cluster, live_check);
	get_sock_dir(&new_cluster, false);

	check_cluster_compatibility(live_check);

	check_and_dump_old_cluster(live_check, &sequence_script_file_name);


	/* -- NEW -- */
	start_postmaster(&new_cluster, true);

	check_new_cluster();
	report_clusters_compatible();

	pg_log(PG_REPORT, "\nPerforming Upgrade\n");
	pg_log(PG_REPORT, "------------------\n");

	prepare_new_cluster();

	stop_postmaster(false);

	/*
	 * Destructive Changes to New Cluster
	 */

	copy_clog_xlog_xid();

	/*
	 * In upgrading from GPDB4, copy the pg_distributedlog over in vanilla.
	 * The assumption that this works needs to be verified
	 */
	copy_subdir_files("pg_distributedlog");

	/* New now using xids of the old system */

	/* -- NEW -- */
	start_postmaster(&new_cluster, true);

	if (user_opts.segment_mode == DISPATCHER)
	{
		prepare_new_databases();

		create_new_objects();
	}

	/*
	 * In a segment, the data directory already contains all the objects,
	 * because the segment is initialized by taking a physical copy of the
	 * upgraded QD data directory. The auxiliary AO tables - containing
	 * information about the segment files, are different in each server,
	 * however. So we still need to restore those separately on each
	 * server.
	 */
	restore_aosegment_tables();

	if (user_opts.segment_mode == DISPATCHER)
	{
		/* freeze master data *right before* stopping */
		freeze_master_data();
	}

	stop_postmaster(false);

	/*
	 * Most failures happen in create_new_objects(), which has completed at
	 * this point.  We do this here because it is just before linking, which
	 * will link the old and new cluster data files, preventing the old
	 * cluster from being safely started once the new cluster is started.
	 */
	if (user_opts.transfer_mode == TRANSFER_MODE_LINK)
		disable_old_cluster();

	transfer_all_new_tablespaces(&old_cluster.dbarr, &new_cluster.dbarr,
								 old_cluster.pgdata, new_cluster.pgdata);

	/*
	 * Assuming OIDs are only used in system tables, there is no need to
	 * restore the OID counter because we have not transferred any OIDs from
	 * the old system, but we do it anyway just in case.  We do it late here
	 * because there is no need to have the schema load use new oids.
	 */
	prep_status("Setting next OID for new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true,
			  "\"%s/pg_resetxlog\" --binary-upgrade -o %u \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtoid,
			  new_cluster.pgdata);
	check_ok();

	/* For non-master segments, uniquify the system identifier. */
	if (user_opts.segment_mode != DISPATCHER)
		reset_system_identifier();

	prep_status("Sync data directory to disk");
	exec_prog(UTILITY_LOG_FILE, NULL, true,
			  "\"%s/initdb\" --sync-only \"%s\"", new_cluster.bindir,
			  new_cluster.pgdata);
	check_ok();

	create_script_for_cluster_analyze(&analyze_script_file_name);
	create_script_for_old_cluster_deletion(&deletion_script_file_name);

	issue_warnings_and_set_wal_level(sequence_script_file_name);

	pg_log(PG_REPORT, "\nUpgrade Complete\n");
	pg_log(PG_REPORT, "----------------\n");

	report_progress(NULL, DONE, "Upgrade complete");
	close_progress();

	output_completion_banner(analyze_script_file_name,
							 deletion_script_file_name);

	pg_free(analyze_script_file_name);
	pg_free(deletion_script_file_name);
	pg_free(sequence_script_file_name);

	cleanup();

	return 0;
}

#ifdef WIN32
typedef BOOL(WINAPI * __CreateRestrictedToken) (HANDLE, DWORD, DWORD, PSID_AND_ATTRIBUTES, DWORD, PLUID_AND_ATTRIBUTES, DWORD, PSID_AND_ATTRIBUTES, PHANDLE);

/* Windows API define missing from some versions of MingW headers */
#ifndef  DISABLE_MAX_PRIVILEGE
#define DISABLE_MAX_PRIVILEGE	0x1
#endif

/*
* Create a restricted token and execute the specified process with it.
*
* Returns 0 on failure, non-zero on success, same as CreateProcess().
*
* On NT4, or any other system not containing the required functions, will
* NOT execute anything.
*/
static int
CreateRestrictedProcess(char *cmd, PROCESS_INFORMATION *processInfo, const char *progname)
{
	BOOL		b;
	STARTUPINFO si;
	HANDLE		origToken;
	HANDLE		restrictedToken;
	SID_IDENTIFIER_AUTHORITY NtAuthority = { SECURITY_NT_AUTHORITY };
	SID_AND_ATTRIBUTES dropSids[2];
	__CreateRestrictedToken _CreateRestrictedToken = NULL;
	HANDLE		Advapi32Handle;

	ZeroMemory(&si, sizeof(si));
	si.cb = sizeof(si);

	Advapi32Handle = LoadLibrary("ADVAPI32.DLL");
	if (Advapi32Handle != NULL)
	{
		_CreateRestrictedToken = (__CreateRestrictedToken)GetProcAddress(Advapi32Handle, "CreateRestrictedToken");
	}

	if (_CreateRestrictedToken == NULL)
	{
		fprintf(stderr, _("%s: WARNING: cannot create restricted tokens on this platform\n"), progname);
		if (Advapi32Handle != NULL)
			FreeLibrary(Advapi32Handle);
		return 0;
	}

	/* Open the current token to use as a base for the restricted one */
	if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ALL_ACCESS, &origToken))
	{
		fprintf(stderr, _("%s: could not open process token: error code %lu\n"), progname, GetLastError());
		return 0;
	}

	/* Allocate list of SIDs to remove */
	ZeroMemory(&dropSids, sizeof(dropSids));
	if (!AllocateAndInitializeSid(&NtAuthority, 2,
		SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_ADMINS, 0, 0, 0, 0, 0,
		0, &dropSids[0].Sid) ||
		!AllocateAndInitializeSid(&NtAuthority, 2,
		SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_POWER_USERS, 0, 0, 0, 0, 0,
		0, &dropSids[1].Sid))
	{
		fprintf(stderr, _("%s: could not allocate SIDs: error code %lu\n"), progname, GetLastError());
		return 0;
	}

	b = _CreateRestrictedToken(origToken,
						DISABLE_MAX_PRIVILEGE,
						sizeof(dropSids) / sizeof(dropSids[0]),
						dropSids,
						0, NULL,
						0, NULL,
						&restrictedToken);

	FreeSid(dropSids[1].Sid);
	FreeSid(dropSids[0].Sid);
	CloseHandle(origToken);
	FreeLibrary(Advapi32Handle);

	if (!b)
	{
		fprintf(stderr, _("%s: could not create restricted token: error code %lu\n"), progname, GetLastError());
		return 0;
	}

#ifndef __CYGWIN__
	AddUserToTokenDacl(restrictedToken);
#endif

	if (!CreateProcessAsUser(restrictedToken,
							NULL,
							cmd,
							NULL,
							NULL,
							TRUE,
							CREATE_SUSPENDED,
							NULL,
							NULL,
							&si,
							processInfo))

	{
		fprintf(stderr, _("%s: could not start process for command \"%s\": error code %lu\n"), progname, cmd, GetLastError());
		return 0;
	}

	return ResumeThread(processInfo->hThread);
}
#endif

static void
get_restricted_token(const char *progname)
{
#ifdef WIN32

	/*
	* Before we execute another program, make sure that we are running with a
	* restricted token. If not, re-execute ourselves with one.
	*/

	if ((restrict_env = getenv("PG_RESTRICT_EXEC")) == NULL
		|| strcmp(restrict_env, "1") != 0)
	{
		PROCESS_INFORMATION pi;
		char	   *cmdline;

		ZeroMemory(&pi, sizeof(pi));

		cmdline = pg_strdup(GetCommandLine());

		putenv("PG_RESTRICT_EXEC=1");

		if (!CreateRestrictedProcess(cmdline, &pi, progname))
		{
			fprintf(stderr, _("%s: could not re-execute with restricted token: error code %lu\n"), progname, GetLastError());
		}
		else
		{
			/*
			* Successfully re-execed. Now wait for child process to capture
			* exitcode.
			*/
			DWORD		x;

			CloseHandle(pi.hThread);
			WaitForSingleObject(pi.hProcess, INFINITE);

			if (!GetExitCodeProcess(pi.hProcess, &x))
			{
				fprintf(stderr, _("%s: could not get exit code from subprocess: error code %lu\n"), progname, GetLastError());
				exit(1);
			}
			exit(x);
		}
	}
#endif
}

static void
setup(char *argv0, bool *live_check)
{
	char		exec_path[MAXPGPATH];	/* full path to my executable */

	/*
	 * make sure the user has a clean environment, otherwise, we may confuse
	 * libpq when we connect to one (or both) of the servers.
	 */
	check_pghost_envvar();

	verify_directories();

	/* no postmasters should be running, except for a live check */
	if (pid_lock_file_exists(old_cluster.pgdata))
	{
		/*
		 * If we have a postmaster.pid file, try to start the server.  If it
		 * starts, the pid file was stale, so stop the server.  If it doesn't
		 * start, assume the server is running.  If the pid file is left over
		 * from a server crash, this also allows any committed transactions
		 * stored in the WAL to be replayed so they are not lost, because WAL
		 * files are not transfered from old to new servers.  We later check
		 * for a clean shutdown.
		 */
		if (start_postmaster(&old_cluster, false))
			stop_postmaster(false);
		else
		{
			if (!user_opts.check)
				pg_fatal("There seems to be a postmaster servicing the old cluster.\n"
						 "Please shutdown that postmaster and try again.\n");
			else
				*live_check = true;
		}
	}

	/* same goes for the new postmaster */
	if (pid_lock_file_exists(new_cluster.pgdata))
	{
		if (start_postmaster(&new_cluster, false))
			stop_postmaster(false);
		else
			pg_fatal("There seems to be a postmaster servicing the new cluster.\n"
					 "Please shutdown that postmaster and try again.\n");
	}

	/* get path to pg_upgrade executable */
	if (find_my_exec(argv0, exec_path) < 0)
		pg_fatal("Could not get path name to pg_upgrade: %s\n", getErrorText());

	/* Trim off program name and keep just path */
	*last_dir_separator(exec_path) = '\0';
	canonicalize_path(exec_path);
	os_info.exec_path = pg_strdup(exec_path);
}


static void
prepare_new_cluster(void)
{
	/*
	 * It would make more sense to freeze after loading the schema, but that
	 * would cause us to lose the frozenids restored by the load. We use
	 * --analyze so autovacuum doesn't update statistics later
	 *
	 * GPDB: after we've copied the master data directory to the segments,
	 * AO tables can't be analyzed because their aoseg tuple counts don't match
	 * those on disk. We therefore skip this step for segments.
	 */
	if (user_opts.segment_mode == DISPATCHER)
	{
		prep_status("Analyzing all rows in the new cluster");
		exec_prog(UTILITY_LOG_FILE, NULL, true,
				  "PGOPTIONS='-c gp_session_role=utility' \"%s/vacuumdb\" %s --all --analyze %s",
				  new_cluster.bindir, cluster_conn_opts(&new_cluster),
				  log_opts.verbose ? "--verbose" : "");
		check_ok();
	}

	/*
	 * We do freeze after analyze so pg_statistic is also frozen. template0 is
	 * not frozen here, but data rows were frozen by initdb, and we set its
	 * datfrozenxid, relfrozenxids, and relminmxid later to match the new xid
	 * counter later.
	 */
	prep_status("Freezing all rows on the new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true,
			  "PGOPTIONS='-c gp_session_role=utility' "
			  "\"%s/vacuumdb\" %s --all --freeze %s",
			  new_cluster.bindir, cluster_conn_opts(&new_cluster),
			  log_opts.verbose ? "--verbose" : "");
	check_ok();

	get_pg_database_relfilenode(&new_cluster);
}


static void
prepare_new_databases(void)
{
	/*
	 * Before we restore anything, set frozenxids of initdb-created tables.
	 */
	set_frozenxids(false);

	/*
	 * Now restore global objects (roles and tablespaces).
	 */
	prep_status("Restoring global objects in the new cluster");

	/*
	 * Install support functions in the global-object restore database to
	 * preserve pg_authid.oid.  pg_dumpall uses 'template0' as its template
	 * database so objects we add into 'template1' are not propogated.  They
	 * are removed on pg_upgrade exit.
	 */
	install_support_functions_in_new_db("template1");

	/*
	 * We have to create the databases first so we can install support
	 * functions in all the other databases.  Ideally we could create the
	 * support functions in template1 but pg_dumpall creates database using
	 * the template0 template.
	 */
	exec_prog(UTILITY_LOG_FILE, NULL, true,
			  "PGOPTIONS='-c gp_session_role=utility' "
			  "\"%s/psql\" " EXEC_PSQL_ARGS " %s -f \"%s\"",
			  new_cluster.bindir, cluster_conn_opts(&new_cluster),
			  GLOBALS_DUMP_FILE);
	check_ok();

	/* we load this to get a current list of databases */
	get_db_and_rel_infos(&new_cluster);
}


static void
create_new_objects(void)
{
	int			dbnum;

	prep_status("Adding support functions to new cluster");

	/*
	 * Technically, we only need to install these support functions in new
	 * databases that also exist in the old cluster, but for completeness we
	 * process all new databases.
	 */
	for (dbnum = 0; dbnum < new_cluster.dbarr.ndbs; dbnum++)
	{
		DbInfo	   *new_db = &new_cluster.dbarr.dbs[dbnum];

		/* skip db we already installed */
		if (strcmp(new_db->db_name, "template1") != 0)
			install_support_functions_in_new_db(new_db->db_name);
	}
	check_ok();

	prep_status("Restoring database schemas in the new cluster\n");

	for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		char		sql_file_name[MAXPGPATH],
					log_file_name[MAXPGPATH];
		DbInfo	   *old_db = &old_cluster.dbarr.dbs[dbnum];
		PQExpBufferData connstr,
					escaped_connstr;

		initPQExpBuffer(&connstr);
		appendPQExpBuffer(&connstr, "dbname=");
		appendConnStrVal(&connstr, old_db->db_name);
		initPQExpBuffer(&escaped_connstr);
		appendShellString(&escaped_connstr, connstr.data);
		termPQExpBuffer(&connstr);

		pg_log(PG_STATUS, "%s", old_db->db_name);
		snprintf(sql_file_name, sizeof(sql_file_name), DB_DUMP_FILE_MASK, old_db->db_oid);
		snprintf(log_file_name, sizeof(log_file_name), DB_DUMP_LOG_FILE_MASK, old_db->db_oid);

		/*
		 * pg_dump only produces its output at the end, so there is little
		 * parallelism if using the pipe.
		 */
		parallel_exec_prog(log_file_name,
						   NULL,
		 "PGOPTIONS='-c gp_session_role=utility' "
		 "\"%s/pg_restore\" %s --exit-on-error --binary-upgrade --verbose --dbname %s \"%s\"",
						   new_cluster.bindir,
						   cluster_conn_opts(&new_cluster),
						   escaped_connstr.data,
						   sql_file_name);

		termPQExpBuffer(&escaped_connstr);
	}

	/* reap all children */
	while (reap_child(true) == true)
		;

	end_progress_output();
	check_ok();

	/*
	 * We don't have minmxids for databases or relations in pre-9.3
	 * clusters, so set those after we have restored the schema.
	 */
	if (GET_MAJOR_VERSION(old_cluster.major_version) < 903)
		set_frozenxids(true);

	/* regenerate now that we have objects in the databases */
	get_db_and_rel_infos(&new_cluster);

	uninstall_support_functions_from_new_cluster();

	/*
	 * If we're upgrading from GPDB4, mark all indexes as invalid.
	 * The page format is incompatible, and while convert heap
	 * and AO tables automatically, we don't have similar code for
	 * indexes. Also, the heap conversion relocates tuples, so
	 * any indexes on heaps would need to be rebuilt for that
	 * reason, anyway.
	 */
	if (GET_MAJOR_VERSION(old_cluster.major_version) == 802)
		new_gpdb5_0_invalidate_indexes();
	else
	{
		/* TODO: Bitmap indexes are not supported, so mark them as invalid. */
		new_gpdb_invalidate_bitmap_indexes();
	}
}


/*
 * Greenplum upgrade involves copying the MASTER_DATA_DIRECTORY to
 * each primary segment. We need to freeze the master data *after* the master
 * schema has been restored to allow the data to be visible on the segments.
 * All databases need to be frozen including those where datallowconn is false.
 *
 * Note: No further updates should occur after freezing the master data
 * directory.
 */
static void
freeze_master_data(void)
{
	PGconn 			*conn;
	PGconn			*conn_template1;
	PGresult		*dbres;
	PGresult		*txid_res;
	PGresult		*dbage_res;
	int				dbnum;
	int				ntups;
	int				i_datallowconn;
	int				i_datname;
	TransactionId	txid_before;
	TransactionId	txid_after;
	int32 			txns_from_freeze;

	prep_status("Freezing all rows in new master after pg_restore");

	/* Temporarily allow connections to all databases for vacuum freeze */
	conn_template1 = connectToServer(&new_cluster, "template1");

	PQclear(executeQueryOrDie(conn_template1, "set allow_system_table_mods=true"));

	dbres = executeQueryOrDie(conn_template1,
							  "SELECT datname, datallowconn "
							  "FROM	pg_catalog.pg_database");

	i_datname = PQfnumber(dbres, "datname");
	i_datallowconn = PQfnumber(dbres, "datallowconn");

	ntups = PQntuples(dbres);
	for (dbnum = 0; dbnum < ntups; dbnum++)
	{
		char *datallowconn = PQgetvalue(dbres, dbnum, i_datallowconn);
		char *datname = PQgetvalue(dbres, dbnum, i_datname);
		char *escaped_datname = pg_malloc(strlen(datname) * 2 + 1);
		PQescapeStringConn(conn_template1, escaped_datname, datname, strlen(datname), NULL);

		/* For vacuum freeze, temporarily set datallowconn to true. */
		if (strcmp(datallowconn, "f") == 0)
			PQclear(executeQueryOrDie(conn_template1,
									  "UPDATE pg_catalog.pg_database "
									  "SET datallowconn = true "
									  "WHERE datname = '%s'", escaped_datname));

		conn = connectToServer(&new_cluster, datname);

		/* Obtain txid_current before vacuum freeze. */
		txid_res = executeQueryOrDie(conn, "SELECT txid_current()");
		txid_before = str2uint(PQgetvalue(txid_res, 0, PQfnumber(txid_res, "txid_current")));
		PQclear(txid_res);

		PQclear(executeQueryOrDie(conn, "VACUUM FREEZE"));

		/*
		 * Obtain txid_current and age after vacuum freeze.
		 *
		 * Note: It is important that this occurs before any other transactions
		 * are executed so verification succeeds.
		 */
		dbage_res = executeQueryOrDie(conn,
									  "SELECT txid_current(), age(datfrozenxid) "
									  "FROM pg_catalog.pg_database "
									  "WHERE datname = '%s'", escaped_datname);
		txid_after = str2uint(PQgetvalue(dbage_res, 0, PQfnumber(dbage_res, "txid_current")));
		uint datfrozenxid_age = str2uint(PQgetvalue(dbage_res, 0, PQfnumber(dbage_res, "age")));
		PQclear(dbage_res);

		/*
		 * Verify that the database was frozen by checking that the database age
		 * is less than the number of transactions taken by "VACUUM FREEZE".
		 * This implies that all transaction ids that are older than the
		 * "VACUUM FREEZE" transaction are frozen, and that the oldest
		 * transaction in the database is newer than the "VACUUM FREEZE"
		 * transaction.
		 */
		txns_from_freeze = txid_after - txid_before;
		if (txns_from_freeze < 0)
		{
			/* Needed if a wrap around occurs between txid after and before. */
			txns_from_freeze = INT32_MAX - Abs(txns_from_freeze);
		}

		/* Reset datallowconn flag before possibly raising an error. */
		if (strcmp(datallowconn, "f") == 0)
			PQclear(executeQueryOrDie(conn_template1,
									  "UPDATE pg_catalog.pg_database "
									  "SET datallowconn = false "
									  "WHERE datname = '%s'", escaped_datname));

		pg_free(escaped_datname);
		PQfinish(conn);

		if (datfrozenxid_age > txns_from_freeze)
		{
			PQfinish(conn_template1);
			pg_fatal("Error database '%s' was not properly frozen. Database age of %d is older than %d.\n",
					 datname, datfrozenxid_age, txns_from_freeze);
		}
	}

	/* Freeze the tuples updated from resetting datallowconn flag */
	PQclear(executeQueryOrDie(conn_template1, "VACUUM FREEZE pg_catalog.pg_database"));

	PQclear(dbres);

	PQfinish(conn_template1);

	check_ok();
}

/*
 * Delete the given subdirectory contents from the new cluster
 */
static void
remove_new_subdir(char *subdir, bool rmtopdir)
{
	char		new_path[MAXPGPATH];

	prep_status("Deleting files from new %s", subdir);

	snprintf(new_path, sizeof(new_path), "%s/%s", new_cluster.pgdata, subdir);
	if (!rmtree(new_path, rmtopdir))
		pg_fatal("could not delete directory \"%s\"\n", new_path);

	check_ok();
}

/*
 * Copy the files from the old cluster into it
 */
static void
copy_subdir_files(char *subdir)
{
	char		old_path[MAXPGPATH];
	char		new_path[MAXPGPATH];

	remove_new_subdir(subdir, true);

	snprintf(old_path, sizeof(old_path), "%s/%s", old_cluster.pgdata, subdir);
	snprintf(new_path, sizeof(new_path), "%s/%s", new_cluster.pgdata, subdir);

	prep_status("Copying old %s to new server", subdir);

	exec_prog(UTILITY_LOG_FILE, NULL, true,
#ifndef WIN32
			  "cp -Rf \"%s\" \"%s\"",
#else
	/* flags: everything, no confirm, quiet, overwrite read-only */
			  "xcopy /e /y /q /r \"%s\" \"%s\\\"",
#endif
			  old_path, new_path);

	check_ok();
}

static void
copy_clog_xlog_xid(void)
{
	/* copy old commit logs to new data dir */
	copy_subdir_files("pg_clog");

	/* set the next transaction id and epoch of the new cluster */
	prep_status("Setting next transaction ID and epoch for new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true,
			  "\"%s/pg_resetxlog\" --binary-upgrade -f -x %u \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtxid,
			  new_cluster.pgdata);
	exec_prog(UTILITY_LOG_FILE, NULL, true,
			  "\"%s/pg_resetxlog\" --binary-upgrade -f -e %u \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtepoch,
			  new_cluster.pgdata);
	check_ok();

	/*
	 * If the old server is before the MULTIXACT_FORMATCHANGE_CAT_VER change
	 * (see pg_upgrade.h) and the new server is after, then we don't copy
	 * pg_multixact files, but we need to reset pg_control so that the new
	 * server doesn't attempt to read multis older than the cutoff value.
	 */
	if (old_cluster.controldata.cat_ver >= MULTIXACT_FORMATCHANGE_CAT_VER &&
		new_cluster.controldata.cat_ver >= MULTIXACT_FORMATCHANGE_CAT_VER)
	{
		copy_subdir_files("pg_multixact/offsets");
		copy_subdir_files("pg_multixact/members");

		prep_status("Setting next multixact ID and offset for new cluster");

		/*
		 * we preserve all files and contents, so we must preserve both "next"
		 * counters here and the oldest multi present on system.
		 */
		exec_prog(UTILITY_LOG_FILE, NULL, true,
				  "\"%s/pg_resetxlog\" --binary-upgrade -O %u -m %u,%u \"%s\"",
				  new_cluster.bindir,
				  old_cluster.controldata.chkpnt_nxtmxoff,
				  old_cluster.controldata.chkpnt_nxtmulti,
				  old_cluster.controldata.chkpnt_oldstMulti,
				  new_cluster.pgdata);
		check_ok();
	}
	else if (new_cluster.controldata.cat_ver >= MULTIXACT_FORMATCHANGE_CAT_VER)
	{
		/*
		 * Remove offsets/0000 file created by initdb that no longer matches
		 * the new multi-xid value.  "members" starts at zero so no need to
		 * remove it.
		 */
		remove_new_subdir("pg_multixact/offsets", false);

		prep_status("Setting oldest multixact ID on new cluster");

		/*
		 * We don't preserve files in this case, but it's important that the
		 * oldest multi is set to the latest value used by the old system, so
		 * that multixact.c returns the empty set for multis that might be
		 * present on disk.  We set next multi to the value following that; it
		 * might end up wrapped around (i.e. 0) if the old cluster had
		 * next=MaxMultiXactId, but multixact.c can cope with that just fine.
		 */
		exec_prog(UTILITY_LOG_FILE, NULL, true,
				  "\"%s/pg_resetxlog\" --binary-upgrade -m %u,%u \"%s\"",
				  new_cluster.bindir,
				  old_cluster.controldata.chkpnt_nxtmulti + 1,
				  old_cluster.controldata.chkpnt_nxtmulti,
				  new_cluster.pgdata);
		check_ok();
	}

	/* now reset the wal archives in the new cluster */
	prep_status("Resetting WAL archives");
	exec_prog(UTILITY_LOG_FILE, NULL, true,
			  /* use timeline 1 to match controldata and no WAL history file */
			  "\"%s/pg_resetxlog\" --binary-upgrade -l 00000001%s \"%s\"", new_cluster.bindir,
			  old_cluster.controldata.nextxlogfile + 8,
			  new_cluster.pgdata);
	check_ok();
}


/*
 *	set_frozenxids()
 *
 * This is called on the new cluster before we restore anything, with
 * minmxid_only = false.  Its purpose is to ensure that all initdb-created
 * vacuumable tables have relfrozenxid/relminmxid matching the old cluster's
 * xid/mxid counters.  We also initialize the datfrozenxid/datminmxid of the
 * built-in databases to match.
 *
 * As we create user tables later, their relfrozenxid/relminmxid fields will
 * be restored properly by the binary-upgrade restore script.  Likewise for
 * user-database datfrozenxid/datminmxid.  However, if we're upgrading from a
 * pre-9.3 database, which does not store per-table or per-DB minmxid, then
 * the relminmxid/datminmxid values filled in by the restore script will just
 * be zeroes.
 *
 * Hence, with a pre-9.3 source database, a second call occurs after
 * everything is restored, with minmxid_only = true.  This pass will
 * initialize all tables and databases, both those made by initdb and user
 * objects, with the desired minmxid value.  frozenxid values are left alone.
 */
static void
set_frozenxids(bool minmxid_only)
{
	int			dbnum;
	PGconn	   *conn,
			   *conn_template1;
	PGresult   *dbres;
	int			ntups;
	int			i_datname;
	int			i_datallowconn;

	if (!minmxid_only)
		prep_status("Setting frozenxid and minmxid counters in new cluster");
	else
		prep_status("Setting minmxid counter in new cluster");

	conn_template1 = connectToServer(&new_cluster, "template1");

	/*
	 * GPDB doesn't allow hacking the catalogs without setting
	 * allow_system_table_mods first.
	 */
	PQclear(executeQueryOrDie(conn_template1,
							  "set allow_system_table_mods=true"));

	if (!minmxid_only)
		/* set pg_database.datfrozenxid */
		PQclear(executeQueryOrDie(conn_template1,
								  "UPDATE pg_catalog.pg_database "
								  "SET	datfrozenxid = '%u'",
								  old_cluster.controldata.chkpnt_nxtxid));

	/* set pg_database.datminmxid */
	PQclear(executeQueryOrDie(conn_template1,
							  "UPDATE pg_catalog.pg_database "
							  "SET	datminmxid = '%u'",
							  old_cluster.controldata.chkpnt_nxtmulti));

	/* get database names */
	dbres = executeQueryOrDie(conn_template1,
							  "SELECT	datname, datallowconn "
							  "FROM	pg_catalog.pg_database");

	i_datname = PQfnumber(dbres, "datname");
	i_datallowconn = PQfnumber(dbres, "datallowconn");

	ntups = PQntuples(dbres);
	for (dbnum = 0; dbnum < ntups; dbnum++)
	{

		char	   *datname = PQgetvalue(dbres, dbnum, i_datname);
		char	   *escaped_datname = NULL;
		char	   *datallowconn = PQgetvalue(dbres, dbnum, i_datallowconn);


		/*
		 * We must update databases where datallowconn = false, e.g.
		 * template0, because autovacuum increments their datfrozenxids,
		 * relfrozenxids, and relminmxid  even if autovacuum is turned off,
		 * and even though all the data rows are already frozen  To enable
		 * this, we temporarily change datallowconn.
		 */
		if (strcmp(datallowconn, "f") == 0)
		{
			escaped_datname = pg_malloc(strlen(datname) * 2 + 1);
			PQescapeStringConn(conn_template1, escaped_datname, datname, strlen(datname), NULL);
			PQclear(executeQueryOrDie(conn_template1,
									  "UPDATE pg_catalog.pg_database "
									  "SET	datallowconn = true "
									  "WHERE datname = '%s'", escaped_datname));
		}

		conn = connectToServer(&new_cluster, datname);

		/*
		 * GPDB doesn't allow hacking the catalogs without setting
		 * allow_system_table_mods first.
		 */
		PQclear(executeQueryOrDie(conn, "set allow_system_table_mods=true"));

		/*
		 * Instead of assuming template0 will be frozen by initdb, its worth
		 * making sure we freeze it here before updating the relfrozenxid
		 * directly for the tables in pg_class and datfrozenxid for the
		 * database in pg_database. Its fast and safe worth than assuming for
		 * template0.
		 */
		if (!minmxid_only && strcmp(datallowconn, "f") == 0)
		{
			PQclear(executeQueryOrDie(conn, "VACUUM FREEZE"));
		}

		if (!minmxid_only)
			/* set pg_class.relfrozenxid */
			PQclear(executeQueryOrDie(conn,
									  "UPDATE	pg_catalog.pg_class "
									  "SET	relfrozenxid = '%u' "
			/*
			 * only heap, materialized view, and TOAST are vacuumed
			 * exclude relations with external storage as well as AO and CO tables
			 *
			 * The logic here should keep consistent with function
			 * should_have_valid_relfrozenxid().
			 *
			 * Notes: if we ever backport this to Greenplum 5X, remove 'm' first
			 * and then replace 'M' with 'm', because 'm' used to be RELKIND
			 * visimap in 4.3/5X, not matview
			 */
									  "WHERE	(relkind IN ('r', 'm', 't') "
									  "AND NOT relfrozenxid = 0) "
									  "OR (relkind IN ('t', 'o', 'b', 'M'))",
									  old_cluster.controldata.chkpnt_nxtxid));

		/* set pg_class.relminmxid */
		PQclear(executeQueryOrDie(conn,
								  "UPDATE	pg_catalog.pg_class "
								  "SET	relminmxid = '%u' "
		/* only heap, materialized view, and TOAST are vacuumed */
								  "WHERE	relkind IN ('r', 'm', 't')",
								  old_cluster.controldata.chkpnt_nxtmulti));
		PQfinish(conn);

		/* Reset datallowconn flag */
		if (strcmp(datallowconn, "f") == 0)
		{
			PQclear(executeQueryOrDie(conn_template1,
									  "UPDATE pg_catalog.pg_database "
									  "SET	datallowconn = false "
									  "WHERE datname = '%s'", escaped_datname));
			pg_free(escaped_datname);
		}
	}

	PQclear(dbres);

	PQfinish(conn_template1);

	check_ok();
}

/*
 * Called for GPDB segments only -- since we have copied the master's
 * pg_control file, we need to assign a new system identifier to each segment.
 */
static void
reset_system_identifier(void)
{
	struct timeval	tv;
	uint64			sysidentifier;

	prep_status("Setting database system identifier for new cluster");

	/*
	 * Use the same initialization process as BootStrapXLOG():
	 *
	 * - 32 bits of [current timestamp] seconds
	 * - 20 bits of [current timestamp] microseconds
	 * - 12 bits of PID
	 *
	 * This doesn't guarantee uniqueness, but if it's good enough for
	 * gpinitsystem it should be good enough for us.
	 */
	gettimeofday(&tv, NULL);
	sysidentifier = ((uint64) tv.tv_sec) << 32;
	sysidentifier |= ((uint64) tv.tv_usec) << 12;
	sysidentifier |= getpid() & 0xFFF;

	exec_prog(UTILITY_LOG_FILE, NULL, true,
			  "\"%s/pg_resetxlog\" --binary-upgrade --system-identifier " UINT64_FORMAT " \"%s\"",
			  new_cluster.bindir, sysidentifier, new_cluster.pgdata);

	check_ok();
}

static void
cleanup(void)
{
	fclose(log_opts.internal);

	/* Remove dump and log files? */
	if (!log_opts.retain)
	{
		int			dbnum;
		char	  **filename;

		for (filename = output_files; *filename != NULL; filename++)
			unlink(*filename);

		/* remove dump files */
		unlink(GLOBALS_DUMP_FILE);

		if (old_cluster.dbarr.dbs)
			for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
			{
				char		sql_file_name[MAXPGPATH],
							log_file_name[MAXPGPATH];
				DbInfo	   *old_db = &old_cluster.dbarr.dbs[dbnum];

				snprintf(sql_file_name, sizeof(sql_file_name), DB_DUMP_FILE_MASK, old_db->db_oid);
				unlink(sql_file_name);

				snprintf(log_file_name, sizeof(log_file_name), DB_DUMP_LOG_FILE_MASK, old_db->db_oid);
				unlink(log_file_name);
			}
	}
}
