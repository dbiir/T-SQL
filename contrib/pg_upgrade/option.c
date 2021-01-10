/*
 *	opt.c
 *
 *	options functions
 *
 *	Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/option.c
 */

#include "postgres_fe.h"

#include "miscadmin.h"
#include "getopt_long.h"

#include "pg_upgrade.h"

#include <time.h>
#include <sys/types.h>
#ifdef WIN32
#include <io.h>
#endif


static void usage(void);
static void check_required_directory(char **dirpath,
						 const char *envVarName, bool useCwd,
						 const char *cmdLineOption, const char *description);
#define FIX_DEFAULT_READ_ONLY "-c default_transaction_read_only=false"


UserOpts	user_opts;


/*
 * parseCommandLine()
 *
 *	Parses the command line (argc, argv[]) and loads structures
 */
void
parseCommandLine(int argc, char *argv[])
{
	static struct option long_options[] = {
		{"old-datadir", required_argument, NULL, 'd'},
		{"new-datadir", required_argument, NULL, 'D'},
		{"old-bindir", required_argument, NULL, 'b'},
		{"new-bindir", required_argument, NULL, 'B'},
		{"old-options", required_argument, NULL, 'o'},
		{"new-options", required_argument, NULL, 'O'},
		{"old-port", required_argument, NULL, 'p'},
		{"new-port", required_argument, NULL, 'P'},

		{"username", required_argument, NULL, 'U'},
		{"check", no_argument, NULL, 'c'},
		{"link", no_argument, NULL, 'k'},
		{"retain", no_argument, NULL, 'r'},
		{"jobs", required_argument, NULL, 'j'},
		{"socketdir", required_argument, NULL, 's'},
		{"verbose", no_argument, NULL, 'v'},

		/* Greenplum specific parameters */
		{"mode", required_argument, NULL, 1},
		{"progress", no_argument, NULL, 2},
		{"add-checksum", no_argument, NULL, 3},
		{"remove-checksum", no_argument, NULL, 4},

		{NULL, 0, NULL, 0}
	};
	int			option;			/* Command line option */
	int			optindex = 0;	/* used by getopt_long */
	int			os_user_effective_id;
	FILE	   *fp;
	char	  **filename;
	time_t		run_time = time(NULL);

	user_opts.transfer_mode = TRANSFER_MODE_COPY;

	os_info.progname = get_progname(argv[0]);

	/* Process libpq env. variables; load values here for usage() output */
	old_cluster.port = getenv("PGPORTOLD") ? atoi(getenv("PGPORTOLD")) : DEF_PGUPORT;
	new_cluster.port = getenv("PGPORTNEW") ? atoi(getenv("PGPORTNEW")) : DEF_PGUPORT;

	os_user_effective_id = get_user_info(&os_info.user);

	user_opts.segment_mode = SEGMENT;

	/* we override just the database user name;  we got the OS id above */
	if (getenv("PGUSER"))
	{
		pg_free(os_info.user);
		/* must save value, getenv()'s pointer is not stable */
		os_info.user = pg_strdup(getenv("PGUSER"));
	}

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_upgrade (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	/* Allow help and version to be run as root, so do the test here. */
	if (os_user_effective_id == 0)
		pg_fatal("%s: cannot be run as root\n", os_info.progname);

	if ((log_opts.internal = fopen_priv(INTERNAL_LOG_FILE, "a")) == NULL)
		pg_fatal("cannot write to log file %s\n", INTERNAL_LOG_FILE);

	while ((option = getopt_long(argc, argv, "d:D:b:B:cj:ko:O:p:P:rs:U:v",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'b':
				old_cluster.bindir = pg_strdup(optarg);
				break;

			case 'B':
				new_cluster.bindir = pg_strdup(optarg);
				break;

			case 'c':
				user_opts.check = true;
				break;

			case 'd':
				old_cluster.pgdata = pg_strdup(optarg);
				break;

			case 'D':
				new_cluster.pgdata = pg_strdup(optarg);
				break;

			case 'j':
				user_opts.jobs = atoi(optarg);
				break;

			case 'k':
				user_opts.transfer_mode = TRANSFER_MODE_LINK;
				break;

			case 'o':
				old_cluster.pgopts = pg_strdup(optarg);
				break;

			case 'O':
				new_cluster.pgopts = pg_strdup(optarg);
				break;

				/*
				 * Someday, the port number option could be removed and passed
				 * using -o/-O, but that requires postmaster -C to be
				 * supported on all old/new versions.
				 */
			case 'p':
				if ((old_cluster.port = atoi(optarg)) <= 0)
				{
					pg_fatal("invalid old port number\n");
					exit(1);
				}
				break;

			case 'P':
				if ((new_cluster.port = atoi(optarg)) <= 0)
				{
					pg_fatal("invalid new port number\n");
					exit(1);
				}
				break;

			case 'r':
				log_opts.retain = true;
				break;

			case 's':
				user_opts.socketdir = pg_strdup(optarg);
				break;

			case 'U':
				pg_free(os_info.user);
				os_info.user = pg_strdup(optarg);
				os_info.user_specified = true;

				/*
				 * Push the user name into the environment so pre-9.1
				 * pg_ctl/libpq uses it.
				 */
				pg_putenv("PGUSER", os_info.user);
				break;

			case 'v':
				pg_log(PG_REPORT, "Running in verbose mode\n");
				log_opts.verbose = true;
				break;

			/*
			 * Greenplum specific parameters
			 */

			case 1:		/* --mode={dispatcher|segment} */
				if (pg_strcasecmp("dispatcher", optarg) == 0)
					user_opts.segment_mode = DISPATCHER;
				else if (pg_strcasecmp("segment", optarg) == 0)
					user_opts.segment_mode = SEGMENT;
				else
				{
					pg_log(PG_FATAL, "invalid segment configuration\n");
					exit(1);
				}

				break;

			case 2:		/* --progress */
				user_opts.progress = true;
				break;

			case 3:		/* --add-checksum */
				user_opts.checksum_mode = CHECKSUM_ADD;
				break;

			case 4:		/* --remove-checksum */
				user_opts.checksum_mode = CHECKSUM_REMOVE;
				break;

			default:
				pg_fatal("Try \"%s --help\" for more information.\n",
						 os_info.progname);
				break;
		}
	}

	/* label start of upgrade in logfiles */
	for (filename = output_files; *filename != NULL; filename++)
	{
		if ((fp = fopen_priv(*filename, "a")) == NULL)
			pg_fatal("cannot write to log file %s\n", *filename);

		/* Start with newline because we might be appending to a file. */
		fprintf(fp, "\n"
		"-----------------------------------------------------------------\n"
				"  pg_upgrade run on %s"
				"-----------------------------------------------------------------\n\n",
				ctime(&run_time));
		fclose(fp);
	}

	/* Turn off read-only mode;  add prefix to PGOPTIONS? */
	if (getenv("PGOPTIONS"))
	{
		char	   *pgoptions = psprintf("%s %s", FIX_DEFAULT_READ_ONLY,
										 getenv("PGOPTIONS"));

		pg_putenv("PGOPTIONS", pgoptions);
		pfree(pgoptions);
	}
	else
		pg_putenv("PGOPTIONS", FIX_DEFAULT_READ_ONLY);

	/* Get values from env if not already set */
	check_required_directory(&old_cluster.bindir, "PGBINOLD", false,
							 "-b", "old cluster binaries reside");
	check_required_directory(&new_cluster.bindir, "PGBINNEW", false,
							 "-B", "new cluster binaries reside");
	check_required_directory(&old_cluster.pgdata, "PGDATAOLD", false,
							 "-d", "old cluster data resides");
	check_required_directory(&new_cluster.pgdata, "PGDATANEW", false,
							 "-D", "new cluster data resides");
	check_required_directory(&user_opts.socketdir, "PGSOCKETDIR", true,
							 "-s", "sockets will be created");

	/* Ensure we are only adding checksums in copy mode */
	if (user_opts.transfer_mode != TRANSFER_MODE_COPY &&
		user_opts.checksum_mode != CHECKSUM_NONE)
		pg_log(PG_FATAL, "Adding and removing checksums only supported in copy mode.\n");
}


static void
usage(void)
{
	printf(_("pg_upgrade upgrades a Greenplum cluster to a different major version.\n\
\nUsage:\n\
  pg_upgrade [OPTION]...\n\
\n\
Options:\n\
  -b, --old-bindir=BINDIR       old cluster executable directory\n\
  -B, --new-bindir=BINDIR       new cluster executable directory\n\
  -c, --check                   check clusters only, don't change any data\n\
  -d, --old-datadir=DATADIR     old cluster data directory\n\
  -D, --new-datadir=DATADIR     new cluster data directory\n\
  -j, --jobs                    number of simultaneous processes or threads to use\n\
  -k, --link                    link instead of copying files to new cluster\n\
  -o, --old-options=OPTIONS     old cluster options to pass to the server\n\
  -O, --new-options=OPTIONS     new cluster options to pass to the server\n\
  -p, --old-port=PORT           old cluster port number (default %d)\n\
  -P, --new-port=PORT           new cluster port number (default %d)\n\
  -r, --retain                  retain SQL and log files after success\n\
  -s, --socketdir=DIR           socket directory to use (default CWD)\n\
  -U, --username=NAME           cluster superuser (default \"%s\")\n\
  -v, --verbose                 enable verbose internal logging\n\
  -V, --version                 display version information, then exit\n\
      --mode=TYPE               designate node type to upgrade, \"segment\" or \"dispatcher\" (default \"segment\")\n\
      --progress                enable progress reporting\n\
      --remove-checksum         remove data checksums when creating new cluster\n\
      --add-checksum            add data checksumming to the new cluster\n\
  -?, --help                    show this help, then exit\n\
\n\
Before running pg_upgrade you must:\n\
  create a new database cluster (using the new version of initdb)\n\
  shutdown the postmaster servicing the old cluster\n\
  shutdown the postmaster servicing the new cluster\n\
\n\
When you run pg_upgrade, you must provide the following information:\n\
  the data directory for the old cluster  (-d DATADIR)\n\
  the data directory for the new cluster  (-D DATADIR)\n\
  the \"bin\" directory for the old version (-b BINDIR)\n\
  the \"bin\" directory for the new version (-B BINDIR)\n\
\n\
For example:\n\
  pg_upgrade -d oldCluster/data -D newCluster/data -b oldCluster/bin -B newCluster/bin\n\
or\n"), old_cluster.port, new_cluster.port, os_info.user);
#ifndef WIN32
	printf(_("\
  $ export PGDATAOLD=oldCluster/data\n\
  $ export PGDATANEW=newCluster/data\n\
  $ export PGBINOLD=oldCluster/bin\n\
  $ export PGBINNEW=newCluster/bin\n\
  $ pg_upgrade\n"));
#else
	printf(_("\
  C:\\> set PGDATAOLD=oldCluster/data\n\
  C:\\> set PGDATANEW=newCluster/data\n\
  C:\\> set PGBINOLD=oldCluster/bin\n\
  C:\\> set PGBINNEW=newCluster/bin\n\
  C:\\> pg_upgrade\n"));
#endif
	printf(_("\nReport bugs to <bugs@greenplum.org>.\n"));
}


/*
 * check_required_directory()
 *
 * Checks a directory option.
 *	dirpath		  - the directory name supplied on the command line, or NULL
 *	envVarName	  - the name of an environment variable to get if dirpath is NULL
 *	useCwd		  - true if OK to default to CWD
 *	cmdLineOption - the command line option for this directory
 *	description   - a description of this directory option
 *
 * We use the last two arguments to construct a meaningful error message if the
 * user hasn't provided the required directory name.
 */
static void
check_required_directory(char **dirpath, const char *envVarName, bool useCwd,
						 const char *cmdLineOption, const char *description)
{
	if (*dirpath == NULL || strlen(*dirpath) == 0)
	{
		const char *envVar;

		if ((envVar = getenv(envVarName)) && strlen(envVar))
			*dirpath = pg_strdup(envVar);
		else if (useCwd)
		{
			char		cwd[MAXPGPATH];

			if (!getcwd(cwd, MAXPGPATH))
				pg_fatal("could not determine current directory\n");
			*dirpath = pg_strdup(cwd);
		}
		else
			pg_fatal("You must identify the directory where the %s.\n"
					 "Please use the %s command-line option or the %s environment variable.\n",
					 description, cmdLineOption, envVarName);
	}

	/*
	 * Clean up the path, in particular trimming any trailing path separators,
	 * because we construct paths by appending to this path.
	 */
	canonicalize_path(*dirpath);
}

/*
 * adjust_data_dir
 *
 * If a configuration-only directory was specified, find the real data dir
 * by quering the running server.  This has limited checking because we
 * can't check for a running server because we can't find postmaster.pid.
 *
 * On entry, cluster->pgdata has been set from command line or env variable,
 * but cluster->pgconfig isn't set.  We fill both variables with corrected
 * values.
 */
void
adjust_data_dir(ClusterInfo *cluster)
{
	char		filename[MAXPGPATH];
	char		cmd[MAXPGPATH],
				cmd_output[MAX_STRING];
	FILE	   *fp,
			   *output;

	/* Initially assume config dir and data dir are the same */
	cluster->pgconfig = pg_strdup(cluster->pgdata);

	/* If there is no postgresql.conf, it can't be a config-only dir */
	snprintf(filename, sizeof(filename), "%s/postgresql.conf", cluster->pgconfig);
	if ((fp = fopen(filename, "r")) == NULL)
		return;
	fclose(fp);

	/* If PG_VERSION exists, it can't be a config-only dir */
	snprintf(filename, sizeof(filename), "%s/PG_VERSION", cluster->pgconfig);
	if ((fp = fopen(filename, "r")) != NULL)
	{
		fclose(fp);
		return;
	}

	/* Must be a configuration directory, so find the real data directory. */

	prep_status("Finding the real data directory for the %s cluster",
				CLUSTER_NAME(cluster));

	/*
	 * We don't have a data directory yet, so we can't check the PG version,
	 * so this might fail --- only works for PG 9.2+.   If this fails,
	 * pg_upgrade will fail anyway because the data files will not be found.
	 */
	snprintf(cmd, sizeof(cmd), "\"%s/postmaster\" -D \"%s\" -C data_directory",
			 cluster->bindir, cluster->pgconfig);

	if ((output = popen(cmd, "r")) == NULL ||
		fgets(cmd_output, sizeof(cmd_output), output) == NULL)
		pg_fatal("Could not get data directory using %s: %s\n",
				 cmd, getErrorText());

	pclose(output);

	/* Remove trailing newline */
	if (strchr(cmd_output, '\n') != NULL)
		*strchr(cmd_output, '\n') = '\0';

	cluster->pgdata = pg_strdup(cmd_output);

	check_ok();
}


/*
 * get_sock_dir
 *
 * Identify the socket directory to use for this cluster.  If we're doing
 * a live check (old cluster only), we need to find out where the postmaster
 * is listening.  Otherwise, we're going to put the socket into the current
 * directory.
 */
void
get_sock_dir(ClusterInfo *cluster, bool live_check)
{
#ifdef HAVE_UNIX_SOCKETS

	/*
	 * sockdir and port were added to postmaster.pid in PG 9.1. Pre-9.1 cannot
	 * process pg_ctl -w for sockets in non-default locations.
	 */
	if (GET_MAJOR_VERSION(cluster->major_version) >= 901)
	{
		if (!live_check)
			cluster->sockdir = user_opts.socketdir;
		else
		{
			/*
			 * If we are doing a live check, we will use the old cluster's
			 * Unix domain socket directory so we can connect to the live
			 * server.
			 */
			unsigned short orig_port = cluster->port;
			char		filename[MAXPGPATH],
						line[MAXPGPATH];
			FILE	   *fp;
			int			lineno;

			snprintf(filename, sizeof(filename), "%s/postmaster.pid",
					 cluster->pgdata);
			if ((fp = fopen(filename, "r")) == NULL)
				pg_fatal("Cannot open file %s: %m\n", filename);

			for (lineno = 1;
			   lineno <= Max(LOCK_FILE_LINE_PORT, LOCK_FILE_LINE_SOCKET_DIR);
				 lineno++)
			{
				if (fgets(line, sizeof(line), fp) == NULL)
					pg_fatal("Cannot read line %d from %s: %m\n", lineno, filename);

				/* potentially overwrite user-supplied value */
				if (lineno == LOCK_FILE_LINE_PORT)
					sscanf(line, "%hu", &old_cluster.port);
				if (lineno == LOCK_FILE_LINE_SOCKET_DIR)
				{
					cluster->sockdir = pg_strdup(line);
					/* strip off newline */
					if (strchr(cluster->sockdir, '\n') != NULL)
						*strchr(cluster->sockdir, '\n') = '\0';
				}
			}
			fclose(fp);

			/* warn of port number correction */
			if (orig_port != DEF_PGUPORT && old_cluster.port != orig_port)
				pg_log(PG_WARNING, "User-supplied old port number %hu corrected to %hu\n",
					   orig_port, cluster->port);
		}
	}
	else

		/*
		 * Can't get sockdir and pg_ctl -w can't use a non-default, use
		 * default
		 */
		cluster->sockdir = NULL;
#else							/* !HAVE_UNIX_SOCKETS */
	cluster->sockdir = NULL;
#endif
}
