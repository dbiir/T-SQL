# gpperfmon

gpperfmon tracks a variety of queries, statistics, system properties, and metrics.

Find more information about the architecture on [the wiki page](https://github.com/greenplum-db/gpdb/wiki/Gpperfmon-Overview)

## Libraries Required

### libsigar:
	https://github.com/hyperic/sigar
	For CentOS 6:
		yum install sigar-devel
	For macOS and CentOS 7 (and others):
	    Use this updated fork: https://github.com/boundary/sigar
		to build:
	    `mkdir build && cd build && cmake .. && make && make install`

## Troubleshooting
	For macOS:
		You may hit a reverse look up issue when viewing the logs of gpperfmon by default $MASTER_DATA_DIRECTORY/gpperfmon/logs
```
	2017-04-11 14:59:56.821681
	PDT,"gpmon","gpperfmon",p40501,th-1633193024,"::1","54006",2017-04-11 14:59:56
	PDT,0,con5,,seg-1,,,,sx1,"FATAL","28000","no pg_hba.conf entry for host
	""::1"", user ""gpmon"", database ""gpperfmon""",,,,,,,0,,"auth.c",608, ```
```
	And also issues at $MASTER_DATA_DIRECTORY/pg_log:
```
		Performance Monitor - failed to connect to gpperfmon database: could not connect to server: No such file or directory
	Is the server running locally and accepting
	connections on Unix domain socket ""/var/pgsql_socket/.s.PGSQL.15432""?",,,,,,,,"SysLoggerMain","syslogger.c",618,
```
		to get pass this you need to do 2 things:
			1) export PGHOST=foo  # where 'foo' is your hostname that is NOT localhost
			2) sudo /etc/hosts   # and separate out (re)definitions of 127.0.0.1, something like:
				127.0.0.1	foo
				127.0.0.1	localhost

