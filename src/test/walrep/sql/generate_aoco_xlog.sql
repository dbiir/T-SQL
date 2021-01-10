-- Test AOCO XLogging
CREATE TABLE generate_aoco_xlog_table(a INT, b INT) WITH (APPENDONLY=TRUE, ORIENTATION=COLUMN);

-- Store the location of xlog in a temporary table so that we can
-- use it to request walsender to start streaming from this point
CREATE TEMP TABLE tmp(dummy int, dbid int, startpoint pg_lsn) distributed by (dummy);
INSERT INTO tmp SELECT 1, gp_execution_segment(),pg_current_xlog_location() FROM
gp_dist_random('gp_id');

-- Generate some xlog records for AOCO
INSERT INTO generate_aoco_xlog_table VALUES (1, 10), (2, 10), (8, 10), (3, 10);

-- GPDB_94_MERGE_FIXME: c function test_xlog_ao() call walrcv_connect() in sql function test_xlog_ao_wrapper(), will fail with message
-- ERROR:  could not connect to the primary server: FATAL:  no pg_hba.conf entry for replication connection from host "[local]", user "gpadmin", SSL off (libpqwalreceiver.c:111)
-- start_ignore
-- Verify that AO xlog record was received
SELECT gp_segment_id, relname, record_type, segment_filenum, recordlen, file_offset
  FROM test_xlog_ao_wrapper(
    (SELECT array_agg(startpoint) FROM 
       (SELECT startpoint from tmp order by dbid) t
    )
  ) 
WHERE spcNode = (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default')
AND dbNode = (SELECT oid FROM pg_database WHERE datname = current_database())
ORDER BY gp_segment_id, xrecoff;
-- end_ignore

-- Store the latest xlog offset
DELETE FROM tmp;
INSERT INTO tmp SELECT 1, gp_execution_segment(),pg_current_xlog_location()
FROM gp_dist_random('gp_id');

-- Generate a truncate XLOG entry for generate_ao_xlog_table.
BEGIN;
INSERT INTO generate_aoco_xlog_table SELECT i,i FROM generate_series(1,10)i;
ABORT;
VACUUM generate_aoco_xlog_table;

-- GPDB_94_MERGE_FIXME: c function test_xlog_ao() call walrcv_connect() in sql function test_xlog_ao_wrapper(), will fail with message
-- ERROR:  could not connect to the primary server: FATAL:  no pg_hba.conf entry for replication connection from host "[local]", user "gpadmin", SSL off (libpqwalreceiver.c:111)
-- start_ignore
-- Verify that truncate AO xlog record was received
SELECT gp_segment_id, relname, record_type, segment_filenum, recordlen, file_offset
  FROM test_xlog_ao_wrapper(
    (SELECT array_agg(startpoint) FROM 
       (SELECT startpoint from tmp order by dbid) t
    )
  ) 
WHERE spcNode = (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default')
AND dbNode = (SELECT oid FROM pg_database WHERE datname = current_database())
ORDER BY gp_segment_id, xrecoff;
-- end_ignore