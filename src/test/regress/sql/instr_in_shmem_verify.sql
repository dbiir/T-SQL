-- This test checks for leaks of instrumentation slots in shmem.
-- gp_instrument_shmem_detail is a function can retrieve
-- instrumentation slots contents on every segment.
-- This test should run after all other regression tests are done,
-- then it calls the function to detect if any left over
-- instrumentation slots exists.

-- start_ignore
DROP SCHEMA IF EXISTS QUERY_METRICS CASCADE; 
-- end_ignore
CREATE SCHEMA QUERY_METRICS;
SET SEARCH_PATH=QUERY_METRICS;
CREATE EXTERNAL WEB TABLE __gp_localid
(
    localid    int
)
EXECUTE E'echo $GP_SEGMENT_ID' FORMAT 'TEXT';
GRANT SELECT ON TABLE __gp_localid TO public;
CREATE EXTERNAL WEB TABLE __gp_masterid
(
    masterid    int
)
EXECUTE E'echo $GP_SEGMENT_ID' ON MASTER FORMAT 'TEXT';
GRANT SELECT ON TABLE __gp_masterid TO public;

CREATE FUNCTION gp_instrument_shmem_detail_f()
RETURNS SETOF RECORD
AS '$libdir/gp_instrument_shmem', 'gp_instrument_shmem_detail'
LANGUAGE C IMMUTABLE;
GRANT EXECUTE ON FUNCTION gp_instrument_shmem_detail_f() TO public;

CREATE VIEW gp_instrument_shmem_detail AS
WITH all_entries AS (
  SELECT C.*
    FROM __gp_localid, gp_instrument_shmem_detail_f() as C (
      tmid int4,ssid int4,ccnt int2,segid int2,pid int4
      ,nid int2,tuplecount int8,nloops int8,ntuples int8
    )
  UNION ALL
  SELECT C.*
    FROM __gp_masterid, gp_instrument_shmem_detail_f() as C (
      tmid int4,ssid int4,ccnt int2,segid int2,pid int4
      ,nid int2,tuplecount int8,nloops int8,ntuples int8
    ))
SELECT tmid, ssid, ccnt,segid, pid, nid, tuplecount, nloops, ntuples
FROM all_entries
ORDER BY segid;
GRANT SELECT ON gp_instrument_shmem_detail TO public;

SET OPTIMIZER=OFF;

SELECT count(*) FROM pg_stat_activity;

-- Expected result is 1 row, means only current query in instrument slots,
-- If more than one row returned, means previous test has leaked slots.
SELECT count(*) FROM (SELECT 1 FROM gp_instrument_shmem_detail GROUP BY ssid, ccnt) t;

DROP SCHEMA QUERY_METRICS CASCADE;
