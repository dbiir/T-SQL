-- start_matchsubs
-- m/INSERT \d+/
-- s/INSERT \d+/INSERT/
-- end_matchsubs
create schema materialize_spill;
set search_path to materialize_spill;

-- start_ignore
create language plpythonu;
-- end_ignore

-- Helper function to verify that a plan spilled to disk. For each node
-- in the plan that used Workfiles (Materialize or Sort nodes, currently),
-- return the number of segments where the node spilled to disk.
create or replace function num_workfiles_created(explain_query text)
returns setof int as
$$
import re
rv = plpy.execute(explain_query)
search_text = 'Work_mem used'
result = []
for i in range(len(rv)):
    cur_line = rv[i]['QUERY PLAN']
    if search_text.lower() in cur_line.lower():
        p = re.compile('.+\((seg[\d]+).+ Workfile: \(([\d+]) spilling\)')
        m = p.match(cur_line)
        workfile_created = int(m.group(2))
        result.append(workfile_created)
return result
$$
language plpythonu;

-- Run a query that contains a Materialize node that spills to disk.
--
-- The expected plan is something like this:
--
--  Gather Motion 3:1
--    ->  Nested Loop Left Join
--          Join Filter: t1.i1 = t2.i2
--          ->  Seq Scan on test_mat_small t1
--          ->  Materialize
--                ->  Redistribute Motion 3:3
--                      Hash Key: t2.i2
--                      ->  Seq Scan on test_mat_large t2
--
-- The planner will put a Materialize node on the inner side, to shield
-- the Motion node from rewinding. Because the larger table doesn't fit
-- in memory, the Materialize will spill to disk.
--
CREATE TABLE test_mat_small (i1 int);
INSERT INTO test_mat_small SELECT i from generate_series(101, 105) i;

-- Scale the larger table's size with the number of segments, so that there is enough
-- data on every segment to cause spilling.
CREATE TABLE test_mat_large (i1 int, i2 int, i3 int, i4 int, i5 int, i6 int, i7 int, i8 int);
INSERT INTO test_mat_large SELECT i,i,i,i,i,i,i,i from
	(select generate_series(1, nsegments * 50000) as i from
	(select count(*) as nsegments from gp_segment_configuration where role='p' and content >= 0) foo) bar;

-- start_ignore
DROP ROLE IF EXISTS role1_memory_test;
DROP RESOURCE GROUP rg1_memory_test;
-- end_ignore
CREATE RESOURCE GROUP rg1_memory_test WITH
(concurrency=2, cpu_rate_limit=10, memory_limit=30, memory_shared_quota=0, memory_spill_ratio=1);
CREATE ROLE role1_memory_test SUPERUSER RESOURCE GROUP rg1_memory_test;
SET ROLE TO role1_memory_test;

set gp_resgroup_print_operator_memory_limits=on;
set enable_hashjoin = false;
set enable_nestloop = true;
-- ORCA doesn't honor enable_nestloop/enable_hashjoin, so this won't produce
-- the kind of plan we're looking for.
set optimizer=off;

-- This is the actual test query.
select * FROM test_mat_small as t1 left outer join test_mat_large AS t2 on t1.i1=t2.i2;

-- Check that the Materialize node spilled to disk, to make sure we're testing spilling
-- as intended. The inner side of the join with the Materialize will not get executed on
-- segments that have no data for the outer side. Therefore, we expect the Materialize
-- node to only be executed, and spilled, on as many segments as there nodes that hold
-- data from test_mat_small.
select n - (select count (distinct gp_segment_id) from test_mat_small) as difference
from num_workfiles_created($$
  explain analyze
  select * FROM test_mat_small as t1 left outer join test_mat_large AS t2 on t1.i1=t2.i2
$$) as n;

-- Repeat, with a LIMIT. This causes the underlying scan to finish earlier.
select * FROM test_mat_small as t1 left outer join test_mat_large AS t2 on t1.i1=t2.i2 limit 10;
select n - (select count (distinct gp_segment_id) from test_mat_small) as difference
from num_workfiles_created($$
  explain analyze
  select * FROM test_mat_small as t1 left outer join test_mat_large AS t2 on t1.i1=t2.i2 limit 10
$$) as n;

drop schema materialize_spill cascade;

-- start_ignore
RESET ROLE;
DROP ROLE IF EXISTS role1_memory_test;
DROP RESOURCE GROUP rg1_memory_test;
-- end_ignore
