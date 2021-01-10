-- start_matchsubs
-- m/INSERT \d+/
-- s/INSERT \d+/INSERT/
-- end_matchsubs
create schema hashjoin_spill;
set search_path to hashjoin_spill;

-- start_ignore
create language plpythonu;
-- end_ignore

-- set workfile is created to true if all segment did it.
create or replace function hashjoin_spill.is_workfile_created(explain_query text)
returns setof int as
$$
import re
query = "select count(*) as nsegments from gp_segment_configuration where role='p' and content >= 0;"
rv = plpy.execute(query)
nsegments = int(rv[0]['nsegments'])
rv = plpy.execute(explain_query)
search_text = 'Work_mem used'
result = []
for i in range(len(rv)):
    cur_line = rv[i]['QUERY PLAN']
    if search_text.lower() in cur_line.lower():
        p = re.compile('.+\((seg[\d]+).+ Workfile: \(([\d+]) spilling\)')
        m = p.match(cur_line)
        workfile_created = int(m.group(2))
        cur_row = int(workfile_created == nsegments)
        result.append(cur_row)
return result
$$
language plpythonu;

-- start_ignore
DROP ROLE IF EXISTS role1_memory_test;
DROP RESOURCE GROUP rg1_memory_test;
-- end_ignore
CREATE RESOURCE GROUP rg1_memory_test WITH
(concurrency=2, cpu_rate_limit=10, memory_limit=30, memory_shared_quota=0, memory_spill_ratio=1);
CREATE ROLE role1_memory_test SUPERUSER RESOURCE GROUP rg1_memory_test;
SET ROLE TO role1_memory_test;

CREATE TABLE test_hj_spill (i1 int, i2 int, i3 int, i4 int, i5 int, i6 int, i7 int, i8 int);
insert into test_hj_spill SELECT i,i,i%1000,i,i,i,i,i from
	(select generate_series(1, nsegments * 15000) as i from
	(select count(*) as nsegments from gp_segment_configuration where role='p' and content >= 0) foo) bar;
set gp_resgroup_print_operator_memory_limits=on;

set gp_workfile_type_hashjoin=buffile;
select avg(i3) from (SELECT t1.* FROM test_hj_spill AS t1 RIGHT JOIN test_hj_spill AS t2 ON t1.i1=t2.i2) foo;
select * from hashjoin_spill.is_workfile_created('explain analyze SELECT t1.* FROM test_hj_spill AS t1 RIGHT JOIN test_hj_spill AS t2 ON t1.i1=t2.i2;');
select * from hashjoin_spill.is_workfile_created('explain analyze SELECT t1.* FROM test_hj_spill AS t1 RIGHT JOIN test_hj_spill AS t2 ON t1.i1=t2.i2 LIMIT 15000;');

set gp_workfile_type_hashjoin=bfz;
set gp_workfile_compress_algorithm=zlib;
select avg(i3) from (SELECT t1.* FROM test_hj_spill AS t1 RIGHT JOIN test_hj_spill AS t2 ON t1.i1=t2.i2) foo;
select * from hashjoin_spill.is_workfile_created('explain analyze SELECT t1.* FROM test_hj_spill AS t1 RIGHT JOIN test_hj_spill AS t2 ON t1.i1=t2.i2');
select * from hashjoin_spill.is_workfile_created('explain analyze SELECT t1.* FROM test_hj_spill AS t1 RIGHT JOIN test_hj_spill AS t2 ON t1.i1=t2.i2 LIMIT 15000;');

set gp_workfile_compress_algorithm=NONE;
select avg(i3) from (SELECT t1.* FROM test_hj_spill AS t1 RIGHT JOIN test_hj_spill AS t2 ON t1.i1=t2.i2) foo;
select * from hashjoin_spill.is_workfile_created('explain analyze SELECT t1.* FROM test_hj_spill AS t1 RIGHT JOIN test_hj_spill AS t2 ON t1.i1=t2.i2');
select * from hashjoin_spill.is_workfile_created('explain analyze SELECT t1.* FROM test_hj_spill AS t1 RIGHT JOIN test_hj_spill AS t2 ON t1.i1=t2.i2 LIMIT 15000;');

drop schema hashjoin_spill cascade;

-- start_ignore
RESET ROLE;
DROP ROLE IF EXISTS role1_memory_test;
DROP RESOURCE GROUP rg1_memory_test;
-- end_ignore
