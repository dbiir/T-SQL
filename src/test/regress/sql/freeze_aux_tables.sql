-- These tests verify that vacuum freeze correctly updates age of auxiliary
-- tables along with the age of a parent appendonly table

-- First, create some helper functions.

CREATE TEMPORARY TABLE dummytable (id int4) distributed randomly;

CREATE FUNCTION advance_xid_counter(n integer) RETURNS void as $$
declare
  i integer;
begin
  -- do multiple updates to advance the XID counter
  -- The purpose of the exception block is to begin a new subtransaction for
  -- each command, to consume an XID.
  for i in 1..n loop
    begin
      insert into dummytable values (i);
    exception when others then
      raise notice 'something went wrong: %', sqlerrm;
    end;
  end loop;
end;
$$ language 'plpgsql';

CREATE FUNCTION classify_age(age integer) RETURNS text AS $$
  SELECT CASE WHEN $1 < 0 THEN 'negative'
              WHEN $1 = 0 THEN 'zero'
	      WHEN $1 < 50 THEN 'very young' -- less than vacuum_freeze_min_age=50 that we use below
	      WHEN $1 < 100 THEN 'young' -- recently processed by vacuum freeze
	      ELSE 'old' END; -- not frozen
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE TYPE rel_ages AS (segid integer, relname text, age integer);

-- Get age(relfrozenxid) of a table, and all its auxiliary tables. On master,
-- and on all segments. For AO and CO table relfrozenxid = 0 and should stay
-- 0. So, essentially base AO / CO tables should not show-up in the results.
CREATE OR REPLACE FUNCTION aux_rel_ages(testrelid regclass) RETURNS SETOF rel_ages as
$$
declare
  rec rel_ages;
begin
  for rec in select gp_segment_id, replace(relname, testrelid::oid::text, '<oid>'), age(relfrozenxid)
    from pg_class
    where relkind in ('r','t','o','b','M') and relstorage not in ('x','f','v')
    and (relname like '%' || testrelid::oid || '%' or oid = testrelid::oid )
    and not relfrozenxid = 0
  loop
    return next rec;
  end loop;

  for rec in select gp_segment_id, replace(relname, testrelid::oid::text, '<oid>'), age(relfrozenxid)
    from gp_dist_random('pg_class')
    where relkind in ('r','t','o','b','M') and relstorage not in ('x','f','v')
    and (relname like '%' || testrelid::oid || '%' or oid = testrelid::oid )
    and not relfrozenxid = 0
  loop
    return next rec;
  end loop;
end;
$$ language plpgsql;


-- Start tests
--
-- The basic test structure is:
-- 1. CREATE TABLE, leave it empty
-- 2. Run some commands, to advance XID counter
-- 3. VACUUM
-- 4. Verify that the relfrozenxid was advanced by the vacuum
--
set vacuum_freeze_min_age = 50;

create table test_table_heap (id int, col1 int) with (appendonly=false);
create table test_table_heap_with_toast (id int, col1 int, col2 text) with (appendonly=false);
create table test_table_ao (id int, col1 int) with (appendonly=true, orientation=row);
create table test_table_ao_with_toast (id int, col1 int, col2 text) with (appendonly=true, orientation=row);
create table test_table_co (id int, col1 int) with (appendonly=true, orientation=column);
create table test_table_co_with_toast (id int, col1 int, col2 text) with (appendonly=true, orientation=column);

create index test_heap_idx on test_table_heap using btree(id);
create index test_heap_wt_idx on test_table_heap_with_toast using btree(id);
create index test_heap_ao_idx on test_table_ao using btree(id);
create index test_heap_ao_wt_idx on test_table_ao_with_toast using btree(id);
create index test_heap_co_idx on test_table_co using btree(id);
create index test_heap_co_wt_idx on test_table_co_with_toast using btree(id);

-- Advance XID counter, vacuum, and check that relfrozenxid was advanced for
-- all the tables, including auxiliary tables, even though there were no
-- dead tuples.
select advance_xid_counter(500);

-- check table ages before vacuum.
select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_heap')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_heap_with_toast')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_ao')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_ao_with_toast')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_co')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_co_with_toast')
group by segid = -1, relname, classify_age(age);

-- Vacuum. This should advance relfrozenxid on all the tables.
vacuum test_table_heap;
vacuum test_table_heap_with_toast;
vacuum test_table_ao;
vacuum test_table_ao_with_toast;
vacuum test_table_co;
vacuum test_table_co_with_toast;

-- Check table ages.
select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_heap')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_heap_with_toast')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_ao')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_ao_with_toast')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_co')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_co_with_toast')
group by segid = -1, relname, classify_age(age);


-- Repeat the tests on a table that has been inserted to, but all the rows
-- have been deleted.
INSERT INTO test_table_heap select i, i*2 from generate_series(1, 20)i;
INSERT INTO test_table_heap_with_toast select i, i*2, i*5 from generate_series(1, 20)i;
INSERT INTO test_table_ao select i, i*2 from generate_series(1, 20)i;
INSERT INTO test_table_ao_with_toast select i, i*2, i*5 from generate_series(1, 20)i;
INSERT INTO test_table_co select i, i*2 from generate_series(1, 20)i;
INSERT INTO test_table_co_with_toast select i, i*2 from generate_series(1, 20)i;

delete from test_table_heap;
delete from test_table_heap_with_toast;
delete from test_table_ao;
delete from test_table_ao_with_toast;
delete from test_table_co;
delete from test_table_co_with_toast;

select count(*) from test_table_heap;
select count(*) from test_table_heap_with_toast;
select count(*) from test_table_ao;
select count(*) from test_table_ao_with_toast;
select count(*) from test_table_co;
select count(*) from test_table_co_with_toast;

select advance_xid_counter(500);

vacuum freeze test_table_heap;
vacuum freeze test_table_heap_with_toast;
vacuum freeze test_table_ao;
vacuum freeze test_table_ao_with_toast;
vacuum freeze test_table_co;
vacuum freeze test_table_co_with_toast;

-- Check table ages again. Because we used VACUUM FREEZE, they should be
-- very young now.
select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_heap')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_heap_with_toast')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_ao')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_ao_with_toast')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_co')
group by segid = -1, relname, classify_age(age);

select segid = -1 as is_master, relname, classify_age(age) from aux_rel_ages('test_table_co_with_toast')
group by segid = -1, relname, classify_age(age);
