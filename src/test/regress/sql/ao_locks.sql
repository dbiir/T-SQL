-- @Description The locks held after different operations
CREATE TABLE ao_locks_table (a INT, b INT) WITH (appendonly=true) distributed by (a);
INSERT INTO ao_locks_table SELECT i as a, i as b FROM generate_series(1, 100) AS i;

create or replace view locktest_master as
select coalesce(
  case when relname like 'pg_toast%index' then 'toast index'
       when relname like 'pg_toast%' then 'toast table'
       when relname like 'pg_aoseg%' then 'aoseg table'
       when relname like 'pg_aovisimap%index' then 'aovisimap index'
       when relname like 'pg_aovisimap%' then 'aovisimap table'
       else relname end, 'dropped table'),
  mode,
  locktype,
  'master'::text as node
from pg_locks l
left outer join pg_class c on ((l.locktype = 'append-only segment file' and l.relation = c.relfilenode) or (l.locktype != 'append-only segment file' and l.relation = c.oid)),
pg_database d
where relation is not null
and l.database = d.oid
and (relname <> 'gp_fault_strategy' and relname != 'locktest_master' or relname is NULL)
and d.datname = current_database()
and l.gp_segment_id = -1
group by l.gp_segment_id, relation, relname, locktype, mode
order by 1, 3, 2;

create or replace view locktest_segments_dist as
select relname,
  mode,
  locktype,
  l.gp_segment_id as node,
  relation
from pg_locks l
left outer join pg_class c on ((l.locktype = 'append-only segment file' and l.relation = c.relfilenode) or (l.locktype != 'append-only segment file' and l.relation = c.oid)),
pg_database d
where relation is not null
and l.database = d.oid
and (relname <> 'gp_fault_strategy' and relname != 'locktest_segments_dist' or relname is NULL)
and d.datname = current_database()
and l.gp_segment_id > -1
group by l.gp_segment_id, relation, relname, locktype, mode;

create or replace view locktest_segments as
SELECT coalesce(
  case when relname like 'pg_toast%index' then 'toast index'
       when relname like 'pg_toast%' then 'toast table'
       when relname like 'pg_aoseg%' then 'aoseg table'
       when relname like 'pg_aovisimap%index' then 'aovisimap index'
       when relname like 'pg_aovisimap%' then 'aovisimap table'
       else relname end, 'dropped table'),
  mode,
  locktype,
  case when count(*) = 1 then '1 segment'
       else 'n segments' end as node
  FROM gp_dist_random('locktest_segments_dist')
  group by relname, relation, mode, locktype;

-- Actual test begins
BEGIN;
INSERT INTO ao_locks_table VALUES (200, 200);
SELECT * FROM locktest_master where coalesce = 'ao_locks_table' or
 coalesce like 'aovisimap%' or coalesce like 'aoseg%';
SELECT * FROM locktest_segments where coalesce = 'ao_locks_table' or
 coalesce like 'aovisimap%' or coalesce like 'aoseg%';
COMMIT;

BEGIN;
DELETE FROM ao_locks_table WHERE a = 1;
SELECT * FROM locktest_master where coalesce = 'ao_locks_table' or
 coalesce like 'aovisimap%' or coalesce like 'aoseg%';
SELECT * FROM locktest_segments where coalesce = 'ao_locks_table' or
 coalesce like 'aovisimap%' or coalesce like 'aoseg%';
COMMIT;

BEGIN;
UPDATE ao_locks_table SET b = -1 WHERE a = 2;
SELECT * FROM locktest_master where coalesce = 'ao_locks_table' or
 coalesce like 'aovisimap%' or coalesce like 'aoseg%';
SELECT * FROM locktest_segments where coalesce = 'ao_locks_table' or
 coalesce like 'aovisimap%' or coalesce like 'aoseg%';
COMMIT;
