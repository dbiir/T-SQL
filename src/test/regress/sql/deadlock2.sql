-- A classic motion hazard / deadlock is between the inner and outer motions,
-- but it is also possible to happen between the outer and joinqual / subplan
-- motions.  A sample plan is as below:
--
--  Gather Motion 3:1  (slice4; segments: 3)
--    ->  Hash Left Join
--          Hash Cond: (t_outer.c2 = t_inner.c2)
--          Join Filter: (NOT (SubPlan))
--          ->  Redistribute Motion 3:3  (slice1; segments: 3)
--                Hash Key: t_outer.c2
--                ->  Seq Scan on t_outer
--          ->  Hash
--                ->  Redistribute Motion 3:3  (slice2; segments: 3)
--                      Hash Key: t_inner.c2
--                      ->  Seq Scan on t_inner
--          SubPlan 1  (slice4; segments: 3)
--            ->  Result
--                  Filter: (t_subplan.c2 = t_outer.c1)
--                  ->  Materialize
--                        ->  Broadcast Motion 3:3  (slice3; segments: 3)
--                              ->  Seq Scan on t_subplan

-- Suppose :x0 is distributed on seg0, it does not matter if it is not.
-- This assumption is only to simplify the explanation.
\set x0 1
\set scale 10000

drop schema if exists deadlock2 cascade;
create schema deadlock2;
set search_path = deadlock2;

create table t_inner (c1 int, c2 int);
create table t_outer (c1 int, c2 int);
create table t_subplan (c1 int, c2 int);

-- First load enough data on all the relations to generate redistribute-both
-- motion instead of broadcast-one motion.
insert into t_inner select i, i from generate_series(1,:scale) i;
insert into t_outer select i, i from generate_series(1,:scale) i;
insert into t_subplan select i, i from generate_series(1,:scale) i;
analyze t_inner;
analyze t_outer;
analyze t_subplan;

-- Then delete all of them and load the real data, do not use TRUNCATE as it
-- will clear the analyze information.
delete from t_inner;
delete from t_outer;
delete from t_subplan;

-- t_inner is the inner relation, it does not need much data as long as it is
-- not empty.
insert into t_inner values (:x0, :x0);

-- t_outer is the outer relation of the hash join, all its data are on seg0,
-- and redistributes to seg0, so outer@slice1@seg0 sends data to
-- hashjoin@slice4@seg0 and waits for ACK from it.  After all the data are sent
-- it will send EOS to all the segments of hashjoin@slice4.  So once
-- hashjoin@slice4@seg1 reads from outer@slice1 it has to wait for EOS.
insert into t_outer select :x0, :x0 from generate_series(1,:scale) i;

-- t_subplan is the subplan relation of the hash join, all its data are on
-- seg0, and broadcasts to seg0 and seg1.  When subplan@slice3@seg0 sends data
-- to hashjoin@slice4@seg1 it has to wait for ACK from it, this can happen
-- before hashjoin@slice4@seg1 reading from outer@slice1.
insert into t_subplan select :x0, :x0 from generate_series(1,:scale) i;

-- In the past hash join do the job like this:
--
-- 10. read all the inner tuples and build the hash table;
-- 20. read an outer tuple;
-- 30. pass the outer tuple to the join qual, reads through the motion in the
--     subplan;
-- 40. if there are more outer tuples goto 20.
--
-- So this makes it possible that hashjoin@slice4@seg0 is reading from
-- subplan@slice3@seg0 while itself is being waited by outer@slice1@seg0, then
-- it forms a deadlock:
--
--      outer@slice1@seg0   --[waits for ACK]->  hashjoin@slice4@seg0
--              ^                                         |
--              |                                         |
--    [waits for outer tuple]                 [waits for subplan tuple]
--              |                                         |
--              |                                         v
--     hashjoin@slice4@seg1  <-[wait for ACK]--  subplan@slice3@seg0
--
-- This deadlock is prevented by prefetching the subplan.

-- In theory this deadlock exists in all of hash join, merge join and nestloop
-- join, but so far we have only constructed a reproducer for hash join.
set enable_hashjoin to on;
set enable_mergejoin to off;
set enable_nestloop to off;

select count(*) from t_inner right join t_outer on t_inner.c2=t_outer.c2
   and not exists (select 0 from t_subplan where t_subplan.c2=t_outer.c1);
