\set has_oids false

-- start_ignore
create extension if not exists gp_debug_numsegments;
-- end_ignore

drop schema if exists test_expand_table_aoco cascade;
create schema test_expand_table_aoco;
set search_path=test_expand_table_aoco,public;
set gp_default_storage_options='appendonly=true,orientation=column';
set allow_system_table_mods=true;

-- Hash distributed tables
select gp_debug_set_create_table_default_numsegments(2);
Create table t1(a int, b int, c int) distributed by (a);
insert into t1 select i,i,0 from generate_series(1,100) I;
Update t1 set c = gp_segment_id;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

begin;
Alter table t1 expand table;
Select gp_segment_id, count(*) from t1 group by gp_segment_id;
abort;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

Alter table t1 expand table;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

select numsegments from gp_distribution_policy where localoid='t1'::regclass;
drop table t1;


select gp_debug_set_create_table_default_numsegments(1);
Create table t1(a int, b int, c int) with (OIDS=:has_oids) distributed by (a,b);
insert into t1 select i,i,0 from generate_series(1,100) I;
Update t1 set c = gp_segment_id;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

begin;
Alter table t1 expand table;
Select gp_segment_id, count(*) from t1 group by gp_segment_id;
abort;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

Alter table t1 expand table;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

select numsegments from gp_distribution_policy where localoid='t1'::regclass;
drop table t1;

-- Test NULLs.
select gp_debug_set_create_table_default_numsegments(2);
Create table t1(a int, b int, c int) distributed by (a,b,c);
insert into t1 values
  (1,    1,    1   ),
  (null, 2,    2   ),
  (3,    null, 3   ),
  (4,    4,    null),
  (null, null, 5   ),
  (null, 6,    null),
  (7,    null, null),
  (null, null, null);
Select gp_segment_id, count(*) from t1 group by gp_segment_id;

begin;
Alter table t1 expand table;
Select gp_segment_id, count(*) from t1 group by gp_segment_id;
abort;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

Alter table t1 expand table;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

select numsegments from gp_distribution_policy where localoid='t1'::regclass;
drop table t1;

select gp_debug_set_create_table_default_numsegments(1);
Create table t1(a int, b int, c int) distributed by (a) partition by list(b) (partition t1_1 values(1), partition t1_2 values(2), default partition other);
insert into t1 select i,i,0 from generate_series(1,100) I;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

begin;
Alter table t1 expand table;
Select gp_segment_id, count(*) from t1 group by gp_segment_id;
abort;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

Alter table t1 expand table;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

select numsegments from gp_distribution_policy where localoid='t1'::regclass;
drop table t1;

-- Random distributed tables
select gp_debug_set_create_table_default_numsegments(2);
Create table r1(a int, b int, c int) distributed randomly;
insert into r1 select i,i,0 from generate_series(1,100) I;
Update r1 set c = gp_segment_id;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

begin;
Alter table r1 expand table;
Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;
abort;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

Alter table r1 expand table;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

select numsegments from gp_distribution_policy where localoid='r1'::regclass;
drop table r1;

select gp_debug_set_create_table_default_numsegments(2);
Create table r1(a int, b int, c int) with (OIDS=:has_oids) distributed randomly;
insert into r1 select i,i,0 from generate_series(1,100) I;
Update r1 set c = gp_segment_id;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

begin;
Alter table r1 expand table;
Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;
abort;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

Alter table r1 expand table;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

select numsegments from gp_distribution_policy where localoid='r1'::regclass;
drop table r1;

select gp_debug_set_create_table_default_numsegments(2);
Create table r1(a int, b int, c int) distributed randomly partition by list(b) (partition r1_1 values(1), partition r1_2 values(2), default partition other);
insert into r1 select i,i,0 from generate_series(1,100) I;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

begin;
Alter table r1 expand table;
Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;
abort;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

Alter table r1 expand table;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

select numsegments from gp_distribution_policy where localoid='r1'::regclass;
drop table r1;

-- Replicated tables
select gp_debug_set_create_table_default_numsegments(1);
Create table r1(a int, b int, c int) distributed replicated;

insert into r1 select i,i,0 from generate_series(1,100) I;

Select count(*) from gp_dist_random('r1');

begin;
Alter table r1 expand table;
Select count(*) from gp_dist_random('r1');
abort;

Select count(*) from gp_dist_random('r1');

Alter table r1 expand table;

Select count(*) from gp_dist_random('r1');

select numsegments from gp_distribution_policy where localoid='r1'::regclass;
drop table r1;

--
select gp_debug_set_create_table_default_numsegments(1);
Create table r1(a int, b int, c int) with (OIDS=:has_oids) distributed replicated;

insert into r1 select i,i,0 from generate_series(1,100) I;

Select count(*) from gp_dist_random('r1');

begin;
Alter table r1 expand table;
Select count(*) from gp_dist_random('r1');
abort;

Select count(*) from gp_dist_random('r1');

Alter table r1 expand table;

Select count(*) from gp_dist_random('r1');

select numsegments from gp_distribution_policy where localoid='r1'::regclass;
drop table r1;

-- multi-level partition tables
select gp_debug_set_create_table_default_numsegments(2);
CREATE TABLE part_t1(a int, b int, c int, d int, e int)
DISTRIBUTED BY(a)
PARTITION BY RANGE (b)
    SUBPARTITION BY RANGE (c)
        SUBPARTITION TEMPLATE (
            START(1) END (3) EVERY(1),
            DEFAULT SUBPARTITION others_c)

    SUBPARTITION BY LIST (d)
        SUBPARTITION TEMPLATE (
            SUBPARTITION one VALUES (1),
            SUBPARTITION two VALUES (2),
            SUBPARTITION three VALUES (3),
            DEFAULT SUBPARTITION others_d)

( START (1) END (2) EVERY (1),
    DEFAULT PARTITION other_b);

insert into part_t1 select i,i%3,i%4,i%5,i from generate_series(1,100) I;
Update part_t1 set e = gp_segment_id;

Select gp_segment_id, count(*) from part_t1 group by gp_segment_id;

begin;
Alter table part_t1 expand table;
Select gp_segment_id, count(*) from part_t1 group by gp_segment_id;
abort;

Select gp_segment_id, count(*) from part_t1 group by gp_segment_id;

Alter table part_t1 expand table;

Select gp_segment_id, count(*) from part_t1 group by gp_segment_id;

select numsegments from gp_distribution_policy where localoid='part_t1'::regclass;
drop table part_t1;

--
select gp_debug_set_create_table_default_numsegments(2);
CREATE TABLE part_t1(a int, b int, c int, d int, e int)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (b)
    SUBPARTITION BY RANGE (c)
        SUBPARTITION TEMPLATE (
            START(1) END (3) EVERY(1),
            DEFAULT SUBPARTITION others_c)

    SUBPARTITION BY LIST (d)
        SUBPARTITION TEMPLATE (
            SUBPARTITION one VALUES (1),
            SUBPARTITION two VALUES (2),
            SUBPARTITION three VALUES (3),
            DEFAULT SUBPARTITION others_d)

( START (1) END (2) EVERY (1),
    DEFAULT PARTITION other_b);

insert into part_t1 select i,i%3,i%4,i%5,i from generate_series(1,100) I;
Update part_t1 set e = gp_segment_id;

Select count(*) from part_t1;
Select count(*) > 0 from part_t1 where gp_segment_id=2;

begin;
Alter table part_t1 expand table;
Select count(*) from part_t1;
Select count(*) > 0 from part_t1 where gp_segment_id=2;
abort;

Select count(*) from part_t1;
Select count(*) > 0 from part_t1 where gp_segment_id=2;

Alter table part_t1 expand table;

Select count(*) from part_t1;
Select count(*) > 0 from part_t1 where gp_segment_id=2;

select numsegments from gp_distribution_policy where localoid='part_t1'::regclass;
drop table part_t1;

-- only expand leaf partitions, not allowed now.
select gp_debug_set_create_table_default_numsegments(2);
CREATE TABLE part_t1(a int, b int, c int, d int, e int)
DISTRIBUTED BY(a)
PARTITION BY RANGE (b)
    SUBPARTITION BY RANGE (c)
        SUBPARTITION TEMPLATE (
            START(1) END (3) EVERY(1),
            DEFAULT SUBPARTITION others_c)

    SUBPARTITION BY LIST (d)
        SUBPARTITION TEMPLATE (
            SUBPARTITION one VALUES (1),
            SUBPARTITION two VALUES (2),
            SUBPARTITION three VALUES (3),
            DEFAULT SUBPARTITION others_d)

( START (1) END (2) EVERY (1),
    DEFAULT PARTITION other_b);

insert into part_t1 select i,i%3,i%4,i%5,i from generate_series(1,100) I;
Update part_t1 set e = gp_segment_id;

select gp_segment_id, * from part_t1_1_prt_other_b_2_prt_2_3_prt_others_d;

alter table part_t1_1_prt_other_b_2_prt_2_3_prt_others_d expand table;
select gp_segment_id, * from part_t1_1_prt_other_b_2_prt_2_3_prt_others_d;

-- try to expand root partition, should success
Alter table part_t1 expand table;
Select gp_segment_id, count(*) from part_t1 group by gp_segment_id;

drop table part_t1;

-- start_ignore
-- We need to do a cluster expansion which will check if there are partial
-- tables, we need to drop the partial tables to keep the cluster expansion
-- run correctly.
reset search_path;
drop schema test_reshuffle_aoco cascade;
-- end_ignore
