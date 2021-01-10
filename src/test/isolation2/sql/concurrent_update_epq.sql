-- If we enable the GDD, then the lock maybe downgrade to
-- RowExclusiveLock, so UPDATE/Delete can be executed
-- concurrently, it may trigger the EvalPlanQual function
-- to recheck the qualifications.
-- If the subPlan have Motion node, then we can not execute
-- EvalPlanQual correctly, so we raise an error when
-- GDD is enabled and EvalPlanQual is tiggered.

-- create heap table
0: show gp_enable_global_deadlock_detector;
0: create table tab_update_epq1 (c1 int, c2 int) distributed randomly;
0: create table tab_update_epq2 (c1 int, c2 int) distributed randomly;
0: insert into tab_update_epq1 values(1,1);
0: insert into tab_update_epq2 values(1,1);
0: select * from tab_update_epq1;
0: select * from tab_update_epq2;

1: set optimizer = off;
2: set optimizer = off;

-- test for heap table
1: begin;
2: begin;
1: update tab_update_epq1 set c1 = c1 + 1 where c2 = 1;
2&: update tab_update_epq1 set c1 = tab_update_epq1.c1 + 1 from tab_update_epq2 where tab_update_epq1.c2 = tab_update_epq2.c2;
1: end;
2<:
2: end;

0: select * from tab_update_epq1;
0: drop table tab_update_epq1;
0: drop table tab_update_epq2;

-- create AO table
0: create table tab_update_epq1 (c1 int, c2 int) with(appendonly=true) distributed randomly;
0: create table tab_update_epq2 (c1 int, c2 int) with(appendonly=true) distributed randomly;
0: insert into tab_update_epq1 values(1,1);
0: insert into tab_update_epq2 values(1,1);
0: select * from tab_update_epq1;
0: select * from tab_update_epq2;

-- test for AO table
1: begin;
2: begin;
1: update tab_update_epq1 set c1 = c1 + 1 where c2 = 1;
2&: update tab_update_epq1 set c1 = tab_update_epq1.c1 + 1 from tab_update_epq2 where tab_update_epq1.c2 = tab_update_epq2.c2;
1: end;
2<:
2: end;

0: select * from tab_update_epq1;
0: drop table tab_update_epq1;
0: drop table tab_update_epq2;
1q:
2q:
0q:

-- enable gdd
-- start_ignore
! gpconfig -c gp_enable_global_deadlock_detector -v on;
! gpstop -rai;
-- end_ignore

-- create heap table
0: show gp_enable_global_deadlock_detector;
0: create table tab_update_epq1 (c1 int, c2 int) distributed randomly;
0: create table tab_update_epq2 (c1 int, c2 int) distributed randomly;
0: insert into tab_update_epq1 values(1,1);
0: insert into tab_update_epq2 values(1,1);
0: select * from tab_update_epq1;
0: select * from tab_update_epq2;

1: set optimizer = off;
2: set optimizer = off;

-- test for heap table
1: begin;
2: begin;
1: update tab_update_epq1 set c1 = c1 + 1 where c2 = 1;
2&: update tab_update_epq1 set c1 = tab_update_epq1.c1 + 1 from tab_update_epq2 where tab_update_epq1.c2 = tab_update_epq2.c2;
1: end;
2<:
2: end;

0: select * from tab_update_epq1;
0: drop table tab_update_epq1;
0: drop table tab_update_epq2;

-- create AO table
0: create table tab_update_epq1 (c1 int, c2 int) with(appendonly=true) distributed randomly;
0: create table tab_update_epq2 (c1 int, c2 int) with(appendonly=true) distributed randomly;
0: insert into tab_update_epq1 values(1,1);
0: insert into tab_update_epq2 values(1,1);
0: select * from tab_update_epq1;
0: select * from tab_update_epq2;

-- test for AO table
1: begin;
2: begin;
1: update tab_update_epq1 set c1 = c1 + 1 where c2 = 1;
2&: update tab_update_epq1 set c1 = tab_update_epq1.c1 + 1 from tab_update_epq2 where tab_update_epq1.c2 = tab_update_epq2.c2;
1: end;
2<:
2: end;

0: select * from tab_update_epq1;
0: drop table tab_update_epq1;
0: drop table tab_update_epq2;
1q:
2q:
0q:

-- disable gdd
-- start_ignore
! gpconfig -c gp_enable_global_deadlock_detector -v off;
! gpstop -rai;
-- end_ignore
