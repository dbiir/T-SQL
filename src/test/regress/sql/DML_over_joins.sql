-- ----------------------------------------------------------------------
-- Test: setup_schema.sql
-- ----------------------------------------------------------------------
create schema DML_over_joins;
set search_path to DML_over_joins;

-- ----------------------------------------------------------------------
-- Test: heap_motion0.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
--   r,s colocated on join attributes
--      delete: using clause, subquery, initplan
--      update: join and subsubquery
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
-- end_ignore
create table r (a int, b int) distributed by (a);
create table s (a int, b int) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;
update r set b = r.b + 1 from s where r.a = s.a;

update r set b = r.b + 1 from s where r.a in (select a from s);

delete from r using s where r.a = s.a;

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a in (select a from s);

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a = (select max(a) from s);



------------------------------------------------------------
-- Updates with motion:
-- 	Redistribute s
------------------------------------------------------------

delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

update r set b = r.b + 4 from s where r.b = s.b;

update r set b = b + 1 where b in (select b from s);

delete from s using r where r.a = s.b;

delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

delete from r using s where r.b = s.b; 


------------------------------------------------------------
-- 	Hash aggregate group by
------------------------------------------------------------

delete from r;
delete from s;

insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

explain update s set b = b + 1 where exists (select 1 from r where s.a = r.b);
update s set b = b + 1 where exists (select 1 from r where s.a = r.b);

explain delete from s where exists (select 1 from r where s.a = r.b);
delete from s where exists (select 1 from r where s.a = r.b);
-- ----------------------------------------------------------------------
-- Test: heap_motion1.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
--   r,s colocated on join attributes
--      delete: using clause, subquery, initplan
--      update: join and subsubquery
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
-- end_ignore
create table r (a int4, b int4) distributed by (a);
create table s (a int4, b int4) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;
update r set b = r.b + 1 from s where r.a = s.a;

update r set b = r.b + 1 from s where r.a in (select a from s);

delete from r using s where r.a = s.a;

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a in (select a from s);

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a = (select max(a) from s);



------------------------------------------------------------
-- Updates with motion:
-- 	Redistribute s
------------------------------------------------------------
delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

update r set b = r.b + 4 from s where r.b = s.b;

update r set b = b + 1 where b in (select b from s);

delete from s using r where r.a = s.b;

delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

delete from r using s where r.b = s.b; 


------------------------------------------------------------
-- 	Hash aggregate group by
------------------------------------------------------------
delete from r;
delete from s;

insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

update s set b = b + 1 where exists (select 1 from r where s.a = r.b);

delete from s where exists (select 1 from r where s.a = r.b);


-- ----------------------------------------------------------------------
-- Test: heap_motion2.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
--   r,s colocated on join attributes
--      delete: using clause, subquery, initplan
--      update: join and subsubquery
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
-- end_ignore
create table r (a int8, b int8) distributed by (a);
create table s (a int8, b int8) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;
update r set b = r.b + 1 from s where r.a = s.a;

update r set b = r.b + 1 from s where r.a in (select a from s);

delete from r using s where r.a = s.a;

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a in (select a from s);

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a = (select max(a) from s);


------------------------------------------------------------
-- Updates with motion:
-- 	Redistribute s
------------------------------------------------------------
delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

update r set b = r.b + 4 from s where r.b = s.b;

update r set b = b + 1 where b in (select b from s);

delete from s using r where r.a = s.b;

delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

delete from r using s where r.b = s.b; 


------------------------------------------------------------
-- 	Hash aggregate group by
------------------------------------------------------------
delete from r;
delete from s;

insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

update s set b = b + 1 where exists (select 1 from r where s.a = r.b);

delete from s where exists (select 1 from r where s.a = r.b);
-- ----------------------------------------------------------------------
-- Test: heap_motion3.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
--   r,s colocated on join attributes
--      delete: using clause, subquery, initplan
--      update: join and subsubquery
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
-- end_ignore
create table r (a float4, b float4) distributed by (a);
create table s (a float4, b float4) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;
update r set b = r.b + 1 from s where r.a = s.a;

update r set b = r.b + 1 from s where r.a in (select a from s);

delete from r using s where r.a = s.a;

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a in (select a from s);

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a = (select max(a) from s);


------------------------------------------------------------
-- Updates with motion:
-- 	Redistribute s
------------------------------------------------------------
delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

update r set b = r.b + 4 from s where r.b = s.b;

update r set b = b + 1 where b in (select b from s);

delete from s using r where r.a = s.b;

delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

delete from r using s where r.b = s.b; 


------------------------------------------------------------
-- 	Hash aggregate group by
------------------------------------------------------------
delete from r;
delete from s;

insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

update s set b = b + 1 where exists (select 1 from r where s.a = r.b);

delete from s where exists (select 1 from r where s.a = r.b);


-- ----------------------------------------------------------------------
-- Test: heap_motion4.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
--   r,s colocated on join attributes
--      delete: using clause, subquery, initplan
--      update: join and subsubquery
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
-- end_ignore
create table r (a float(24), b float(24)) distributed by (a);
create table s (a float(24), b float(24)) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;
update r set b = r.b + 1 from s where r.a = s.a;

update r set b = r.b + 1 from s where r.a in (select a from s);

delete from r using s where r.a = s.a;

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a in (select a from s);

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a = (select max(a) from s);


------------------------------------------------------------
-- Updates with motion:
-- 	Redistribute s
------------------------------------------------------------
delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

update r set b = r.b + 4 from s where r.b = s.b;

update r set b = b + 1 where b in (select b from s);

delete from s using r where r.a = s.b;

delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

delete from r using s where r.b = s.b; 


------------------------------------------------------------
-- 	Hash aggregate group by
------------------------------------------------------------
delete from r;
delete from s;

insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

update s set b = b + 1 where exists (select 1 from r where s.a = r.b);

delete from s where exists (select 1 from r where s.a = r.b);
-- ----------------------------------------------------------------------
-- Test: heap_motion5.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
--   r,s colocated on join attributes
--      delete: using clause, subquery, initplan
--      update: join and subsubquery
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
-- end_ignore
create table r (a float(53), b float(53)) distributed by (a);
create table s (a float(53), b float(53)) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;
update r set b = r.b + 1 from s where r.a = s.a;

update r set b = r.b + 1 from s where r.a in (select a from s);

delete from r using s where r.a = s.a;

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a in (select a from s);

delete from r;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
delete from r where a = (select max(a) from s);


------------------------------------------------------------
-- Updates with motion:
-- 	Redistribute s
------------------------------------------------------------
delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

update r set b = r.b + 4 from s where r.b = s.b;

update r set b = b + 1 where b in (select b from s);

delete from s using r where r.a = s.b;

delete from r;
delete from s;
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

delete from r using s where r.b = s.b; 


------------------------------------------------------------
-- 	Hash aggregate group by
------------------------------------------------------------
delete from r;
delete from s;

insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

update s set b = b + 1 where exists (select 1 from r where s.a = r.b);

delete from s where exists (select 1 from r where s.a = r.b);


-- ----------------------------------------------------------------------
-- Test: unsupported_cases0.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion: unsupported cases
--     	Updating the distribution key
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore

create table r (a int, b int) distributed by (a);
create table s (a int, b int) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

create table p (a int, b int, c int) 
	partition by range (c) (start(1) end(5) every(1), default partition extra);
insert into p select generate_series(1,10000), generate_series(1,10000)*3, generate_series(1,10000)%6;

update s set a = r.a from r where r.b = s.b;


------------------------------------------------------------
--	Statement contains correlated subquery
------------------------------------------------------------
update s set b = (select min(a) from r where b = s.b);

delete from s where b = (select min(a) from r where b = s.b);


------------------------------------------------------------
--	Update partition key (requires moving tuples from one partition to another)
------------------------------------------------------------
update p set c = c + 1 where c = 0;

update p set c = c + 1 where b in (select b from s) and c = 0;


-- ----------------------------------------------------------------------
-- Test: unsupported_cases1.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion: unsupported cases
--     	Updating the distribution key
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore

create table r (a int4, b int4) distributed by (a);
create table s (a int4, b int4) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

create table p (a int4, b int4, c int4) 
	partition by range (c) (start(1) end(5) every(1), default partition extra);
insert into p select generate_series(1,10000), generate_series(1,10000)*3, generate_series(1,10000)%6;

update s set a = r.a from r where r.b = s.b;


------------------------------------------------------------
--	Statement contains correlated subquery
------------------------------------------------------------
update s set b = (select min(a) from r where b = s.b);

delete from s where b = (select min(a) from r where b = s.b);


------------------------------------------------------------
--	Update partition key (requires moving tuples from one partition to another)
------------------------------------------------------------
update p set c = c + 1 where c = 0;

update p set c = c + 1 where b in (select b from s) and c = 0;
-- ----------------------------------------------------------------------
-- Test: unsupported_cases2.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion: unsupported cases
--     	Updating the distribution key
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore

create table r (a int8, b int8) distributed by (a);
create table s (a int8, b int8) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

create table p (a int8, b int8, c int8) 
	partition by range (c) (start(1) end(5) every(1), default partition extra);
insert into p select generate_series(1,10000), generate_series(1,10000)*3, generate_series(1,10000)%6;

update s set a = r.a from r where r.b = s.b;


------------------------------------------------------------
--	Statement contains correlated subquery
------------------------------------------------------------
update s set b = (select min(a) from r where b = s.b);

delete from s where b = (select min(a) from r where b = s.b);


------------------------------------------------------------
--	Update partition key (requires moving tuples from one partition to another)
------------------------------------------------------------

update p set c = c + 1 where c = 0;

update p set c = c + 1 where b in (select b from s where b = 36);
-- ----------------------------------------------------------------------
-- Test: unsupported_cases3.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion: unsupported cases
--     	Updating the distribution key
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore

create table r (a float4, b float4) distributed by (a);
create table s (a float4, b float4) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

create table p (a float4, b float4, c float4) 
	partition by range (c) (start(1) end(5) every(1), default partition extra);
insert into p select generate_series(1,10000), generate_series(1,10000)*3, generate_series(1,10000)%6;

update s set a = r.a from r where r.b = s.b;


------------------------------------------------------------
--	Statement contains correlated subquery
------------------------------------------------------------
update s set b = (select min(a) from r where b = s.b);

delete from s where b = (select min(a) from r where b = s.b);


------------------------------------------------------------
--	Update partition key (requires moving tuples from one partition to another)
------------------------------------------------------------
update p set c = c + 1 where c = 0;

update p set c = c + 1 where b in (select b from s) and c = 0;
-- ----------------------------------------------------------------------
-- Test: unsupported_cases4.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion: unsupported cases
--     	Updating the distribution key
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore

create table r (a float(24), b float(24)) distributed by (a);
create table s (a float(24), b float(24)) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

create table p (a float(24), b float(24), c float(24)) 
	partition by range (c) (start(1) end(5) every(1), default partition extra);
insert into p select generate_series(1,10000), generate_series(1,10000)*3, generate_series(1,10000)%6;

update s set a = r.a from r where r.b = s.b;


------------------------------------------------------------
--	Statement contains correlated subquery
------------------------------------------------------------
update s set b = (select min(a) from r where b = s.b);

delete from s where b = (select min(a) from r where b = s.b);


------------------------------------------------------------
--	Update partition key (requires moving tuples from one partition to another)
------------------------------------------------------------
update p set c = c + 1 where c = 0;

update p set c = c + 1 where b in (select b from s) and c = 0;


-- ----------------------------------------------------------------------
-- Test: unsupported_cases5.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion: unsupported cases
--     	Updating the distribution key
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore

create table r (a float(53), b float(53)) distributed by (a);
create table s (a float(53), b float(53)) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 4;

create table p (a float(53), b float(53), c float(53)) 
	partition by range (c) (start(1) end(5) every(1), default partition extra);
insert into p select generate_series(1,10000), generate_series(1,10000)*3, generate_series(1,10000)%6;

update s set a = r.a from r where r.b = s.b;


------------------------------------------------------------
--	Statement contains correlated subquery
------------------------------------------------------------
update s set b = (select min(a) from r where b = s.b);

delete from s where b = (select min(a) from r where b = s.b);


------------------------------------------------------------
--	Update partition key (requires moving tuples from one partition to another)
------------------------------------------------------------
update p set c = c + 1 where c = 0;

update p set c = c + 1 where b in (select b from s) and c = 0;


-- ----------------------------------------------------------------------
-- Test: partition_motion0.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore
create table r (a int, b int) distributed by (a);
create table s (a int, b int) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 3;

create table p (a int, b int, c int)
	distributed by (a)
        partition by range (c) (start(1) end(5) every(1), default partition extra);
	
insert into p select generate_series(1,10000), generate_series(1,10000) * 3, generate_series(1,10000) % 6;


------------------------------------------------------------
-- Update with Motion:
--	Motion on p, append node, hash agg
------------------------------------------------------------
update p set b = b + 1 where a in (select b from r where a = p.c);

delete from p where p.a in (select b from r where a = p.c);

delete from p using r where p.a = r.b and r.a = p.c;


------------------------------------------------------------
-- Updates with motion:
-- 	No motion, colocated distribution key
------------------------------------------------------------
delete from p where a in (select a from r where a = p.c);

delete from p using r where p.a = r.a and r.a = p.c;


------------------------------------------------------------
-- 	No motion of s
------------------------------------------------------------
delete from s where a in (select a from p where p.b = s.b);

select count(*) from s;
select * from s;
delete from s where b in (select a from p where p.c = s.b);

-- ----------------------------------------------------------------------
-- Test: partition_motion1.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore
create table r (a int4, b int4) distributed by (a);
create table s (a int4, b int4) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 3;

create table p (a int4, b int4, c int4)
	distributed by (a)
        partition by range (c) (start(1) end(5) every(1), default partition extra);
	
insert into p select generate_series(1,10000), generate_series(1,10000) * 3, generate_series(1,10000) % 6;


------------------------------------------------------------
-- Update with Motion:
--	Motion on p, append node, hash agg
------------------------------------------------------------
update p set b = b + 1 where a in (select b from r where a = p.c);

delete from p where p.a in (select b from r where a = p.c);

delete from p using r where p.a = r.b and r.a = p.c;


------------------------------------------------------------
-- Updates with motion:
-- 	No motion, colocated distribution key
------------------------------------------------------------
delete from p where a in (select a from r where a = p.c);

delete from p using r where p.a = r.a and r.a = p.c;


------------------------------------------------------------
-- 	No motion of s
------------------------------------------------------------
delete from s where a in (select a from p where p.b = s.b);

select count(*) from s;
select * from s;
delete from s where b in (select a from p where p.c = s.b);


-- ----------------------------------------------------------------------
-- Test: partition_motion2.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore
create table r (a int8, b int8) distributed by (a);
create table s (a int8, b int8) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 3;

create table p (a int8, b int8, c int8)
	distributed by (a)
        partition by range (c) (start(1) end(5) every(1), default partition extra);
	
insert into p select generate_series(1,10000), generate_series(1,10000) * 3, generate_series(1,10000) % 6;


------------------------------------------------------------
-- Update with Motion:
--	Motion on p, append node, hash agg
------------------------------------------------------------
update p set b = b + 1 where a in (select b from r where a = p.c);

delete from p where p.a in (select b from r where a = p.c);

delete from p using r where p.a = r.b and r.a = p.c;


------------------------------------------------------------
-- Updates with motion:
-- 	No motion, colocated distribution key
------------------------------------------------------------
delete from p where a in (select a from r where a = p.c);

delete from p using r where p.a = r.a and r.a = p.c;


------------------------------------------------------------
-- 	No motion of s
------------------------------------------------------------
delete from s where a in (select a from p where p.b = s.b);

select count(*) from s;
select * from s;
delete from s where b in (select a from p where p.c = s.b);


-- ----------------------------------------------------------------------
-- Test: partition_motion3.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore
create table r (a float4, b float4) distributed by (a);
create table s (a float4, b float4) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 3;

create table p (a float4, b float4, c float4)
	distributed by (a)
        partition by range (c) (start(1) end(5) every(1), default partition extra);
	
insert into p select generate_series(1,10000), generate_series(1,10000) * 3, generate_series(1,10000) % 6;


------------------------------------------------------------
-- Update with Motion:
--	Motion on p, append node, hash agg
------------------------------------------------------------
update p set b = b + 1 where a in (select b from r where a = p.c);

delete from p where p.a in (select b from r where a = p.c);

delete from p using r where p.a = r.b and r.a = p.c;


------------------------------------------------------------
-- Updates with motion:
-- 	No motion, colocated distribution key
------------------------------------------------------------
delete from p where a in (select a from r where a = p.c);

delete from p using r where p.a = r.a and r.a = p.c;


------------------------------------------------------------
-- 	No motion of s
------------------------------------------------------------
delete from s where a in (select a from p where p.b = s.b);

select count(*) from s;
select * from s;
delete from s where b in (select a from p where p.c = s.b);


-- ----------------------------------------------------------------------
-- Test: partition_motion4.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore
create table r (a float(24), b float(24)) distributed by (a);
create table s (a float(24), b float(24)) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 3;

create table p (a float(24), b float(24), c float(24))
	distributed by (a)
        partition by range (c) (start(1) end(5) every(1), default partition extra);
	
insert into p select generate_series(1,10000), generate_series(1,10000) * 3, generate_series(1,10000) % 6;


------------------------------------------------------------
-- Update with Motion:
--	Motion on p, append node, hash agg
------------------------------------------------------------
update p set b = b + 1 where a in (select b from r where a = p.c);

delete from p where p.a in (select b from r where a = p.c);

delete from p using r where p.a = r.b and r.a = p.c;


------------------------------------------------------------
-- Updates with motion:
-- 	No motion, colocated distribution key
------------------------------------------------------------
delete from p where a in (select a from r where a = p.c);

delete from p using r where p.a = r.a and r.a = p.c;


------------------------------------------------------------
-- 	No motion of s
------------------------------------------------------------
delete from s where a in (select a from p where p.b = s.b);

select count(*) from s;
select * from s;
delete from s where b in (select a from p where p.c = s.b);


-- ----------------------------------------------------------------------
-- Test: partition_motion5.sql
-- ----------------------------------------------------------------------

------------------------------------------------------------
-- Update with Motion:
------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
-- end_ignore
create table r (a float(53), b float(53)) distributed by (a);
create table s (a float(53), b float(53)) distributed by (a);
insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 3;

create table p (a float(53), b float(53), c float(53))
	distributed by (a)
        partition by range (c) (start(1) end(5) every(1), default partition extra);
	
insert into p select generate_series(1,10000), generate_series(1,10000) * 3, generate_series(1,10000) % 6;


------------------------------------------------------------
-- Update with Motion:
--	Motion on p, append node, hash agg
------------------------------------------------------------
update p set b = b + 1 where a in (select b from r where a = p.c);

delete from p where p.a in (select b from r where a = p.c);

delete from p using r where p.a = r.b and r.a = p.c;


------------------------------------------------------------
-- Updates with motion:
-- 	No motion, colocated distribution key
------------------------------------------------------------
delete from p where a in (select a from r where a = p.c);

delete from p using r where p.a = r.a and r.a = p.c;


------------------------------------------------------------
-- 	No motion of s
------------------------------------------------------------
delete from s where a in (select a from p where p.b = s.b);

select count(*) from s;
select * from s;
delete from s where b in (select a from p where p.c = s.b);


-- ----------------------------------------------------------------------
-- Test: mpp1070.sql
-- ----------------------------------------------------------------------

--
-- MPP-1070
--
-- start_ignore
DROP TABLE IF EXISTS update_test;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
-- end_ignore

CREATE TABLE update_test (
	e INT DEFAULT 1,
	a INT DEFAULT 10,
	b INT,
	c TEXT
);

INSERT INTO update_test(a,b,c) VALUES (5, 10, 'foo');
INSERT INTO update_test(b,a) VALUES (15, 10);

SELECT a,b,c FROM update_test ORDER BY a,c;
UPDATE update_test SET a = DEFAULT, b = DEFAULT;
SELECT a,b,c FROM update_test ORDER BY a,c;

-- aliases for the UPDATE target table
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;
SELECT a,b,c FROM update_test ORDER BY a,c;

UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;
SELECT a,b,c FROM update_test ORDER BY a,c;

UPDATE update_test SET a=v.i FROM (VALUES(100, 20)) AS v(i, j)
	WHERE update_test.b = v.j;
SELECT a,b,c FROM update_test ORDER BY a,c;


-- ----------------------------------------------
-- Create 2 tables with the same columns, but distributed differently.
CREATE TABLE t1 (id INTEGER, data1 INTEGER, data2 INTEGER) DISTRIBUTED BY (id);
CREATE TABLE t2 (id INTEGER, data1 INTEGER, data2 INTEGER) DISTRIBUTED BY (data1);

INSERT INTO t1 (id, data1, data2) VALUES (1, 1, 1);
INSERT INTO t1 (id, data1, data2) VALUES (2, 2, 2);
INSERT INTO t1 (id, data1, data2) VALUES (3, 3, 3);
INSERT INTO t1 (id, data1, data2) VALUES (4, 4, 4);

INSERT INTO t2 (id, data1, data2) VALUES (1, 2, 101);
INSERT INTO t2 (id, data1, data2) VALUES (2, 1, 102);
INSERT INTO t2 (id, data1, data2) VALUES (3, 4, 103);
INSERT INTO t2 (id, data1, data2) VALUES (4, 3, 104);

-- Now let's try an update that would require us to move info across segments 
-- (depending upon exactly where the data is stored, which will vary depending 
-- upon the number of segments; in my case, I used only 2 segments).
UPDATE t1 SET data2 = t2.data2 FROM t2 WHERE t1.data1 = t2.data1;

SELECT * from t1;


-- ----------------------------------------------------------------------
-- Test: query00.sql
-- ----------------------------------------------------------------------

-- start_ignore
drop table if exists r;
drop table if exists s;
drop table if exists p;
drop table if exists t cascade;
drop table if exists m;

drop table if exists sales cascade;
drop table if exists sales_par cascade;
drop table if exists sales_par2 cascade;
drop table if exists sales_par_CO cascade;

DROP FUNCTION InsertIntoSales(VARCHAR, INTEGER, VARCHAR);
DROP FUNCTION InsertManyIntoSales(INTEGER, VARCHAR);
-- end_ignore

create table r (a int, b int) distributed by (a);
create table s (a int, b int) distributed by (a);
create table m ();
	alter table m add column a int;
	alter table m add column b int;
create table t (region text, id int) distributed by (region);
create table p (a int, b int, c int)
        distributed by (a)
        partition by range (c) (start(1) end(5) every(1), default partition extra);


CREATE TABLE sales (id int, year int, month int, day int, region text)
   DISTRIBUTED BY (id);
-- Create one or more indexes before we insert data.
CREATE INDEX sales_index_on_id ON sales (id);
CREATE INDEX sales_index_on_year ON sales (year);



CREATE TABLE sales_par (id int, year int, month int, day int, region text)
   DISTRIBUTED BY (id)
   PARTITION BY LIST (region)
   (	PARTITION usa VALUES ('usa'),
	PARTITION europe VALUES ('europe'),
	PARTITION asia VALUES ('asia'),
	DEFAULT PARTITION other_regions)
   ;


CREATE TABLE sales_par2 (id int, year int, month int, day int, region text)
   DISTRIBUTED BY (id)
   PARTITION BY RANGE (year)
      SUBPARTITION BY LIST (region)
         SUBPARTITION TEMPLATE (
            SUBPARTITION usa VALUES ('usa'),
            SUBPARTITION europe VALUES ('europe'),
            SUBPARTITION asia VALUES ('asia'),
            DEFAULT SUBPARTITION other_regions)
   (PARTITION year2002 START (2002) INCLUSIVE,
    PARTITION year2003 START (2003) INCLUSIVE,
    PARTITION year2004 START (2004) INCLUSIVE,
    PARTITION year2005 START (2005) INCLUSIVE,
    PARTITION year2006 START (2006) INCLUSIVE
                        END  (2007) EXCLUSIVE)
   ;
-- Add default partition
ALTER TABLE sales_par2 ADD DEFAULT PARTITION yearXXXX;


CREATE TABLE sales_par_CO (id int, year int, month int, day int, region text)
   WITH (APPENDONLY=True, ORIENTATION=column, COMPRESSTYPE='zlib', COMPRESSLEVEL=1)
   DISTRIBUTED BY (id)
   PARTITION BY RANGE (year)
      SUBPARTITION BY LIST (region)
         SUBPARTITION TEMPLATE (
            SUBPARTITION usa VALUES ('usa'),
            SUBPARTITION europe VALUES ('europe'),
            SUBPARTITION asia VALUES ('asia'),
            DEFAULT SUBPARTITION other_regions)
   (PARTITION year2002 START (2002) INCLUSIVE,
    PARTITION year2003 START (2003) INCLUSIVE,
    PARTITION year2004 START (2004) INCLUSIVE,
    PARTITION year2005 START (2005) INCLUSIVE,
    PARTITION year2006 START (2006) INCLUSIVE
    			END  (2007) EXCLUSIVE)
   ;

-- Add default partition
ALTER TABLE sales_par_CO ADD DEFAULT PARTITION yearXXXX
	WITH (APPENDONLY=True, ORIENTATION=column, COMPRESSTYPE='zlib', COMPRESSLEVEL=1);



-- Create a function to insert data.
CREATE FUNCTION insertIntoSales(VARCHAR, INTEGER, VARCHAR)
RETURNS VOID AS
$$
DECLARE
   tablename VARCHAR;
BEGIN
   tablename = $1;
   if (tablename = 'sales')
   	then INSERT INTO sales (id, year, month, day, region)
 		VALUES ($2, 2002 + ($2 % 7), ($2 % 12) + 1, ($2 % 28) + 1, $3);
   elsif (tablename = 'sales_par') 
	then INSERT INTO sales_par (id, year, month, day, region)
 		VALUES ($2, 2002 + ($2 % 7), ($2 % 12) + 1, ($2 % 28) + 1, $3);
   elsif (tablename = 'sales_par2') 
	then INSERT INTO sales_par2 (id, year, month, day, region)
 		VALUES ($2, 2002 + ($2 % 7), ($2 % 12) + 1, ($2 % 28) + 1, $3);
   elsif (tablename = 'sales_par_CO') 
	then INSERT INTO sales_par_CO (id, year, month, day, region)
 		VALUES ($2, 2002 + ($2 % 7), ($2 % 12) + 1, ($2 % 28) + 1, $3);
   end if;
END;
$$ LANGUAGE plpgsql;



CREATE FUNCTION InsertManyIntoSales(INTEGER, VARCHAR)
RETURNS VOID AS
$$
DECLARE
   rowCount INTEGER;
   region VARCHAR;
   tablename VARCHAR;
BEGIN
   rowCount = $1;
   tablename = $2;
   FOR i IN 1 .. rowCount LOOP
      region = 'antarctica';  -- Never actually used.
      IF (i % 4) = 0
         THEN region := 'asia';
      ELSEIF (i % 4) = 1
         THEN region := 'europe';
      ELSEIF (i % 4) = 2
         THEN region := 'usa';
      ELSEIF (i % 4) = 3
         THEN region := 'australia';
      END IF;
      PERFORM insertIntoSales(tablename, i, region );
   END LOOP;
END;
$$ LANGUAGE plpgsql;


--
SELECT InsertManyIntoSales(20,'sales_par_CO');
-- created tables, functions are cleaned up at test99.sql
-- ----------------------------------------------------------------------
-- Test: query01.sql
-- ----------------------------------------------------------------------

-- update/delete requires motion by joining 3 tables
delete from r;
delete from s;
delete from sales;

insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 3;
SELECT InsertManyIntoSales(40,'sales');

ANALYZE r;
ANALYZE s;
ANALYZE sales;

-- index on distribution key
select sales.* from sales,s,r where sales.id = s.b and sales.month = r.b+1;
update sales set month = month+1 from r,s where sales.id = s.b and sales.month = r.b+1;
select sales.* from sales,s,r where sales.id = s.b and sales.month = r.b+2;

select sales.* from sales where id in (select s.b from s, r where s.a = r.b) and day in (select a from r);
update sales set region = 'new_region' where id in (select s.b from s, r where s.a = r.b) and day in (select a from r);
select sales.* from sales where id in (select s.b from s, r where s.a = r.b) and day in (select a from r);

select sales.* from sales where id in (select s.b-1 from s,r where s.a = r.b);
delete from sales where id in (select s.b-1 from s,r where s.a = r.b);
select sales.* from sales where id in (select s.b-1 from s,r where s.a = r.b);

-- no index on distribution key
select s.* from s, r,sales where s.a = r.b and s.b = sales.id;
delete from s using r,sales where s.a = r.b and s.b = sales.id;
select s.* from s, r,sales where s.a = r.b and s.b = sales.id;

select r.* from r,s,sales where s.a = sales.day and sales.month = r.b;
update r set b = r.b + 1 from s,sales where s.a = sales.day and sales.month = r.b;
select r.* from r,s,sales where s.a = sales.day and sales.month = r.b-1;
-- ----------------------------------------------------------------------
-- Test: query02.sql
-- ----------------------------------------------------------------------
-- 3 tables: heap and partition table
delete from r;
delete from s;
delete from sales_par;

insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 3;

SELECT InsertManyIntoSales(20,'sales_par');

-- update partition key
select sales_par.* from sales_par where id in (select s.b from s, r where s.a = r.b) and day in (select a from r);
-- start_ignore
update sales_par set region = 'new_region' where id in (select s.b from s, r where s.a = r.b) and day in (select a from r);
-- end_ignore
select sales_par.* from sales_par where id in (select s.b from s, r where s.a = r.b) and day in (select a from r);

select sales_par.* from sales_par,s,r where sales_par.id = s.b and sales_par.month = r.b+1;
update sales_par set month = month+1 from r,s where sales_par.id = s.b and sales_par.month = r.b+1;
select sales_par.* from sales_par,s,r where sales_par.id = s.b and sales_par.month = r.b+2;

select sales_par.* from sales_par where id in (select s.b-1 from s,r where s.a = r.b);
delete from sales_par where id in (select s.b-1 from s,r where s.a = r.b);
select sales_par.* from sales_par where id in (select s.b-1 from s,r where s.a = r.b);

-- heap table
select s.* from s, r,sales_par where s.a = r.b and s.b = sales_par.id;
delete from s using r,sales_par where s.a = r.b and s.b = sales_par.id;
select s.* from s, r,sales_par where s.a = r.b and s.b = sales_par.id;

-- ----------------------------------------------------------------------
-- Test: query03.sql
-- ----------------------------------------------------------------------
-- 3 tables: heap and 2-level partition CO table + prepare
-- direct dispatch
delete from r;
delete from s;
delete from sales_par2;

insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 3;
SELECT InsertManyIntoSales(20,'sales_par2');

-- partition key
select sales_par2.* from sales_par2,s,r where sales_par2.id = s.b and sales_par2.month = r.b+1;
update sales_par2 set month = month+1 from r,s where sales_par2.id = s.b and sales_par2.month = r.b+1;
select sales_par2.* from sales_par2,s,r where sales_par2.id = s.b and sales_par2.month = r.b+2;

PREPARE plan0 as update sales_par2 set month = month+1 from r,s where sales_par2.id = s.b and sales_par2.month = r.b+2;
EXECUTE plan0;
select sales_par2.* from sales_par2,s,r where sales_par2.id = s.b and sales_par2.month = r.b+3;


select sales_par2.* from sales_par2 where id in (select s.b-1 from s,r where s.a = r.b);
delete from sales_par2 where id in (select s.b-1 from s,r where s.a = r.b);
select sales_par2.* from sales_par2 where id in (select s.b-1 from s,r where s.a = r.b);

-- heap table
select s.* from s, r,sales_par2 where s.a = r.b and s.b = sales_par2.id;
delete from s using r,sales_par2 where s.a = r.b and s.b = sales_par2.id;
select s.* from s, r,sales_par2 where s.a = r.b and s.b = sales_par2.id;
-- ----------------------------------------------------------------------
-- Test: query05.sql
-- ----------------------------------------------------------------------

-- 4 tables: heap, AO, CO tables + duplicate distribution key
delete from r;
delete from s;
drop table if exists s_ao;

insert into r select generate_series(1, 10000), generate_series(1, 10000) * 3;
insert into s select generate_series(1, 100), generate_series(1, 100) * 3;
create table s_ao (a int, b int) with (appendonly = true) distributed by (a);
insert into s_ao select generate_series(1, 100), generate_series(1, 100) * 3;


-- heap table: delete --
select * from r where b in (select month-1 from sales_par_CO, s_ao where sales_par_CO.id = s_ao.b);
delete from r where b in (select month-1 from sales_par_CO, s_ao where sales_par_CO.id = s_ao.b);
select * from r where b in (select month-1 from sales_par_CO, s_ao where sales_par_CO.id = s_ao.b);


-- hdeap table: update: duplicate distribution key -- 
SELECT InsertManyIntoSales(20,'sales_par_CO');

select * from r where a in (select sales_par_CO.id from sales_par_CO, s_ao where sales_par_CO.id = s_ao.b);
update r set b = r.b + 1 where a in (select sales_par_CO.id from sales_par_CO, s_ao where sales_par_CO.id = s_ao.b);
select * from r where a in (select sales_par_CO.id from sales_par_CO, s_ao where sales_par_CO.id = s_ao.b);

-- heap table: delete:
select * from r where a in (select month from sales_par_CO, s_ao, s where sales_par_CO.id = s_ao.b and s_ao.a = s.b);
delete from r where a in (select month from sales_par_CO, s_ao, s where sales_par_CO.id = s_ao.b and s_ao.a = s.b);
select * from r where a in (select month from sales_par_CO, s_ao, s where sales_par_CO.id = s_ao.b and s_ao.a = s.b);
-- ----------------------------------------------------------------------
-- Test: query06.sql
-- ----------------------------------------------------------------------

-- 3 tables: direct dispatch + duplicate distribution key
delete from s;
delete from m;
delete from sales_par;

insert into s select generate_series(1, 100), generate_series(1, 100) * 3;
insert into s select generate_series(1, 10), generate_series(1, 10) * 4;
insert into m select generate_series(1, 1000), generate_series(1, 1000) * 4;
SELECT InsertManyIntoSales(20,'sales_par');

-- heap table: duplicate distribution key: delete --
select * from s where a in (select day from sales_par,m where sales_par.id = m.b);
delete from s where a in (select day from sales_par,m where sales_par.id = m.b);
select * from s where a in (select day from sales_par,m where sales_par.id = m.b);

-- direct dispatch: heap table: update -- 
select * from s where a = 4 and a in (select b from m);
update s set b = b + 1 where a = 4 and a in (select b from m);
select * from s where a = 4 and a in (select b from m);

-- direct dispatch: partitioned table: update --
select distinct sales_par.* from sales_par,s where sales_par.id in (s.b, s.b+1) and region='europe';
update sales_par set month = month+1 from s where sales_par.id in (s.b, s.b+1) and region = 'europe';
select distinct sales_par.* from sales_par,s where sales_par.id in (s.b, s.b+1) and region='europe';

-- direct dispatch: partitioned table: delete --
select * from sales_par where region='asia' and id in (select b from s where a = 1);
delete from sales_par where region='asia' and id in (select b from s where a = 1);
select * from sales_par where region='asia' and id in (select b from s where a = 1);

select * from sales_par where region='asia' and id in (select b from m where a = 2);
delete from sales_par where region='asia' and id in (select b from m where a = 2);
select * from sales_par where region='asia' and id in (select b from m where a = 2);


-- ----------------------------------------------------------------------
-- Test: query08.sql
-- ----------------------------------------------------------------------

-- prepared statement: 3 tables: direct dispatch + duplicate distribution key
delete from s;
delete from m;
delete from sales_par;

insert into s select generate_series(1, 100), generate_series(1, 100) * 3;
insert into s select generate_series(1, 10), generate_series(1, 10) * 4;
insert into m select generate_series(1, 1000), generate_series(1, 1000) * 4;
SELECT InsertManyIntoSales(20,'sales_par');

-- heap table: duplicate distribution key: delete --
select * from s where a in (select day from sales_par,m where sales_par.id = m.b);
PREPARE plan1 AS delete from s where a in (select day from sales_par,m where sales_par.id = m.b);
EXECUTE plan1;
select * from s where a in (select day from sales_par,m where sales_par.id = m.b);

-- direct dispatch: heap table: update -- 
select * from s where a = 4 and a in (select b from m);
PREPARE plan2 AS update s set b = b + 1 where a = 4 and a in (select b from m);
EXECUTE plan2;
select * from s where a = 4 and a in (select b from m);

-- direct dispatch: partitioned table: update --
select distinct sales_par.* from sales_par,s where sales_par.id in (s.b, s.b+1) and region='europe';
PREPARE plan3 AS update sales_par set month = month+1 from s where sales_par.id in (s.b, s.b+1) and region = 'europe';
EXECUTE plan3;
select distinct sales_par.* from sales_par,s where sales_par.id in (s.b, s.b+1) and region='europe';

-- direct dispatch: partitioned table: delete --
select * from sales_par where region='asia' and id in (select b from s where a = 1);
PREPARE plan4 AS delete from sales_par where region='asia' and id in (select b from s where a = 1);
EXECUTE plan4;
select * from sales_par where region='asia' and id in (select b from s where a = 1);

select * from sales_par where region='asia' and id in (select b from m where a = 2);
PREPARE plan5 AS delete from sales_par where region='asia' and id in (select b from m where a = 2);
EXECUTE plan5;
select * from sales_par where region='asia' and id in (select b from m where a = 2);


-- ----------------------------------------------------------------------
-- Test: teardown.sql
-- ----------------------------------------------------------------------
-- start_ignore
drop schema DML_over_joins cascade;
-- end_ignore
