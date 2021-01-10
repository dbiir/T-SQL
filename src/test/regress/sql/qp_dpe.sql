-- Testing Dynamic Partition Elimination

-- ----------------------------------------------------------------------
-- Test: setup.sql
-- ----------------------------------------------------------------------

-- start_ignore
create schema qp_dpe;
set search_path to qp_dpe;
SET datestyle = "ISO, DMY";
-- end_ignore

RESET ALL;

-- ----------------------------------------------------------------------
-- Test: DPE not being applied for tables partitioned by a string column.
-- ----------------------------------------------------------------------

--
-- both sides have varchar(10)
--

-- start_ignore
create language plpythonu;
-- end_ignore

create or replace function count_operator(query text, operator text) returns int as
$$
rv = plpy.execute('EXPLAIN '+ query)
search_text = operator
result = 0
for i in range(len(rv)):
    cur_line = rv[i]['QUERY PLAN']
    if search_text.lower() in cur_line.lower():
        result = result+1
return result
$$
language plpythonu;

-- ----------------------------------------------------------------------
-- Test: DPE not being applied for tables partitioned by a string column
-- ----------------------------------------------------------------------

--
-- both sides have varchar(10)
--
-- start_ignore
drop table if exists foo1;
drop table if exists foo2;

create table foo1 (i int, j varchar(10)) 
partition by list(j)
(partition p1 values('1'), partition p2 values('2'), partition p3 values('3'), partition p4 values('4'), partition p5 values('5'),partition p0 values('0'));

insert into foo1 select i , i%5 || '' from generate_series(1,100) i;

create table foo2 (i int, j varchar(10));
insert into foo2 select i , i ||'' from generate_series(1,2) i;

analyze foo1;
analyze foo2;
-- end_ignore

select count_operator('select count(*) from foo1,foo2 where foo1.j = foo2.j;', 'Append') > 0;
select count_operator('select count(*) from foo1,foo2 where foo1.j = foo2.j;', 'Dynamic Seq Scan') > 0;

select count(*) from foo1,foo2 where foo1.j = foo2.j;

--
-- both sides have text
--
-- start_ignore
drop table if exists foo1;
drop table if exists foo2;

create table foo1 (i int, j text) 
partition by list(j)
(partition p1 values('1'), partition p2 values('2'), partition p3 values('3'), partition p4 values('4'), partition p5 values('5'),partition p0 values('0'));

insert into foo1 select i , i%5 || '' from generate_series(1,100) i;

create table foo2 (i int, j text);
insert into foo2 select i , i ||'' from generate_series(1,2) i;

analyze foo1;
analyze foo2;
-- end_ignore

select count_operator('select count(*) from foo1,foo2 where foo1.j = foo2.j;', 'Append') > 0;
select count_operator('select count(*) from foo1,foo2 where foo1.j = foo2.j;', 'Dynamic Seq Scan') > 0;

select count(*) from foo1,foo2 where foo1.j = foo2.j;

--
-- partition side has text and other varchar(10)
--
-- start_ignore
drop table if exists foo1;
drop table if exists foo2;

create table foo1 (i int, j text) 
partition by list(j)
(partition p1 values('1'), partition p2 values('2'), partition p3 values('3'), partition p4 values('4'), partition p5 values('5'),partition p0 values('0'));

insert into foo1 select i , i%5 || '' from generate_series(1,100) i;

create table foo2 (i int, j varchar(10));
insert into foo2 select i , i ||'' from generate_series(1,2) i;

analyze foo1;
analyze foo2;
-- end_ignore

select count_operator('select count(*) from foo1,foo2 where foo1.j = foo2.j;', 'Append') > 0;
select count_operator('select count(*) from foo1,foo2 where foo1.j = foo2.j;', 'Dynamic Seq Scan') > 0;

select count(*) from foo1,foo2 where foo1.j = foo2.j;

--
-- partition side has varchar(10) and other side text
--
-- start_ignore
drop table if exists foo1;
drop table if exists foo2;

create table foo1 (i int, j varchar(10)) 
partition by list(j)
(partition p1 values('1'), partition p2 values('2'), partition p3 values('3'), partition p4 values('4'), partition p5 values('5'),partition p0 values('0'));

insert into foo1 select i , i%5 || '' from generate_series(1,100) i;

create table foo2 (i int, j text);
insert into foo2 select i , i ||'' from generate_series(1,2) i;

analyze foo1;
analyze foo2;
-- end_ignore

select count_operator('select count(*) from foo1,foo2 where foo1.j = foo2.j;', 'Append') > 0;
select count_operator('select count(*) from foo1,foo2 where foo1.j = foo2.j;', 'Dynamic Seq Scan') > 0;

select count(*) from foo1,foo2 where foo1.j = foo2.j;

--
-- partition side has varchar(20) and other side varchar(10)
--
-- start_ignore
drop table if exists foo1;
drop table if exists foo2;

create table foo1 (i int, j varchar(20)) 
partition by list(j)
(partition p1 values('1'), partition p2 values('2'), partition p3 values('3'), partition p4 values('4'), partition p5 values('5'),partition p0 values('0'));

insert into foo1 select i , i%5 || '' from generate_series(1,100) i;

create table foo2 (i int, j text);
insert into foo2 select i , i ||'' from generate_series(1,2) i;

analyze foo1;
analyze foo2;
-- end_ignore

select count_operator('select count(*) from foo1,foo2 where foo1.j = foo2.j;', 'Append') > 0;
select count_operator('select count(*) from foo1,foo2 where foo1.j = foo2.j;', 'Dynamic Seq Scan') > 0;

select count(*) from foo1,foo2 where foo1.j = foo2.j;

RESET ALL;

-- start_ignore
drop table if exists foo1;
drop table if exists foo2;

create table foo1 (i int, j varchar(10))
partition by list(j)
(partition p1 values('1'), partition p2 values('2'), partition p3 values('3'), partition p4 values('4'), partition p5 values('5'),partition p0 values('0'));

insert into foo1 select i , i%5 || '' from generate_series(1,100) i;

create table foo2 (i int, j varchar(10));
insert into foo2 select i , i ||'' from generate_series(1,2) i;

analyze foo1;
analyze foo2;
-- end_ignore

-- ----------------------------------------------------------------------
-- Should not apply DPE when the inner side has a subplan
-- ----------------------------------------------------------------------

select count_operator('select count(*) from foo1,foo2 where foo1.j =foo2.j;', 'Append') > 0;
select count_operator('select count(*) from foo1,foo2 where foo1.j =foo2.j;', 'Dynamic Seq Scan') > 0;

select count_operator('select count(*) from foo1,foo2 where foo1.j =foo2.j and foo2.i <= ALL(select 1 UNION select 2);', 'Dynamic Seq Scan') > 0;

select count(*) from foo1,foo2 where foo1.j =foo2.j and foo2.i <= ALL(select 1 UNION select 2);
RESET ALL;
