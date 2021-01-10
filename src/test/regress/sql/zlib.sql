-- Test temporary file compression.
--
-- The test file is called 'zlib' for historical reasons. GPDB uses Zstandard
-- rather than zlib for temporary file compression, nowadays.

-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
-- end_ignore

-- If the server is built without libzstd (configure --without-zstd), this
-- fails with error "workfile compresssion is not supported by this build".
-- The tests are less interesting in that case, but they should still pass.
-- So use a gpdiff rule to ignore that error:
--
-- start_matchignore
-- m/ERROR:  workfile compresssion is not supported by this build/
-- end_matchignore
SET gp_workfile_compression = on;

DROP TABLE IF EXISTS test_zlib_hashjoin;
CREATE TABLE test_zlib_hashjoin (i1 int, i2 int, i3 int, i4 int, i5 int, i6 int, i7 int, i8 int) WITH (APPENDONLY=true) DISTRIBUTED BY (i1) ; 
INSERT INTO test_zlib_hashjoin SELECT i,i,i,i,i,i,i,i FROM 
	(select generate_series(1, nsegments * 333333) as i from 
	(select count(*) as nsegments from gp_segment_configuration where role='p' and content >= 0) foo) bar;

SET statement_mem=5000;

--Fail after workfile creation and before add it to workfile set
select gp_inject_fault('workfile_creation_failure', 'reset', 2);
select gp_inject_fault('workfile_creation_failure', 'error', 2);

SELECT COUNT(t1.*) FROM test_zlib_hashjoin AS t1, test_zlib_hashjoin AS t2 WHERE t1.i1=t2.i2;

select gp_inject_fault('workfile_creation_failure', 'status', 2);

RESET statement_mem;
DROP TABLE IF EXISTS test_zlib_hagg; 
CREATE TABLE test_zlib_hagg (i1 int, i2 int, i3 int, i4 int);
INSERT INTO test_zlib_hagg SELECT i,i,i,i FROM 
	(select generate_series(1, nsegments * 300000) as i from 
	(select count(*) as nsegments from gp_segment_configuration where role='p' and content >= 0) foo) bar;

SET statement_mem=2000;

--Fail after workfile creation and before add it to workfile set
select gp_inject_fault('workfile_creation_failure', 'reset', 2);
select gp_inject_fault('workfile_creation_failure', 'error', 2);

SELECT MAX(i1) FROM test_zlib_hagg GROUP BY i2;

select gp_inject_fault('workfile_creation_failure', 'status', 2);

-- Reset faultinjectors
select gp_inject_fault('workfile_creation_failure', 'reset', 2);

create table test_zlib (i int, j text);
insert into test_zlib select i, i from generate_series(1,1000000) as i;
create table test_zlib_t1(i int, j int);

set statement_mem='10MB';

create or replace function FuncA()
returns void as
$body$
begin
 	insert into test_zlib values(2387283, 'a');
 	insert into test_zlib_t1 values(1, 2);
    CREATE TEMP table TMP_Q_QR_INSTM_ANL_01 WITH(APPENDONLY=true,COMPRESSLEVEL=5,ORIENTATION=row,COMPRESSTYPE=zlib) on commit drop as
    SELECT t1.i from test_zlib as t1 join test_zlib as t2 on t1.i = t2.i;
EXCEPTION WHEN others THEN
 -- do nothing
end
$body$ language plpgsql;

-- Inject fault before we close workfile in ExecHashJoinNewBatch
select gp_inject_fault('workfile_creation_failure', 'reset', 2);
select gp_inject_fault('workfile_creation_failure', 'error', 2);

select FuncA();
select * from test_zlib_t1;

select gp_inject_fault('workfile_creation_failure', 'status', 2);

drop function FuncA();
drop table test_zlib;
drop table test_zlib_t1;

select gp_inject_fault('workfile_creation_failure', 'reset', 2);
