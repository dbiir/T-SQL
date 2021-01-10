set optimizer_print_missing_stats = off;
--
-- Cursor regression tests
--
BEGIN;

DECLARE foo1 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

DECLARE foo2 CURSOR FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

DECLARE foo3 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

DECLARE foo4 CURSOR FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

DECLARE foo5 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

DECLARE foo6 CURSOR FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

DECLARE foo7 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

DECLARE foo8 CURSOR FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

DECLARE foo9 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

DECLARE foo10 CURSOR FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

DECLARE foo11 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

DECLARE foo12 CURSOR FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

DECLARE foo13 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

DECLARE foo14 CURSOR FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

DECLARE foo15 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

DECLARE foo16 CURSOR FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

DECLARE foo17 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

DECLARE foo18 CURSOR FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

DECLARE foo19 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

DECLARE foo20 CURSOR FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

DECLARE foo21 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

DECLARE foo22 CURSOR FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

DECLARE foo23 SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

FETCH 1 in foo1;

FETCH 2 in foo2;

FETCH 3 in foo3;

FETCH 4 in foo4;

FETCH 5 in foo5;

FETCH 6 in foo6;

FETCH 7 in foo7;

FETCH 8 in foo8;

FETCH 9 in foo9;

FETCH 10 in foo10;

FETCH 11 in foo11;

FETCH 12 in foo12;

FETCH 13 in foo13;

FETCH 14 in foo14;

FETCH 15 in foo15;

FETCH 16 in foo16;

FETCH 17 in foo17;

FETCH 18 in foo18;

FETCH 19 in foo19;

FETCH 20 in foo20;

FETCH 21 in foo21;

FETCH 22 in foo22;

FETCH 23 in foo23;


CLOSE foo1;

CLOSE foo2;

CLOSE foo3;

CLOSE foo4;

CLOSE foo5;

CLOSE foo6;

CLOSE foo7;

CLOSE foo8;

CLOSE foo9;

CLOSE foo10;

CLOSE foo11;

CLOSE foo12;

-- leave some cursors open, to test that auto-close works.

-- record this in the system view as well (don't query the time field there
-- however)
SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors ORDER BY 1;

END;

SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors;

--
-- NO SCROLL disallows backward fetching
--

BEGIN;

DECLARE foo24 NO SCROLL CURSOR FOR SELECT * FROM tenk1 ORDER BY unique2;

FETCH 1 FROM foo24;

FETCH BACKWARD 1 FROM foo24; -- should fail

END;

--
-- Cursors outside transaction blocks
--


SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors;

BEGIN;

DECLARE foo25 CURSOR WITH HOLD FOR SELECT * FROM tenk2 ORDER BY 1,2,3,4;

FETCH FROM foo25;

FETCH FROM foo25;

COMMIT;

FETCH FROM foo25;

--FETCH BACKWARD FROM foo25;

--FETCH ABSOLUTE -1 FROM foo25;

SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors;

CLOSE foo25;

--
-- ROLLBACK should close holdable cursors
--

BEGIN;

DECLARE foo26 CURSOR WITH HOLD FOR SELECT * FROM tenk1 ORDER BY unique2;

ROLLBACK;

-- should fail
FETCH FROM foo26;

--
-- Parameterized DECLARE needs to insert param values into the cursor portal
--

BEGIN;

CREATE FUNCTION declares_cursor(text)
   RETURNS void
   AS 'DECLARE c CURSOR FOR SELECT stringu1 FROM tenk1 WHERE stringu1 LIKE $1;'
   LANGUAGE SQL READS SQL DATA;

SELECT declares_cursor('AB%');

FETCH ALL FROM c;

ROLLBACK;

--
-- Test behavior of both volatile and stable functions inside a cursor;
-- in particular we want to see what happens during commit of a holdable
-- cursor
--
create temp table tt1(f1 int);

create function count_tt1_v() returns int8 as
'select count(*) from tt1' language sql volatile READS SQL DATA;

create function count_tt1_s() returns int8 as
'select count(*) from tt1' language sql stable READS SQL DATA;

begin;

insert into tt1 values(1);

declare c1 cursor for select count_tt1_v(), count_tt1_s();

insert into tt1 values(2);

-- fetch all from c1; -- DISABLED: see open JIRA MPP-835

rollback;

begin;

insert into tt1 values(1);

declare c2 cursor with hold for select count_tt1_v(), count_tt1_s();

insert into tt1 values(2);

commit;

delete from tt1;

-- fetch all from c2; -- DISABLED: see open JIRA MPP-835

drop function count_tt1_v();
drop function count_tt1_s();


-- Create a cursor with the BINARY option and check the pg_cursors view
BEGIN;
SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors;
DECLARE bc BINARY CURSOR FOR SELECT * FROM tenk1;
SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors;
ROLLBACK;

-- We should not see the portal that is created internally to
-- implement EXECUTE in pg_cursors
PREPARE cprep AS
  SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors;
EXECUTE cprep;

-- test CLOSE ALL;
SELECT name FROM pg_cursors ORDER BY 1;
CLOSE ALL;
SELECT name FROM pg_cursors ORDER BY 1;
BEGIN;
DECLARE foo1 CURSOR WITH HOLD FOR SELECT 1;
DECLARE foo2 CURSOR WITHOUT HOLD FOR SELECT 1;
SELECT name FROM pg_cursors ORDER BY 1;
CLOSE ALL;
SELECT name FROM pg_cursors ORDER BY 1;
COMMIT;

--
-- Tests for updatable cursors
--

-- In GPDB, we use a dummy column as distribution key, so that all the
-- rows land on the same segment. Otherwise the order the cursor returns
-- the rows is unstable.
CREATE TEMP TABLE uctest(f1 int, f2 text, distkey text) distributed by (distkey);
INSERT INTO uctest VALUES (1, 'one'), (2, 'two'), (3, 'three');
SELECT f1, f2 FROM uctest;

-- Check DELETE WHERE CURRENT
BEGIN;
DECLARE c1 CURSOR FOR SELECT f1, f2 FROM uctest;
FETCH 2 FROM c1;
DELETE FROM uctest WHERE CURRENT OF c1;
-- should show deletion
SELECT f1, f2 FROM uctest;
-- cursor did not move
FETCH ALL FROM c1;
-- cursor is insensitive
--MOVE BACKWARD ALL IN c1; -- backwards scans not supported in GPDB
--FETCH ALL FROM c1;
COMMIT;
-- should still see deletion
SELECT f1, f2 FROM uctest;

-- Check UPDATE WHERE CURRENT; this time use FOR UPDATE
BEGIN;
DECLARE c1 CURSOR FOR SELECT f1, f2 FROM uctest FOR UPDATE;
FETCH c1;
UPDATE uctest SET f1 = 8 WHERE CURRENT OF c1;
SELECT f1, f2 FROM uctest;
COMMIT;
SELECT f1, f2 FROM uctest;

-- Check repeated-update and update-then-delete cases
BEGIN;
DECLARE c1 CURSOR FOR SELECT f1, f2 FROM uctest;
FETCH c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
SELECT f1, f2 FROM uctest;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1; -- currently broken on GPDB! (does nothing)
SELECT f1, f2 FROM uctest;
-- insensitive cursor should not show effects of updates or deletes
--FETCH RELATIVE 0 FROM c1;
DELETE FROM uctest WHERE CURRENT OF c1; -- currently broken on GPDB! (does nothing)
SELECT f1, f2 FROM uctest;
DELETE FROM uctest WHERE CURRENT OF c1; -- no-op
SELECT f1, f2 FROM uctest;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1; -- no-op
SELECT f1, f2 FROM uctest;
--FETCH RELATIVE 0 FROM c1;
ROLLBACK;
SELECT f1, f2 FROM uctest;

BEGIN;
DECLARE c1 CURSOR FOR SELECT f1, f2 FROM uctest FOR UPDATE;
FETCH c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
SELECT f1, f2 FROM uctest;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1; -- currently broken on GPDB! (does nothing)
SELECT f1, f2 FROM uctest;
DELETE FROM uctest WHERE CURRENT OF c1; -- currently broken on GPDB! (does nothing)
SELECT f1, f2 FROM uctest;
DELETE FROM uctest WHERE CURRENT OF c1; -- no-op
SELECT f1, f2 FROM uctest;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1; -- no-op
SELECT f1, f2 FROM uctest;
--- sensitive cursors can't currently scroll back, so this is an error:
FETCH RELATIVE 0 FROM c1;
ROLLBACK;
SELECT f1, f2 FROM uctest;

-- Check inheritance cases
CREATE TEMP TABLE ucchild () inherits (uctest);
INSERT INTO ucchild values(100, 'hundred');
SELECT f1, f2 FROM uctest;

BEGIN;
DECLARE c1 CURSOR FOR SELECT f1, f2 FROM uctest FOR UPDATE;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
FETCH 1 FROM c1;
COMMIT;
SELECT f1, f2 FROM uctest;

-- Can update from a self-join, but only if FOR UPDATE says which to use
BEGIN;
DECLARE c1 CURSOR FOR SELECT * FROM uctest a, uctest b WHERE a.f1 = b.f1 + 5;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;  -- fail
ROLLBACK;
BEGIN;
DECLARE c1 CURSOR FOR SELECT * FROM uctest a, uctest b WHERE a.f1 = b.f1 + 5 FOR UPDATE;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;  -- fail
ROLLBACK;
BEGIN;
DECLARE c1 CURSOR FOR SELECT * FROM uctest a, uctest b WHERE a.f1 = b.f1 + 5 FOR SHARE OF a;
FETCH 1 FROM c1;
UPDATE uctest SET f1 = f1 + 10 WHERE CURRENT OF c1;
SELECT * FROM uctest;
ROLLBACK;

-- Check various error cases

DELETE FROM uctest WHERE CURRENT OF c1;  -- fail, no such cursor
DECLARE cx CURSOR WITH HOLD FOR SELECT * FROM uctest;
DELETE FROM uctest WHERE CURRENT OF cx;  -- fail, can't use held cursor
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM tenk2;
DELETE FROM uctest WHERE CURRENT OF c;  -- fail, cursor on wrong table
ROLLBACK;
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM tenk2 FOR SHARE;
DELETE FROM uctest WHERE CURRENT OF c;  -- fail, cursor on wrong table
ROLLBACK;
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM tenk1 JOIN tenk2 USING (unique1);
DELETE FROM tenk1 WHERE CURRENT OF c;  -- fail, cursor is on a join
ROLLBACK;
BEGIN;
DECLARE c CURSOR FOR SELECT f1,count(*) FROM uctest GROUP BY f1;
DELETE FROM uctest WHERE CURRENT OF c;  -- fail, cursor is on aggregation
ROLLBACK;
BEGIN;
DECLARE c1 CURSOR FOR SELECT * FROM uctest;
DELETE FROM uctest WHERE CURRENT OF c1; -- fail, no current row
ROLLBACK;
BEGIN;
DECLARE c1 CURSOR FOR SELECT MIN(f1) FROM uctest FOR UPDATE;
ROLLBACK;

-- WHERE CURRENT OF may someday work with views, but today is not that day.
-- For now, just make sure it errors out cleanly.
CREATE TEMP VIEW ucview AS SELECT f1, f2 FROM uctest;
CREATE RULE ucrule AS ON DELETE TO ucview DO INSTEAD
  DELETE FROM uctest WHERE f1 = OLD.f1;
BEGIN;
DECLARE c1 CURSOR FOR SELECT * FROM ucview;
FETCH FROM c1;
DELETE FROM ucview WHERE CURRENT OF c1; -- fail, views not supported
ROLLBACK;

-- Check cursors for functions.
BEGIN;
DECLARE c1 CURSOR FOR SELECT * FROM LOWER('TEST');
FETCH ALL FROM c1;
COMMIT;
-- Check WHERE CURRENT OF with an index-only scan
BEGIN;
EXPLAIN (costs off)
DECLARE c1 CURSOR FOR SELECT stringu1 FROM onek WHERE stringu1 = 'DZAAAA';
DECLARE c1 CURSOR FOR SELECT stringu1 FROM onek WHERE stringu1 = 'DZAAAA';
FETCH FROM c1;
DELETE FROM onek WHERE CURRENT OF c1;
SELECT stringu1 FROM onek WHERE stringu1 = 'DZAAAA';
ROLLBACK;

-- start_ignore
-- ignore the block, because cursor can only scan forward
-- Check behavior with rewinding to a previous child scan node,
-- as per bug #15395
BEGIN;
CREATE TABLE current_check (currentid int, payload text);
CREATE TABLE current_check_1 () INHERITS (current_check);
CREATE TABLE current_check_2 () INHERITS (current_check);
INSERT INTO current_check_1 SELECT i, 'p' || i FROM generate_series(1,9) i;
INSERT INTO current_check_2 SELECT i, 'P' || i FROM generate_series(10,19) i;

DECLARE c1 SCROLL CURSOR FOR SELECT * FROM current_check;

-- This tests the fetch-backwards code path
FETCH ABSOLUTE 12 FROM c1;
FETCH ABSOLUTE 8 FROM c1;
DELETE FROM current_check WHERE CURRENT OF c1 RETURNING *;

-- This tests the ExecutorRewind code path
FETCH ABSOLUTE 13 FROM c1;
FETCH ABSOLUTE 1 FROM c1;
DELETE FROM current_check WHERE CURRENT OF c1 RETURNING *;

SELECT * FROM current_check;
ROLLBACK;
-- end_ignore

-- Make sure snapshot management works okay, per bug report in
-- 235395b90909301035v7228ce63q392931f15aa74b31@mail.gmail.com

-- GPDB_90_MERGE_FIXME: This doesn't work correctly. Two issues:
-- 1. In GPDB, an UPDATE, or FOR UPDATE, locks the whole table. Because of
--    that, there cannot be concurrent updates, and we don't bother with
--    LockRows nodes in FOR UPDATE plans. However, in the upstream, the
--    LockRows node also handles fetching the latest tuple version, if it
--    was updated in the same transaction, by a *later* command.
--
-- 2. Even if we had LockRows in the plan, it still wouldn't work, at least
--    not always. In PostgreSQL, the LockRows node checks the visibility
--    when a row is FETCHed. Not before that. So if a row is UPDATEd in
--    the same transaction, before it's FETCHed, the FETCH is supposed to
--    see the effects of the UPDATE. In GPDB, however, a cursor starts
--    executing in the segments, as soon as the DECLARE CURSOR is issued,
--    so there's a race condition.

BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
CREATE TABLE cursor (a int, b int);
INSERT INTO cursor VALUES (1, 1);
DECLARE c1 NO SCROLL CURSOR FOR SELECT * FROM cursor FOR UPDATE;
UPDATE cursor SET b = 2;
FETCH ALL FROM c1;
COMMIT;
DROP TABLE cursor;

-- Check rewinding a cursor containing a stable function in LIMIT,
-- per bug report in 8336843.9833.1399385291498.JavaMail.root@quick

-- GPDB: ignore the result of the FETCH, because the order the rows
-- arrive from the segments is arbitrary in GPDB. This test isn't
-- very useful in GPDB anyway, as the bug that this was testing
-- happened when rewinding the cursor, and GPDB doesn't support
-- MOVE BACKWARD at all. But doesn't hurt to keep it to the extent
-- we can, I guess..
begin;
create function nochange(int) returns int
  as 'select $1 limit 1' language sql stable;
declare c cursor for select * from int8_tbl limit nochange(3);
-- start_ignore
fetch all from c;
-- end_ignore
move backward all in c;
fetch all from c;
rollback;
