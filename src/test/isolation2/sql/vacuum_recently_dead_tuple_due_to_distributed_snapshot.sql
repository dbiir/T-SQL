-- Test that VACUUM in utility mode doesn't remove tuples that are still
-- visible to a distributed snapshot, even if there is no corresponding
-- local snapshot in a QE node yet.
create table test_recently_dead_utility(a int);

insert into test_recently_dead_utility select g from generate_series(1, 100) g;

-- A function that:
--
-- 1. Establishes a snapshot, without launching any QE backends. It does this
--    by selecting from pg_database.
-- 2. Sleeps 10 seconds
-- 3. runs "select count(*) from test_recently_dead_utility"
--
-- It would've been more clear to use a transaction block, "BEGIN; ... COMMIT"
-- for these steps. However, BEGIN immediately starts connections to the QE
-- nodes, and we don't want to acquire local snapshots in the QE nodes, until
-- step 3. Instead, run these steps in a function that's marked as STABLE.
-- (A VOLATILE function would acquire a different snapshot for each command
-- in the function, but we want it all to run with a single snapshot.)
--
-- (All on one line because of limitations in the isolation2 test language.)

create or replace function afunc() returns int4 as $$ declare c int; begin select count(*) into c from pg_Database; perform pg_sleep(10); select count(*) into c from test_recently_dead_utility; return c; end; $$ language plpgsql stable;

-- In one session, launch afunc().
1&: select afunc();

-- Meanwhile, delete some rows from foo. These rows should still be visible to
-- afunc(). (But first sleep a bit to make sure afunc() has established a
-- snapshot before the delete.)
2: select pg_sleep(1);
2: delete from test_recently_dead_utility;

-- Run VACUUM in utility mode. It should *not* remove the deleted tuples yet.
0U: vacuum verbose test_recently_dead_utility;

-- Continue afunc().  It should see all the rows in the table, ie. 100 rows.
1<:
