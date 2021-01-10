-- Test using different operator classes in DISTRIBUTED BY.

--
-- Test joins involving tables with distribution keys using non-default
-- hash opclasses.
--

-- For the tests, we define our own equality operator called |=|, which
-- compares the absolute values. For example -1 |=| 1.
CREATE FUNCTION abseq(int, int) RETURNS BOOL AS $$
  begin return abs($1) = abs($2); end;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;
CREATE OPERATOR |=| (
  PROCEDURE = abseq,
  LEFTARG = int,
  RIGHTARG = int,
  COMMUTATOR = |=|,
  hashes, merges);

-- and a hash opclass to back it.
CREATE FUNCTION abshashfunc(int) RETURNS int AS $$
  begin return abs($1); end;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;

CREATE OPERATOR FAMILY abs_int_hash_ops USING hash;

CREATE OPERATOR CLASS abs_int_hash_ops FOR TYPE int4
  USING hash FAMILY abs_int_hash_ops AS
  OPERATOR 1 |=|,
  FUNCTION 1 abshashfunc(int);

-- we need a btree opclass too. Otherwise the planner won't consider
-- collocation of joins.
CREATE FUNCTION abslt(int, int) RETURNS BOOL AS $$
  begin return abs($1) < abs($2); end;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;
CREATE OPERATOR |<| (PROCEDURE = abslt, LEFTARG = int, RIGHTARG = int, hashes);
CREATE FUNCTION absgt(int, int) RETURNS BOOL AS $$
  begin return abs($1) > abs($2); end;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;
CREATE OPERATOR |>| (PROCEDURE = absgt, LEFTARG = int, RIGHTARG = int, hashes);

CREATE FUNCTION abscmp(int, int) RETURNS int AS $$
  begin return btint4cmp(abs($1),abs($2)); end;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;

CREATE OPERATOR CLASS abs_int_btree_ops FOR TYPE int4
  USING btree AS
  OPERATOR 1 |<|,
  OPERATOR 3 |=|,
  OPERATOR 5 |>|,
  FUNCTION 1 abscmp(int, int);

-- Create test tables. At first, use the default opclasses, suitable
-- for the normal = equality operator.
CREATE TABLE abstab_a (a int) DISTRIBUTED BY (a);
INSERT INTO abstab_a VALUES (-1), (0), (1);
CREATE TABLE abstab_b (b int) DISTRIBUTED BY (b);
INSERT INTO abstab_b VALUES (-1), (0), (1), (2);

-- The default opclass isn't helpful with the |=| operator, so this needs
-- a Motion.
-- FIXME: This is currently broken with ORCA, see
-- https://github.com/greenplum-db/gpdb/issues/6796
set optimizer=off;
EXPLAIN (COSTS OFF) SELECT a, b FROM abstab_a, abstab_b WHERE a |=| b;
SELECT a, b FROM abstab_a, abstab_b WHERE a |=| b;
reset optimizer;

-- Change distribution key of abstab_a to use our fancy opclass.
DROP TABLE abstab_a;
CREATE TABLE abstab_a (a int) DISTRIBUTED BY (a abs_int_hash_ops);
INSERT INTO abstab_a VALUES (-1), (0), (1);

-- The other side is still distributed using the default opclass,
-- so we still need a Motion.
EXPLAIN (COSTS OFF) SELECT a, b FROM abstab_a, abstab_b WHERE a |=| b;
SELECT a, b FROM abstab_a, abstab_b WHERE a |=| b;

-- Change distribution policy on abstab_a to match. (Drop and recreate, rather
-- than use ALTER TABLE, so that both CREATE and ALTER TABLE get tested.)
ALTER TABLE abstab_b SET DISTRIBUTED BY (b abs_int_hash_ops);

-- Now we should be able to answer this query without a Redistribute Motion
EXPLAIN (COSTS OFF) SELECT a, b FROM abstab_a, abstab_b WHERE a |=| b;
SELECT a, b FROM abstab_a, abstab_b WHERE a |=| b;

-- On the other hand, a regular equality join now needs a Redistribute Motion
-- (Given the way our hash function is defined, it could actually be used for
-- normal equality too. But the planner has no way to know that.)
EXPLAIN (COSTS OFF) SELECT a, b FROM abstab_a, abstab_b WHERE a = b;
SELECT a, b FROM abstab_a, abstab_b WHERE a = b;

-- Check dependency. The table should be marked as dependent on the operator
-- class, which prevents dropping it.
DROP OPERATOR CLASS abs_int_hash_ops USING hash;

-- With CASCADE, though, the table should be dropped too. (It would be nicer
-- if it just changed the distribution policy to randomly distributed, but
-- that's not how it works currently.)

DROP OPERATOR CLASS abs_int_hash_ops USING hash CASCADE;

-- Recreate the opclass, and a table using it again. This is just so that we
-- leave behind a custom operator class and a table that uses it in the
-- distribution key, so that it gets tested by pg_upgrade tests that run
-- on the regression database, after all the tests.
CREATE OPERATOR CLASS abs_int_hash_ops FOR TYPE int4
  USING hash FAMILY abs_int_hash_ops AS
  OPERATOR 1 |=|,
  FUNCTION 1 abshashfunc(int);

CREATE TABLE abs_opclass_test (i int, t text, j int)
  DISTRIBUTED BY (i abs_int_hash_ops, t, j abs_int_hash_ops);
INSERT INTO abs_opclass_test VALUES
  (-1, 'minus one', 1),
  (0, 'zero', 0),
  (1, 'one', 1);

-- Test deparsing of the non-default opclass
\d abs_opclass_test



--
-- Test scenario, where a type has a hash operator class, but not a default
-- one.
--

-- Doesn't work, because 'point' doesn't have a hash opclass
CREATE TABLE dist_by_point1(p point) DISTRIBUTED BY (p);

-- Neither does the same via ALTER TABLE
CREATE TABLE dist_by_point2(p point) DISTRIBUTED RANDOMLY;
ALTER TABLE dist_by_point2 SET DISTRIBUTED BY (p);

-- But if we create it one, then we can use it.
CREATE FUNCTION pointhashfunc(point) RETURNS int AS $$
  begin return hashfloat8(point_distance(point(0,0), $1)); end;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;

CREATE OPERATOR CLASS point_hash_ops FOR TYPE point
  USING hash AS
  OPERATOR 1 ~=,
  FUNCTION 1 pointhashfunc(point);

-- This still doesn't work, because there's still no *default* opclass.
CREATE TABLE dist_by_point3(p point) DISTRIBUTED BY (p);

CREATE TABLE dist_by_point3(p point) DISTRIBUTED RANDOMLY;
ALTER TABLE dist_by_point3 SET DISTRIBUTED BY (p);

-- But explicitly specifying the opclass works.
CREATE TABLE dist_by_point4(p point) DISTRIBUTED BY (p point_hash_ops);

ALTER TABLE dist_by_point4 SET DISTRIBUTED RANDOMLY;
ALTER TABLE dist_by_point4 SET DISTRIBUTED BY (p point_hash_ops);
