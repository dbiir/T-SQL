--
-- Test queries mixes window functions with aggregate functions or grouping.
--
DROP TABLE IF EXISTS test_group_window;

CREATE TABLE test_group_window(c1 int, c2 int);

WITH tt AS (SELECT * FROM test_group_window)
SELECT tt.c1, COUNT() over () as fraction
FROM tt
GROUP BY tt.c1
ORDER BY tt.c1;

DROP TABLE test_group_window;

--
-- Set up
--
CREATE TABLE bfv_cte_foo AS SELECT i as a, i+1 as b from generate_series(1,10)i;
CREATE TABLE bfv_cte_bar AS SELECT i as c, i+1 as d from generate_series(1,10)i;

--
-- Test with CTE inlining disabled
--
set optimizer_cte_inlining = off;

--
-- With clause select test 1
--
WITH t AS
(
 SELECT e.*,f.*
 FROM
    (
      SELECT * FROM bfv_cte_foo WHERE a < 10
    ) e
 LEFT OUTER JOIN
    (
       SELECT * FROM bfv_cte_bar WHERE c < 10
    ) f

  ON e.a = f.d )
SELECT t.a,t.d, count(*) over () AS window
FROM t
GROUP BY t.a,t.d ORDER BY t.a,t.d LIMIT 2;

--
-- With clause select test 2
--
WITH t(a,b,d) AS
(
  SELECT bfv_cte_foo.a,bfv_cte_foo.b,bfv_cte_bar.d FROM bfv_cte_foo,bfv_cte_bar WHERE bfv_cte_foo.a = bfv_cte_bar.d
)
SELECT t.b,avg(t.a), rank() OVER (PARTITION BY t.a ORDER BY t.a) FROM bfv_cte_foo,t GROUP BY bfv_cte_foo.a,bfv_cte_foo.b,t.b,t.a ORDER BY 1,2,3 LIMIT 5;

--
-- With clause select test 3
--
WITH t(a,b,d) AS
(
  SELECT bfv_cte_foo.a,bfv_cte_foo.b,bfv_cte_bar.d FROM bfv_cte_foo,bfv_cte_bar WHERE bfv_cte_foo.a = bfv_cte_bar.d
)
SELECT cup.*, SUM(t.d) OVER(PARTITION BY t.b) FROM
  (
    SELECT bfv_cte_bar.*, AVG(t.b) OVER(PARTITION BY t.a ORDER BY t.b desc) AS e FROM t,bfv_cte_bar
  ) AS cup,
t WHERE cup.e < 10
GROUP BY cup.c,cup.d, cup.e ,t.d, t.b
ORDER BY 1,2,3,4
LIMIT 10;

--
-- With clause select test 4
--
WITH t(a,b,d) AS
(
  SELECT bfv_cte_foo.a,bfv_cte_foo.b,bfv_cte_bar.d FROM bfv_cte_foo,bfv_cte_bar WHERE bfv_cte_foo.a = bfv_cte_bar.d
)
SELECT cup.*, SUM(t.d) FROM
  (
    SELECT bfv_cte_bar.*, count(*) OVER() AS e FROM t,bfv_cte_bar WHERE t.a = bfv_cte_bar.c
  ) AS cup,
t GROUP BY cup.c,cup.d, cup.e,t.a
HAVING AVG(t.d) < 10 ORDER BY 1,2,3,4 LIMIT 10;

--
-- With clause select test 5
--
WITH t(a,b,d) AS
(
  SELECT bfv_cte_foo.a,bfv_cte_foo.b,bfv_cte_bar.d FROM bfv_cte_foo,bfv_cte_bar WHERE bfv_cte_foo.a = bfv_cte_bar.d
)
SELECT cup.*, SUM(t.d) OVER(PARTITION BY t.b) FROM
  (
    SELECT bfv_cte_bar.c as e,r.d FROM
		(
			SELECT t.d, avg(t.a) over() FROM t
		) r,bfv_cte_bar
  ) AS cup,
t WHERE cup.e < 10
GROUP BY cup.d, cup.e, t.d, t.b
ORDER BY 1,2,3
LIMIT 10;

--
-- Test with CTE inlining enabled
--
set optimizer_cte_inlining = on;
set optimizer_cte_inlining_bound = 1000;

--
-- With clause select test 1
--
WITH t AS
(
 SELECT e.*,f.*
 FROM
    (
      SELECT * FROM bfv_cte_foo WHERE a < 10
    ) e
 LEFT OUTER JOIN
    (
       SELECT * FROM bfv_cte_bar WHERE c < 10
    ) f

  ON e.a = f.d )
SELECT t.a,t.d, count(*) over () AS window
FROM t
GROUP BY t.a,t.d ORDER BY t.a,t.d LIMIT 2;

--
-- With clause select test 2
--
WITH t(a,b,d) AS
(
  SELECT bfv_cte_foo.a,bfv_cte_foo.b,bfv_cte_bar.d FROM bfv_cte_foo,bfv_cte_bar WHERE bfv_cte_foo.a = bfv_cte_bar.d
)
SELECT t.b,avg(t.a), rank() OVER (PARTITION BY t.a ORDER BY t.a) FROM bfv_cte_foo,t GROUP BY bfv_cte_foo.a,bfv_cte_foo.b,t.b,t.a ORDER BY 1,2,3 LIMIT 5;

--
-- With clause select test 3
--
WITH t(a,b,d) AS
(
  SELECT bfv_cte_foo.a,bfv_cte_foo.b,bfv_cte_bar.d FROM bfv_cte_foo,bfv_cte_bar WHERE bfv_cte_foo.a = bfv_cte_bar.d
)
SELECT cup.*, SUM(t.d) OVER(PARTITION BY t.b) FROM
  (
    SELECT bfv_cte_bar.*, AVG(t.b) OVER(PARTITION BY t.a ORDER BY t.b desc) AS e FROM t,bfv_cte_bar
  ) AS cup,
t WHERE cup.e < 10
GROUP BY cup.c,cup.d, cup.e ,t.d, t.b
ORDER BY 1,2,3,4
LIMIT 10;

--
-- With clause select test 4
--
WITH t(a,b,d) AS
(
  SELECT bfv_cte_foo.a,bfv_cte_foo.b,bfv_cte_bar.d FROM bfv_cte_foo,bfv_cte_bar WHERE bfv_cte_foo.a = bfv_cte_bar.d
)
SELECT cup.*, SUM(t.d) FROM
  (
    SELECT bfv_cte_bar.*, count(*) OVER() AS e FROM t,bfv_cte_bar WHERE t.a = bfv_cte_bar.c
  ) AS cup,
t GROUP BY cup.c,cup.d, cup.e,t.a
HAVING AVG(t.d) < 10 ORDER BY 1,2,3,4 LIMIT 10;

--
-- With clause select test 5
--
WITH t(a,b,d) AS
(
  SELECT bfv_cte_foo.a,bfv_cte_foo.b,bfv_cte_bar.d FROM bfv_cte_foo,bfv_cte_bar WHERE bfv_cte_foo.a = bfv_cte_bar.d
)
SELECT cup.*, SUM(t.d) OVER(PARTITION BY t.b) FROM
  (
    SELECT bfv_cte_bar.c as e,r.d FROM
		(
			SELECT t.d, avg(t.a) over() FROM t
		) r,bfv_cte_bar
  ) AS cup,
t WHERE cup.e < 10
GROUP BY cup.d, cup.e, t.d, t.b
ORDER BY 1,2,3
LIMIT 10;

DROP TABLE IF EXISTS bfv_cte_foo;
DROP TABLE IF EXISTS bfv_cte_bar;
reset optimizer_cte_inlining;
reset optimizer_cte_inlining_bound;

--
-- Test for an old bug with rescanning window functions. This needs to use
-- catalog tables, otherwise the plan will contain a Motion node that
-- materializes the result, and masks the problem.
--
with t (a,b,d) as (select 1,2,1 from pg_class limit 1)
SELECT cup.* FROM
 t, (
SELECT sum(t.b) OVER(PARTITION BY t.a ) AS e FROM (select 1 as a, 2 as b from pg_class limit 1)foo,t
) as cup
GROUP BY cup.e;
