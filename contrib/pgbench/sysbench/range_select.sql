\set step 100
\setrandom id1 1 99900
\set id2 :id1 + :step
SELECT pad FROM sbtest WHERE id BETWEEN :id1 AND :id2;
