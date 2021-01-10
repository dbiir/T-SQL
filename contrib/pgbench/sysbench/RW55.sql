\setrandom id1 1 1000000

\setrandom id2 1 1000000

\setrandom id3 1 1000000

\setrandom id4 1 1000000

\setrandom id5 1 1000000

\setrandom id6 1 1000000

\setrandom id7 1 1000000

\setrandom id8 1 1000000

\setrandom id9 1 1000000

\setrandom id10 1 1000000

BEGIN;

SELECT pad FROM sbtest WHERE id = :id1;

SELECT pad FROM sbtest WHERE id = :id2;

SELECT pad FROM sbtest WHERE id = :id3;

SELECT pad FROM sbtest WHERE id = :id4;

SELECT pad FROM sbtest WHERE id = :id5;

UPDATE sbtest SET k=k+1 WHERE id = :id6;

UPDATE sbtest SET k=k+1 WHERE id = :id7;

UPDATE sbtest SET k=k+1 WHERE id = :id8;

UPDATE sbtest SET k=k+1 WHERE id = :id9;

UPDATE sbtest SET k=k+1 WHERE id = :id10;

COMMIT;
