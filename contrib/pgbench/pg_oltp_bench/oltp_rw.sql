\set table_size 10000000
\set range_size 100
\set id1 random(1, :table_size)
\set id2 random(1, :table_size)
\set id3 random(1, :table_size)
\set id4 random(1, :table_size)
\set id5 random(1, :table_size)
\set id6 random(1, :table_size)
\set id7 random(1, :table_size)
\set id8 random(1, :table_size)
\set id9 random(1, :table_size)
\set id10 random(1, :table_size)
\set r1l random(1, :table_size)
\set r1u :r1l + :range_size
\set r2l random(1, :table_size)
\set r2u :r2l + :range_size
\set r3l random(1, :table_size)
\set r3u :r3l + :range_size
\set r4l random(1, :table_size)
\set r4u :r4l + :range_size
\set u1 random(1, :table_size)
\set u2 random(1, :table_size)
\set u3 random(1, :table_size)
\set u4 random(1, :table_size)
BEGIN;
SELECT c FROM sbtest WHERE id = :id1;
SELECT c FROM sbtest WHERE id = :id2;
SELECT c FROM sbtest WHERE id = :id3;
SELECT c FROM sbtest WHERE id = :id4;
SELECT c FROM sbtest WHERE id = :id5;
SELECT c FROM sbtest WHERE id = :id6;
SELECT c FROM sbtest WHERE id = :id7;
SELECT c FROM sbtest WHERE id = :id8;
SELECT c FROM sbtest WHERE id = :id9;
SELECT c FROM sbtest WHERE id = :id10;
SELECT c FROM sbtest WHERE id BETWEEN :r1l AND :r1u;
SELECT SUM(K) FROM sbtest WHERE id BETWEEN :r2l AND :r2u;
SELECT c FROM sbtest WHERE id BETWEEN :r3l AND :r3u ORDER BY c;
SELECT DISTINCT c FROM sbtest WHERE id BETWEEN :r4l AND :r4u;
UPDATE sbtest SET k = k + 1 WHERE id = :u1;
UPDATE sbtest SET c = sb_rand_str('###########-###########-###########-###########-###########-###########-###########-###########-###########-###########') WHERE id = :u2;
DELETE FROM sbtest WHERE id = :u3;
INSERT INTO sbtest (id, k, c, pad) VALUES (:u3, :u4, sb_rand_str('###########-###########-###########-###########-###########-###########-###########-###########-###########-###########'), sb_rand_str('###########-###########-###########-###########-###########')) ON CONFLICT DO NOTHING;
COMMIT;