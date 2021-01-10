-- @Description Tests basic index usage behavior after vacuuming

CREATE TABLE uao_index_test (a INT, b INT, c CHAR(128)) WITH (appendonly=true);
CREATE INDEX uao_index_test_index ON uao_index_test(b);
INSERT INTO uao_index_test SELECT i as a, i as b, 'hello world' as c FROM generate_series(1,10) AS i;
INSERT INTO uao_index_test SELECT i as a, i as b, 'hello world' as c FROM generate_series(1,10) AS i;

VACUUM uao_index_test;
SELECT * FROM uao_index_test WHERE b = 5;
