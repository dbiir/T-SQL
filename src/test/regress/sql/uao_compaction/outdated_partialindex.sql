-- @Description Tests the behavior when the index of an ao table
-- has not been cleaned (e.g. because of a crash) in combination
-- with a partial index.

CREATE TABLE uao_outdated_partial (a INT, b INT, c CHAR(128)) WITH (appendonly=true) DISTRIBUTED BY (a);
CREATE INDEX uao_outdated_partial_index ON uao_outdated_partial(b) WHERE b < 20;
INSERT INTO uao_outdated_partial SELECT i as a, i as b, 'hello world' as c FROM generate_series(1, 50) AS i;
INSERT INTO uao_outdated_partial SELECT i as a, i as b, 'hello world' as c FROM generate_series(51, 100) AS i;
ANALYZE uao_outdated_partial;

SET enable_seqscan=false;
DELETE FROM uao_outdated_partial WHERE a < 16;
VACUUM uao_outdated_partial;
SELECT * FROM uao_outdated_partial WHERE b = 20;
SELECT * FROM uao_outdated_partial WHERE b = 10;
INSERT INTO uao_outdated_partial SELECT i as a, i as b, 'Good morning' as c FROM generate_series(101, 110) AS i;
SELECT * FROM uao_outdated_partial WHERE b = 10;
SELECT * FROM uao_outdated_partial WHERE b = 102;
