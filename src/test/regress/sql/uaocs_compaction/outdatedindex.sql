-- @Description Tests the behavior when the index of an ao table
-- has not been cleaned.

CREATE TABLE uaocs_outdatedindex (a INT, b INT, c CHAR(128)) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (a);
CREATE INDEX uaocs_outdatedindex_index ON uaocs_outdatedindex(b);
INSERT INTO uaocs_outdatedindex SELECT i as a, i as b, 'hello world' as c FROM generate_series(1, 50) AS i;
INSERT INTO uaocs_outdatedindex SELECT i as a, i as b, 'hello world' as c FROM generate_series(51, 100) AS i;
ANALYZE uaocs_outdatedindex;

SET enable_seqscan=false;
DELETE FROM uaocs_outdatedindex WHERE a < 16;
VACUUM uaocs_outdatedindex;
SELECT * FROM uaocs_outdatedindex WHERE b = 20;
SELECT * FROM uaocs_outdatedindex WHERE b = 10;
INSERT INTO uaocs_outdatedindex SELECT i as a, i as b, 'Good morning' as c FROM generate_series(1, 10) AS i;
SELECT * FROM uaocs_outdatedindex WHERE b = 10;
