--
-- Greenplum disallows concurrent index creation. It allows concurrent index
-- drops, so we want to test for it. Though, due to this difference with
-- upstream we can not keep the tests completely in sync and we add them here.
-- Original tests are in create_index.sql
--
CREATE TABLE tbl_drop_ind_concur (f1 text, f2 text, dk text) distributed by (dk);
CREATE INDEX tbl_drop_index1 ON tbl_drop_ind_concur(f2,f1);
INSERT INTO tbl_drop_ind_concur VALUES  ('a','b', '1');
INSERT INTO tbl_drop_ind_concur VALUES  ('b','b', '1');
INSERT INTO tbl_drop_ind_concur VALUES  ('c','c', '2');
INSERT INTO tbl_drop_ind_concur VALUES  ('d','d', '3');
CREATE UNIQUE INDEX tbl_drop_index2 ON tbl_drop_ind_concur(dk, f1);
CREATE INDEX tbl_drop_index3 on tbl_drop_ind_concur(f2) WHERE f1='a';
CREATE INDEX tbl_drop_index4 on tbl_drop_ind_concur(f2) WHERE f1='x';

DROP INDEX CONCURRENTLY "tbl_drop_index2";				-- works
DROP INDEX CONCURRENTLY IF EXISTS "tbl_drop_index2";		-- notice

-- failures
DROP INDEX CONCURRENTLY "tbl_drop_index2", "tbl_drop_index3";
BEGIN;
DROP INDEX CONCURRENTLY "tbl_drop_index4";
ROLLBACK;

-- successes
DROP INDEX CONCURRENTLY IF EXISTS "tbl_drop_index3";
DROP INDEX CONCURRENTLY "tbl_drop_index4";
DROP INDEX CONCURRENTLY "tbl_drop_index1";

\d tbl_drop_ind_concur

DROP TABLE tbl_drop_ind_concur;
