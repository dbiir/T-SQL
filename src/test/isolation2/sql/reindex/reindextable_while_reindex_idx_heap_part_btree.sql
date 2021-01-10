DROP TABLE IF EXISTS reindex_crtab_part_heap_btree;

CREATE TABLE reindex_crtab_part_heap_btree ( id INTEGER, owner VARCHAR, description VARCHAR, property BOX, poli POLYGON, target CIRCLE, v VARCHAR, t TEXT, f FLOAT, p POINT, c CIRCLE, filler VARCHAR DEFAULT 'Big data is difficult to work with using most relational database management systems and desktop statistics and visualization packages, requiring instead massively parallel software running on tens, hundreds, or even thousands of servers.What is considered big data varies depending on the capabilities of the organization managing the set, and on the capabilities of the applications.This is here just to take up space so that we use more pages of data and sequential scans take a lot more time. ')DISTRIBUTED BY (id) PARTITION BY RANGE (id) ( PARTITION p_one START('1') INCLUSIVE END ('10') EXCLUSIVE, DEFAULT PARTITION de_fault );
insert into reindex_crtab_part_heap_btree (id, owner, description, property, poli, target) select i, 'user' || i, 'Testing GiST Index', '((3, 1300), (33, 1330))','( (22,660), (57, 650), (68, 660) )', '( (76, 76), 76)' from generate_series(1,1000) i ;
insert into reindex_crtab_part_heap_btree (id, owner, description, property, poli, target) select i, 'user' || i, 'Testing GiST Index', '((3, 1300), (33, 1330))','( (22,660), (57, 650), (68, 660) )', '( (76, 76), 76)' from generate_series(1,1000) i ;
create index on reindex_crtab_part_heap_btree(id);
-- @Description Ensures that a reindex table during reindex index operations is ok
-- 

DELETE FROM reindex_crtab_part_heap_btree  WHERE id < 128;
1: BEGIN;
1: create temp table old_relfilenodes as
   (select gp_segment_id as dbid, relfilenode, oid, relname from gp_dist_random('pg_class')
    where relname like 'reindex_crtab_part_heap_btree%_idx'
    union all
    select gp_segment_id as dbid, relfilenode, oid, relname from pg_class
    where relname like 'reindex_crtab_part_heap_btree%_idx');
2: BEGIN;
1: REINDEX TABLE  reindex_crtab_part_heap_btree;
2&: reindex index reindex_crtab_part_heap_btree_1_prt_de_fault_id_idx;
1: COMMIT;
2<:
-- Session 2 has not committed yet.  Session 1 should see effects of
-- its own reindex command above in pg_class.  The following query
-- validates that reindex command in session 1 indeed generates new
-- relfilenode for the index.
1: insert into old_relfilenodes
   (select gp_segment_id as dbid, relfilenode, oid, relname from gp_dist_random('pg_class')
    where relname like 'reindex_crtab_part_heap_btree%_idx'
    union all
    select gp_segment_id as dbid, relfilenode, oid, relname from pg_class
    where relname like 'reindex_crtab_part_heap_btree%_idx');
-- Expect two distinct relfilenodes per segment in old_relfilenodes table.
1: select distinct count(distinct relfilenode), relname from old_relfilenodes group by dbid, relname;
2: COMMIT;
-- After session 2 commits, the relfilenode it assigned to the
-- "1_prt_de_fault" index is visible to session 1.
1: insert into old_relfilenodes
   (select gp_segment_id as dbid, relfilenode, oid, relname from gp_dist_random('pg_class')
    where relname like 'reindex_crtab_part_heap_btree%_idx'
    union all
    select gp_segment_id as dbid, relfilenode, oid, relname from pg_class
    where relname like 'reindex_crtab_part_heap_btree%_idx');
-- Expect three distinct relfilenodes per segment for "1_prt_de_fault" index.
1: select distinct count(distinct relfilenode), relname from old_relfilenodes group by dbid, relname;

3: select count(*) from reindex_crtab_part_heap_btree where id = 998;
3: set enable_seqscan=false;
3: set enable_indexscan=true;
3: select count(*) from reindex_crtab_part_heap_btree where id = 999;
