CREATE TABLE reindex_dropindex_serialize_tab_heap (a INT, b text, c date, d numeric, e bigint, f char(10), g float) distributed by (a);
insert into reindex_dropindex_serialize_tab_heap select 1, 'abc'||i, now(),i*100.43, i*-187, 'a'|| i*-1, i*2.23 from generate_series(1,1000) i;
insert into reindex_dropindex_serialize_tab_heap select 1, 'abc'||i, now(),i*100.43, i*-187, 'a'|| i*-1, i*2.23 from generate_series(1,1000) i;
insert into reindex_dropindex_serialize_tab_heap select 1, 'abc'||i, now(),i*100.43, i*-187, 'a'|| i*-1, i*2.23 from generate_series(1,1000) i;
insert into reindex_dropindex_serialize_tab_heap select 1, 'abc'||i, now(),i*100.43, i*-187, 'a'|| i*-1, i*2.23 from generate_series(1,1000) i;
create index idxa_reindex_dropindex_serialize_tab_heap on reindex_dropindex_serialize_tab_heap(a);
create index idxb_reindex_dropindex_serialize_tab_heap on reindex_dropindex_serialize_tab_heap(b);
create index idxc_reindex_dropindex_serialize_tab_heap on reindex_dropindex_serialize_tab_heap(c);
create index idxd_reindex_dropindex_serialize_tab_heap on reindex_dropindex_serialize_tab_heap(d);
create index idxe_reindex_dropindex_serialize_tab_heap on reindex_dropindex_serialize_tab_heap(e);
create index idxf_reindex_dropindex_serialize_tab_heap on reindex_dropindex_serialize_tab_heap(f);
create index idxg_reindex_dropindex_serialize_tab_heap on reindex_dropindex_serialize_tab_heap(g);
-- start_ignore
SET gp_create_table_random_default_distribution=off;
-- end_ignore
1: BEGIN;
2: BEGIN;
2: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
2: select 'dummy select to establish snapshot';
1: drop index idxg_reindex_dropindex_serialize_tab_heap;
1: COMMIT;
-- Remember index relfilenodes from master and segments before
-- reindex.
2: create temp table old_relfilenodes as
   (select gp_segment_id as dbid, relfilenode, oid, relname from gp_dist_random('pg_class')
    where relname like 'idx%_reindex_dropindex_serialize_tab_heap'
    union all
    select gp_segment_id as dbid, relfilenode, oid, relname from pg_class
    where relname like 'idx%_reindex_dropindex_serialize_tab_heap');
2: reindex table reindex_dropindex_serialize_tab_heap;
2: COMMIT;
-- Validate that reindex changed all index relfilenodes on master as well as
-- segments.  The following query should return 0 tuples.
2: select oldrels.* from old_relfilenodes oldrels join
   (select gp_segment_id as dbid, relfilenode, relname from gp_dist_random('pg_class')
    where relname like 'idx%_reindex_dropindex_serialize_tab_heap'
    union all
    select gp_segment_id as dbid, relfilenode, relname from pg_class
    where relname like 'idx%_reindex_dropindex_serialize_tab_heap') newrels
    on oldrels.relfilenode = newrels.relfilenode
    and oldrels.dbid = newrels.dbid
    and oldrels.relname = newrels.relname;

3: select count(*) from  reindex_dropindex_serialize_tab_heap where a = 1;
3: set enable_seqscan=false;
3: set enable_indexscan=true;
3: select count(*) from  reindex_dropindex_serialize_tab_heap where a = 1;

-- expect index to be dropped
3: select 1-count(*) as index_dropped from (select * from pg_class union all select * from gp_dist_random('pg_class')) t where t.relname = 'idxg_reindex_dropindex_serialize_tab_heap';
