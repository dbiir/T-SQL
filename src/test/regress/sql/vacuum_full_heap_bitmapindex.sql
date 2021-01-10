drop table if exists vfheapbm;
create table vfheapbm(a, b, c) as
select 1, i, repeat('x', 1000 + i % 2) from generate_series(1, 100)i distributed by (a);
create index ivfheapbm on vfheapbm using bitmap(b, c);

delete from vfheapbm where b between 0 and (select count(*) / 2 from vfheapbm);
select pg_relation_size('vfheapbm') from gp_dist_random('gp_id') where gp_segment_id = 1;
select pg_relation_size('pg_bitmapindex.pg_bm_' || 'ivfheapbm'::regclass::oid::text) from gp_dist_random('gp_id') where gp_segment_id = 1;

vacuum full freeze vfheapbm;

select pg_relation_size('vfheapbm') from gp_dist_random('gp_id') where gp_segment_id = 1;
select pg_relation_size('pg_bitmapindex.pg_bm_' || 'ivfheapbm'::regclass::oid::text) from gp_dist_random('gp_id') where gp_segment_id = 1;
