-- create functions to query uao auxiliary tables through gp_dist_random instead of going through utility mode
CREATE OR REPLACE FUNCTION gp_aovisimap_dist_random(
  IN relation_name text) RETURNS setof record AS $$
DECLARE
  relid oid;
  result record;
BEGIN
  select into relid oid from pg_class where relname=quote_ident(relation_name);
  for result in
      EXECUTE 'select * from gp_dist_random(''pg_aoseg.pg_aovisimap_' || relid ||''');'
  loop
      return next result;
  end loop;
  return;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gp_aovisimap_count(
  IN relation_name text) RETURNS int AS $$
DECLARE
  aovisimap_record record;
  result int := 0;
BEGIN
  for aovisimap_record in
      EXECUTE 'select gp_toolkit.__gp_aovisimap(''' || relation_name || '''::regclass) from gp_dist_random(''gp_id'');'
  loop
      result := result + 1;
  end loop;
  return result;
END;
$$ LANGUAGE plpgsql;

-- Verify empty visimap for uao table
create table uao_table_check_empty_visimap (i int, j varchar(20), k int ) with (appendonly=true) DISTRIBUTED BY (i);
insert into uao_table_check_empty_visimap values(1,'test',2);
SELECT 1 FROM pg_appendonly WHERE visimapidxid is not NULL AND visimapidxid is not NULL AND relid='uao_table_check_empty_visimap'::regclass;

-- Verify GUC select_invisible=true for uao tables
create table uao_table_check_select_invisible (i int, j varchar(20), k int ) with (appendonly=true) DISTRIBUTED BY (i);
insert into uao_table_check_select_invisible values(1,'test',4);
select * from uao_table_check_select_invisible;
update uao_table_check_select_invisible set j = 'test_update' where i = 1;
select * from uao_table_check_select_invisible;
set gp_select_invisible = true;
select * from uao_table_check_select_invisible;
set gp_select_invisible = false;

-- Verify that the visimap changes when delete from uao table
create table uao_table_check_visimap_changes_after_delete (i int, j varchar(20), k int ) with (appendonly=true) DISTRIBUTED BY (i);
select * from uao_table_check_visimap_changes_after_delete;
select count(*) from gp_aovisimap_dist_random('uao_table_check_visimap_changes_after_delete') as (segno integer, first_row_no bigint, visimap bytea);
insert into uao_table_check_visimap_changes_after_delete select i,'aa'||i,i+10 from generate_series(1,5) as i;
delete from uao_table_check_visimap_changes_after_delete where i=3;
select * from uao_table_check_visimap_changes_after_delete;
select count(*) from gp_aovisimap_dist_random('uao_table_check_visimap_changes_after_delete') as (segno integer, first_row_no bigint, visimap bytea);

-- Verify that the visimap changes when delete and truncate from uao table
create table uao_table_visimap_changes_after_trunc (i int, j varchar(20), k int ) with (appendonly=true) DISTRIBUTED BY (i);
select * from uao_table_visimap_changes_after_trunc;
select count(*) from gp_aovisimap_dist_random('uao_table_visimap_changes_after_trunc') as (segno integer, first_row_no bigint, visimap bytea);
insert into uao_table_visimap_changes_after_trunc select i,'aa'||i,i+10 from generate_series(1,5) as i;
delete from uao_table_visimap_changes_after_trunc where i=3;
select * from uao_table_visimap_changes_after_trunc;
select count(*) from gp_aovisimap_dist_random('uao_table_visimap_changes_after_trunc') as (segno integer, first_row_no bigint, visimap bytea);
truncate table uao_table_visimap_changes_after_trunc;
select * from uao_table_visimap_changes_after_trunc;
select count(*) from gp_aovisimap_dist_random('uao_table_visimap_changes_after_trunc') as (segno integer, first_row_no bigint, visimap bytea);

-- Verify the usage of UDF gp_aovisimap for deleted tuple
create table uao_table_gp_aovisimap_after_delete(i int, j varchar(20), k int ) with (appendonly=true) DISTRIBUTED BY (i);
insert into uao_table_gp_aovisimap_after_delete select i,'aa'||i,i+10 from generate_series(1,10) as i;
select count(*) from gp_dist_random('uao_table_gp_aovisimap_after_delete');
select * from gp_aovisimap_count('uao_table_gp_aovisimap_after_delete');
delete from uao_table_gp_aovisimap_after_delete;
select count(*) from gp_dist_random('uao_table_gp_aovisimap_after_delete');
select * from gp_aovisimap_count('uao_table_gp_aovisimap_after_delete');

-- Verify the usage of UDF gp_aovisimap for update tuple
create table uao_table_gp_aovisimap_after_update(i int, j varchar(20), k int ) with (appendonly=true) DISTRIBUTED BY (i);
insert into uao_table_gp_aovisimap_after_update select i,'aa'||i,i+10 from generate_series(1,10) as i;
select count(*) from gp_dist_random('uao_table_gp_aovisimap_after_update');
select * from gp_aovisimap_count('uao_table_gp_aovisimap_after_update');
update uao_table_gp_aovisimap_after_update set j = j || '_9';
select count(*) from gp_dist_random('uao_table_gp_aovisimap_after_update');
select * from gp_aovisimap_count('uao_table_gp_aovisimap_after_update');

-- Verify the tupcount changes in pg_aoseg when deleting from uao table
create table uao_table_tupcount_changes_after_delete(i int, j varchar(20), k int ) with (appendonly=true) DISTRIBUTED BY (i);
insert into uao_table_tupcount_changes_after_delete select i,'aa'||i,i+10 from generate_series(1,10) as i;
select sum(tupcount) from gp_toolkit.__gp_aoseg('uao_table_tupcount_changes_after_delete');
select count(*) from uao_table_tupcount_changes_after_delete;
delete from uao_table_tupcount_changes_after_delete where i = 1;
select sum(tupcount) from gp_toolkit.__gp_aoseg('uao_table_tupcount_changes_after_delete');
select count(*) from uao_table_tupcount_changes_after_delete;
vacuum full uao_table_tupcount_changes_after_delete;
select sum(tupcount) from gp_toolkit.__gp_aoseg('uao_table_tupcount_changes_after_delete');
select count(*) from uao_table_tupcount_changes_after_delete;

-- Verify the tupcount changes in pg_aoseg when updating uao table
create table uao_table_tupcount_changes_after_update(i int, j varchar(20), k int ) with (appendonly=true) DISTRIBUTED BY (i);
insert into uao_table_tupcount_changes_after_update select i,'aa'||i,i+10 from generate_series(1,10) as i;
select tupcount from gp_toolkit.__gp_aoseg('uao_table_tupcount_changes_after_update');
select count(*) from uao_table_tupcount_changes_after_update;
update uao_table_tupcount_changes_after_update set j=j||'test11' where i = 1;
select tupcount from gp_toolkit.__gp_aoseg('uao_table_tupcount_changes_after_update');
select count(*) from uao_table_tupcount_changes_after_update;
vacuum full uao_table_tupcount_changes_after_update;
select sum(tupcount) from gp_toolkit.__gp_aoseg('uao_table_tupcount_changes_after_update');
select count(*) from uao_table_tupcount_changes_after_update;

-- Verify the hidden tup_count using UDF gp_aovisimap_hidden_info(oid) for uao relation after delete and vacuum
create table uao_table_check_hidden_tup_count_after_delete(i int, j varchar(20), k int ) with (appendonly=true) DISTRIBUTED BY (i);
insert into uao_table_check_hidden_tup_count_after_delete select i,'aa'||i,i+10 from generate_series(1,10) as i;
select gp_toolkit.__gp_aovisimap_hidden_info('uao_table_check_hidden_tup_count_after_delete'::regclass) from gp_dist_random('gp_id');
delete from uao_table_check_hidden_tup_count_after_delete;
select gp_toolkit.__gp_aovisimap_hidden_info('uao_table_check_hidden_tup_count_after_delete'::regclass) from gp_dist_random('gp_id');
vacuum full uao_table_check_hidden_tup_count_after_delete;
select gp_toolkit.__gp_aovisimap_hidden_info('uao_table_check_hidden_tup_count_after_delete'::regclass) from gp_dist_random('gp_id');

-- Verify the hidden tup_count using UDF gp_aovisimap_hidden_info(oid) for uao relation after update and vacuum
create table uao_table_check_hidden_tup_count_after_update(i int, j varchar(20), k int ) with (appendonly=true) DISTRIBUTED BY (i);
insert into uao_table_check_hidden_tup_count_after_update select i,'aa'||i,i+10 from generate_series(1,10) as i;
select gp_toolkit.__gp_aovisimap_hidden_info('uao_table_check_hidden_tup_count_after_update'::regclass) from gp_dist_random('gp_id');
update uao_table_check_hidden_tup_count_after_update set j = 'test21';
select gp_toolkit.__gp_aovisimap_hidden_info('uao_table_check_hidden_tup_count_after_update'::regclass) from gp_dist_random('gp_id');
vacuum full uao_table_check_hidden_tup_count_after_update;
select gp_toolkit.__gp_aovisimap_hidden_info('uao_table_check_hidden_tup_count_after_update'::regclass) from gp_dist_random('gp_id');
