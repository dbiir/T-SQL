drop table if exists select_rocksdb;
create table if not exists select_rocksdb (
    id int primary key,
    name text
) with(storage_engine=rocksdb);

insert into select_rocksdb values (10, 'simon');
insert into select_rocksdb values (20, 'luccy');
insert into select_rocksdb values (17, 'lio');
insert into select_rocksdb values (15, 'loy');
insert into select_rocksdb values (16, 'trow');
insert into select_rocksdb values (14, 'lily');
insert into select_rocksdb values (12, 'lilly');

update select_rocksdb set name='feona' where id=12;
update select_rocksdb set name='ruby' where id=14;

select * from select_rocksdb;
