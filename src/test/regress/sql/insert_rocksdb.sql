drop table if exists select_rocksdb;
create table select_rocksdb (
    id int primary key,
    name text
) with(storage_engine=rocksdb);

insert into select_rocksdb values (10, 'simon');
insert into select_rocksdb values (16, 'trow');
insert into select_rocksdb values (14, 'lily');
insert into select_rocksdb values (12, 'lilly');

select * from select_rocksdb;
