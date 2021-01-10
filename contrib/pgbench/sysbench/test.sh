#! /bin/bash

pgbench -c 10 -j 10 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
pgbench -c 30 -j 30 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
pgbench -c 50 -j 50 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
pgbench -c 70 -j 70 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
pgbench -c 90 -j 90 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
pgbench -c 100 -j 100 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
pgbench -c 110 -j 110 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
pgbench -c 130 -j 130 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
pgbench -c 150 -j 150 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
pgbench -c 170 -j 170 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
pgbench -c 190 -j 190 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
pgbench -c 200 -j 200 -r -T 60 -P 1 -s 10 -f insert-only.sql -d template1 -x storage_engine=rocksdb -n >> a.log
