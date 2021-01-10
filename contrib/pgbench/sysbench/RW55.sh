#! /bin/bash

pgbench -c 10 -j 10 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 20 -j 20 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 30 -j 30 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 40 -j 40 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 50 -j 50 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 60 -j 60 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 70 -j 70 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 80 -j 80 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 90 -j 90 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 100 -j 100 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 110 -j 110 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 120 -j 120 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 130 -j 130 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 140 -j 140 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log
pgbench -c 150 -j 150 -r -T 6  -f ./RW55.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1 >> b.log

