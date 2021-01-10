#! /bin/bash

k=$1
pgbench -c $k -j $k -r -T 10  -f ./RW91.sql -P 1 -s 1 -n -U gpadmin -p 15432 -h 127.0.0.1 -d template1
