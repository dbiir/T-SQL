cd /home/gpadmin/pgpool/testsql
psql -d template1 -f pgpool-recovery.sql
psql -d template1 -f pgpool-regclass.sql
psql -d template1 -f insert_lock.sql

pgpool -n -d 2>&1 > /home/gpadmin/pgpool.log
