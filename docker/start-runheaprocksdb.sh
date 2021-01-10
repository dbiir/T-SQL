psql penguindb << EOF
    ALTER DATABASE penguindb SET default_transaction_isolation = 'repeatable read';
EOF

pgbench -i -s 100 -x storage_engine=rocksdb -n 

number_of_threads=(8 16 32 64 128 256 512)
for var in ${number_of_threads[@]}
do
    dstat --output /home/gpadmin/tpcb-$var-onpgbench.csv 2>&1 >/dev/null &

    # pgbench -c $var -j $var -r -T 60 -n
    pgbench -c $var -j $var -r -T 60 -n >> /home/gpadmin/tpcb-res-rocksdb.txt
    sleep 30
    kill -9 $(ps -aux | grep dstat | grep -v grep | grep -v bash | awk '{print $2}')

done

gpstop -M immediate -a -r 

pgbench -i -s 100 -n 
number_of_threads=(8 16 32 64 128 256 512)
for var in ${number_of_threads[@]}
do
    dstat --output /home/gpadmin/tpcb-$var-onpgbench.csv 2>&1 >/dev/null &

    pgbench -c $var -j $var -r -T 60 -n >> /home/gpadmin/tpcb-res-heap.txt
    sleep 15
    kill -9 $(ps -aux | grep dstat | grep -v grep | grep -v bash | awk '{print $2}')

done
