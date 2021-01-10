#! /bin/bash
threads=(10 30 50 70 90 100 110 130 150 170 190 200)
for var in ${threads[@]};
do
	dstat --output /home/gpadmin/data/update-$var.csv & pgbench -c $var -j $var -r -T 60 -P 1 -s 1000 1000 1000 1000 1000 1000 1000 1000 1000 1000 -f update-only.sql -n >> res.txt
	a=`ps -ef | grep "dstat" | grep -v "grep" | awk '{print $2}'`
	kill -s 9 $a
done
echo "finished"
