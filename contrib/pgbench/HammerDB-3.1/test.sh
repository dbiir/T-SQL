#! /bin/bash

trap "ps -ef|grep -E 'iostat|dstat'|grep -v grep|awk '{print $2}'|xargs kill -9; exit" 2

if [ $# -lt 3 ]; then
  echo "usage: $0 host port path"
fi

host=$1
port=$2

#sysbenchPath=/usr/local/sysbench1.0 
tpccPath=/data/zzh/HammerDB-3.1

tardir=$3
basedir=$(cd `dirname $0`; pwd)

<<C
if [ ! -d $sysbenchPath ]; then
  echo "[ERR] No such sysbench path"
  exit
fi
C

if [ ! -d $tpccPath ]; then
  echo "[ERR] No such tpcc path"
  exit
fi

check_tardir() {
  if [ -d $tardir ]; then
    rm -r $tardir
  fi
}

oltp_test() {
  check_tardir
  mkdir -p $tardir/oltp
  $basedir/oltp_test.sh $host $port $sysbenchPath $tardir/oltp $monitor_mode $lua_type | tee -a $tardir/oltp/$mode.txt
}

tpcc_test() {
  check_tardir
  mkdir -p $tardir/tpcc_oracle_res
  #$basedir/tpcc_test.sh $host $port $tpccPath cleanup
  $basedir/tpcc_test.sh $host $port $tpccPath $wh prepare
  #$basedir/tpcc_test.sh $host $port $tpccPath $wh run
}

######### MAIN ###########

wh_list="128";
for wh in $wh_list
do
  sshpass -p TDSQL@bj2018 ssh $host $tardir/install.sh $port $tardir/binrun/
  tpcc_test $wh
done

