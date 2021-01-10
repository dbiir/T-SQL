#! /bin/bash

wh_list="128 256 512 1024"
thread_list="1 2 4 8 16 32 64"
base_wh=20
base_thread=16

ready_time=60
run_time=300


tpccTest(){
  res=`$path/tpcc_start -h $host -P $port -d tpcc -u test -p test -w $1 -c $2 -r $ready_time -l $run_time | grep "TpmC"`
  echo -e "wh: $1,\tthread: $2,\ttpmC: `echo $res | awk '{print $2}'`"
}

if [ $# -lt 4 ];then
  echo "usage: $0 host port tpcc-path (prepare|run|cleanup)"
  echo "arg num: $#"
  exit
fi

host=$1
port=$2
path=$3
wh=$4
mode=$5

# prepare
case $mode in
prepare)
  #echo 'exit' | $path/hammerdbcli
  #echo "source $path/src/mysql/build.tcl" | $path/hammerdbcli
  $path/hammerdbcli << EOF
  source $path/src/mysql/build_$wh.tcl
EOF
  ;;

run)
  $path/hammerdbcli << EOF
  source $path/src/mysql/runvu.tcl
EOF
  ;;

cleanup)
  # cleanup
  mysql --user=test --password=test --host=$host --port=$port -e "DROP DATABASE TPCC;"
  ;;
  
  *)
  echo "Invalid mode."
  esac

