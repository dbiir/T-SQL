#! /bin/bash

n=0;
host=100.107.157.216;
port=6670;

while(($n<=80)); do
  res="$(mysql --user=test --password=test --host=$host --port=$port -e "show global status where Variable_name = 'Com_commit' or Variable_name =  'Com_rollback';")";
  commit=$(echo $res | awk '{print $4}')
  rollback=$(echo $res | awk '{print $6}')
  let sum=$commit+$rollback
  echo $sum >> /data/zzh/res1.txt
  n=$((n + 1));
  sleep 10;
done

