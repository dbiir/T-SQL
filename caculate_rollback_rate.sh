#!/usr/bin/env bash
date1=$2
date2=$3
t1=`date -d "$date1" +%s`
t2=`date -d "$date2" +%s`
alltransaction=0
rollbacktransaction=0
committransaction=0
OLDIFS=$IFS
for f in $(ls $1)
    do
    IFS=$'\n';
    for line in $(cat $f|grep RUCC) 
    do 
        echo $line
        IFS=$OLDIFS
        ts=$(echo $line|awk '{print $1" "$2}')
        tn=`date -d "$ts" +%s`
        if [[ $tn -gt $t1 ]] && [[ $tn -lt $t2 ]]
        then
            alltransaction=`expr $alltransaction + $(echo $line|awk '{print $8}')`;
            rollbacktransaction=`expr $rollbacktransaction + $(echo $line|awk '{print $12}')`;
            committransaction=`expr $rollbacktransaction + $(echo $line|awk '{print $14}')`;
        fi
        IFS=$'\n'; 
    done
done
IFS=$OLDIFS
rate1=`echo "scale=4; $rollbacktransaction/$alltransaction" | bc`
echo "all1="$alltransaction
echo "rollback="$rollbacktransaction
echo "commit="$committransaction
echo $rate1