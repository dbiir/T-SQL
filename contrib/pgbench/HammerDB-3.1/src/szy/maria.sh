#!/bin/bash
echo "..begin.."
for i in {1..4};
do
echo "..drop database.."
mysql -u root -proot -P3312 -h 100.107.157.228  <<EOF
drop database tpcc;
EOF
echo "..build $i.."
case $i in
1) ./hammerdbcli <<EOF
 source /data/zzh/HammerDB-3.1/src/szy/build128.tcl
 exit
EOF
;;
2) ./hammerdbcli <<EOF
 source /data/zzh/HammerDB-3.1/src/szy/build256.tcl
 exit
EOF
;;
3) ./hammerdbcli <<EOF
 source /data/zzh/HammerDB-3.1/src/szy/build512.tcl
 exit
EOF
;;
4) ./hammerdbcli <<EOF
 source /data/zzh/HammerDB-3.1/src/szy/build1025.tcl
 exit
EOF
;;
esac
echo "..runing.."
./hammerdbcli <<EOF
 source /data/zzh/HammerDB-3.1/src/szy/runvu.tcl
 exit
EOF
done
