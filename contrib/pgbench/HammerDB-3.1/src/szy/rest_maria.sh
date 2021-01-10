#!/bin/bash
echo "512run"
./hammerdbcli <<!
 source /data/zzh/HammerDB-3.1/src/szy/runvu.tcl
!	
echo "mysql drop tpcc"
mysql -u root -proot -P3312 -h 100.107.157.228 <<EOF
drop database tpcc;
EOF
echo "build1024"
./hammerdbcli <<!
 source /data/zzh/HammerDB-3.1/src/szy/build1024.tcl
!
echo "1024run"
./hammerdbcli <<!
 source /data/zzh/HammerDB-3.1/src/szy/runvu.tcl
!	
echo "1024 complete"
