#!/bin/bash
echo "..begin.."
for i in {2..4};
do
echo "..build $i.."
case $i in 
1) ./hammerdbcli <<EOF
 source /data/zzh/HammerDB-3.1/src/szy/build128.tcl
 exit
EOF
;;
2)echo "... soucer build256..." 
./hammerdbcli <<EOF 
source /data/zzh/HammerDB-3.1/src/szy/build256.tcl
after 2400000
exit
EOF
;;
3) ./hammerdbcli <<EOF
 source /data/zzh/HammerDB-3.1/src/szy/build512.tcl
after 2400000
exit
EOF
;;
4) ./hammerdbcli <<EOF
 source /data/zzh/HammerDB-3.1/src/szy/build1025.tcl
after 2400000
exit
EOF
;;
esac
echo "..runing.."
./hammerdbcli <<EOF
 source /data/zzh/HammerDB-3.1/src/szy/runvu_mssql.tcl
 exit
EOF
echo "..drop database.."
sqlcmd -S 100.107.157.228\\sqlserver,1433 -U SA -P TDSQL@12 <<EOF
drop database tpcc;
go
EOF
done
