#!/bin/bash
sqlcmd -S 100.107.157.228\\sqlserver,1433 -U SA -P TDSQL@12 <<EOF
drop database tpcc;
go
EOF

