#!/bin/tclsh
puts "SETTING CONFIGURATION"
global complete
proc wait_to_complete {} {
global complete
set complete [vucomplete]
if {!$complete} {after 30000 wait_to_complete} else { exit }
}
dbset db mssqls
dbset bm TPC-C
diset connection mssqls_linux_server 100.107.157.228
diset connection mssqls_uid SA
diset connection mssqls_pass TDSQL@12
diset tpcc mssqls_count_ware 128
diset tpcc mssqls_num_vu 16
vuset logtotemp 1
diset tpcc mssqls_driver timed
diset tpcc mssqls_rampup 5
diset tpcc mssqls_duration 30
buildschema
wait_to_complete

