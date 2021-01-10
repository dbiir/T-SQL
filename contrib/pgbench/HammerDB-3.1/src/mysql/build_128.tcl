$ more schemabuild.tcl
#!/bin/tclsh
puts "SETTING CONFIGURATION"

global complete
proc wait_to_complete {} {
global complete
set complete [vucomplete]
if {!$complete} {after 10000 wait_to_complete} else { exit }
}

dbset db mysql
dbset bm TPC-C
diset tpcc mysql_count_ware 128
diset tpcc mysql_num_vu 128
diset connection mysql_host 100.107.157.216
diset connection mysql_port 6670
diset tpcc mysql_user test
diset tpcc mysql_pass test
vuset logtotemp 1
vuset unique 1
diset tpcc mysql_driver timed
diset tpcc mysql_rampup 2
diset tpcc mysql_duration 10

buildschema

wait_to_complete

