$ more schemabuild.tcl
#!/bin/tclsh
puts "SETTING CONFIGURATION"

global complete
proc wait_to_complete {} {
global complete
set complete [vucomplete]
if {!$complete} {after 10000 wait_to_complete} else { exit }
}

dbset db ora
dbset bm TPC-C
diset tpcc count_ware 512
diset tpcc num_vu 128
diset connection system_user system
diset connection system_password TDSQLbj2018
diset connection instance orcl
diset tpcc tpcc_user test1
diset tpcc tpcc_pass test1
vuset logtotemp 1
vuset unique 1
diset tpcc ora_driver timed
diset tpcc rampup 2
diset tpcc duration 7

buildschema

wait_to_complete

