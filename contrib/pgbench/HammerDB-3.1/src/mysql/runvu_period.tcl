$ more mysqlrun.tcl
!/bin/tclsh
proc runtimer { seconds } {
set x 0
set timerstop 0
	while {!$timerstop} {
		incr x
		after 1000
		if { ![ expr {$x % 60} ] } {
			set y [ expr $x / 60 ]
			puts "Timer: $y minutes elapsed"
		}
		update
		if {  [ vucomplete ] || $x eq $seconds } { set timerstop 1 }
	}
	return
}

puts "SETTING CONFIGURATION"
dbset db mysql
dbset bm TPC-C
diset connection mysql_host 100.107.157.216
diset connection mysql_port 6670
diset tpcc mysql_user test
diset tpcc mysql_pass test
diset tpcc mysql_driver timed
diset tpcc mysql_total_iterations 10000000000
vuset logtotemp 1
diset tpcc mysql_rampup 1
diset tpcc mysql_duration 10
loadscript
puts "SEQUENCE STARTED"
puts "16 VU TEST"
vuset vu 16
vucreate
vurun
runtimer 2400
vudestroy
after 5000

puts "TEST SEQUENCE COMPLETE"
