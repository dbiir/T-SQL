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
dbset db ora
dbset bm TPC-C
diset connection system_user  system
diset connection system_password TDSQLbj2018
diset connection instance orcl
diset tpcc tpcc_user test2
diset tpcc tpcc_pass test2
vuset logtotemp 1
diset tpcc ora_driver timed
diset tpcc rampup 2
diset tpcc duration 7
loadscript

puts "SEQUENCE STARTED"
foreach z { 16 32 64 128 256 512 } {
	puts "$z VU TEST"
	vuset vu $z
	vucreate
	vurun
	runtimer 2400
	vudestroy
	after 5000
}
puts "TEST SEQUENCE COMPLETE"
exit
