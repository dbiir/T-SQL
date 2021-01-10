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
                          diset connection mysql_host 100.107.157.228
                          diset connection mysql_port 3312
                          diset tpcc mysql_user root
                          diset tpcc mysql_pass root
                          diset tpcc mysql_driver timed
                          vuset logtotemp 1
                          vuset unique 1
			  diset tpcc mysql_rampup 5
                          diset tpcc mysql_duration 30
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
