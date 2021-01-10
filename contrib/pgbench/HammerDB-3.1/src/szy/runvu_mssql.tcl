#!/bin/tclsh
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
                          dbset db mssqls
			dbset bm TPC-C
			diset connection mssqls_linux_server 100.107.157.228
			diset connection mssqls_uid SA
			diset connection mssqls_pass TDSQL@12
			vuset logtotemp 1
			diset tpcc mssqls_driver timed
			diset tpcc mssqls_rampup 5 
			diset tpcc mssqls_duration 30
vuset unique 1			
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
