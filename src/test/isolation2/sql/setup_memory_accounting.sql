-- start_ignore
! gpconfig -c gp_vmem_limit_per_query -v '2MB' --skipvalidation
! gpconfig -c gp_vmem_protect_limit -v '20'
! gpconfig -c runaway_detector_activation_percent -v 0
! gpstop -rai;
-- end_ignore
