-- Check if retry logic handles errors correctly.  The retry logic had
-- a bug where error state wasn't cleaned up correctly during retries,
-- leading to PANIC due to ERRORDATA_STACK_SIZE exeeded.
set dtx_phase2_retry_count = 11;
-- Set a fault on one of the QEs such that an error is raised exactly
-- 10 times at the beginning of 2nd phase of 2PC.
-- ERRORDATA_STACK_SIZE is defined as 10.  By erroring out 10 times,
-- the error handling logic in QD gets invoked 10 times.  If error
-- state is not cleaned up before each retry, it is suffice to return
-- an error 10 times to hit the above mentioned PANIC.

--
-- COMMIT_PREPARED
--
create extension if not exists gp_inject_fault;
select gp_inject_fault('finish_prepared_start_of_function', 'error', '', '', '', 1, 10, 0, dbid)
from gp_segment_configuration where role = 'p' and status = 'u' and content = 1;
begin;
create table dtm_retry_table (a int) distributed by (a);
-- Expected behavior is QD makes 11 attempts to commit and the last
-- one succeeds.
end;
-- Reset all faults.
select gp_inject_fault_infinite('all', 'reset', dbid) from gp_segment_configuration;
-- Verify that the table got created properly on all segments.
insert into dtm_retry_table select * from generate_series(1,12);

--
-- ABORT_SOME_PREPARED
--
-- Let content 0 primary error out during prepare.  This leads to
-- abort_some_prepared broadcast in 2nd phase.
select gp_inject_fault('start_prepare', 'error', dbid)
from gp_segment_configuration where role = 'p' and status = 'u' and content = 0;
-- Let content 1 primary, which had successfully prepared the
-- transaction, report error 10 times upon receiving
-- abort_some_prepared request.
select gp_inject_fault('finish_prepared_start_of_function', 'error', '', '', '', 1, 10, 0, dbid)
from gp_segment_configuration where role = 'p' and status = 'u' and content = 1;
begin;
truncate table dtm_retry_table;
-- Expected behavior: QD's 11th attempt to broadcast
-- abort_some_prepared message succeeds.
end;
-- Verify that the table wasn't truncated due to prevous transaction
-- being aborted.
select count(*) = 12 from dtm_retry_table;

-- Reset all faults.
select gp_inject_fault_infinite('all', 'reset', dbid) from gp_segment_configuration;

--
-- ABORT_PREPARED
--
-- After successful prepare, QD should error out, leading to
-- abort_prepared broadcast in 2nd phase.
select gp_inject_fault('dtm_broadcast_prepare', 'error', dbid)
from gp_segment_configuration where role = 'p' and content = -1;
-- Let content 0 primary error out 10 times during second phase.
select gp_inject_fault('finish_prepared_start_of_function', 'error', '', '', '', 1, 10, 0, dbid)
from gp_segment_configuration where role = 'p' and status = 'u' and content = 0;
begin;
truncate table dtm_retry_table;
-- Expected behavior: QD's 11th attempt to broadcast
-- abort_prepared message succeeds.
end;
-- Verify that the table wasn't truncated due to prevous transaction
-- being aborted.
select count(*) = 12 from dtm_retry_table;

-- Reset all faults.
select gp_inject_fault_infinite('all', 'reset', dbid) from gp_segment_configuration;
