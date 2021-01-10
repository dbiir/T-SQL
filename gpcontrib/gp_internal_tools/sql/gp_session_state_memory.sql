-- Before the extension is loaded no information is available
select * from session_state.session_level_memory_consumption limit 0;

CREATE EXTENSION gp_internal_tools;

select * from session_state.session_level_memory_consumption limit 0;

-- Verify that we have 1 entry per segment, as we are only considering our current session.
select 1 as session_entry_count from session_state.session_level_memory_consumption, pg_stat_activity where pid = pg_backend_pid() 
and session_state.session_level_memory_consumption.sess_id = pg_stat_activity.sess_id 
having count(1) = (select count(1) from gp_segment_configuration where preferred_role = 'p');

DROP EXTENSION gp_internal_tools;

-- Should error out as we uninstalled
select * from session_state.session_level_memory_consumption limit 0;