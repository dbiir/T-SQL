-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE adhoc11 ACTIVE THRESHOLD 1;

-- select from pg_resqueue table
select * from pg_resqueue where rsqname='adhoc11';

--create role and assign role to resource queue
CREATE ROLE role11 with LOGIN RESOURCE QUEUE adhoc11;

-- select role, resource queue details from pg_roles and pg_resqueue tables
SELECT rolname, rsqname FROM pg_roles AS r,pg_resqueue AS q WHERE r.rolresqueue=q.oid and rolname='role11';

-- drop role
DROP ROLE role11;

-- drop resource queue
DROP RESOURCE QUEUE adhoc11;
-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE adhoc12 ACTIVE THRESHOLD 2;

-- select from pg_resqueue table
select * from pg_resqueue where rsqname='adhoc12';

-- drop resource queue
DROP RESOURCE QUEUE adhoc12;

-- select from pg_resqueue table
--select * from pg_resqueue;
-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE adhoc13 ACTIVE THRESHOLD 2;
CREATE RESOURCE QUEUE adhoc14 ACTIVE THRESHOLD 3;

-- select from pg_resqueue table
select * from pg_resqueue where rsqname='adhoc13' and rsqname='adhoc14';

--create role and assign role to resource queue
CREATE ROLE role13 with LOGIN RESOURCE QUEUE adhoc13;
CREATE ROLE role14 with LOGIN RESOURCE QUEUE adhoc14;

-- select role, resource queue details from pg_roles and pg_resqueue tables
SELECT rolname, rsqname FROM pg_roles AS r,pg_resqueue AS q WHERE r.rolresqueue=q.oid and rolname in ('role13','role14');

-- drop role
DROP ROLE IF EXISTS role13;
DROP ROLE IF EXISTS role14;

-- drop resource queue
DROP RESOURCE QUEUE adhoc13;
DROP RESOURCE QUEUE adhoc14;
-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE adhoc1 ACTIVE THRESHOLD 1;
CREATE RESOURCE QUEUE adhoc2 ACTIVE THRESHOLD 2;
CREATE RESOURCE QUEUE adhoc3 ACTIVE THRESHOLD 3;
--CREATE RESOURCE QUEUE adhoc4 ACTIVE THRESHOLD 4;

-- select from pg_resqueue table
--select * from pg_resqueue;

-- drop resource queue
DROP RESOURCE QUEUE adhoc1;
DROP RESOURCE QUEUE adhoc2;
DROP RESOURCE QUEUE adhoc3;
--DROP RESOURCE QUEUE adhoc4;

-- select from pg_resqueue table
--select * from pg_resqueue;

-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE adhoc1 ACTIVE THRESHOLD 1;
CREATE RESOURCE QUEUE webuser1 ACTIVE THRESHOLD 3;
CREATE RESOURCE QUEUE mgmtuser1 ACTIVE THRESHOLD 5;

-- select from pg_resqueue table
select * from pg_resqueue where rsqname in ('adhoc1','webuser1','mgmtuser1');

-- drop resource queue
DROP RESOURCE QUEUE adhoc1;
DROP RESOURCE QUEUE webuser1;
DROP RESOURCE QUEUE mgmtuser1;


-- select from pg_resqueue table
--select * from pg_resqueue;
-- Test Workload Administration and Resource queuing

-- create resource queue and assuming that 8 is the maximum limit for the resource queue and 1 resource queue is already present in the table
-- it will give error if it crosses the maximum limit of resource queue
CREATE RESOURCE QUEUE adhoc1 ACTIVE THRESHOLD 1;
CREATE RESOURCE QUEUE webuser1 ACTIVE THRESHOLD 3;
CREATE RESOURCE QUEUE mgmtuser1 ACTIVE THRESHOLD 5;
CREATE RESOURCE QUEUE mgmtuser2 ACTIVE THRESHOLD 7;
CREATE RESOURCE QUEUE mgmtuser3 ACTIVE THRESHOLD 9;
CREATE RESOURCE QUEUE mgmtuser4 ACTIVE THRESHOLD 8;
CREATE RESOURCE QUEUE mgmtuser5 ACTIVE THRESHOLD 6;
CREATE RESOURCE QUEUE mgmtuser6 ACTIVE THRESHOLD 2;

-- select from pg_resqueue table
select * from pg_resqueue;

-- drop resource queue
DROP RESOURCE QUEUE adhoc1;
DROP RESOURCE QUEUE webuser1;
DROP RESOURCE QUEUE mgmtuser1;
DROP RESOURCE QUEUE mgmtuser2;
DROP RESOURCE QUEUE mgmtuser3;
DROP RESOURCE QUEUE mgmtuser4;
DROP RESOURCE QUEUE mgmtuser5;
DROP RESOURCE QUEUE mgmtuser6;

-- select from pg_resqueue table
select * from pg_resqueue;
-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE bob ACTIVE THRESHOLD 1;

-- select from pg_resqueue table
select * from pg_resqueue where rsqname='bob';

-- ALTER Resource Queue
ALTER RESOURCE QUEUE bob ACTIVE THRESHOLD 7;

-- select from pg_resqueue table
select * from pg_resqueue where rsqname='bob';

-- drop resource queue
DROP RESOURCE QUEUE bob;

-- select from pg_resqueue table
-- select * from pg_resqueue;
-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE sameera ACTIVE THRESHOLD 2;

-- select from pg_resqueue table
select * from pg_resqueue where rsqname='sameera';

--create role and assign role to resource queue
CREATE ROLE aryan with LOGIN RESOURCE QUEUE sameera;

-- ALTER Resource Queue
ALTER ROLE aryan RESOURCE QUEUE none;

-- select role, resource queue details from pg_roles and pg_resqueue tables
SELECT rolname, rsqname FROM pg_roles AS r,pg_resqueue AS q WHERE r.rolresqueue=q.oid and rolname='aryan';

-- drop role
DROP ROLE aryan;

-- drop resource queue
DROP RESOURCE QUEUE sameera;

-- select from pg_resqueue table
--select * from pg_resqueue;
-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE ram ACTIVE THRESHOLD 1;

-- select from pg_resqueue table
select * from pg_resqueue where rsqname='ram';

--create role and assign role to resource queue
CREATE ROLE sita with LOGIN RESOURCE QUEUE ram;
CREATE ROLE samrat with LOGIN RESOURCE QUEUE ram;

-- ALTER ROLE
ALTER ROLE sita RESOURCE QUEUE ram;
ALTER ROLE samrat RESOURCE QUEUE ram;

-- select role, resource queue details from pg_roles and pg_resqueue tables
SELECT rolname, rsqname FROM pg_roles AS r,pg_resqueue AS q WHERE r.rolresqueue=q.oid and rsqname='ram';

-- drop role
DROP ROLE sita;
DROP ROLE samrat;


-- drop resource queue
DROP RESOURCE QUEUE ram;
-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE camy11 COST THRESHOLD 200.0;
CREATE RESOURCE QUEUE camy22 COST THRESHOLD 500.0;


-- select from pg_resqueue table
select * from pg_resqueue_status where rsqname in ('camy11','camy22');

--create role and assign role to resource queue
CREATE ROLE creig11 with LOGIN RESOURCE QUEUE camy11;
CREATE ROLE creig22 with LOGIN RESOURCE QUEUE camy22;

-- ALTER Resource Queue
ALTER ROLE creig11 RESOURCE QUEUE camy11;
ALTER ROLE creig22 RESOURCE QUEUE camy22;

-- drop role
DROP ROLE creig11;
DROP ROLE creig22;

-- drop resource queue
DROP RESOURCE QUEUE camy11;
DROP RESOURCE QUEUE camy22;

-- select from pg_resqueue_status table
--select * from pg_resqueue_status;
-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE tom ACTIVE THRESHOLD 20;

-- ALTER RESOURCE QUEUE
ALTER RESOURCE QUEUE tom COST THRESHOLD 100.0;

-- select from pg_resqueue table
select * from pg_resqueue_status where rsqname='tom';

-- drop resource queue
DROP RESOURCE QUEUE tom;

-- select from pg_resqueue_status table
--select * from pg_resqueue_status;



-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE myq11 ACTIVE THRESHOLD 10;

-- ALTER RESOURCE QUEUE
ALTER RESOURCE QUEUE myq11 COST THRESHOLD 50.0;
ALTER RESOURCE QUEUE myq11 COST THRESHOLD 3e+7;

-- select from pg_resqueue table
select * from pg_resqueue_status where rsqname='myq11';

-- drop resource queue
DROP RESOURCE QUEUE myq11;

-- select from pg_resqueue_status table
--select * from pg_resqueue_status;



-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE myq21 ACTIVE THRESHOLD 7;

-- ALTER RESOURCE QUEUE
ALTER RESOURCE QUEUE myq21 COST THRESHOLD 70.0;
ALTER RESOURCE QUEUE myq21 COST THRESHOLD 3e+9 NOOVERCOMMIT;

-- select from pg_resqueue table
select * from pg_resqueue_status where rsqname='myq21';

-- drop resource queue
DROP RESOURCE QUEUE myq21;

-- select from pg_resqueue_status table
--select * from pg_resqueue_status;



-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE tom11 ACTIVE THRESHOLD 20;

--create role and assign role to resource queue
CREATE ROLE shaun11 with LOGIN RESOURCE QUEUE tom11;

-- select from pg_resqueue, pg_roles table
SELECT rolname, rsqname FROM pg_roles, pg_resqueue WHERE pg_roles.rolresqueue=pg_resqueue.oid and rolname='shaun11';

-- create a view
CREATE VIEW role2que AS SELECT rolname, rsqname FROM pg_roles, pg_resqueue WHERE pg_roles.rolresqueue=pg_resqueue.oid and rolname='shaun11';

-- select from view
select * from role2que where rolname='shaun11';

-- drop role name
DROP ROLE shaun11;

-- drop resource queue
DROP RESOURCE QUEUE tom11;

-- select from view
--select * from role2que;

-- drop view
DROP VIEW role2que;

-- Test Workload Administration and Resource queuing

-- create resource queue
CREATE RESOURCE QUEUE tom55 ACTIVE THRESHOLD 10;

--create role and assign role to resource queue
CREATE ROLE shaun55 with LOGIN RESOURCE QUEUE tom55;

-- select from pg_resqueue, pg_role table
SELECT rolname, rsqname, pg_locks.pid, granted, query,datname FROM pg_roles, pg_resqueue, pg_locks, pg_stat_activity WHERE pg_roles.rolresqueue=pg_locks.objid AND pg_locks.objid=pg_resqueue.oid AND pg_stat_activity.pid=pg_locks.pid;

-- create a view
CREATE VIEW resq_procs AS SELECT rolname, rsqname, pg_locks.pid, granted, query,datname FROM pg_roles, pg_resqueue, pg_locks, pg_stat_activity WHERE pg_roles.rolresqueue=pg_locks.objid AND pg_locks.objid=pg_resqueue.oid AND pg_stat_activity.pid=pg_locks.pid;

-- select from view
select * from resq_procs;

-- drop role name
DROP ROLE shaun55;

-- drop resource queue
DROP RESOURCE QUEUE tom55;

-- select from view
--select * from resq_procs;

-- drop view
DROP VIEW resq_procs;


