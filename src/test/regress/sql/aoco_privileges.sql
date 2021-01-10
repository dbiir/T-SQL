-- Create roles. --
CREATE ROLE new_aoco_table_owner;
CREATE ROLE user_with_privileges_from_first_owner;
CREATE ROLE user_with_privileges_from_second_owner;
CREATE ROLE user_with_no_privileges;

-- Create the table. --
CREATE TABLE aoco_privileges_table (a int) WITH (appendonly=true, orientation=column) DISTRIBUTED randomly;

-- Grant privileges to only one user
GRANT ALL ON aoco_privileges_table TO user_with_privileges_from_first_owner;
SELECT has_table_privilege('user_with_privileges_from_first_owner', 'aoco_privileges_table', 'SELECT');

-- The original owner should have privileges
SELECT has_table_privilege('aoco_privileges_table', 'SELECT');

-- The following users should not have privileges
SELECT has_table_privilege('new_aoco_table_owner', 'aoco_privileges_table', 'SELECT');
SELECT has_table_privilege('user_with_privileges_from_second_owner', 'aoco_privileges_table', 'SELECT');
SELECT has_table_privilege('user_with_no_privileges', 'aoco_privileges_table', 'SELECT');
SET ROLE user_with_no_privileges;
SELECT * FROM aoco_privileges_table;
RESET ROLE;

-- Now change ownership to the new owner
ALTER TABLE aoco_privileges_table OWNER TO new_aoco_table_owner;

-- The original owner should still have privileges
SELECT has_table_privilege('aoco_privileges_table', 'SELECT');
SELECT * FROM aoco_privileges_table;

-- The people to whom the original owner granted privileges should still have privileges
SELECT has_table_privilege('user_with_privileges_from_first_owner', 'aoco_privileges_table', 'SELECT');
SET ROLE user_with_privileges_from_first_owner;
SELECT * FROM aoco_privileges_table;

-- The new owner of the table should have privileges
SET ROLE new_aoco_table_owner;
SELECT has_table_privilege('aoco_privileges_table', 'SELECT');
SELECT * FROM aoco_privileges_table;

-- The new owner should be able to grant privileges
GRANT ALL ON aoco_privileges_table TO user_with_privileges_from_second_owner;
SELECT has_table_privilege('user_with_privileges_from_second_owner', 'aoco_privileges_table', 'SELECT');
SET ROLE user_with_privileges_from_second_owner;
SELECT * FROM aoco_privileges_table;

-- The no privileges user should have no privileges still
SET ROLE user_with_no_privileges;
SELECT has_table_privilege('aoco_privileges_table', 'SELECT');
SELECT * FROM aoco_privileges_table;

-- Revoke privileges
RESET ROLE;
REVOKE ALL ON aoco_privileges_table FROM user_with_privileges_from_first_owner;
SELECT has_table_privilege('user_with_privileges_from_first_owner', 'aoco_privileges_table', 'SELECT');

-- Clean up
DROP TABLE  aoco_privileges_table;
DROP ROLE new_aoco_table_owner;
DROP ROLE user_with_privileges_from_first_owner;
DROP ROLE user_with_privileges_from_second_owner;
DROP ROLE user_with_no_privileges;
