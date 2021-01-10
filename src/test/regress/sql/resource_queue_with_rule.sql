-- Given I am logged in as a non-super user
	CREATE USER resource_queue_test_user WITH PASSWORD 'testpwd';
	GRANT ALL PRIVILEGES ON DATABASE regression to resource_queue_test_user;
	set role resource_queue_test_user;

-- And I have created a rule linking two tables
	DROP TABLE IF EXISTS test_table;
	CREATE TABLE test_table (col1 INT NULL) DISTRIBUTED BY (col1);
	
	DROP TABLE IF EXISTS test_table_deletions;
	CREATE TABLE test_table_deletions (col1 INT NULL) DISTRIBUTED BY (col1);
	
	CREATE OR REPLACE RULE test_rule AS ON DELETE TO test_table
	        DO ALSO INSERT INTO test_table_deletions VALUES (OLD.col1);

-- When I run a query that triggers the rule
	INSERT INTO test_table VALUES (1);
	SELECT * FROM test_table;
	DELETE FROM test_table;
-- Then the output should not contain this error:
-- ERROR: out of shared memory adding portal increments;
-- Error while executing the query (SQLSTATE=53200) (7)

-- And there should not be any rows in the table:
	SELECT * FROM test_table;

-- Cleanup after test:
	RESET ROLE;
	DROP OWNED BY resource_queue_test_user CASCADE;
	DROP USER resource_queue_test_user;
