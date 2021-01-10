create table my_first_table ( a int) distributed by (a);

--
CREATE TABLE mdt_test_part1 (id int, rank int, year date, gender
char(1)) DISTRIBUTED BY (id, gender, year)
partition by list (gender)
subpartition by range (year)
subpartition template (
start (date '2001-01-01'))
(
values ('M'),
values ('F')
);

alter table mdt_test_part1 add default partition default_part;

drop table mdt_test_part1;


--
create table mdt_part_tbl_add (a char, b int, d char)
partition by range (b)
subpartition by list (d)
subpartition template (
 subpartition sp1 values ('a'),
 subpartition sp2 values ('b'))
(
start (1) end (10) every (5)
);

alter table mdt_part_tbl_add set subpartition template ();
alter table mdt_part_tbl_add add partition p3 end (13) (subpartition sp3 values ('c'));

drop table mdt_part_tbl_add;


--
CREATE TABLE mdt_part_tbl (
id int,
rank int,
year int,
gender char(1),
count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
SUBPARTITION BY RANGE (year)
SUBPARTITION TEMPLATE (
SUBPARTITION year1 START (2001),
SUBPARTITION year2 START (2002),
SUBPARTITION year6 START (2006) END (2007) )
(PARTITION girls VALUES ('F'),
PARTITION boys VALUES ('M') );


alter table mdt_part_tbl alter partition girls add default partition gfuture;
alter table mdt_part_tbl alter partition boys add default partition bfuture;

drop table mdt_part_tbl;


--
CREATE TABLE mdt_test_part1 (id int, rank int, year date, gender
char(1)) DISTRIBUTED BY (id, gender, year)
partition by list (gender)
subpartition by range (year)
subpartition template (
start (date '2001-01-01'))
(
values ('M'),
values ('F')
);

alter table mdt_test_part1 add default partition default_part;
alter table mdt_test_part1 DROP default partition if exists;

drop table mdt_test_part1;


--
create table mdt_part_tbl (aa date, bb date) partition by range (bb)
(partition foo start('2008-01-01'));

alter table mdt_part_tbl add partition a2 start ('2007-02-01') end ('2007-03-01');
alter table mdt_part_tbl DROP partition a2;
alter table mdt_part_tbl DROP partition if exists a2;

drop table mdt_part_tbl;


--
CREATE TABLE mdt_part_tbl_partrange (
        unique1         int4,
        unique2         int4
) partition by range (unique1)
( partition aa start (0) end (500) every (100), default partition default_part );


CREATE TABLE mdt_part_tbl_partrange_A (
        unique1         int4,
        unique2         int4);

alter table mdt_part_tbl_partrange exchange default partition with table mdt_part_tbl_partrange_A with validation;

drop table mdt_part_tbl_partrange;
drop table mdt_part_tbl_partrange_A;


--
create table mdt_part_tbl_rename (aa date, bb date) partition by range (bb)
(partition foo start('2008-01-01'));

alter table mdt_part_tbl_rename add partition a2 start ('2007-02-01') end ('2007-03-01');
alter table mdt_part_tbl_rename rename partition a2 to aa2;

drop table mdt_part_tbl_rename;



--
CREATE TABLE mdt_test_part1 (id int, rank int, year date, gender
char(1)) DISTRIBUTED BY (id, gender, year)
partition by list (gender)
subpartition by range (year)
subpartition template (
start (date '2001-01-01'))
(
values ('M'),
values ('F')
);

alter table mdt_test_part1_1_prt_1_2_prt_1 set distributed randomly;

ALTER TABLE mdt_test_part1 ALTER PARTITION FOR('M'::bpchar) alter PARTITION FOR(RANK(1)) set distributed by (id, gender, year);

drop table mdt_test_part1;



--
create table mdt_part_tbl_subpartition (a char, b int, d char)
partition by range (b)
subpartition by list (d)
subpartition template (
 subpartition sp1 values ('a'),
 subpartition sp2 values ('b'))
(
start (1) end (4) every (2)
);

alter table mdt_part_tbl_subpartition set subpartition template ();
alter table mdt_part_tbl_subpartition add partition p3 end (13) (subpartition sp3 values ('c'));
alter table mdt_part_tbl_subpartition set subpartition template (subpartition sp3 values ('c'));

drop table mdt_part_tbl_subpartition;



--
create table mdt_part_tbl_split_list (a text, b text) partition by list (a) (partition foo values ('foo'), partition bar values ('bar'), default partition baz);

alter table mdt_part_tbl_split_list split default partition at ('baz') into (partition bing, default partition);

drop table mdt_part_tbl_split_list;



--
--ADD table_constraint

          CREATE TABLE mdt_distributors (
          did integer,
          name varchar(40)
          ) DISTRIBUTED BY (name);

          ALTER TABLE mdt_distributors ADD UNIQUE(name);

--DROP CONSTRAINT constraint_name [ RESTRICT | CASCADE ]

          CREATE TABLE mdt_films (
          code char(5),
          title varchar(40),
          did integer,
          date_prod date,
          kind varchar(10),
          len interval hour to minute,
          CONSTRAINT production UNIQUE(date_prod)
          ) distributed by (date_prod);

          ALTER TABLE mdt_films DROP CONSTRAINT production RESTRICT;

          CREATE TABLE mdt_films1 (
          code char(5),
          title varchar(40),
          did integer,
          date_prod date,
          kind varchar(10),
          len interval hour to minute,
          CONSTRAINT production UNIQUE(date_prod)
          ) distributed by (date_prod);

          ALTER TABLE mdt_films1 DROP CONSTRAINT production CASCADE;

drop table mdt_distributors;
drop table mdt_films;
drop table mdt_films1;


--
--drop toast column    
CREATE TABLE mdt_test_column(
    toast_col text,
    bigint_col bigint,
    char_vary_col character varying(30),
    numeric_col numeric,
    int_col int4,
    float_col float4,
    int_array_col int[],
    non_toast_col numeric,
    a_ts_without timestamp without time zone,
    b_ts_with timestamp with time zone,
    date_column date,
    col_with_constraint numeric UNIQUE,
    col_with_default_text character varying(30) DEFAULT 'test1'
    ) distributed by (col_with_constraint);

    ALTER TABLE mdt_test_column DROP COLUMN toast_col ;

--drop non toast column
    CREATE TABLE mdt_test_column1(
    toast_col text,
    bigint_col bigint,
    char_vary_col character varying(30),
    numeric_col numeric,
    int_col int4,
    float_col float4,
    int_array_col int[],
    non_toast_col numeric,
    a_ts_without timestamp without time zone,
    b_ts_with timestamp with time zone,
    date_column date,
    col_with_constraint numeric UNIQUE,
    col_with_default_text character varying(30) DEFAULT 'test1'
    ) distributed by (col_with_constraint);

    ALTER TABLE mdt_test_column1 DROP COLUMN non_toast_col ;

drop table mdt_test_column;
drop table mdt_test_column1;




--INHERIT & NO INHERIT mdt_parent_table

          CREATE TABLE mdt_parent_table (
          text_col text,
          bigint_col bigint,
          char_vary_col character varying(30),
          numeric_col numeric
          ) DISTRIBUTED RANDOMLY;

          CREATE TABLE mdt_child_table(
          text_col text,
          bigint_col bigint,
          char_vary_col character varying(30),
          numeric_col numeric
          ) DISTRIBUTED RANDOMLY;

          CREATE TABLE mdt_child_table1(
          text_col text,
          bigint_col bigint,
          char_vary_col character varying(30),
          numeric_col numeric
          ) DISTRIBUTED RANDOMLY;

          ALTER TABLE mdt_child_table INHERIT mdt_parent_table;

          ALTER TABLE mdt_child_table1 INHERIT mdt_parent_table;
          ALTER TABLE mdt_child_table1 NO INHERIT mdt_parent_table;

drop table mdt_child_table ;
drop table mdt_child_table1 ;
drop table mdt_parent_table ;



--
--OWNER TO new_owner

          CREATE TABLE mdt_table_owner (
          text_col text,
          bigint_col bigint,
          char_vary_col character varying(30),
          numeric_col numeric
          )DISTRIBUTED RANDOMLY;

          CREATE ROLE mdt_user1;

          ALTER TABLE mdt_table_owner OWNER TO mdt_user1;

drop table mdt_table_owner;
drop role mdt_user1;



--
--ALTER Schema name

          CREATE SCHEMA mdt_dept;
          CREATE TABLE mdt_dept.mdt_csc(
          stud_id int,
          stud_name varchar(20)
          ) DISTRIBUTED RANDOMLY;

          CREATE SCHEMA mdt_new_dept;
          ALTER TABLE mdt_dept.mdt_csc SET SCHEMA mdt_new_dept;

drop table mdt_new_dept.mdt_csc;
drop schema mdt_new_dept;
drop schema mdt_dept;



--
--SET WITHOUT OIDS

          CREATE TABLE mdt_table_with_oid (
          text_col text,
          bigint_col bigint,
          char_vary_col character varying(30),
          numeric_col numeric
          ) WITH OIDS DISTRIBUTED RANDOMLY;

          ALTER TABLE mdt_table_with_oid SET WITHOUT OIDS;

drop table mdt_table_with_oid;
 


--
--SET & RESET ( storage_parameter = value , ... )

          CREATE TABLE mdt_table_set_storage_parameters (
          text_col text,
          bigint_col bigint,
          char_vary_col character varying(30),
          numeric_col numeric
          ) with (APPENDONLY=TRUE) DISTRIBUTED RANDOMLY;

          ALTER TABLE mdt_table_set_storage_parameters SET WITH (COMPRESSLEVEL= 5);

          CREATE TABLE mdt_table_set_storage_parameters1 (
          text_col text,
          bigint_col bigint,
          char_vary_col character varying(30),
          numeric_col numeric
          ) with (APPENDONLY=TRUE) DISTRIBUTED RANDOMLY;

          ALTER TABLE mdt_table_set_storage_parameters1 SET WITH (FILLFACTOR=50);

          CREATE TABLE mdt_table_set_storage_parameters2 (
          text_col text,
          bigint_col bigint,
          char_vary_col character varying(30),
          numeric_col numeric
          ) with (APPENDONLY=TRUE) DISTRIBUTED RANDOMLY;

          ALTER TABLE mdt_table_set_storage_parameters2 SET WITH (FILLFACTOR=50);
          ALTER TABLE mdt_table_set_storage_parameters2 RESET (FILLFACTOR);


drop table mdt_table_set_storage_parameters;
drop table mdt_table_set_storage_parameters1;
drop table mdt_table_set_storage_parameters2;


--
--ENABLE & DISABLE TRIGGER

          CREATE TABLE mdt_price_change (
          apn CHARACTER(15) NOT NULL,
          effective TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          price NUMERIC(9,2),
          UNIQUE (apn, effective)
          ) distributed by (apn, effective);

          CREATE TABLE mdt_stock(
          mdt_stock_apn CHARACTER(15) NOT NULL,
          mdt_stock_effective TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          mdt_stock_price NUMERIC(9,2)
          )DISTRIBUTED RANDOMLY;


          CREATE TABLE mdt_stock1(
          mdt_stock_apn CHARACTER(15) NOT NULL,
          mdt_stock_effective TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          mdt_stock_price NUMERIC(9,2)
          )DISTRIBUTED RANDOMLY;

          --trigger function to insert records as required:

          CREATE OR REPLACE FUNCTION mdt_insert_mdt_price_change() RETURNS trigger AS '
          DECLARE
          changed boolean;
          BEGIN
          IF tg_op = ''DELETE'' THEN
          INSERT INTO mdt_price_change(apn, effective, price)
          VALUES (old.barcode, CURRENT_TIMESTAMP, NULL);
          RETURN old;
          END IF;
          IF tg_op = ''INSERT'' THEN
          changed := TRUE;
          ELSE
          changed := new.price IS NULL != old.price IS NULL OR new.price != old.price;
          END IF;
          IF changed THEN
          INSERT INTO mdt_price_change(apn, effective, price)
          VALUES (new.barcode, CURRENT_TIMESTAMP, new.price);
          END IF;
          RETURN new;
          END
          ' LANGUAGE plpgsql MODIFIES SQL DATA;

          --create a trigger on the table you wish to monitor:

          CREATE TRIGGER mdt_insert_mdt_price_change AFTER INSERT OR DELETE OR UPDATE ON mdt_stock
          FOR EACH ROW EXECUTE PROCEDURE mdt_insert_mdt_price_change();

          CREATE TRIGGER mdt_insert_mdt_price_change1 AFTER INSERT OR DELETE OR UPDATE ON mdt_stock1
          FOR EACH ROW EXECUTE PROCEDURE mdt_insert_mdt_price_change();


          ALTER TABLE mdt_stock DISABLE TRIGGER mdt_insert_mdt_price_change;
          ALTER TABLE mdt_stock1 ENABLE TRIGGER mdt_insert_mdt_price_change1;

drop table mdt_price_change;
drop table mdt_stock;
drop table mdt_stock1;
drop function mdt_insert_mdt_price_change();



--
--CLUSTER ON index_name & SET WITHOUT CLUSTER

          CREATE TABLE mdt_cluster_index_table (col1 int,col2 int) distributed randomly;

          create index mdt_clusterindex on mdt_cluster_index_table(col1);
          ALTER TABLE mdt_cluster_index_table CLUSTER on mdt_clusterindex;

          CREATE TABLE mdt_cluster_index_table1 (col1 int,col2 int) distributed randomly;

          create index mdt_clusterindex1 on mdt_cluster_index_table1(col1);
          ALTER TABLE mdt_cluster_index_table1 CLUSTER on mdt_clusterindex1;
          ALTER TABLE mdt_cluster_index_table1 SET WITHOUT CLUSTER;

drop table mdt_cluster_index_table;
drop table mdt_cluster_index_table1;



--
CREATE TABLE mdt_st_GistTable1 (
 id INTEGER,
 property BOX, 
 filler VARCHAR DEFAULT 'This is here just to take up space so that we use more pages of data and sequential scans take a lot more time.  Stones tinheads and mixers coming; we did it all on our own; this summer I hear the crunching; 11 dead in Ohio. Got right down to it; we were cutting us down; could have had fun but, no; left them face down dead on the ground.  How can you listen when you know?'
 )
 DISTRIBUTED BY (id);


INSERT INTO mdt_st_GistTable1 (id, property) VALUES (1, '( (0,0), (1,1) )');
INSERT INTO mdt_st_GistTable1 (id, property) VALUES (2, '( (0,0), (2,2) )');

CREATE INDEX mdt_st_GistIndex1 ON mdt_st_GistTable1 USING GiST (property);
CREATE INDEX mdt_st_GistIndex2 ON mdt_st_GistTable1 USING GiST (property);
CREATE INDEX mdt_st_GistIndex3 ON mdt_st_GistTable1 USING GiST (property);

ALTER INDEX mdt_st_GistIndex1 RENAME TO mdt_new_st_GistIndex1;
ALTER INDEX mdt_new_st_GistIndex1 RENAME TO mdt_st_GistIndex1;
ALTER INDEX mdt_st_GistIndex2 SET (fillfactor =100);
ALTER INDEX mdt_st_GistIndex3 SET (fillfactor =100);
ALTER INDEX mdt_st_GistIndex3 RESET (fillfactor) ;

drop table mdt_st_GistTable1;



--
CREATE ROLE mdt_grp_role1;
CREATE ROLE mdt_grp_role2;
CREATE GROUP mdt_db_group1 WITH SUPERUSER CREATEDB  INHERIT LOGIN CONNECTION LIMIT  1 ENCRYPTED PASSWORD 'passwd';
CREATE GROUP mdt_db_grp2 WITH NOSUPERUSER NOCREATEDB  NOINHERIT NOLOGIN  UNENCRYPTED PASSWORD 'passwd';
CREATE GROUP mdt_db_grp3 WITH NOCREATEROLE NOCREATEUSER;
CREATE GROUP mdt_db_grp4 WITH CREATEROLE CREATEUSER;
CREATE GROUP mdt_db_grp5 WITH VALID UNTIL '2009-02-13 01:51:15';
CREATE GROUP mdt_db_grp6 WITH IN ROLE mdt_grp_role1; 
CREATE GROUP mdt_db_grp7 WITH IN GROUP mdt_db_group1; 
CREATE GROUP mdt_db_grp8 WITH ROLE mdt_grp_role2;
CREATE GROUP mdt_db_grp9 WITH ADMIN mdt_db_grp8;
CREATE GROUP mdt_db_grp10 WITH USER mdt_db_group1;
CREATE GROUP mdt_db_grp11 SYSID 100 ;
CREATE RESOURCE QUEUE mdt_grp_rsq1 ACTIVE THRESHOLD 1;
CREATE GROUP mdt_db_grp12 RESOURCE QUEUE mdt_grp_rsq1;


CREATE USER mdt_test_user_1;
ALTER GROUP mdt_db_grp7 ADD USER mdt_test_user_1;
ALTER GROUP mdt_db_grp12 ADD USER mdt_test_user_1;
ALTER GROUP mdt_db_grp12 DROP USER mdt_test_user_1;
ALTER GROUP mdt_db_grp11 RENAME TO mdt_new_db_grp11;


drop role mdt_grp_role1;
drop role mdt_grp_role2;
drop group mdt_db_group1;;
drop group mdt_db_grp2;
drop group mdt_db_grp3;
drop group mdt_db_grp4;
drop group mdt_db_grp5;
drop group mdt_db_grp6;
drop group mdt_db_grp7;
drop group mdt_db_grp8;
drop group mdt_db_grp9;
drop group mdt_db_grp10;
drop group mdt_new_db_grp11;
drop group mdt_db_grp12;
drop resource queue mdt_grp_rsq1;
drop user mdt_test_user_1;



--
CREATE TABLE mdt_test_emp (ename varchar(20),eno int,salary int,ssn int,gender char(1)) distributed by (ename,eno,gender);

CREATE UNIQUE INDEX mdt_eno_idx ON mdt_test_emp (eno);
CREATE INDEX mdt_gender_bmp_idx ON mdt_test_emp USING bitmap (gender);
CREATE INDEX mdt_lower_ename_idex  ON mdt_test_emp ((upper(ename))) WHERE upper(ename)='JIM';
CREATE  INDEX mdt_ename_idx ON mdt_test_emp  (ename) WITH (fillfactor =80);
CREATE  INDEX mdt_ename_idx1 ON mdt_test_emp  (ename) WITH (fillfactor =80);

ALTER INDEX mdt_gender_bmp_idx RENAME TO mdt_new_gender_bmp_idx;
ALTER INDEX mdt_ename_idx SET (fillfactor =100);
ALTER INDEX mdt_ename_idx1 SET (fillfactor =100);
ALTER INDEX mdt_ename_idx1 RESET (fillfactor) ;

drop index mdt_eno_idx;
drop index mdt_new_gender_bmp_idx;
drop index mdt_lower_ename_idex;
drop index mdt_ename_idx ;
drop index mdt_ename_idx1 ;
drop table mdt_test_emp;



--
create schema myschema;
create schema myschema_new;

create table myschema.test_tbl_heap ( a int, b text) distributed by (a);
analyze myschema.test_tbl_heap;
vacuum myschema.test_tbl_heap;
ALTER TABLE myschema.test_tbl_heap  SET SCHEMA myschema_new;
analyze myschema_new.test_tbl_heap;
vacuum myschema_new.test_tbl_heap;

create table myschema.test_tbl_ao ( a int, b text) with (appendonly=true) distributed by (a);  
analyze myschema.test_tbl_ao;
vacuum myschema.test_tbl_ao;
ALTER TABLE myschema.test_tbl_ao  SET SCHEMA myschema_new;
analyze myschema_new.test_tbl_ao;
vacuum myschema_new.test_tbl_ao;

create table myschema.test_tbl_co ( a int, b text) with (appendonly=true, orientation='column') distributed by (a);
analyze myschema.test_tbl_co;
vacuum myschema.test_tbl_co;
ALTER TABLE myschema.test_tbl_co  SET SCHEMA myschema_new;
analyze myschema_new.test_tbl_co;
vacuum myschema_new.test_tbl_co;

create view myschema.test_view as SELECT * from myschema_new.test_tbl_heap;
create index test_tbl_idx on myschema_new.test_tbl_heap(a);
create sequence myschema.seq1 start with 101;
ALTER SEQUENCE myschema.seq1 SET SCHEMA myschema_new;


drop view myschema.test_view;
drop table myschema_new.test_tbl_heap;
drop table myschema_new.test_tbl_ao;
drop table myschema_new.test_tbl_co;
drop sequence myschema_new.seq1;
drop schema myschema;
drop schema myschema_new;



--
create schema myschema;
create schema myschema_new;
create schema myschema_diff;

CREATE TABLE myschema.mdt_supplier_hybrid_part(
                S_SUPPKEY INTEGER,
                S_NAME CHAR(25),
                S_ADDRESS VARCHAR(40),
                S_NATIONKEY INTEGER,
                S_PHONE CHAR(15),
                S_ACCTBAL decimal,
                S_COMMENT VARCHAR(101)
                )
partition by range (s_suppkey)
subpartition by list (s_nationkey) subpartition template (
        values('22','21','17'),
        values('6','11','1','7','16','2') WITH (orientation='column', appendonly=true),
        values('18','20') WITH (checksum=false, appendonly=true,blocksize=1171456, compresslevel=3),
        values('9','23','13') WITH (checksum=true,appendonly=true,blocksize=1335296,compresslevel=7),
        values('0','3','12','15','14','8','4','24','19','10','5')
)
(
partition p1 start('1') end('10001') every(10000)

);

Vacuum myschema.mdt_supplier_hybrid_part;
ALTER TABLE myschema.mdt_supplier_hybrid_part SET SCHEMA myschema_new;
Vacuum myschema_new.mdt_supplier_hybrid_part;
ALTER TABLE myschema.mdt_supplier_hybrid_part_1_prt_p1 SET SCHEMA myschema_new;
ALTER TABLE myschema.mdt_supplier_hybrid_part_1_prt_p1_2_prt_1 SET SCHEMA myschema_diff;
Vacuum myschema_diff.mdt_supplier_hybrid_part_1_prt_p1_2_prt_1;


drop table myschema_new.mdt_supplier_hybrid_part;
drop schema myschema;
drop schema myschema_new;
drop schema myschema_diff;



--
CREATE RESOURCE QUEUE db_resque_new1 ACTIVE THRESHOLD 2 COST THRESHOLD 2000.00;
CREATE RESOURCE QUEUE db_resque_new2 COST THRESHOLD 3000.00 OVERCOMMIT;
CREATE RESOURCE QUEUE db_resque_new3 ACTIVE THRESHOLD 2 COST THRESHOLD 2000.00;
CREATE RESOURCE QUEUE db_resque_new4 ACTIVE THRESHOLD 2 COST THRESHOLD 2000.00;
CREATE RESOURCE QUEUE db_resque_new5 COST THRESHOLD 3000.00 OVERCOMMIT;
CREATE RESOURCE QUEUE db_resque_new6 ACTIVE THRESHOLD 2 COST THRESHOLD 2000.00;
CREATE RESOURCE QUEUE db_resque_new7 ACTIVE THRESHOLD 2 COST THRESHOLD 2000.00;
CREATE RESOURCE QUEUE db_resque_new8 ACTIVE THRESHOLD 2 COST THRESHOLD 2000.00;

ALTER RESOURCE QUEUE db_resque_new1 with(priority=high, max_cost=300) without (cost_overcommit=false) ;
ALTER RESOURCE QUEUE db_resque_new2 without (cost_overcommit=false);
ALTER RESOURCE QUEUE db_resque_new3 with(priority=low); 
ALTER RESOURCE QUEUE db_resque_new4 with(MAX_COST=3.0);
ALTER RESOURCE QUEUE db_resque_new5 with(MIN_COST=1.0);
ALTER RESOURCE QUEUE db_resque_new6 with(ACTIVE_STATEMENTS=3);
ALTER RESOURCE QUEUE db_resque_new7 without(cost_overcommit=false);
ALTER RESOURCE QUEUE db_resque_new8 without(ACTIVE_STATEMENTS=2);

drop resource queue db_resque_new1;
drop resource queue db_resque_new2;
drop resource queue db_resque_new3;
drop resource queue db_resque_new4;
drop resource queue db_resque_new5;
drop resource queue db_resque_new6;
drop resource queue db_resque_new7;
drop resource queue db_resque_new8;




--
CREATE ROLE mdt_db_role1 WITH SUPERUSER CREATEDB  INHERIT LOGIN CONNECTION LIMIT  1 ENCRYPTED PASSWORD 'passwd';
CREATE ROLE mdt_db_role2 WITH NOSUPERUSER NOCREATEDB  NOINHERIT NOLOGIN  UNENCRYPTED PASSWORD 'passwd';
CREATE ROLE mdt_db_role3 WITH NOCREATEROLE NOCREATEUSER;
CREATE ROLE mdt_db_role4 WITH CREATEROLE CREATEUSER;
CREATE ROLE mdt_db_role5 WITH VALID UNTIL '2009-02-13 01:51:15';
CREATE ROLE mdt_db_role6 WITH IN ROLE mdt_db_role1;
CREATE GROUP mdt_db_grp1;
CREATE ROLE mdt_db_role7 WITH IN GROUP mdt_db_grp1;
CREATE ROLE mdt_db_role8 WITH ROLE mdt_db_role7;
CREATE ROLE mdt_db_role9 WITH ADMIN mdt_db_role8;
CREATE ROLE mdt_db_role10 WITH USER mdt_db_role1;
CREATE ROLE mdt_db_role11 SYSID 100 ;
CREATE RESOURCE QUEUE mdt_db_resque1 ACTIVE THRESHOLD 2 COST THRESHOLD 2000.00;
CREATE ROLE  mdt_db_role12 RESOURCE QUEUE mdt_db_resque1;
CREATE role mdt_db_role13 ;
CREATE role mdt_db_role14;
CREATE role mdt_db_role15;
CREATE role mdt_db_role16;
CREATE role mdt_db_role17;
CREATE role mdt_db_role18 login;
CREATE role mdt_db_role19 login ENCRYPTED PASSWORD 'passwd';
CREATE role mdt_db_role20;
CREATE role mdt_db_role21;
CREATE role mdt_db_role22;
CREATE role mdt_db_role23;
CREATE role mdt_db_role24;
CREATE role mdt_db_role25;
CREATE role mdt_db_role26;
CREATE role mdt_db_role27;

ALTER ROLE mdt_db_role1 WITH NOSUPERUSER NOCREATEDB  NOINHERIT NOLOGIN  UNENCRYPTED PASSWORD 'passwd';
ALTER ROLE mdt_db_role2 WITH SUPERUSER CREATEDB  INHERIT LOGIN CONNECTION LIMIT  1 ENCRYPTED PASSWORD 'passwd';
ALTER ROLE mdt_db_role3 WITH CREATEROLE CREATEUSER;
ALTER ROLE mdt_db_role4 WITH NOCREATEROLE NOCREATEUSER;
ALTER ROLE mdt_db_role5 WITH VALID UNTIL '2009-06-13 01:51:15';
ALTER ROLE mdt_db_role6 WITH CONNECTION LIMIT 5;
ALTER ROLE mdt_db_role7 WITH RESOURCE QUEUE mdt_db_resque1;
ALTER ROLE mdt_db_role8 RENAME TO mdt_new_role8;
CREATE SCHEMA mdt_db_schema1;
ALTER ROLE mdt_db_role9 SET search_path TO mdt_db_schema1;
ALTER ROLE mdt_db_role10 SET search_path TO mdt_db_schema1;
ALTER ROLE mdt_db_role10 RESET search_path ;
ALTER role mdt_db_role13 WITH SUPERUSER;
ALTER role mdt_db_role14 WITH CREATEDB;
ALTER role mdt_db_role15 WITH INHERIT;
ALTER role mdt_db_role16 WITH LOGIN;
ALTER role mdt_db_role17 WITH CONNECTION LIMIT  1;
ALTER role mdt_db_role18 WITH ENCRYPTED PASSWORD 'passwd';;
ALTER role mdt_db_role19 WITH UNENCRYPTED PASSWORD 'passwd';
ALTER role mdt_db_role20 WITH NOSUPERUSER;
ALTER role mdt_db_role21 WITH NOCREATEDB;
ALTER role mdt_db_role22 WITH NOINHERIT;
ALTER role mdt_db_role23 WITH NOLOGIN;
ALTER role mdt_db_role24 WITH CREATEROLE;
ALTER role mdt_db_role25 WITH NOCREATEROLE;
ALTER role mdt_db_role26 WITH CREATEUSER;
ALTER role mdt_db_role27 WITH NOCREATEUSER;

drop role mdt_db_grp1 ;
drop role mdt_db_role1 ;
drop role mdt_db_role2 ;
drop role mdt_db_role3 ;
drop role mdt_db_role4 ;
drop role mdt_db_role5 ;
drop role mdt_db_role6 ;
drop role mdt_db_role7 ;
drop role mdt_new_role8 ;
drop role mdt_db_role9 ;
drop role mdt_db_role10 ;
drop role mdt_db_role11 ;
drop role mdt_db_role12 ;
drop schema mdt_db_schema1;
drop resource queue mdt_db_resque1;
drop role mdt_db_role13 ;
drop role mdt_db_role14;
drop role mdt_db_role15;
drop role mdt_db_role16;
drop role mdt_db_role17;
drop role mdt_db_role18;
drop role mdt_db_role19;
drop role mdt_db_role20;
drop role mdt_db_role21;
drop role mdt_db_role22;
drop role mdt_db_role23;
drop role mdt_db_role24;
drop role mdt_db_role25;
drop role mdt_db_role26;
drop role mdt_db_role27;



--
CREATE TEMPORARY SEQUENCE  mdt_db_seq1 START WITH 101;
CREATE SEQUENCE  mdt_db_seq9 START WITH 101;
CREATE TEMP SEQUENCE  mdt_db_seq2 START 101;
CREATE SEQUENCE  mdt_db_seq10 START 101;
CREATE SEQUENCE mdt_db_seq3  INCREMENT BY 2 MINVALUE 1 MAXVALUE  100;
CREATE SEQUENCE mdt_db_seq4  INCREMENT BY 2 NO MINVALUE  NO MAXVALUE ;
CREATE SEQUENCE mdt_db_seq5  INCREMENT BY 2 MINVALUE 1 MAXVALUE  100  CACHE 100 CYCLE;
CREATE SEQUENCE mdt_db_seq6  INCREMENT BY 2 MINVALUE 1 MAXVALUE  100  NO CYCLE;
CREATE SEQUENCE mdt_db_seq7 START 101 OWNED BY NONE;
CREATE TABLE mdt_test_tbl ( col1 int, col2 int) DISTRIBUTED RANDOMLY;
INSERT INTO mdt_test_tbl values (generate_series(1,100),generate_series(1,100));
CREATE SEQUENCE mdt_db_seq8 START 101 OWNED BY mdt_test_tbl.col1;



ALTER SEQUENCE mdt_db_seq1 RESTART WITH 100;
ALTER SEQUENCE mdt_db_seq9 RESTART WITH 100;
ALTER SEQUENCE mdt_db_seq2 INCREMENT BY 2 MINVALUE 101 MAXVALUE  400  CACHE 100 CYCLE;
ALTER SEQUENCE mdt_db_seq10 INCREMENT BY 2 MINVALUE 101 MAXVALUE  400  CACHE 100 CYCLE;
ALTER SEQUENCE mdt_db_seq3  INCREMENT BY 2 NO MINVALUE  NO MAXVALUE;
ALTER SEQUENCE mdt_db_seq4 INCREMENT BY 2 MINVALUE 1 MAXVALUE  100;
ALTER SEQUENCE mdt_db_seq5 NO CYCLE;
CREATE SCHEMA mdt_db_schema9;
ALTER SEQUENCE mdt_db_seq6 SET SCHEMA mdt_db_schema9;
ALTER SEQUENCE mdt_db_seq7  OWNED BY mdt_test_tbl.col2;
ALTER SEQUENCE mdt_db_seq7  OWNED BY NONE;

drop sequence if exists mdt_db_seq1;
drop sequence mdt_db_seq2 cascade;
drop sequence mdt_db_seq3 restrict;
drop sequence mdt_db_seq4;
drop sequence mdt_db_seq5;
drop sequence mdt_db_schema9.mdt_db_seq6;
drop sequence mdt_db_seq7;
drop sequence mdt_db_seq8;
drop schema mdt_db_schema9 ;
drop table mdt_test_tbl;
drop sequence mdt_db_seq9;
drop sequence mdt_db_seq10;



--
create table mdt_test_reindex_tbl (col1 int,col2 int) distributed randomly;
create index mdt_clusterindex on mdt_test_reindex_tbl(col1);
REINDEX index "public".mdt_clusterindex;

drop table mdt_test_reindex_tbl;



--
-- Table with all data types

CREATE TABLE mdt_all_types( bit1 bit(1), bit2 bit varying(50), boolean1 boolean, char1 char(50), charvar1 character varying(50), char2 character(50),
varchar1 varchar(50),date1 date,dp1 double precision,int1 integer,interval1 interval,numeric1 numeric,real1 real,smallint1 smallint,time1 time,bigint1 bigint,
bigserial1 bigserial,bytea1 bytea,cidr1 cidr,decimal1 decimal,inet1 inet,macaddr1 macaddr,money1 money,serial1 serial,text1 text,time2 time without time zone,
time3 time with time zone,time4 timestamp without time zone,time5 timestamp with time zone) distributed randomly;

drop table mdt_all_types;


--
--th CO

CREATE TABLE mdt_busted_co ( a int,col001 char DEFAULT 'z',col002 numeric,col003 boolean DEFAULT false,col004 bit(3) DEFAULT
'111',col005 text DEFAULT 'pookie',col006 integer[] DEFAULT '{5, 4, 3, 2, 1}', col007 character varying(512) DEFAULT 'Now is the time',
col008 character varying DEFAULT 'Now is the time', col009 character varying(512)[], col010 numeric(8),col011 int,col012 double precision,
col013 bigint, col014 char(8), col015 bytea,col016 timestamp with time zone,col017 interval, col018 cidr, col019 inet, col020 macaddr,
col022 money, col024 timetz,col025 circle, col026 box, col027 name,col028 path, col029 int2, col031 bit varying(256),col032 date,
col034 lseg,col035 point,col036 polygon,col037 real,col039 time, col040 timestamp )with (orientation='column',appendonly=true);

drop table mdt_busted_co;


--
--Table Creation using Create Table As (CTAS) with both the new tables columns being explicitly or implicitly created


    CREATE TABLE mdt_test_table(
    text_col text,
    bigint_col bigint,
    char_vary_col character varying(30),
    numeric_col numeric,
    int_col int4,
    float_col float4,
    int_array_col int[],
    before_rename_col int4,
    change_datatype_col numeric,
    a_ts_without timestamp without time zone,
    b_ts_with timestamp with time zone,
    date_column date,
    col_set_default numeric)DISTRIBUTED RANDOMLY;

CREATE TABLE mdt_ctas_table1 AS SELECT * FROM mdt_test_table;
CREATE TABLE mdt_ctas_table2 AS SELECT text_col,bigint_col,char_vary_col,numeric_col FROM mdt_test_table;

drop table mdt_test_table;
drop table mdt_ctas_table1;
drop table mdt_ctas_table2;



--
--local table

    CREATE LOCAL TEMP TABLE mdt_table_local (
    text_col text,
    bigint_col bigint,
    char_vary_col character varying(30),
    numeric_col numeric
    ) DISTRIBUTED RANDOMLY;

--like parent_table

    CREATE TABLE mdt_table_like_parent (
    like mdt_table_local
    ) DISTRIBUTED RANDOMLY;

--like parent_table

    CREATE TABLE mdt_table_like_parent1 (
    like mdt_table_local INCLUDING DEFAULTS
    ) DISTRIBUTED RANDOMLY;

--like parent_table

    CREATE TABLE mdt_table_like_parent2 (
    like mdt_table_local INCLUDING CONSTRAINTS
    ) DISTRIBUTED RANDOMLY;

--like parent_table

    CREATE TABLE mdt_table_like_parent3 (
    like mdt_table_local EXCLUDING DEFAULTS
    ) DISTRIBUTED RANDOMLY;

--like parent_table

    CREATE TABLE mdt_table_like_parent4 (
    like mdt_table_local EXCLUDING CONSTRAINTS
    ) DISTRIBUTED RANDOMLY;


drop table mdt_table_like_parent;
drop table mdt_table_like_parent1;
drop table mdt_table_like_parent2;
drop table mdt_table_like_parent3;
drop table mdt_table_like_parent4;
drop table mdt_table_local;



--
--Table constraint

    CREATE TABLE mdt_table_constraint  (
    did integer,
    name varchar(40)
    CONSTRAINT con1 CHECK (did > 99 AND name <> '')
    )DISTRIBUTED RANDOMLY;

--Tables with Default and column constraint

    CREATE TABLE mdt_table_with_default_constraint (
    col_with_default_numeric numeric DEFAULT 0,
    col_with_default_text character varying(30) DEFAULT 'test1',
    col_with_constraint numeric UNIQUE
    ) DISTRIBUTED BY (col_with_constraint);

--Table with column constraint

    CREATE TABLE mdt_table_with_default_constraint1 (
    col_with_default_numeric numeric PRIMARY KEY,
    col_with_default_text character varying(30) DEFAULT 'test1',
    col_with_constraint numeric
    ) DISTRIBUTED BY (col_with_default_numeric);

drop table mdt_table_constraint ;
drop table mdt_table_with_default_constraint;
drop table mdt_table_with_default_constraint1;


--
--Tables with distributed randomly and distributed columns

    CREATE TABLE mdt_table_distributed_by (
    col_with_default numeric DEFAULT 0,
    col_with_default_drop_default character varying(30) DEFAULT 'test1',
    col_with_constraint numeric UNIQUE
    ) DISTRIBUTED BY (col_with_constraint);

    CREATE TABLE mdt_table_distributed_randomly (
    text_col text,
    bigint_col bigint,
    char_vary_col character varying(30),
    numeric_col numeric
    ) DISTRIBUTED RANDOMLY;

drop table mdt_table_distributed_by;
drop table mdt_table_distributed_randomly;



--
--Table with Oids

    CREATE TABLE mdt_table_with_oid (
    text_col text,
    bigint_col bigint,
    char_vary_col character varying(30),
    numeric_col numeric
    ) WITH OIDS DISTRIBUTED RANDOMLY;

drop table mdt_table_with_oid;



--
create user mdt_user1 with superuser;
create group mdt_group1 with superuser;

CREATE TABLE mdt_test_tbl_seq ( col1 int, col2 int) DISTRIBUTED RANDOMLY;
INSERT INTO mdt_test_tbl_seq values (generate_series(1,100),generate_series(1,100));
CREATE SEQUENCE mdt_db_seq9 START 101 OWNED BY mdt_test_tbl_seq.col1;
grant usage on sequence mdt_db_seq9 to mdt_user1 with grant option;

CREATE SEQUENCE  mdt_db_seq10 START WITH 101;

CREATE SEQUENCE  mdt_db_seq11 START WITH 101;
grant all on sequence mdt_db_seq11 to public;

CREATE SEQUENCE  mdt_db_seq12 START WITH 101;
grant all privileges on sequence mdt_db_seq12 to public;


drop table mdt_test_tbl_seq;
drop sequence  mdt_db_seq10 ;
drop sequence  mdt_db_seq11 ;
drop sequence  mdt_db_seq12 ;
drop user mdt_user1;
drop group mdt_group1;

create user mdt_user1 with superuser;
create group mdt_group1 with superuser;

CREATE TABLE mdt_test_tbl_seq ( col1 int, col2 int) DISTRIBUTED RANDOMLY;
INSERT INTO mdt_test_tbl_seq values (generate_series(1,100),generate_series(1,100));
CREATE SEQUENCE mdt_db_seq9 START 101 OWNED BY mdt_test_tbl_seq.col1;
grant usage on sequence mdt_db_seq9 to mdt_user1 with grant option;

CREATE SEQUENCE  mdt_db_seq10 START WITH 101;

CREATE SEQUENCE  mdt_db_seq11 START WITH 101;
grant all on sequence mdt_db_seq11 to public;

CREATE SEQUENCE  mdt_db_seq12 START WITH 101;
grant all privileges on sequence mdt_db_seq12 to public;


drop table mdt_test_tbl_seq;
drop sequence  mdt_db_seq10 ;
drop sequence  mdt_db_seq11 ;
drop sequence  mdt_db_seq12 ;
drop user mdt_user1;
drop group mdt_group1;



--
create user mdt_user1 with superuser;
create group mdt_group1 with superuser;

create table mdt_test_table1 ( a int, b text) distributed by (a);

create table mdt_test_table2 ( a int, b text) distributed by (a);
grant delete, references, trigger on  mdt_test_table2 to group mdt_group1 with grant option;

create table mdt_test_table3 ( a int, b text) distributed by (a);
grant all on mdt_test_table3 to mdt_user1 with grant option;

create table mdt_test_table4 ( a int, b text) distributed by (a);
grant all privileges on mdt_test_table4 to mdt_user1 with grant option;

create table mdt_test_table5 ( a int, b text) distributed by (a);
grant all privileges on mdt_test_table5 to group mdt_group1 with grant option;

create table mdt_test_table6 ( a int, b text) distributed by (a);
create table mdt_test_table7 ( a int, b text) distributed by (a);
grant all on mdt_test_table6,mdt_test_table7 to public ;


drop table mdt_test_table1;
drop table mdt_test_table2;
drop table mdt_test_table3;
drop table mdt_test_table4;
drop table mdt_test_table5;
drop table mdt_test_table6;
drop table mdt_test_table7;
drop user mdt_user1;
drop group mdt_group1;



--
create user mdt_user1 with superuser;
create group mdt_group1 with superuser;

create database mdt_db_1;
grant create on database mdt_db_1 to mdt_user1 with grant option;
revoke grant option for create on database mdt_db_1 from mdt_user1 cascade;

create database mdt_db_2;
grant connect on database mdt_db_2 to group mdt_group1 with grant option;
revoke connect on database mdt_db_2 from group mdt_group1 restrict;

create database mdt_db_3;
grant all privileges on database mdt_db_3 to public;
revoke all privileges on database mdt_db_3 from public;

create database mdt_db_4;
grant all on database mdt_db_4 to public;
revoke all on database mdt_db_4 from public;

create database mdt_db_5;
grant TEMPORARY on database mdt_db_5 to mdt_user1 with grant option;
revoke TEMPORARY on database mdt_db_5 from mdt_user1;

create database mdt_db_6;
grant TEMP on database mdt_db_6 to group mdt_group1 with grant option;
revoke TEMP on database mdt_db_6 from group mdt_group1;

drop database mdt_db_1;
drop database mdt_db_2;
drop database mdt_db_3;
drop database mdt_db_4;
drop database mdt_db_5;
drop database mdt_db_6;
drop user mdt_user1;
drop group mdt_group1;


--
create user mdt_user1 with superuser;
create group mdt_group1 with superuser;


          CREATE OR REPLACE FUNCTION mdt_insert_mdt_price_change1() RETURNS trigger AS '
          DECLARE
          changed boolean;
          BEGIN
          IF tg_op = ''DELETE'' THEN
          INSERT INTO mdt_price_change(apn, effective, price)
          VALUES (old.barcode, CURRENT_TIMESTAMP, NULL);
          RETURN old;
          END IF;
          IF tg_op = ''INSERT'' THEN
          changed := TRUE;
          ELSE
          changed := new.price IS NULL != old.price IS NULL OR new.price != old.price;
          END IF;
          IF changed THEN
          INSERT INTO mdt_price_change(apn, effective, price)
          VALUES (new.barcode, CURRENT_TIMESTAMP, new.price);
          END IF;
          RETURN new;
          END
          ' LANGUAGE plpgsql MODIFIES SQL DATA;

grant execute on function mdt_insert_mdt_price_change1() to mdt_user1 with grant option;
revoke grant option for execute on function mdt_insert_mdt_price_change1() from mdt_user1 cascade;


          CREATE OR REPLACE FUNCTION mdt_insert_mdt_price_change2() RETURNS trigger AS '
          DECLARE
          changed boolean;
          BEGIN
          IF tg_op = ''DELETE'' THEN
          INSERT INTO mdt_price_change(apn, effective, price)
          VALUES (old.barcode, CURRENT_TIMESTAMP, NULL);
          RETURN old;
          END IF;
          IF tg_op = ''INSERT'' THEN
          changed := TRUE;
          ELSE
          changed := new.price IS NULL != old.price IS NULL OR new.price != old.price;
          END IF;
          IF changed THEN
          INSERT INTO mdt_price_change(apn, effective, price)
          VALUES (new.barcode, CURRENT_TIMESTAMP, new.price);
          END IF;
          RETURN new;
          END
          ' LANGUAGE plpgsql MODIFIES SQL DATA;

grant execute on function mdt_insert_mdt_price_change2() to group mdt_group1 with grant option;
revoke execute on function mdt_insert_mdt_price_change2() from group mdt_group1 restrict;


          CREATE OR REPLACE FUNCTION mdt_insert_mdt_price_change3() RETURNS trigger AS '
          DECLARE
          changed boolean;
          BEGIN
          IF tg_op = ''DELETE'' THEN
          INSERT INTO mdt_price_change(apn, effective, price)
          VALUES (old.barcode, CURRENT_TIMESTAMP, NULL);
          RETURN old;
          END IF;
          IF tg_op = ''INSERT'' THEN
          changed := TRUE;
          ELSE
          changed := new.price IS NULL != old.price IS NULL OR new.price != old.price;
          END IF;
          IF changed THEN
          INSERT INTO mdt_price_change(apn, effective, price)
          VALUES (new.barcode, CURRENT_TIMESTAMP, new.price);
          END IF;
          RETURN new;
          END
          ' LANGUAGE plpgsql MODIFIES SQL DATA;

grant all on function mdt_insert_mdt_price_change3() to public;
revoke all on function mdt_insert_mdt_price_change3() from public;

          CREATE OR REPLACE FUNCTION mdt_insert_mdt_price_change4() RETURNS trigger AS '
          DECLARE
          changed boolean;
          BEGIN
          IF tg_op = ''DELETE'' THEN
          INSERT INTO mdt_price_change(apn, effective, price)
          VALUES (old.barcode, CURRENT_TIMESTAMP, NULL);
          RETURN old;
          END IF;
          IF tg_op = ''INSERT'' THEN
          changed := TRUE;
          ELSE
          changed := new.price IS NULL != old.price IS NULL OR new.price != old.price;
          END IF;
          IF changed THEN
          INSERT INTO mdt_price_change(apn, effective, price)
          VALUES (new.barcode, CURRENT_TIMESTAMP, new.price);
          END IF;
          RETURN new;
          END
          ' LANGUAGE plpgsql MODIFIES SQL DATA;

grant all privileges on function mdt_insert_mdt_price_change4() to public;
revoke all privileges on function mdt_insert_mdt_price_change4() from public;

drop function mdt_insert_mdt_price_change1();
drop function mdt_insert_mdt_price_change2();
drop function mdt_insert_mdt_price_change3();
drop function mdt_insert_mdt_price_change4();
drop user mdt_user1;
drop group mdt_group1;



--
create user mdt_user1 with superuser;
create group mdt_group1 with superuser;

grant usage on language plpgsql to group mdt_group1 with grant option;
REVOKE  usage on language plpgsql from group mdt_group1 cascade;

grant all privileges on language sql to public;
revoke all privileges on language sql from public restrict; 

drop user mdt_user1;
drop group mdt_group1;


--
create user mdt_user1 with superuser;
create group mdt_group1 with superuser;

create schema mdt_schema1;
grant create on  schema mdt_schema1 to mdt_user1 with grant option;
revoke grant option for create on schema mdt_schema1 from mdt_user1 cascade;

create schema mdt_schema2;
grant usage on schema mdt_schema2 to group mdt_group1 with grant option;
revoke usage on schema mdt_schema2 from group mdt_group1 restrict;

create schema mdt_schema3;
grant all privileges on schema mdt_schema3 to public;
revoke all privileges on schema mdt_schema3 from public;

create schema mdt_schema4;
grant all on schema mdt_schema4 to public;
revoke all on schema mdt_schema4 from public;

drop schema mdt_schema1;
drop schema mdt_schema2;
drop schema mdt_schema3;
drop schema mdt_schema4;
drop user mdt_user1;
drop group mdt_group1;


--
create user mdt_user1 with superuser;
create group mdt_group1 with superuser;

CREATE TABLE mdt_test_tbl_seq ( col1 int, col2 int) DISTRIBUTED RANDOMLY;
INSERT INTO mdt_test_tbl_seq values (generate_series(1,100),generate_series(1,100));
CREATE SEQUENCE mdt_db_seq9 START 101 OWNED BY mdt_test_tbl_seq.col1;
grant usage on sequence mdt_db_seq9 to mdt_user1 with grant option;
revoke grant option for usage on sequence mdt_db_seq9 from mdt_user1 cascade;

CREATE SEQUENCE  mdt_db_seq10 START WITH 101;

CREATE SEQUENCE  mdt_db_seq11 START WITH 101;
grant all on sequence mdt_db_seq11 to public;
revoke all on sequence mdt_db_seq11 from public;

CREATE SEQUENCE  mdt_db_seq12 START WITH 101;
grant all privileges on sequence mdt_db_seq12 to public;
revoke all privileges on sequence mdt_db_seq12 from public;

drop table mdt_test_tbl_seq;
drop sequence  mdt_db_seq10 ;
drop sequence  mdt_db_seq11 ;
drop sequence  mdt_db_seq12 ;
drop user mdt_user1;
drop group mdt_group1;


--
create user mdt_user1 with superuser;
create group mdt_group1 with superuser;

create table mdt_test_table1 ( a int, b text) distributed by (a);

create table mdt_test_table2 ( a int, b text) distributed by (a);
grant delete, references, trigger on  mdt_test_table2 to group mdt_group1 with grant option;
revoke delete, references, trigger on  mdt_test_table2 from group mdt_group1 restrict;

create table mdt_test_table3 ( a int, b text) distributed by (a);
grant all on mdt_test_table3 to mdt_user1 with grant option;
revoke all on mdt_test_table3 from mdt_user1;

create table mdt_test_table4 ( a int, b text) distributed by (a);
grant all privileges on mdt_test_table4 to mdt_user1 with grant option;
revoke all privileges on mdt_test_table4 from mdt_user1;

create table mdt_test_table5 ( a int, b text) distributed by (a);
grant all privileges on mdt_test_table5 to group mdt_group1 with grant option;
revoke all privileges on mdt_test_table5 from group mdt_group1;

create table mdt_test_table6 ( a int, b text) distributed by (a);
create table mdt_test_table7 ( a int, b text) distributed by (a);
grant all on mdt_test_table6,mdt_test_table7 to public ;
revoke all on mdt_test_table6,mdt_test_table7 from public ;

drop table mdt_test_table1;
drop table mdt_test_table2;
drop table mdt_test_table3;
drop table mdt_test_table4;
drop table mdt_test_table5;
drop table mdt_test_table6;
drop table mdt_test_table7;
drop user mdt_user1;
drop group mdt_group1;



--
CREATE TABLE mdt_all_types ( a int,col001 char DEFAULT 'z',col002 numeric,col003 boolean DEFAULT false,col004 bit(3) DEFAULT '111',col005 text DEFAULT 'pookie', col006 integer[] DEFAULT '{5, 4, 3, 2, 1}', col007 character varying(512) DEFAULT 'Now is the time', col008 character varying DEFAULT 'Now is the time', col009 character varying(512)[], col010 numeric(8),col011 int,col012 double precision, col013 bigint, col014 char(8), col015 bytea,col016 timestamp with time zone,col017 interval, col018 cidr, col019 inet, col020 macaddr,col021 serial, col022 money, col023 bigserial, col024 timetz,col025 circle, col026 box, col027 name,col028 path, col029 int2, col031 bit varying(256),col032 date, col034 lseg,col035 point,col036 polygon,col037 real,col039 time, col040 timestamp )with (appendonly=true);

INSERT INTO mdt_all_types VALUES (1,'a',11,true,'111', '1_one', '{1,2,3,4,5}', 'Hello .. how are you 1', 'Hello .. how are you 1',    '{one,two,three,four,five}',  12345678, 1, 111.1111,  11,  '1_one_11',   'd',  '2001-12-13 01:51:15+1359',  '11',   '0.0.0.0', '0.0.0.0', 'AA:AA:AA:AA:AA:AA', 1,  '34.23',   1,'00:00:00+1359',  '((2,2),1)',   '((1,2),(2,1))',   'hello', '((1,2),(2,1))', 11,   '010101',   '2001-12-13', '((1,1),(2,2))', '(1,1)', '((1,2),(2,3),(3,4),(4,3),(3,2),(2,1))',    111111, '23:00:00',   '2001-12-13 01:51:15');

INSERT INTO mdt_all_types VALUES (    2,   'b',   22,  false, '001',   '2_one',  '{6,7,8,9,10}',  'Hey.. whtz up 2', 'Hey .. whtz up 2',    '{one,two,three,four,five}', 76767669, 1, 222.2222, 11,   '2_two_22',   'c',   '2002-12-13 01:51:15+1359',   '22',    '0.0.0.0',  '0.0.0.0',  'BB:BB:BB:BB:BB:BB',2, '200.00',   2, '00:00:05', '((3,3),2)',  '((3,2),(2,3))',   'hello',  '((3,2),(2,3))', 22,    '01010100101',  '2002-12-13', '((2,2),(3,3))', '(2,2)', '((1,2),(2,3),(3,4),(4,3),(3,2),(2,1))', 11111,  '22:00:00', '2002-12-13 01:51:15');

VACUUM ANALYSE  mdt_all_types ;

create schema myschema;
create schema myschema_new;
create schema myschema_diff;

CREATE TABLE myschema.mdt_supplier_hybrid_part(
                S_SUPPKEY INTEGER,
                S_NAME CHAR(25),
                S_ADDRESS VARCHAR(40),
                S_NATIONKEY INTEGER,
                S_PHONE CHAR(15),
                S_ACCTBAL decimal,
                S_COMMENT VARCHAR(101)
                )
partition by range (s_suppkey)
subpartition by list (s_nationkey) subpartition template (
        values('22','21','17'),
        values('6','11','1','7','16','2') WITH (orientation='column', appendonly=true),
        values('18','20') WITH (checksum=false, appendonly=true,blocksize=1171456, compresslevel=3),
        values('9','23','13') WITH (checksum=true,appendonly=true,blocksize=1335296,compresslevel=7),
        values('0','3','12','15','14','8','4','24','19','10','5')
)
(
partition p1 start('1') end('10001') every(10000)

);

Vacuum analyse myschema.mdt_supplier_hybrid_part;
ALTER TABLE myschema.mdt_supplier_hybrid_part SET SCHEMA myschema_new;
Vacuum myschema_new.mdt_supplier_hybrid_part;
ALTER TABLE myschema.mdt_supplier_hybrid_part_1_prt_p1 SET SCHEMA myschema_new;
ALTER TABLE myschema.mdt_supplier_hybrid_part_1_prt_p1_2_prt_1 SET SCHEMA myschema_diff;
Vacuum analyse myschema_diff.mdt_supplier_hybrid_part_1_prt_p1_2_prt_1;


drop table myschema_new.mdt_supplier_hybrid_part;
drop schema myschema;
drop schema myschema_new;
drop schema myschema_diff;
drop table mdt_all_types;


--
CREATE TABLE mdt_all_types ( a int,col001 char DEFAULT 'z',col002 numeric,col003 boolean DEFAULT false,col004 bit(3) DEFAULT '111',col005 text DEFAULT 'pookie', col006 integer[] DEFAULT '{5, 4, 3, 2, 1}', col007 character varying(512) DEFAULT 'Now is the time', col008 character varying DEFAULT 'Now is the time', col009 character varying(512)[], col010 numeric(8),col011 int,col012 double precision, col013 bigint, col014 char(8), col015 bytea,col016 timestamp with time zone,col017 interval, col018 cidr, col019 inet, col020 macaddr,col021 serial, col022 money, col023 bigserial, col024 timetz,col025 circle, col026 box, col027 name,col028 path, col029 int2, col031 bit varying(256),col032 date, col034 lseg,col035 point,col036 polygon,col037 real,col039 time, col040 timestamp )with (appendonly=true);

INSERT INTO mdt_all_types VALUES (1,'a',11,true,'111', '1_one', '{1,2,3,4,5}', 'Hello .. how are you 1', 'Hello .. how are you 1',    '{one,two,three,four,five}',  12345678, 1, 111.1111,  11,  '1_one_11',   'd',  '2001-12-13 01:51:15+1359',  '11',   '0.0.0.0', '0.0.0.0', 'AA:AA:AA:AA:AA:AA', 1,  '34.23',   1,'00:00:00+1359',  '((2,2),1)',   '((1,2),(2,1))',   'hello', '((1,2),(2,1))', 11,   '010101',   '2001-12-13', '((1,1),(2,2))', '(1,1)', '((1,2),(2,3),(3,4),(4,3),(3,2),(2,1))',    111111, '23:00:00',   '2001-12-13 01:51:15');

INSERT INTO mdt_all_types VALUES (    2,   'b',   22,  false, '001',   '2_one',  '{6,7,8,9,10}',  'Hey.. whtz up 2', 'Hey .. whtz up 2',    '{one,two,three,four,five}', 76767669, 1, 222.2222, 11,   '2_two_22',   'c',   '2002-12-13 01:51:15+1359',   '22',    '0.0.0.0',  '0.0.0.0',  'BB:BB:BB:BB:BB:BB',2, '200.00',   2, '00:00:05', '((3,3),2)',  '((3,2),(2,3))',   'hello',  '((3,2),(2,3))', 22,    '01010100101',  '2002-12-13', '((2,2),(3,3))', '(2,2)', '((1,2),(2,3),(3,4),(4,3),(3,2),(2,1))', 11111,  '22:00:00', '2002-12-13 01:51:15');

vacuum full analyse mdt_all_types(col002);

drop table mdt_all_types;


--
CREATE TABLE mdt_all_types ( a int,col001 char DEFAULT 'z',col002 numeric,col003 boolean DEFAULT false,col004 bit(3) DEFAULT '111',col005 text DEFAULT 'pookie', col006 integer[] DEFAULT '{5, 4, 3, 2, 1}', col007 character varying(512) DEFAULT 'Now is the time', col008 character varying DEFAULT 'Now is the time', col009 character varying(512)[], col010 numeric(8),col011 int,col012 double precision, col013 bigint, col014 char(8), col015 bytea,col016 timestamp with time zone,col017 interval, col018 cidr, col019 inet, col020 macaddr,col021 serial, col022 money, col023 bigserial, col024 timetz,col025 circle, col026 box, col027 name,col028 path, col029 int2, col031 bit varying(256),col032 date, col034 lseg,col035 point,col036 polygon,col037 real,col039 time, col040 timestamp )with (appendonly=true);

INSERT INTO mdt_all_types VALUES (1,'a',11,true,'111', '1_one', '{1,2,3,4,5}', 'Hello .. how are you 1', 'Hello .. how are you 1',    '{one,two,three,four,five}',  12345678, 1, 111.1111,  11,  '1_one_11',   'd',  '2001-12-13 01:51:15+1359',  '11',   '0.0.0.0', '0.0.0.0', 'AA:AA:AA:AA:AA:AA', 1,  '34.23',   1,'00:00:00+1359',  '((2,2),1)',   '((1,2),(2,1))',   'hello', '((1,2),(2,1))', 11,   '010101',   '2001-12-13', '((1,1),(2,2))', '(1,1)', '((1,2),(2,3),(3,4),(4,3),(3,2),(2,1))',    111111, '23:00:00',   '2001-12-13 01:51:15');

INSERT INTO mdt_all_types VALUES (    2,   'b',   22,  false, '001',   '2_one',  '{6,7,8,9,10}',  'Hey.. whtz up 2', 'Hey .. whtz up 2',    '{one,two,three,four,five}', 76767669, 1, 222.2222, 11,   '2_two_22',   'c',   '2002-12-13 01:51:15+1359',   '22',    '0.0.0.0',  '0.0.0.0',  'BB:BB:BB:BB:BB:BB',2, '200.00',   2, '00:00:05', '((3,3),2)',  '((3,2),(2,3))',   'hello',  '((3,2),(2,3))', 22,    '01010100101',  '2002-12-13', '((2,2),(3,3))', '(2,2)', '((1,2),(2,3),(3,4),(4,3),(3,2),(2,1))', 11111,  '22:00:00', '2002-12-13 01:51:15');

VACUUM FULL mdt_all_types ;

drop table mdt_all_types;

