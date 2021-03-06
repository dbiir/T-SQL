CREATE TABLE hobby (
	name		text primary key,
	person 		text
) with(storage_engine=rocksdb);

CREATE TABLE equipment (
	name 		text primary key,
	hobby		text
) with(storage_engine=rocksdb);

CREATE TABLE onek (
	unique1		int4 primary key,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) with(storage_engine=rocksdb);

CREATE TABLE tenk1 (
	unique1		int4 primary key,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) with(storage_engine=rocksdb);

CREATE TABLE tenk2 (
	unique1 	int4 primary key,
	unique2 	int4,
	two 	 	int4,
	four 		int4,
	ten			int4,
	twenty 		int4,
	hundred 	int4,
	thousand 	int4,
	twothousand int4,
	fivethous 	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) with(storage_engine=rocksdb);


CREATE TABLE person (
	name 		text primary key,
	age			int4,
	location 	point
) with(storage_engine=rocksdb);

CREATE TABLE aggtest (
	a 			int2 primary key,
	b			float4
) with(storage_engine=rocksdb);

CREATE TABLE hash_i4_heap (
	seqno 		int4 primary key,
	random 		int4
) with(storage_engine=rocksdb) distributed by (seqno);

CREATE TABLE hash_name_heap (
	seqno 		int4 primary key,
	random 		name
) with(storage_engine=rocksdb) distributed by (seqno);

CREATE TABLE hash_txt_heap (
	seqno 		int4 primary key,
	random 		text
) with(storage_engine=rocksdb) distributed by (seqno);

CREATE TABLE hash_f8_heap (
	seqno		int4 primary key,
	random 		float8
) with(storage_engine=rocksdb) distributed by (seqno);

CREATE TABLE bt_i4_heap (
	seqno 		int4 primary key,
	random 		int4
) with(storage_engine=rocksdb);

CREATE TABLE bt_name_heap (
	seqno 		name primary key,
	random 		int4
) with(storage_engine=rocksdb);

CREATE TABLE bt_txt_heap (
	seqno 		text primary key,
	random 		int4
) with(storage_engine=rocksdb);

CREATE TABLE bt_f8_heap (
	seqno 		float8 primary key,
	random 		int4
) with(storage_engine=rocksdb);

CREATE TABLE array_op_test (
	seqno		int4 primary key,
	i			int4[],
	t			text[]
) with(storage_engine=rocksdb);

CREATE TABLE array_index_op_test (
	seqno		int4 primary key,
	i			int4[],
	t			text[]
) with(storage_engine=rocksdb);
