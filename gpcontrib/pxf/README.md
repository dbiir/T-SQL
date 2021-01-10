# The PXF extension for GPDB

PXF is an extension framework that allows GPDB or any other database to query external distributed datasets. The framework is built in Java and provides built-in connectors for accessing data of various formats(text,sequence files, avro, orc,etc) that may exist inside HDFS files, Hive tables, HBase tables and many more stores.
PXF consists of a server side JVM based component and a C client component which serves as the means for GPDB to interact with the PXF service.
This module only includes the PXF C client and the build instructions only builds the client.
Using the 'pxf' protocol with external table, GPDB can query external datasets via PXF service that runs alongside GPDB segments.

## Usage

### Enable PXF extension in GPDB build process.

Configure GPDB to build the pxf extension by adding the `--with-pxf`
configure option. This is required to setup the PXF build environment.

### Build the PXF extension

```
make
```

The build will produce the pxf client shared library named `pxf.so`.

### Run unit tests

This will run the unit tests located in the `test` directory

```
make unittest-check
```
 
### Install the PXF extension
```
make install
```
 
This will copy the `pxf.so` shared library into `$GPHOME/lib/postgresql.`


To create the PXF extension in the database, connect to the database and run as a GPDB superuser in psql:
```
# CREATE EXTENSION pxf;
```

Additional instructions on building and starting a GPDB cluster can be
found in the top-level [README.md](../../../README.md) ("_Build the
database_" section).


### Install PXF Server
Please refer to [PXF Development](https://github.com/greenplum-db/pxf/blob/master/README.md) for instructions to setup PXF.
You will need one PXF server agent per Segment host.

### Create and use PXF external table
If you wish to simply test drive PXF extension without hitting any external data source, you can avoid starting any of the hadoop components (while installing the PXF Server) and simply use the Demo Profile.

The Demo profile demonstrates how GPDB using its segments can access static data served by the PXF service(s) in parallel.
```
# CREATE EXTERNAL TABLE pxf_read_test (a TEXT, b TEXT, c TEXT) \
LOCATION ('pxf://localhost:5888/tmp/dummy1' \
'?FRAGMENTER=org.greenplum.pxf.api.examples.DemoFragmenter' \
'&ACCESSOR=org.greenplum.pxf.api.examples.DemoAccessor' \
'&RESOLVER=org.greenplum.pxf.api.examples.DemoTextResolver') \
FORMAT 'TEXT' (DELIMITER ',');
```


```
# SELECT * from pxf_read_test order by a;

       a        |   b    |   c
----------------+--------+--------
 fragment1 row1 | value1 | value2
 fragment1 row2 | value1 | value2
 fragment2 row1 | value1 | value2
 fragment2 row2 | value1 | value2
 fragment3 row1 | value1 | value2
 fragment3 row2 | value1 | value2
(6 rows)
```


If you wish to use PXF with Hadoop, you will need to integrate with Hdfs or Hive, you can refer to the above doc on steps to install them.



### Run regression tests

```
make installcheck
```

This will connect to the running database, and run the regression
tests located in the `regress` directory.