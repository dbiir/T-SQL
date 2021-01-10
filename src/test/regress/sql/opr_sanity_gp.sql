-- Additional GPDB-specific sanity checks.


-- Test that all GPDB-hashable datatypes, i.e. those that you can use as the
-- distribution key for a table, are also mergejoinable. (We use the same
-- PathKey structure internally that is used to represent sort ordering, to
-- represent the distribution columns.)
--
--- NB: This list of datatypes should match that in IsGreenplumDbHashable
select * from pg_operator where oprname='=' and oprleft = oprright
and not oprcanmerge
and oprleft IN (
  'int2'::regtype,
  'int4'::regtype,
  'int8'::regtype,
  'float4'::regtype,
  'float8'::regtype,
  'numeric'::regtype,
  'char'::regtype,
  'bpchar '::regtype,
  'text'::regtype,
  'pg_node_tree'::regtype,
  'varchar'::regtype,
  'bytea'::regtype,
  'name'::regtype,
  'oid'::regtype,
  'tid'::regtype,
  'regproc'::regtype,
  'regprocedure'::regtype,
  'regoper'::regtype,
  'regoperator'::regtype,
  'regclass'::regtype,
  'regtype'::regtype,
  'timestamp'::regtype,
  'timestamptz'::regtype,
  'date'::regtype,
  'time'::regtype,
  'timetz '::regtype,
  'interval'::regtype,
  'abstime'::regtype,
  'reltime'::regtype,
  'tinterval'::regtype,
  'inet'::regtype,
  'cidr'::regtype,
  'macaddr'::regtype,
  'bit'::regtype,
  'varbit'::regtype,
  'bool'::regtype,
  'anyarray'::regtype,
  'oidvector'::regtype,
  'money'::regtype);
