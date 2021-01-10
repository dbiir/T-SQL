\set scale 10
\set naccounts 100000 * :scale
\setrandom aid 1 :naccounts
DELETE FROM pgbench_accounts WHERE aid = :aid;
