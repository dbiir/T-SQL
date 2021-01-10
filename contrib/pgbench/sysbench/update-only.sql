\set scale 10
\set naccounts 100000 * :scale
\setrandom aid 1 :naccounts
\setrandom delta -5000 5000
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid returning aid;
