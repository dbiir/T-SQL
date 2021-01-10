\setrandom id1 1 10000000
SET transam_mode=occ;
UPDATE sbtest SET k=k+1 WHERE id = :id1;
