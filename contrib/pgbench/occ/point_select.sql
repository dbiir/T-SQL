\setrandom id1 1 10000000
SET transam_mode=occ;
SELECT pad FROM sbtest WHERE id = :id1;
