set log_min_messages=debug5;
set log_min_error_statement=debug5;
SET transam_mode=occ;
INSERT INTO sbtest(k, c, pad) values((random() * 10000000)::int + 1,sb_rand_str('###########-###########-###########-###########-###########-###########-###########-###########-###########-###########'),sb_rand_str('###########-###########-###########-###########-###########'));
SET transam_mode=default;
