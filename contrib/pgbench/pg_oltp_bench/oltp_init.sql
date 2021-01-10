DROP TABLE IF EXISTS sbtest;

CREATE TABLE sbtest(
	id SERIAL PRIMARY KEY,
	k INTEGER DEFAULT '0' NOT NULL,
	c CHAR(120) DEFAULT '' NOT NULL,
	pad CHAR(60) DEFAULT '' NOT NULL);

INSERT INTO sbtest (k, c, pad)
SELECT
	(random() * 10000000)::int + 1 AS k,
	sb_rand_str('###########-###########-###########-###########-###########-###########-###########-###########-###########-###########') AS c,
	sb_rand_str('###########-###########-###########-###########-###########') AS pad
FROM
	generate_series(1, 10000000) j;

CREATE INDEX sbtest_k_idx ON sbtest(k);
