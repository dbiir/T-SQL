# contrib/pg_oltp_bench/Makefile

MODULES = pg_oltp_bench

EXTENSION = pg_oltp_bench
DATA = pg_oltp_bench--1.0.sql
PGFILEDESC = "pg_oltp_bench - supporting function for oltp benchmark"

REGRESS = pg_oltp_bench

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_oltp_bench
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
