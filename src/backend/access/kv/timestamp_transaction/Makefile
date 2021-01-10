
subdir = src/backend/access/kv/timestamp_transaction
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

OBJS = storage_rocksdb.o transaction_engine.o timestamp_generate_key.o http.o lts_generate_key.o \
		storage_lts.o hlc.o

include $(top_srcdir)/src/backend/common.mk
