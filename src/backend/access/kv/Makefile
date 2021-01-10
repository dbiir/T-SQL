subdir = src/backend/access/kv
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = pg_transaction timestamp_transaction his_transaction

OBJS = bootstraprange.o decode.o encode.o range_struct.o\
	   kv_universal.o kvam.o kvindex.o ms_plan.o paxos_message.o range_allocator_score.o\
	   range_plan.o range_processor.o range_universal.o range.o rangecache.o\
	   rangeid_generator.o rangestatistics.o rocks_engine.o\
	   route.o seg_plan.o session_processor.o storage_param.o storage_processor.o memwatch.o\
	   historical_transfer.o\

include $(top_srcdir)/src/backend/common.mk