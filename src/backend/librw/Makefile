#-------------------------------------------------------------------------
#
# Makefile for libtcp
#
# Copyright (c) 1994, Regents of the University of California
#
# src/backend/libtcp
#
#-------------------------------------------------------------------------

subdir = src/backend/librw
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
# override CPPFLAGS := -I$(libpq_srcdir) -I. -I$(srcdir) $(CPPFLAGS)

OBJS = hash.o key_range.o key_xid.o rts_cache.o slice.o

include $(top_srcdir)/src/backend/common.mk