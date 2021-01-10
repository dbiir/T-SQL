subdir = src/backend/ms
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = msreceiver.o ms.o mssender.o 

include $(top_srcdir)/src/backend/common.mk
