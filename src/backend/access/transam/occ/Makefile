#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/transam
#
# IDENTIFICATION
#    src/backend/access/transam/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/transam/occ
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

OBJS = occ_comm.o occ_validation.o rollback.o

include $(top_srcdir)/src/backend/common.mk

# ensure that version checks in xlog.c get recompiled when catversion.h changes
xlog.o: xlog.c $(top_srcdir)/src/include/catalog/catversion.h
