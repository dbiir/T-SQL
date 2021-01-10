#-------------------------------------------------------------------------
#
# Makefile for paxos_storage
#
# Copyright (c) 1994, Regents of the University of California
#
# src/backend/paxos_storage
#
#-------------------------------------------------------------------------

subdir = src/backend/paxos_storage
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
# override CPPFLAGS := -I$(libpq_srcdir) -I. -I$(srcdir) $(CPPFLAGS)

OBJS = acceptor.o committer.o echo_server.o learner.o message_event.o paxos_msg.pb.o sm_base.o udp.o \
base.o communicate.o echo_sm.o learner_sender.o msg_counter.o paxos_server_manager.o socket.o util.o \
breakpoint.o concurrent.o event_base.o log_store.o network.o pnode.o system_v_sm.o wait_lock.o \
bytes_buffer.o config.o event_loop.o logger.o node.o propose_batch.o system_variables_store.o \
checkpoint_receiver.o cp_mgr.o group.o notifier_pool.o proposer.o tcp.o \
checkpoint_sender.o crc32.o inside_options.o master_mgr.o notify.o replayer.o tcp_acceptor.o \
cleaner.o db.o instance.o master_sm.o options.o serial_lock.o tcp_client.o \
commitctx.o dfnetwork.o ioloop.o master_variables_store.o paxos_log.o sm.o timer.o

include $(top_srcdir)/src/backend/common.mk