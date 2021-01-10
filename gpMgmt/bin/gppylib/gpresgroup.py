#!/usr/bin/env python
#
# Copyright (c) 2017, Pivotal Software Inc.
#

from gppylib.commands import base
from gppylib.commands.unix import *
from gppylib.commands.gp import *
from gppylib.gparray import GpArray
from gppylib.gplog import get_default_logger
from gppylib.gphostcache import *

class GpResGroup(object):

    def __init__(self):
        self.logger = get_default_logger()

    def validate(self):
        pool = base.WorkerPool()
        gp_array = GpArray.initFromCatalog(dbconn.DbURL(), utility=True)
        host_list = list(set(gp_array.get_hostlist(True)))
        msg = None

        for h in host_list:
            cmd = Command(h, "gpcheckresgroupimpl", REMOTE, h)
            pool.addCommand(cmd)
        pool.join()

        items = pool.getCompletedItems()
        failed = []
        for i in items:
            if not i.was_successful():
                failed.append("[%s:%s]"%(i.remoteHost, i.get_stderr().rstrip()))
        pool.haltWork()
        pool.joinWorkers()
        if failed:
            msg = ",".join(failed)
        return msg

