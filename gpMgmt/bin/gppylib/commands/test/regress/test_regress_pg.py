#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved. 
#
# Unit Testing of pg commands
#

import os
import unittest
import tempfile

from gppylib.db import dbconn
from gppylib.db.test import skipIfDatabaseDown
from gppylib import gplog
from gppylib.commands import pg
from gppylib.gparray import GpArray

logger = gplog.get_default_logger()
gplog.enable_verbose_logging()

@skipIfDatabaseDown()
class PgCommandsTestCase(unittest.TestCase):
    def setUp(self):
         pass
     
    def tearDown(self):
        pass
    
    def testReadPostmasterTempFile(self):
        logger.info("testReadPostmasterTempFile")
        url = dbconn.DbURL()
        gpdb = GpArray.initFromCatalog(url)
        
        logger.info("Search for valid master port: %s" % gpdb.master.port)
        cmd = pg.ReadPostmasterTempFile.local('test pg tempfile read',gpdb.master.port)        
        (exists,PID,datadir)=cmd.getResults()
        logger.info("exists:=%s PID=%d datadir='%s'" % (exists,PID,datadir))                
        self.assertTrue(exists)
        self.assertTrue(PID > 0)
        self.assertEquals(datadir,gpdb.master.datadir)
        
        
        gpdb.master.port=4000
        logger.info("Search for bogus master port: %s" % gpdb.master.port)        
        cmd = pg.ReadPostmasterTempFile.local('test pg tempfile read',gpdb.master.port)        
        (exists,PID,datadir)=cmd.getResults()
        logger.info("exists:=%s PID=%d datadir='%s'" % (exists,PID,datadir))        
        self.assertFalse(exists)
        
        
    
            



#------------------------------- Mainline --------------------------------
if __name__ == '__main__': 
    unittest.main()    
