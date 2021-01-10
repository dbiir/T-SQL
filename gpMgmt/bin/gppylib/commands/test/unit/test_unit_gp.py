#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2012. All Rights Reserved. 
#

from gppylib.commands.base import CommandResult
from mock import patch

from gppylib.commands.gp import is_pid_postmaster, get_postmaster_pid_locally
from test.unit.gp_unittest import GpTestCase, run_tests


class GpCommandTestCase(GpTestCase):

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "1234", "", True, False)])
    def test_is_pid_postmaster_pid_exists(self, mock1, mock2):
        pid = 1234
        datadir = '/data/primary/gpseg0'
        self.assertTrue(is_pid_postmaster(datadir, pid))

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False)])
    def test_is_pid_postmaster_pid_doesnt_exists(self, mock1, mock2):
        pid = 1234
        datadir = '/data/primary/gpseg0'
        self.assertFalse(is_pid_postmaster(datadir, pid))

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "1234", "", True, False)])
    def test_is_pid_postmaster_pid_as_string(self, mock1, mock2):
        pid = '1234'
        datadir = '/data/primary/gpseg0'
        self.assertTrue(is_pid_postmaster(datadir, pid))

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False)])
    def test_is_pid_postmaster_no_result(self, mock1, mock2):
        pid = '1234'
        datadir = None
        self.assertFalse(is_pid_postmaster(datadir, pid))

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(127, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False)])
    def test_is_pid_postmaster_no_pgrep(self, mock1, mock2):
        pid = '1234'
        datadir = '/data/primary/gpseg0' 
        self.assertTrue(is_pid_postmaster(datadir, pid))

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(127, "", "", False, False),
                                                                   CommandResult(0, "", "", True, False)])
    def test_is_pid_postmaster_no_pwdx(self, mock1, mock2):
        pid = '1234'
        datadir = '/data/primary/gpseg0' 
        self.assertTrue(is_pid_postmaster(datadir, pid))

    @patch('gppylib.commands.gp.Command.run', side_effect=[None, None, Exception('Error')]) 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(1, "", "", False, False)])
    def test_is_pid_postmaster_pgrep_failed(self, mock1, mock2):
        pid = '1234'
        datadir = '/data/primary/gpseg0' 
        self.assertTrue(is_pid_postmaster(datadir, pid))

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "1234", "", True, False)])
    def test_is_pid_postmaster_pid_exists_remote(self, mock1, mock2):
        pid = 1234
        datadir = '/data/primary/gpseg0'
        remoteHost = 'smdw'
        self.assertTrue(is_pid_postmaster(datadir, pid, remoteHost=remoteHost))

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False)])
    def test_is_pid_postmaster_pid_doesnt_exists_remote(self, mock1, mock2):
        pid = 1234
        datadir = '/data/primary/gpseg0'
        remoteHost = 'smdw'
        self.assertFalse(is_pid_postmaster(datadir, pid, remoteHost=remoteHost))

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "1234", "", True, False)])
    def test_is_pid_postmaster_pid_as_string_remote(self, mock1, mock2):
        pid = '1234'
        datadir = '/data/primary/gpseg0'
        remoteHost = 'smdw'
        self.assertTrue(is_pid_postmaster(datadir, pid, remoteHost=remoteHost))

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False)])
    def test_is_pid_postmaster_no_result_remote(self, mock1, mock2):
        pid = '1234'
        datadir = None
        remoteHost = 'smdw'
        self.assertFalse(is_pid_postmaster(datadir, pid, remoteHost=remoteHost))

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(127, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False)])
    def test_is_pid_postmaster_no_pgrep_remote(self, mock1, mock2):
        pid = '1234'
        datadir = '/data/primary/gpseg0' 
        remoteHost = 'smdw'
        self.assertTrue(is_pid_postmaster(datadir, pid, remoteHost=remoteHost))

    @patch('gppylib.commands.gp.Command.run') 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(127, "", "", False, False),
                                                                   CommandResult(0, "", "", True, False)])
    def test_is_pid_postmaster_no_pwdx_remote(self, mock1, mock2):
        pid = '1234'
        datadir = '/data/primary/gpseg0' 
        remoteHost = 'smdw'
        self.assertTrue(is_pid_postmaster(datadir, pid, remoteHost=remoteHost))

    @patch('gppylib.commands.gp.Command.run', side_effect=[None, None, Exception('Error')]) 
    @patch('gppylib.commands.gp.Command.get_results', side_effect=[CommandResult(0, "", "", True, False),
                                                                   CommandResult(0, "", "", True, False),
                                                                   CommandResult(1, "", "", False, False)])
    def test_is_pid_postmaster_pgrep_failed_remote(self, mock1, mock2):
        pid = '1234'
        datadir = '/data/primary/gpseg0' 
        remoteHost = 'smdw'
        self.assertTrue(is_pid_postmaster(datadir, pid, remoteHost=remoteHost))

    @patch('gppylib.commands.gp.Command.run')
    @patch('gppylib.commands.gp.Command.get_results', return_value=CommandResult(0, "1234", "", True, False))
    def test_get_postmaster_pid_locally(self, mock1, mock2):
        self.assertEqual(get_postmaster_pid_locally('/tmp'), 1234)

    @patch('gppylib.commands.gp.Command.run', side_effect=Exception('Error'))
    def test_get_postmaster_pid_locally_error(self, mock1):
        self.assertEqual(get_postmaster_pid_locally('/tmp'), -1)

    @patch('gppylib.commands.gp.Command.run', return_value=CommandResult(0, "", "", True, False))
    def test_get_postmaster_pid_locally_empty(self, mock1):
        self.assertEqual(get_postmaster_pid_locally('/tmp'), -1)

if __name__ == '__main__':
    run_tests()
