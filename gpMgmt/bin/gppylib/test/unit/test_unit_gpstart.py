import imp
import logging
import os
import sys

from mock import Mock, patch

from gparray import Segment, GpArray
from gppylib.operations.startSegments import StartSegmentsResult
from gppylib.test.unit.gp_unittest import GpTestCase, run_tests
from gppylib.commands import gp


class GpStart(GpTestCase):
    def setUp(self):
        # because gpstart does not have a .py extension,
        # we have to use imp to import it
        # if we had a gpstart.py, this is equivalent to:
        #   import gpstart
        #   self.subject = gpstart
        gpstart_file = os.path.abspath(os.path.dirname(__file__) + "/../../../gpstart")
        self.subject = imp.load_source('gpstart', gpstart_file)
        self.subject.logger = Mock(
            spec=['log', 'warn', 'info', 'debug', 'error', 'warning', 'fatal', 'warning_to_file_only'])

        self.os_environ = dict(MASTER_DATA_DIRECTORY='/tmp/mdd', GPHOME='/tmp/gphome', GP_MGMT_PROCESS_COUNT=1,
                               LANGUAGE=None)
        self.gparray = self._createGpArrayWith2Primary2Mirrors()
        self.segments_by_content_id = GpArray.getSegmentsByContentId(self.gparray.getSegDbList())

        start_result = StartSegmentsResult()
        start_result.addSuccess(self.primary0)

        self.apply_patches([
            patch('os.getenv', side_effect=self._get_env),
            patch('gpstart.os.path.exists'),
            patch('gpstart.gp'),
            patch('gpstart.pgconf'),
            patch('gpstart.unix'),
            patch('gpstart.dbconn.DbURL'),
            patch('gpstart.dbconn.connect'),
            patch('gpstart.GpArray.initFromCatalog', return_value=self.gparray),
            patch('gpstart.GpArray.getSegmentsByContentId', return_value=self.segments_by_content_id),
            patch('gpstart.GpArray.getSegmentsGroupedByValue',
                  side_effect=[{2: self.primary0, 3: self.primary1}, [], []]),
            patch('gpstart.GpEraFile'),
            patch('gpstart.userinput'),
            patch('gpstart.HeapChecksum'),
            patch('gpstart.log_to_file_only'),
            patch("gpstart.StartSegmentsOperation"),
            patch("gpstart.base.WorkerPool"),
            patch("gpstart.gp.MasterStart.local"),
            patch("gpstart.pg.DbStatus.local"),
            patch("gpstart.TableLogger"),
            patch('gpstart.PgControlData'),
        ])

        self.mock_start_result = self.get_mock_from_apply_patch('StartSegmentsOperation')
        self.mock_start_result.return_value.startSegments.return_value.getSuccessfulSegments.return_value = start_result.getSuccessfulSegments()

        self.mock_os_path_exists = self.get_mock_from_apply_patch('exists')
        self.mock_gp = self.get_mock_from_apply_patch('gp')
        self.mock_pgconf = self.get_mock_from_apply_patch('pgconf')
        self.mock_userinput = self.get_mock_from_apply_patch('userinput')
        self.mock_heap_checksum = self.get_mock_from_apply_patch('HeapChecksum')
        self.mock_heap_checksum.return_value.get_segments_checksum_settings.return_value = ([1], [1])
        self.mock_heap_checksum.return_value.are_segments_consistent.return_value = True
        self.mock_heap_checksum.return_value.check_segment_consistency.return_value = ([], [], None)
        self.mock_pgconf.readfile.return_value = Mock()
        self.mock_gplog_log_to_file_only = self.get_mock_from_apply_patch("log_to_file_only")

        self.mock_gp.get_masterdatadir.return_value = 'masterdatadir'
        self.mock_gp.GpCatVersion.local.return_value = 1
        self.mock_gp.GpCatVersionDirectory.local.return_value = 1
        self.mock_gp.DEFAULT_GPSTART_NUM_WORKERS = gp.DEFAULT_GPSTART_NUM_WORKERS
        sys.argv = ["gpstart"]  # reset to relatively empty args list

    def tearDown(self):
        super(GpStart, self).tearDown()

    def test_option_master_success_without_auto_accept(self):
        sys.argv = ["gpstart", "-m"]
        self.mock_userinput.ask_yesno.return_value = True
        self.subject.unix.PgPortIsActive.local.return_value = False

        self.mock_os_path_exists.side_effect = os_exists_check

        parser = self.subject.GpStart.createParser()
        options, args = parser.parse_args()

        gpstart = self.subject.GpStart.createProgram(options, args)
        return_code = gpstart.run()

        self.assertEqual(self.mock_userinput.ask_yesno.call_count, 1)
        self.mock_userinput.ask_yesno.assert_called_once_with(None, '\nContinue with master-only startup', 'N')
        self.subject.logger.info.assert_any_call('Starting Master instance in admin mode')
        self.subject.logger.info.assert_any_call('Master Started...')
        self.assertEqual(return_code, 0)

    def test_option_master_success_with_auto_accept(self):
        sys.argv = ["gpstart", "-m", "-a"]
        self.mock_userinput.ask_yesno.return_value = True
        self.subject.unix.PgPortIsActive.local.return_value = False

        self.mock_os_path_exists.side_effect = os_exists_check

        parser = self.subject.GpStart.createParser()
        options, args = parser.parse_args()

        gpstart = self.subject.GpStart.createProgram(options, args)
        return_code = gpstart.run()

        self.assertEqual(self.mock_userinput.ask_yesno.call_count, 0)
        self.subject.logger.info.assert_any_call('Starting Master instance in admin mode')
        self.subject.logger.info.assert_any_call('Master Started...')
        self.assertEqual(return_code, 0)

    def test_output_to_stdout_and_log_for_master_only_happens_before_heap_checksum(self):
        sys.argv = ["gpstart", "-m"]
        self.mock_userinput.ask_yesno.return_value = True
        self.subject.unix.PgPortIsActive.local.return_value = False
        self.mock_os_path_exists.side_effect = os_exists_check
        parser = self.subject.GpStart.createParser()
        options, args = parser.parse_args()
        gpstart = self.subject.GpStart.createProgram(options, args)

        return_code = gpstart.run()

        self.assertEqual(return_code, 0)
        self.assertEqual(self.mock_userinput.ask_yesno.call_count, 1)
        self.mock_userinput.ask_yesno.assert_called_once_with(None, '\nContinue with master-only startup', 'N')
        self.subject.logger.info.assert_any_call('Starting Master instance in admin mode')
        self.subject.logger.info.assert_any_call('Master Started...')

        self.assertEquals(self.mock_gplog_log_to_file_only.call_count, 0)

    def test_skip_checksum_validation_succeeds(self):
        sys.argv = ["gpstart", "-a", "--skip-heap-checksum-validation"]
        self.mock_heap_checksum.return_value.get_segments_checksum_settings.return_value = ([1], [1])
        self.subject.unix.PgPortIsActive.local.return_value = False
        self.mock_os_path_exists.side_effect = os_exists_check
        parser = self.subject.GpStart.createParser()
        options, args = parser.parse_args()
        gpstart = self.subject.GpStart.createProgram(options, args)

        return_code = gpstart.run()

        self.assertEqual(return_code, 0)
        messages = [msg[0][0] for msg in self.subject.logger.info.call_args_list]
        self.assertNotIn('Heap checksum setting is consistent across the cluster', messages)
        self.subject.logger.warning.assert_any_call('Because of --skip-heap-checksum-validation, '
                                                    'the GUC for data_checksums '
                                                    'will not be checked between master and segments')

    def test_log_when_heap_checksum_validation_fails(self):
        sys.argv = ["gpstart", "-a", "-S"]
        self.mock_os_path_exists.side_effect = os_exists_check
        self.mock_heap_checksum.return_value.get_master_value.return_value = 1
        start_failure = StartSegmentsResult()
        start_failure.addFailure(self.mirror1, "fictitious reason", gp.SEGSTART_ERROR_CHECKSUM_MISMATCH)
        self.mock_start_result.return_value.startSegments.return_value.getFailedSegmentObjs.return_value = start_failure.getFailedSegmentObjs()

        parser = self.subject.GpStart.createParser()
        options, args = parser.parse_args()
        gpstart = self.subject.GpStart.createProgram(options, args)

        return_code = gpstart.run()
        self.assertEqual(return_code, 1)
        messages = [msg[0][0] for msg in self.subject.logger.info.call_args_list]
        self.assertIn("DBID:5  FAILED  host:'sdw1' datadir:'/data/mirror1' with reason:'fictitious reason'", messages)

    def test_standby_startup_skipped(self):
        sys.argv = ["gpstart", "-a", "-y"]

        parser = self.subject.GpStart.createParser()
        options, args = parser.parse_args()
        gpstart = self.subject.GpStart.createProgram(options, args)

        return_value = gpstart._start_standby()
        self.assertFalse(return_value)
        messages = [msg[0][0] for msg in self.subject.logger.info.call_args_list]
        self.assertIn("No standby master configured.  skipping...", messages)

    def _createGpArrayWith2Primary2Mirrors(self):
        self.master = Segment.initFromString(
            "1|-1|p|p|s|u|mdw|mdw|5432|/data/master")
        self.primary0 = Segment.initFromString(
            "2|0|p|p|s|u|sdw1|sdw1|40000|/data/primary0")
        self.primary1 = Segment.initFromString(
            "3|1|p|p|s|u|sdw2|sdw2|40001|/data/primary1")
        self.mirror0 = Segment.initFromString(
            "4|0|m|m|s|u|sdw2|sdw2|50000|/data/mirror0")
        self.mirror1 = Segment.initFromString(
            "5|1|m|m|s|u|sdw1|sdw1|50001|/data/mirror1")
        self.standby = Segment.initFromString(
            "6|-1|m|m|s|u|sdw3|sdw3|5433|/data/standby")
        return GpArray([self.master, self.primary0, self.primary1, self.mirror0, self.mirror1])

    def _get_env(self, arg):
        if arg not in self.os_environ:
            return None
        return self.os_environ[arg]


def os_exists_check(arg):
    # Skip file related checks
    if 'pg_log' in arg:
        return True
    elif 'postmaster.pid' in arg or '.s.PGSQL' in arg:
        return False
    return False


if __name__ == '__main__':
    run_tests()
