from mock import *
from gp_unittest import *
from gppylib.operations.package import IsVersionCompatible, ListPackages, MigratePackages, AlreadyInstalledError, \
    ARCHIVE_PATH, SyncPackages, CleanGppkg
from gppylib.mainUtils import ExceptionNoStackTraceNeeded

import os
import pickle
import platform


class IsVersionCompatibleTestCase(GpTestCase):
    def setUp(self):
        self.gppkg_mock_values = \
            {
                'main_rpm': 'plperl-1.1-2.x86_64.rpm',
             'postupdate': [],
             'pkgname': 'plperl',
             'description': 'some description.',
             'postinstall': [{'Master': "some reason to restart database"}],
             'postuninstall': [],
             'abspath': 'plperl-ossv5.12.4_pv1.3_gpdb4.3-rhel5-x86_64.gppkg',
             'preinstall': [],
             'version': 'ossv5.12.4_pv1.2_gpdb4.3',
             'pkg': 'plperl-ossv5.12.4_pv1.3_gpdb4.3-rhel5-x86_64.gppkg',
             'dependencies': [],
             'file_list': ['deps',
                           'gppkg_spec.yml',
                           'plperl-1.1-2.x86_64.rpm'],
             'gpdbversion': Mock(),
             'preuninstall': [],
             'os': 'rhel5',
             'architecture': 'x86_64'}

        self.apply_patches([
            patch('gppylib.operations.package.logger', return_value=Mock(spec=['log', 'info', 'debug', 'error'])),
        ])
        self.mock_logger = self.get_mock_from_apply_patch('logger')

    def test__execute_happy_compatible(self):
        gppkg = Mock(**self.gppkg_mock_values)

        subject = IsVersionCompatible(gppkg)
        subject.execute()

        log_messages = [args[1][0] for args in self.mock_logger.method_calls]
        self.assertTrue(len(log_messages) > 2)
        self.assertFalse(any("requires" in message for message in log_messages))

    def test__execute_no_version_fails(self):
        self.gppkg_mock_values['gpdbversion'].isVersionRelease.return_value = False
        gppkg = Mock(**self.gppkg_mock_values)

        subject = IsVersionCompatible(gppkg)
        subject.execute()

        log_messages = [args[1][0] for args in self.mock_logger.method_calls]
        self.assertTrue(len(log_messages) > 2)
        self.assertTrue(any("requires" in message for message in log_messages))


class ListPackagesTestCase(GpTestCase):
    def setUp(self):
        self.apply_patches([
            patch('gppylib.operations.package.logger', return_value=Mock(spec=['log', 'info', 'debug', 'error'])),
            patch('gppylib.operations.package.ListFilesByPattern.run'),
            patch('gppylib.operations.package.MakeDir.run'),
        ])
        self.mock_logger = self.get_mock_from_apply_patch('logger')

        self.subject = ListPackages()
        self.mock_list_files_by_pattern_run = self.get_mock_from_apply_patch('run')

    def test__execute_happy_list_no_packages(self):
        self.mock_list_files_by_pattern_run.return_value = []
        package_name_list = self.subject.execute()
        self.assertTrue(len(package_name_list) == 0)

    def test__execute_happy_list_all_packages(self):
        self.mock_list_files_by_pattern_run.return_value = ['sample.gppkg', 'sample-version-random_OS-arch_type.gppkg']
        package_name_list = self.subject.execute()
        self.assertTrue(len(package_name_list) == 2)
        self.assertTrue(package_name_list == ['sample', 'sample-version'])

    def test__execute_fail_raise_error_with_no_gppkg_postfix(self):
        self.mock_list_files_by_pattern_run.return_value = ['sample']
        with self.assertRaisesRegexp(Exception, "unable to parse sample as a gppkg"):
            self.subject.execute()


class MigratePackagesTestCase(GpTestCase):
    def setUp(self):
        self.apply_patches([
            patch('os.path.samefile'),
            patch('os.makedirs'),
            patch('os.listdir'),
            patch('gppylib.operations.package.InstallPackageLocally'),
            patch('gppylib.operations.package.InstallDebPackageLocally'),
            patch('gppylib.operations.package.CleanGppkg'),
            patch('gppylib.operations.package.logger', return_value=Mock(spec=['log', 'info', 'debug', 'error'])),
        ])

        if platform.linux_distribution()[0] == 'Ubuntu':
            self.mock_install_package_locally = self.get_mock_from_apply_patch('InstallDebPackageLocally')
        else:
            self.mock_install_package_locally = self.get_mock_from_apply_patch('InstallPackageLocally')
        self.mock_listdir = self.get_mock_from_apply_patch('listdir')
        self.mock_logger = self.get_mock_from_apply_patch('logger')
        self.os_path_samefile = self.get_mock_from_apply_patch('samefile')
        self.os_path_samefile.return_value = True

        self.args = dict(from_gphome="/from/GPHOME",
                         to_gphome="/to/gphome",
                         standby_host="standbyhost",
                         segment_host_list=["remotehost"])

    def test__execute_target_gphome_mismatch_raises(self):
        self.os_path_samefile.return_value = False
        self.args['to_gphome'] = '/wrong/gphome'
        subject = MigratePackages(**self.args)

        expected_raise = "The target GPHOME, %s, must match the current \$GPHOME used to launch gppkg." % self.args['to_gphome']
        with self.assertRaisesRegexp(ExceptionNoStackTraceNeeded, expected_raise):
            subject.execute()

    def test__execute_identical_source_target_raises(self):
        self.args['to_gphome'] = '/from/gphome'
        subject = MigratePackages(**self.args)

        expected_raise = "The source and target GPHOMEs, %s => %s, must differ for packages to be migrated." % (self.args['from_gphome'], self.args['to_gphome'])
        with self.assertRaisesRegexp(ExceptionNoStackTraceNeeded, expected_raise):
            subject.execute()

    def test__execute_finds_no_packages(self):
        self.os_path_samefile.side_effect = [True, False]
        subject = MigratePackages(**self.args)
        subject.execute()
        self.mock_logger.info.assert_called_with('There are no packages to migrate from %s.' % self.args['from_gphome'])

    def test__execute_finds_some_packages(self):
        self.os_path_samefile.side_effect = [True, False]
        self.mock_listdir.return_value = ['sample.gppkg', 'sample2.gppkg', 'another-long-one.gppkg']

        subject = MigratePackages(**self.args)
        subject.execute()

        log_messages = [args[1][0] for args in self.mock_logger.method_calls]
        self.assertIn('The following packages will be migrated: %s' % ", ".join(['sample.gppkg', 'sample2.gppkg', 'another-long-one.gppkg']), log_messages)
        self.assertIn('The package migration has completed.', log_messages)

    def test__execute_catches_AlreadyInstalledError(self):
        self.os_path_samefile.side_effect = [True, False]
        self.mock_listdir.return_value = ['sample.gppkg', 'sample2.gppkg', 'another-long-one.gppkg']
        self.mock_install_package_locally.return_value.run.side_effect = AlreadyInstalledError("sample.gppkg")

        subject = MigratePackages(**self.args)
        subject.execute()
        log_messages = [args[1][0] for args in self.mock_logger.method_calls]
        self.assertIn('The following packages will be migrated: %s' % ", ".join(['sample.gppkg', 'sample2.gppkg', 'another-long-one.gppkg']), log_messages)
        self.assertIn('sample.gppkg is already installed.', log_messages)
        self.assertIn('The package migration has completed.', log_messages)

    def test__execute_catches_Exception_failed_to_migrate(self):
        self.os_path_samefile.side_effect = [True, False]
        self.mock_listdir.return_value = ['sample.gppkg', 'sample2.gppkg', 'another-long-one.gppkg']

        # Let's make the second package fail/raise an exception
        self.mock_install_package_locally.return_value.run.side_effect = [None, Exception("foobar something bad"), None]

        subject = MigratePackages(**self.args)
        subject.execute()
        log_messages = [args[1][0] for args in self.mock_logger.method_calls]
        self.assertIn('The following packages will be migrated: %s' % ", ".join(['sample.gppkg', 'sample2.gppkg', 'another-long-one.gppkg']), log_messages)
        self.assertIn('Failed to migrate %s from %s' % (os.path.join(self.args['from_gphome'], ARCHIVE_PATH),
                                                        'sample2.gppkg'), log_messages)
        self.assertIn('The package migration has completed.', log_messages)


class SyncPackagesTestCase(GpTestCase):
    def setUp(self):
        self.apply_patches([
            patch('gppylib.operations.package.CheckDir'),
            patch('gppylib.operations.package.MakeDir'),
            patch('gppylib.operations.package.CheckRemoteDir'),
            patch('gppylib.operations.package.MakeRemoteDir'),
            patch('gppylib.operations.package.Scp'),
            patch('gppylib.operations.package.RemoteOperation'),
            patch('gppylib.operations.package.RemoveRemoteFile'),
            patch('gppylib.operations.package.InstallPackageLocally'),
            patch('os.listdir'),
            patch('gppylib.operations.unix.Command'),
            patch('gppylib.operations.package.logger', return_value=Mock(spec=['log', 'info', 'debug', 'error'])),
        ])

        self.check_dir_mock = self.get_mock_from_apply_patch('CheckDir')
        self.make_dir_mock = self.get_mock_from_apply_patch('MakeDir')
        self.check_remote_dir_mock = self.get_mock_from_apply_patch('CheckRemoteDir')
        self.make_remote_dir_mock = self.get_mock_from_apply_patch('MakeRemoteDir')
        self.mock_listdir = self.get_mock_from_apply_patch('listdir')
        self.mock_command = self.get_mock_from_apply_patch('Command')
        self.mock_logger = self.get_mock_from_apply_patch('logger')
        self.mock_scp = self.get_mock_from_apply_patch('Scp')
        self.mock_install_packages_locally = self.get_mock_from_apply_patch('InstallPackageLocally')


    def test__execute_succeeds_when_local_and_remote_match(self):
        self.check_dir_mock.return_value.run.return_value = False
        self.check_remote_dir_mock.return_value.run.return_value = False
        self.make_dir_mock.return_value.run.return_value = None
        self.mock_listdir.return_value = ['synced.gppkg']
        self.mock_command.return_value.get_results.return_value.stdout = pickle.dumps(['synced.gppkg'])

        subject = SyncPackages('localhost')
        subject.execute()
        self.assertEqual(self.make_dir_mock.call_count, 1)
        self.assertEqual(self.make_remote_dir_mock.call_count, 1)

        log_messages = [args[1][0] for args in self.mock_logger.method_calls]
        self.assertIn('The packages on localhost are consistent.', log_messages)

    def test__execute_install_on_segments_when_package_are_missing(self):
        self.check_dir_mock.return_value.run.return_value = False
        self.check_remote_dir_mock.return_value.run.return_value = False
        self.make_dir_mock.return_value.run.return_value = None
        self.mock_listdir.return_value = ['foo.gppkg', 'bar.gppkg', 'zing.gppkg']
        self.mock_command.return_value.get_results.return_value.stdout = pickle.dumps(['foo.gppkg'])

        hostname = 'localhost'
        subject = SyncPackages(hostname)
        subject.execute()
        self.assertEqual(self.mock_scp.call_count, 2)
        self.assertEqual(self.make_dir_mock.call_count, 1)
        self.assertEqual(self.make_remote_dir_mock.call_count, 1)

        log_messages = [args[1][0] for args in self.mock_logger.method_calls]
        self.assertNotIn('The packages on %s are consistent.' % hostname, log_messages)

    def test__execute_uninstall_on_segments_when_package_is_missing_on_master(self):
        self.check_dir_mock.return_value.run.return_value = False
        self.check_remote_dir_mock.return_value.run.return_value = False
        self.make_dir_mock.return_value.run.return_value = None
        self.mock_listdir.return_value = ['ba.gppkg']
        self.mock_command.return_value.get_results.return_value.stdout = pickle.dumps(['ba.gppkg',
                                                                                       'zing.gppkg',
                                                                                       'ga.gppkg'])
        hostname = 'localhost'
        subject = SyncPackages(hostname)
        subject.execute()
        self.assertEqual(self.make_dir_mock.call_count, 1)
        self.assertEqual(self.make_remote_dir_mock.call_count, 1)

        log_messages = [args[1][0] for args in self.mock_logger.method_calls]
        self.assertIn('The following packages will be uninstalled on localhost: zing.gppkg, ga.gppkg', log_messages)
        self.assertNotIn('The packages on %s are consistent.' % hostname, log_messages)


class CleanGppkgTestCase(GpTestCase):

    def setUp(self):
        self.apply_patches([
            patch('gppylib.operations.package.SyncPackages'),
            patch('gppylib.operations.package.ParallelOperation'),
                ])
        self.sync_packages_mock = self.get_mock_from_apply_patch('SyncPackages')
        self.parallel_operation_mock = self.get_mock_from_apply_patch('ParallelOperation')

    def test_two_segments_unreachable(self):
        self.sync_packages_mock.return_value.get_ret.side_effect = [Exception('first failure'), Exception('second failure')]
        subject = CleanGppkg("localhost", ["fiction", "fairytale"])

        with self.assertRaisesRegexp(Exception, "SyncPackages failed:\nfirst failure\nsecond failure"):
            subject.execute()


if __name__ == '__main__':
    run_tests()
