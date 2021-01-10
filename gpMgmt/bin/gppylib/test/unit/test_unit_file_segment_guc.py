from mock import *
from gp_unittest import *
from gpconfig_modules.file_segment_guc import FileSegmentGuc
from gpconfig_modules.segment_guc import SegmentGuc


class FileSegmentGucTest(GpTestCase):
    def setUp(self):
        row = ['contentid', 'guc_name', 'value_from_file', 'dbid']
        self.subject = FileSegmentGuc(row)

    def test_when_segment_report_success_format_file(self):
        self.subject.context = '0'
        self.assertEquals(self.subject.report_success_format(), "Segment value: value_from_file")

    def test_when_master_report_success_format_file(self):
        self.subject.context = '-1'
        self.assertEquals(self.subject.report_success_format(), "Master  value: value_from_file")

    def test_report_fail_format_file(self):
        self.assertEquals(self.subject.report_fail_format(),
                          ["[context: contentid] [dbid: dbid] [name: guc_name] [value: value_from_file]"])

    def test_report_fail_format_file_when_unset(self):
        self.subject.value = None
        self.assertEquals(self.subject.report_fail_format(),
                          ["[context: contentid] [dbid: dbid] [name: guc_name] [not set in file]"])

    def test_init_with_insufficient_file_values_raises(self):
        row = ['contentid', 'guc_name', 'value_from_file']
        with self.assertRaisesRegexp(Exception, "must provide \['context', 'guc name', 'value', 'dbid'\]"):
            FileSegmentGuc(row)

    def test_when_master_when_integer_dbid_report_success_format_file(self):
        row = [-1, 'guc_name', 'value', 0]
        self.subject = FileSegmentGuc(row)

        self.assertEquals(self.subject.report_success_format(), "Master  value: value")
        self.assertEqual(self.subject.dbid, '0')

    def test_when_value_none_report_success_prints_message(self):
        self.subject.value = None

        self.assertEquals(self.subject.report_success_format(), "No value is set on segments")
