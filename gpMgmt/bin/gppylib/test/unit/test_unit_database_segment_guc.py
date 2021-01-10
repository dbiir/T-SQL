from mock import *
from gp_unittest import *
from gpconfig_modules.database_segment_guc import DatabaseSegmentGuc
from gpconfig_modules.segment_guc import SegmentGuc


class DatabaseSegmentGucTest(GpTestCase):
    def setUp(self):
        row = ['contentid', 'guc_name', 'sql_value']
        self.subject = DatabaseSegmentGuc(row)

    def test_when_segment_report_success_format_database(self):
        self.subject.context = '0'
        self.assertEquals(self.subject.report_success_format(), "Segment value: sql_value")

    def test_when_master_report_success_format_database(self):
        self.subject.context = '-1'
        self.assertEquals(self.subject.report_success_format(), "Master  value: sql_value")

    def test_report_fail_format_database(self):
        self.assertEquals(self.subject.report_fail_format(),
                          ["[context: contentid] [name: guc_name] [value: sql_value]"])

    def test_init_with_insufficient_database_values_raises(self):
        row = ['contentid', 'guc_name']
        with self.assertRaisesRegexp(Exception, "must provide \['context', 'guc name', 'value'\]"):
            DatabaseSegmentGuc(row)

