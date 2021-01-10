from os import listdir, path

from ytest_base_fail import BaseFail
from ytest_log import YTestLog


class DamageFileFail(BaseFail):
    def __init__(self, node_name, ytest_log):
        ytest_log.create_table('DamageFileFail')
        super().__init__(
            node_name,
            'YTest_DamageFileFail',
            ytest_log,
            'DamageFileFail',
            self.__run,
            tuple())
        self.__target_directory = None
        self.__low = None
        self.__high = None
        self.__tab1 = None
        self.__tab2 = None

    def set_target_directory(self, target_directory):
        self.write_warning_message(
            'Set target directory to %s' % target_directory)
        self.__target_directory = target_directory
        self.write_info_message(
            'Now target directory is %s' % self.__target_directory)
        return self

    def set_low(self, low):
        self.write_warning_message('Set low to %f' % low)
        self.__low = low
        self.write_info_message('Now low is %f' % self.__low)

        return self

    def set_high(self, high):
        self.write_warning_message('Set high to %f' % high)
        self.__high = high
        self.write_info_message('Now high is %f' % self.__high)

        return self

    def set_tab1(self, tab1):
        self.write_warning_message('Set tab1 to %f' % tab1)
        self.__tab1 = tab1
        self.write_info_message('Now tab1 is %f' % self.__tab1)

        return self

    def set_tab2(self, tab2):
        self.write_warning_message('Set tab2 to %f' % tab2)
        self.__tab2 = tab2
        self.write_info_message('Now tab2 is %f' % self.__tab2)

        return self
    
    def __gci(self, fp, fl):
        for fi in listdir(fp):
            fip = path.join(fp, fi)
            if path.isdir(fip):
                self.__gci(fip, fl)
            else:
                fl.append(fip)

    def __run(self):
        self.write_info_message('Start damage files...')

        echo_command = '''
        echo "*^^&$#*$#*$##*^^&$#*$#*$##*^^&$#*$##*^^&$#*$#*$##*^^&$#*$#*$##*^^&
        $#*$#*$3##*^^&$#*$#*$##*^^&$#*$#*$#*^^&$#*$#*$12##*^^&$#*$#*$##*^^$#*$#*
        $##*^^$#*$#*$123##*^^$#*$#*$##*$#*$^*$#*$##**(^*&$$&$&(1&*$)123421312)" 
        > %s
        '''
        append_command = '''
        echo "*^^&$#*$#*$##*^^&$#*$#*$##*^^&$#*$##*^^&$#*$#*$##*^^&$#*$#*$##*^^&
        $#*$#*$3##*^^&$#*$#*$##*^^&$#*$#*$#*^^&$#*$#*$12##*^^&$#*$#*$##*^^$#*$#*
        $##*^^$#*$#*$123##*^^$#*$#*$##*$#*$^*$#*$##**(^*&$$&$&(1&*$)123421312)" 
        >> %s
        '''
        delete_command = '''
        rm -r %s
        '''

        while True:
            if self.have_done:
                self.write_info_message(
                    'Damage files work done. Exiting..')
                break

            self.sleep(self.get_random(self.__low, self.__high))

            target_file_list = list()
            self.__gci(self.__target_directory, target_file_list)

            if not len(target_file_list):
                self.write_info_message(
                    "Can't find any file in target directory. Waiting...")
                continue

            self.write_info_message(
                "Found %d files in target directory." % len(target_file_list))

            index = self.get_random(0, len(target_file_list) - 1)
            self.write_info_message(
                'Chosen file: %s' % target_file_list[index])
            r = self.get_0_1_random()
            if r < self.__tab1:
                self.write_warning_message(
                    'Random result: %f, less than tab1 %f, so let target file'
                    ' full of garbage.' % (r, self.__tab1))
                if self.exec(echo_command % target_file_list[index]):
                    self.write_info_message('Echo garbage successfully.')
                else:
                    self.write_error_message('Echo garbage fail.')
            elif r < self.__tab2:
                self.write_warning_message(
                    'Random result: %f, bigger than tab1 %f, but less than tab2'
                    ' %f, so append some garbage to target file...' %
                    (r, self.__tab1, self.__tab2))
                if self.exec(append_command % target_file_list[index]):
                    self.write_info_message('Append garbage successfully.')
                else:
                    self.write_error_message('Append garbage fail.')
            else:
                self.write_warning_message(
                    'Random result: %f, bigger than tab2 %f, so directly remove'
                    ' target file...' % (r, self.__tab2))
                if self.exec(delete_command % target_file_list[index]):
                    self.write_info_message('Delete file successfully.')
                else:
                    self.write_error_message('Delete file fail.')
            self.write_info_message('Damage target files done once.')


if __name__ == '__main__':
    '''test and example'''
    y = YTestLog('DamageFileFailTest')
    DamageFileFail('test_node', y) \
        .set_target_directory('/') \
        .set_low(0) \
        .set_high(3) \
        .set_tab1(0.45) \
        .set_tab2(0.9) \
        .start() \
        .sleep(60) \
        .stop()
    y.save_log(['DamageFileFail']).terminate()
