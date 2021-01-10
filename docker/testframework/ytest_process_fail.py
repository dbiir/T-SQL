from threading import Lock

from psutil import process_iter

from ytest_base_fail import BaseFail
from ytest_log import YTestLog


class DisturbProcessFail(BaseFail):
    def __init__(self, node_name, ytest_log):
        ytest_log.create_table('DisturbProcessFail')
        super().__init__(
            node_name,
            'YTest_DisturbProcessFail',
            ytest_log,
            'DisturbProcessFail',
            self.__run,
            tuple())
        self.__keywords = None
        self.__low = None
        self.__high = None
        self.__tab1 = None
        self.__tab2 = None

    def set_keywords(self, keywords):
        self.write_warning_message(
            'Set keywords to [%s]' % ', '.join(keywords))
        self.__keywords = keywords
        self.write_info_message(
            'Now keywords are [%s]' % ', '.join(self.__keywords))

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

    def __run(self):
        self.write_info_message('Start disturb processes...')

        while True:
            if self.have_done:
                self.write_info_message(
                    'Disturb processes work done. Exiting..')
                break

            self.sleep(self.get_random(self.__low, self.__high))

            target_process_pid_list = list()
            target_process_name_list = list()
            for proc in process_iter():
                tag = 0
                for kw in self.__keywords:
                    if kw in proc.name():
                        tag = 1
                        break
                if tag:
                    target_process_pid_list.append(proc.pid)
                    target_process_name_list.append(
                        str((proc.pid, proc.name())))

            if not len(target_process_pid_list):
                self.write_info_message(
                    "Can't find any process with any keywords. Waiting...")
                continue

            self.write_info_message(
                'Target processes: [%s]' % ', '.join(target_process_name_list))
            self.write_info_message(
                'Selected %d target processes.' % len(target_process_pid_list))

            index = self.get_random(0, len(target_process_pid_list) - 1)
            self.write_info_message(
                'Selected process: %s' % str(target_process_name_list[index]))
            r = self.get_0_1_random()
            if r < self.__tab1:
                self.write_warning_message(
                    'Random result: %f, less than tab1 %f, so hang up the '
                    'selected process...' % (r, self.__tab1))
                if self.exec(
                    'kill -s STOP %d' % target_process_pid_list[index]):
                    self.write_info_message('Hanged up the selected process.')
                else:
                    self.write_error_message(
                        'Fail to hang up the selected process.')
                    continue

                self.sleep(self.get_random(self.__low, self.__high))

                self.write_info_message('Now recover the selected process...')

                if self.exec('kill -s CONT %d' %
                             target_process_pid_list[index]):
                    self.write_info_message('Recovered the selected process.')
                else:
                    self.write_error_message(
                        'Fail to recover the selected process.')
            elif r < self.__tab2:
                self.write_info_message(
                    'Random result: %f, bigger than tab1 %f, but less than'
                    ' tab2 %f, so modify the priority of the selected process'
                    '...' % (r, self.__tab1, self.__tab2))
                if self.exec(
                    'renice -n 19 -p %d' % target_process_pid_list[index]):
                    self.write_info_message(
                        "Modify the selected process's priority done.")
                else:
                    self.write_error_message(
                        "Fail to Modify the selected process's priority.")
                    continue

                self.sleep(self.get_random(self.__low, self.__high))

                self.write_info_message(
                    "Now reset the selected process's priority.")
                if self.exec(
                    'renice -n 0 -p %d' % target_process_pid_list[index]):
                    self.write_info_message(
                        "Reset the selected process's priority done.")
                else:
                    self.write_error_message(
                        "Fail to reset the selected process's priority.")
            else:
                self.write_info_message(
                    'Random result: %f, bigger than tab2 %f, so directly'
                    ' kill the selected process...' % (r, self.__tab2))
                if self.exec(
                    'kill -s KILL %d' % target_process_pid_list[index]):
                    self.write_info_message('Killed the selected process.')
                else:
                    self.write_error_message(
                        'Fail to kill the selected process.')


if __name__ == '__main__':
    '''test and example'''
    y = YTestLog('DisturbProcessFailTest')
    DisturbProcessFail('test_node', y) \
        .set_keywords(['test']) \
        .set_low(0) \
        .set_high(3) \
        .set_tab1(0.45) \
        .set_tab2(0.9) \
        .start() \
        .sleep(60) \
        .stop()
    y.save_log(['DisturbProcessFail']).terminate()
