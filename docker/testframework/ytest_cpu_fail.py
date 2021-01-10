from multiprocessing import Process as Mp
from os import cpu_count

from psutil import Process as Pp

from ytest_base_fail import BaseFail
from ytest_log import YTestLog


class CPUOverloadFail(BaseFail):
    def __init__(self, node_name, ytest_log):
        ytest_log.create_table('CPUOverloadFail')
        super().__init__(
            node_name,
            'YTest_CPUOverloadFail',
            ytest_log,
            'CPUOverloadFail',
            self.__run,
            tuple())
        self.__ratio = None
        self.__timeslice = None
        self.__create_temp_processes()

    def __create_temp_processes(self):
        cpus = cpu_count()
        self.write_info_message('Found cpu: %d.' % cpus)
        self.__mp_list = list()
        self.__pp_list = list()
        for i in range(cpus):
            mp = Mp(target=self.__noop, args=tuple())
            mp.start()
            self.__mp_list.append(mp)
            pp = Pp(mp.pid)
            pp.suspend()
            self.__pp_list.append(pp)

    def set_ratio(self, ratio):
        self.write_warning_message('Set ratio to %f' % ratio)
        self.__ratio = ratio
        self.write_info_message('Now ratio is %f' % self.__ratio)

        return self

    def set_timeslice(self, timeslice):
        self.write_warning_message('Set timeslice to %f' % timeslice)
        self.__timeslice = timeslice
        self.write_info_message('Now timeslice is %f' % self.__timeslice)

        return self

    def clear(self):
        self.write_info_message('Clearing temp processes...')
        for p in self.__mp_list:
            p.terminate()
            p.join()
        self.write_info_message('Clear temp processes done.')

        return self

    def __noop(self):
        a = 0
        b = 1
        while True:
            c = a + b
            a = b
            b = c

    def __run(self):
        self.write_info_message('Start overload CPU...')

        while True:
            if self.have_done:
                self.write_info_message('Overload CPU work done. Exiting...')
                break

            r = self.get_0_1_random()
            if r < self.__ratio:
                self.write_info_message(
                    'Random result: %f, less than ratio %f, start overload cpu'
                    '...' %(r, self.__ratio))
                self.write_warning_message('Now resuming all temp processes...')
                for p in self.__pp_list:
                    p.resume()
                self.write_info_message('Resume temp processes done.')

                self.sleep(self.__timeslice)

                self.write_warning_message(
                    'Now suspending all temp processes...')
                for p in self.__pp_list:
                    p.suspend()
                self.write_info_message('Suspend processes done.')

                self.write_info_message('Overload cpu done once...')
            else:
                self.write_info_message(
                    'Random result: %f, bigger than ratio %f, nothing will '
                    'happen.' % (r, self.__ratio))
                self.sleep(self.__timeslice)


if __name__ == '__main__':
    '''test and example'''
    y = YTestLog('CPUOverloadFailTest')
    CPUOverloadFail('test_node', y) \
        .set_ratio(0.7) \
        .set_timeslice(2) \
        .start() \
        .sleep(60) \
        .stop() \
        .clear()
    y.save_log(['CPUOverloadFail']).terminate()
