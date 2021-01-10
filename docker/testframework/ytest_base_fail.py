from random import random, randint
from os import system
from threading import Thread
from time import sleep


class BaseFail(object):
    def __init__(self,
                 node_name,
                 failure_name,
                 ytest_log,
                 table_name,
                 target,
                 args):
        self.__node_name = node_name
        self.__failure_name = failure_name
        self.__ytest_log = ytest_log
        self.__table_name = table_name
        self.__thread = Thread(target=target, args=args)
        self.have_done = False

    def write_info_message(self, content):
        self.__ytest_log.write_log(
            0,
            '%s-%s:%s' % (self.__node_name, self.__failure_name, content),
            self.__table_name)

        return self

    def write_warning_message(self, content):
        self.__ytest_log.write_log(
            1,
            '%s-%s:%s' % (self.__node_name, self.__failure_name, content),
            self.__table_name)

        return self

    def write_error_message(self, content):
        self.__ytest_log.write_log(
            -1,
            '%s-%s:%s' % (self.__node_name, self.__failure_name, content),
            self.__table_name)

        return self

    def start(self):
        self.write_warning_message('Starting YTest...')
        self.__thread.start()
        self.write_warning_message('Start YTest done.')

        return self

    def stop(self):
        self.write_warning_message('Stopping YTest...')
        self.have_done = True
        self.__thread.join()
        self.write_warning_message('Stop YTest done.')

        return self

    def sleep(self, t):
        self.write_info_message('Sleep for %f second. zzZ...' % t)
        sleep(t)
        self.write_info_message('Waked up.')

        return self

    def exec(self, command):
        self.write_warning_message("Now executing command: '%s'..." % command)
        r = system(command)
        if r:
            self.write_error_message(
                "Command '%s' executed fail!\nReturn value: %d" % (command, r))
            return False
        else:
            self.write_info_message("Done! '%s'" % command)
            return True

    def get_0_1_random(self):
        return random()

    def get_random(self, low, high):
        return randint(low if low < high else high, high)
