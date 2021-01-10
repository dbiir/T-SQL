from atexit import register
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from multiprocessing import Process
from os import getenv, fork, umask, chdir, setsid, dup2, remove
from sys import exit, stdin, stdout, stderr
from time import sleep

from grpc import server

from ytest_cpu_fail import CPUOverloadFail
from ytest_file_fail import DamageFileFail
from ytest_framework_msg_pb2 import YTestResponse
from ytest_framework_msg_pb2 import SaveLogResponse
from ytest_framework_msg_pb2 import CPUOverloadFailResponse
from ytest_framework_msg_pb2 import DamageFileFailResponse
from ytest_framework_msg_pb2 import DisturbNetworkPackageFailResponse
from ytest_framework_msg_pb2 import DisturbProcessFailResponse
from ytest_framework_msg_pb2_grpc import add_YTestServicer_to_server
from ytest_framework_msg_pb2_grpc import YTestServicer
from ytest_network_fail import DisturbNetworkPackageFail
from ytest_process_fail import DisturbProcessFail
from ytest_log import YTestLog


class YTestServer(YTestServicer):
    def __init__(self):
        self.__host_name = getenv('MYNAME')
        self.__metadata_table_name = '%s_metadata' % self.__host_name
        self.__ylog = YTestLog(self.__host_name) \
            .create_table(self.__metadata_table_name)
        self.__cof = None
        self.__dff = None
        self.__dnpf = None
        self.__dpf = None
        # add new fail type here

    def __write_info_message(self, content):
        self.__ylog.write_log(0, content, self.__metadata_table_name)

    def __write_warning_message(self, content):
        self.__ylog.write_log(1, content, self.__metadata_table_name)

    def __write_error_message(self, content):
        self.__ylog.write_log(-1, content, self.__metadata_table_name)

    def GetRequest(self, request, context):
        if request.type == -1:
            self.__write_info_message("Received save log request.")
            return self.deal_with_save_log_request(request.slr)
        elif request.type == 0:
            self.__write_info_message("Received noop request.")
            return self.deal_with_noop_request()
        elif request.type == 1:
            self.__write_info_message('Received cpu overload fail request')
            return self.deal_with_cpu_overload_fail_request(request.cofr)
        elif request.type == 2:
            self.__write_info_message('Received damage file fail request')
            return self.deal_with_damage_file_fail_request(request.dffr)
        elif request.type == 3:
            self.__write_info_message(
                'Received disturb network package fail request')
            return self.deal_with_disturb_network_package_fail_request(
                request.dnpfr)
        elif request.type == 4:
            self.__write_info_message('Received disturb process fail request')
            return self.deal_with_disturb_process_fail_request(request.dpfr)
        # add new fail type here
        else:
            self.__write_error_message(
                'Received error message! Request type: %d' % request.type)
            return self.deal_with_error_request()
    
    def deal_with_noop_request(self):
        return YTestResponse(type=0)

    def deal_with_error_request(self):
        return YTestResponse(
            type=-2,
            error_type=1,
            error_message='Error type request.')

    def deal_with_save_log_request(self, request):
        save_log_list = list()
        if request.save_cof_log:
            save_log_list.append('CPUOverloadFail')
        if request.save_dff_log:
            save_log_list.append('DamageFileFail')
        if request.save_dnpf_log:
            save_log_list.append('DisturbNetworkPackageFail')
        if request.save_dpf_log:
            save_log_list.append('DisturbProcessFail')
        # add new fail type here
        self.__ylog.save_log(save_log_list)
        # todo: wait for save log done then return response
        return YTestResponse(
            type=-1,
            slr=SaveLogResponse(successfully=True))

    def deal_with_cpu_overload_fail_request(self, request):
        if request.type == 0:
            self.__write_info_message(
                'Received cpu overload fail noop request.')
            return YTestResponse(
                type=1,
                cofr=CPUOverloadFailResponse(type=0))
        elif request.type == 1:
            if self.__cof:
                self.__write_error_message(
                    'Error! CPUOverloadFail already exists.')
                return YTestResponse(
                    type=1,
                    cofr=CPUOverloadFailResponse(
                        type=2,
                        error_type=1,
                        error_message="CPUOverloadFail already exists."))
            self.__cof = CPUOverloadFail(self.__host_name, self.__ylog) \
                .set_ratio(request.ratio) \
                .set_timeslice(request.timeslice) \
                .start()
            self.__write_info_message('CPUOverloadFail started successfully.')
            return YTestResponse(
                type=1,
                cofr=CPUOverloadFailResponse(type=1))
        elif request.type == 2:
            if not self.__cof:
                self.__write_error_message(
                    "Error! CPUOverloadFail doesn't exist.")
                return YTestResponse(
                    type=1,
                    cofr=CPUOverloadFailResponse(
                        type=2,
                        error_type=2,
                        error_message="CPUOverloadFail doesn't exist."))
            self.__cof.stop().clear()
            self.__cof = None
            self.__write_info_message('CPUOverloadFail stopped successfully.')
            return YTestResponse(
                type=1,
                cofr=CPUOverloadFailResponse(type=1))
        elif request.type == 3:
            if not self.__cof:
                self.__write_error_message(
                    "Error! CPUOverloadFail doesn't exist.")
                return YTestResponse(
                    type=1,
                    cofr=CPUOverloadFailResponse(
                        type=2,
                        error_type=2,
                        error_message="CPUOverloadFail doesn't exist."))
            self.__cof.set_ratio(request.ratio)
            self.__write_info_message('Set ratio successfully.')
            return YTestResponse(
                type=1,
                cofr=CPUOverloadFailResponse(type=1))
        elif request.type == 4:
            if not self.__cof:
                self.__write_error_message(
                    "Error! CPUOverloadFail doesn't exist.")
                return YTestResponse(
                    type=1,
                    cofr=CPUOverloadFailResponse(
                        type=2,
                        error_type=2,
                        error_message="CPUOverloadFail doesn't exist."))
            self.__cof.set_timeslice(request.timeslice)
            self.__write_info_message('Set timeslice successfully.')
            return YTestResponse(
                type=1,
                cofr=CPUOverloadFailResponse(type=1))
        else:
            self.__write_error_message(
                'Received cpu overload fail error request!')
            return YTestResponse(
                type=1,
                cofr=CPUOverloadFailResponse(
                    type=2,
                    error_type=3,
                    error_message="Error request!"))

    def deal_with_damage_file_fail_request(self, request):
        if request.type == 0:
            self.__write_info_message('Received damage file fail noop request.')
            return YTestResponse(
                type=2,
                dffr=DamageFileFailResponse(type=0))
        elif request.type == 1:
            if self.__dff:
                self.__write_error_message(
                    'Error! DamageFileFail already exists.')
                return YTestResponse(
                    type=2,
                    dffr=DamageFileFailResponse(
                        type=2,
                        error_type=1,
                        error_message="DamageFileFail already exists."))
            self.__dff = DamageFileFail(self.__host_name, self.__ylog) \
                .set_target_directory(request.target_directory) \
                .set_low(request.low) \
                .set_high(request.high) \
                .set_tab1(request.tab1) \
                .set_tab2(request.tab2) \
                .start()
            self.__write_info_message('DamageFileFail started successfully.')
            return YTestResponse(
                type=2,
                dffr=DamageFileFailResponse(type=1))
        elif request.type == 2:
            if not self.__dff:
                self.__write_error_message(
                    "Error! DamageFileFail doesn't exist.")
                return YTestResponse(
                    type=2,
                    dffr=DamageFileFailResponse(
                        type=2,
                        error_type=2,
                        error_message="DamageFileFail doesn't exist."))
            self.__dff.stop()
            self.__dff = None
            self.__write_info_message('DamageFileFail stopped successfully.')
            return YTestResponse(
                type=2,
                dffr=DamageFileFailResponse(type=1))
        elif request.type == 3:
            if not self.__dff:
                self.__write_error_message(
                    "Error! DamageFileFail doesn't exist.")
                return YTestResponse(
                    type=2,
                    dffr=DamageFileFailResponse(
                        type=2,
                        error_type=2,
                        error_message="DamageFileFail doesn't exist."))
            self.__dff.set_low(request.low)
            self.__write_info_message('Set low successfully.')
            return YTestResponse(
                type=2,
                dffr=DamageFileFailResponse(type=1))
        elif request.type == 4:
            if not self.__dff:
                self.__write_error_message(
                    "Error! DamageFileFail doesn't exist.")
                return YTestResponse(
                    type=2,
                    dffr=DamageFileFailResponse(
                        type=2,
                        error_type=2,
                        error_message="DamageFileFail doesn't exist."))
            self.__dff.set_high(request.high)
            self.__write_info_message('Set high successfully.')
            return YTestResponse(
                type=2,
                dffr=DamageFileFailResponse(type=1))
        elif request.type == 5:
            if not self.__dff:
                self.__write_error_message(
                    "Error! DamageFileFail doesn't exist.")
                return YTestResponse(
                    type=2,
                    dffr=DamageFileFailResponse(
                        type=2,
                        error_type=2,
                        error_message="DamageFileFail doesn't exist."))
            self.__dff.set_tab1(request.tab1)
            self.__write_info_message('Set tab1 successfully.')
            return YTestResponse(
                type=2,
                dffr=DamageFileFailResponse(type=1))
        elif request.type == 6:
            if not self.__dff:
                self.__write_error_message(
                    "Error! DamageFileFail doesn't exist.")
                return YTestResponse(
                    type=2,
                    dffr=DamageFileFailResponse(
                        type=2,
                        error_type=2,
                        error_message="DamageFileFail doesn't exist."))
            self.__dff.set_tab2(request.tab2)
            self.__write_info_message('Set tab2 successfully.')
            return YTestResponse(
                type=2,
                dffr=DamageFileFailResponse(type=1))
        else:
            self.__write_error_message(
                'Received damage file fail error request!')
            return YTestResponse(
                type=2,
                dffr=DamageFileFailResponse(
                    type=2,
                    error_type=3,
                    error_message="Error request!"))

    def deal_with_disturb_network_package_fail_request(self, request):
        if request.type == 0:
            self.__write_info_message(
                'Received disturb network package fail noop request.')
            return YTestResponse(
                type=3,
                dnpfr=DisturbNetworkPackageFailResponse(type=0))
        elif request.type == 1:
            if self.__dnpf:
                self.__write_error_message(
                    'Error! DisturbNetworkPackageFail already exists.')
                return YTestResponse(
                    type=3,
                    dnpfr=DisturbNetworkPackageFailResponse(
                        type=2,
                        error_type=1,
                        error_message="DisturbNetworkPackageFail already exists."))
            self.__dnpf = DisturbNetworkPackageFail(
                self.__host_name,
                self.__ylog) \
                .set_target_network_device(request.target_network_device) \
                .start()
            self.__write_info_message(
                'DisturbNetworkPackageFail started successfully.')
            return YTestResponse(
                type=3,
                dnpfr=DisturbNetworkPackageFailResponse(type=1))
        elif request.type == 2:
            if not self.__dnpf:
                self.__write_error_message(
                    "Error! DisturbNetworkPackageFail doesn't exist.")
                return YTestResponse(
                    type=3,
                    dnpfr=DisturbNetworkPackageFailResponse(
                        type=2,
                        error_type=2,
                        error_message="DisturbNetworkPackageFail doesn't exist."))
            self.__dnpf.stop()
            self.__dnpf = None
            self.__write_info_message(
                'DisturbNetworkPackageFail stopped successfully.')
            return YTestResponse(
                type=3,
                dnpfr=DisturbNetworkPackageFailResponse(type=1))
        else:
            self.__write_error_message(
                'Received disturb network package fail error request!')
            return YTestResponse(
                type=3,
                dnpfr=DisturbNetworkPackageFailResponse(
                    type=2,
                    error_type=3,
                    error_message="Error request!"))

    def deal_with_disturb_process_fail_request(self, request):
        if request.type == 0:
            self.__write_info_message(
                'Received disturb process fail noop request.')
            return YTestResponse(
                type=4,
                dpfr=DisturbProcessFailResponse(type=0))
        elif request.type == 1:
            if self.__dpf:
                self.__write_error_message(
                    'Error! DisturbProcessFail already exists.')
                return YTestResponse(
                    type=4,
                    dpfr=DisturbProcessFailResponse(
                        type=2,
                        error_type=1,
                        error_message="DisturbProcessFail already exists."))
            self.__dpf = DisturbProcessFail(self.__host_name, self.__ylog) \
                .set_keywords(request.keywords.split()) \
                .set_low(request.low) \
                .set_high(request.high) \
                .set_tab1(request.tab1) \
                .set_tab2(request.tab2) \
                .start()
            self.__write_info_message(
                'DisturbProcessFail started successfully.')
            return YTestResponse(
                type=4,
                dpfr=DisturbProcessFailResponse(type=1))
        elif request.type == 2:
            if not self.__dpf:
                self.__write_error_message(
                    "Error! DisturbProcessFail doesn't exist.")
                return YTestResponse(
                    type=4,
                    dpfr=DisturbProcessFailResponse(
                        type=2,
                        error_type=2,
                        error_message="DisturbProcessFail doesn't exist."))
            self.__dpf.stop()
            self.__dpf = None
            self.__write_info_message(
                'DisturbProcessFail stopped successfully.')
            return YTestResponse(
                type=4,
                dpfr=DisturbProcessFailResponse(type=1))
        elif request.type == 3:
            if not self.__dpf:
                self.__write_error_message(
                    "Error! DisturbProcessFail doesn't exist.")
                return YTestResponse(
                    type=4,
                    dpfr=DisturbProcessFailResponse(
                        type=2,
                        error_type=2,
                        error_message="DisturbProcessFail doesn't exist."))
            self.__dpf.set_low(request.low)
            self.__write_info_message('Set low successfully.')
            return YTestResponse(
                type=4,
                dpfr=DisturbProcessFailResponse(type=1))
        elif request.type == 4:
            if not self.__dpf:
                self.__write_error_message(
                    "Error! DisturbProcessFail doesn't exist.")
                return YTestResponse(
                    type=4,
                    dpfr=DisturbProcessFailResponse(
                        type=2,
                        error_type=2,
                        error_message="DisturbProcessFail doesn't exist."))
            self.__dpf.set_high(request.high)
            self.__write_info_message('Set high successfully.')
            return YTestResponse(
                type=4,
                dpfr=DisturbProcessFailResponse(type=1))
        elif request.type == 5:
            if not self.__dpf:
                self.__write_error_message(
                    "Error! DisturbProcessFail doesn't exist.")
                return YTestResponse(
                    type=4,
                    dpfr=DisturbProcessFailResponse(
                        type=2,
                        error_type=2,
                        error_message="DisturbProcessFail doesn't exist."))
            self.__dpf.set_tab1(request.tab1)
            self.__write_info_message('Set tab1 successfully.')
            return YTestResponse(
                type=4,
                dpfr=DisturbProcessFailResponse(type=1))
        elif request.type == 6:
            if not self.__dpf:
                self.__write_error_message(
                    "Error! DisturbProcessFail doesn't exist.")
                return YTestResponse(
                    type=4,
                    dpfr=DisturbProcessFailResponse(
                        type=2,
                        error_type=2,
                        error_message="DisturbProcessFail doesn't exist."))
            self.__dpf.set_tab2(request.tab2)
            self.__write_info_message('Set tab2 successfully.')
            return YTestResponse(
                type=4,
                dpfr=DisturbProcessFailResponse(type=1))
        else:
            self.__write_error_message('Received disturb process fail error request!')
            return YTestResponse(
                type=4,
                dpfr=DisturbProcessFailResponse(
                    type=2,
                    error_type=3,
                    error_message="Error request!"))


def main():
    sleep(30)
    s = server(ThreadPoolExecutor(max_workers=5))
    add_YTestServicer_to_server(YTestServer(), s)
    s.add_insecure_port(
        '[::]:%s' % ConfigParser().read('config.ini')['Network']['port'])
    s.start()
    s.wait_for_termination()


if __name__ == '__main__':
    pid = fork()
    if pid:
        exit(0)
    chdir('/')
    umask(0)
    setsid()
    pid = fork()
    if pid:
        exit(0)
    stdout.flush()
    stderr.flush()

    with open('/dev/null') as read_null, open('/dev/null', 'w') as write_null:
        dup2(read_null.fileno(), stdin.fileno())
        dup2(write_null.fileno(), stdout.fileno())
        dup2(write_null.fileno(), stderr.fileno())

    main()
