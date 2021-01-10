from configparser import ConfigParser
from time import sleep
from sys import argv

from grpc import insecure_channel

from ytest_framework_msg_pb2 import YTestRequest
from ytest_framework_msg_pb2 import SaveLogRequest
from ytest_framework_msg_pb2 import CPUOverloadFailRequest
from ytest_framework_msg_pb2 import DamageFileFailRequest
from ytest_framework_msg_pb2 import DisturbNetworkPackageFailRequest
from ytest_framework_msg_pb2 import DisturbProcessFailRequest
from ytest_framework_msg_pb2_grpc import YTestStub


class YTestController:
    def __init__(self, config_file):
        self.__cp = ConfigParser()
        self.__cp.read(config_file)
        self.__response_list = list()
        self.__stub = [
            YTestStub(
                insecure_channel(
                    '%s:%s' %
                    (self.__cp['Network']['master'],
                     self.__cp['Network']['port']))),
            YTestStub(
                insecure_channel(
                    '%s:%s' %
                    (self.__cp['Network']['segment1'],
                     self.__cp['Network']['port']))),
            YTestStub(
                insecure_channel(
                    '%s:%s' %
                    (self.__cp['Network']['segment2'],
                     self.__cp['Network']['port']))),
            YTestStub(
                insecure_channel(
                    '%s:%s' %
                    (self.__cp['Network']['segment3'],
                     self.__cp['Network']['port'])))]

    def __start_fail_on(self, node, fail_type, *arg):
        if fail_type == -1:
            return self.__stub[node].GetRequest.future(
                YTestRequest(
                    type=-1,
                    slr=SaveLogRequest(
                        save_cof_log=arg[0],
                        save_dff_log=arg[1],
                        save_dnpf_log=arg[2],
                        save_dpf_log=arg[3])))
        elif fail_type == 0:
            return self.__stub[node].GetRequest.future(
                YTestRequest(type=0))
        elif fail_type == 1:
            return self.__stub[node].GetRequest.future(
                YTestRequest(
                    type=1,
                    cofr=CPUOverloadFailRequest(
                        type=1,
                        ratio=arg[0],
                        timeslice=arg[1])))
        elif fail_type == 2:
            return self.__stub[node].GetRequest.future(
                YTestRequest(
                    type=2,
                    dffr=DamageFileFailRequest(
                        type=1,
                        target_directory=arg[0],
                        low=arg[1],
                        high=arg[2],
                        tab1=arg[3],
                        tab2=arg[4])))
        elif fail_type == 3:
            return self.__stub[node].GetRequest.future(
                YTestRequest(
                    type=3,
                    dnpfr=DisturbNetworkPackageFailRequest(
                        type=1,
                        target_network_device=arg[0])))
        elif fail_type == 4:
            return self.__stub[node].GetRequest.future(
                YTestRequest(
                    type=4,
                    dpfr=DisturbProcessFailRequest(
                        type=1,
                        keywords=arg[0],
                        low=arg[1],
                        high=arg[2],
                        tab1=arg[3],
                        tab2=arg[4])))
        else:
            print("Error request.")

    def __stop_fail_on(self, node, fail_type):
        if fail_type == 1:
            return self.__stub[node].GetRequest.future(
                YTestRequest(
                    type=1,
                    cofr=CPUOverloadFailRequest(type=2)))
        elif fail_type == 2:
            return self.__stub[node].GetRequest.future(
                YTestRequest(
                    type=2,
                    dffr=DamageFileFailRequest(type=2)))
        elif fail_type == 3:
            return self.__stub[node].GetRequest.future(
                YTestRequest(
                    type=3,
                    dnpfr=DisturbNetworkPackageFailRequest(type=2)))
        elif fail_type == 4:
            return self.__stub[node].GetRequest.future(
                YTestRequest(
                    type=4,
                    dpfr=DisturbProcessFailRequest(type=2)))
        else:
            print("Error request.")

    def start(self):
        if len(argv) > 1:
            self.autofail(argv[1])
        else:
            self.repl()

    def autofail(self, fail_name):
        sleep(40)

    def repl(self):
        while True:
            cmd = input('''
            Command:
            q: exit
            s: send rpc
            o: output logs
            p: print rpc result
            d: stop and delete fail
            ''')
            if cmd == 'q':
                break
            elif cmd == 's':
                node_list = input('''
                input one or more node number, split by ' '
                0: master
                1: segment1
                2: segment2
                3: segment3
                ''').split(' ')
                fail = int(input('''
                input one fail type name
                0: noop rpc, do nothing just test if rpc is ok
                1: overload cpu, useless process take nearly 100% cpu
                2: damage file. corrupt file, delete file, etc
                3: disturb network package. loss, reorder, etc
                4: disturb process. randomly kill, hung, renice process, etc
                '''))
                if fail == 1:
                    ratio = float(input('''
                    input how frequent overload cpu, a float number in [0,1]
                    '''))
                    timeslice = float(input('''
                    input how long do u want overload last, a positive float 
                    or int number
                    '''))
                    for n in node_list:
                        print('u are sending a cpu fail rpc to node:%s' % n)
                        print('Response index:%d' % len(self.__response_list))
                        self.__response_list.append(
                            self.__start_fail_on(int(n), 1, ratio, timeslice))
                elif fail == 0:
                    for n in node_list:
                        print('u are sending a noop rpc to node:%s' % n)
                        print('Response index:%d' % len(self.__response_list))
                        self.__response_list.append(
                            self.__start_fail_on(int(n), 0))
                elif fail == 2:
                    td = input('''
                    input the target directory u want to damage.
                    for example:
                        /home/gpadmin/gpdata
                    dont use this rpc in productive envirnment.
                    ''')
                    args = input('''
                    input four float args 'a b c d' read readme for why.
                    ''').split()
                    for n in node_list:
                        print('u are sending a damage file rpc to node:%s' % n)
                        print('Response index:%d' % len(self.__response_list))
                        self.__response_list.append(
                            self.__start_fail_on(
                                int(n), 
                                2, 
                                float(args[0]),
                                float(args[1]),
                                float(args[2]),
                                float(args[3])))
                elif fail == 3:
                    td = input('''
                    input target network device.
                    it should be 'eth0' if u r using docker.
                    ''')
                    for n in node_list:
                        print('u are sending a disturb network rpc to node:%s' % n)
                        print('Response index:%d' % len(self.__response_list))
                        self.__response_list.append(
                            self.__start_fail_on(
                                int(n),
                                3,
                                td))
                elif fail == 4:
                    kw = input('''
                    input keywords of u want to disturb process with.
                    ''').split()
                    args = input('''
                    input four float args 'a b c d' read readme for why.
                    ''').split()
                    for n in node_list:
                        print('u are sending a disturb process rpc to node:%s' % n)
                        print('Response index:%d' % len(self.__response_list))
                        self.__response_list.append(
                            self.__start_fail_on(
                                int(n), 
                                4, 
                                float(args[0]),
                                float(args[1]),
                                float(args[2]),
                                float(args[3])))
                else:
                    print('''
                    unknown type rpc.
                    ''')
            elif cmd == 'o':
                node_list = input('''
                input one or more node number, split by ' '
                0: master
                1: segment1
                2: segment2
                3: segment3
                ''').split()
                print('''
                maybe u can wait for some time, then
                open another terminal, and run:
                    'docker cp container_id:/*.log .'
                to check logs on containe
                r
                ''')
                for n in node_list:
                    self.__start_fail_on(int(n), -1, True,True,True,True)
            elif cmd == 'p':
                index = int(input('input index of rpc response:'))
                r = self.__response_list[index].result()
                if r.type == 0:
                    print('noop response.')
                elif r.type == 1:
                    print('cpu overload fail response.')
                    if r.cofr.type == 0:
                        print('cpu overload fail response noop.')
                    elif r.cofr.type == 1:
                        print('cpu overload fail response ok.')
                    else:
                        print('cpu overload fail response error.')
                        print('error type: %d' % r.cofr.error_type)
                        print('error message: %s' % r.cofr.error_message)
                elif r.type == 2:
                    print('damage file fail response.')
                    if r.dffr.type == 0:
                        print('damage file fail response noop.')
                    elif r.dffr.type == 1:
                        print('damage file fail response ok.')
                    else:
                        print('damage file fail response error.')
                        print('error type: %d' % r.dffr.error_type)
                        print('error message: %s' % r.dffr.error_message)
                elif r.type == 3:
                    print('disturb network fail response.')
                    if r.dnpfr.type == 0:
                        print('disturb network fail response noop.')
                    elif r.dnpfr.type == 1:
                        print('disturb network fail response ok.')
                    else:
                        print('disturb network fail response error.')
                        print('error type: %d' % r.dnpfr.error_type)
                        print('error message: %s' % r.dnpfr.error_message)
                elif r.type == 4:
                    print('disturb process fail response.')
                    if r.dpfr.type == 0:
                        print('disturb process fail response noop.')
                    elif r.dpfr.type == 1:
                        print('disturb process fail response ok.')
                    else:
                        print('disturb process fail response error.')
                        print('error type: %d' % r.dpfr.error_type)
                        print('error message: %s' % r.dpfr.error_message)
                else:
                    print('error type.')
            elif cmd == 'd':
                print('delete every fail on every node.')
                for n in range(4):
                    for d in range(1,5):
                        self.__stop_fail_on(n,d)
            else:
                print('unknown cmd.')


if __name__ == '__main__':
    YTestController('config.ini').start()
