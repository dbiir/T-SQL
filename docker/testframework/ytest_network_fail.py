from ytest_base_fail import BaseFail
from ytest_log import YTestLog


# todo: 1. first, use tc class to filter the package through the target port.
#       2. second, use iptables to create network partition.
#       3. then, directly catch, filter, and reroute network package instead
#       of using linux tc.
class DisturbNetworkPackageFail(BaseFail):
    def __init__(self, node_name, ytest_log):
        ytest_log.create_table('DisturbNetworkPackageFail')
        super().__init__(
            node_name,
            'YTest_DisturbNetworkPackageFail',
            ytest_log,
            'DisturbNetworkPackageFail',
            self.__run,
            tuple())
        self.__target_network_device = None

    def set_target_network_device(self, target_network_device):
        self.write_warning_message(
            'Set target network device to %s' % target_network_device)
        self.__target_network_device = target_network_device
        self.write_info_message(
            'Now target network device is %s' % self.__target_network_device)

        return self

    def __reset_network(self):
        self.write_info_message('Resetting target network device...')
        if self.exec(
            'tc qdisc del dev %s root netem' % self.__target_network_device):
            self.write_info_message('Network device reset done.')
        else:
            self.write_error_message('Network device reset fail.')

    def __run(self):
        self.write_info_message('Start disturb network package...')

        self.write_info_message(
            'Target network device: %s' % self.__target_network_device)

        while True:
            if self.have_done:
                self.write_info_message(
                    'Disturb network package work done. Exiting..')
                break

            base_delay = self.get_random(0, 1000)
            float_delay = self.get_random(100, 1000)
            delay_prob = self.get_random(80, 100)
            loss_prob = self.get_random(10, 50)
            duplicate_prob = self.get_random(10, 50)
            corrupt_prob = self.get_random(10, 50)
            reorder_prob = self.get_random(10, 50)

            self.write_info_message('base_delay: %dms' % base_delay)
            self.write_info_message('float_delay: %dms' % float_delay)
            self.write_info_message('delay_prob: %d%%' % delay_prob)
            self.write_info_message('loss_prob: %d%%' % loss_prob)
            self.write_info_message('duplicate_prob: %d%%' % duplicate_prob)
            self.write_info_message('corrupt_prob: %d%%' % corrupt_prob)
            self.write_info_message('reorder_prob: %d%%' % reorder_prob)

            if self.exec(
                'tc qdisc add dev %s root netem delay %dms %dms %d%% loss %d%%'
                ' duplicate %d%% corrupt %d%% reorder %d%%' %
                (self.__target_network_device,
                 base_delay,
                 float_delay,
                 delay_prob,
                 loss_prob,
                 duplicate_prob,
                 corrupt_prob,
                 reorder_prob)):
                self.write_info_message(
                    'Disturb network package successfully.')
            else:
                self.write_error_message('Disturb network package fail.')

            self.sleep(2)

            self.write_info_message('Now recover target device...')
            self.__reset_network()
            self.write_info_message('Recover target device done.')

            self.write_info_message('Disturb network package done once.')


if __name__ == '__main__':
    '''test and example'''
    y = YTestLog('DisturbNetworkPackageFailTest')
    DisturbNetworkPackageFail('test_node', y) \
        .set_target_network_device('lo') \
        .start() \
        .sleep(60) \
        .stop()
    y.save_log(['DisturbNetworkPackageFail']).terminate()
