from datetime import datetime
from queue import Queue, Empty
from sqlite3 import connect
from sys import stderr, stdout
from threading import Thread

INFO = '[INFO]'
WARN = '[WARN]'
ERRO = '[ERRO]'

CREATE_TABLE       = 0
WRITE_LOG          = 1
SAVE_LOG           = 2
TERMINATE_SAFELY   = 3
TERMINATE_UNSAFELY = 4


def generate_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')


class YTestLog:
    def __init__(self, db_name):
        self.__db_name = db_name
        self.__log_file_name = '%s.db' % self.__db_name
        self.__start_timestamp = generate_timestamp()
        self.__connect = None
        self.__queue = Queue()
        self.__have_done = False
        self.__waiting_time = 1
        self.__thread = Thread(target=self.__run, args=tuple())
        self.__thread.start()

    def create_table(self, table_name):
        self.__queue.put((CREATE_TABLE, table_name))

        return self

    def write_log(self, prompt, content, table_name):
        self.__queue.put((
            WRITE_LOG,
            generate_timestamp(),
            prompt,
            content,
            table_name))

        return self

    def save_log(self, table_name_list):
        self.__queue.put((SAVE_LOG, table_name_list))

        return self

    def terminate(self, safely=True):
        self.__queue.put((TERMINATE_SAFELY if safely else TERMINATE_UNSAFELY,))
        self.__thread.join()

        return self

    def __create_table(self, table_name):
        sql = '''
        CREATE TABLE %s (
        timestamp CHARACTER(50),
        prompt CHARACTER(10),
        content VARCHAR(1024)
        );
        ''' % table_name
        try:
            self.__connect.execute(sql)
        except Exception:
            stderr.write('Bad sql: %s\n' % sql)
        else:
            self.__connect.commit()

    def __write_log(self, timestamp, prompt, content, table_name):
        sql = '''
        INSERT INTO %s VALUES ( "%s", "%s", "%s" );
        ''' % (
            table_name,
            timestamp,
            INFO if not prompt else (WARN if prompt > 0 else ERRO),
            content)
        try:
            self.__connect.execute(sql)
        except Exception:
            stderr.write('Bad sql: %s\n' % sql)
        else:
            self.__connect.commit()

    def __save_log(self, table_name):
        sql = '''
        SELECT *
        FROM %s
        WHERE timestamp > "%s"
        ORDER BY timestamp;
        ''' % (table_name, self.__start_timestamp)
        try:
            res = self.__connect.execute(sql)
        except Exception:
            stderr.write('Bad sql: %s\n' % sql)
        else:
            self.__connect.commit()
            with open('/%s_%s.log' % (self.__db_name, table_name), 'a') as f:
                for row in res:
                    f.write('%s %s:%s\n' % (row[0], row[1], row[2]))

    def __run(self):
        self.__connect = connect(self.__log_file_name)

        while True:
            try:
                r = self.__queue.get(timeout=self.__waiting_time)
            except Empty:
                if self.__have_done:
                    stdout.write('Work done. Exiting...\n')
                    break
                self.__waiting_time *= 2
            else:
                stdout.write('Received message.\n')
                self.__waiting_time = 1

                wt = r[0]
                if wt == CREATE_TABLE:
                    tn = r[1]
                    self.__create_table(tn)
                elif wt == WRITE_LOG:
                    ts, pr, ct, tn = r[1], r[2], r[3], r[4]
                    self.__write_log(ts, pr, ct, tn)
                elif wt == SAVE_LOG:
                    tnl = r[1]
                    for tn in tnl:
                        self.__save_log(tn)
                elif wt == TERMINATE_SAFELY:
                    self.__have_done = True
                    stdout.write(
                        'Received end safely command. Now wait for work done '
                        'and exit...\n')
                elif wt == TERMINATE_UNSAFELY:
                    stderr.write(
                        'Received end unsafely command. Now exit '
                        'immediately...\n')
                    break
                else:
                    stderr.write(
                        'Received bad message!\nError message: %s\n' % str(r))
                    break


if __name__ == '__main__':
    '''test and example'''
    y = YTestLog('test') \
        .create_table('test_table') \
        .write_log(0, 'input simple message.', 'test_table') \
        .write_log(1, 'input warning message.', 'test_table') \
        .write_log(-1, 'input error message.', 'test_table') \
        .save_log(['test_table']) \
        .terminate()
