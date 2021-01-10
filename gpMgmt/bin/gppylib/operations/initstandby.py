#!/usr/bin/env python

import os
import base64
import pickle

from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.mainUtils import addStandardLoggingAndHelpOptions
from collections import defaultdict
from gppylib import gplog
from gppylib.commands import unix
from gppylib.commands.base import REMOTE, WorkerPool, Command
from gppylib.mainUtils import ExceptionNoStackTraceNeeded
from gppylib.operations import Operation

PG_HBA_BACKUP = 'pg_hba.conf.bak'

logger = gplog.get_default_logger()

DEFAULT_BATCH_SIZE = 16

def get_standby_pg_hba_info(standby_host, is_hba_hostnames=False):
    standby_ips = unix.InterfaceAddrs.remote('get standby ips', standby_host)
    current_user = unix.UserId.local('get userid')
    new_section = ['# standby master host ip addresses\n']
    for ip in standby_ips:
        if not is_hba_hostnames:
            cidr_suffix = '/128' if ':' in ip else '/32' # MPP-15889
            address = ip + cidr_suffix
        else:
            address = standby_host
        new_section.append('host\tall\t%s\t%s\ttrust\n' % (current_user, address))
    standby_pg_hba_info = ''.join(new_section)
    return standby_pg_hba_info 

def cleanup_pg_hba_backup(data_dirs_list):
    """
    cleanup the backup of pg_hba.config created on segments
    """
    try:
        for data_dir in data_dirs_list:
            backup_file = os.path.join(data_dir, PG_HBA_BACKUP)
            logger.info('Removing pg_hba.conf backup file %s' % backup_file)
            if os.path.exists(backup_file):
                os.remove(backup_file)
    except Exception, ex:
        logger.error('Unable to cleanup backup of pg_hba.conf %s' % ex)

def cleanup_pg_hba_backup_on_segment(gparr):
    """
    Cleanup the pg_hba.conf on all of the segments
    present in the array
    """
    logger.debug('Removing pg_hba.conf backup file on segments...')

    host_to_seg_map = defaultdict(list)
    for seg in gparr.getDbList():
        if not seg.isSegmentMaster() and not seg.isSegmentStandby():
            host_to_seg_map[seg.getSegmentHostName()].append(seg.getSegmentDataDirectory())

    pool = WorkerPool(numWorkers=DEFAULT_BATCH_SIZE)

    try:
        for host, data_dirs_list in host_to_seg_map.items():
            pickled_data_dirs_list = base64.urlsafe_b64encode(pickle.dumps(data_dirs_list))
            cmdStr = "$GPHOME/lib/python/gppylib/operations/initstandby.py -d %s -D" % pickled_data_dirs_list
            cmd = Command('Cleanup the pg_hba.conf backups on remote hosts', cmdStr=cmdStr , ctxt=REMOTE, remoteHost=host)
            pool.addCommand(cmd)

        pool.join()

        for item in pool.getCompletedItems():
            result = item.get_results()
            if result.rc != 0:
                logger.error('Unable to cleanup pg_hba.conf backup file %s' % str(result.stderr))
                logger.error('Please check the segment for more details')

    finally:
        pool.haltWork()
        pool.joinWorkers()
        pool = None

def restore_pg_hba(data_dirs_list):
    for data_dir in data_dirs_list:
        logger.info('Restoring pg_hba.conf for %s' % data_dir)
        os.system('mv %s/%s %s/pg_hba.conf' % (data_dir, PG_HBA_BACKUP, data_dir))

def restore_pg_hba_on_segment(gparr):
    """
    Restore the pg_hba.conf on all of the segments
    present in the array
    """
    logger.debug('Restoring pg_hba.conf file on segments...')

    host_to_seg_map = defaultdict(list)
    for seg in gparr.getDbList():
        if not seg.isSegmentMaster() and not seg.isSegmentStandby():
            host_to_seg_map[seg.getSegmentHostName()].append(seg.getSegmentDataDirectory())

    pool = WorkerPool(numWorkers=DEFAULT_BATCH_SIZE)

    try:
        for host, data_dirs_list in host_to_seg_map.items():
            pickled_data_dirs_list = base64.urlsafe_b64encode(pickle.dumps(data_dirs_list))
            cmdStr = "$GPHOME/lib/python/gppylib/operations/initstandby.py -d %s -r" % pickled_data_dirs_list
            cmd = Command('Restore the pg_hba.conf on remote hosts', cmdStr=cmdStr , ctxt=REMOTE, remoteHost=host)
            pool.addCommand(cmd)

        pool.join()

        for item in pool.getCompletedItems():
            result = item.get_results()
            if result.rc != 0:
                logger.error('Unable to restore pg_hba.conf %s' % str(result.stderr))
                logger.error('Please check the segment for more details')

    finally:
        pool.haltWork()
        pool.joinWorkers()
        pool = None


def backup_pg_hba(data_dirs_list):
    try:
        for data_dir in data_dirs_list:
            logger.info('Backing up pg_hba.conf for %s' % data_dir)
            # back it up
            os.system('cp %s/pg_hba.conf %s/%s'% (data_dir, data_dir, PG_HBA_BACKUP))
    except Exception, ex:
        raise Exception('Failed to backup pg_hba.config file %s' % ex)

def update_pg_hba(standby_pg_hba_info, data_dirs_list):
    backup_pg_hba(data_dirs_list)
    pg_hba_content_list = []
    for data_dir in data_dirs_list:
        logger.info('Updating pg_hba.conf for %s' % data_dir)
        with open('%s/pg_hba.conf' % data_dir) as fp:
            pg_hba_contents = fp.read()

        if standby_pg_hba_info in pg_hba_contents:
            continue
        else:
            pg_hba_contents += standby_pg_hba_info
          
        with open('%s/pg_hba.conf' % data_dir, 'w') as fp:
            fp.write(pg_hba_contents)

        pg_hba_content_list.append(pg_hba_contents)
    return pg_hba_content_list

def update_pg_hba_conf_on_segments(gparr, standby_host, is_hba_hostnames=False):
    """
    Updates the pg_hba.conf on all of the segments 
    present in the array
    """
    logger.debug('Updating pg_hba.conf file on segments...')
    standby_pg_hba_info = get_standby_pg_hba_info(standby_host, is_hba_hostnames)
    pickled_standby_pg_hba_info = base64.urlsafe_b64encode(pickle.dumps(standby_pg_hba_info))

    host_to_seg_map = defaultdict(list) 
    for seg in gparr.getDbList():
        if not seg.isSegmentMaster() and not seg.isSegmentStandby():
            host_to_seg_map[seg.getSegmentHostName()].append(seg.getSegmentDataDirectory())

    pool = WorkerPool(numWorkers=DEFAULT_BATCH_SIZE)

    try:
        for host, data_dirs_list in host_to_seg_map.items():
            pickled_data_dirs_list = base64.urlsafe_b64encode(pickle.dumps(data_dirs_list))
            cmdStr = "$GPHOME/lib/python/gppylib/operations/initstandby.py -p %s -d %s" % (pickled_standby_pg_hba_info, pickled_data_dirs_list)
            cmd = Command('Update the pg_hba.conf on remote hosts', cmdStr=cmdStr, ctxt=REMOTE, remoteHost=host)
            pool.addCommand(cmd)

        pool.join()

        for item in pool.getCompletedItems():
            result = item.get_results()
            if result.rc != 0:
                logger.error('Unable to update pg_hba.conf %s' % str(result.stderr))
                logger.error('Please check the segment log file for more details')

    finally:
        pool.haltWork()
        pool.joinWorkers()
        pool = None

def create_parser():
    parser = OptParser(option_class=OptChecker,
                       description='update the pg_hba.conf on all segments')

    addStandardLoggingAndHelpOptions(parser, includeNonInteractiveOption=True)
    parser.add_option('-p', '--pg-hba-info', dest='pg_hba_info', metavar='<pg_hba entries>',
                      help='Entries that get added to pg_hba.conf file')
    parser.add_option('-d', '--data-dirs', dest='data_dirs', metavar='<list of data dirs>',
                      help='A list of all data directories present on this host')
    parser.add_option('-b', '--backup', action='store_true',
                      help='Backup the pg_hba.conf file')
    parser.add_option('-r', '--restore', action='store_true',
                      help='Restore the pg_hba.conf file')
    parser.add_option('-D', '--delete', action='store_true',
                      help='Cleanup the pg_hba.conf backup file')

    return parser

if __name__ == '__main__':
    parser = create_parser()
    (options, args) = parser.parse_args()
    data_dirs = pickle.loads(base64.urlsafe_b64decode(options.data_dirs))
    if options.pg_hba_info:
        pg_hba_info = pickle.loads(base64.urlsafe_b64decode(options.pg_hba_info))
        update_pg_hba(pg_hba_info, data_dirs)
    elif options.backup:
        backup_pg_hba(data_dirs)
    elif options.restore:
        restore_pg_hba(data_dirs)
    else:
        cleanup_pg_hba_backup(data_dirs)
