#!/usr/bin/env python

import getpass
import os
from pathlib import Path
import subprocess

from sshfs_offline import filesystem
from sshfs_offline import log
from sshfs_offline import common
from sshfs_offline.threads import rpc

CACHE_TIMEOUT = 5 * 60

def main():
    common.threadLocal.operation = 'cli'
    common.threadLocal.path = None
    import argparse
    import argparse
    parser = argparse.ArgumentParser(description='Access remote filesystem using SSH.')    
    subparsers = parser.add_subparsers(dest='operation', help='Available operations')

    # mount
    mount_parser = subparsers.add_parser('mount', help='Mount filesystem')
    mount_parser.add_argument('mountpoint', help='local mount point (eg, ~/mnt)')   
    mount_parser.add_argument('--host', help='hostname or IP address of remote host')
    mount_parser.add_argument('-p', '--port', help='port number (default=22)', default=22)
    mount_parser.add_argument('-u', '--user', help='user on remote host', default=getpass.getuser())
    mount_parser.add_argument('-d', '--remotedir', help='directory on remote host (eg, ~/)')
    mount_parser.add_argument('--debug', help='run in debug mode', action='store_true')
    mount_parser.add_argument('--verbose', help='run in verbose debug mode', action='store_true')
    mount_parser.add_argument('--pathfilter', type=str, help='path to filter for logging', default=None)
    mount_parser.add_argument('--updateinterval', type=int, help=f'Update interval in seconds for updating remote filesystem and refreshing cached data (default is {common.UPDATE_INTERVAL})', default=common.UPDATE_INTERVAL)
    mount_parser.add_argument('--clearcache', help='clear the cache', action='store_true')
    mount_parser    
    # unmount
    unmount_parser = subparsers.add_parser('unmount', help='Unmount filesystem')
    unmount_parser.add_argument('mountpoint', help='local mount point (eg, ~/mnt)')
    unmount_parser.add_argument('--host', help='hostname or IP address of remote host')

    # rpc operations
    status_parser = subparsers.add_parser('status', help='Dump status every 10 seconds') 
    status_parser.add_argument('--host', help='hostname or IP address of remote host') 
    directories_parser = subparsers.add_parser('directories', help='Dump directories')
    directories_parser.add_argument('--host', help='hostname or IP address of remote host') 
    event_parser = subparsers.add_parser('eventqueue', help='Dump event queue')
    event_parser.add_argument('--host', help='hostname or IP address of remote host')
    metadata_parser = subparsers.add_parser('metadata', help='Dump metadata')    
    metadata_parser.add_argument('--host', help='hostname or IP address of remote host')
    unread_parser = subparsers.add_parser('unread', help='Dump unread data blocks')
    unread_parser.add_argument('--host', help='hostname or IP address of remote host')

    args = parser.parse_args()
    if args.host == None:
        print('Error: --host is required')
        exit(1)

    common.dataDir = os.path.join(Path.home(), '.sshfs-offline', args.host)
    if not os.path.exists(common.dataDir):
        os.mkdir(common.dataDir)    

    if args.operation == None:
        parser.print_help()
        exit(1)

    if args.operation == 'unmount':
        rc = subprocess.run(['fusermount', '-u', args.mountpoint]).returncode
        if rc == 0:
            print('unmount successful')
        exit(rc)     

    if args.operation == 'mount':
        common.host = args.host
        common.remotedir = args.remotedir        
        common.debug = args.debug 
        common.verbose = args.verbose 
        common.pathfilter = args.pathfilter
        common.updateinterval = args.updateinterval
        common.mountpoint = args.mountpoint
              
        log.Log().setupConfig(debug=args.debug, verbose=args.verbose) 
        filesystem.FuseOps(args)
    else:
        try:  
            pass     
            if args.operation == 'unread':
                rpc.client.unread()
            elif args.operation == 'directories':
                rpc.client.directories()
            elif args.operation == 'eventqueue':
                rpc.client.eventqueue()
            elif args.operation == 'metadata':
                rpc.client.metadata()
            elif args.operation == 'directories':
                rpc.client.directories()
            elif args.operation == 'status':
                rpc.client.status()
        except Exception as e:
            print('RPC connection failed.')
            print('Make sure that your filesystem is mounted.')   

if __name__ == '__main__':
    main()