#!/usr/bin/env python

import getpass

from sshfs_offline import fuseops
from sshfs_offline import log

CACHE_TIMEOUT = 5 * 60

def main():
    import argparse
    parser = argparse.ArgumentParser()  
    parser.description = 'To unmount use: fusermount -u mountpoint'
    parser.add_argument('host', help='remote host name')
    parser.add_argument('mountpoint', help='local mount point (eg, ~/mnt)')
    parser.add_argument('-p', '--port', help='port number (default=22)', default=22)
    parser.add_argument('-u', '--user', help='user on remote host', default=getpass.getuser())
    parser.add_argument('-d', '--remotedir', help='directory on remote host (eg, ~/)')
    parser.add_argument('--debug', help='run in debug mode', action='store_true')
    parser.add_argument('--cachetimeout', type=int, help='duration in seconds to keep metadata cached (default is 5 minutes)', default=CACHE_TIMEOUT)

    args = parser.parse_args()   

    log.Log().setupConfig(debug=args.debug)            
    
    fuseops.FuseOps(args)     

if __name__ == '__main__':
    main()