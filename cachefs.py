#!/usr/bin/env python

import errno
from logging import getLogger
import os
from pathlib import Path

import getpass
import sys

import sftp
from sftp import fixPath

from fuse import FUSE, FuseOSError, Operations

import data
import metadata

class Main(Operations):
    '''
    A simple SFTP filesystem. Requires paramiko: http://www.lag.net/paramiko/

    You need to be able to login o remote host without entering a password.
    '''

    HOME_DIR = str(Path.home())
    CACHE_TIMEOUT = 5 * 60
                        
    def __init__(self, args):        
        host = args.host
        user = args.user
        remotedir = args.remotedir
        port = args.port
               
        self.log = getLogger('main')
        
        sftp.manager = sftp.SFTPManager(host, user, remotedir, port) 
        metadata.cache = metadata.Metadata(host, remotedir, args.cachetimeout)
        data.cache = data.Data(host, remotedir)

        sftp.manager.sftp() # verify connection to host
         
    def chmod(self, path, mode):          
        metadata.cache.deleteMetadata(path)
        return sftp.manager.sftp().chmod(fixPath(path), mode)

    def chown(self, path, uid, gid):
        metadata.cache.deleteMetadata(path)
        return sftp.manager.sftp().chown(fixPath(path), uid, gid)    
    
    def create(self, path, mode):        
        metadata.cache.deleteMetadata(path)
        metadata.cache.deleteParentMetadata(path)
        f = sftp.manager.sftp().open(fixPath(path), 'w')
        f.chmod(mode)
        f.close()
        return 0

    def destroy(self, path):        
        sftp.manager.sftp().close()
        self.client.close()

    def getattr(self, path, fh=None):
        self.log.debug('getattr: %s %s', path, fh)
        d = metadata.cache.getattr(path, None)
        if d != None:
            if d == {}:
                raise FuseOSError(errno.ENOENT)
            else:
                self.log.debug('<- getattr: %s %s', path, d)
                return d # cache hit
        
        try:
            st = sftp.manager.sftp().lstat(fixPath(path))            
        except IOError as e: 
            metadata.cache.getattr(path, {}) # negative cache entry          
            raise FuseOSError(errno.ENOENT)

        d = dict((key, getattr(st, key)) for key in (
            'st_atime', 'st_gid', 'st_mode', 'st_mtime', 'st_size', 'st_uid'))
        metadata.cache.getattr(path, d)
        self.log.debug('<- getattr: %s %s', path, d)
        return d
    
    def statfs(self, path):       
        stv = data.cache.statvfs(path)    
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def mkdir(self, path, mode):       
        metadata.cache.deleteMetadata(path)
        metadata.cache.deleteParentMetadata(path)
        return sftp.manager.sftp().mkdir(fixPath(path), mode)

    def read(self, path, size, offset, fh):  
        self.log.debug('read: %s input: size=%d offset=%d fd=%d', path, size, offset, fh)

        return data.cache.read(path, size, offset, fh)

        
    def readdir(self, path, fh):
        self.log.debug('readdir: %s %s', path, fh)
        s = metadata.cache.readdir(path)
        if s != None:
            self.log.debug('<- readdir: %s %s', path, s)
            return s
        s = ['.', '..'] + [name
                              for name in sftp.manager.sftp().listdir(fixPath(path))]
        metadata.cache.readdir(path, s)
        self.log.debug('<- readdir: %s %s', path, s)
        s = metadata.cache.readdir(path)
        return s

    def readlink(self, path):
        link = metadata.cache.readlink(path)
        if link != None:
            return link
        else:
            link = sftp.manager.sftp().readlink(fixPath(path))
            return metadata.cache.readlink(path, link)

    def rename(self, old, new):        
        metadata.cache.deleteMetadata(old)
        return sftp.manager.sftp().rename(fixPath(old), fixPath(new))

    def rmdir(self, path):       
        metadata.cache.deleteMetadata(path)
        metadata.cache.deleteParentMetadata(path)
        return sftp.manager.sftp().rmdir(fixPath(path))

    def symlink(self, target, source):       
        return sftp.manager.sftp().symlink(fixPath(source), fixPath(target))

    def truncate(self, path, length, fh=None):        
        metadata.cache.deleteMetadata(path)
        data.cache.removeStaleBlocks(path)
        return sftp.manager.sftp().truncate(fixPath(path), length)

    def unlink(self, path):        
        metadata.cache.deleteMetadata(path)
        metadata.cache.deleteParentMetadata(path)
        data.cache.removeStaleBlocks(path)
        return sftp.manager.sftp().unlink(fixPath(path))

    def utimens(self, path, times=None):
        metadata.cache.deleteMetadata(path)
        data.cache.removeStaleBlocks(path)
        return sftp.manager.sftp().utime(fixPath(path), times)

    def write(self, path, buf, offset, fh):        
        self.log.debug('write: %s %d', path, offset)
        metadata.cache.deleteMetadata(path)  
        data.cache.removeStaleBlocks(path)
        self.log.debug('write: write to remote file %s %d', path, offset)
        with sftp.manager.sftp().open(fixPath(path), 'r+') as file:
            file.seek(offset, 0)
            file.write(buf)
            file.close()

        return len(buf)
        
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()  
    parser.description = 'To unmount use: fusermount -u mountpoint'
    parser.add_argument('host', help='remote host name')
    parser.add_argument('mountpoint', help='local mount point (eg, ~/mnt)')
    parser.add_argument('-p', '--port', help='port number (default=22)', default=22)
    parser.add_argument('-u', '--user', help='user on remote host', default=getpass.getuser())
    parser.add_argument('-d', '--remotedir', help='directory on remote host (eg, ~/)', default=Main.HOME_DIR)
    parser.add_argument('--debug', help='run in debug mode', action='store_true')
    parser.add_argument('--cachetimeout', type=int, help='duration in seconds to keep metadata cached (default is 5 minutes)', default=Main.CACHE_TIMEOUT)

    args = parser.parse_args()

    import logging
    level = logging.WARNING
    if args.debug:
        level = logging.DEBUG
        logging.getLogger("fuse").setLevel(logging.WARNING)    
        logging.getLogger("paramiko").setLevel(logging.WARNING)   

    logging.basicConfig(
        format='%(asctime)s:%(levelname)s:%(name)s %(message)s',
        datefmt='%H:%M:%S',
        level=level
    )     

    main = Main(args)

    #print(args.host, args.login)
    #exit()
    #breakpoint()
    fuse = FUSE(
        main,
        args.mountpoint,
        foreground=args.debug,
        nothreads=False,
        allow_other=True,
    )