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
               
        self.log = getLogger('main    ')
        
        main = self
        sftp.manager = sftp.SFTPManager(host, user, remotedir, port) 
        metadata.cache = metadata.Metadata(host, remotedir, args.cachetimeout)
        data.cache = data.Data(host, remotedir)

        sftp.manager.sftp() # verify connection to host
         
    def chmod(self, path, mode):  
        self.log.debug('-> chmod: %s %s', path, mode)        
        metadata.cache.deleteMetadata(path)
        sftp.manager.sftp().chmod(fixPath(path), mode)
        self.log.debug('<- chmod: %s', path) 

    def chown(self, path, uid, gid):
        self.log.debug('-> chown: %s %s %s', path, uid, gid)  
        metadata.cache.deleteMetadata(path)
        sftp.manager.sftp().chown(fixPath(path), uid, gid)  
        self.log.debug('<- chown: %s', path)   
    
    def create(self, path, mode):  
        self.log.debug('-> create: %s %s', path, mode)       
        metadata.cache.deleteMetadata(path)
        metadata.cache.deleteParentMetadata(path)
        f = sftp.manager.sftp().open(fixPath(path), 'w')
        f.chmod(mode)
        f.close()
        self.log.debug('<- create: %s', path) 
        return 0

    def destroy(self, path):  
        self.log.debug('-> destroy: %s', path)       
        sftp.manager.sftp().close()        
        self.log.debug('<- destroy: %s', path) 

    def getattr(self, path, fh=None):
        self.log.debug('-> getattr: %s', path)
        d = metadata.cache.getattr(path)
        if d != None:
            if d == {}:
                raise FuseOSError(errno.ENOENT)
            else:
                self.log.debug('<- getattr: %s', path)
                return d # cache hit
        
        try:
            st = sftp.manager.sftp().lstat(fixPath(path))            
        except IOError as e: 
            metadata.cache.getattr_save(path, {}) # negative cache entry          
            raise FuseOSError(errno.ENOENT)

        d = dict((key, getattr(st, key)) for key in (
            'st_atime', 'st_gid', 'st_mode', 'st_mtime', 'st_size', 'st_uid'))
        metadata.cache.getattr_save(path, d)
        self.log.debug('<- getattr: %s %s', path, d)
        return d
    
    def statfs(self, path): 
        self.log.debug('-> statfs: %s', path)       
        stv = data.cache.statvfs(path)        
        dic = dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))
        self.log.debug('<- statfs: %s %s', path, dic)  
        return dic

    def mkdir(self, path, mode):  
        self.log.debug('-> mkdir: %s %s', path, mode)       
        metadata.cache.deleteMetadata(path)
        metadata.cache.deleteParentMetadata(path)
        sftp.manager.sftp().mkdir(fixPath(path), mode)
        self.log.debug('<- mkdir: %s', path)

    def read(self, path, size, offset, fh):  
        self.log.debug('-> read: %s size=%d offset=%d', path, size, offset)

        buf = data.cache.read(path, size, offset, fh)

        self.log.debug('<- read: %s %d', path, len(buf))
        return buf

        
    def readdir(self, path, fh):
        self.log.debug('-> readdir: %s', path)
        s = metadata.cache.readdir(path)
        if s != None:
            self.log.debug('<- readdir: %s %d', path, len(s))
            return s
        s = ['.', '..'] + [name
                              for name in sftp.manager.sftp().listdir(fixPath(path))]
        metadata.cache.readdir_save(path, s)        
        s = metadata.cache.readdir(path)
        self.log.debug('<- readdir: %s %d', path, len(s))
        return s

    def readlink(self, path):
        self.log.debug('-> readlink: %s', path)
        link = metadata.cache.readlink(path)
        if link == None:        
            link = sftp.manager.sftp().readlink(fixPath(path))
            metadata.cache.readlink_save(path, link)
        
        self.log.debug('<- readlink: %s %s', path, link)
        return link

    def rename(self, old, new):
        self.log.debug('-> rename: %s %s', old, new)        
        metadata.cache.deleteMetadata(old)
        sftp.manager.sftp().rename(fixPath(old), fixPath(new))
        self.log.debug('<- rename: %s %s', old, new)

    def rmdir(self, path):  
        self.log.debug('-> rmdir: %s', path)     
        metadata.cache.deleteMetadata(path)
        metadata.cache.deleteParentMetadata(path)
        sftp.manager.sftp().rmdir(fixPath(path))
        self.log.debug('<- rmdir: %s', path)    

    def symlink(self, target, source): 
        self.log.debug('-> symlink: %s %s', target, source)           
        sftp.manager.sftp().symlink(fixPath(source), fixPath(target))
        self.log.debug('<- symlink: %s %s', target, source)     

    def truncate(self, path, length, fh=None):  
        self.log.debug('-> truncate: %s %d', path, length)           
        metadata.cache.deleteMetadata(path)
        data.cache.deleteStaleFile(path)
        sftp.manager.sftp().truncate(fixPath(path), length)
        self.log.debug('<- truncate: %s', path)     

    def unlink(self, path):   
        self.log.debug('-> unlink: %s', path)         
        metadata.cache.deleteMetadata(path)
        metadata.cache.deleteParentMetadata(path)
        data.cache.deleteStaleFile(path)
        sftp.manager.sftp().unlink(fixPath(path))
        self.log.debug('<- unlink: %s', path)    

    def utimens(self, path, times=None):
        self.log.debug('-> utimens: %s', path)    
        metadata.cache.deleteMetadata(path)
        data.cache.deleteStaleFile(path)
        sftp.manager.sftp().utime(fixPath(path), times)
        self.log.debug('<- utimens: %s', path)    

    def write(self, path, buf, offset, fh):        
        self.log.debug('-> write: %s %d', path, offset)
        metadata.cache.deleteMetadata(path)  
        data.cache.removeStaleBlocks(path)
        #self.log.debug('write: write to remote file %s %d', path, offset)
        with sftp.manager.sftp().open(fixPath(path), 'r+') as file:
            file.seek(offset, 0)
            file.write(buf)
            file.close()
        self.log.debug('<- write: %s %d', path, len(buf))
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

    dir = os.path.join(Path.home(), '.sshfs-offline')
    if not os.path.exists(dir):
        os.makedirs(dir)

    import logging
    level = logging.WARNING
    logFileName = None
    if args.debug:
        level = logging.DEBUG
        logging.getLogger("fuse").setLevel(logging.WARNING)    
        logging.getLogger("paramiko").setLevel(logging.WARNING)
    else:
        logFileName = os.path.join(dir, 'log.txt')

        
    logging.basicConfig(
        format='%(asctime)s:%(levelname)s:%(name)s %(message)s',
        datefmt='%H:%M:%S',
        level=level,
        filename=logFileName
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