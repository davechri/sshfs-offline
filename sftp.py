import errno
from logging import getLogger
import logging
import os
from pathlib import Path
import time
from typing import Iterator
import paramiko
import threading

import getpass
import socket

from fuse import FuseOSError

import metrics
import log

BLOCK_SIZE = 131072
WINDOW_SIZE = 1073741824 

def fixPath(path):
    return os.path.splitroot(path)[-1]

class Connection:
    def __init__(self, sshClient: paramiko.SSHClient, sftpClient: paramiko.SFTPClient):
        self.sshClient: paramiko.SSHClient  = sshClient
        self.sftpClient: paramiko.SFTPClient = sftpClient
        self.offline = False

class SftpOffline:
    def close(self) -> None:
        pass
    def listdir(self, path: str = ".") -> list[str]:
        raise FuseOSError(errno.ENETDOWN)
    def listdir_attr(self, path: str = ".") -> list[paramiko.SFTPAttributes]:
        raise FuseOSError(errno.ENETDOWN)
    def listdir_iter(self, path: bytes | str = ".", read_aheads: int = 50) -> Iterator[paramiko.SFTPAttributes]:
        raise FuseOSError(errno.ENETDOWN)
    def open(self, filename: bytes | str, mode: str = "r", bufsize: int = -1) -> paramiko.SFTPFile:
        raise FuseOSError(errno.ENETDOWN)
    file = open
    def remove(self, path: bytes | str) -> None:
        raise FuseOSError(errno.ENETDOWN)
    unlink = remove
    def rename(self, oldpath: bytes | str, newpath: bytes | str) -> None:
        raise FuseOSError(errno.ENETDOWN)
    def posix_rename(self, oldpath: bytes | str, newpath: bytes | str) -> None:
        raise FuseOSError(errno.ENETDOWN)
    def mkdir(self, path: bytes | str, mode: int = 511) -> None:
        raise FuseOSError(errno.ENETDOWN)
    def rmdir(self, path: bytes | str) -> None:
        raise FuseOSError(errno.ENETDOWN)        
    def stat(self, path: bytes | str) -> paramiko.SFTPAttributes:
        raise FuseOSError(errno.ENETDOWN)
    def lstat(self, path: bytes | str) -> paramiko.SFTPAttributes:
        raise FuseOSError(errno.ENETDOWN)
    def symlink(self, source: bytes | str, dest: bytes | str) -> None:
        raise FuseOSError(errno.ENETDOWN)
    def chmod(self, path: bytes | str, mode: int) -> None:
        raise FuseOSError(errno.ENETDOWN)
    def chown(self, path: bytes | str, uid: int, gid: int) -> None:
        raise FuseOSError(errno.ENETDOWN)
    def utime(self, path: bytes | str, times: tuple[float, float] | None) -> None:
        raise FuseOSError(errno.ENETDOWN)
    def truncate(self, path: bytes | str, size: int) -> None:
        raise FuseOSError(errno.ENETDOWN)
    def readlink(self, path: bytes | str) -> str | None:
        raise FuseOSError(errno.ENETDOWN)
    def normalize(self, path: bytes | str) -> str:
        raise FuseOSError(errno.ENETDOWN)
    def chdir(self, path: None | bytes | str = None) -> None:
        raise FuseOSError(errno.ENETDOWN)
    def getcwd(self) -> str | None:
        raise FuseOSError(errno.ENETDOWN)         

class SFTPManager:
    def __init__(self, host, user, remotedir, port):
        self.log = getLogger(log.SFTP)
        self.host = host
        self.user = user 
        self.password = None      
        self.remotedir = remotedir
        self.port = port 
        self.local = threading.local()
        self.connections: dict[str, Connection] = dict() 
        self.offline = False
        self.keepaliveStarted = False   
        self.keepaliveStopped = False  

    def isConnected(self):
        return not isinstance(self.sftp(), SftpOffline) and not self.offline

    def sftp(self) -> paramiko.SFTPClient | SftpOffline:                    
        threadId = threading.get_native_id()
        if self.offline:
            if threadId in self.connections:
                self.sftpClose()
            return SftpOffline()
        
        if (threadId not in self.connections or 
            not (self.connections[threadId].sshClient.get_transport().is_active() and
                 self.connections[threadId].sshClient.get_transport().is_alive())):  
            if threadId in self.connections:
                self.sftpClose()      
            sshClient = paramiko.SSHClient()
            sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            sshClient.load_system_host_keys()            
            try:
                sshClient.connect(self.host, port=self.port, username=self.user, password=self.password)
               #sshClient.get_transport().set_keepalive(60)                
            except socket.gaierror:
                self.log.debug('sftp: Cannot connect to host '+self.host)
                print('Cannot connect to host ' + self.host + '.   Only cached data will be available.')
                metrics.counts.incr('sftp_connect_err') 
                return SftpOffline()
            except OSError as e:
                self.log.debug('sftp: %s', e)
                print('{}.   Only cached data will be available.'.format(e))
                metrics.counts.incr('sftp_network_err') 
                return SftpOffline()
            except paramiko.ssh_exception.AuthenticationException:
                self.password = getpass.getpass("Enter password: ")
                try:
                    sshClient.connect(self.host, port=self.port, username=self.user, password=self.password)
                except paramiko.ssh_exception.AuthenticationException:
                    self.log.debug("sftp: Authentication failed")
                    print('Invalid user or password')
                    metrics.counts.incr('sftp_auth_err') 
                    exit(1)
            
            metrics.counts.incr('sftp_connected') 
            sshClient.get_transport().default_window_size = WINDOW_SIZE
            self.connections[threadId] = Connection(sshClient, sshClient.open_sftp())
            self.connections[threadId].sftpClient.SFTP_FILE_OBJECT_BLOCK_SIZE = BLOCK_SIZE
            try:
                self.connections[threadId].sftpClient.chdir(self.remotedir)
                metrics.counts.incr('sftp_chdir') 
            except IOError:
                self.log.debug('--remotedir '+self.remotedir+' not found on host '+self.host)
                print('--remotedir '+self.remotedir+' not found on host '+self.host)
                metrics.counts.incr('sftp_chdir_err') 
                exit(1)
                        
                           
        return self.connections[threadId].sftpClient    
    
    def sftpClose(self):
        metrics.counts.incr('sftp_close')
        threadId = threading.get_native_id()
        val = self.connections.pop(threadId)
        val.sftpClient.close()
        val.sshClient.close() 

    def startKeepalive(self):
        if self.keepaliveStarted == False:
            metrics.counts.incr('sftp_start')
            self.keepaliveStarted = True
            threading.Thread(target=self.keepaliveThread).start()

    def keepaliveThread(self):        
        metrics.counts.incr('sftp_keepaliveThread')
        while True:
            if self.keepaliveStopped:
                metrics.counts.incr('sftp_stopped')
                break
            time.sleep(10)
            sshClient = paramiko.SSHClient()
            sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            sshClient.load_system_host_keys()            
            try:
                sshClient.connect(self.host, port=self.port, username=self.user, password=self.password)                              
            except Exception as e:
                if self.offline == False:
                    self.offline = True
                    metrics.counts.incr('sftp_offline')
            else:                
                if self.offline:
                    metrics.counts.incr('sftp_online')
                    self.offline = False                    
                sshClient.close()

    def stop(self):
        self.log.info('sftp_stop')
        metrics.counts.incr('sftp_stop')
        self.keepaliveStopped = True

manager: SFTPManager = None
