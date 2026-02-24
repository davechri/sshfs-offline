import errno
import logging
import os
from pathlib import Path
import time
import paramiko
import threading

import getpass
import socket

from fuse import FuseOSError

from sshfs_offline import metrics, common
from sshfs_offline.log import logger

BLOCK_SIZE = 131072
WINDOW_SIZE = 1073741824 

def fixPath(path):
    return os.path.splitroot(path)[-1]

class Connection:
    def __init__(self, sshClient: paramiko.SSHClient, sftpClient: paramiko.SFTPClient):
        self.ssh: paramiko.SSHClient  = sshClient
        self.sftp: paramiko.SFTPClient = sftpClient

class Remote:
    def __init__(self, host, user, remotedir, port):
        self.host = host
        self.user = user 
        self.password = None      
        self.remotedir = remotedir
        self.port = port        
        self.connections: dict[str, Connection] = dict() 
        self.offline = False        

    def getConnection(self, startup=False) -> Connection:                    
        threadId = threading.get_native_id()
        if self.offline:
            if threadId in self.connections:
                self.sftpClose()
            raise FuseOSError(errno.ENETDOWN)
        
        if (threadId not in self.connections or 
            not (self.connections[threadId].ssh.get_transport().is_active() and
                 self.connections[threadId].ssh.get_transport().is_alive())):  
            if threadId in self.connections:
                self.sftpClose()      
            sshClient = paramiko.SSHClient()
            sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            sshClient.load_system_host_keys()            
            try:
                sshClient.connect(self.host, port=self.port, username=self.user, password=self.password, timeout=common.TIMEOUT)
            except socket.gaierror:
                logger.warning('sftp: Cannot connect to host '+self.host)
                print('Cannot connect to host ' + self.host + '.   Only cached data will be available.')
                metrics.counts.incr('remote_connect_err') 
                if startup:
                    return None
                raise
            except OSError as e:
                logger.warning('sftp: %s', e)
                print('{}.   Only cached data will be available.'.format(e))
                metrics.counts.incr('remote_network_err') 
                if startup:
                    return None
                raise
            except paramiko.ssh_exception.AuthenticationException:
                if startup: 
                    self.password = getpass.getpass("Enter password: ")
                    try:
                        sshClient.connect(self.host, port=self.port, username=self.user, password=self.password)
                    except paramiko.ssh_exception.AuthenticationException:
                        logger.error("sftp: Authentication failed")
                        print('Invalid user or password')
                        metrics.counts.incr('remote_auth_err') 
                        exit(1)                    
                raise
            
            metrics.counts.incr('remote_connected') 
            sshClient.get_transport().default_window_size = WINDOW_SIZE
            self.connections[threadId] = Connection(sshClient, sshClient.open_sftp())
            self.connections[threadId].sftp.remote_FILE_OBJECT_BLOCK_SIZE = BLOCK_SIZE
            try:
                self.connections[threadId].sftp.chdir(self.remotedir)
                metrics.counts.incr('remote_chdir') 
            except IOError:
                logger.error('--remotedir '+self.remotedir+' not found on host '+self.host)
                print('--remotedir '+self.remotedir+' not found on host '+self.host)
                metrics.counts.incr('remote_chdir_err') 
                exit(1)                       
                           
        return self.connections[threadId]    
    
    def sftpClose(self):
        metrics.counts.incr('remote_close')
        threadId = threading.get_native_id()
        val = self.connections.pop(threadId)
        val.sftp.close()
        val.ssh.close()
        
def getConnection(startup: bool=False) -> Connection:
    return cnn.getConnection(startup)

cnn: Remote = None
