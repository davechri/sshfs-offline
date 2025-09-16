from logging import getLogger
import logging
import os
from pathlib import Path
import paramiko
import threading

import getpass
import socket

import subprocess

BLOCK_SIZE = 131072
WINDOW_SIZE = 1073741824 

def fixPath(path):
    return os.path.splitroot(path)[-1]

class Connection:
    def __init__(self, sshClient: paramiko.SSHClient, sftpClient: paramiko.SFTPClient):
        self.sshClient: paramiko.SSHClient  = sshClient
        self.sftpClient: paramiko.SFTPClient = sftpClient
                

class SFTPManager:
    def __init__(self, host, user, remotedir, port):
        self.log = getLogger('sftp')
        self.host = host
        self.user = user 
        self.password = None      
        self.remotedir = remotedir
        self.port = port 
        self.local = threading.local()
        self.connections: dict[str, Connection] = dict()        
                  
    def sftp(self) -> paramiko.SFTPClient:                    
        threadId = threading.get_native_id()
        if (threadId not in self.connections or not
            self.connections[threadId].sshClient.get_transport().is_active()):        
            sshClient = paramiko.SSHClient()
            sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            sshClient.load_system_host_keys()            
            try:
                sshClient.connect(self.host, port=self.port, username=self.user, password=self.password)
            except socket.gaierror:
                self.log.debug('sftp: Cannot connect to host '+self.host)
                print('Cannot connect to host ' + self.host)
                exit(1)
            except paramiko.ssh_exception.AuthenticationException:
                self.password = getpass.getpass("Enter password: ")
                try:
                    sshClient.connect(self.host, port=self.port, username=self.user, password=self.password)
                except paramiko.ssh_exception.AuthenticationException:
                    self.log.debug("sftp: Authentication failed")
                    print('Invalid user or password')
                    exit(1)
            
            sshClient.get_transport().default_window_size = WINDOW_SIZE
            self.connections[threadId] = Connection(sshClient, sshClient.open_sftp())
            self.connections[threadId].sftpClient.SFTP_FILE_OBJECT_BLOCK_SIZE = BLOCK_SIZE
            try:
                self.connections[threadId].sftpClient.chdir(self.remotedir)
            except IOError:
                self.log.debug('--remotedir '+self.remotedir+' not found on host '+self.host)
                print('--remotedir '+self.remotedir+' not found on host '+self.host)
                exit(1)
                        
                           
        return self.connections[threadId].sftpClient    
    
    def sftpClose(self):
        threadId = threading.get_native_id()
        val = self.connections.pop(threadId)
        val.sftpClient.close()
        val.sshClient.close()    

manager: SFTPManager = None
