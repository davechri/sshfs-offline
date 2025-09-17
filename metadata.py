
from pathlib import Path
import os

import json
from logging import getLogger

import shutil
import time

from errno import ENOENT

import data
import sftp

class Metadata:
    '''
    Metadata cache for getattr, readdir and read link operations.
    '''
    METADATA_DIR = os.path.join(Path.home(), '.sshfs-offline', 'metadata')
    GETATTR = 'getattr'
    READDIR = 'readdir'
    READLINK = 'readlink'
    
    def __init__(self, host: str, basedir: str, cachetimeout: float):
        self.log = getLogger('metadata')

        self.cachetimeout = cachetimeout
        
        self.metadataDir = os.path.join(Metadata.METADATA_DIR, host, os.path.splitroot(basedir)[-1])
        if not os.path.exists(self.metadataDir):
            os.makedirs(self.metadataDir)

    def deleteMetadata(self, path):
        if not sftp.manager.isConnected():
            return
        
        p = self._metadataPath(path)
        if os.path.exists(p):
            shutil.rmtree(p)
        
    def deleteParentMetadata(self, path):
        if not sftp.manager.isConnected():
            return
        
        p = os.path.split(path)[0]
        self.deleteMetadata(p)
        
    # 'st_atime', 'st_gid', 'st_mode', 'st_mtime', 'st_size', 'st_uid'    
    def getattr(self, path, dic: dict=None)-> dict:        
        if dic == {}:
            data.cache.removeStaleBlocks(path)            
            self._saveMetadata(path, Metadata.GETATTR, dic)
        elif dic != None:           
            data.cache.removeStaleBlocks(path, dic['st_mtime']) 
            self._saveMetadata(path, Metadata.GETATTR, dic)
        else:
            return self._readMetadata(path, Metadata.GETATTR)

    def readdir(self, path, s: list[str]=None)-> list[str]:
        if s != None:    
            self._saveMetadata(path, Metadata.READDIR, s)
        else:            
            return self._readMetadata(path, Metadata.READDIR)
                
    def readlink(self, path:str, link: str=None) -> str | None:
        self.log.debug('readlink: %s %s', path, link)
        if link != None:    
            self._saveMetadata(path, Metadata.READLINK, link)
        else:                
            return self._readMetadata(path, Metadata.READLINK)
        return None
    
    # 
    # Private methods:
    #

    def _metadataPath(self, path: str, operation: str=None) -> str:
        p = path.replace('/','%').replace('\\', '%')
        d = os.path.join(self.metadataDir, p)
        if not os.path.exists(d):
            os.mkdir(d)
        if operation == None:
            return d
        else:
            return os.path.join(d, operation)
                   
    def _saveMetadata(self, path, operation, d: dict | list[str] | str):  
        self.log.debug('saveMetadata: %s: %s', operation, path)     
        if not sftp.manager.isConnected():
            return
         
        p = self._metadataPath(path, operation)
        with open(p, "w") as file:
            if d == ENOENT:
                json.dump(d, None)
            else:
                json.dump(d, file, indent=4)

    def _readMetadata(self, path, operation) -> dict | list[str] | str:       
        p = self._metadataPath(path, operation)        
        if os.path.exists(p):
            if time.time() > os.lstat(p).st_ctime + self.cachetimeout:
                self.log.debug('readMetadata: %s: Metadata Expired %s', operation, path)
                os.unlink(p)
                return None
            else:                
                with open(p, 'r') as file:                    
                    d = json.load(file)  
                    self.log.debug('readMetadata: %s: %s %s', operation, path, d)                               
                    return d
        self.log.debug('readMetadata: %s: Metadata not found %s', operation, path)
        return None
          

cache: Metadata = None