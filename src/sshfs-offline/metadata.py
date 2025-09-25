
import math
from pathlib import Path
import os
import log

import json
from logging import getLogger

import shutil
import time

from errno import ENOENT

import data
import metrics
import sftp

from fuse import FuseOSError

class Metadata:
    '''
    Metadata cache for getattr, readdir and read link operations.
    '''
    METADATA_DIR = os.path.join(Path.home(), '.sshfs-offline', 'metadata')
    GETATTR = 'getattr'
    READDIR = 'readdir'
    READLINK = 'readlink'
    BLOCKMAP = 'blockmap'
    
    def __init__(self, host: str, basedir: str, cachetimeout: float):
        self.log = getLogger(log.METADATA)

        self.cachetimeout = cachetimeout
        
        self.metadataDir = os.path.join(Metadata.METADATA_DIR, host, os.path.splitroot(basedir)[-1])
        if not os.path.exists(self.metadataDir):
            os.makedirs(self.metadataDir)

    def deleteMetadata(self, path, files=[GETATTR, READDIR, READLINK]):
        if not sftp.manager.isConnected():
            return
        
        mdPath = self._metadataPath(path)
        if os.path.exists(mdPath):
            for file in files:
                filePath = os.path.join(mdPath, file)
                if os.path.exists(filePath):
                    metrics.counts.incr('deleteMetadata')
                    os.unlink(filePath)
        
    def deleteParentMetadata(self, path):
        if not sftp.manager.isConnected():
            return
        
        p = os.path.split(path)[0]
        self.deleteMetadata(p)
        
    # 'st_atime', 'st_gid', 'st_mode', 'st_mtime', 'st_size', 'st_uid'    
    def getattr(self, path)-> dict:
        return self._readCache(path, Metadata.GETATTR)
        
    def getattr_save(self, path, dic: dict):        
        if dic == {}:
            data.cache.deleteStaleFile(path)            
            self._storeCache(path, Metadata.GETATTR, dic)
        elif dic != None:           
            data.cache.deleteStaleFile(path, dic['st_mtime']) 
            self._storeCache(path, Metadata.GETATTR, dic)
       
    def readdir(self, path)-> list[str]:       
        return self._readCache(path, Metadata.READDIR)
        
    def readdir_save(self, path, s: list[str]=None):
        self._storeCache(path, Metadata.READDIR, s)        
                
    def readlink(self, path:str) -> str | None:        
        return self._readCache(path, Metadata.READLINK)
           
    def readlink_save(self, path:str, link: str=None):        
        self._storeCache(path, Metadata.READLINK, link)        
    
    def blockmap(self, path:str) -> bytearray | None:                   
        bm = self._readCache(path, Metadata.BLOCKMAP)
        metrics.counts.incr('blockmap')
        # not sure how the blockMap can be zero length?
        if bm != None and len(bm) == 0:
            bm = None
            self.log.warning('blockmap: %s zero length blockMap?', path)

        if bm == None:
            fileSize = 0
            st = self.getattr(path)
            if st != None:
                fileSize = st['st_size']               
            else:
                fileSize = sftp.manager.sftp().lstat(sftp.fixPath(path)).st_size
                
            bm = bytearray(math.ceil(fileSize/data.cache.BLOCK_SIZE))            
        
        return bm    
    
    def blockmap_save(self, path:str, blockMap: bytearray):
        metrics.counts.incr('blockmap_save')
        self._storeCache(path, Metadata.BLOCKMAP, blockMap)        
    
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
                   
    def _storeCache(self, path, operation, d: dict | list[str] | str | bytearray):  
        self.log.debug('_storeCace.%s: %s', operation, path)     
        if not sftp.manager.isConnected():
            return
         
        p = self._metadataPath(path, operation)
        if operation == Metadata.BLOCKMAP:            
            with open(p, "wb") as file:
                file.write(bytes(d))
        else:
            with open(p, "w") as file:
                if d == ENOENT:
                    json.dump(d, None)
                else:
                    json.dump(d, file, indent=4)

    def _readCache(self, path, operation) -> dict | list[str] | str | bytearray:       
        metadataPath = self._metadataPath(path, operation)        
        if os.path.exists(metadataPath):
            if operation == Metadata.BLOCKMAP:
                with open(metadataPath, 'rb') as file:
                    buf = file.read()
                    self.log.debug('_readCache.%s: %s %s', operation, path, str(buf))   
                    metrics.counts.incr('blockmap_hit')
                    return bytearray(buf)
            else:
                if time.time() > os.lstat(metadataPath).st_ctime + self.cachetimeout and sftp.manager.isConnected():            
                    self.log.debug('_readCache.%s: expired %s', operation, path)
                    os.unlink(metadataPath)
                    metrics.counts.incr(operation+'_expired')    
                    return None
                else:                
                    with open(metadataPath, 'r') as file:                    
                        d = json.load(file)
                        logMd = ''  
                        if operation != Metadata.READDIR:
                            logMd = d
                        self.log.debug('_readCache.%s: %s %s', operation, path, logMd)  
                        metrics.counts.incr(operation+'_hit')                             
                        return d
                    
        self.log.debug('readCache.%s: not found %s', operation, path)
        return None
          

cache: Metadata = None