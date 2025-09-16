
import math
from pathlib import Path
import os

from logging import getLogger
import time

import sftp
from sftp import fixPath

from errno import ENOENT

import metadata

class Data:
    '''
    On demand data file cache.   The files are cached in 64k chunks (blocks).  Only the blocks of the file that is read by
    the user are cached.  Subsequent reads for the same data block are very fast.
    '''
    DATA_DIR = os.path.join(Path.home(), '.cachefs', 'data') 
    BLOCK_SIZE = sftp.BLOCK_SIZE  
 
    def __init__(self, host: str, basedir: str):
        self.log = getLogger('data')
            
        # make data cache directory ~/.cachefs/data
        self.dataDir = os.path.join(Data.DATA_DIR, host, os.path.splitroot(basedir)[-1])
        if not os.path.exists(self.dataDir):
            os.makedirs(self.dataDir)             
     
    def _dataPath(self, path: str) -> str:
        #p = path.replace('/','%').replace('\\', '%')
        return os.path.join(self.dataDir, path[1:]) 
      
    def statvfs(self, path: str):
        self.log.debug('statvfs: %s', path)              
        dataPath = self._dataPath(path)
        dir = os.path.dirname(dataPath)
        if os.path.exists(dir):
            for entry in os.listdir(dir):
                if entry.startswith(os.path.basename(path)+'-block'):
                    entryPath = os.path.join(dir, entry)
                    return os.statvfs(entryPath)  
        
    def removeStaleBlocks(self, path, mtime: float=None ): 
        self.log.debug('removeStaleBlocks: %s', path)    
        dataPath = self._dataPath(path)
        dir = os.path.dirname(dataPath)
        if os.path.exists(dir):
            for entry in os.listdir(dir):
                entryPath = os.path.join(dir, entry)                
                if entry.startswith(os.path.basename(path)+'-block') and \
                    (mtime == None or os.lstat(entryPath).st_ctime < mtime):
                    self.log.debug('removeStaleBlocks: delete block %s %s', path, entry) 
                    os.unlink(entryPath)

    def read(self, path, size, offset, fh):  
        self.log.debug('read: %s input: size=%d offset=%d fd=%d', path, size, offset, fh)

        buf = bytearray()

        dataPath = self._dataPath(path)
        d = os.path.dirname(dataPath)
        if not os.path.exists(d):
            os.makedirs(d)

        i = 0
        blockSlice = range(math.floor(offset / Data.BLOCK_SIZE) , math.floor((offset + size) / Data.BLOCK_SIZE)+1)
        for blockNum in blockSlice:  
            dataBlockPath = '{}-block{}'.format(dataPath, blockNum)           
            if os.path.exists(dataBlockPath):                
                self.log.debug('read: %s cached block: %d', path, blockNum)                 
                with open(dataBlockPath, 'rb') as file:
                    if len(buf) == 0:
                        file.seek(offset%Data.BLOCK_SIZE)                        
                        buf = file.read(size)                    
                    else:
                        buf += file.read(min(Data.BLOCK_SIZE, size-len(buf)))
            else:
                self.log.debug('read: %s read remote block: %d', path, blockNum) 
                if i%2 == 0:
                    with sftp.manager.sftp().open(fixPath(path), 'rb') as file:                        
                        file.seek(blockNum*Data.BLOCK_SIZE)

                        # use prefetch, if 2 blocks are needed
                        stop = 2 
                        if i+1 == len(blockSlice):                            
                            stop = 1
                        else:
                            file.prefetch(2*Data.BLOCK_SIZE)

                        for j in range(0,stop):
                            block = file.read(Data.BLOCK_SIZE) 
                            if len(buf) == 0:                               
                                buf = block[offset%Data.BLOCK_SIZE : min(Data.BLOCK_SIZE, size)]
                            else:
                                buf += block[0 : min(Data.BLOCK_SIZE, size-len(buf))]
                            
                            with open(dataBlockPath, 'wb') as file:
                                file.write(block) 
                            
                            dataBlockPath = '{}-block{}'.format(dataPath, blockNum+1)               

            i += 1
            
        return bytes(buf)        

cache: Data = None