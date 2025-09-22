
import math
from pathlib import Path
import os
import log

from logging import getLogger
import queue
import threading
import time

import metrics
import sftp
from sftp import fixPath

from errno import ENOENT

import metadata

from fuse import FuseOSError

class Data:
    '''
    On demand data file cache.   The files are cached in 64k chunks (blocks).  Only the blocks of the file that is read by
    the user are cached.  Subsequent reads for the same data block are very fast.
    '''
    DATA_DIR = os.path.join(Path.home(), '.sshfs-offline', 'data') 
    BLOCK_SIZE = sftp.BLOCK_SIZE  
 
    def __init__(self, host: str, basedir: str):
        self.log = getLogger(log.DATA)
            
        # make data cache directory ~/.sshfs-offline/data
        self.dataDir = os.path.join(Data.DATA_DIR, host, os.path.splitroot(basedir)[-1])
        if not os.path.exists(self.dataDir):
            os.makedirs(self.dataDir) 

        self.fileReaderQueue = queue.Queue()

        threading.Thread(target=self.fileReaderThread).start()        
     
    def _dataPath(self, path: str) -> str:
        #p = path.replace('/','%').replace('\\', '%')
        return os.path.join(self.dataDir, path[1:]) 
      
    def statvfs(self, path: str):
        self.log.debug('statvfs: %s', path)              
        dataPath = self._dataPath(path)
       
        if os.path.exists(dataPath):
            return os.statvfs(dataPath) 
                    
    def deleteStaleFile(self, path, mtime: float=None ): 
        self.log.debug('deleteStaleFile: %s', path)    
        if not sftp.manager.isConnected():
            return
        
        dataPath = self._dataPath(path)        
     
        if os.path.exists(dataPath) and os.path.isfile(path):                               
            if (mtime == None or os.lstat(dataPath).st_ctime < mtime):
                self.log.debug('deleteStaleFile: deleting %s', path) 
                metrics.counts.incr('deleteStaleFile')
                os.unlink(dataPath)  
                metadata.cache.deleteMetadata(path, [metadata.Metadata.BLOCKMAP])             

    def read(self, path, size, offset, fh):  
        #self.log.debug('read: %s input: size=%d offset=%d fd=%d', path, size, offset, fh)

        buf = bytearray()

        dataPath = self._dataPath(path)
        d = os.path.dirname(dataPath)
        if not os.path.exists(d):
            os.makedirs(d)
        if not os.path.exists(dataPath):
            with open(dataPath, 'wb') as file:
                fileSize = 0
                st = metadata.cache.getattr(path)
                if st != None:
                    fileSize = st['st_size']
                else:
                    fileSize = sftp.manager.sftp().lstat(fixPath(path)).st_size
                file.truncate(fileSize)       

        blockMap = metadata.cache.blockmap(path)
        blockNumSlice = range(math.floor(offset / Data.BLOCK_SIZE) , min(math.ceil((offset + size) / Data.BLOCK_SIZE), len(blockMap))) 
              
        try:
            if not 1 in blockMap[blockNumSlice[0]:blockNumSlice[-1]+1]:            
                with sftp.manager.sftp().open(fixPath(path), 'rb') as file:
                    file.seek(blockNumSlice[0]*Data.BLOCK_SIZE)
                    tempBuf = file.read(len(blockNumSlice)*Data.BLOCK_SIZE)

                with open(dataPath, 'rb+') as file:
                    file.seek(blockNumSlice[0]*Data.BLOCK_SIZE)
                    file.write(tempBuf)

                for blockNum in blockNumSlice:                    
                    blockMap[blockNum] = 1
                metadata.cache.blockmap_save(path, blockMap)

                blockOffset = offset%Data.BLOCK_SIZE
                buf = tempBuf[blockOffset:min(len(tempBuf), blockOffset+size)]

                # More unread blocks?
                if 0 in blockMap:
                    self.fileReaderQueue.put(path)
            else:                     
                for blockNum in blockNumSlice:
                    if blockMap[blockNum] == 0:
                        #self.log.debug('read: %s get block %d from remote', path, blockNum)
                        with sftp.manager.sftp().open(fixPath(path), 'rb') as file:                                       
                            file.seek(blockNum*Data.BLOCK_SIZE)
                            block = file.read(Data.BLOCK_SIZE) 
                                                
                        with open(dataPath, 'rb+') as file:
                            file.seek(blockNum*Data.BLOCK_SIZE)
                            file.write(block) 

                        blockMap[blockNum] = 1
                        metadata.cache.blockmap_save(path, blockMap)

                        if len(buf) == 0:
                            blockOffset = offset%Data.BLOCK_SIZE                            
                            buf = block[blockOffset : min(Data.BLOCK_SIZE, blockOffset+size)]
                        else:
                            buf += block[0 : min(Data.BLOCK_SIZE, size-len(buf))]
                    else:
                        with open(dataPath, 'rb') as file:
                            if len(buf) == 0:
                                file.seek(offset)                        
                                buf = file.read(min(size, Data.BLOCK_SIZE-offset%Data.BLOCK_SIZE))                    
                            else:
                                file.seek(blockNum*Data.BLOCK_SIZE)    
                                buf += file.read(min(Data.BLOCK_SIZE, size-len(buf)))                           
        except Exception as e:            
            self.log.error('read: %s size=%d offset=%d', path, size, offset)
            self.log.error('read: %s blockMap=%s', path, blockMap)
            self.log.error('read: %s blockMap=%s', path, blockNumSlice)
            raise e
       
        #self.log.debug('read: %s %d', path,= len(buf))
        return bytes(buf)  

    def fileReaderThread(self):
        while True:
            path = self.fileReaderQueue.get()
            self.log.debug('-> fileReaderThread %s', path)
            blockMap = metadata.cache.blockmap(path)  
            unreadBlockFound = False          
            for i in range(0, len(blockMap)):
                if blockMap[i] == 0: 
                    size = Data.BLOCK_SIZE
                    offset = i * Data.BLOCK_SIZE
                    self.log.debug('<- fileReaderThread %s size=%s offset=%s', path, size, offset)
                    self.read(path, size, offset, 0) 
                    unreadBlockFound = True                    
                    break

            if unreadBlockFound:
                metrics.counts.incr('fileReaderThread')
            else:
                self.log.debug('<- fileReaderThread %s all blocks read', path)

cache: Data = None