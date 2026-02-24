import math

from typing import Callable
from sshfs_offline import db
from sshfs_offline import common
from sshfs_offline import metrics
from sshfs_offline.log import logger

DATA = 'data'

class Key:
    def __init__(self, localId: str, blockNumber: int, offset: int, size: int):
        self.localId = localId
        self.blockNumber = blockNumber
        self.offset = offset
        self.size = size
        
DEL = '//' # key delimiter

class Data:
    def __init__(self):
        pass

    def _key(self, id: str, offset: int, size: int) -> str: 
        blockNumber = math.floor(offset/common.BLOCK_SIZE) + 1       
        return f'{DATA}{DEL}{id}{DEL}{blockNumber:06d}{DEL}{offset:010d}{DEL}{size}'
    
    def _parseKey(self, key: bytes|str) -> Key:
        if isinstance(key, bytes):
            key = str(key, 'utf-8')
        tokens = key.split(DEL)
        return Key(tokens[1], int(tokens[2]), int(tokens[3]), int(tokens[4]))
    
    def _prefixBlockNumber(self, localId: str, offset: int):
        blockNumber = math.ceil(offset/common.BLOCK_SIZE) + 1
        return f'{DATA}{DEL}{localId}{DEL}{blockNumber:06d}{DEL}'
    
    def _prefixLocalId(self, localId: str):         
        return f'{DATA}{DEL}{localId}{DEL}'

    def isEntireFileCached(self, path: str, localId: str, fileSize: int):
        isCoherent = self._coherencyCheck(path, localId, fileSize)
        if not isCoherent:          
            metrics.counts.incr('data_entire_file_not_cached_coherency_failed')
            return False
        metrics.counts.incr('data_entire_file_coherency_ok')
        return True
    
    def getUnreadBlockCount(self, path: str, localId: str, fileSize: int) -> int:
        
        blockMap = bytearray(math.ceil(fileSize / common.BLOCK_SIZE))
        
        it = db.cache.getIterator()
        for key, _ in it(prefix=bytes(self._prefixLocalId(localId), encoding='utf-8')):
            k = self._parseKey(key)
            blockMap[k.blockNumber - 1] = 1

        count = blockMap.count(0)        
        logger.debug(f'data.cache.getUnreadBlockCount: {path} {localId} unread blocks={count} of {len(blockMap)} size={fileSize}')
        return count
    
    def getCachedFileSize(self, path: str, localId: str) -> int:
        size = 0
        it = db.cache.getIterator()
        for key, _ in it(prefix=bytes(self._prefixLocalId(localId), encoding='utf-8')):
            k = self._parseKey(key)
            size += k.size
        return size

    def findNextUncachedBlockOffset(self, path: str, localId: str, fileSize: int, reverse: bool) -> int | None:
        if fileSize == 0:
            return None
        
        blockMap = bytearray(math.ceil(fileSize / common.BLOCK_SIZE))
        
        it = db.cache.getIterator()
        for key, _ in it(prefix=bytes(self._prefixLocalId(localId), encoding='utf-8')):
            k = self._parseKey(key)
            blockMap[k.blockNumber - 1] = 1
       
        index = blockMap.find(0) if not reverse else blockMap.rfind(0)
        if index != -1:
            return (index * common.BLOCK_SIZE) # offset          
        return None 
        
    def copyDataToFile(self, path: str, localId: str, fileSize: int, localFilePath: str) -> int: 
        fileSize = 0
        with open(localFilePath, 'wb') as f:
            it = db.cache.getIterator()
            offset = 0            
            for key, value in it(prefix=bytes(self._prefixLocalId(localId), encoding='utf-8')):
                k = self._parseKey(key)
                if k.offset != offset:
                    self._coherencyCheck(path, localId, fileSize)
                    raise ValueError(f'data.cache.copyDataToFile: {path} local_id={localId} Inconsistent data offset for key={key} expected={offset} actual={k.offset}')
                if k.size != len(value):
                    self._coherencyCheck(path, localId, fileSize)
                    raise ValueError(f'data.cache.copyDataToFile: {path} local_id={localId} Inconsistent data size for key={key} expected={k.size} actual={len(value)}')
                offset += k.size
                f.write(value) 
                fileSize += len(value)
        logger.debug('data.cache.copyDataToFile: %s local_id=%s wrote %d bytes to %s', path, localId, fileSize, localFilePath)
        if fileSize < fileSize:
            self._coherencyCheck(path, localId, fileSize)
            raise ValueError(f'data.cache.copyDataToFile: {path} local_id={localId} Returned data size {fileSize} is not expected data size {fileSize}')               
        return fileSize

    def read(self, path: str, localId: str, offset: int, size: int, fileSize: int, getDataCallback: None|Callable[[int, int], bytes]) -> bytes: 
        output = bytearray()           
        it = db.cache.getIterator()
        
        blockStart = offset-offset%common.BLOCK_SIZE
        if blockStart >= fileSize:
            self._coherencyCheck(path, localId, fileSize)
            raise ValueError(f'data.cache.read: {path} local_id={localId} Read offset {offset} beyond EOF {fileSize}') 
        
        while True:
            blockSize = 0
            key = None
            k = None
            prevKey = None
            prevK = None
            for key, value in it(prefix=bytes(self._prefixBlockNumber(localId, blockStart), encoding='utf-8')):
                k = self._parseKey(key)
                key = str(key, 'utf-8')
                logger.debug(f'data.cache.read cache: {path} key={key}')
                blockSize += k.size               
                if offset + len(output) >= k.offset and offset + len(output) < k.offset + len(value):
                    if prevK != None and k.offset != prevK.offset + prevK.size:
                        raise ValueError(f'data.cache.read: {path} Missing data prevKey={prevKey} key={key}')  
                    start = offset-k.offset if offset > k.offset else 0
                    end = min(start + k.size, start + size-len(output))
                    output += value[start:end]

                    logger.debug(f'data.cache.read cache: {path} Copied {end-start} bytes from key={key} offset={offset} size={size} output_size={len(output)}')
                    if len(output) == fileSize or len(output) == size:
                        return output
                    prevKey = key
                    prevK = k
                elif k.offset >= offset + len(output):
                    return output
           
            if blockSize > 0 or getDataCallback == None or common.offline:
                # last chunk of data is before the end of the block
                if k.offset + k.size < blockStart + common.BLOCK_SIZE:
                    logger.debug(f'data.cache.read cache: {path} key={key} Finished reading {len(output)} available bytes from cached blocks at offset={blockStart} for requested offset={offset} size={size}')
                    return output           
            else: # If we did not find any data in cache, read from network
                readLen = min(common.BLOCK_SIZE, fileSize-blockStart)
                block = getDataCallback(readLen, blockStart)               
                metrics.counts.incr('data_read_network_block')
                if len(block) != readLen:
                    raise ValueError(f'data.cache.read: Unexpected block size {len(block)} expected {readLen}')
                
                blockSize = len(block)                
                
                start = offset-blockStart if offset > blockStart else 0
                end = min(start + len(block), start + size-len(output))
                output += block[start:end]

                logger.debug(f'data.cache.read network: {path} local_id={localId} Copied {end-start} {start}:{end} bytes from network block offset={blockStart} block_size={len(block)} output_size={len(output)} file_size={fileSize}')

                self.write(path, localId, blockStart, block) # cache the data block

                if len(output) == size:
                    return output
                
            if blockSize > common.BLOCK_SIZE:                
                raise ValueError(f'data.cache.read: {path} local_id={localId} Read block size {blockSize} exceeds block size {common.BLOCK_SIZE}')                
            if len(output) + offset == fileSize:               
                metrics.counts.incr('data_read_eof')
                return output
            if blockSize != common.BLOCK_SIZE:
                raise ValueError(f'data.cache.read: {path} local_id={localId} Incomplete block size {blockSize} at offset {blockStart} for file size={fileSize} before EOF')      
            if len(output) >= size:
                raise ValueError(f'data.cache.read: {path} local_id={localId} Read size {len(output)} exceeds expected size {size}')
            
            blockStart += common.BLOCK_SIZE # move to next block

    def truncate(self, localId: str, maxSize: int) -> list[bytes]:
        output: list[bytes] = []
        it = db.cache.getIterator()
        currentSize = 0
        for key, value in it(prefix=bytes(self._prefixLocalId(localId), encoding='utf-8')):
            k = self._parseKey(key)
            if k.offset == currentSize and currentSize < maxSize:
                if currentSize + len(value) > maxSize:
                    output.append(value[:(maxSize-currentSize)])  
                    currentSize = maxSize              
                else:
                    output.append(value)
                    currentSize += len(value)
            else:
                db.cache.delete(key, DATA)
        return output

    def write(self, path: str, localId: str, offset: int, data: bytes) -> None:      
        metrics.counts.incr('data_write')
        # metrics.counts.incr(f'data_write_size_{len(data)}')
        blockSize = len(data)  
        blockStart = 0
        blockEnd = min(common.BLOCK_SIZE-(offset%common.BLOCK_SIZE), blockSize)  
        blockOffset = offset    
        while True:            
            blockData = data[blockStart:blockEnd]
            self._deleteBlock(path, localId, blockOffset, len(blockData)) # delete old data block
            key = self._key(localId, blockOffset, len(blockData))
            metrics.counts.incr('data_write_block')
            logger.debug('data.cache.write: %s local_id=%s key=%s size=%d', path, localId, key, len(blockData))
            db.cache.put(key, blockData, DATA)  
            blockSize -= len(blockData)
            if blockSize == 0:
                break          
            blockStart = blockEnd
            blockEnd = min(blockStart + common.BLOCK_SIZE, blockStart + blockSize)  
            blockOffset += len(blockData)

    def deleteByID(self, path:str, id: str) -> None:
        it = db.cache.getIterator()
        for key, _ in it(prefix=bytes(self._prefixLocalId(id), encoding='utf-8')):
            db.cache.delete(key, DATA)
     
        metrics.counts.incr('data_deletebyid')

    def _deleteBlock(self, path:str, id: str, offset: int, size: int) -> None:
        it = db.cache.getIterator()
        for key, _ in it(prefix=bytes(self._prefixBlockNumber(id, offset), encoding='utf-8')):
            k = self._parseKey(key)
            if k.offset >= offset and k.offset < offset + size:
                logger.debug('data.cache.deleteBlock: %s local_id=%s key=%s size=%d', path, k.localId, key, k.size)
                db.cache.delete(key, DATA)

    def _coherencyCheck(self, path: str, id: str, fileSize: int) -> bool:  
        errors = list[str]()    
        it = db.cache.getIterator() 
        offset = 0    
        blockNumber = 0
        blockSize = 0
        prevKey = None
        for key, value in it(prefix=bytes(self._prefixLocalId(id), encoding='utf-8')):
            logger.debug(f'data.cache.coherencyCheck: checking {path} {key} file_size={fileSize} offset={offset} value_size={len(value)}')
            k = self._parseKey(key)
            if k.size != len(value):
                errors.append(f'{prevKey} {key} size {k.size} does not match data size {len(value)}')
                break
            if k.offset > offset:
                errors.append(f'{prevKey} {key} {k.offset-offset} bytes are missing at offset {offset}')
                break
            if k.offset < offset:
                errors.append(f'{prevKey} {key} {offset-k.offset} overlapping bytes at offset {offset}')
                break
            offset = k.size + k.offset
            if k.blockNumber == 0:
                errors.append(f'{prevKey} {key} blockNumber cannot be zero')
                break
        
            if k.blockNumber != blockNumber:
                if k.blockNumber - 1 != blockNumber:
                    errors.append(f'{prevKey} {key} blockNumber {k.blockNumber} is not sequential after blockNumber {blockNumber}')
                    break
                if k.offset != (k.blockNumber-1)*common.BLOCK_SIZE:
                    errors.append(f'{prevKey} {key} blockNumber {k.blockNumber} offset {k.offset} is not expected block boundary offset {(blockNumber)*common.BLOCK_SIZE}')
                    break
                if blockNumber > 0:
                    if blockSize != common.BLOCK_SIZE and offset + k.size != fileSize:
                        errors.append(f'{prevKey} {key} block size {blockSize} is not full block size {common.BLOCK_SIZE}')
                        break
                blockNumber = k.blockNumber
                blockSize = 0
            blockSize += k.size
            prevKey = key

        if len(errors) == 0 and offset < fileSize:
            errors.append(f'Total cached size {offset} does not match expected file size {fileSize}')

        if len(errors) > 0:            
            logger.error(f'data.cache.coherencyCheck: {path} {id} size={fileSize}\n {'\n'.join(errors)}')

        return len(errors) == 0

cache = Data()