
import math
import os
import errno
import queue
import threading
from time import time

from fuse import FuseOSError

from sshfs_offline import common, commonfunc, data, db, gitignore, lock, metadata, metrics, stats
from sshfs_offline.log import logger
from sshfs_offline.remote import cnn

UPLOAD = 'upload'

class Upload:
    def __init__(self):
        self.uploadQueue = queue.Queue()
        self.activeThreadCount = 0 
        self.exceptionCount = 0
    
    def init(self):                     
        self.uploadDir = os.path.join(common.dataDir, 'tupload')
        os.makedirs(self.uploadDir, exist_ok=True)
        files = os.listdir(self.uploadDir)
        for f in files:
            fullPath =  os.path.join(self.uploadDir, f)
            if os.path.isfile(fullPath):
                os.remove(fullPath)
        it = db.cache.getIterator()
        for key, value in it(prefix=bytes(UPLOAD, 'utf-8')):
            key = str(key, 'utf-8')
            db.cache.delete(key, UPLOAD)
    
    def write(self, path:str, buf: bytes, offset: int) -> int:
        d = metadata.cache.getattr(path)
        if d is None:
            raise FuseOSError(errno.ENOENT)
        metrics.counts.incr(f'tupload_put_block')
        data.cache.write(path, d['local_id'], offset, buf)
        
        db.cache.put(f'{UPLOAD}:{d["local_id"]}', b'1', UPLOAD)
        
        metadata.cache.getattr_increase_size(path, offset + len(buf))        
      
        return len(buf)
    
    def isFlushPending(self, path: str, localId: str) -> bool:
        return db.cache.get(f'{UPLOAD}:{localId}', UPLOAD) is not None

    def flush(self, path: str) -> int:
        if common.offline:
            metrics.counts.incr('tupload_flush_offline')
            return 0
        
        d = metadata.cache.getattr(path)
        if d is None:          
            metrics.counts.incr(f'tupload_flush_noattr')
            return 0
        
        if d['st_ino'] == 0:
            return 0
        
        if db.cache.get(f'{UPLOAD}:{d.get("local_id")}', UPLOAD) == None:         
            metrics.counts.incr(f'tupload_flush_nopending')
            return 0

        if not data.cache.isEntireFileCached(path, d.get('local_id'), d.get('st_size')):       
            metrics.counts.incr('tupload_flush_file_is not_cached')
            return 0
        
        if gitignore.parser.isIgnored(path):
            logger.debug(f'tupload.flush: {path} is ignored by .gitignore, skipping upload')
            metrics.counts.incr('tupload_flush_ignored_by_gitignore')
            return 0
                
        localPath = None
        sentBytes = 0
        try:
            with lock.get(path):              
                flushFileToRemote = db.cache.get(f'{UPLOAD}:{d.get("local_id")}', UPLOAD) is not None
                if flushFileToRemote:
                    db.cache.delete(f'{UPLOAD}:{d.get("local_id")}', UPLOAD)       
                else:
                    metrics.counts.incr(f'tupload_flush_noop')
                    return 0

            self.enqueueUploadQueue(path, d.get('local_id'))            
        finally:            
            if localPath is not None:               
                os.remove(localPath)                
        return 0
    
    def start(self):         
        self.stopped = False
        for i in range(common.NUMBER_OF_FILE_READER_THREADS):
            threading.Thread(target=self.uploadThread, args=(i+1,), daemon=True).start()

    def stop(self):        
        logger.info('tupload.stop')
        self.stopped = True

    def enqueueUploadQueue(self, path: str, localId: str):
        self.uploadQueue.put((path, localId))

    def uploadThread(self, number: int):
        
        common.threadLocal.operation = 'tupload_%d' % number
        common.threadLocal.path = None
        while not self.stopped:            
            (path, localId) = self.uploadQueue.get()                     

            common.threadLocal.path = (path,)
            metrics.counts.incr('tupload_dequeued')
            metrics.counts.startExecution('tupload_%d' % number)
            try:
                self.activeThreadCount += 1
                logger.info('--> upload.uploadThread %s local_id=%s', path, localId)  

                if common.offline:
                    metrics.counts.incr('tupload_offline')
                    logger.info(f'tupload.uploadThread {number} offline skipping upload of {path} local_id={localId}')                   
                    continue                 

                d = metadata.cache.getattr_by_id(localId)
                if d is None:
                    logger.warning(f'tupload.uploadThread: {path} noop no attr for local_id={localId}')
                    continue

                localPath = os.path.join(self.uploadDir, f'{localId}-{time()}')
                fileSize = data.cache.copyDataToFile(path, localId, d['st_size'], localPath)
                if fileSize > d['st_size']:
                    metrics.counts.incr(f'data.cache_copy_updated_size_to_{fileSize}')
                    logger.warning(f'data.cache.copyDataToFile: {path} Cached file size {fileSize} is larger than attr file size {d["st_size"]}, updated metadata')
                    d['st_size'] = fileSize
                    metadata.cache.getattr_increase_size(path, fileSize)     

                metrics.counts.incr(f'tupload_flush_wrote_local_file_bytes', fileSize)

                stats.remoteStats.write += 1
                with open(localPath, 'rb') as f:
                    st = cnn.getConnection().sftp.putfo(f, cnn.fixPath(path), fileSize)
                    d['st_size'] = st.st_size
                    d['st_atime'] = st.st_atime
                    d['st_mtime'] = st.st_mtime
                    d['st_ctime'] = st.st_mtime
                    sentBytes = st.st_size  

                metadata.cache.getattr_save(path, d)                                  
                
                metrics.counts.incr(f'tupload_flush_network_bytes_sent', sentBytes) 
                
                if fileSize != sentBytes:
                    logger.warning('tupload.flush: %s local_id=%s local file size %d differs from sent bytes %d', path, localId, fileSize, sentBytes)
                    metrics.counts.incr(f'tupload_flush_size_mismatch')
                    raise FuseOSError(errno.EIO)
                
            except Exception as e:               
                if isinstance(e, TimeoutError):
                    metrics.counts.incr('tupload_timeouterror')
                    logger.error(f'<-- TimeoutError writing file {path} {localPath}: {e}')
                else:
                    self.exceptionCount += 1
                    metrics.counts.incr('tupload_exception')
                    raisedBy = commonfunc.exceptionRaisedBy(e)
                    logger.exception(f"<-- uploadThread: exception writing file {path} {localPath}: {raisedBy}")
            finally:
                self.activeThreadCount -= 1
                metrics.counts.endExecution('tupload_%d' % number)
                logger.info('<-- %s local_id=%s', path, localId)

manager = Upload()