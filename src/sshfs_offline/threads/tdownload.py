
from importlib.metadata import metadata
import math
from pathlib import Path
import queue
import threading

from sshfs_offline import common, commonfunc, data, remote, metadata, metrics, log, stats
from sshfs_offline.log import logger
from sshfs_offline.remote import cnn
from sshfs_offline.remote import attr

READ_FRONT = False
READ_BACK = True

class Download:
    """
    Handles local caching of file data for gdrive-filesys.    
    """
     
    def __init__(self):               
        self.downloadQueue = queue.Queue() 
        self.activeThreadCount = 0
        self.exceptionCount = 0
        self.errorsByLocalId: dict[str, str] = dict()
    
    def read(self, path, size, offset, readEntireFile: bool, queueEnd: bool=READ_FRONT) -> bytes:
       
        d = metadata.cache.getattr(path)
        if d == None:
            d = attr.execute(path)
        localId = d.get('local_id')            
       
        try:
            def getDataCallback(size: int, offset: int) -> bytes: 
                stats.remoteStats.read += 1               
                with cnn.getConnection().sftp.open(cnn.fixPath(path), 'r') as file:
                    file.seek(offset, 0)
                    data = file.read(size)
                    file.close()
                    logger.info('fetching remote data %s offset=%d size=%d return=%d', path, offset, size, len(data))
                    return data   
            buf = data.cache.read(path, localId, offset, size, d.get('st_size', 0),
                                  getDataCallback if not d.get('local_only', False) else None)                   
        except Exception as e:            
            self.errorsByLocalId[localId] = str(e)
            raise e

        # If the entire file is being read and all blocks are not cached, queue it for background reading
        if readEntireFile:
            count = data.cache.getUnreadBlockCount(path, d.get('local_id'), d.get('st_size', 0))
            if count > 0:
                if queueEnd == READ_FRONT or count > 1:
                    self.downloadQueue.put((path, d.get('local_id'), queueEnd))            
                    metrics.counts.incr('tdownload_read_enqueue_downloadqueue')                
            else:
                metrics.counts.incr('tdownload_read_all_blocks_cached')       
       
        return bytes(buf)
    
    def start(self):
        """
        Starts the file reader thread by setting the 'stopped' flag to False and launching a new thread
        that runs the 'tdownloadThread' method.
        """         
        self.stopped = False
        for i in range(common.NUMBER_OF_FILE_READER_THREADS):
            threading.Thread(target=self.downloadThread, args=(i+1,), daemon=True).start()

    def stop(self):
        """
        Stops the data processing by setting the stopped flag to True and logging the action.
        This method is typically called to gracefully halt ongoing operations.
        """
        logger.info('tdownload.stop')
        self.stopped = True

    def enqueueDownloadQueue(self, path: str, d: dict[str, any]):
        """
        Adds a file path to the file reader queue for processing.
        This method places the specified file path into the queue, allowing
        the file reader thread to pick it up and read unread blocks from the file.
        Args:
            path (str): The file path to be added to the queue.
        """
        localId = d.get('local_id')
        count = data.cache.getUnreadBlockCount(path, localId, d.get('st_size', 0))
        if count > 0:
            self.downloadQueue.put((path, localId, READ_BACK))
        if count > 1:
            self.downloadQueue.put((path, localId, READ_FRONT))

    def downloadThread(self, number: int):
        """
        Continuously processes file paths from the downloadQueue, reading unread blocks from each file.
        For each file path retrieved from the queue:
        - Retrieves the block map indicating which blocks have been read.
        - Searches for the first unread block (where blockMap[i] == 0).
        - If an unread block is found, reads the block using the `read` method and increments the 'tdownloadThread' metric.
        - If all blocks have been read, logs that all blocks are read for the file.
        The thread runs until the `self.stopped` flag is set.
        """
        common.threadLocal.operation = 'tdownload_%d' % number
        common.threadLocal.path = None
        while not self.stopped:            
            (path, localId, queueEnd) = self.downloadQueue.get()
           
            common.threadLocal.path = (path,)
            metrics.counts.incr('tdownload_dequeued')
            metrics.counts.startExecution('tdownload_%d' % number)
            try:
                self.activeThreadCount += 1
                logger.info('--> downloadThread %s local_id=%s queueEnd=%s', path, localId, 'front' if queueEnd==READ_FRONT else 'back')
                
                d = metadata.cache.getattr_by_id(localId)
                if d == None:
                    metrics.counts.incr('tdownload_file_deleted')
                    logger.warning('tdownloadThread: file was deleted %s local_id=%s', path, localId)
                    continue
                
                localId = d.get('local_id')
                        
                offset = data.cache.findNextUncachedBlockOffset(path, localId, d.get('st_size', 0), reverse=queueEnd)
                if offset is None:
                    metrics.counts.incr('tdownload_all_blocks_cached')
                    logger.info('<-- downloadThread %s local_id=%s all blocks cached', path, localId)
                    continue

                size = common.BLOCK_SIZE                
                self.read(path, size, offset, readEntireFile=True, queueEnd=queueEnd)
                
                metrics.counts.incr('tdownload_block_read'+('_front' if queueEnd==READ_FRONT else '_back'))
                metrics.counts.incr('tdownload_bytes_read'+('_front' if queueEnd==READ_FRONT else '_back'), size)

                logger.info('<-- downloadThread %s local_id=%s size=%s offset=%s', path, localId, size, offset)
                
            except Exception as e:                
                if isinstance(e, TimeoutError):
                    metrics.counts.incr('tdownload_timeouterror')
                    logger.error(f'<-- downloadThread: TimeoutError reading file {path} local_id={localId}: {e}')
                else:
                    self.exceptionCount += 1
                    metrics.counts.incr('tdownload_exception')
                    raisedBy = commonfunc.exceptionRaisedBy(e)
                    logger.exception(f"<-- downloadThread: exception reading file {path} local_id={localId}: {raisedBy}")
            finally:
                self.activeThreadCount -= 1
                metrics.counts.endExecution('tdownload_%d' % number)
                
manager: Download = Download()