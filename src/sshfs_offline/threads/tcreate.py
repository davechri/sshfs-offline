
import math
from pathlib import Path
import queue
import stat
import threading

from sshfs_offline import common, commonfunc, metrics, metadata, lock
from sshfs_offline.log import logger
from sshfs_offline.remote import create, symlink, attr
from sshfs_offline.threads import tupload

class Create:   
    def __init__(self):       
        self.stopped = False        
        self.queue = queue.Queue() 
        self.activeThreadCount = 0
        self.exceptionCount = 0 
        self.pendingCreates: dict[str, bool] = {}        
            
    def start(self):           
        self.stopped = False
        for i in range(common.NUMBER_OF_FILE_READER_THREADS):
            threading.Thread(target=self.worker, args=(i+1,), daemon=True).start()

    def stop(self):       
        logger.info('tcreate.stop')
        self.stopped = True

    def enqueue(self, path: str, localId: str):        
        self.pendingCreates[localId] = True  
        self.queue.put((path, localId))

    def worker(self, number: int):        
        common.threadLocal.operation = 'create_%d' % number
        common.threadLocal.path = None
        while not self.stopped:            
            (path, localId) = self.queue.get()
           
            try:
                with lock.get(path):
                    common.threadLocal.path = (path,)
                    metrics.counts.incr('create_dequeued')
                    metrics.counts.startExecution('create_%d' % number)
                    try:
                        self.activeThreadCount += 1
                        logger.info('--> workerThread %s local_id=%s', path, localId)

                        for timeout in commonfunc.apiTimeoutRange():
                            try:                            
                                d = metadata.cache.getattr_by_id(localId)
                                if d == None:
                                    logger.error(f'create: {path} noop already deleted local_id={localId}')
                                    break

                                currentPath = attr.getPathFromStat(d) # Ensure path is set
                                if path != currentPath:
                                    logger.warning(f'create: {path} path changed to {currentPath} local_id={localId}, using updated path')
                                    path = currentPath

                                if d["st_mode"] & stat.S_IFREG == stat.S_IFREG:
                                    create.execute(path, d["st_mode"], localId, runAsync=False)
                                elif d["st_mode"] & stat.S_IFLNK == stat.S_IFLNK:
                                    target = metadata.cache.readlink(path)
                                    if target is None:
                                        logger.error(f'create: {path} symlink target is None local_id={localId}')
                                        break
                                    symlink.execute(path, localId, target, runAsync=False)
                                self.pendingCreates.pop(localId)
                                break
                            except TimeoutError as e:
                                logger.error(f'create timeout {e}')
                                metrics.counts.incr('create_network_timeout') 
                                if commonfunc.isLastAttempt(timeout):
                                    raise
                        
                        logger.info('<-- workerThread %s local_id=%s', path, localId)
                    
                    except Exception as e:                    
                        if isinstance(e, TimeoutError):
                            metrics.counts.incr('create_timeouterror')
                            logger.error(f'<-- workerThread: TimeoutError creating file {path} local_id={localId}: {e}')
                        else:
                            self.exceptionCount += 1
                            metrics.counts.incr('create_exception')
                            raisedBy = commonfunc.exceptionRaisedBy(e)
                            logger.exception(f"<-- workerThread: exception creating file {path} local_id={localId}: {raisedBy}")
                    finally:
                        self.activeThreadCount -= 1
                        metrics.counts.endExecution('create_%d' % number)

                logger.info(f'flush --> {path} local_id={localId}')
                metrics.counts.incr('execute_flush')
                size = tupload.manager.flush(path)
                logger.info(f'flush <-- {path} local_id={localId} size={size}')                
            except Exception as e:
                raisedBy = commonfunc.exceptionRaisedBy(e)
                logger.exception(f'workerThread: exception processing create for {path} local_id={localId}: {raisedBy}')            
                
manager: Create = Create()