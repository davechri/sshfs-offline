
import errno
import math
from pathlib import Path
import os
import queue
import threading

from sshfs_offline import common, commonfunc, metrics, lock, stats
from sshfs_offline.log import logger
from sshfs_offline.remote import cnn
from sshfs_offline.remote.readdir import FuseOSError

class Delete:   
    def __init__(self):       
        self.stopped = False        
        self.queue = queue.Queue() 
        self.activeThreadCount = 0
        self.exceptionCount = 0  
            
    def start(self):           
        self.stopped = False
        for i in range(common.NUMBER_OF_FILE_READER_THREADS):
            threading.Thread(target=self.worker, args=(i+1,), daemon=True).start()

    def stop(self):       
        logger.info('tdelete.stop')
        self.stopped = True

    def enqueue(self, path: str, localId: str,inode: int):        
        self.queue.put((path, localId, inode))

    def worker(self, number: int):        
        common.threadLocal.operation = 'tdelete_%d' % number
        common.threadLocal.path = None
        while not self.stopped:            
            (path, localId, inode) = self.queue.get()            
           
            with lock.get(path):
                common.threadLocal.path = (path,)
                metrics.counts.incr('tdelete_dequeued')
                metrics.counts.startExecution('tdelete_%d' % number)
                try:
                    self.activeThreadCount += 1
                    logger.info('--> workerThread %s local_id=%s', path, localId)

                    if common.offline:
                        metrics.counts.incr('tdelete_offline')
                        logger.info(f'tdelete.worker {number} offline skipping delete of {path} local_id={localId}')                
                        continue

                    for timeout in commonfunc.apiTimeoutRange():
                        try: 
                            if 'dir' in localId:
                                stats.remoteStats.unlink += 1
                            else:
                                stats.remoteStats.rmdir += 1
                            stdin, stdout, stderr = cnn.getConnection().ssh.exec_command(f'rm -r "{cnn.fixPath(path)}"', timeout=common.TIMEOUT)
                            stderr = stderr.read().decode('utf-8') 
                            if 'No such file or directory' in stderr: # No such file or directory
                                logger.warning(f'tdelete not found {path}')
                                break
                            if stderr != '':
                                logger.debug(f'tdelete error {stderr}')
                                raise Exception(f'rmdir error: {stderr}')
                            break
                        except TimeoutError as e:
                            logger.error(f'rmdir timeout {e}')
                            metrics.counts.incr('tdelete_network_timeout') 
                            if commonfunc.isLastAttempt(timeout):
                                raise
                    
                    logger.info('<-- workerThread %s local_id=%s', path, localId)
                
                except Exception as e:                    
                    if isinstance(e, TimeoutError):
                        metrics.counts.incr('tdelete_timeouterror')
                        logger.error(f'<-- workerThread: TimeoutError deleting file {path} local_id={localId}: {e}')
                    else:
                        self.exceptionCount += 1
                        metrics.counts.incr('tdelete_exception')
                        raisedBy = commonfunc.exceptionRaisedBy(e)
                        logger.exception(f"<-- workerThread: exception deleting file {path} local_id={localId}: {raisedBy}")
                finally:
                    self.activeThreadCount -= 1
                    metrics.counts.endExecution('tdelete_%d' % number)
                
manager: Delete = Delete()