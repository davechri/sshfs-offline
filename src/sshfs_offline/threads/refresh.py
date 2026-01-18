
import threading
import time

from fuse import FuseOSError

from sshfs_offline import common, eventq, commonfunc, metrics
from sshfs_offline.log import logger
from sshfs_offline.threads import tcreate, tdelete,refreshcache
from sshfs_offline import log

class Refresh:
    def __init__(self):       
        self.event: threading.Event
        self.refreshRunning: bool = False
        self.exceptionCount: int = 0
        
    def start(self):
        threading.Thread(target=self.refreshThread, daemon=True).start() 

    def trigger(self):
        """Triggers the refresh thread to start the refresh process by setting the event flag."""       
        self.event.set()       
     
    def refreshThread(self):
        common.threadLocal.operation = 'refresh'
        common.threadLocal.path = None
        metrics.counts.incr('refresh_thread_started')
        self.event = threading.Event()   

        lastRefreshTime = 0
        while True:
            try:
                metrics.counts.incr('refresh_wait')                
                self.event.wait()              
                metrics.counts.incr('refresh_start')
                metrics.counts.startExecution('refresh')

                eventq.queue.executeEvents() # Execute any pending events first
                 
                # Update cached data at least every common.updateinterval seconds
                elapsed = time.time() - lastRefreshTime
                if elapsed > common.updateinterval:  
                    metrics.counts.incr('refresh', int(elapsed))
                                            
                    lastRefreshTime =  time.time() + common.updateinterval
                
                    if tdelete.manager.activeThreadCount > 0 or tdelete.manager.queue.qsize() > 0 or tcreate.manager.activeThreadCount > 0 or tcreate.manager.queue.qsize() > 0:
                        metrics.counts.incr('refresh_delay_for_gddelete')
                        logger.info('refresh: delaying refresh due to active gddelete operations')
                    else:
                        logger.info('--> refresh: refreshing all files and directories')                
                        self.refreshRunning = True                                
                        refreshcache.refreshAll() # Refresh all files                
                        metrics.counts.incr('refresh_complete')
                        logger.info('<-- refresh: refresh complete')
            except Exception as e:
                if isinstance(e, TimeoutError):
                    logger.error(f"refresh TimeoutError in refreshThread: {e}")
                    metrics.counts.incr('refresh_timeout_error') 
                elif isinstance(e, FuseOSError):
                    logger.error(f"refresh FuseOSError in refreshThread: {e}")
                    metrics.counts.incr('refresh_fuse_error') 
                else:
                    self.exceptionCount += 1
                    metrics.counts.incr('refresh_exception')
                    raisedBy = commonfunc.exceptionRaisedBy(e)
                    logger.exception(f'refreshThread: {raisedBy}')
                    logger.info('<-- refresh: refresh failed')
            finally:
                self.refreshRunning = False
                metrics.counts.endExecution('refresh')
                self.event.clear()

thread: Refresh = Refresh()