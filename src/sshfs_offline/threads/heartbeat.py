from multiprocessing import connection
import threading
import time

from sshfs_offline import common, metrics, commonfunc
from sshfs_offline.log import logger
from sshfs_offline.threads import refresh
from sshfs_offline.remote import cnn

class Heartbeat:
    def __init__(self):
        self.started = False   
        self.stopped = False  

    def start(self):
        if self.started == False:
            logger.info('heartbeat_start')
            metrics.counts.incr('heartbeat_start')
            self.started = True
            threading.Thread(target=self.heartbeatThread, daemon=True).start()
 
    def heartbeatThread(self):   
        common.threadLocal.operation = 'heartbeat' 
        common.threadLocal.path = None          
        metrics.counts.incr('heartbeat_heartbeatThread')
        while True:
            if self.stopped:
                metrics.counts.incr('heartbeat_stopped')
                break
            time.sleep(10)
            try:
                metrics.counts.startExecution('heartbeat')
                connection = cnn.getConnection()   
                if connection == None:
                    metrics.counts.incr('heartbeat_no_connection')
                    continue
                                                    
                connection.ssh.exec_command('echo heartbeat', timeout=common.TIMEOUT)                
                refresh.thread.trigger()                               
            except Exception as e:
                raisedBy = commonfunc.exceptionRaisedBy(e) 
                logger.error(f'heartbeat exception: {raisedBy}')
                if common.offline == False:
                    common.offline = True
                    metrics.counts.incr('heartbeat_offline')
            else:                
                if common.offline:
                    metrics.counts.incr('heartbeat_online')
                    common.offline = False 
            finally:
                metrics.counts.endExecution('heartbeat')       

    def stop(self):
        logger.info('heartbeat_stop')
        metrics.counts.incr('heartbeat_stop')
        self.stopped = True

monitor: Heartbeat = Heartbeat()