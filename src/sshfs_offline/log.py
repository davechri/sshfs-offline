    

import logging
import os
from pathlib import Path

from sshfs_offline import color, common

_SSHFS     = 'sshfs'
_SSHFS_METRICS = 'sshfs_metrics'

_FUSE        = 'fuse'
_PARAMIKO    = 'paramiko.transport'
_PARAMIKO_SFTP = 'paramiko.transport.sftp'

class ThreadId:
    def __init__(self):
        self.num = 0
        self.idToNum: dict[str,int] = dict()
    def toNum(self, id):
        if not id in self.idToNum:
            self.num = self.num + 1
            self.idToNum[id] = self.num
        return self.idToNum[id]

threadId = ThreadId()

class TidFilter(logging.Filter):
    def filter(self, record):
        record.thread = threadId.toNum(record.thread)

        if record.levelname == 'INFO':
            record.levelname = color.green(record.levelname)
        elif record.levelname == 'WARNING':
            record.levelname = color.yellow(record.levelname)
        elif record.levelname == 'ERROR':
            record.levelname = color.red(record.levelname)
        elif record.levelname == 'DEBUG':
            record.levelname = color.cyan(record.levelname)
        
        if hasattr(common.threadLocal, 'operation') and (record.name == _SSHFS):
            status = color.red('OFFLINE ') if common.offline else color.green('ONLINE ')
            record.name = status + color.cyan(common.threadLocal.operation+":")
        else:
            record.name = color.cyan(record.name )
        return True
    
class HttpFilter(logging.Filter):
    def filter(self, record): 
        if record.levelno >= logging.ERROR:
            s = logging.Formatter().format(record)             
            return s.find('HttpError') != -1
        return False

class ErrorFilter(logging.Filter):
    def filter(self, record):         
        if record.levelno >= logging.ERROR:
            s = logging.Formatter().format(record)               
            return s.find('HttpError') == -1 and s.find('ConnectionError') == -1
        return False
    
class FuseLogFilter(logging.Filter):
    def filter(self, _record):
        return True
    
class PathFilter(logging.Filter):
    def filter(self, _record): 
        if common.pathfilter == None:
            return True   
        if common.threadLocal.path == None:
            return False 
        for path in common.threadLocal.path:
            if isinstance(path, str) and path.find(common.pathfilter) != -1:
                return True       
        return False

class Log:
    def __init__(self):
        self.logDir = common.dataDir
        if not os.path.exists(self.logDir):
            os.makedirs(self.logDir)
        self.formatter = logging.Formatter('%(asctime)s %(levelname)s TID=%(thread)d %(name)s %(message)s')

    def setupConfig(self, debug: bool, verbose: bool):    
        
        ## debug logging
        if debug or verbose:            
            logging.getLogger(_FUSE).setLevel(logging.WARNING)            
            logging.basicConfig(
                format='%(asctime)s %(levelname)s TID=%(thread)d %(name)s %(message)s',
                datefmt='%H:%M:%S',
                level=logging.DEBUG if verbose else logging.INFO                
            ) 

        # error logging
        for name in [_SSHFS, _FUSE, _PARAMIKO, _PARAMIKO_SFTP]:
            logger = logging.getLogger(name)

            httpErrorHandler = logging.FileHandler(os.path.join(self.logDir, 'httpError.log'), mode='w')
            httpErrorHandler.setFormatter(self.formatter) 
            httpErrorHandler.setLevel(logging.ERROR)
            httpErrorHandler.addFilter(HttpFilter())
            logger.addHandler(httpErrorHandler)

            errorHandler = logging.FileHandler(os.path.join(self.logDir, 'error.log'), mode='w')
            errorHandler.setFormatter(self.formatter) 
            errorHandler.setLevel(logging.ERROR)
            errorHandler.addFilter(ErrorFilter())
            logger.addHandler(errorHandler) 

            if not debug and not verbose:
                logger.setLevel(logging.ERROR)   

         # metrics logging
        metricsHandler = logging.FileHandler(os.path.join(self.logDir, 'metrics.log'), mode='w')
        metricsHandler.setFormatter(self.formatter)             
        metricsLogger = logging.getLogger(_SSHFS_METRICS)
        metricsLogger.addHandler(metricsHandler)
        metricsLogger.setLevel(logging.INFO) 

logger = logging.getLogger(_SSHFS)
metricsLogger = logging.getLogger(_SSHFS_METRICS)

for name in [_SSHFS, _SSHFS_METRICS, _PARAMIKO, _PARAMIKO_SFTP]:
    l = logging.getLogger(name)
    l.addFilter(TidFilter())
    l.addFilter(PathFilter()) if name != _SSHFS_METRICS else None
