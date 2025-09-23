    
import logging
import os
from pathlib import Path

MAIN        = 'main    '
SFTP        = 'sftp    '
METADATA    = 'metadata'
DATA        = 'data    '
METRICS     = 'metrics'

FUSE        = 'fuse'
PARAMIKO    = 'paramiko'

class Log:
    def __init__(self):
        self.logDir = os.path.join(Path.home(), '.sshfs-offline')
        if not os.path.exists(self.logDir):
            os.makedirs(self.logDir)
        self.formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s %(message)s')

    def setupConfig(self, debug: bool):    

        ## debug logging
        if debug:            
            logging.getLogger(FUSE).setLevel(logging.WARNING)    
            logging.getLogger(PARAMIKO).setLevel(logging.WARNING)            
            logging.basicConfig(
                format='%(asctime)s:%(levelname)s:%(name)s %(message)s',
                datefmt='%H:%M:%S',
                level=logging.DEBUG                                
            ) 
                
        # error logging 
        for name in [MAIN, SFTP, METADATA, DATA, FUSE, PARAMIKO]:
            logger = logging.getLogger(name)
            errorHandler = logging.FileHandler(os.path.join(self.logDir, 'error.log'), mode='w')
            errorHandler.setFormatter(self.formatter) 
            errorHandler.setLevel(logging.ERROR)
            logger.addHandler(errorHandler)
            if not debug:
                logger.setLevel(logging.ERROR)   

        # metrics logging
        metricsHandler = logging.FileHandler(os.path.join(self.logDir, 'metrics.log'), mode='w')
        metricsHandler.setFormatter(self.formatter)             
        logger = logging.getLogger(METRICS)
        logger.addHandler(metricsHandler)
        logger.setLevel(logging.DEBUG)        