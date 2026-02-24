import os
import shutil

from sshfs_offline import config
from sshfs_offline.log import logger
from sshfs_offline import common, commonfunc, metadata
from sshfs_offline.remote import symlink

from pathlib import Path

class LocalOnly:
    def __init__(self):
        pass

    def init(self):
        self.localonlyDir = os.path.join(common.dataDir, 'localonly')
        os.makedirs(self.localonlyDir, exist_ok=True) 

    def isInLocalOnlyConfig(self, path: str) -> bool:        
        name = os.path.basename(path)
        localOnly = name in config.config.getLocalOnlyDirs()
        if localOnly:
            logger.info(f'localonly.isInLocalOnlyConfig: {path}')
        
        return localOnly   

    def createDirSymLink(self, path: str) -> None:                 
        targetPath = os.path.join(self.localonlyDir, path[1:])
        logger.info(f'localonly.createDirSymLink: path={path} targetPath={targetPath}') 
        os.makedirs(targetPath, exist_ok=True)     
        localId = commonfunc.generateLocalId(path, 'symlink', 'localonly symlink', localOnly=True)
        symlink.execute(path, localId, targetPath)
            
    def deleteDirSymLink(self, path: str) -> None:        
        targetPath = metadata.cache.readlink(path)
        if targetPath == None or targetPath.startswith(self.localonlyDir) == False:            
            return
        logger.info(f'localonly.deleteDirSymLink: path={path} targetPath={targetPath}')
        shutil.rmtree(targetPath, ignore_errors=True)

    def deleteAll(self) -> None:
        logger.info(f'localonly.deleteAll')
        for entry in os.listdir(self.localonlyDir):
            entryPath = os.path.join(self.localonlyDir, entry)
            shutil.rmtree(entryPath, ignore_errors=True)

lconfig = LocalOnly()