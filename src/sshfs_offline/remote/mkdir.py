
import errno

import os
from pathlib import Path
import stat
import time

from fuse import FuseOSError

from sshfs_offline import common, commonfunc, directories, localonly, metadata, gitignore, metrics, eventq
from sshfs_offline.log import logger
from sshfs_offline.remote import attr, attrnew, cnn
from sshfs_offline.stats import remoteStats

def execute(path: str, mode: int, runAsync: bool=True) -> str:
    logger.debug(f'remote.mkdir: path={path} mode={oct(mode)} runAsync={runAsync}')

    if not runAsync and common.offline:
        raise Exception(f'remote.mkdir: cannot mkdir while offline {path}')

    if path == '/':
        raise FuseOSError(errno.ENOENT)
    
    # Check if directory already exists and not called by eventq.queue.enqueueDirEvent
    if directories.store.getDirectoryByPath(path) != None and runAsync == True:       
        raise FuseOSError(errno.EEXIST)
    
    parentDirectory = directories.store.getParentDirectory(path) 
    if parentDirectory == None:
        raise FuseOSError(errno.ENOENT)

    if localonly.lconfig.isInLocalOnlyConfig(path):
        logger.info(f'remote.mkdir: localOnly {path}')
        if not parentDirectory.localOnly:
            localonly.lconfig.createDirSymLink(path)
            return None
        
    localOnly = False
    localId = metadata.cache.getattr_get_local_id(path)
    if localId == None:
        localOnly = gitignore.parser.isIgnored(path)                
   
    mode2 = stat.S_IFDIR | mode
    
    inode = 0       
    if not runAsync and not localOnly:        
        for timeout in commonfunc.apiTimeoutRange():
            try:  
                metrics.counts.incr('mkdir_network')
                if parentDirectory.inode == 0:                   
                    raise Exception('remote.mkdir: parentDirectory.inode is 0 for path=%s %s', path, parentDirectory.__dict__)
                remoteStats.mkdir += 1
                cnn.getConnection().sftp.mkdir(cnn.fixPath(path), mode2)                
                d = attr.execute(path)                
                inode = d.get('st_ino', 0)
                break
            except TimeoutError as e:
                logger.error(f'mkdir timeout: {e}')
                metrics.counts.incr('mkdir_network_timeout')
                if commonfunc.isLastAttempt(timeout):
                    raise
    else:
        d = attrnew.newAttr(path, 4096, mode2, parentDirectory, localOnly, localId=localId)
        localId = d.get('local_id')
        if localOnly:            
            metrics.counts.incr('mkdir_localonly')
        else:            
            metrics.counts.incr('mkdir_enqueue_event')        
            eventq.queue.enqueueDirEvent(path, localId, inode=0)           
   
    metadata.cache.getattr_save(path, d)

    parentPath = parentDirectory.path
    metadata.cache.readdir_add_entry(parentPath, d.get('file_name'), localId)
        
    directories.store.addDirectory(path, d.get('st_ino'), d.get('local_id'), parentDirectory.localId, localOnly)

    metadata.cache.readdir_add_entry(path, '.', d.get('local_id'))
    metadata.cache.readdir_add_entry(path, '..', parentDirectory.localId)
    
    return inode
