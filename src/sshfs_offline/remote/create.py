
import errno

import os
import stat
import time
from pathlib import Path
from sshfs_offline import common, commonfunc, directories, gitignore, metrics, eventq, metadata
from sshfs_offline.log import logger
from sshfs_offline.remote import attr, attrnew
from fuse import FuseOSError

from sshfs_offline.remote import cnn
from sshfs_offline.stats import remoteStats

def execute(path: str, mode: int, localId: str|None = None, runAsync: bool=True) -> None:
    """
    Creates a new file at the specified path with the given mode.
    Args:
        path (str): The path where the new file will be created.
        mode (int): The file mode (permissions) for the new file.
    Returns:
        str: The ID of the newly created file in Google Drive.
    Raises:
        FuseOSError: If an error occurs during file creation.
    """ 
    logger.debug(f'remote.create: path={path} mode={oct(mode)} localId={localId} runAsync={runAsync}')

    if not runAsync and common.offline:
        raise Exception(f'remote.create: cannot create while offline {path}')
    
    if path == '/':
        raise FuseOSError(errno.EINVAL)
   
    parentDirectory = directories.store.getParentDirectory(path)    
    if parentDirectory == None:
        raise FuseOSError(errno.ENOENT)

    d = metadata.cache.getattr(path, localId)
    if d != None:
        localId = d.get('local_id')
        if d.get('st_ino', 0) != 0:
            metrics.counts.incr('create_truncate')
            if not runAsync:    
                remoteStats.truncate += 1          
                st = cnn.getConnection().sftp.truncate(cnn.fixPath(path), 0)
                d['st_atime'] = st.st_atime
                d['st_mtime'] = st.st_mtime
                d['st_ctime'] = st.st_mtime 
                metadata.cache.getattr_save(path, d)   
            elif not d.get('local_only'):
                metrics.counts.incr('create_truncate_enqueue_event')
                eventq.queue.enqueueTruncateEvent(path, d.get('local_id'), d.get('st_ino'), 0)
            else:
                logger.debug(f'remote.create: path={path} is local only, not truncating on Google Drive')
                metrics.counts.incr('create_truncate_local_only')
            d['st_size'] = 0
            metadata.cache.getattr_save(path, d)
            return
    
    localOnly = False
    if localId == None:
        if gitignore.parser.isIgnored(path):
            logger.debug(f'remote.create: {path} is ignored by .gitignore, creating as local only')
            localOnly = True
        
    name = os.path.basename(path)   
    mode = stat.S_IFREG | mode      
    inode = 0       
    if not runAsync and not localOnly:
        for timeout in commonfunc.apiTimeoutRange():
            try:
                metrics.counts.incr('create_network')  
                if parentDirectory.inode == 0:                    
                    raise Exception('remote.create: parentDirectory.inode is 0 for path=%s', path)

                remoteStats.create += 1 
                with cnn.getConnection().sftp.open(cnn.fixPath(path), 'w') as f:
                    f.chmod(mode)
                d = attr.execute(path)
                inode = d.get('st_ino', 0)
                break
            except TimeoutError as e:
                logger.error(f'create timed out: {e}')
                metrics.counts.incr('create_network_timeout')
                if commonfunc.isLastAttempt(timeout):
                    raise
    else:
        metrics.counts.incr('create_enqueue_event')
        if d == None:
            d = attrnew.newAttr(path, 0, mode, parentDirectory, localOnly, localId=localId)
            localId = d.get('local_id')
            metadata.cache.getattr_save(path, d)
       
    if localOnly:            
        metrics.counts.incr('create_localonly')
    else:
        eventq.queue.enqueueFileEvent(path, localId, inode=0)
        logger.debug('remote.create enqueue event: %s', d)

    parentPath = parentDirectory.path
    metadata.cache.readdir_add_entry(parentPath, name, localId)
    
    return inode
    