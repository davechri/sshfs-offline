import io
import os
import stat
from fuse import FuseOSError
import errno

from sshfs_offline import common, commonfunc, data, metadata, eventq, metrics
from sshfs_offline.log import logger
from sshfs_offline.remote import attr, cnn
from sshfs_offline.stats import remoteStats

def execute(oldpath: str, newpath: str, runAsync: bool=True) -> None: 
    logger.debug(f'remote.rename: oldpath={oldpath} newpath={newpath} runAsync={runAsync}')

    if not runAsync and common.offline:
        raise Exception(f'remote.rename: cannot rename while offline {oldpath} to {newpath}')

    if oldpath == '/' or newpath == '/':
        metrics.counts.incr('rename_root_is_invalid')
        raise FuseOSError(errno.EINVAL)
    
    dOld = metadata.cache.getattr(oldpath)
    if dOld == None:
        raise FuseOSError(errno.ENOENT)  
    
    dNew = metadata.cache.getattr(newpath)
    if dNew is not None:
        if dNew['st_mode'] &  stat.S_IFDIR:
            if dOld['local_only']:
                logger.debug(f'remote.rename: newpath={newpath} is local only, not removing existing directory from Google Drive')
                metrics.counts.incr('rename_delete_existing_dir_local_only')
            elif runAsync:
                metrics.counts.incr('rename_delete_existing_dir_enqueue')
                eventq.queue.enqueueDirEvent(newpath, dNew['local_id'], dNew['st_ino'])
            else:
                metrics.counts.incr('rename_delete_existing_dir')
                service = common.getApiClient()
                request = service.files().delete(fileId=dNew['local_id'])
                request.execute()            
        else:
            if dNew['local_only']:
                logger.debug(f'remote.rename: newpath={newpath} is local only, not removing existing file from Google Drive')
                metrics.counts.incr('rename_delete_existing_file_local_only')
            elif runAsync:
                if dNew['st_mode'] & stat.S_IFLNK == stat.S_IFLNK:
                    metrics.counts.incr('rename_delete_existing_symlink_enqueue')
                    eventq.queue.enqueueSymlinkEvent(newpath, dNew['local_id'], dNew['st_ino'])
                else:
                    metrics.counts.incr('rename_delete_existing_file_enqueue')
                    eventq.queue.enqueueFileEvent(newpath, dNew['local_id'], dNew['st_ino'])
            else:
                metrics.counts.incr('rename_delete_existing_file')
                service = common.getApiClient()
                request = service.files().delete(fileId=dNew['local_id'])
                request.execute()

        data.cache.deleteByID(newpath, dNew['local_id'])
        metadata.cache.deleteMetadata(newpath, dNew['local_id'], 'rename: delete metadata')
        
    metadata.cache.renameMetadata(oldpath, newpath, dOld['local_id'])

    if not runAsync:             
        metrics.counts.incr('rename_network')            
        doRename(oldpath, newpath, dOld)        
    elif not dOld['local_only']:   
        metrics.counts.incr('rename_enqueue_event')
        eventq.queue.enqueueRenameEvent(oldpath, newpath, dOld['local_id'], dOld['st_ino'])
    else:
        logger.debug(f'remote.rename: oldpath={oldpath} is local only, not renaming on remote server')
        metrics.counts.incr('rename_local_only')

def doRename(oldpath: str, newpath: str, d: dict[str,any]) -> None:
    for timeout in commonfunc.apiTimeoutRange():
        try:     
            remoteStats.rename += 1       
            _, stdout, stderr = cnn.getConnection().ssh.exec_command(f'mv {cnn.fixPath(oldpath)} {cnn.fixPath(newpath)}', timeout=common.TIMEOUT)
            stderr = stderr.read().decode('utf-8') 
            if 'No such file or directory' in stderr: # No such file or directory
                logger.debug(f'remote.doRename not found {oldpath}')
                return
            if stderr != '':
                logger.debug(f'remote.doRename error {stderr}')
                raise FuseOSError(errno.ENOENT)
            break
        except TimeoutError as e:
            logger.error(f'rename timeout: {e}')
            metrics.counts.incr('rename_network_timeout')
            if commonfunc.isLastAttempt(timeout):
                raise 
    
   