
import errno
import stat

from fuse import FuseOSError

from sshfs_offline import common, commonfunc, metrics, eventq, metadata
from sshfs_offline.log import logger
from sshfs_offline.remote import cnn
from sshfs_offline.stats import remoteStats

def execute(path:str, localId: str, mode: int, d: dict[str,any], runAsync: bool=True) -> None:
    logger.debug(f'remote.chmod: path={path} localId={localId} mode={oct(mode)} runAsync={runAsync}')
    if not runAsync and common.offline:
        raise Exception(f'remote.chmod: cannot chmod while offline {path}')    
   
    oldMode = d['st_mode']
    d['st_mode'] = mode | ((stat.S_IFDIR | stat.S_IFLNK | stat.S_IFREG) & d['st_mode'])
    if oldMode == d['st_mode']:
        logger.info(f'remote.chmod: path={path} mode is unchanged {oct(mode)}')
        return
    metrics.counts.incr(f'chmod_{oct(oldMode)}_to_{oct(d["st_mode"])}')
    metadata.cache.getattr_save(path, d)

    inode = d.get('st_ino', 0)
    if d.get('local_only', False):
        logger.debug(f'remote.chmod: path={path} is local only, not updating Google Drive')
        metrics.counts.incr('chmod_local_only')  
    elif runAsync:
        metrics.counts.incr('chmod_enqueue_event')
        eventq.queue.enqueueChmodEvent(path, localId, inode)    
    else:
        for timeout in commonfunc.apiTimeoutRange():
            try:
                metrics.counts.incr('chmod_network')  
                remoteStats.chmod += 1                            
                st = cnn.getConnection().sftp.chmod(cnn.fixPath(path), mode) 
                d['st_atime'] = st.st_atime
                d['st_mtime'] = st.st_mtime
                d['st_ctime'] = st.st_mtime 
                metadata.cache.getattr_save(path, d)             
                break             
            except TimeoutError as e:
                logger.error(f'chmod timed out: {e}')            
                metrics.counts.incr('chmod_network_timeout')
                if commonfunc.isLastAttempt(timeout):
                    raise 
                       
        
        