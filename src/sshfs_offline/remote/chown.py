
import errno
import stat
from venv import logger

from fuse import FuseOSError
from sshfs_offline import metrics, metadata
from sshfs_offline.log import logger

def execute(path:str, uid: int, gid: int) -> None:
    logger.debug(f'remote.chown: path={path} uid={uid} gid={gid}')
    d = metadata.cache.getattr(path) 
    if d is not None:   
        if uid != -1:
            d['st_uid'] = uid
            metrics.counts.incr(f'chown_uid_{uid}')
        if gid != -1:
            d['st_gid'] = gid
            metrics.counts.incr(f'chown_gid_{gid}')
        
        metadata.cache.getattr_save(path, d)
    else:
        metrics.counts.incr('chown_enoent')
        raise FuseOSError(errno.ENOENT)
    
    
        