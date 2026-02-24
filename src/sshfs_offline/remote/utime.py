from datetime import datetime
from time import time
import errno
from venv import logger

from fuse import FuseOSError

from sshfs_offline import common, eventq, metrics, metadata
from sshfs_offline.log import logger

def execute(path: str, id: str, times) -> int:
    
    if times is not None:
        (atime, mtime) = times
    else:
        now = time()
        (atime, mtime) = (now, now)

    d = metadata.cache.getattr(path, id) 
    if d is None:
        raise FuseOSError(errno.ENOENT)
    
    d['st_atime'] = int(atime)
    d['st_mtime'] = int(mtime)
    metadata.cache.getattr_save(path, d)

    if not d['local_only']:
        eventq.queue.enqueueFileEvent(path, d.get('st_local_id'), d.get('st_ino'))
        metrics.counts.incr('utime_enqueue_event') 
    else:
        logger.debug(f'remote.utime: path={path} is local only, not updating Google Drive')
        metrics.counts.incr('utime_local_only') 
    
    return 0