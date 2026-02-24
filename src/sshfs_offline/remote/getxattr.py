import errno
from fuse import FuseOSError

from sshfs_offline import common, metrics, metadata, stats
from sshfs_offline.log import logger
from sshfs_offline.remote import cnn

_cache: dict[str, dict[str, any]] = {}

def execute(path: str) -> dict[str, any]:
    if common.offline:
        metrics.counts.incr('getxattr_offline')
        raise FuseOSError(errno.ENETDOWN)    
    
    fileId = metadata.cache.getattr(path)
    if fileId is None:
        metrics.counts.incr('getxattr_enoent')
        raise FuseOSError(errno.ENOENT)
    
    if fileId in _cache:        
        file = _cache[fileId]
    else:
        metrics.counts.incr('getxattr_network')
        stats.remoteStats.lstat += 1
        file = cnn.getConnection().sftp.lstat(cnn.fixPath(path), extended=True)
        _cache[fileId] = file

    dic: dict[str, str] = {}
    for key in file.keys():
        value = file.get(key)
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                if isinstance(subvalue, bool) and subvalue is False:
                    continue                    
                dic[f'{key}.{subkey}'] = str(subvalue, 'utf-8') if isinstance(subvalue, bytes) else str(subvalue)
        else:
            if isinstance(value, bool) and value is False:
                continue
            dic[key] = str(value, 'utf-8') if isinstance(value, bytes) else str(value)
            
    return dic
    
        