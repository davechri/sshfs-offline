
import errno
import math
import time
import os
import stat
from pathlib import Path
from fuse import FuseOSError
from sshfs_offline import directories, metadata
from sshfs_offline.remote import cnn, readdir
from sshfs_offline.log import logger
from sshfs_offline.stats import remoteStats

def execute(path: str) -> dict[str, any]:
    logger.info(f'remote.getattr {path}')
    remoteStats.getattr +=1
           
    if path == '/':
        dirEntries = readdir.execute(path)
        localId = dirEntries.get('.', None)
        d = metadata.cache.getattr(path, localId)
        if d == None:
            logger.error(f'remote.getattr: root directory not found in cache')
            raise FuseOSError(errno.ENOENT)
        return d
    else:
        dirEntries = readdir.execute(os.path.dirname(path))    
        if dirEntries is not None and dirEntries.get(os.path.basename(path)):        
            return metadata.cache.getattr(path)

    raise FuseOSError(errno.ENOENT)

def getPathFromStat(d: dict[str, any]) -> str|None:
    if d['file_name'] == '/':
        return '/'
    parentDirectory = directories.store.getDirectoryByLocalId(d.get('local_parent_id', ''))
    if parentDirectory is None:
        logger.warning(f'attr.getPathFromStat: parentDirectory not found for local_parent_id={d.get("local_parent_id", "")}')
        return None
    return os.path.join(parentDirectory.path, d.get('file_name')) if parentDirectory != None else None