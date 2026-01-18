
import os
from fuse import FuseOSError
import errno
from sshfs_offline import metadata
from sshfs_offline.log import logger
from sshfs_offline.remote import attr

def execute(path: str) -> str:
    logger.debug(f'remote.readlink: path={path}')
           
    target = metadata.cache.readlink(path)

    if target == None:
        d = metadata.cache.getattr(path)
        if d is None:
            d = attr.execute(path)
            if d is None:
                raise FuseOSError(errno.ENOENT) 
            target = metadata.cache.readlink(path)        
    
    if target is None:
        logger.error(f'remote.readlink: path={path} is not a symlink')
        raise Exception(f'remote.readlink: path={path} is not a symlink')
    
    baseDir = os.path.dirname(path)

    logger.debug(f'remote.readlink: {path} baseDir={baseDir} targetPath={target}')   
    return os.path.relpath(target, baseDir)      
    