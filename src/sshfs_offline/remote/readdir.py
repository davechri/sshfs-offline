
import errno
import copy
import math
import os
from pathlib import Path
import stat
from fuse import FuseOSError
from sshfs_offline import common, commonfunc, directories, directories, metadata, lock, eventq, metrics, data
from sshfs_offline.log import logger
from sshfs_offline.remote import attrnew, cnn
from sshfs_offline.stats import remoteStats

def execute(path: str, deleteEntries=False) -> dict[str, str]:
    logger.info(f'remote.readdir {path}')
    if common.offline:
        metrics.counts.incr('readdir_offline')
        raise FuseOSError(errno.ENETDOWN)
    metrics.counts.incr('readdir_network')
    remoteStats.readdir += 1
    # 1757987266.8084266750 1757987266.8084266750 1767382078.5648721200 1000 1000 775 4096 d 1 444 .mirrorfs
    (_, stdout, stderr) = cnn.getConnection().ssh.exec_command(f'find {cnn.fixPath(path)} -maxdepth 1 -printf "%T@ %C@ %A@ %U %G %m %s %y %n %i %P target=%l\n"', timeout=common.TIMEOUT) 
    stderr = stderr.read().decode('utf-8') 
    if 'No such file or directory' in stderr: # No such file or directory
        logger.debug(f'remote.readdir not found {path}')
        if deleteEntries:
            oldEntries = metadata.cache.readdir(path)
            if oldEntries is not None:
                for name, localId in oldEntries.items():
                    logger.info(f'remote.readdir deleting removed entry {name} from {path}')
                    data.cache.deleteByID(os.path.join(path, name), localId)               
                    metadata.cache.deleteMetadata(os.path.join(path, name), localId, 'remote.readdir: entry removed')
                localId = metadata.cache.getattr_get_local_id(path)
                if localId:
                    metadata.cache.deleteMetadata(path, localId, 'remote.readdir: directory removed')
        else:
            raise FuseOSError(errno.ENOENT)
    if stderr != '':
        logger.debug(f'remote.readdir error {stderr}')
        raise FuseOSError(errno.ENOENT)
    
    with lock.get(path):        
        dirEntries = metadata.cache.readdir(path)
        oldEntries = copy.deepcopy(dirEntries) if deleteEntries and dirEntries is not None else {}    
        if dirEntries == None:
            dirEntries: dict[str, str] = {}
        lines = stdout.read().decode('utf-8').splitlines()        
        thisDirectory = directories.store.getDirectoryByPath(path) 
        if thisDirectory == None:
            parentDirectory = directories.store.getParentDirectory(os.path.dirname(path))           
            thisDirectory = directories.Directory(
                inode=0,
                localId=commonfunc.generateLocalId(path, 'dir', 'readdir', localOnly=False),
                path=path,
                name=os.path.basename(path) if path != '/' else '/',
                localParentId=parentDirectory.localId if parentDirectory != None else None,
                localOnly=False
            )
            directories.store.addDirectory(
                thisDirectory.path,
                thisDirectory.inode,
                thisDirectory.localId,
                thisDirectory.localParentId,
                thisDirectory.localOnly
            )
        parentDirectory = directories.store.getParentDirectory(path)

        i = 0
        for line in lines:
            parts = line.split('target=',1)
            target = parts[1] if len(parts) > 1 else ''        
            parts = parts[0].split(' ',10)
            if i == 0 and parts[10] == ' ':  # first line is the directory itself
                parts.pop()  # remove the '' entry added by find               
                parts.append('.')  # add current directory entry
            i += 1
            
            t = 'target='+target if len(target) > 0 else ''
            logger.debug(f'remote.readdir processing line: {parts} {t}')
            ctime = float(parts[0])
            atime = float(parts[1])
            mtime = float(parts[2])
            _uid = int(parts[3])
            _gid = int(parts[4])
            mode = int(parts[5], 8)
            size = int(parts[6])
            ftype = parts[7]
            nlink = int(parts[8])
            inode = int(parts[9])
            name = parts[10].rstrip()
            if ftype == 'l':
                mode |= stat.S_IFLNK
            elif ftype == 'd':
                mode |= stat.S_IFDIR
            else:
                mode |= stat.S_IFREG

            logger.debug(f'remote.readdir found entry: name={name} inode={inode} size={size} mode={oct(mode)} ftype={ftype} nlink={nlink} ctime={ctime} mtime={mtime} atime={atime} target={target}')
            if name == '.':  
                if thisDirectory.inode == 0:
                    thisDirectory.inode = inode                    
                    directories.store.addDirectory(
                        thisDirectory.path,
                        thisDirectory.inode,
                        thisDirectory.localId,
                        thisDirectory.localParentId,
                        thisDirectory.localOnly
                    )
                        
                dirEntries[name] = thisDirectory.localId
                oldEntries.pop(name, None)

                d = metadata.cache.getattr(path)
                if d is not None:
                    if d['st_ino'] != inode:
                        if d['st_ino'] == 0:
                            logger.info(f'readdir: setting inode for {path} to {inode}')
                        else:
                            logger.warning(f'readdir: updating inode for {path} from {d["st_ino"]} to {inode}')
                        d['st_ino'] = inode
                        metadata.cache.getattr_save(path, d)
                    continue                
                d = attrnew.newAttr(
                    path=path,
                    size=size,
                    mode=mode,
                    parentDirectory=parentDirectory,
                    localOnly=False,
                    ctime=ctime,
                    mtime=mtime,
                    atime=atime,
                    nlink=nlink,
                    inode=inode,
                    localId=thisDirectory.localId
                )            
                metadata.cache.getattr_save(path, d)
                continue
            else:
                d = metadata.cache.getattr(os.path.join(path, name))  
                if d is not None:
                    if d['st_ino'] != inode:
                        if d['st_ino'] == 0:
                            logger.info(f'readdir: setting inode for {path} to {inode}')
                        else:
                            logger.warning(f'readdir: updating inode for {path} from {d["st_ino"]} to {inode}')
                        d['st_ino'] = inode
                        metadata.cache.getattr_save(path, d)
                    dirEntries[name] = d['local_id']  
                    oldEntries.pop(name, None)
                    continue 
            
            # If an event is queued for this inode, do not update the directory entry.        
            if inode != 0 and eventq.queue.isEventQueuedForInode(inode):
                continue

            d = attrnew.newAttr(
                path=os.path.join(path, name),
                size=size,
                mode=mode,
                parentDirectory=thisDirectory,
                localOnly=False,
                ctime=ctime,
                mtime=mtime,
                atime=atime,
                nlink=nlink,
                inode=inode,
            )         
            
            if ftype == 'd':                
                directories.store.addDirectory(
                    os.path.join(path, name), 
                    d['st_ino'], 
                    d['local_id'], 
                    d['local_parent_id'], 
                    localOnly=False)

            metadata.cache.getattr_save(os.path.join(path, name), d)
            dirEntries[name] = d['local_id']   
            oldEntries.pop(name, None)

            if len(target) > 0:            
                metadata.cache.readlink_save(os.path.join(path, name), d['local_id'], target.rstrip())     

        if path != '/':
            dirEntries['..'] = thisDirectory.localParentId
            oldEntries.pop('..', None)

        if deleteEntries:
            for name, localId in oldEntries.items():
                logger.info(f'remote.readdir deleting removed entry {name} from {path}')
                data.cache.deleteByID(os.path.join(path, name), localId)               
                metadata.cache.deleteMetadata(os.path.join(path, name), localId, 'remote.readdir: entry removed')

        logger.info(f'remote.readdir {path} entries={dirEntries}')
        metadata.cache.readdir_save(path, dirEntries)    
    return dirEntries