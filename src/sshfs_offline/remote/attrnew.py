
import math
import os
import stat
from pathlib import Path
from sshfs_offline import commonfunc, directories, directories

def newAttr(
        path: str,
        size: int,
        mode: int,
        parentDirectory: directories.Directory, 
        localOnly: bool,
        ctime = 0,
        mtime = 0,
        atime = 0,
        nlink = 1,
        inode = 0,
        localId: str|None = None
    ) -> dict[str, any]:

    type = 'file'
    if mode & stat.S_IFDIR == stat.S_IFDIR:
        type = 'dir'
    elif mode & stat.S_IFLNK == stat.S_IFLNK:
        type = 'symlink'
    localId = commonfunc.generateLocalId(path, type, 'remote.attr.newAttr', localOnly) if localId == None else localId
    d = {
            'st_size': size,
            'st_mode': mode,
            'st_ctime': ctime,
            'st_mtime': mtime,
            'st_atime': atime,
            'st_ino': inode,
            'local_id': localId,        
            'st_nlink': nlink,
            'local_parent_id': parentDirectory.localId if parentDirectory != None else None,
            'st_uid': os.getuid(),
            'st_gid': os.getgid(),
            'st_blocks': math.ceil(size / 512),
            'st_blksize': 512,
            'file_name': os.path.basename(path) if path != '/' else '/',
            'local_only': localOnly
        }  
    st = os.stat(Path.home()) # use home dir stat for uid/gid defaults
    d['st_uid'] = st.st_uid
    d['st_gid'] = st.st_gid
        
    return d