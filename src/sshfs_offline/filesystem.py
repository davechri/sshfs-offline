import errno
import os
import copy

from sshfs_offline import config, eventq, localonly, metrics, common, commonfunc
from sshfs_offline import stats
from sshfs_offline.remote import attr, chown, getxattr, mkdir, mkdir, readdir, cnn, readlink, rmdir
from sshfs_offline import log

from fuse import FUSE, FuseOSError, Operations

from sshfs_offline import data, directories
from sshfs_offline import metadata
from sshfs_offline import db
from sshfs_offline.log import logger
from sshfs_offline.threads import heartbeat, refreshcache, rpc, tcreate, tdelete, tdownload, tupload,refresh
from sshfs_offline.remote import truncate, chmod, utime, create, remove, rename, symlink
from sshfs_offline.stats import calledStats

class FuseOps(Operations):
    '''
    SSH File System with offline access to cached files.
    '''    
                        
    def __init__(self, args): 
        common.threadLocal.operation = 'filesys_init'
        common.threadLocal.path = None
        host = args.host
        user = args.user
        if args.remotedir == None:
            remotedir = os.path.join('/home', user)
        else:
            remotedir = args.remotedir
        port = args.port
               
        metrics.counts = metrics.Metrics()        
        cnn.cnn = cnn.Remote(host, user, remotedir, port) 
        db.cache = db.Db(args.clearcache)
        data.cache = data.Data()
        config.config.init()
        localonly.lconfig.init()
        tupload.manager.init()
    
        cnn.getConnection(startup=True) # verify connection to host

        try:
            fuse = FUSE(
                self,
                args.mountpoint,
                foreground=args.debug,
                nothreads=False,
                allow_other=True,
                big_writes=True,
                max_read=cnn.BLOCK_SIZE, # Set max read size (e.g., 128KB)
                max_write=cnn.BLOCK_SIZE, # Set max write size (e.g., 128KB)
            )
        except Exception as e:
            print(f'Ensure that no terminal is referencing the mountpoint directory {args.mountpoint},')
            print('and that the mountpoint directory exists and is empty.')
            print('To unmount filesystem: sshfs-offline unmount {}'.format(args.mountpoint))
    
    def _handleException(self, name: str, logStr: str, e: Exception):       
        raisedBy = commonfunc.exceptionRaisedBy(e)
        
        if isinstance(e,  FuseOSError):            
            if e.errno == errno.ENOENT:
                logger.info(f'{logStr} {raisedBy}')
            else:
                logger.warning(f'{logStr} {raisedBy}')
            metrics.counts.incr(f'{name}_{os.strerror(e.errno)}')
            raise e        
        elif isinstance(e, Exception):
            calledStats.exceptions += 1
            logger.exception(f'{logStr} {raisedBy}')
            metrics.counts.incr(f'{name}_except')
            raise FuseOSError(errno.EINVAL)

    def init(self, path):
        calledStats.init += 1
        common.threadLocal.operation = 'init'
        common.threadLocal.path = (path,)

        try:
            logger.info('--> %s', path)
            metrics.counts.startExecution('init')
            metrics.counts.start()
            log.Log().setupConfig(common.debug, common.verbose)
            directories.store.populateFromDb()
            heartbeat.monitor.start()  
            tupload.manager.start()
            tdownload.manager.start()
            tcreate.manager.start()
            tdelete.manager.start()
            refresh.thread.start()
            eventq.queue.init()
            rpc.server.start()             
            logger.info('<-- %s', path) 
        except Exception as e:
            self._handleException('init', f'<-- {path}', e)            
        finally:
            metrics.counts.endExecution('init')   
         
    def chmod(self, path, mode): 
        calledStats.chmod += 1
        common.threadLocal.operation = 'chmod'
        common.threadLocal.path = (path,)
        try: 
            metrics.counts.startExecution('chmod')
            logger.info('--> %s %s', path, oct(mode))   
            metrics.counts.incr('chmod')
            d = metadata.cache.getattr(path)
            if d == None:
                raise FuseOSError(errno.ENOENT)
            chmod.execute(path, d.get('local_id'), mode, d)
            logger.info('<-- %s %s', path, oct(mode))
        except Exception as e:
            self._handleException('chmod', f'<-- {path} {oct(mode)}', e)
        finally:
            metrics.counts.endExecution('chmod')

    def chown(self, path, uid, gid):
        calledStats.chown += 1
        common.threadLocal.operation = 'chown'
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('chown')
            logger.info('--> %s %s %s', path, uid, gid) 
            metrics.counts.incr('chown')
            chown.execute(path, uid, gid)            
            logger.info('<-- %s', path)
        except Exception as e:
            self._handleException('chown', f'<-- {path} {uid} {gid}', e)
        finally:
            metrics.counts.endExecution('chown')

    def create(self, path, mode): 
        calledStats.create += 1
        common.threadLocal.operation = 'create'
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('create')
            logger.info('--> %s %s', path, oct(mode)) 
            create.execute(path, mode, None) 
            metrics.counts.incr('create')                         
            logger.info('<-- %s %s', path, oct(mode))             
            return 0
        except Exception as e:
            self._handleException('create', f'<-- {path} {oct(mode)}', e)            
        finally:
            metrics.counts.endExecution('create')  
        
    def destroy(self, path): 
        calledStats.destroy += 1
        common.threadLocal.operation = 'destroy' 
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('destroy')
            logger.info('--> %s', path)  
            metrics.counts.incr('destroy')   
            cnn.getConnection().sftp.close()       
            logger.info('<-- %s', path)
        except Exception as e:
            self._handleException('destroy', f'<-- {path}', e)
        finally:
            metrics.counts.stop()
            heartbeat.monitor.stop()
            tcreate.manager.stop()
            tdelete.manager.stop()
            tdownload.manager.stop()  
            rpc.server.stop()
            metrics.counts.endExecution('destroy') 

    def flush(self, path, fh):
        calledStats.flush += 1
        common.threadLocal.operation = 'flush'
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('flush')
            logger.info('--> %s', path)
            metrics.counts.incr('flush')
            rc = 0
            rc = tupload.manager.flush(path)
            logger.info('<-- %s rc=%d', path, rc)
            return rc
        except Exception as e:
            self._handleException('flush', f'<-- {path}', e)
        finally:
            metrics.counts.endExecution('flush')     

    def getxattr(self, path, name, position=0) -> str:
        calledStats.getattr += 1
        common.threadLocal.operation = 'getxattr'
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('getxattr')
            logger.info('--> %s %s', path, name)
            metrics.counts.incr('getxattr')
            if common.xattrEnabled:                 
                attrs = getxattr.execute(path)                
            else:
                attrs = {}
            logger.info('<-- %s %s %s', path, name, attrs)
            return attrs[name].encode('utf-8') if name in attrs else b''
        except Exception as e:
            self._handleException('getxattr', f'<-- {path}', e)
        finally:
            metrics.counts.endExecution('getxattr')    

    def getattr(self, path, fh=None):
        calledStats.getattr += 1
        common.threadLocal.operation = 'getattr'
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('getattr')
            logger.info('--> %s', path)
            metrics.counts.incr('getattr')
            d = metadata.cache.getattr(path, enoent_except=True)
            if d != None:   
                c = copy.deepcopy(d)
                c['st_mode'] = oct(c['st_mode'])              
                logger.info('<-- %s %s', path, c)
                return d # cache hit   
            try:  
                d = attr.execute(path)
                c = copy.deepcopy(d)
                c['st_mode'] = oct(c['st_mode'])  
                logger.info('<-- %s %s', path, c)
                return d 
            except FuseOSError as e:    
                if e.errno == errno.ENOENT:
                    logger.info(f'getattr: path={path} ENOENT')
                    if path != '/':
                        localId = commonfunc.generateLocalId(path, 'negative', 'negative cache entry', localOnly=False)
                        metadata.cache.addNegativeCache(path, localId, 'getattr')
                raise e           
        except Exception as e:
            self._handleException('getattr', f'<-- {path}', e)
        finally:
            metrics.counts.endExecution('getattr')
        
    def statfs(self, path): 
        calledStats.statFs += 1
        common.threadLocal.operation = 'statfs' 
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('statfs')
            logger.info('--> %s', path) 
            metrics.counts.incr('statfs')            
            stv = os.statvfs(common.dataDir)
            dic = dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
                'f_frsize', 'f_namemax'))
            dic['f_bsize'] = common.BLOCK_SIZE
            dic['f_frsize'] = common.BLOCK_SIZE
            logger.info('<-- %s %s', path, dic)  
            return dic       
        except Exception as e:
            self._handleException('statfs', f'<-- {path}', e)
        finally:
            metrics.counts.endExecution('statfs')

    def listxattr(self, path) -> list[str]:
        calledStats.listxattr += 1
        common.threadLocal.operation = 'listxattr'
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('listxattr')
            logger.info('--> %s', path)
            metrics.counts.incr('listxattr')
            if common.xattrEnabled: 
                attrs = []
            else:               
                attrs = []
            logger.info('<-- %s %s', path, attrs)
            return attrs
        except Exception as e:
            self._handleException('listxattr', f'<-- {path}', e)
        finally:
            metrics.counts.endExecution('listxattr')

    def mkdir(self, path, mode): 
        calledStats.mkdir += 1
        common.threadLocal.operation = 'mkdir'
        common.threadLocal.path = (path,)
        try: 
            metrics.counts.startExecution('mkdir')
            logger.info('--> %s %s', path, oct(mode)) 
            metrics.counts.incr('mkdir')            
            mkdir.execute(path, mode)
            logger.info('<-- %s %s', path, oct(mode))        
        except Exception as e:
            self._handleException('mkdir', f'<-- {path} {oct(mode)}', e)
        finally:
            metrics.counts.endExecution('mkdir')
        
    def read(self, path, size, offset, fh):  
        calledStats.read += 1 
        common.threadLocal.operation = 'read'
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('read')
            logger.info('--> %s size=%d offset=%d', path, size, offset)
            metrics.counts.incr('read')

            buf = tdownload.manager.read(path, size, offset, readEntireFile=False)
            
            logger.info(f'<-- %s size=%d', path, len(buf))
            return bytes(buf)
        except Exception as e:
            self._handleException('read', f'<-- {path} {size} {offset}', e)           
        finally:
            metrics.counts.endExecution('read')

    def readdir(self, path, fh):
        calledStats.readdir += 1
        common.threadLocal.operation = 'readdir'
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('readdir')
            logger.info('--> %s', path)
            metrics.counts.incr('readdir')
            dirEntries = metadata.cache.readdir(path)
            if dirEntries != None:
                logger.info(f'<-- %s {dirEntries.keys()}', path)
                return list(dirEntries.keys())
            dirEntries = readdir.execute(path)
            logger.info('<-- %s', path)
            return list(dirEntries.keys())   
        except Exception as e:
            self._handleException('readdir', f'<-- {path}', e)            
        finally:
            metrics.counts.endExecution('readdir')
        
    def readlink(self, path):
        calledStats.readlink += 1
        common.threadLocal.operation = 'readlink'
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('readlink')
            logger.info('--> %s', path)
            metrics.counts.incr('readlink')
            link = metadata.cache.readlink(path)
            if link == None: 
                link = readlink.execute(path) 
                if link == None:
                    raise FuseOSError(errno.ENOENT)      
                
            logger.info('<-- %s %s', path, link)
            return link        
        except Exception as e:
            self._handleException('readlink', f'<-- {path}', e)
        finally:
            metrics.counts.endExecution('readlink')

    def rename(self, old, new):
        calledStats.rename += 1
        common.threadLocal.operation = 'rename'
        common.threadLocal.path = (old, new)
        try:
            metrics.counts.startExecution('rename')
            logger.info('--> %s %s', old, new) 
            metrics.counts.incr('rename')
            rename.execute(old, new)
            logger.info('<-- %s %s', old, new)        
        except Exception as e:
            self._handleException('rename', f'<-- {old} {new}', e)            
        finally:
            metrics.counts.endExecution('rename')

    def rmdir(self, path):  
        calledStats.rmdir += 1
        common.threadLocal.operation = 'rmdir' 
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('rmdir')
            logger.info('--> %s', path)   
            metrics.counts.incr('rmdir')
            rmdir.execute(path)
            logger.info('<-- %s', path)
        except Exception as e:
            self._handleException('rmdir', f'<-- {path}', e)
        finally:
            metrics.counts.endExecution('rmdir')

    def symlink(self, source, target): 
        calledStats.symlink += 1
        common.threadLocal.operation = 'symlink'
        common.threadLocal.path = (source, target)
        try:
            metrics.counts.startExecution('symlink')
            logger.info('--> %s %s', source, target)   
            metrics.counts.incr('symlink')        
            symlink.execute(source, None, target)
            logger.info('<-- %s %s', source, target) 
        except Exception as e:
            self._handleException('symlink', f'<-- {source} {target}', e)
        finally:
            metrics.counts.endExecution('symlink')

    def truncate(self, path, length, fh=None): 
        calledStats.truncate += 1
        common.threadLocal.operation = 'truncate'
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('truncate')
            logger.info('--> %s %d', path, length)  
            metrics.counts.incr('truncate') 
            d = metadata.cache.getattr(path)
            if d == None:
                raise FuseOSError(errno.ENOENT)
            truncate.execute(path, d['local_id'], length, d)
            logger.info('<-- %s %d', path, length)
        except Exception as e:
            self._handleException('truncate', f'<-- {path} {length}', e)
        finally:
            metrics.counts.endExecution('truncate')

    def unlink(self, path):  
        calledStats.unlink += 1 
        common.threadLocal.operation = 'unlink'
        common.threadLocal.path = (path,)
        try: 
            metrics.counts.startExecution('unlink')
            logger.info('--> %s', path)    
            metrics.counts.incr('unlink')
            remove.execute(path)            
            logger.info('<-- %s', path)        
        except Exception as e:
            self._handleException('unlink', f'<-- {path}', e)
        finally:
            metrics.counts.endExecution('unlink')

    def utimens(self, path, times=None):
        calledStats.utimens += 1
        common.threadLocal.operation = 'utimens'  
        common.threadLocal.path = (path,)
        try:
            metrics.counts.startExecution('utimens')
            logger.info('--> %s', path) 
            metrics.counts.incr('utimens')
            utime.execute(path,None, times)            
            logger.info('<-- %s', path)        
        except Exception as e:
            self._handleException('utimens', f'<-- {path}', e)
        finally:
            metrics.counts.endExecution('utimens')
        
    def write(self, path, buf, offset, fh): 
        calledStats.write += 1
        common.threadLocal.operation = 'write'
        common.threadLocal.path = (path,)
        try:  
            metrics.counts.startExecution('write')
            logger.info('--> %s size=%d offset=%d', path, len(buf), offset)
            metrics.counts.incr('write') 
            tupload.manager.write(path, buf, offset)              
            logger.info('<-- %s %d', path, len(buf))
            return len(buf)         
        except Exception as e:
            self._handleException('write', f'<-- {path} {offset}', e)            
        finally:
            metrics.counts.endExecution('write') 
