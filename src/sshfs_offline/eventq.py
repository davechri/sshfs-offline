
import stat
import json

from sshfs_offline import common, commonfunc, db, log, metrics, metadata, gitignore
from sshfs_offline.log import logger
from sshfs_offline.remote import attr, chmod, mkdir, create, rename, symlink, truncate
from sshfs_offline.threads import tdelete, tupload, tcreate

FILE_EVENT = 'file'
DIR_EVENT = 'dir'
RENAME_EVENT = 'rename'
SYMLINK_EVENT = 'symlink'
CHMOD_EVENT = 'chmod'
TRUNCATE_EVENT = 'truncate'

EVENT_PREFIX = 'event'
EVENT_SEQ_NUM_KEY = '_eventseq'

eventCount = 0

class Key:
    def __init__(self, event: str, fromOperation: str, seqNum: int):
        self.event = event
        self.fromOperation = fromOperation
        self.seqNum = seqNum

class Value:
    def __init__(self, path: str, path2: str|None, localId: str, inode: int, failedCount: int, retryCount: int):
        self.path = path
        self.path2 = path2
        self.localId = localId
        self.inode = inode
        self.failedCount = failedCount
        self.retryCount = retryCount
        
class RetryException(Exception):
    def __init__(self, message: str):
        super().__init__(message)

class EventQueue:
    def init(self):
        seqNum = db.cache.get(EVENT_SEQ_NUM_KEY, EVENT_PREFIX) 
        if seqNum == None:
            seqNum = 0
        else:
            seqNum = int(str(seqNum, 'utf-8'))       
        self.seqNum = seqNum
        global eventCount
        it = db.cache.getIterator()
        for key, value in it(prefix=bytes(EVENT_PREFIX, 'utf-8')):
            eventCount += 1
        logger.info(f'init: seqNum={self.seqNum} eventCount={eventCount}')
    
    def key(self, event: str, fromOperation: str, seqNum: int) -> str:
        return f'{EVENT_PREFIX}:{seqNum:010d}:{event.ljust(10)}:{fromOperation.ljust(10)}'
    
    def enqueueFileEvent(self, path: str, localId: str, inode: int) -> None:
        self.enqueueEvent(path, None, localId, inode, FILE_EVENT)

    def enqueueDirEvent(self, path: str, localId: str, inode: int) -> None:
        self.enqueueEvent(path, None, localId, inode, DIR_EVENT)
    def enqueueRenameEvent(self, oldpath: str, newPath: str, localId: str, inode: int) -> None:
        if newPath is None:
            raise Exception(f'enqueueRenameEvent: {oldpath} newPath cannot be None')
        self.enqueueEvent(oldpath, newPath, localId, inode, RENAME_EVENT)
    def enqueueSymlinkEvent(self, path: str,  localId: str, inode: int) -> None:
        self.enqueueEvent(path, None, localId, inode, SYMLINK_EVENT)

    def enqueueChmodEvent(self, path: str, localId: str, inode: int) -> None:
        self.enqueueEvent(path, None, localId, inode, CHMOD_EVENT)
    def enqueueTruncateEvent(self, path: str, localId: str, inode: int) -> None:
        self.enqueueEvent(path, None, localId, inode, TRUNCATE_EVENT)

    def enqueueEvent(self, path: str, path2: str|None, localId: str, inode: int, event: str) -> None:
        fromOperation = common.threadLocal.operation
        metrics.counts.incr(f'enqueue_{fromOperation}_{event}_event')
        self.seqNum += 1
        logger.info(f'enqueue {event} event: {path} {path2} local_id={localId} inode={inode} seqNum={self.seqNum}')
        data = {
            'path': path,
            'path2': path2,
            'local_id': localId,
            'inode': inode
        }
        db.cache.put(self.key(event, fromOperation, self.seqNum), bytes(json.dumps(data), 'utf-8'), EVENT_PREFIX)
        db.cache.put(EVENT_SEQ_NUM_KEY, bytes(str(self.seqNum), 'utf-8'), EVENT_PREFIX)
        global eventCount
        eventCount += 1
    
    def parseKey(self, key: bytes) -> Key:
        key = str(key, 'utf-8')
        (_, seqNum, event, fromOperation) = key.split(':', 4)
        return Key(event.strip(), fromOperation.strip(), int(seqNum))
    
    def parseValue(self, value: bytes) -> Value:
        data = json.loads(value)
        path = data.get('path')
        path2 = data.get('path2')
        localId = data.get('local_id')
        inode = data.get('inode', 0)
        failedCount = data.get('failed_count', 0)
        retryCount = data.get('retry_count', 0)
        return Value(path, path2, localId, inode, failedCount, retryCount)
    
    def isEventQueuedForInode(self, inode: int) -> bool:
        it = db.cache.getIterator()
        for _, value in it(prefix=bytes(EVENT_PREFIX, 'utf-8')):
            data = json.loads(value)  
            inode2 = data.get('inode', 0)
            logger.debug(f'isEventQueuedForInode: checking inode={inode2} against inode={inode} {data}')
            if inode2 == inode:
                return True            
        return False

    def executeEvents(self) -> None:
        metrics.counts.incr('execute_events')
        logger.debug('executeEvents: start')
        saveOperation = common.threadLocal.operation
        savePath = common.threadLocal.path
        try:
            common.threadLocal.operation = 'eventq.py'
            common.threadLocal.path = None
            files: list[tuple[str, str]] = []
            it = db.cache.getIterator()
            for key, value in it(prefix=bytes(EVENT_PREFIX, 'utf-8')):
                key = str(key, 'utf-8')
                (_, seqNum, event, fromOperation) = key.split(':', 4)
                event = event.strip()
                fromOperation = fromOperation.strip()
                data = json.loads(value)
                path = data.get('path')
                path2 = data.get('path2')
                localId = data.get('local_id')
                inode = data.get('inode', 0)
                failedCount = data.get('failed_count', 0)  
                retryCount = data.get('retry_count', 0)              
                exception = None
                try:                   
                    common.threadLocal.path = (path,)                    
                    if event == FILE_EVENT:
                        common.threadLocal.operation = f'{fromOperation}_file_event'
                        self.executeFileEvent(path, localId, inode, seqNum, fromOperation)
                    elif event == DIR_EVENT:
                        common.threadLocal.operation = f'{fromOperation}_dir_event'
                        self.executeDirEvent(path, localId, inode, seqNum, fromOperation)  
                    elif event == RENAME_EVENT:  
                        op = fromOperation + '_' if fromOperation != 'rename' else ''
                        common.threadLocal.operation = f'{op}rename_event'
                        self.executeRenameEvent(path, path2,localId, inode, seqNum, fromOperation)               
                    elif event == SYMLINK_EVENT:
                        op = fromOperation + '_' if fromOperation != 'symlink' else ''
                        common.threadLocal.operation = f'{op}symlink_event'
                        self.executeSymlinkEvent(path, localId, inode, seqNum, fromOperation)  
                    elif event == CHMOD_EVENT:
                        op = fromOperation + '_' if fromOperation != 'chmod' else ''
                        common.threadLocal.operation = f'{op}chmod_event'
                        self.executeChmodEvent(path, localId, inode, seqNum, fromOperation)
                    elif event == TRUNCATE_EVENT:
                        op = fromOperation + '_' if fromOperation != 'truncate' else ''
                        common.threadLocal.operation = f'{op}truncate_event'
                        self.executeTruncateEvent(path, localId, inode, seqNum, fromOperation)   
                    else:
                        logger.error(f'executeEvents: unknown event {event} for path={path} local_id={localId} inode={inode} seqNum={seqNum}')
                except Exception as e:                    
                    raisedBy = commonfunc.exceptionRaisedBy(e) 
                    if isinstance(e, RetryException):
                        logger.warning(f'executeEvents retry exception: event={event} path={path} path2={path2} local_id={localId} inode={inode} seqNum={seqNum} {raisedBy}')    
                        metrics.counts.incr('eventqueue_retry_exception')                
                    else:                   
                        logger.exception(f'executeEvents exception: event={event} path={path} local_id={localId} inode={inode} seqNum={seqNum} {raisedBy}')
                        metrics.counts.incr('eventqueue_exception')
                    
                        exception = e                        
                finally:
                    if exception == None:
                        db.cache.delete(self.key(event, fromOperation, int(seqNum)), EVENT_PREFIX) 
                        global eventCount
                        eventCount -= 1
                    else:
                        metrics.counts.incr('eventqueue_requeue_event')
                        if isinstance(exception, RetryException):
                            retryCount += 1
                        else:
                            failedCount += 1
                        data = {
                            'path': path,
                            'path2': path2,
                            'local_id': localId,
                            'inode': inode,
                            'failed_count': failedCount,
                            'retry_count': retryCount
                        }
                        db.cache.put(self.key(event, fromOperation, int(seqNum)), bytes(json.dumps(data), 'utf-8'), EVENT_PREFIX)                               
        except Exception as e:
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'executeEvents: exception {raisedBy}')
            metrics.counts.incr('eventqueue_exception')            
        finally:
            common.threadLocal.operation = saveOperation
            common.threadLocal.path = savePath
    
    def executeDirEvent(self, path: str, localId: str, inode: int, seqNum, fromOperation) -> None:
        metrics.counts.incr(f'execute_{fromOperation}DirEvent')
        logger.info(f'execute: path={path} local_id={localId} inode={inode} seqNum={seqNum}')        
        d = metadata.cache.getattr(path, localId)
        if d != None:
            if d.get('local_only', False):
                logger.warning(f'execute: path={path} is local only, not creating on Google Drive')
            elif gitignore.parser.isIgnored(path):
                logger.info(f'execute: path={path} is gitignored, not creating on Google Drive')
                metadata.cache.changeToLocalOnly(path, localId, d)
            elif d.get('st_ino', 0) == 0:               
                logger.info(f'mkdir --> {path} local_id={localId}  inode={inode} mode={oct(d.get("st_mode", 0))}')
                metrics.counts.incr('execute_create_dir')
                mkdir.execute(path, d.get('st_mode', 0), runAsync=False)
                logger.info(f'mkdir <-- {path} local_id={localId} inode={inode}')
            else:
                logger.info(f'dir has inode - noop: {path} local_id={localId}')
        else: 
            if inode == 0: 
                metrics.counts.incr('execute_dir_was_deleted')
                logger.info(f'noop directory was deleted: {path} local_id={localId} inode={inode}')            
            else:
                d = metadata.cache.getattr(path)
                if d != None:
                    logger.info(f'directory still exists locally, not deleting: {path} local_id={localId} inode={inode}')
                    return
                logger.info(f'rmdir --> {path} local_id={localId} inode={inode}')
                metrics.counts.incr('execute_delete_dir')
                tdelete.manager.enqueue(path, localId=localId, inode=inode)
                logger.info(f'rmdir <-- {path} local_id={localId} inode={inode}') 

    def executeFileEvent(self, path: str, localId: str, inode: int, seqNum, fromOperation) -> None:
        metrics.counts.incr(f'execute_{fromOperation}FileEvent')
        logger.info(f'execute: path={path} local_id={localId} inode={inode} seqNum={seqNum}')
        d = metadata.cache.getattr(path, localId)
        if d != None:
            if d.get('local_only', False):
                logger.warning(f'execute: path={path} is local only, not creating on Google Drive')
            elif gitignore.parser.isIgnored(path):
                logger.info(f'execute: path={path} is gitignored, not creating on Google Drive')
                metadata.cache.changeToLocalOnly(path, localId, d)
            elif d.get('st_ino', 0) == 0:
                tcreate.manager.enqueue(path, localId)  
                return              
            else:
                logger.info(f'file has inode - noop: {path} local_id={localId} inode={d.get("st_ino")}')
            logger.info(f'flush --> {path} local_id={localId} inode={d.get("st_ino")}')
            metrics.counts.incr('execute_flush')
            size = tupload.manager.flush(path)
            logger.info(f'flush <-- {path} local_id={localId} inode={d.get("st_ino")} size={size}')
        else:
            if inode == 0:
                metrics.counts.incr('execute_file_was_deleted')
                logger.info(f'noop file was deleted or renamed: {path} local_id={localId}')
            else:
                d = metadata.cache.getattr(path)
                if d != None:
                    logger.info(f'file still exists locally, not deleting: {path} local_id={localId} inode={inode}')
                    return
                logger.info(f'delete --> {path} local_id={localId} inode={inode}')
                metrics.counts.incr('execute_delete_file')
                tdelete.manager.enqueue(path, localId=localId, inode=inode)
                logger.info(f'delete <-- {path} local_id={localId} inode={inode}')
    
    
    def executeRenameEvent(self, oldPath: str, newPath:str, localId: str, inode: int, seqNum, fromOperation) -> None:
        metrics.counts.incr(f'execute_{fromOperation}RenameEvent')
        logger.info(f'execute: oldPath={oldPath} newPath={newPath} local_id={localId} inode={inode} seqNum={seqNum}')        
        d = metadata.cache.getattr(newPath, localId)
        if d != None:
            if d.get('local_only', False):
                logger.warning(f'execute: newPath={newPath} is local only, not creating on Google Drive')               
            elif d.get('st_ino', 0) == 0:
                if tcreate.manager.pendingCreates.get(localId) != None:                    
                    raise RetryException(f'executeRenameEvent: create pending for local_id={localId}, retrying later')                
                    
                logger.info(f'execute: create non-existing file with new path {newPath} local_id={localId} inode={inode}')
                if d.get('st_mode') & stat.S_IFDIR:
                    self.executeDirEvent(newPath, localId, inode, seqNum, fromOperation)
                elif d.get('st_mode') & stat.S_IFREG == stat.S_IFREG:
                    self.executeFileEvent(newPath, localId, inode, seqNum, fromOperation)
                elif d.get('st_mode') & stat.S_IFLNK == stat.S_IFLNK:
                    self.executeSymlinkEvent(newPath, localId, inode, seqNum, fromOperation)
            else:
                path = attr.getPathFromStat(d)
                if newPath != path:
                    logger.error(f'executeRenameEvent: newPath {newPath} does not match {path} for local_id={localId} inode={inode}')
                    return
                if d.get('st_ino') == 0:
                    logger.error(f'executeRe/git/allproxy/.git/objects/pack/tmp_pack_bj1wIqnameEvent: inode is 0 local_id={localId} oldPath={oldPath} newPath={newPath} inode={d.get("st_ino")}')
                    return
                logger.info(f'rename --> old={oldPath} new={newPath} local_id={localId}  inode={d.get("st_ino")}')
                metrics.counts.incr('execute_rename')
                rename.doRename(oldPath, newPath, d)    
                logger.info(f'rename <-- old={oldPath} new={newPath} local_id={localId}  inode={d.get("st_ino")}')        
        else:            
            metrics.counts.incr('execute_rename_was_deleted')
            logger.info(f'noop rename was deleted: old={oldPath} new={newPath} local_id={localId} inode={inode}')
            
    def executeSymlinkEvent(self, path: str, localId: str, inode: int, seqNum, fromOperation) -> None:
        metrics.counts.incr(f'execute_{fromOperation}SymlinkEvent')
        logger.info(f'execute: path={path} local_id={localId} inode={inode} seqNum={seqNum}')
        d = metadata.cache.getattr(path, localId)
        if d != None:
            if d.get('local_only', False):
                logger.warning(f'execute: path={path} is local only, not creating on Google Drive')
            elif gitignore.parser.isIgnored(path):
                logger.info(f'execute: path={path} is gitignored, not creating on Google Drive')
                metadata.cache.changeToLocalOnly(path, localId, d)
            elif d.get('st_ino', 0) == 0:
                target = metadata.cache.readlink(path)
                symlink.execute(path, localId, target, runAsync=False)
            else:
                logger.info(f'symlink inode - noop: {path} local_id={localId} inode={d.get("st_ino")}')
        else:
            if inode == 0:
                metrics.counts.incr('execute_symlink_was_deleted')
                logger.info(f'noop symlink was deleted or renamed: {path} local_id={localId} inode={inode}')
            else:
                d = metadata.cache.getattr(path)
                if d != None:
                    logger.info(f'symlink still exists locally, not deleting: {path} local_id={localId} inode={d.get("st_ino")}')
                    return
                logger.info(f'remove --> {path} local_id={localId} inode={inode}')
                metrics.counts.incr('execute_delete_symlink')
                tdelete.manager.enqueue(path, localId=localId, inode=inode)
                logger.info(f'remove <-- {path} local_id={localId} inode={inode}')

    def executeChmodEvent(self, path: str, localId: str, inode: int, seqNum, fromOperation) -> None:
        metrics.counts.incr(f'execute_{fromOperation}ChmodEvent')
        logger.info(f'execute: path={path} local_id={localId} inode={inode} seqNum={seqNum}')
        d = metadata.cache.getattr(path, localId)
        if d != None:
            if d.get('local_only', False):
                logger.warning(f'execute: path={path} is local only, not changing mode on Google Drive')   
            logger.info(f'chmod --> {path} local_id={localId} inode={inode} mode={oct(d["st_mode"])}')
            metrics.counts.incr('execute_chmod')
            chmod.execute(path, localId, d["st_mode"], d, runAsync=False)
            logger.info(f'chmod <-- {path} local_id={localId} inode={inode}')
        else:
            logger.info(f'chmod inode - noop: {path} local_id={localId} inode={inode}')

    def executeTruncateEvent(self, path: str, localId: str, inode: int, seqNum, fromOperation) -> None:
        metrics.counts.incr(f'execute_{fromOperation}TruncateEvent')
        logger.info(f'execute: path={path} local_id={localId} inode={inode} seqNum={seqNum}')
        d = metadata.cache.getattr(path, localId)
        if d != None:
            if d.get('local_only', False):
                logger.warning(f'execute: path={path} is local only, not truncating on Google Drive')
            elif d.get('st_ino', 0) == 0:
                raise RetryException(f'executeTruncateEvent: path={path} local_id={localId} has no inode, retrying later')
            else:
                logger.info(f'truncate --> {path} local_id={localId} inode={inode} size={d.get("st_size")}')
                metrics.counts.incr('execute_truncate')
                truncate.execute(path, localId, d.get("st_size"), d, runAsync=False)
                logger.info(f'truncate <-- {path} local_id={localId} inode={inode}')
        else:
            logger.info(f'truncate inode - noop: {path} local_id={localId} inode={inode}')

queue = EventQueue()