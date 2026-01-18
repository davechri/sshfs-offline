

import json
import math
import os
import humanize
import stat
import threading
import time
import subprocess
from xmlrpc.server import SimpleXMLRPCServer
from datetime import datetime
from sshfs_offline import common, commonfunc, db, data, directories, eventq, filesystem, mem, metrics, metadata, localonly
from sshfs_offline.remote import attr
from sshfs_offline.log import logger
from sshfs_offline.threads import tdelete
from sshfs_offline.threads import tcreate, tdownload, tupload
from sshfs_offline.stats import calledStats, calledStatsSnapshot, remoteStats, remoteStatsSnapshot

class RpcServer:
    def __init__(self):
        self.server: SimpleXMLRPCServer | None = None

    def start(self):
        logger.info('Starting RPC server thread')
        threading.Thread(target=self.rpcServerThread, daemon=True).start() 

    def stop(self):
        self.server.shutdown() if self.server != None else None 

    def rpcServerThread(self):
        common.threadLocal.operation = 'rpcserver'
        common.threadLocal.path = None
        metrics.counts.incr('rpcserver_thread_started')
        try:
            logger.info(f'RPC.rpcServerThread starting on port {common.RPC_SERVER_PORT}')
            self.server = SimpleXMLRPCServer(("localhost", common.RPC_SERVER_PORT), allow_none=True)
            self.server.register_function(self.eventqueue, "eventqueue")
            self.server.register_function(self.metadata, 'metadata')
            self.server.register_function(self.directories, 'directories')
            self.server.register_function(self.unread, "unread")
            self.server.register_function(self.status, "status")
            self.server.serve_forever()
        except Exception as e:
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'RPC.rpcServerThread exception: {raisedBy}')
            metrics.counts.incr('rpcserver_thread_exception')

    def status(self) -> str:
        common.threadLocal.path = None
        metrics.counts.incr('rpc_status')             
        output: list[str] = []
        try:            
            class EventQueue:               
                totalEvents: int = 0
                failedCount: int = 0
                retryCount: int = 0
                eventTypes: dict[str,int] = dict()
            eventQueue = EventQueue()

            class Counts:
                dirs: int = 0
                files: int = 0
                links: int = 0
                fileBytes: int = 0
                cacheBytes: int = 0
                localOnly: int = 0           
            
            counts = {'local': Counts(), 'remote': Counts()}

            it = db.cache.getIterator()
            for key, value in it(prefix=bytes(mem.GETATTR, encoding='utf-8')):
                key = str(key, 'utf-8')                
                d = json.loads(value)
                mode = d.get('st_mode', 0)  
                localId = d.get('local_id')
                size = d.get('st_size', 0)
                path = attr.getPathFromStat(d)
                key = 'local' if d.get('local_only', False) else 'remote'
                if localId.find('file') != -1:
                    counts[key].files += 1
                    counts[key].fileBytes += size 
                    counts[key].cacheBytes += data.cache.getCachedFileSize(None, localId)
                    if mode & stat.S_IFLNK == stat.S_IFLNK:
                        logger.error(f'File cannot be a symlink: localId={localId} path={path} mode={oct(mode)}')
                elif (localId.find('dir') != -1):
                    counts[key].dirs += 1
                elif (localId.find('symlink') != -1):
                    counts[key].links += 1      
                    if mode & stat.S_IFLNK != stat.S_IFLNK:
                        logger.error(f'Symlink must have S_IFLNK set: localId={localId} path={path} mode={oct(mode)}')
                    
            for key, value in it(prefix=bytes(eventq.EVENT_PREFIX, encoding='utf-8')): 
                eventQueue.totalEvents += 1
                keyObj = eventq.queue.parseKey(key)
                eventKey = f'{keyObj.fromOperation}_{keyObj.event}' if keyObj.event not in keyObj.fromOperation else keyObj.fromOperation
                if eventKey not in eventQueue.eventTypes:
                    eventQueue.eventTypes[eventKey] = 0
                eventQueue.eventTypes[eventKey] += 1                
                valueObj = eventq.queue.parseValue(value)
                eventQueue.failedCount += valueObj.failedCount
                eventQueue.retryCount += valueObj.retryCount

            failedCount = ''
            if eventQueue.failedCount > 0:
                failedCount = f' failed={eventQueue.failedCount}'
            retryCount = ''
            if eventQueue.retryCount > 0:
                retryCount = f' retry={eventQueue.retryCount}'
            eventCounts = ''
            for eventType, count in eventQueue.eventTypes.items():
                eventCounts += f'{eventType}={count} '

            now = datetime.now().strftime('%H:%M:%S')
            state = 'OFFLINE' if common.offline else 'ONLINE'
            
            eventQueueStr = ''
            if eventQueue.totalEvents > 0 or eventQueue.failedCount > 0 or eventQueue.retryCount > 0:
                eventQueueStr = f'\n\tEVENT_QUEUE:  total={eventQueue.totalEvents}{failedCount}{retryCount} {eventCounts}'

            downloadStr = ''
            downloadExceptionsStr = ''
            if tdownload.manager.exceptionCount > 0:
                downloadExceptionsStr = f'exceptions={tdownload.manager.exceptionCount}'
            if tdownload.manager.activeThreadCount > 0 or tdownload.manager.downloadQueue.qsize() > 0 or tdownload.manager.exceptionCount > 0:
                downloadStr = f'\n\tDOWNLOAD:     qsize={tdownload.manager.downloadQueue.qsize()} active={tdownload.manager.activeThreadCount} {downloadExceptionsStr}'

            uploadStr = ''
            uploadExceptionsStr = ''
            if tupload.manager.exceptionCount > 0:
                uploadExceptionsStr = f'exceptions={tupload.manager.exceptionCount}'
            if tupload.manager.activeThreadCount > 0 or tupload.manager.uploadQueue.qsize() > 0 or tupload.manager.exceptionCount > 0:
                uploadStr = f'\n\tUPLOAD:       qsize={tupload.manager.uploadQueue.qsize()} active={tupload.manager.activeThreadCount} {uploadExceptionsStr}'

            tcreateStr = ''
            if tcreate.manager.activeThreadCount > 0 or tcreate.manager.queue.qsize() > 0 or tcreate.manager.exceptionCount > 0 or len(tcreate.manager.pendingCreates) > 0:
                tcreateExceptionsStr = ''
                if tcreate.manager.exceptionCount > 0:
                    tcreateExceptionsStr = f'exceptions={tcreate.manager.exceptionCount}'
                tcreateStr = f'\n\tCREATE:       qsize={tcreate.manager.queue.qsize()} active={tcreate.manager.activeThreadCount} pending={len(tcreate.manager.pendingCreates)} {tcreateExceptionsStr}'

            deleteStr = ''
            if tdelete.manager.activeThreadCount > 0 or tdelete.manager.queue.qsize() > 0 or tdelete.manager.exceptionCount > 0:
                deleteExceptionsStr = ''
                if tdelete.manager.exceptionCount > 0:
                    deleteExceptionsStr = f'exceptions={tdelete.manager.exceptionCount}'
                deleteStr = f'\n\tDELETE:       qsize={tdelete.manager.queue.qsize()} active={tdelete.manager.activeThreadCount} {deleteExceptionsStr}'

            filesystemStatsStr = ''
            stats = calledStats.capture(calledStatsSnapshot)
            statsFieldsStr = ''
            for key, value in stats.items():
                if value > 0:
                    statsFieldsStr += f'{key}={value} '
            if statsFieldsStr != '':
                filesystemStatsStr = f'\n\tOPERATIONS:   {statsFieldsStr}'

            remoteStatsStr = ''
            stats = remoteStats.capture(remoteStatsSnapshot)
            statsFieldsStr = ''
            for key, value in stats.items():
                if value > 0:
                    statsFieldsStr += f'{key}={value} '
            if statsFieldsStr != '':
                remoteStatsStr = f'\n\tREMOTE CALLS: {statsFieldsStr}'

            localDirsStr = ''
            result = subprocess.run(["du", "-s", localonly.lconfig.localonlyDir], capture_output=True, text=True)
            if result.stderr != '':
                output.append(f'du command failed: {result.stderr}') 
            else:                          
                size = result.stdout.split('/')[0]
                dir = result.stdout[len(size):].strip()
                if int(size) > 4:
                    localDirsStr = f'\n\tLOCAL_ONLY:   {dir}={humanize.naturalsize(int(size)*1024)}'

            localOnlyStr = ''
            if counts['local'].dirs > 0 or counts['local'].files > 0 or counts['local'].links > 0:
                localOnlyStr = f'\n\tGITIGNORE:    dirs={str(counts["local"].dirs):5} files={str(counts["local"].files):8} links={str(counts["local"].links):3}  cached={humanize.naturalsize(counts["local"].cacheBytes):8} fileBytes={humanize.naturalsize(counts["local"].fileBytes)}'

            remoteFsStr = f'\n\tREMOTE_FILES: dirs={str(counts["remote"].dirs):5} files={str(counts["remote"].files):8} links={str(counts["remote"].links):3}  cached={humanize.naturalsize(counts["remote"].cacheBytes):8}  fileBytes={humanize.naturalsize(counts["remote"].fileBytes)}'
                                                
            output.append(f'{now} {state}:{filesystemStatsStr}{remoteStatsStr}{eventQueueStr}{tcreateStr}{deleteStr}{downloadStr}{uploadStr}{localDirsStr}{localOnlyStr}{remoteFsStr}')

        except Exception as e:
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'RPC.status exception: {raisedBy}')
            metrics.counts.incr('rpc_status_exception')
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'<-- rpc_status: {raisedBy}')            
            output.append(str(e))
            output.append(f'See error details in {os.path.join(common.dataDir, "error.log")}')

        return output
    
    def eventqueue(self) -> str:        
        metrics.counts.incr('rpc_eventqueue')       
        output: list[str] = []
        try:
            totalEvents = 0           
            it = db.cache.getIterator()
            for key, value in it(prefix=bytes(eventq.EVENT_PREFIX, encoding='utf-8')):
                totalEvents += 1
                key = str(key, 'utf-8')
                d = json.loads(value)
                output.append(f'{key} {d}')              
            output.append(f'Events={totalEvents}')         
        except Exception as e:
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'RPC.eventqueue exception: {raisedBy}')
            metrics.counts.incr('rpc_eventqueue_exception')
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'<-- rpc_eventqueue: {raisedBy}')            
            output.append(str(e))
            output.append(f'See error details in {os.path.join(common.dataDir, "error.log")}')

        return output

    def metadata(self) -> str:       
        metrics.counts.incr('rpc_metadata')       
        output: list[str] = []
        try:
            totalFiles = 0
            totalDirectories = 0
            totalLinks = 0
            totalInvalid = 0
            it = db.cache.getIterator()
            for key, value in it(prefix=bytes(mem.GETATTR, encoding='utf-8')):
                key = str(key, 'utf-8')
                d = json.loads(value)                
                localId = d.get('local_id')
                mode = d.get('st_mode',0)
                path = attr.getPathFromStat(d)
                
                msg = '' if not localId in tdownload.manager.errorsByLocalId else f'ERROR: {tdownload.manager.errorsByLocalId[localId]}'
                
                output.append(f'{path} {key} {d} {msg}')
                
                if (mode & stat.S_IFLNK == stat.S_IFLNK):
                    totalLinks += 1
                elif (mode & stat.S_IFREG == stat.S_IFREG):
                    totalFiles += 1
                elif (mode & stat.S_IFDIR): 
                    totalDirectories += 1                
                else:
                    totalInvalid += 1

            for key, value in it(prefix=bytes(mem.READDIR, encoding='utf-8')):
                key = str(key, 'utf-8')
                d = json.loads(value)                
                localId = key.split('-', 1)[1]
                directory = directories.store.getDirectoryByLocalId(localId)
                path = None
                if directory != None:
                    path = directory.path
                                
                output.append(f'{path} {key} {d}')

            for key, value in it(prefix=bytes(mem.READLINK, encoding='utf-8')):
                key = str(key, 'utf-8')
                d = json.loads(value)                
                localId = key.split('-', 1)[1]
                d2 = metadata.cache.getattr_by_id(localId)  # Ensure path is set
                
                path = attr.getPathFromStat(d2) if d2 != None else None              
                                
                output.append(f'{path} {key} {d}')

            output.sort()

            output.append(f'TOTAL: files={totalFiles} directories={totalDirectories}, symlinks={totalLinks}')          
        except Exception as e:
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'RPC.metadata exception: {raisedBy}')
            metrics.counts.incr('rpc_metadata_exception')
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'<-- rpc_metadata: {raisedBy}')
            output.append(str(e))
            output.append(f'See error details in {os.path.join(common.dataDir, "error.log")}')

        return output

    def directories(self) -> str:        
        metrics.counts.incr('rpc_directories')    
        output: list[str] = []
        try:
            for dir in directories.store.getAllDirectories():                
                path = dir.path 
                
                output.append(f'ByPath: path={path} name={dir.name} inode={dir.inode} local_id={dir.localId} local_parent_id={dir.localParentId}')
                key = directories.store.key(dir.localId)
                dirBytes = db.cache.get(key, directories.DIRECTORY_PREFIX) 
                if dirBytes == None:
                    output.append(f'ERROR: Directory not found in db: {key}')
                    raise Exception(f'Directory not found in db: {key}')                
                dirByLocalId = directories.store.getDirectoryByLocalId(dir.localId)
                if dirByLocalId is not dir:
                    output.append(f'ByLocalId: path={dirByLocalId.path} name={dirByLocalId.name} inode={dirByLocalId.inode} local_id={dirByLocalId.localId} local_parent_id={dirByLocalId.localParentId}')
                    raise Exception(f'Directory lookup by localId failed for localId={dir.localId} path={path}')                    
          
        except Exception as e:
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'RPC.directories exception: {raisedBy}')
            metrics.counts.incr('rpc_directories_exception')
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'<-- rpc_directories: {raisedBy}')
            output.append(str(e))
            output.append(f'See error details in {os.path.join(common.dataDir, "error.log")}')

        return output
    
    def unread(self) -> str:
        
        metrics.counts.incr('rpc_unread')       
        output: list[str] = []
        try:
            totalFiles = 0
            totalBlocks = 0
            unreadFiles = 0            
            unreadBlocks = 0            

            it = db.cache.getIterator()
            for key, value in it(prefix=bytes(mem.GETATTR, encoding='utf-8')):
                key = str(key, 'utf-8')
                d = json.loads(value)
                mode = d.get('st_mode', 0)  
                localId = d.get('local_id')              
                if not (mode & stat.S_IFREG == stat.S_IFREG):
                    continue  
               
                path = attr.getPathFromStat(d)           
            
                totalFiles += 1                    
                size = d.get('st_size', 0)                    
                if size > 0:                        
                    unreadBlockCount = data.cache.getUnreadBlockCount(path, localId, d['st_size'])
                    if unreadBlockCount > 0:                            
                        unreadFiles += 1
                        unreadBlocks += unreadBlockCount

                        msg = f'UNREAD BLOCKS={unreadBlockCount}' if not localId in tdownload.manager.errorsByLocalId else f'ERROR: {tdownload.manager.errorsByLocalId[localId]}'
                        output.append(f'local_id={localId} path={path} {msg}')

                    totalBlocks += math.ceil(size/ common.BLOCK_SIZE)
            output.append(f'TOTAL: files={totalFiles} blocks={totalBlocks}, UNREAD: files={unreadFiles} blocks={unreadBlocks}')
    
        except Exception as e:
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'RPC.unread exception: {raisedBy}')
            metrics.counts.incr('rpc_unread_exception')
            raisedBy = commonfunc.exceptionRaisedBy(e)
            logger.exception(f'<-- rpc_unread: {raisedBy}')
            output.append(str(e))
            output.append(f'See error details in {os.path.join(common.dataDir, "error.log")}')

        return output

class RpcClient:
    def __init__(self):
        import xmlrpc.client
        self.client = xmlrpc.client.ServerProxy(f'http://localhost:{common.RPC_SERVER_PORT}/', allow_none=True)
    
    def eventqueue(self):
        output = self.client.eventqueue()
        for line in output:
            print(line+'\n')

    def unread(self):
        output = self.client.unread()
        for line in output:
            print(line+'\n')

    def metadata(self):
        output = self.client.metadata()
        for line in output:
            print(line+'\n')

    def directories(self):
        output = self.client.directories()
        for line in output:
            print(line+'\n')

    def status(self):
        while True:
            output = self.client.status()
            for line in output:
                line = line.replace('OFFLINE', '\033[1m\033[31mOFFLINE\033[00m')
                line = line.replace('ONLINE', '\033[1m\033[32mONLINE\033[00m')
                line = line.replace('retry', '\033[1m\033[33mretry\033[00m')
                line = line.replace('failed', '\033[1m\033[31mfailed\033[00m')
                line = line.replace('exceptions', '\033[1m\033[31mexceptions\033[00m')
                print(line)
            time.sleep(10)

server: RpcServer = RpcServer()
client: RpcClient = RpcClient()