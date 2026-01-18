
import json
import os
import stat

from humanize import metric

from sshfs_offline import db, directories, gitignore, metrics

GETATTR = 'getattr'
READDIR = 'readdir'
READLINK = 'readlink'

class Memory:
    def __init__(self):
        self.map: dict[str, dict | list[str] | str | bytearray] = {}

    def keyToId(self, key: str):
        return key.split('-', 1)[1] 

    def initMemCache(self):        
        it = db.cache.getIterator()
        for key, value in it(fill_cache=False):
           key = str(key, 'utf-8')
           
           if key.startswith(GETATTR):
                metric.counts.incr('memcache_init_getattr')
                d = json.loads(value)
                self.map[key] = d 
                if d.get('file_name') == '.gitignore':
                    parentDirectory = directories.store.getDirectoryByLocalId(d['local_parent_id'])                    
                    path = parentDirectory.path + '/' if parentDirectory.path != '/' else '/'
                    path += d['file_name']  
                    gitignore.parser.addRepo(os.path.dirname(path), d)
                
           if key.startswith(READDIR):
                metrics.counts.incr('memcache_init_readdir')
                self.map[key] = json.loads(value)
           if key.startswith(READLINK):
                metrics.counts.incr('memcache_init_readlink')                
                self.map[key] = json.loads(value)
        
    def get(self, key: str, operation: str) -> dict | list[str] | str | bytearray | None:
        metrics.counts.incr(f'memcache_get_{operation}')
        value =  self.map.get(key)
        if value is None:
            metrics.counts.incr(f'memcache_get_{operation}_miss')
        return value

    def put(self, key: str, value: dict | list[str] | str | bytearray, operation: str) -> None:
        self.map[key] = value
        metrics.counts.incr(f'memcache_put_{operation}')

    def delete(self, key: str, operation: str) -> None:
        if key in self.map:
            del self.map[key]
            metrics.counts.incr(f'memcache_delete_{operation}')

    def exists(self, key: str) -> bool:
        return key in self.map

cache = Memory()