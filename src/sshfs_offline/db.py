import os

from typing import Optional

from sshfs_offline import common
from sshfs_offline import metrics
from sshfs_offline.log import logger

import plyvel

class Db:
    def __init__(self, clearcache: bool):       
        self.dbDir = os.path.join(common.dataDir, 'db')
        if clearcache:           
            if os.path.exists(self.dbDir):
                import shutil
                shutil.rmtree(self.dbDir)
        os.makedirs(self.dbDir, exist_ok=True)

        self.db = plyvel.DB(self.dbDir, create_if_missing=True)

    def close(self):
        metrics.counts.incr('db_close')     
        self.db.close()

    def put(self, key: str|bytes, value: bytes, operation: str) -> None:
        metrics.counts.incr(f'db_put_{operation}')      
        if isinstance(key, str):
            key = key.encode('utf-8')
        self.db.put(key, value)
    
    def get(self, key: str|bytes, operation: str) -> Optional[bytes]:
        metrics.counts.incr(f'db_get_{operation}')
        if isinstance(key, str):
            key = key.encode('utf-8')
        value = self.db.get(key)
        if value is None:         
            metrics.counts.incr(f'db_get_{operation}_miss')       
        return value

    def delete(self, key: str|bytes, operation: str) -> None:
        metrics.counts.incr(f'db_delete_{operation}')       
        if isinstance(key, str):
            key = key.encode('utf-8')
        self.db.delete(key)

    def getIterator(self):
        metrics.counts.incr('db_iterator')
        return self.db.iterator

cache: Db = None