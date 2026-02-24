import os
from gitignore_parser import parse_gitignore_str

from sshfs_offline import data, directories, metrics, stats
from sshfs_offline.log import logger
from sshfs_offline.remote import cnn

class GitIgnore:
    def __init__(self):
        self.gitignoreParsers: dict[str, callable] = {} # key=repoPath, value=parser function
        
    def addRepo(self, repoPath: str, d: dict[str,any]) -> None: 
        metrics.counts.incr('gitignore_loading')
        logger.info('gitignore.addRepo: loading .gitignore from %s', repoPath)  

        if repoPath in self.gitignoreParsers:
            metrics.counts.incr('gitignore_reload')
            logger.info('gitignore.addRepo: reloading .gitignore for repoPath=%s', repoPath)
            del self.gitignoreParsers[repoPath] # remove existing parser to force reload

        if d.get('st_size', 0) == 0:
            logger.info('gitignore.addRepo: .gitignore is empty for repoPath=%s', repoPath)
            return

        gitignorePath = os.path.join(repoPath, '.gitignore')
        def getDataCallback(size: int, offset: int) -> bytes:
                 stats.remoteStats.read += 1
                 with cnn.getConnection().sftp.open(cnn.fixPath(gitignorePath), 'r') as file:
                    file.seek(offset, 0)
                    data = file.read(size)
                    file.close()
                    logger.info('fetching remote data %s offset=%d size=%d return=%d', gitignorePath, offset, size, len(data))
                    return data      
               
        size = d.get('st_size', 0)
        buf = data.cache.read(gitignorePath, d.get('local_id'), 0, size, size,
                                  getDataCallback if not d.get('local_only', False) else None)
        logger.info('gitignore.addRepo: loaded .gitignore for %s, size=%d', 
                    gitignorePath, len(buf))
        parser = parse_gitignore_str(str(buf, 'utf-8'), base_dir=repoPath)
        self.gitignoreParsers[repoPath] = parser        
               
    def isIgnored(self, path: str) -> bool:
        parentDirectory = directories.store.getParentDirectory(path) 
        if parentDirectory != None and parentDirectory.localOnly:
            metrics.counts.incr('gitignore_match_parent')
            logger.info('gitignore.isIgnored: path=%s parent is localOnly', path)  
            return True       
        for repoPath, matches in self.gitignoreParsers.items(): 
            if path.startswith(repoPath+'/'):
                match = matches(path)
                if match:
                    metrics.counts.incr('gitignore_match')
                    logger.info('gitignore.isIgnored: path=%s matched repoPath=%s', path, repoPath)                
                return match
        return False
            
parser = GitIgnore()