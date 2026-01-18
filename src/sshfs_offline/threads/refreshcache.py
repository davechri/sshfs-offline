
from sshfs_offline import directories, eventq, common
from sshfs_offline.remote import readdir, cnn
from sshfs_offline.log import logger

firstRefresh = True

def refreshAll() -> None:
    global firstRefresh
    logger.info('refreshcache.refreshAll: starting full refresh of cache')

    if common.offline:
        logger.info('refreshcache.refreshAll: skipping refresh due to offline mode')
        return
    if eventq.eventCount > 0:
        logger.info('refreshcache.refreshAll: stopping refresh due to queued events')
        return
    
    dirs = directories.store.getAllDirectories()
    script: list[str] = []
    for directory in dirs:
        if firstRefresh:
            readdir.execute(directory.path, deleteEntries=True)
            firstRefresh = False
            continue
        p = cnn.fixPath(directory.path) if directory.path != '/' else '.'
        script.append(f'find {p.replace(" ", "\\ ")} -maxdepth 1 -type d -cmin 1.5')
       
    if len(script) > 0:
        command = f'echo "{'\n'.join(script)}" > /tmp/sshfs-refresh; chmod +x /tmp/sshfs-refresh; /tmp/sshfs-refresh'
        logger.info(f'refreshcache.refreshAll: executing refresh script on remote server {command}')
        _, stdout, stderr = cnn.getConnection().ssh.exec_command(command)
        stderr = stderr.read().decode('utf-8') 
        if stderr != '':
            logger.error(f'refreshcache.refreshAll: error during refresh script execution: {stderr}')   
        else:
            logger.info('refreshcache.refreshAll: refresh script executed successfully')
            for line in stdout.read().decode('utf-8').splitlines():
                path = '/' + line.strip() if line != '.' else '/'
                logger.info(f'refreshcache.refreshAll: refreshing directory {path}')
                readdir.execute(path, deleteEntries=True)

            for line in stderr.splitlines():
                # find: ‘xxxx’: No such file or directory
                if 'No such file or directory' in line:
                   logger.info(f'refreshcache.refreshAll: directory not found during refresh: {line}')
                   path = line.split("'")[1]
                   readdir.execute('/' + path, deleteEntries=True)
    