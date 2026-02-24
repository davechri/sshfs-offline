import os
import traceback
import uuid

from sshfs_offline.log import logger  

API_TIMEOUT = 20

def apiTimeoutRange():
    return range(API_TIMEOUT, API_TIMEOUT*4, API_TIMEOUT) 
def isLastAttempt(timeout: int) -> bool:
    return timeout >= API_TIMEOUT*3

def generateLocalId(path: str, type: str, context: str, localOnly: bool) -> str:
    if localOnly:
        prefix = 'localonly-' + type
    else:
        prefix = 'local-' + type
    localId = f'{prefix}-{os.path.basename(path) if path != "/" else "root"}-{str(uuid.uuid4())}'
    logger.info('Generated localId=%s for path=%s type=%s in context=%s', localId, path, type, context)
    return localId

def isInLocalOnlyConfigLocalId(localId: str) -> bool:
    return localId.startswith('localonly-')

def isLocalIdDirectory(localId: str) -> bool:
    return localId.startswith('local-dir-') or localId.startswith('localonly-dir-')

def exceptionRaisedBy(e:Exception) -> str:
    traceback_details = traceback.extract_tb(e.__traceback__)
    # The last element in traceback_details corresponds to the point where the exception was raised.
    filename = os.path.basename(traceback_details[-1].filename)
    line_number = traceback_details[-1].lineno
    function_name = traceback_details[-1].name
    raised = f'raised by={filename}:{line_number} {function_name}() {e}'
    return raised