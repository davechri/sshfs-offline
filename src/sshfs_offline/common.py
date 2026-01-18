import os
import uuid
from pathlib import Path
from enum import Enum
import traceback

import threading

TIMEOUT=10
UPDATE_INTERVAL = 60
BLOCK_SIZE = 131072
NUMBER_OF_FILE_READER_THREADS = 5
RPC_SERVER_PORT = 11111

host = ''
remotedir = ''
dataDir = ''
debug = False
verbose = False
pathfilter: str|None = None
updateinterval = UPDATE_INTERVAL
mountpoint = None
offline = False
xattrEnabled = False

threadLocal = threading.local()
