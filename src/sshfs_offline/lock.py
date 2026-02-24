
import threading

lockByPath: dict[str, threading.Lock] = {}

def get(path: str):
    lock = lockByPath.get(path)
    if lock is None: 
        lock = threading.Lock()           
        lockByPath[path] = lock      
    return lock
