def green(s):
    return f"\033[1m\033[32m{s}\033[00m"

def red(s):
    return f"\033[1m\033[31m{s}\033[00m"
    
def yellow(s):
    return f"\033[1m\033[33m{s}\033[00m"  
    
def lightPurple(s):
    return f"\033[1m\033[34m{s}\033[00m"

def purple(s):
    return f"\033[1m\033[35m{s}\033[00m"

def cyan(s):
    return f"\033[1m\033[36m{s}\033[00m"

def lightGray(s):
    return f"\033[1m\033[37m{s}\033[00m"

def black(s):
    return f"\033[1m\033[38m{s}\033[00m"

def reset(s):
    return f"\033[1m\033[0m{s}\033[00m"