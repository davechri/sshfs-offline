cachefs
=======

Files are cached locally using the SSH protocol to connect to the remote host.
The cache can be accessed even when the network is down.

Features:

  - Based on FUSE (Filesystem in Userspace framework for Linux)

  - Multithreading: more than one request can be on it's way to the
    server

  - Metadata and Data are cached locally to improve performance.

  - Cached data can be accessed when the remote host is not reachable

  - Read/Write file system

Install Dependencies
====================

```sh
pip install -r requirements.txt
```

How to mount a filesystem
=========================

Usage:

    ```sh
    usage: cachefs.py [-h] [-p PORT] [-u USER] [-d REMOTEDIR] [--debug] [--cachetimeout CACHETIMEOUT] host mountpoint

    To unmount use: fusermount -u mountpoint

    positional arguments:
      host                  remote host name
      mountpoint            local mount point (eg, ~/mnt)

    options:
      -h, --help            show this help message and exit
      -p PORT, --port PORT  port number (default=22)
      -u USER, --user USER  user on remote host
      -d REMOTEDIR, --remotedir REMOTEDIR
                            directory on remote host (eg, ~/)
      --debug               run in debug mode
      --cachetimeout CACHETIMEOUT
                            duration in seconds to keep metadata cached (default is 5 minutes)

    ```

Example:

    ```sh
    ./cachefs.py localhost ~/mnt
    ```

Note, that it's recommended to run it as user, not as root.  For this
to work the mountpoint must be owned by the user.  If the username is
different on the host you are connecting to, then use the --user option.

If you need to enter a password cachefs will ask for it. 
You can also specify a remote directory using --remotedir.  The default
is your home directory.

The cache timeout defaults to 5 minutes, and can be set with the -cachetimeout option.

To unmount the filesystem:

    fusermount -u mountpoint

Cache Implementation
====================

The data and metadata are cached in the **.cachefs** directory.  In this example, the **test/myfile.txt** file has two 132K blocks.  The data is cached in the **data** sub-directory, and the metadata is cached in the **metadata** sub-directory.

```sh
➜  .cachefs
├── data
│   └── localhost   # host name
│       └── home
│           └── dave
│               └── test
│                   ├── myfile.txt-block0  # block 0 of test/myfile.txt
│                   └── myfile.txt-block1  # block 1 of test/myfile.txt
└── metadata
    └── localhost   # host name
        └── home
            └── user                
                ├── %test        # test direcotry
                │   ├── getattr  # lstat status for directory
                │   └── readdir  # directory entries
                ├── %test%myfile.txt  # test/myfile.txt file
                    └── getattr       # lstat status for file                

```

