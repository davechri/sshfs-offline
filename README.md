sshfs-offline
=============

SSH File System with offline access to cached files.

Features:

  - Based on FUSE (Filesystem in Userspace framework for Linux)

  - Metadata and Data are cached locally to improve performance.

  - Offline access to cached data when the remote host is not reachable

  - Read/Write file system

Install Script:
===============

```sh
$ pip install sshfs-offline

```

How to mount a filesystem
=========================

Usage:

    ```sh
    usage: sshfs-offline [-h] [-p PORT] [-u USER] [-d REMOTEDIR] [--debug] [--cachetimeout CACHETIMEOUT] host mountpoint

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
    sshfs-offline localhost ~/mnt
    ```

Note, that it's recommended to run it as user, not as root.  For this
to work the mountpoint must be owned by the user.  If the username is
different on the host you are connecting to, then use the --user option.

If you need to enter a password sshfs-offline will ask for it. 
You can also specify a remote directory using --remotedir.  The default
is your home directory.

The cache timeout defaults to 5 minutes, and can be set with the -cachetimeout option.

To unmount the filesystem:

    fusermount -u mountpoint

Cache Implementation
====================

The data and metadata are cached in the **.sshfs-offline** directory.  In this example, the **test/myfile.txt** file has two 132K blocks.  The data is cached in the **data** sub-directory, and the metadata is cached in the **metadata** sub-directory.

```sh
➜  .sshfs-offline
├── data
│   └── localhost   # host name
│       └── home
│           └── dave
│               └── test
│                   └── myfile.txt 
└── metadata
    └── localhost   # host name
        └── home
            └── user                
                ├── %test        # test direcotry
                │   ├── getattr  # lstat status for directory
                │   └── readdir  # directory entries
                └── %test%myfile.txt  # test/myfile.txt file
                    ├── blockmap      # track blocks that are cached
                    └── getattr       # lstat status for file 
```

Debugging
=========

* Metrics are logged to the **~/.sshfs-offline/metrics.log** file.
* In production  (--debug=False), the log level is set to **warning**, and logs are writtend to the **~/.sshfs-offline/error.log** file.
* If the --debug option is specified, the log level is set to **debug**, the process is run in the foreground, and logs are written to stdout.

Using the tail command to follow the metrics:
```sh
$ tail -f ~/.sshfs-offline
2025-09-23 07:26:19,237:INFO:metrics 
   getattr         : 181
   getattr_hit     : 181
   init            : 1
   readdir         : 26
   readdir_hit     : 26
   readlink        : 104
   readlink_hit    : 104
   sftp_chdir      : 1
   sftp_connected  : 1
   sftp_healthThread: 1
   sftp_start      : 1
```

Development
===========

Create Virtual Environment:

```sh
$ python3 -m venv my-venv-name
$ source ~/my-venv-name/bin/activate
$ pip install -r requirements.txt
```

Mount filesystem:

```sh
$ ./sshfs-offline/cli.py localhost ~/mnt

```

