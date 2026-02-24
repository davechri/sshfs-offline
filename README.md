# sshfs-offline

**sshfs-offline is a Linux or macOS network filesystem.**

sshfs-offline is a network filesystem that gives your computer direct access to your
files on a remote server.  sshfs-offline allows you to use files on your remote server as if they
were files on your local computer.
[text](../gdrive-filesys/README.md)
sshfs-offline performs an on-demand download of files when your computer
attempts to use them.  

## Key features

sshfs-offline has several nice features:

- **Only download files when you use them.** The deafult is to only
  download a file if you (or a program on your computer) uses that file. 

- **Bidirectional sync.** sshfs-offline will only download a file when you access a file
  that has been changed remotely. If you somehow simultaneously
  modify a file both locally on your computer and also remotely on your server,
  sshfs-offline will keep the most recently modified file.  The default is to check your remote server once every minute for any remotely changed files.

- **Can be used offline.** Files you've opened previously will be available even
  if your computer has no access to the internet.  While your computer is disconnected
  from the internet, you can continue to update the local filesystem and when you
  reconnect to the internet, the locally updated files will be uploaded to your server. 
    
- **Fast.** sshfs-offline asynchronously performs network requests to your server once every 10 seconds. sshfs-offline caches both filesystem metadata and file contents both in memory and on-disk.  Filesytem operations will always be fast unless a large file must be read from Google Drive.

- **gitignore files.** Files that match a `.gitignore` file are only cached locally and are
  not uploaded to your server.  This means that Python virtual environment packages in `.venv`
  and nodejs packages in `node_modules` are only installed locally.  
  
 **Local Only directories.** Directories can be configued to only store files locally in your native filesystem to improve small file performance.  The `~/.sshfs-offline/<host>/config.toml` file is used to configure local only directories.  The`node_modules` directory is configured as a local only directory so node packages are stored locally in your native filesystem, and `npm install` will run very fast.

 This is the default `config.toml' file:
 ```json
### This is the configuration file.
### It should be placed in $HOME/.sshfs-offline/<host>/config.toml

# Directory names that are only created localy and not synced to Google Drive.
# Files in local only directories are stored in the native filessystem for fast
# updates.  Install directories are good candidates to be stored locally.
local_only = [
    "node_modules",    
]
 ```
 
## Quick start

### Install FUSE

#### OSX

On Mac OSX, sshfs-offline requires [osxfuse](https://osxfuse.github.io/) and [pkg-config](http://macappstore.org/pkg-config/):

```bash
$ brew update; brew install pkg-config; brew tap homebrew/cask; brew install --cask osxfuse
```

#### Ubuntu / Debian

On Ubuntu / Debian, sshfs-offline requires [libfuse-dev](https://packages.ubuntu.com/disco/libfuse-dev), [libssl-dev](https://packages.ubuntu.com/disco/libssl-dev) and [pkg-config](https://packages.ubuntu.com/disco/pkg-config):

```bash
sudo apt-get install -y fuse3 libfuse-dev libssl-dev pkg-config
```

#### Fedora

On Fedora, sshfs-offline requires gcc, fuse3-devel, and pkg-config:

```bash
sudo dnf install -y gcc fuse3-devel pkg-config
```

### Authentication
SSH keys or password authentication are supported.

### Install sshfs-offline:

### MacOS

Install pipx:
```sh
brew install pipx
pipx ensurepath
sudo pipx ensurepath --global # optional to allow pipx actions with --global argument
pipx install sshfs-offline
```

#### Ubuntu / Debian

```sh
sudo apt update
sudo apt install pipx
pipx ensurepath
sudo pipx ensurepath --global # optional to allow pipx actions with --global argument
pipx install sshfs-offline
```

#### Fedora

```sh
sudo dnf install pipx
pipx ensurepath
sudo pipx ensurepath --global # optional to allow pipx actions with --global argument
pipx install sshfs-offline
```

### Mount Filesystem

Create mountpoint directory:
```sh
MOUNTPOINT=~/mnt
mkdir $MOUNTPOINT
```

Mount Filesystem:
```sh
sshfs-offline mount $MOUNTPOINT --host <your hostname> # add --downloadall to sync all files
```

Notes:
- It's recommended to run it as user, not as root.  For this to work the mountpoint must be owned by the user.
- The cache update interval defaults to 1 minutes, and can be set with the --updateinterval option.

### Unmount Filesystem

```sh
sshfs-offline unmount $MOUNTPOINT --host <your hostname>
```
or
```sh
fusermount -u $MOUNTPOINT
```

### Display sshfs-offline activity:

```sh
sshfs-offline status --host <your hostname>
```

## Building sshfs-offline yourself

Create Virtual Environment:

```sh
cd sshfs-offline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

Mount filesystem:

```sh
$ ./src/sshfs_offline/cli.py $MOUNTPOINT

```

### Running tests

The best way to test is to clone a repo, and install, build and run the application.

Mount Filesystem:
```sh
sshfs-offline mount $MOUNTPOINT
```

Build and run web application:
```bash
cd $MOUNTPOINT
git clone https://github.com/allproxy/allproxy.git
npm install
npm run build
npm start
```

Run python application:
```bash
cd $MOUNTPOINT
git clone https://github.com/davechri/sshfs-offline.git
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
./src/sshfs_offline/cli.py status
```

## Troubleshooting

If the filesystem appears to hang, its possible the fileystem has crashed. To resolve this:
- **Remount:** `fusermount -u $MOUNTPOINT` and remount `sshfs-offline mount $MOUNTPOINT  --host <your hostname>`
- **Clear the local cache:** `fusermount -u $MOUNTPOINT` and remount `sshfs-offline mount $MOUNTPOINT --clearcache --host <your hostname>`
- **Restart your computer**

### Logging

- **Error Log:** error logs are logged to `~/.sshfs-offline/error.log`

#### Debug Logging

For debug logging use the --debug option.  The --verbose option will produce more verbose logging.
```sh
sshfs-offline mount $MOUNTPOINT --host <your hostname> --debug &> ~/path_to_my_log_file
```

### Display sshfs-offline status

Filesystem statistics are displayed every 10 seconds.

```sh
sshfs-offline status
```
```sh
08:28:45 ONLINE:
	LOCAL_ONLY:   /home/dave/.sshfs-offline/home/localonly=1.0 GB
	REMOTE_FILES: dirs=526   files=934      links=17   cached=13.1 MB   fileBytes=1.1 GB
08:28:57 ONLINE:
	LOCAL_ONLY:   /home/dave/.sshfs-offline/home/localonly=1.0 GB
	REMOTE_FILES: dirs=526   files=934      links=17   cached=13.1 MB   fileBytes=1.1 GB
08:29:09 ONLINE:
	REMOTE CALLS: readdir=1 
	LOCAL_ONLY:   /home/dave/.sshfs-offline/home/localonly=1.0 GB
	REMOTE_FILES: dirs=564   files=941      links=20   cached=13.1 MB   fileBytes=1.1 GB
```

### Dump Internal State

Display metadata:
```sh
sshfs-offline metadata --host <your hostname>
```

Display directory entries:
```sh
sshfs-offline directories --host <your hostname>
```

Diaplay all unread files:
```sh
sshfs-offline unread --host <your hostname>
```

Display the event queue:
```sh
sshfs-offline eventqueue --host <your hostname>
```



