# Configuration

## Prerequisites

- **Go 1.26.1+** (for building from source)
- **rclone** (auto-downloaded on first run if not present)
- At least one configured rclone remote (Google Drive, Dropbox, etc.)

## rclone Setup

pdrive uses rclone as the transport layer. Configure your cloud providers with rclone first:

```bash
# Configure a Google Drive remote
rclone config
# → Choose "Google Drive"
# → Follow OAuth flow
# → Name it something like "gdrive"

# Configure a Dropbox remote
rclone config
# → Choose "Dropbox"
# → Follow OAuth flow
# → Name it something like "dropbox"
```

The rclone config file is typically at `~/.config/rclone/rclone.conf`.

## pdrive Data Directory

All pdrive data is stored in `~/.pdrive/`:

```
~/.pdrive/
  config.toml      Optional TOML configuration (persistent settings)
  metadata.db      SQLite database (files, chunks, providers)
  daemon.pid       PID file for the running daemon
  spool/           Temp directory for WebDAV/FUSE read/write operations
  rclone           Auto-downloaded rclone binary (if needed)
```

## Sync Directory

pdrive creates a local sync directory at `~/pdrive/` by default. This directory:

- Mirrors your cloud files as lightweight stubs (zero-byte files with xattr metadata)
- Automatically syncs changes (create, modify, rename, delete) to the cloud via fsnotify
- Supports pinning (download to local) and unpinning (replace with stub)

## rclone Child Process

pdrive spawns rclone as a child process with RC (remote control) API enabled:

```
rclone rcd --rc-addr=127.0.0.1:5572 --rc-no-auth
  --drive-use-trash=false     # Don't use Google Drive trash
  --drive-chunk-size=256M     # Large upload chunks for speed
  --transfers=12              # Concurrent transfers
  --drive-pacer-min-sleep=10ms # Fast API pacing
```

The rclone process is automatically restarted if it crashes.

## Ports

| Service            | Address          | Purpose                 |
| ------------------ | ---------------- | ----------------------- |
| pdrive HTTP/WebDAV | `127.0.0.1:8765` | CLI API + WebDAV mount  |
| rclone RC          | `127.0.0.1:5572` | Internal rclone control |

Both listen only on localhost — not exposed to the network.

## Engine Tuning

These constants are currently hardcoded:

| Parameter             | Value | Description                                 |
| --------------------- | ----- | ------------------------------------------- |
| `uploadRatePerSec`    | 12    | Max upload API operations per second        |
| `uploadRateBurst`     | 20    | Burst allowance for rate limiter            |
| `maxUploadWorkers`    | 12    | Max concurrent chunk uploads                |
| `maxUploadRetries`    | 5     | Retry count before giving up                |
| `AsyncWriteThreshold` | 4 MB  | Files above this upload asynchronously      |

## Chunk Sizing

| Parameter              | Value  |
| ---------------------- | ------ |
| Default chunk size     | 32 MB  |
| Max chunk size         | 4 GiB  |
| Target chunks per file | ~25    |

Files smaller than 32 MB are stored as a single chunk. Larger files scale chunk size up dynamically (capped at 4 GiB) to keep chunk counts near the target.

## Config File

pdrive supports an optional TOML configuration file at `~/.pdrive/config.toml`. CLI flags always override config file values.

```toml
# Example config.toml
sync_dir       = "~/pdrive"
rclone_addr    = "127.0.0.1:5572"
webdav_addr    = "127.0.0.1:8765"
broker_policy  = "pfrd"
debug          = false

# FUSE mount settings
mount_backend  = "fuse"           # "webdav" (default) or "fuse"
mount_point    = "~/pdrive-fuse"  # FUSE mountpoint directory
```

All fields are optional. Unset fields use the built-in defaults.

## FUSE Mount

pdrive can expose your files as a native filesystem mount using FUSE. This provides a real directory (not WebDAV/network drive) that works with any application.

### Prerequisites

- **macOS:** Install [macFUSE](https://osxfuse.github.io/) or [FUSE-T](https://www.fuse-t.org/)
- **Linux:** Install `fuse3` (e.g., `sudo apt install fuse3` or `sudo dnf install fuse3`)

### Usage

```bash
# Start daemon with FUSE backend
pdrive --backend fuse --mountpoint ~/pdrive-fuse

# Or set it in config.toml and just run:
pdrive

# Mount/unmount independently
pdrive mount
pdrive unmount
```

### How it works

The FUSE layer translates kernel filesystem calls (open, read, write, mkdir, rename, etc.) into pdrive engine operations. Writes are staged to temp files in the spool directory and uploaded to the cloud on file close. Files larger than 4 MB are uploaded asynchronously so the close returns immediately.

## Service Installation

### macOS (LaunchAgent)

A LaunchAgent plist is provided in `scripts/com.pdrive.daemon.plist` to auto-start pdrive on login:

```bash
# Edit the plist to set paths and username if needed
vim scripts/com.pdrive.daemon.plist

# Install
cp scripts/com.pdrive.daemon.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.pdrive.daemon.plist
```

### Linux (systemd)

A systemd unit file is provided in `scripts/pdrive.service`:

```bash
# Edit the unit file for your environment
vim scripts/pdrive.service

# Install
cp scripts/pdrive.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now pdrive
```
