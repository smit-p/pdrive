# pdrive

[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Aggregate multiple cloud storage accounts into a single unified drive. Drop files into `~/pdrive` and they upload automatically — chunked and distributed across Google Drive, Dropbox, OneDrive, Box, or any provider [rclone](https://rclone.org) supports.

Two free Google accounts (15 GB each) + a free Dropbox account (2 GB) = **32 GB of unified storage** — accessible as a single folder, network drive, or FUSE mount.

## How It Works

```
~/pdrive/                         Cloud Storage
┌──────────────────┐              ┌─────────────────────────┐
│  report.pdf      │──┐           │  Google Drive            │
│  photos/         │  │           │    pdrive-chunks/a1b2..  │
│  video.mp4       │  ├─ chunk ──▶│    pdrive-chunks/c3d4..  │
│  ...             │  │  upload   │  Dropbox                 │
└──────────────────┘  │           │    pdrive-chunks/e5f6..  │
                      │           │  OneDrive                │
  SQLite metadata ◀───┘           │    pdrive-chunks/g7h8..  │
  (~/.pdrive/metadata.db)         └─────────────────────────┘
```

1. Files dropped into `~/pdrive` are detected via filesystem watcher (fsnotify)
2. Each file is split into dynamically-sized chunks (32 MB – 4 GiB, targeting ~25 chunks per file)
3. A broker distributes chunks across providers based on available free space
4. A local SQLite database tracks where every chunk lives
5. Cloud-only files appear locally as stub files (0-byte with xattrs)

pdrive uses rclone in RC daemon mode as the transport layer — any rclone-supported backend works.

## Features

- **Local sync folder** — `~/pdrive` works like Dropbox. Drop files in, they upload in the background. Delete or rename files locally, changes sync to cloud.
- **FUSE mount** — Native kernel-level filesystem mount via go-fuse. Full read/write support with temp-file staging and async uploads. Works with any application.
- **Content-hash dedup** — Files with identical SHA-256 share cloud chunks. No duplicate uploads.
- **Dynamic chunk sizing** — Small files get 32 MB chunks, large files up to 4 GiB, targeting ~25 chunks per file.
- **Erasure coding** — Optional Reed-Solomon parity shards (e.g. `3+1`) so files survive the loss of a cloud provider.
- **Pre-upload space validation** — Checks aggregate free space across all providers before uploading. Rejects files that won't fit with a clear error.
- **Stub files** — Cloud-only files appear as 0-byte stubs with xattrs marking them as cloud-only. Use `pdrive pin` to download on demand.
- **Full CLI** — List, upload, download, pin/unpin, move, search, and inspect storage from the terminal. No GUI needed.
- **Interactive TUI** — `pdrive browse` launches a full-screen file browser with keyboard navigation.
- **Browser UI** — Full-featured file manager at `http://localhost:8765` with dark/light mode. Browse directories, pin/unpin/delete/move/download files, create folders, search by pattern, view tree structure, monitor uploads, and inspect storage metrics — all from the browser.
- **WebDAV** — Mount as a network drive in Finder, Explorer, or any WebDAV client.
- **Auto-restart** — Run as a background daemon that auto-starts on login.
- **Live remote detection** — New rclone remotes are auto-detected every 60 seconds without a daemon restart. Or trigger immediately with `pdrive remotes add`.
- **Activity log** — All user-visible actions (uploads, downloads, deletes, moves, pins) are logged with timestamps.
- **Metadata backup** — SQLite DB is auto-backed up to every cloud provider. On a fresh install, the newest backup is auto-restored — just connect the same cloud accounts. Restored backups are validated against actual cloud chunks before use.
- **Interrupted upload resume** — Daemon restart picks up where it left off.
- **Orphan GC** — Periodic garbage collection removes cloud chunks with no DB record and purges DB records with missing cloud data.
- **Failed deletion retry** — Cloud deletions that fail are persisted and retried hourly (up to 10 attempts).
- **Partial upload cleanup** — If a multi-chunk upload fails partway through, successfully uploaded chunks are cleaned up automatically.
- **Rate-limit awareness** — Configurable API rate limiting with automatic backoff tripling on 429 responses.
- **Quota-exceeded detection** — Detects provider-specific quota/disk-full errors and skips retries for unrecoverable failures.
- **Config file** — Optional TOML configuration at `~/.pdrive/config.toml` for persistent settings. CLI flags override config values.

## Quick Start

### Install

**Homebrew (macOS):**

```bash
brew install smit-p/tap/pdrive
```

**Shell script (macOS / Linux):**

```bash
curl -fsSL https://raw.githubusercontent.com/smit-p/pdrive/main/install.sh | bash
```

**Go install (requires C toolchain for CGO):**

```bash
CGO_ENABLED=1 go install github.com/smit-p/pdrive/cmd/pdrive@latest
```

**From source:**

```bash
git clone https://github.com/smit-p/pdrive.git && cd pdrive
go build -o pdrive ./cmd/pdrive
```

Or download a pre-built binary from [GitHub Releases](https://github.com/smit-p/pdrive/releases).

> **Note:** rclone is required but will be **downloaded automatically** on first run if not already installed. To install it manually: `brew install rclone` or see [rclone.org/install](https://rclone.org/install/).

### Prerequisites

- **rclone** with at least one configured remote (`rclone config`) — auto-downloaded if missing

### Run

```bash
pdrive --debug
```

That's it. On first run, pdrive will:

1. Download rclone if not found on your system
2. Start rclone RC in the background
3. Create `~/pdrive` as the sync folder
4. Start the HTTP/WebDAV server at `localhost:8765`

### Install as a Service

Run the daemon in the background:

```bash
./pdrive
```

The daemon auto-detaches and runs in the background. Use `pdrive stop` to stop it.

## Usage

### Sync Folder

Drop files into `~/pdrive` — they upload automatically after a 2-second debounce:

```bash
cp movie.mp4 ~/pdrive/
# uploads in background, visible in: curl localhost:8765/api/uploads
```

Files in the cloud but not downloaded locally appear as stubs:

```bash
ls -la ~/pdrive/movie.mp4
# -rw-r--r--  0 B  (stub — cloud only)
```

Pin a file to download it locally:

```bash
pdrive pin /movie.mp4
```

Free up space by evicting the local copy:

```bash
pdrive unpin /movie.mp4
```

### Browser UI

Open `http://localhost:8765` in any browser. The web UI is a single-page app with full feature parity to the CLI:

- **Files** — Browse directories, sort by name/size/state/date, multi-select with bulk actions
- **File Info** — Click any file to see metadata, chunk locations, SHA-256 hash, and image previews
- **Actions** — Pin, unpin, delete, move/rename, download, create folders — all from the browser
- **Dashboard** — Health status, total file count, per-provider storage quotas with visual bars
- **Uploads** — Live upload progress with file name, speed, and percentage
- **Search** — Glob pattern search across all files (`*.pdf`, `report*`, etc.)
- **Tree** — Recursive directory tree view from any root
- **Metrics** — Upload/download/delete counters, chunk stats, dedup hits
- **Keyboard shortcuts** — `j`/`k` navigation, `/` search, `~` home, `i` info, `Space` select, `Escape` close
- **Responsive** — Works on mobile with collapsible sidebar
- **Dark/light mode** — Follows system `prefers-color-scheme`

### WebDAV Mount

```bash
# macOS — Finder → Go → Connect to Server
open http://localhost:8765

# Linux
mount -t davfs http://localhost:8765 /mnt/pdrive
```

### HTTP API

| Endpoint                   | Method | Description                                                 |
| -------------------------- | ------ | ----------------------------------------------------------- |
| `/api/ls?path=/`           | GET    | Directory listing with `local_state` (local/stub/uploading) |
| `/api/status`              | GET    | Total files, bytes, per-provider quotas                     |
| `/api/health`              | GET    | Uptime, DB status, in-flight uploads                        |
| `/api/uploads`             | GET    | In-flight upload progress                                   |
| `/api/metrics`             | GET    | Telemetry counters (files/chunks/bytes)                     |
| `/api/remotes`             | GET    | List configured remotes with enabled status                 |
| `/api/info?path=/file`     | GET    | File metadata, chunks, provider locations                   |
| `/api/tree?path=/`         | GET    | Recursive directory tree                                    |
| `/api/find?pattern=*.pdf`  | GET    | Glob search across all files                                |
| `/api/du?path=/`           | GET    | Disk usage summary for a directory                          |
| `/api/download?path=/file` | GET    | Download decrypted file content                             |
| `/api/activity`            | GET    | Recent activity log entries                                 |
| `/api/verify`              | GET    | Verify chunk integrity on cloud providers                   |
| `/api/pin?path=/file`      | POST   | Download cloud file to local                                |
| `/api/unpin?path=/file`    | POST   | Evict local data, replace with stub                         |
| `/api/delete?path=/file`   | POST   | Delete file from cloud and local                            |
| `/api/mv?src=/a&dst=/b`    | POST   | Move or rename a file                                       |
| `/api/mkdir?path=/dir`     | POST   | Create a new directory                                      |
| `/api/upload`              | POST   | Upload a file (multipart form)                              |
| `/api/upload/cancel`       | POST   | Cancel an in-progress upload                                |
| `/api/resync`              | POST   | Trigger immediate provider re-discovery from rclone         |
| `/api/logs`                | GET    | Recent daemon log entries                                   |
| `/api/logs/stream`         | GET    | Server-sent event stream of live log entries                |

## CLI Reference

pdrive has a full CLI for managing files without touching the browser or Finder:

```
pdrive                          Start the daemon (default)

Navigation:
  pdrive browse                   Interactive file browser (TUI)
  pdrive ls [path|number]         List files and directories
  pdrive tree [path]              Show directory tree recursively
  pdrive find <pattern> [path]    Search for files by name

File operations:
  pdrive cat <path|number>        Print file contents to stdout
  pdrive get <path|number> [dest] Download file to local filesystem
  pdrive put <local-path> [dir]   Upload local file or directory
  pdrive pin <path|number> [...]  Download cloud-only files locally
  pdrive unpin <path|number> [...] Evict local copies (keep in cloud)
  pdrive mv <src> <dst>           Move or rename files/directories
  pdrive rm <path|number> [...]   Delete files/directories from cloud
  pdrive mkdir <path>             Create a directory

Info:
  pdrive info <path|number>       Show detailed file metadata and chunks
  pdrive du [path]                Show disk usage summary
  pdrive status                   Show storage summary and provider quotas
  pdrive remotes                  List rclone remotes and which are enabled
  pdrive remotes add <name>       Enable a remote for pdrive
  pdrive remotes remove <name>    Disable a remote from pdrive
  pdrive remotes reset            Use all remotes (clear selection)
  pdrive uploads                  Show in-flight upload progress
  pdrive health                   Check daemon health
  pdrive metrics                  Show telemetry counters

Management:
  pdrive stop                     Stop the daemon
  pdrive mount [--mountpoint=PATH] Switch to FUSE backend
  pdrive unmount                  Unmount FUSE and stop the daemon
  pdrive version                  Print version information
  pdrive help                     Show all daemon flags
```

All subcommands talk to the running daemon over HTTP — you need the daemon running first (`pdrive` to start).

Use numbers from `ls` output as shorthand: `pdrive ls` → `pdrive cat 3`. Use `..` to go up a directory. Fuzzy matching works too: `pdrive cat vacation`.

### Remote Management

By default pdrive uses **all** configured rclone remotes. To limit which remotes are active, use the `remotes` subcommand — no daemon required:

```bash
pdrive remotes              # list all remotes and their enabled/disabled status
pdrive remotes add gdrive   # enable a remote
pdrive remotes remove gdrive # disable a remote
pdrive remotes reset        # reset to "use all remotes"
```

Selection is saved to `~/.pdrive/remotes.json` and loaded automatically on daemon start. You can also pass `--remotes gdrive,dropbox` on the command line for a one-off override.

#### Examples

```bash
# Browse your cloud files
pdrive ls /
pdrive ls /photos

# Check what's happening
pdrive status
pdrive uploads
pdrive health

# Download a file to your current directory
pdrive get /reports/2024-q4.pdf

# Download to a specific path
pdrive get /reports/2024-q4.pdf ~/Desktop/report.pdf

# Pipe file contents (useful for text files, scripts, etc.)
pdrive cat /notes.txt
pdrive cat /data.csv | head -20

# Manage local storage
pdrive pin /video.mp4         # download cloud file locally
pdrive unpin /video.mp4       # evict local copy, keep in cloud
```

| Flag               | Default          | Description                                                               |
| ------------------ | ---------------- | ------------------------------------------------------------------------- |
| `--config-dir`     | `~/.pdrive`      | Configuration directory (DB, spool, key)                                  |
| `--sync-dir`       | `~/pdrive`       | Local sync folder; empty disables sync                                    |
| `--webdav-addr`    | `127.0.0.1:8765` | HTTP/WebDAV listen address                                                |
| `--rclone-addr`    | `127.0.0.1:5572` | rclone RC address                                                         |
| `--rclone-bin`     | (auto-detected)  | Path to rclone binary                                                     |
| `--broker-policy`  | `pfrd`           | Placement policy: `pfrd` (weighted random) or `mfs` (most free space)     |
| `--min-free-space` | `256 MB`         | Minimum free bytes per provider                                           |
| `--chunk-size`     | `0` (dynamic)    | Override chunk size in bytes; 0 = dynamic (32 MB – 4 GiB)                 |
| `--rate-limit`     | `0`              | Cloud API calls per second (0 = unlimited)                                |
| `--erasure`        | (none)           | Reed-Solomon erasure coding (e.g. `3+1` = 3 data + 1 parity)              |
| `--skip-restore`   | `false`          | Skip restoring DB from cloud on startup                                   |
| `--remotes`        | (all)            | Comma-separated rclone remote names to use                                |
| `--foreground`     | `false`          | Run in foreground instead of backgrounding (for systemd/debugging)        |
| `--backend`        | `webdav`         | Mount backend: `webdav` (default) or `fuse`                               |
| `--mountpoint`     | (none)           | FUSE mount point (e.g. `/Volumes/pdrive`); required with `--backend=fuse` |
| `--debug`          | `false`          | Enable debug logging                                                      |

## Architecture

```
cmd/pdrive/           CLI entry, flags, signal handling, launchd install
internal/
  daemon/             Lifecycle: rclone subprocess, HTTP server, browser UI, periodic provider sync
  engine/             Core I/O: write/read/delete, chunked upload, DB sync, GC, space checks
  broker/             Chunk placement: assigns chunks to providers by free space
  chunker/            Split, hash (SHA-256), reassemble
  config/             TOML config file loading and defaults
  erasure/            Reed-Solomon erasure coding (data + parity shards)
  fusefs/             FUSE filesystem backend (go-fuse)
  junkfile/           Detection and purging of OS junk files (.DS_Store, Thumbs.db, etc.)
  logutil/            Structured logging helpers and WebSocket log streaming
  metadata/           SQLite schema, migrations, all CRUD queries
  rclonebin/          Auto-download and management of the rclone binary
  rclonerc/           HTTP client for rclone RC API, provider identity detection
  vfs/                WebDAV filesystem, sync folder watcher, stub files
scripts/              Service files (launchd, systemd) and E2E test scripts
web/                  Browser UI (HTML/CSS/JS single-page app) and Playwright E2E tests
```

### Database Schema (8 tables)

| Table              | Purpose                                     |
| ------------------ | ------------------------------------------- |
| `providers`        | Cloud accounts, quotas, rate-limit tracking |
| `files`            | Virtual file entries keyed by path          |
| `chunks`           | File pieces with SHA-256 hashes             |
| `chunk_locations`  | Maps chunks to cloud providers              |
| `directories`      | Explicit directory records                  |
| `failed_deletions` | Tracks failed cloud deletions for retry     |
| `activity_log`     | Timestamped log of user-visible actions     |
| `counters`         | Cumulative telemetry (bytes up/down, etc.)  |

### Key Paths

| Path                             | Purpose                            |
| -------------------------------- | ---------------------------------- |
| `~/.pdrive/metadata.db`          | SQLite metadata database           |
| `~/.pdrive/config.toml`          | Optional TOML configuration file   |
| `~/.pdrive/remotes.json`         | Persistent remote selection        |
| `~/.pdrive/daemon.pid`           | PID file for the background daemon |
| `~/.pdrive/spool/`               | Temp files for in-progress uploads |
| `~/pdrive/`                      | Local sync folder                  |
| Cloud: `pdrive-chunks/*`         | Chunk storage                      |
| Cloud: `pdrive-meta/metadata.db` | Metadata backup                    |

## Tests

```bash
# Unit tests
go test ./...

# Browser UI E2E tests (requires running daemon)
npm install
npx playwright install chromium
npx playwright test

# Python E2E tests
python3 scripts/test_e2e.py    # upload, download, delete
python3 scripts/test_dirs.py   # directory operations
python3 scripts/test_large.py  # large file chunking
```

The Playwright suite covers 82 tests across the web UI: layout, file browser, info panel, file actions (download/pin/unpin/delete/move/mkdir), dashboard, uploads, search, tree, metrics, keyboard shortcuts, navigation, toasts, and responsive layout.

## Dependencies

- [rclone](https://rclone.org) — cloud storage transport (RC daemon mode)
- [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) — pure-Go SQLite driver
- [golang.org/x/net/webdav](https://pkg.go.dev/golang.org/x/net/webdav) — WebDAV server
- [go-fuse](https://github.com/hanwen/go-fuse) — FUSE filesystem bindings
- [fsnotify](https://github.com/fsnotify/fsnotify) — filesystem event watcher
- [reedsolomon](https://github.com/klauspost/reedsolomon) — Reed-Solomon erasure coding
- [bubbletea](https://github.com/charmbracelet/bubbletea) — terminal UI framework (TUI browser)
- [go-toml](https://github.com/pelletier/go-toml) — TOML config file parser

## Contributing

Contributions are welcome! Please open an issue to discuss your idea before submitting a pull request.

```bash
git clone https://github.com/smit-p/pdrive.git && cd pdrive
go build ./...
go test ./...
```

## License

[MIT](LICENSE)
