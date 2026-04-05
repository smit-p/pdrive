# pdrive

Aggregate multiple cloud storage accounts into a single unified drive. Drop files into `~/pdrive` and they upload automatically — encrypted, chunked, and distributed across Google Drive, Dropbox, OneDrive, or any provider [rclone](https://rclone.org) supports.

## How It Works

```
~/pdrive/                         Cloud Storage
┌──────────────────┐              ┌─────────────────────────┐
│  report.pdf      │──┐           │  Google Drive            │
│  photos/         │  │  encrypt  │    pdrive-chunks/a1b2..  │
│  video.mp4       │  ├─ chunk ──▶│    pdrive-chunks/c3d4..  │
│  ...             │  │  upload   │  Dropbox                 │
└──────────────────┘  │           │    pdrive-chunks/e5f6..  │
                      │           │  OneDrive                │
  SQLite metadata ◀───┘           │    pdrive-chunks/g7h8..  │
  (~/.pdrive/metadata.db)         └─────────────────────────┘
```

1. Files dropped into `~/pdrive` are detected via filesystem watcher (fsnotify)
2. Each file is split into dynamically-sized chunks (32–128 MB, targeting ~25 chunks per file)
3. Chunks are encrypted with AES-256-GCM
4. A broker distributes chunks across providers based on available free space
5. A local SQLite database tracks where every chunk lives
6. Cloud-only files appear locally as stub files (0-byte with xattrs)

pdrive uses rclone in RC daemon mode as the transport layer — any rclone-supported backend works.

## Features

- **Local sync folder** — `~/pdrive` works like Dropbox. Drop files in, they upload in the background. Delete or rename files locally, changes sync to cloud.
- **Encryption at rest** — AES-256-GCM with password-based key derivation (Argon2id). Set a password on first run and use it on any machine. Legacy raw key files (`~/.pdrive/enc.key`) still supported.
- **Content-hash dedup** — Files with identical SHA-256 share cloud chunks. No duplicate uploads.
- **Dynamic chunk sizing** — Small files get 32 MB chunks, large files up to 128 MB, keeping API call count low.
- **Stub files** — Cloud-only files appear as 0-byte stubs with xattrs marking them as cloud-only. Use `pdrive pin` to download on demand.
- **Full CLI** — List, download, pin/unpin, and inspect storage from the terminal. No GUI needed.
- **Interactive TUI** — `pdrive browse` launches a full-screen file browser with keyboard navigation.
- **Browser UI** — Navigate files at `http://localhost:8765` with a clean dark/light mode interface. Click to download.
- **WebDAV** — Mount as a network drive in Finder, Explorer, or any WebDAV client.
- **Auto-restart** — Run as a background daemon that auto-starts on login.
- **Metadata backup** — SQLite DB is encrypted (AES-256-GCM) and auto-backed up to every cloud provider. The Argon2id salt is stored alongside the backup so the same password works on any machine. On a fresh install, the newest backup is auto-restored — just connect the same cloud accounts and enter your password.
- **Interrupted upload resume** — Daemon restart picks up where it left off.
- **Orphan GC** — Periodic garbage collection removes cloud chunks with no DB record and purges DB records with missing cloud data.
- **Failed deletion retry** — Cloud deletions that fail are persisted and retried hourly (up to 10 attempts).
- **Rate-limit awareness** — Configurable API rate limiting with automatic backoff tripling on 429 responses.

## Quick Start

### Prerequisites

- **Go 1.21+**
- **rclone** with at least one configured remote (`rclone config`)

### Build & Run

```bash
go build -o pdrive ./cmd/pdrive
./pdrive --debug
```

That's it. On first run, pdrive will:

1. Prompt you to set an encryption password (Argon2id → AES-256)
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

Open `http://localhost:8765` in any browser. Navigate directories, click files to download, and view storage status — all with dark mode support.

### WebDAV Mount

```bash
# macOS — Finder → Go → Connect to Server
open http://localhost:8765

# Linux
mount -t davfs http://localhost:8765 /mnt/pdrive
```

### HTTP API

| Endpoint                | Method | Description                                                 |
| ----------------------- | ------ | ----------------------------------------------------------- |
| `/api/status`           | GET    | Total files, bytes, per-provider quotas                     |
| `/api/uploads`          | GET    | In-flight upload progress                                   |
| `/api/ls?path=/`        | GET    | Directory listing with `local_state` (local/stub/uploading) |
| `/api/pin?path=/file`   | POST   | Download cloud file to local                                |
| `/api/unpin?path=/file` | POST   | Evict local data, replace with stub                         |
| `/api/health`           | GET    | Uptime, DB status, in-flight uploads                        |
| `/api/metrics`          | GET    | Telemetry counters (files/chunks/bytes)                     |

## CLI Reference

pdrive has a full CLI for managing files without touching the browser or Finder:

```
pdrive                          Start the daemon (default)
pdrive browse                   Interactive file browser (TUI)
pdrive ls [path]                List files and directories
pdrive status                   Show storage summary and provider quotas
pdrive uploads                  Show in-flight upload progress
pdrive health                   Check daemon health
pdrive metrics                  Show telemetry counters
pdrive pin <path> [path...]     Download cloud-only file to local
pdrive unpin <path> [path...]   Evict local data, replace with stub
pdrive cat <path>               Print file contents to stdout
pdrive get <path> [dest]        Download file to local filesystem
pdrive stop                     Stop the daemon
pdrive help                     Show CLI usage
```

All subcommands talk to the running daemon over HTTP — you need the daemon running first (`pdrive` to start).

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

| Flag               | Default          | Description                                                           |
| ------------------ | ---------------- | --------------------------------------------------------------------- |
| `--config-dir`     | `~/.pdrive`      | Configuration directory (DB, spool, key)                              |
| `--sync-dir`       | `~/pdrive`       | Local sync folder; empty disables sync                                |
| `--webdav-addr`    | `127.0.0.1:8765` | HTTP/WebDAV listen address                                            |
| `--rclone-addr`    | `127.0.0.1:5572` | rclone RC address                                                     |
| `--rclone-bin`     | (auto-detected)  | Path to rclone binary                                                 |
| `--password`       | (interactive)    | Encryption password (derives AES-256 key via Argon2id)                |
| `--enc-key`        | (none)           | Legacy: 64-char hex AES-256 key; prefer `--password`                  |
| `--broker-policy`  | `pfrd`           | Placement policy: `pfrd` (weighted random) or `mfs` (most free space) |
| `--min-free-space` | `256 MB`         | Minimum free bytes per provider                                       |
| `--chunk-size`     | `0` (dynamic)    | Override chunk size in bytes; 0 = dynamic (32–128 MB)                 |
| `--rate-limit`     | `8`              | Cloud API calls per second                                            |
| `--skip-restore`   | `false`          | Skip restoring DB from cloud on startup                               |
| `--debug`          | `false`          | Enable debug logging                                                  |

## Architecture

```
cmd/pdrive/           CLI entry, flags, signal handling, launchd install
internal/
  daemon/             Lifecycle: rclone subprocess, HTTP server, browser UI
  engine/             Core I/O: write/read/delete, chunked upload, DB sync, GC
  broker/             Chunk placement: assigns chunks to providers by free space
  chunker/            Split, encrypt (AES-256-GCM), decrypt, reassemble
  metadata/           SQLite schema, migrations, all CRUD queries
  rclonerc/           HTTP client for rclone RC API
  vfs/                WebDAV filesystem, sync folder watcher, stub files
scripts/              E2E test scripts
```

### Database Schema (6 tables)

| Table              | Purpose                                     |
| ------------------ | ------------------------------------------- |
| `providers`        | Cloud accounts, quotas, rate-limit tracking |
| `files`            | Virtual file entries keyed by path          |
| `chunks`           | File pieces with SHA-256 hashes             |
| `chunk_locations`  | Maps chunks to cloud providers              |
| `directories`      | Explicit directory records                  |
| `failed_deletions` | Tracks failed cloud deletions for retry     |

### Key Paths

| Path                                 | Purpose                                       |
| ------------------------------------ | --------------------------------------------- |
| `~/.pdrive/metadata.db`              | SQLite metadata database                      |
| `~/.pdrive/enc.salt`                 | Argon2id salt for password-derived key        |
| `~/.pdrive/enc.key`                  | Legacy AES-256 key (raw 32 bytes)             |
| `~/.pdrive/spool/`                   | Temp files for in-progress uploads            |
| `~/pdrive/`                          | Local sync folder                             |
| Cloud: `pdrive-chunks/*`             | Encrypted chunk storage                       |
| Cloud: `pdrive-meta/metadata.db.enc` | Encrypted metadata backup                     |
| Cloud: `pdrive-meta/enc.salt`        | Argon2id salt (for multi-machine portability) |

## Tests

```bash
go test ./...              # 136 unit tests
python3 scripts/test_e2e.py    # E2E: upload, download, delete
python3 scripts/test_dirs.py   # directory operations
python3 scripts/test_browser.py # browser UI
python3 scripts/test_large.py  # large file chunking
```

## Dependencies

- [rclone](https://rclone.org) — cloud storage transport (RC daemon mode)
- [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) — pure-Go SQLite driver
- [golang.org/x/net/webdav](https://pkg.go.dev/golang.org/x/net/webdav) — WebDAV server
- [fsnotify](https://github.com/fsnotify/fsnotify) — filesystem event watcher

## License

MIT
