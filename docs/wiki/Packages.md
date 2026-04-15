# Package Guide

## Directory Structure

```
cmd/pdrive/          CLI entry point and interactive TUI
internal/
  broker/            Chunk placement policies
  chunker/           File splitting and reassembly
  config/            TOML configuration file loading
  daemon/            Daemon process, HTTP API, background tasks
  engine/            Core file operations orchestrator
  fusefs/            FUSE filesystem (native kernel mount)
  junkfile/          Shared OS junk-file detection
  metadata/          SQLite database layer
  rclonebin/         Auto-download of rclone binary
  rclonerc/          rclone RC API client
  vfs/               WebDAV filesystem and local sync directory
```

## Package Details

### `cmd/pdrive`

| File        | Purpose                                                                                                                                                                                           |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `main.go`   | CLI entry point, daemon lifecycle, daemonize logic, PID file management                                                                                                                           |
| `cli.go`    | All client-side CLI commands (`ls`, `status`, `uploads`, `cat`, `get`, `rm`, `tree`, `find`, `mv`, `mkdir`, `info`, `du`, `remotes`, `health`, `metrics`). Uses HTTP calls to the running daemon. |
| `browse.go` | Interactive TUI file browser using Bubble Tea and Lip Gloss. Supports keyboard navigation, file preview, upload, download, delete, and rename.                                                    |

### `internal/broker`

Chunk placement policy engine. Given a chunk size, selects the best cloud provider based on available free space.

- **`Policy`** type — `PFRD` (Proportional Free Remaining Distribution) or `MFS` (Most Free Space)
- **`Broker.Pick(chunkSize)`** — Returns the chosen remote name
- **`ErrNoSpace`** — Returned when no provider has sufficient space

### `internal/config`

TOML configuration file loading from `~/.pdrive/config.toml`.

- **`File`** struct — All settings with TOML field tags (sync_dir, rclone_addr, mount_backend, mount_point, etc.)
- **`Load(configDir)`** — Reads and parses the config file; returns zero-value `File` if the file does not exist

### `internal/chunker`

File splitting, streaming, and reassembly.

| File           | Key Exports                                                                                                                                                                                                                                      |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `chunker.go`   | `Split()` — Splits an `io.Reader` into `[]Chunk` with SHA-256 hashes. `ChunkReader` — Streaming iterator that yields one chunk at a time (memory-efficient). `ChunkSizeForFile()` — Auto-sizes chunks (32–128 MB) targeting ~25 chunks per file. |
| `assembler.go` | `Assemble()` — Concatenates chunks into an `io.Reader`, verifying SHA-256.                                                                                                                                                                       |

### `internal/metadata`

SQLite database layer with WAL mode and embedded schema.

**Tables:** `providers`, `files`, `chunks`, `chunk_locations`, `directories`, `failed_deletions`, `activity_log`

Key query methods: `InsertFile`, `InsertChunk`, `SetChunkLocation`, `GetFileByPath`, `GetChunksByFileID`, `ListFiles`, `DeleteFile`, `RenameFile`, `FindDuplicate`, `ListOrphanChunks`, `LogActivity`, and more.

### `internal/engine`

Core orchestrator for all cloud operations.

**Key types:**

- `CloudStorage` interface — Abstracts rclone operations for testing
- `Engine` — Main struct with rate limiter, file gate, async upload WG, backup timer, telemetry counters
- `MetricsSnapshot` — Upload/download/delete/dedup counters

**Key constants:** `uploadRatePerSec=6`, `maxUploadWorkers=10`, `maxUploadRetries=5`, `AsyncWriteThreshold=4MB`

**Operations:** Upload (with dedup), Download, Delete, Rename, ListFiles, DiskUsage, StorageStatus, GarbageCollect, RetryFailedDeletions, VerifyFile

### `internal/daemon`

Daemon process that ties everything together.

**HTTP API endpoints:** `/api/ls`, `/api/status`, `/api/uploads`, `/api/remotes`, `/api/pin`, `/api/unpin`, `/api/health`, `/api/metrics`, `/api/download`, `/api/delete`, `/api/tree`, `/api/find`, `/api/mv`, `/api/mkdir`, `/api/info`, `/api/du`, `/api/upload`, `/api/verify`, `/api/activity`, `/api/resync`

**Background tasks:** Orphan GC (60 s after startup, then every 24 h), failed deletion retry (every hour), metadata backup (debounced 30 s), periodic provider re-sync from rclone (every 60 s)

### `internal/fusefs`

Native FUSE filesystem backed by the pdrive engine. Uses `hanwen/go-fuse/v2` with the `fs.InodeEmbedder` API.

| File        | Purpose                                                                                                                                                                                 |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fs.go`     | `Root` node (Lookup, Readdir, Getattr, Create, Mkdir, Unlink, Rmdir, Rename) and `fuseFileHandle` (Read, Write, Flush, Release). Writes are staged to temp files and uploaded on Flush. |
| `server.go` | `Server` lifecycle — `Mount(mountPoint, engine, spoolDir)` and `Unmount()`. Sets 5 s entry/attr timeouts, 128 KB ReadAhead.                                                             |
| `errors.go` | `toErrno(err)` — Maps Go errors to FUSE-compatible `syscall.Errno` values (ENOENT, EEXIST, EACCES, ENOTEMPTY, etc.).                                                                    |

### `internal/junkfile`

Shared detection of OS-generated junk files (`.DS_Store`, `._*` resource forks, `Thumbs.db`, `desktop.ini`). Used by both `daemon` and `vfs` packages to skip these files during sync and upload.

### `internal/rclonerc`

HTTP client for the rclone RC API at `127.0.0.1:5572`.

| File            | Purpose                                                                                                        |
| --------------- | -------------------------------------------------------------------------------------------------------------- |
| `client.go`     | Base HTTP client with JSON-RPC call method                                                                     |
| `operations.go` | `PutFile` (async copyfile + job polling), `GetFile` (download to temp), `DeleteFile`, `ListRemotes`, `ListDir` |
| `identity.go`   | `GetAccountEmail()` — Fetches email from Google/Dropbox/OneDrive/Box                                           |
| `quota.go`      | `QuotaCache` — Cached quota info per remote with 15-minute TTL                                                 |

### `internal/rclonebin`

Auto-downloads the rclone binary from `downloads.rclone.org` if not present locally. Detects OS and architecture for the correct download.

### `internal/vfs`

| File         | Purpose                                                                                                                                                          |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `webdav.go`  | `WebDAVFS` — Implements `webdav.FileSystem`. Uses temp files for both reads and writes to avoid memory issues with large files.                                  |
| `stub.go`    | Stub file utilities — Creates lightweight placeholder files with extended attributes (`user.pdrive.stub`, `user.pdrive.size`) for cloud-only files.              |
| `syncdir.go` | `SyncDir` — Watches a local directory via fsnotify. Debounces events (2 s), detects renames (500 ms window), creates stubs for cloud-only files on initial sync. |
