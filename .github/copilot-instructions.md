# pdrive — Copilot Instructions

## Project Overview

pdrive is a Go daemon that aggregates multiple cloud storage accounts (Google Drive, Dropbox, etc.) into a single unified virtual drive. Files are split into dynamically-sized chunks (32–128 MB based on file size), encrypted with AES-256-GCM, and distributed across cloud providers via rclone's RC API. A local SQLite database tracks all metadata. Users interact through a WebDAV server (mountable in Finder/Explorer), a browser UI, a local sync folder (`~/pdrive/`) with stub files for cloud-only entries, an interactive TUI (`pdrive browse`), or a full CLI.

## Architecture

```
cmd/pdrive/main.go           — CLI entry point, flags, signal handling
cmd/pdrive/browse.go         — Interactive TUI file browser (bubbletea)
internal/daemon/              — Lifecycle orchestrator, rclone subprocess, HTTP API, browser handler
internal/engine/              — File I/O coordinator: write/read/delete, dirs, DB cloud sync, rate limiting
internal/engine/dbsync.go     — Debounced metadata backup, orphan chunk GC
internal/chunker/             — Split (4 MB), encrypt/decrypt (AES-256-GCM), assemble with SHA-256
internal/broker/              — Assigns chunks to the provider with the most free space (PFRD/MFS)
internal/metadata/            — SQLite schema, DB lifecycle (WAL mode), all CRUD queries
internal/rclonerc/            — HTTP client for rclone RC API (upload, download, delete, list, quota)
internal/vfs/                 — WebDAV filesystem (golang.org/x/net/webdav)
internal/vfs/syncdir.go       — Local sync folder: fsnotify watcher, stub files, pin/unpin
scripts/                      — E2E test scripts (Python), rclone download helper
```

## Key Design Decisions

- **Directories are hybrid**: Explicit records in a `directories` table + implicit from file paths. Both must be considered in `IsDir`/`ListDir`/`ListSubdirectories`.
- **Chunks live on cloud, metadata lives locally**: `metadata.db` is the source of truth. It's auto-backed-up (encrypted with AES-256-GCM) to all providers at `pdrive-meta/metadata.db.enc` and auto-restored on fresh installs. Backups include a nanosecond timestamp; restore picks the newest copy across all remotes. The Argon2id salt is also backed up to cloud (`pdrive-meta/enc.salt`) so the same password derives the same key on any machine. Legacy unencrypted backups (`pdrive-meta/metadata.db`) are supported for backward compatibility.
- **Content-hash dedup**: Files with identical SHA-256 share cloud chunks (cloned via `cloneFileFromDonor`). `deleteCloudChunks` checks `RemotePathRefCount` before deleting shared objects.
- **Dynamic chunk sizing**: `ChunkSizeForFile` targets ~25 chunks per file (32 MB min, 128 MB max). Overridable via `--chunk-size` flag or `SetChunkSize()`.
- **rclone RC quirks**: `operations/uploadfile` takes `fs`/`remote` as query params (not multipart fields), and `remote` is the parent directory (not the full path). Use `operations/copyfile` for downloads (not `operations/cat`).
- **Password-based encryption (Argon2id)**: Users set a password on first run; `DeriveKey(password, salt)` in `chunker/crypto.go` uses Argon2id (3 iterations, 64 MB, 4 threads) to produce a 32-byte AES-256 key. The 16-byte salt is stored locally at `~/.pdrive/enc.salt` and uploaded to cloud alongside the DB backup. On a new machine, the daemon downloads the cloud salt before deriving the key. Legacy raw key files (`~/.pdrive/enc.key`) and `--enc-key` hex flag are still supported for backward compatibility.
- **Streaming reads (OOM prevention)**: `ReadFileToTempFile` downloads chunks to a temp file sequentially. Peak memory stays bounded to one chunk (~32–128 MB) regardless of file size. WebDAV, browser, and pin operations all use this path.
- **Transactional DB writes**: `insertChunkMetadata` and `cloneFileFromDonor` wrap all inserts in a single SQLite transaction for atomicity.
- **Failed deletion persistence**: When `deleteCloudChunks` fails to delete a cloud chunk, the failure is persisted in a `failed_deletions` table. A background goroutine retries every hour (max 10 attempts).
- **Upload progress cleanup**: The `uploads` map entry is cleaned up via `defer` when the async upload goroutine completes (success or failure), preventing memory leaks.
- **Chunk sequence validation**: Both the assembler and `ReadFileToTempFile` validate that chunk sequences are contiguous (0, 1, 2, ..., n-1) before reassembly.
- **Stub files**: Cloud-only files are represented locally as 0-byte files with xattrs (`com.pdrive.stub`, `com.pdrive.size`).
- **Event suppression**: `suppressEvent(path)` must be called BEFORE any filesystem write that the fsnotify watcher should ignore. The watcher checks and clears the suppress map on each event.
- **Engine lifecycle**: `NewEngine`/`NewEngineWithCloud` start a rate-limit refill goroutine. Always call `engine.Close()` to stop it (idempotent via `closeCh`).
- **Token bucket rate limiting**: `uploadTokens` channel (burst=16 prod, 256 test), refilled at configurable rate (default 8 tokens/sec). All cloud API calls (including deletes) must consume a token first. Use `--rate-limit` flag to override.
- **Retry with jitter**: Failed chunk uploads retry up to 5 times with exponential backoff (1s, 2s, 4s, ... capped at 30s) plus up to 50% random jitter. Rate-limited errors (HTTP 429) trigger triple backoff.
- **Graceful shutdown**: `engine.Close()` waits up to 30s for in-flight async uploads via `asyncWG`.
- **Telemetry counters**: Atomic counters for files/chunks/bytes uploaded/downloaded, dedup hits, and deletes. Exposed via `/api/metrics`.
- **Upload progress tracking**: In-flight async uploads are tracked in `uploads` map and exposed via `/api/uploads`.

## CLI Subcommands & Flags

```
pdrive daemon [flags]         — Run the daemon (default)
pdrive browse                 — Interactive TUI file browser
pdrive pin <path> [path...]   — Download cloud-only file to local (HTTP API call)
pdrive unpin <path> [path...] — Evict local data, replace with stub (HTTP API call)
```

Key flags: `--config-dir` (~/.pdrive), `--sync-dir` (~/pdrive), `--rclone-addr` (127.0.0.1:5572),
`--webdav-addr` (127.0.0.1:8765), `--password`, `--enc-key` (legacy), `--broker-policy` (pfrd|mfs), `--rate-limit` (default 8), `--debug`

## HTTP API Endpoints

| Endpoint           | Method | Description                                                 |
| ------------------ | ------ | ----------------------------------------------------------- |
| `/api/status`      | GET    | Total files, bytes, provider quotas                         |
| `/api/uploads`     | GET    | In-flight upload progress                                   |
| `/api/ls?path=`    | GET    | Directory listing with `local_state` (local/stub/uploading) |
| `/api/pin?path=`   | POST   | Pin (download) a cloud-only file                            |
| `/api/unpin?path=` | POST   | Unpin (evict local data)                                    |
| `/api/health`      | GET    | Uptime, DB status, in-flight uploads                        |
| `/api/metrics`     | GET    | Engine telemetry counters (files/chunks/bytes/dedup)        |

## Build & Test

```bash
go build -o pdrive ./cmd/pdrive       # build
go test ./...                          # unit tests
python3 scripts/test_e2e.py            # E2E: upload, download, delete
python3 scripts/test_dirs.py           # directory operations
python3 scripts/test_browser.py        # browser HTML listing
```

Start the daemon:

```bash
./pdrive daemon --password <your-password> --debug
```

## Code Conventions

- **Go standard library style**: `log/slog` for structured logging, `fmt.Errorf` with `%w` for error wrapping.
- **No external frameworks**: Only stdlib + `golang.org/x/net/webdav`, `golang.org/x/crypto/argon2`, `golang.org/x/term`, `modernc.org/sqlite`, `google/uuid`.
- **`internal/` packages**: Everything is internal; the only public entry point is `cmd/pdrive`.
- **Embedded SQL schema**: `schema.sql` is embedded via `//go:embed` in `db.go`.
- **Error handling**: Return errors up the call stack. Only log at the call site that can handle it. Use `slog.Warn` for non-fatal issues (e.g., failed chunk cleanup).
- **Concurrency**: Engine fields `closeCh` (shutdown), `backupMu`/`backupTimer` (debounced backup), `uploadsMu` (progress map), `fileGate` (serializes file uploads).

## Common Pitfalls

- **Dedup + delete**: When deleting a file whose chunks are shared (dedup), `deleteCloudChunks` checks `RemotePathRefCount` to avoid breaking the clone. Always call this check before cloud deletion.
- **rclone remote names** must end with `:` — use `ensureColon()` in `rclonerc/operations.go`.
- **Foreign key order**: Insert `files` record before `chunks` (FK constraint).
- **SQLite single-connection deadlock**: When using `db.Conn().Begin()`, pre-fetch all data you need from the DB BEFORE starting the transaction (the tx holds the single connection).
- **SQLite WAL**: Checkpoint WAL before backing up the DB file (`PRAGMA wal_checkpoint(TRUNCATE)`).
- **WebDAV + browsers**: The `browserHandler` wrapper intercepts GET/HEAD with `Accept: text/html` for HTML listings; all other methods pass through to the WebDAV handler.
- **Path normalization**: Always use `cleanPath()` in the VFS layer. Paths must start with `/`.
- **pin/unpin in main.go**: These subcommands are HTTP API calls — they must be handled BEFORE rclone binary lookup.
- **Sync folder skips**: `.DS_Store`, `._*` resource forks, `.pdrive*`, and system dirs (`.Trash`, `.Spotlight-V100`, etc.).
- **Async writes**: Files > 4 MB (`AsyncWriteThreshold`) are spooled to `~/.pdrive/spool/` and uploaded in background. `ResumeUploads()` handles interrupted uploads on restart.

## Database Schema (6 tables)

- `providers` — Cloud account credentials and quota tracking
- `files` — Virtual file entries keyed by `virtual_path`
- `chunks` — File pieces (32–128 MB, dynamic) with SHA-256 hashes (FK to files, cascade delete)
- `chunk_locations` — Maps chunks to cloud providers with upload confirmation timestamps
- `directories` — Explicit directory records for empty dir support
- `failed_deletions` — Tracks cloud chunk deletions that failed for background retry (max 10 attempts)

## Runtime Paths

| Path                                 | Purpose                                             |
| ------------------------------------ | --------------------------------------------------- |
| `~/.pdrive/`                         | Config dir: DB, spool, rclone conf                  |
| `~/.pdrive/metadata.db`              | SQLite metadata database                            |
| `~/.pdrive/enc.salt`                 | Argon2id salt for password-derived key              |
| `~/.pdrive/enc.key`                  | Legacy raw AES-256 key (32 bytes)                   |
| `~/.pdrive/spool/`                   | Temp files for in-progress uploads                  |
| `~/.pdrive/daemon.log`               | Daemon log output                                   |
| `~/pdrive/`                          | Local sync folder (configurable)                    |
| Cloud: `pdrive-chunks/<uuid>`        | Encrypted chunk storage                             |
| Cloud: `pdrive-meta/metadata.db.enc` | Encrypted metadata backup (AES-256-GCM + timestamp) |
| Cloud: `pdrive-meta/enc.salt`        | Argon2id salt (multi-machine portability)           |

## Daemon Lifecycle

**Start**: open DB → start rclone RC → restore DB if empty → create spool dir → create Broker + Engine → start SyncDir → start HTTP server → resume uploads → start GC goroutine (60s initial, 24h recurring) → start failed-deletion retry goroutine (1h recurring)

**Stop**: SyncDir.Stop → webdavServer.Close → engine.FlushBackup → engine.Close → rclone.Stop → db.Close
