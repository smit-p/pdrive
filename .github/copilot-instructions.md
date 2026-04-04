# pdrive — Copilot Instructions

## Project Overview

pdrive is a Go daemon that aggregates multiple cloud storage accounts (Google Drive, Dropbox, etc.) into a single unified virtual drive. Files are split into 4 MB chunks, encrypted with AES-256-GCM, and distributed across cloud providers via rclone's RC API. A local SQLite database tracks all metadata. Users interact through a WebDAV server (mountable in Finder/Explorer), a browser UI, or a local sync folder (`~/pdrive/`) with Finder integration (stub files, Finder tags, right-click Quick Actions).

## Architecture

```
cmd/pdrive/main.go           — CLI entry point, flags, signal handling, launchd install, Quick Actions
internal/daemon/              — Lifecycle orchestrator, rclone subprocess, HTTP API, browser handler
internal/engine/              — File I/O coordinator: write/read/delete, dirs, DB cloud sync, rate limiting
internal/engine/dbsync.go     — Debounced metadata backup, orphan chunk GC
internal/chunker/             — Split (4 MB), encrypt/decrypt (AES-256-GCM), assemble with SHA-256
internal/broker/              — Assigns chunks to the provider with the most free space (PFRD/MFS)
internal/metadata/            — SQLite schema, DB lifecycle (WAL mode), all CRUD queries
internal/rclonerc/            — HTTP client for rclone RC API (upload, download, delete, list, quota)
internal/vfs/                 — WebDAV filesystem (golang.org/x/net/webdav)
internal/vfs/syncdir.go       — Local sync folder: fsnotify watcher, stub files, Finder tags, pin/unpin
scripts/                      — E2E test scripts (Python), rclone download helper
```

## Key Design Decisions

- **Directories are hybrid**: Explicit records in a `directories` table + implicit from file paths. Both must be considered in `IsDir`/`ListDir`/`ListSubdirectories`.
- **Chunks live on cloud, metadata lives locally**: `metadata.db` is the source of truth. It's auto-backed-up to cloud (`pdrive-meta/metadata.db`) and auto-restored on fresh installs.
- **rclone RC quirks**: `operations/uploadfile` takes `fs`/`remote` as query params (not multipart fields), and `remote` is the parent directory (not the full path). Use `operations/copyfile` for downloads (not `operations/cat`).
- **Encryption key is user-managed**: 32-byte AES-256 key passed via `--enc-key` flag. Not stored anywhere by pdrive.
- **Stub files**: Cloud-only files are represented locally as 0-byte files with xattrs (`com.pdrive.stub`, `com.pdrive.size`) and Finder tags (gray = cloud-only, green = local).
- **Event suppression**: `suppressEvent(path)` must be called BEFORE any filesystem write that the fsnotify watcher should ignore. The watcher checks and clears the suppress map on each event.
- **Engine lifecycle**: `NewEngine`/`NewEngineWithCloud` start a rate-limit refill goroutine. Always call `engine.Close()` to stop it (idempotent via `closeCh`).
- **Token bucket rate limiting**: `uploadTokens` channel (burst=16 prod, 256 test), refilled at 8 tokens/sec. All cloud API calls must consume a token first.

## CLI Subcommands & Flags

```
pdrive daemon [flags]         — Run the daemon (default)
pdrive pin <path> [path...]   — Download cloud-only file to local (HTTP API call)
pdrive unpin <path> [path...] — Evict local data, replace with stub (HTTP API call)
pdrive --install              — Install launchd service + Quick Actions
pdrive --uninstall            — Remove launchd service + Quick Actions
```

Key flags: `--config-dir` (~/.pdrive), `--sync-dir` (~/pdrive), `--rclone-addr` (127.0.0.1:5572),
`--webdav-addr` (127.0.0.1:8765), `--enc-key`, `--broker-policy` (pfrd|mfs), `--debug`

## HTTP API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/status` | GET | Total files, bytes, provider quotas |
| `/api/uploads` | GET | In-flight upload progress |
| `/api/ls?path=` | GET | Directory listing with `local_state` (local/stub/uploading) |
| `/api/pin?path=` | POST | Pin (download) a cloud-only file |
| `/api/unpin?path=` | POST | Unpin (evict local data) |

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
./pdrive daemon --enc-key <64-char-hex> --debug
```

## Code Conventions

- **Go standard library style**: `log/slog` for structured logging, `fmt.Errorf` with `%w` for error wrapping.
- **No external frameworks**: Only stdlib + `golang.org/x/net/webdav`, `modernc.org/sqlite`, `google/uuid`.
- **`internal/` packages**: Everything is internal; the only public entry point is `cmd/pdrive`.
- **Embedded SQL schema**: `schema.sql` is embedded via `//go:embed` in `db.go`.
- **Error handling**: Return errors up the call stack. Only log at the call site that can handle it. Use `slog.Warn` for non-fatal issues (e.g., failed chunk cleanup).
- **Concurrency**: Engine fields `closeCh` (shutdown), `backupMu`/`backupTimer` (debounced backup), `uploadsMu` (progress map), `fileGate` (serializes file uploads).

## Common Pitfalls

- **rclone remote names** must end with `:` — use `ensureColon()` in `rclonerc/operations.go`.
- **Foreign key order**: Insert `files` record before `chunks` (FK constraint).
- **SQLite WAL**: Checkpoint WAL before backing up the DB file (`PRAGMA wal_checkpoint(TRUNCATE)`).
- **WebDAV + browsers**: The `browserHandler` wrapper intercepts GET/HEAD with `Accept: text/html` for HTML listings; all other methods pass through to the WebDAV handler.
- **Path normalization**: Always use `cleanPath()` in the VFS layer. Paths must start with `/`.
- **pin/unpin in main.go**: These subcommands are HTTP API calls — they must be handled BEFORE rclone binary lookup, since Finder's Quick Actions use a stripped PATH.
- **Sync folder skips**: `.DS_Store`, `._*` resource forks, `.pdrive*`, and system dirs (`.Trash`, `.Spotlight-V100`, etc.).
- **Async writes**: Files > 4 MB (`AsyncWriteThreshold`) are spooled to `~/.pdrive/spool/` and uploaded in background. `ResumeUploads()` handles interrupted uploads on restart.

## Database Schema (5 tables)

- `providers` — Cloud account credentials and quota tracking
- `files` — Virtual file entries keyed by `virtual_path`
- `chunks` — 4 MB file pieces with SHA-256 hashes (FK to files, cascade delete)
- `chunk_locations` — Maps chunks to cloud providers with upload confirmation timestamps
- `directories` — Explicit directory records for empty dir support

## Runtime Paths

| Path | Purpose |
|---|---|
| `~/.pdrive/` | Config dir: DB, spool, rclone conf |
| `~/.pdrive/metadata.db` | SQLite metadata database |
| `~/.pdrive/spool/` | Temp files for in-progress uploads |
| `~/.pdrive/bin/pdrive` | Installed binary (by `--install`) |
| `~/.pdrive/daemon.log` | Daemon log output (launchd) |
| `~/pdrive/` | Local sync folder (configurable) |
| `~/Library/LaunchAgents/com.smit.pdrive.plist` | launchd agent plist |
| `~/Library/Services/pdrive Pin to Local.workflow/` | Finder Quick Action: pin |
| `~/Library/Services/pdrive Free Up Space.workflow/` | Finder Quick Action: unpin |
| Cloud: `pdrive-chunks/<uuid>` | Encrypted chunk storage |
| Cloud: `pdrive-meta/metadata.db` | Metadata backup |

## Daemon Lifecycle

**Start**: open DB → start rclone RC → restore DB if empty → create spool dir → create Broker + Engine → start SyncDir → start HTTP server → resume uploads → start GC goroutine (60s initial, 24h recurring)

**Stop**: SyncDir.Stop → webdavServer.Close → engine.FlushBackup → engine.Close → rclone.Stop → db.Close
