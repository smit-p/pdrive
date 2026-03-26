# pdrive — Copilot Instructions

## Project Overview

pdrive is a Go daemon that aggregates multiple cloud storage accounts (Google Drive, Dropbox, etc.) into a single unified virtual drive. Files are split into 4 MB chunks, encrypted with AES-256-GCM, and distributed across cloud providers via rclone's RC API. A local SQLite database tracks all metadata. Users interact through a WebDAV server (mountable in Finder/Explorer) or a browser UI.

## Architecture

```
cmd/pdrive/main.go          — CLI entry point, flag parsing, signal handling
internal/daemon/             — Lifecycle orchestrator, rclone subprocess manager, browser handler
internal/engine/             — File I/O coordinator: write/read/delete, directory ops, DB cloud sync
internal/chunker/            — Split (4 MB), encrypt/decrypt (AES-256-GCM), assemble with SHA-256 verification
internal/broker/             — Assigns chunks to the provider with the most free space
internal/metadata/           — SQLite schema, DB lifecycle (WAL mode), all CRUD queries
internal/rclonerc/           — HTTP client for rclone RC API (upload, download, delete, list, quota)
internal/vfs/                — WebDAV filesystem implementation (golang.org/x/net/webdav)
scripts/                     — E2E test scripts (Python), rclone download helper
```

## Key Design Decisions

- **Directories are hybrid**: Explicit records in a `directories` table + implicit from file paths. Both must be considered in `IsDir`/`ListDir`/`ListSubdirectories`.
- **Chunks live on cloud, metadata lives locally**: `metadata.db` is the source of truth. It's auto-backed-up to cloud (`pdrive-meta/metadata.db`) and auto-restored on fresh installs.
- **rclone RC quirks**: `operations/uploadfile` takes `fs`/`remote` as query params (not multipart fields), and `remote` is the parent directory (not the full path). Use `operations/copyfile` for downloads (not `operations/cat`).
- **Encryption key is user-managed**: 32-byte AES-256 key passed via `--enc-key` flag. Not stored anywhere by pdrive.

## Build & Test

```bash
go build -o pdrive ./cmd/pdrive       # build
go test ./...                          # unit tests (chunker, metadata)
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

## Common Pitfalls

- **rclone remote names** must end with `:` — use `ensureColon()` in `rclonerc/operations.go`.
- **Foreign key order**: Insert `files` record before `chunks` (FK constraint).
- **SQLite WAL**: Checkpoint WAL before backing up the DB file (`PRAGMA wal_checkpoint(TRUNCATE)`).
- **WebDAV + browsers**: The `browserHandler` wrapper intercepts GET/HEAD with `Accept: text/html` for HTML listings; all other methods pass through to the WebDAV handler.
- **Path normalization**: Always use `cleanPath()` in the VFS layer. Paths must start with `/`.

## Database Schema (5 tables)

- `providers` — Cloud account credentials and quota tracking
- `files` — Virtual file entries keyed by `virtual_path`
- `chunks` — 4 MB file pieces with SHA-256 hashes (FK to files, cascade delete)
- `chunk_locations` — Maps chunks to cloud providers with upload confirmation timestamps
- `directories` — Explicit directory records for empty dir support

## Runtime Paths

- `~/.pdrive/metadata.db` — SQLite metadata database
- `~/.config/rclone/rclone.conf` — rclone remote configuration (fallback)
- Cloud: `<provider>/pdrive-chunks/<chunk-uuid>` — encrypted chunk storage
- Cloud: `<provider>/pdrive-meta/metadata.db` — metadata backup
