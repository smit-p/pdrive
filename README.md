# pdrive

A daemon that aggregates multiple cloud storage accounts (Google Drive, Dropbox, OneDrive, etc.) into a single logical drive using block-level cross-account chunking.

## How it works

1. Files are split into fixed 4 MB chunks
2. Each chunk is encrypted with AES-256-GCM
3. Chunks are distributed across your cloud storage accounts using configurable broker policies (`pfrd` or `mfs`) with a minimum-free-space guard
4. A local SQLite database tracks where each chunk lives
5. A WebDAV server exposes the unified filesystem to your OS

pdrive uses [rclone](https://rclone.org) in RC (remote control) daemon mode as the transport layer for all cloud provider operations.

## Architecture

```
┌──────────────┐
│  Finder/App  │  (any WebDAV client)
└──────┬───────┘
       │ WebDAV (localhost:8765)
┌──────┴───────┐
│   pdrive     │
│  ┌─────────┐ │
│  │ Engine  │ │  split → encrypt → assign → upload
│  │ Broker  │ │  configurable placement policy (pfrd/mfs)
│  │Metadata │ │  SQLite WAL — tracks files & chunks
│  └────┬────┘ │
│       │ HTTP  │
│  ┌────┴────┐ │
│  │ rclone  │ │  RC daemon on localhost:5572
│  │  (child)│ │
│  └─────────┘ │
└──────────────┘
       │
   ┌───┴───┬────────┐
   │       │        │
 GDrive  Dropbox  OneDrive ...
```

## Prerequisites

- **Go 1.21+** for building
- **rclone** configured with at least one remote (`rclone config`)

## Build

```bash
go build -o pdrive ./cmd/pdrive
```

## Setup

1. Install and configure rclone:

   ```bash
   # Install rclone
   ./scripts/download-rclone.sh

   # Or install manually: https://rclone.org/install/
   # Then configure your remotes:
   rclone config
   ```

2. Build pdrive:

   ```bash
   go build -o pdrive ./cmd/pdrive
   ```

3. Run:

   ```bash
   ./pdrive
   ```

   The WebDAV server starts on `localhost:8765`. Mount it:
   - **macOS**: Finder → Go → Connect to Server → `http://localhost:8765`
   - **Linux**: `mount -t davfs http://localhost:8765 /mnt/pdrive`
   - **Windows**: Map network drive → `http://localhost:8765`

## Flags

| Flag               | Default          | Description                                |
| ------------------ | ---------------- | ------------------------------------------ |
| `--config-dir`     | `~/.pdrive`      | Directory for metadata DB and config       |
| `--rclone-addr`    | `127.0.0.1:5572` | rclone RC listen address                   |
| `--webdav-addr`    | `127.0.0.1:8765` | WebDAV server listen address               |
| `--enc-key`        | (test key)       | Hex-encoded 32-byte AES-256 encryption key |
| `--broker-policy`  | `pfrd`           | Chunk placement policy: `pfrd` or `mfs`    |
| `--min-free-space` | `268435456`      | Minimum free bytes required per provider   |
| `--debug`          | `false`          | Enable debug logging                       |

## Running Tests

```bash
go test ./...
```

## Project Status

This is a **v0 proof of concept**. Current limitations:

- No provider auto-discovery (providers must be registered manually)
- Single-replica storage (no redundancy across providers)
- Encryption key management is basic (CLI flag)

See also: `docs/fuse-mount-plan.md` for the FUSE migration plan.

## License

MIT
