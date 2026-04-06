# Architecture

## Overview

pdrive runs as a local daemon process that coordinates several subsystems:

```
┌─────────────────────────────────────────────────────────┐
│                    pdrive daemon                        │
│                                                         │
│  ┌─────────┐  ┌────────┐  ┌──────────┐  ┌───────────┐ │
│  │   CLI   │  │ WebDAV │  │ HTTP API │  │  SyncDir  │ │
│  │ client  │  │ server │  │  server  │  │  watcher  │ │
│  └────┬────┘  └───┬────┘  └────┬─────┘  └─────┬─────┘ │
│       │           │            │               │        │
│       │      ┌────┴────┐       │               │        │
│       │      │  FUSE   │       │               │        │
│       │      │  mount  │       │               │        │
│       │      └────┬────┘       │               │        │
│       │           │            │               │        │
│       └───────────┴────────┬───┘───────────────┘        │
│                            │                            │
│                     ┌──────┴──────┐                     │
│                     │   Engine    │                     │
│                     │ (core ops)  │                     │
│                     └──────┬──────┘                     │
│              ┌─────────────┼─────────────┐              │
│         ┌────┴────┐  ┌─────┴─────┐  ┌───┴────┐        │
│         │ Chunker │  │  Broker   │  │Metadata│        │
│         │+Crypto  │  │(placement)│  │(SQLite)│        │
│         └─────────┘  └───────────┘  └────────┘        │
│                            │                            │
│                     ┌──────┴──────┐                     │
│                     │  rclonerc   │                     │
│                     │ (RC client) │                     │
│                     └──────┬──────┘                     │
│                            │                            │
└────────────────────────────┼────────────────────────────┘
                             │ HTTP (127.0.0.1:5572)
                      ┌──────┴──────┐
                      │   rclone    │
                      │  (child     │
                      │  process)   │
                      └──────┬──────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
        Google Drive    Dropbox       OneDrive
```

## Component Responsibilities

### Engine (`internal/engine`)

The core orchestrator. Handles upload (hash → dedup → chunk → encrypt → upload), download (fetch → decrypt → verify → reassemble), delete, rename, dedup detection, orphan garbage collection, failed-deletion retry, and encrypted metadata backup to all providers.

### Daemon (`internal/daemon`)

Ties all subsystems together. Manages the rclone child process, opens the metadata DB, creates the engine, launches the WebDAV + HTTP API server, watches the sync directory, and runs periodic background tasks.

### Chunker (`internal/chunker`)

Splits files into size-appropriate chunks (32–128 MB), provides a streaming `ChunkReader` for memory-efficient splitting, handles AES-256-GCM encryption/decryption, and reassembles chunks with SHA-256 verification.

### Broker (`internal/broker`)

Decides which cloud provider receives each chunk. Supports two policies:

- **PFRD** (Proportional Free Remaining Distribution) — Distributes proportionally to free space.
- **MFS** (Most Free Space) — Always picks the provider with the most free space.

### Metadata (`internal/metadata`)

SQLite database (WAL mode) storing all file metadata, chunk records, chunk locations, provider info, directories, failed deletions, and activity logs. Schema is embedded and auto-migrated.

### rclonerc (`internal/rclonerc`)

HTTP client for the rclone RC API. Handles async file upload (copyfile + job polling), streaming download to temp files, rate-limit detection, account identity fetching, and quota caching.

### VFS (`internal/vfs`)

WebDAV filesystem implementation and local sync directory. WebDAV uses temp files for reads/writes to avoid memory bloat. SyncDir watches for local changes via fsnotify with debounce and rename detection.

### FUSE Filesystem (`internal/fusefs`)

Native kernel-level mount that exposes pdrive as a local directory. Uses go-fuse (hanwen/go-fuse/v2) with the `fs.InodeEmbedder` API. Supports full read/write: Lookup, Getattr, Readdir, Open, Read, Create, Write, Flush, Release, Mkdir, Unlink, Rmdir, and Rename. Writes are staged to temp files in the spool directory and uploaded on Flush (async for files > 4 MB). Selected via the `--backend fuse` flag or `mount_backend = "fuse"` in config.

### Config (`internal/config`)

TOML configuration file support. Loads settings from `~/.pdrive/config.toml`, allowing persistent defaults for all CLI flags including the mount backend and mountpoint.

## Data Flow

### Upload

1. User runs `pdrive upload file.pdf` or saves to sync dir
2. Engine computes SHA-256 hash and checks for deduplication
3. Chunker splits file into appropriately-sized chunks
4. Each chunk is encrypted with AES-256-GCM (unique nonce per chunk)
5. Broker selects target provider per chunk based on free space
6. rclonerc uploads chunks concurrently (up to 10 workers) with retry/backoff
7. Metadata DB records file, chunks, and their locations

### Download

1. Engine looks up file metadata and chunk locations
2. Chunks are downloaded sequentially via rclonerc
3. Each chunk is decrypted and SHA-256 verified
4. Chunks are reassembled into the original file
