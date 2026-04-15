# pdrive

**pdrive** is a command-line and daemon-based tool that aggregates multiple cloud accounts into one unified drive. It splits files into chunks, distributes chunks across providers, and serves access through CLI, browser UI, WebDAV, and optional FUSE mount.

## Key Features

- **Chunked multi-provider storage** — Files are split into dynamic chunks (32 MB to 4 GiB) and placed across configured providers.
- **Content-hash deduplication** — SHA-256-based dedup prevents duplicate cloud uploads for identical content.
- **Local sync directory** — `~/pdrive` mirrors cloud files. Cloud-only files appear as lightweight stubs via extended attributes.
- **Browser UI** — Built-in web app at `http://127.0.0.1:8765` for browsing, uploads, search, metrics, and management.
- **WebDAV and FUSE access** — Use network-drive style WebDAV or native kernel mount via FUSE.
- **Resume and repair** — Interrupted uploads resume, orphan chunks are garbage-collected, and failed deletions are retried.
- **Configurable runtime** — Optional TOML config (`~/.pdrive/config.toml`) plus CLI flags.

## Quick Start

```bash
# Install (requires CGO toolchain)
CGO_ENABLED=1 go install github.com/smit-p/pdrive/cmd/pdrive@latest

# Start daemon
pdrive

# List files
pdrive ls /

# Upload a file
pdrive put ~/Documents/report.pdf

# Launch interactive TUI
pdrive browse
```

## Wiki Pages

| Page                                  | Description                                           |
| ------------------------------------- | ----------------------------------------------------- |
| [Architecture](Architecture.md)       | System design, components, and data flow              |
| [Packages](Packages.md)               | Package-by-package codebase guide                     |
| [Upload Pipeline](Upload-Pipeline.md) | End-to-end upload lifecycle and failure handling      |
| [CLI Reference](CLI-Reference.md)     | Commands, flags, and examples                         |
| [HTTP API](HTTP-API.md)               | `/api/*` endpoints, methods, and response shapes      |
| [Configuration](Configuration.md)     | Paths, defaults, config file, and service integration |
