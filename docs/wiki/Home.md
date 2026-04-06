# pdrive

**pdrive** is a command-line tool that provides encrypted, chunked cloud storage using rclone as the transport layer. It splits large files into chunks, encrypts them with AES-256-GCM, distributes them across multiple cloud providers, and reassembles them transparently on download.

## Key Features

- **Client-side encryption** — AES-256-GCM with Argon2id key derivation; your cloud providers never see plaintext.
- **Chunked storage** — Files are split into 32–128 MB chunks (auto-sized) and distributed across providers.
- **Multi-provider** — Google Drive, Dropbox, OneDrive, Box, and any other rclone-supported remote.
- **Deduplication** — SHA-256 content hashing prevents storing the same file twice.
- **Local sync directory** — A macOS/Linux folder that mirrors your cloud files. Unpinned files appear as lightweight stubs (via extended attributes).
- **WebDAV interface** — Mount pdrive as a network drive in Finder or any WebDAV client.
- **FUSE mount** — Native kernel-level filesystem mount via go-fuse. Full read/write support with temp-file staging and async uploads.
- **Config file** — Optional TOML configuration at `~/.pdrive/config.toml` for persistent settings.
- **Interactive TUI** — Browse files, navigate directories, and manage storage from the terminal.
- **HTTP API** — 20+ JSON endpoints for programmatic access to all operations.

## Quick Start

```bash
# Install
go install github.com/smit-p/pdrive/cmd/pdrive@latest

# Start the daemon (first run prompts for encryption passphrase)
pdrive

# List files
pdrive ls

# Upload a file
pdrive put ~/Documents/report.pdf

# Interactive browser
pdrive browse
```

## Wiki Pages

| Page                                  | Description                                          |
| ------------------------------------- | ---------------------------------------------------- |
| [Architecture](Architecture.md)       | System design, component diagram, data flow          |
| [Packages](Packages.md)               | Guide to every Go package in the project             |
| [Encryption](Encryption.md)           | AES-256-GCM, Argon2id, key derivation details        |
| [Upload Pipeline](Upload-Pipeline.md) | End-to-end upload flow with chunking and retry       |
| [CLI Reference](CLI-Reference.md)     | Every command with usage examples                    |
| [HTTP API](HTTP-API.md)               | All `/api/*` endpoints with request/response formats |
| [Configuration](Configuration.md)     | Daemon config, rclone remotes, sync directory setup  |
