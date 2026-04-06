# HTTP API

The pdrive daemon exposes a JSON API at `http://127.0.0.1:8765`. All endpoints accept GET unless noted otherwise.

## File Operations

### `GET /api/ls?path=<dir>&sort=<field>&order=<asc|desc>`
List files in a directory.

**Response:**
```json
{
  "files": [
    {"name": "report.pdf", "path": "/report.pdf", "size": 1048576, "hash": "abc123...", "state": "ready", "created_at": "2024-01-15T10:30:00Z"}
  ],
  "dirs": ["/photos", "/documents"]
}
```

### `POST /api/upload`
Upload a file. Multipart form with `file` field and optional `path` query parameter.

```bash
curl -F "file=@report.pdf" "http://127.0.0.1:8765/api/upload?path=/docs/"
```

### `GET /api/download?path=<file>`
Download a file. Returns the file content with appropriate Content-Type.

### `POST /api/delete?path=<path>`
Delete a file or directory.

### `POST /api/mv?src=<old>&dst=<new>`
Rename or move a file.

### `POST /api/mkdir?path=<dir>`
Create a virtual directory.

## Search & Navigation

### `GET /api/find?q=<query>`
Search files by name.

### `GET /api/tree?path=<dir>`
Get directory tree structure.

### `GET /api/du?path=<dir>`
Get disk usage summary.

### `GET /api/info?path=<file>`
Get detailed file information including chunk details.

## Status & Monitoring

### `GET /api/status`
Overall storage status — total/used/free across providers.

### `GET /api/uploads`
Active upload progress.

**Response:**
```json
[
  {"path": "/video.mp4", "total_chunks": 10, "uploaded_chunks": 7, "total_bytes": 335544320, "uploaded_bytes": 234881024}
]
```

### `GET /api/remotes`
List configured providers with email identity and quota.

### `GET /api/health`
Daemon health check. Returns 200 with uptime.

### `GET /api/metrics`
Engine telemetry counters (uploads, downloads, deletes, dedup hits).

### `GET /api/activity?limit=<n>`
Recent activity log entries.

## Sync Directory

### `POST /api/pin?path=<file>`
Pin a file — download to the local sync directory.

### `POST /api/unpin?path=<file>`
Unpin a file — remove local copy, replace with stub.

### `POST /api/verify?path=<file>`
Verify file integrity by re-downloading and hash-checking all chunks.

## WebDAV

The daemon also serves a WebDAV interface at `http://127.0.0.1:8765/dav/`. This can be mounted as a network drive:

**macOS Finder:**
1. Finder → Go → Connect to Server
2. Enter `http://127.0.0.1:8765/dav/`
3. Browse files as if they were local

**Command line:**
```bash
# Mount via mount_webdav (macOS)
mkdir ~/pdrive-mount
mount_webdav http://127.0.0.1:8765/dav/ ~/pdrive-mount
```
