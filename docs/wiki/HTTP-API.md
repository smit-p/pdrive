# HTTP API

The pdrive daemon exposes an HTTP API at `http://127.0.0.1:8765`.

## Endpoint Summary

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/ls?path=/` | GET | Directory listing (files + dirs with local state). |
| `/api/status` | GET | Storage totals and per-provider quota usage. |
| `/api/remotes` | GET | Configured remotes and active/inactive status. |
| `/api/health` | GET | Daemon health (`status`, uptime, DB state, uploads). |
| `/api/metrics` | GET | Engine counters (uploads/downloads/chunks/bytes/dedup). |
| `/api/uploads` | GET | In-flight upload progress list. |
| `/api/tree?path=/` | GET | Recursive file tree entries from a root path. |
| `/api/find?path=/&pattern=*.pdf` | GET | Glob search across files. |
| `/api/info?path=/file` | GET | File metadata + chunk/provider info. |
| `/api/du?path=/` | GET | Disk usage summary (`file_count`, `total_bytes`). |
| `/api/download?path=/file` | GET/HEAD | Stream file content (or headers only for HEAD). |
| `/api/upload` | POST | Multipart upload (`file` field, optional `dir` form field). |
| `/api/upload/cancel?path=/file` | POST | Cancel an active upload. |
| `/api/delete?path=/file-or-dir` | POST | Delete file or directory recursively. |
| `/api/mv?src=/a&dst=/b` | POST | Move or rename file/directory. |
| `/api/mkdir?path=/dir` | POST | Create directory. |
| `/api/pin?path=/file` | POST | Download cloud file to local sync folder. |
| `/api/unpin?path=/file` | POST | Replace local file with cloud stub. |
| `/api/verify?path=/file` | GET | Verify chunk integrity for a file. |
| `/api/activity?limit=50` | GET | Recent activity records. |
| `/api/resync` | GET/POST | Trigger immediate provider re-sync. |
| `/api/logs` | GET | Recent daemon logs from in-memory ring buffer. |
| `/api/logs/stream` | GET | Live server-sent event stream of logs. |

## Selected Responses

### `GET /api/ls?path=<dir>`

List files in a directory.

**Response:**

```json
{
  "path": "/",
  "dirs": ["photos"],
  "files": [
    {
      "name": "report.pdf",
      "path": "/report.pdf",
      "size": 1048576,
      "modified_at": 1712345678,
      "local_state": "local"
    }
  ]
}
```

### `GET /api/uploads`

**Response:**

```json
[
  {
    "VirtualPath": "/video.mp4",
    "TotalChunks": 10,
    "ChunksUploaded": 7,
    "SizeBytes": 335544320,
    "BytesDone": 234881024,
    "BytesTotal": 335544320,
    "HashBytesRead": 335544320,
    "HashBytesTotal": 335544320,
    "SpeedBPS": 10485760,
    "StartedAt": "2026-04-15T12:00:00Z",
    "Failed": false,
    "Preparing": false
  }
]
```

### `POST /api/upload`

Upload via multipart form using `file` and optional `dir` form field.

```bash
curl -F "file=@report.pdf" -F "dir=/docs" http://127.0.0.1:8765/api/upload
```

## WebDAV

The daemon also serves WebDAV at the same base address (`http://127.0.0.1:8765`). This can be mounted as a network drive:

**macOS Finder:**

1. Finder → Go → Connect to Server
2. Enter `http://127.0.0.1:8765`
3. Browse files as if they were local

**Command line:**

```bash
# Mount via mount_webdav (macOS)
mkdir ~/pdrive
mount_webdav http://127.0.0.1:8765 ~/pdrive
```
