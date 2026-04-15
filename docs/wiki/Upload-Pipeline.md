# Upload Pipeline

This page describes the end-to-end upload flow from file selection to cloud storage.

## Overview

```
File → SHA-256 hash → Dedup check → Chunk → Upload → Record metadata
```

## Detailed Steps

### 1. Pre-Upload Space Check

Before hashing, the engine checks aggregate free space across all providers. If the file won't fit, the upload is rejected immediately with a clear error — no work is wasted.

### 2. File Hashing

The engine computes a SHA-256 hash of the entire file. This hash is the primary deduplication key.

### 3. Deduplication Check

The hash is compared against all existing files in the metadata DB via `FindDuplicate()`. If a match is found, the file is recorded as a new path pointing to existing chunks — no upload needed.

### 4. Chunk Sizing

`ChunkSizeForFile()` auto-selects chunk size based on file size:

| File Size      | Chunk Size          | Target Chunks |
| -------------- | ------------------- | ------------- |
| < 32 MB        | File size (1 chunk) | 1             |
| 32 MB+         | Dynamic             | ~25           |

Chunk size starts at 32 MB and scales up with file size, capped at 4 GiB.

### 5. Splitting

The file is split using `ChunkReader`, a streaming iterator that reads one chunk at a time to keep memory usage constant regardless of file size. Each chunk gets a SHA-256 hash for later verification.

### 6. Provider Selection

The `Broker` picks a target provider for each chunk. Two policies:

- **PFRD** — Weighted random selection proportional to free space
- **MFS** — Always picks the provider with the most free space

### 7. Concurrent Upload

Chunks are uploaded concurrently with these controls:

| Parameter              | Value                                 |
| ---------------------- | ------------------------------------- |
| Max concurrent workers | 12                                    |
| Upload rate limit      | 12/sec (burst 20)                     |
| Max retries per chunk  | 5                                     |
| Backoff                | Exponential: 2s → 4s → 8s → 16s → 32s |

Each upload uses rclone's async `operations/copyfile` + job polling (exponential backoff from 100ms to 5s, max ~720 iterations ≈ 1 hour timeout).

### 8. Metadata Recording

After all chunks are uploaded, the engine:

1. Records the file in the `files` table
2. Records each chunk in the `chunks` table (with hash, size, index)
3. Records each chunk's location in `chunk_locations` (remote + path)
4. Logs the upload to `activity_log`
5. Schedules a debounced metadata backup to all providers

### 9. Async Writes

Files smaller than 4 MB (`AsyncWriteThreshold`) are uploaded synchronously — the caller blocks until complete. Larger files are uploaded asynchronously; the engine tracks them via `UploadProgress()` which the CLI and API can poll.

## Error Handling

- **Rate limiting:** Detected by string matching on rclone error messages (`IsRateLimited`). Triggers exponential backoff.
- **Upload failure:** Retried up to 5 times with exponential backoff. After 5 failures, the upload is abandoned and the error is reported.
- **Partial upload:** If some chunks succeed but others fail, the engine automatically cleans up the successfully uploaded chunks. Any chunks that fail to delete during cleanup become orphans, cleaned up by the periodic GC (60 s after startup, then every 24 h).
