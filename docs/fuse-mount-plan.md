# pdrive FUSE Mount Plan

## Why This Upgrade

WebDAV is useful for quick compatibility, but Finder and WebDAV clients can hit limitations:

- HTTP request timeout behavior on long operations
- weaker lock semantics versus native filesystems
- extra copy/upload staging overhead for large files
- less predictable behavior for rename/delete under retries

A native FUSE mount is the biggest architecture upgrade for user experience and reliability.

## Goals

- Native mount on macOS/Linux with standard filesystem semantics
- Proper file locking and lower-latency metadata operations
- Eliminate WebDAV HTTP timeout constraints
- Improve large file IO path and Finder behavior
- Preserve current pdrive chunking/encryption/provider logic

## Non-Goals (Phase 1)

- Rewriting the storage engine or chunk format
- Cross-process distributed locking across machines
- Full Windows support in first release

## Library Choice

Two viable options were evaluated:

1. go-fuse (hanwen/go-fuse)
- Strong performance, widely used, low-level control
- Good choice if we want maximum behavior control

2. cgofuse
- Portable wrapper with easier API in some areas
- Simpler transition for teams that prefer a more direct callback style

Decision for pdrive v1: go-fuse.
Reason: better ecosystem traction and performance profile for high-operation workloads.

## Target Architecture

- New package: internal/fusefs
- New mount command mode in daemon (or separate cmd):
  - pdrive mount --mountpoint /Volumes/pdrive
- FUSE filesystem layer maps syscall operations to Engine methods
- Keep WebDAV mode as fallback during transition

Flow:
1. Kernel/Finder issues FS op -> fusefs handler
2. fusefs resolves path and operation semantics
3. calls Engine (ReadFile, WriteFileStream, DeleteFile, DeleteDir, RenameDir, etc.)
4. Engine handles chunking/encryption/provider assignment exactly as today

## Work Breakdown

### Phase 0: Groundwork (1-2 days)

- Add feature flag/config:
  - mount backend: webdav or fuse
- Introduce operation metrics hooks in Engine for baseline comparisons
- Add integration test harness helpers for mount lifecycle

Deliverable: no behavior change, infra ready.

### Phase 1: Read-Only FUSE MVP (2-3 days)

Implement in internal/fusefs:

- Lookup / Getattr / Readdir
- Open / Read / Release
- Path normalization and root handling

Mapping:
- Readdir -> Engine.ListDir
- Getattr -> Engine.Stat + Engine.IsDir
- Read -> Engine.ReadFile (same current read path)

Deliverable: stable browsing + file reads from mounted volume.

### Phase 2: Basic Write Path (3-5 days)

Implement:

- Create / Open with write flags
- Write / Flush / Fsync / Release
- Mkdir / Unlink / Rmdir / Rename

Strategy:
- Reuse temp-file staging model used in WebDAV layer
- On Release/Flush for large files, dispatch async write path already implemented
- For small files, use synchronous WriteFileStream

Deliverable: full CRUD with parity to current WebDAV behavior.

### Phase 3: Correctness Semantics (3-4 days)

- Advisory locking support (flock/posix lock handling where supported)
- Better errno mapping (ENOENT, EEXIST, ENOTEMPTY, EROFS, ENOSPC)
- Atomic rename semantics and cross-directory rename edge cases
- Directory delete behavior under concurrent access

Deliverable: Finder and shell operations behave predictably under contention.

### Phase 4: Performance and Stability (3-5 days)

- Read cache tuning (optional bounded LRU)
- Writeback queue limits and backpressure controls
- Parallel readdir/stat optimization
- Benchmark suite against WebDAV baseline

KPIs:
- reduced median create/rename/delete latency
- no timeout-induced failures on multi-GB operations
- stable memory usage under concurrent copy workloads

### Phase 5: Rollout (2 days)

- Keep WebDAV as fallback and default for one release candidate
- Add docs, migration guide, troubleshooting
- Collect user telemetry/log feedback
- Flip default backend to FUSE once stable

## API / Code Changes Required

1. Daemon config
- Add mount backend selection and mountpoint
- Add startup path for FUSE server lifecycle

2. New package
- internal/fusefs
  - fs.go: node and file handlers
  - errors.go: errno mapping
  - staging.go: temp write file lifecycle

3. Engine touch points
- Optional context-aware cancellation support for long writes
- Optional lightweight handle-level read streaming API (future optimization)

4. CLI
- Add command/flags:
  - --backend webdav|fuse
  - --mountpoint

## Testing Plan

### Unit

- Path normalization and dir/file detection
- Error translation to errno
- Rename/delete edge cases

### Integration

- Mount lifecycle test (mount, read, write, rename, delete, unmount)
- Multi-GB file copy test via Finder and cp
- Concurrent operation tests (copy + delete + rename)
- Provider rate-limit simulation while writing

### Regression

- Existing engine/chunker/metadata tests remain green
- Existing WebDAV mode remains functional while FUSE is introduced

## Risks and Mitigations

1. FUSE behavior differs by OS version
- Mitigation: macOS and Linux CI matrix, explicit compatibility table

2. Locking semantics mismatch
- Mitigation: implement strict default lock mode + configuration toggle

3. Increased complexity during dual-backend period
- Mitigation: keep backend-specific logic isolated in vfs/webdav and fusefs packages; Engine remains shared core

4. Background upload visibility
- Mitigation: add upload-state metadata and surface status via logs/API

## Proposed Milestones

- M1 (week 1): Read-only FUSE mount works reliably
- M2 (week 2): Full CRUD parity with WebDAV
- M3 (week 3): Locking/error semantics hardened + perf pass
- M4 (week 4): Release candidate with backend toggle, docs, and migration guide

## Recommended Next Step

Start with Phase 1 in a separate feature branch and keep WebDAV fully intact. Build a mount integration test harness first so each new FUSE operation is validated immediately as it lands.
