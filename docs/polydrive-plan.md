# Polydrive — Project Planning Document

> Generated from design session. Feed this into Copilot Chat as context before starting implementation.

---

## The Concept

Build software that aggregates multiple free-tier (and paid) cloud storage accounts — Google Drive, Dropbox, OneDrive, Box, etc. — into a single logical storage pool. Two free Dropbox accounts (5 GB each) + two free Google accounts (15 GB each) = 40 GB unified storage, accessible as a single mounted drive on your OS.

**Core technical differentiator vs existing tools:** True block-level cross-account chunking. Existing tools like rclone Union route whole files to upstreams — a 20 GB file can't span two 10 GB accounts. This project splits files into 4 MB chunks that are individually routed to whichever account has space, so a single large file can span N accounts seamlessly.

---

## Competitive Landscape

| Tool | What it does | Gap |
|---|---|---|
| MultCloud, CloudMounter, Air Explorer | Unified browser/manager for multiple clouds | No capacity pooling — each account is still a separate silo |
| rclone Union backend | Combines multiple remotes, mounts as one drive | Routes whole files, not chunks — a file must fit on one upstream |
| rclone Chunker backend | Splits large files | Doesn't cooperate with Union for cross-remote placement |
| **This project** | Block-level chunking across accounts | Fills the gap: a 20 GB file spanning 4 × 5 GB accounts |

The rclone community has explicitly raised the need for automatic cross-remote chunking (file too large for any single remote, must span multiple) — this is the unmet need.

---

## Architecture Decision: Build on Top of rclone

### Why rclone as the provider layer

- rclone has plugins for 70+ providers, all already built and battle-tested
- OAuth2 flows, token refresh, retry logic, rate limit handling — all handled by rclone
- You write zero provider-specific API client code
- rclone is open source (MIT), Go-based, actively maintained

### How to integrate: rclone RC daemon mode

Don't shell out to rclone CLI commands. Instead:

1. Bundle rclone as a sidecar binary in your distributable
2. Your daemon spawns rclone at startup: `rclone rcd --rc-addr localhost:5572 --rc-no-auth --config ~/.polydrive/rclone.conf`
3. All communication is HTTP to `localhost:5572`
4. When your daemon exits, it kills rclone

**Key rclone RC endpoints you'll use:**
- `operations/putfile` — upload a chunk to a remote
- `operations/copyfile` — copy between remotes
- `core/stats` — quota and usage info
- `config/create` / `config/update` — add/update a remote programmatically after OAuth
- `config/providers` — list available provider types

---

## Full System Architecture

```
┌─────────────────────────────────────────────────┐
│           User (FUSE mount or WebDAV)           │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│              Your Daemon  (new code)            │
│  ┌─────────────┐ ┌───────────────┐ ┌─────────┐ │
│  │ VFS handler │→│ Chunk engine  │→│Metadata │ │
│  │FUSE/WebDAV  │ │split+enc+route│ │  DB     │ │
│  └─────────────┘ └───────────────┘ └─────────┘ │
│         │                │                      │
│  ┌──────▼────────────────▼──────────────────┐   │
│  │         rclone RC client                 │   │
│  │   HTTP calls to localhost:5572           │   │
│  └──────────────────┬───────────────────────┘   │
└─────────────────────│───────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────┐
│         rclone (bundled sidecar binary)          │
│    OAuth + token refresh handled entirely here  │
└──┬───────────┬───────────┬───────────┬──────────┘
   │           │           │           │
   ▼           ▼           ▼           ▼
Dropbox A  Dropbox B  Google A   OneDrive
 (5 GB)     (5 GB)    (15 GB)    (5 GB)
```

---

## File Write Flow (step by step)

1. **Write syscall intercepted** — OS write hits FUSE mount → daemon VFS handler
2. **Chunking engine** — file split into fixed 4 MB pieces, each SHA-256 hashed
3. **Encryption** — AES-256-GCM per chunk, key derived from master password via PBKDF2
4. **Space broker** — each chunk assigned to the account with the most available free space
5. **Parallel upload** — chunks pushed concurrently via rclone RC `operations/putfile` calls
6. **Metadata committed** — `chunk_id → provider + remote_path` written to SQLite

**Read path** is the exact reverse: metadata lookup → parallel download from N accounts → decrypt → reassemble → stream to OS.

**Critical write ordering:** Upload chunk → confirm success → write metadata row → only then ACK the write to FUSE. Never write metadata before the upload confirms. A crash between upload and metadata write leaves an orphaned chunk; a crash between metadata write and FUSE ACK means the client retries harmlessly.

---

## Metadata Database Schema (SQLite)

```sql
CREATE TABLE files (
    id          TEXT PRIMARY KEY,  -- UUID
    virtual_path TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    sha256_full TEXT NOT NULL
);

CREATE TABLE chunks (
    id          TEXT PRIMARY KEY,  -- UUID
    file_id     TEXT NOT NULL REFERENCES files(id),
    sequence    INTEGER NOT NULL,  -- reassembly order
    size_bytes  INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    encrypted_size INTEGER NOT NULL
);

CREATE TABLE chunk_locations (
    chunk_id    TEXT NOT NULL REFERENCES chunks(id),
    provider_id TEXT NOT NULL REFERENCES providers(id),
    remote_path TEXT NOT NULL,
    upload_confirmed_at INTEGER  -- NULL = in-flight or failed
);

CREATE TABLE providers (
    id                  TEXT PRIMARY KEY,
    type                TEXT NOT NULL,   -- 'drive', 'dropbox', 'onedrive', etc.
    display_name        TEXT NOT NULL,
    quota_total_bytes   INTEGER,
    quota_free_bytes    INTEGER,
    quota_polled_at     INTEGER,
    rate_limited_until  INTEGER          -- NULL = not rate limited
);
```

**SQLite config:** Enable WAL mode (`PRAGMA journal_mode=WAL`) immediately after opening. This prevents crashes from corrupting the DB and allows concurrent reads during writes.

**Metadata backup:** On every N writes (configurable, default 100), serialize the DB and upload it encrypted to one of the cloud accounts. This is your disaster recovery. Without the metadata DB, encrypted chunks in the cloud are unrecoverable.

**`upload_confirmed_at = NULL`** is your integrity signal. A background job (runs every 15 min) looks for chunk_locations rows where this is NULL and older than 5 minutes, and retries the upload.

---

## Space Broker Logic

```
function assign_chunk(chunk_size_bytes) -> provider_id:
    candidates = providers where:
        quota_free_bytes > chunk_size_bytes
        AND (rate_limited_until IS NULL OR rate_limited_until < now())
    
    if candidates is empty:
        raise StorageFullError
    
    return candidates.sort_by(quota_free_bytes DESC).first()
```

Start with "most free space wins." Future improvements:
- Weight by upload speed (measured, cached)
- Prefer keeping chunks from the same directory on the same provider (reduces API calls for directory listings)
- Round-robin within a quota tier to spread API call load

**Quota polling:** Poll all provider quotas every 15 minutes via rclone RC `core/stats`. Cache results locally. Never make a live quota API call during a write — it'll add 200-500ms latency per chunk.

---

## Encryption Design

- **Algorithm:** AES-256-GCM (authenticated encryption — detects tampering)
- **Key derivation:** PBKDF2-HMAC-SHA256 with 310,000 iterations (OWASP 2023 recommendation)
- **Key hierarchy:**
  - Master password → PBKDF2 → master key (stored encrypted in `~/.polydrive/config.toml`)
  - Per-file key derived from master key + file UUID (never reuse keys across files)
  - Per-chunk nonce: 96-bit random, prepended to the encrypted chunk blob
- **Chunk blob format on disk:** `[12 bytes nonce][16 bytes GCM tag][N bytes ciphertext]`
- **What to store in config:** The PBKDF2 salt + the master key encrypted with itself. User provides master password at daemon startup; key is held in memory only, never written to disk in plaintext.

---

## Deletion & Garbage Collection

Deletion is more complex than it looks:

1. User deletes `/myvideo.mp4`
2. Look up all chunk_locations rows for that file
3. For each chunk: call rclone RC to delete the remote file
4. If any delete fails (API error, rate limit): mark it as `pending_delete` in a new table
5. Remove metadata rows
6. Background GC retries `pending_delete` rows

**Orphan GC (run weekly):** List all files in each provider account. Cross-reference against `chunk_locations`. Any remote file with no corresponding metadata row is an orphaned chunk — delete it. This handles crash scenarios where a chunk was uploaded but metadata was never written.

---

## Rate Limit Handling

Each provider has different limits:
- **Google Drive free:** 750 GB/day upload, ~10 req/sec per user
- **Dropbox:** 25 GB/day for developer apps, burst limits vary
- **OneDrive:** 10,000 requests per 10 minutes per app

Your daemon needs to:
1. Track requests-per-minute per provider account (in-memory counter)
2. When rclone RC returns a 429 or rate limit error, set `rate_limited_until = now() + backoff` in the providers table
3. Space broker skips rate-limited providers
4. Exponential backoff on retry (1s, 2s, 4s, 8s... capped at 5 min)

---

## User-Facing Mount Options

### macOS — WebDAV first, FUSE later

WebDAV is easier to ship on macOS for v1:
- Your daemon runs a WebDAV server on `localhost:8765`
- User mounts via Finder: Go → Connect to Server → `http://localhost:8765`
- Or automate the mount via `mount_webdav` in your LaunchAgent
- Zero kernel extensions required (macFUSE requires a reboot + security exception)

For v2+: add macFUSE support via `go-fuse` or `cgofuse`. Gives a proper Finder sidebar entry and better performance. Requires the user to install macFUSE separately (similar to how Parallels or similar tools do it).

### Linux

FUSE is first-class on Linux, no complications. Use `go-fuse` or `bazil.org/fuse`.

### Windows

WinFsp (Windows equivalent of FUSE). Distributable installer can bundle WinFsp. Alternatively, WebDAV works natively on Windows too — simpler for v1.

---

## Distributable Package Layout

```
polydrive-v0.1.0-macos-arm64.tar.gz
├── polydrive              # your Go daemon binary (~8 MB, no runtime)
├── rclone                 # official pre-built rclone binary for this platform
└── install.sh             # sets up LaunchAgent, creates ~/.polydrive/, opens browser

~/.polydrive/              # created on first run
├── config.toml            # master password hash, preferences
├── metadata.db            # SQLite — the critical file
├── metadata.db.wal        # SQLite WAL journal
└── rclone.conf            # rclone remote configs (written by your daemon)
```

---

## Language & Tech Stack

| Component | Choice | Reason |
|---|---|---|
| Daemon | **Go** | Single static binary, no runtime, trivial cross-compile, same ecosystem as rclone |
| Web UI | **React + Vite** | Served as static files by daemon on localhost:8765 |
| DB | **SQLite** (WAL mode) | Zero-config, embedded, transactional, sufficient for local use |
| FUSE (Linux) | `bazil.org/fuse` or `go-fuse` | Mature Go FUSE bindings |
| WebDAV (v1) | `golang.org/x/net/webdav` | Stdlib-adjacent, simple |
| Encryption | Go `crypto/aes` + `crypto/cipher` | Standard library, no external dep |
| Release CI | **GoReleaser** | One config file → binaries for all platforms + GitHub release artifacts |

**Why not Python:** PyInstaller produces 100MB+ bundles, false-positive AV hits on Windows, painful cross-compile story, slower startup for a daemon process.

**Why not Electron/Tauri:** Unnecessary weight. The UI is a browser tab served by your daemon — no desktop framework needed for v1.

---

## Repository Structure

```
polydrive/
├── cmd/
│   └── polydrive/
│       └── main.go              # entry point: parse flags, start daemon
├── internal/
│   ├── daemon/
│   │   ├── daemon.go            # lifecycle: start rclone, start WebDAV/FUSE, handle shutdown
│   │   └── rclone_manager.go    # spawn rclone RC, health check, restart on crash
│   ├── vfs/
│   │   ├── webdav.go            # WebDAV server (v1)
│   │   └── fuse.go              # FUSE mount (v2)
│   ├── chunker/
│   │   ├── chunker.go           # split file → []Chunk
│   │   ├── assembler.go         # []Chunk → file stream
│   │   └── crypto.go            # AES-256-GCM encrypt/decrypt
│   ├── broker/
│   │   └── broker.go            # space allocation: assign chunk → provider
│   ├── metadata/
│   │   ├── db.go                # open DB, run migrations
│   │   ├── schema.sql           # embedded schema
│   │   └── queries.go           # typed query functions (no raw SQL elsewhere)
│   └── rclonerc/
│       ├── client.go            # HTTP client for rclone RC API
│       ├── operations.go        # putfile, deletefile, copyfile wrappers
│       └── quota.go             # quota polling, caching
├── web/
│   ├── src/
│   │   ├── App.tsx
│   │   ├── pages/
│   │   │   ├── Setup.tsx        # first-run wizard
│   │   │   ├── Dashboard.tsx    # pooled capacity overview
│   │   │   └── Accounts.tsx     # add/remove provider accounts
│   │   └── api.ts               # fetch wrapper for daemon REST API
│   └── dist/                    # built output, embedded in Go binary via embed.FS
├── scripts/
│   └── download-rclone.sh       # fetch correct rclone binary for current platform
├── .goreleaser.yaml
├── go.mod
├── go.sum
└── README.md
```

**Embed the web UI in the binary:** Use Go's `//go:embed web/dist/*` to bake the compiled React app directly into the daemon binary. No separate web files to ship — just one binary that serves its own UI.

---

## OAuth Strategy for Distributable

**The problem:** Adding a Google Drive account normally requires the user to create a Google Cloud project. That's a 20-step process that kills onboarding for anyone non-technical.

**Option A — Ship your own OAuth credentials (recommended for public release):**
- Create one Google Cloud project, one Dropbox app, one Microsoft Azure app registration
- Bake the client ID + client secret into your binary
- Users authorize *your app* to access their accounts
- This is how every cloud desktop client works (Dropbox client, rclone itself, etc.)
- For open source: client secrets in a public repo are technically visible, but Google's installed-app policy accepts this; it's mitigated by domain verification + app review
- You'll need to go through Google OAuth app verification for `drive.file` scope (takes ~1 week, requires a privacy policy URL)

**Option B — User brings their own credentials (acceptable for v1 personal/technical use):**
- README explains how to create a Google Cloud project and paste client ID/secret into config
- Fine for homelab/technical audience
- Not viable for general users

**Recommendation:** Start with Option B for v1 (personal use + sharing with technical friends). Plan Option A for the first public GitHub release.

---

## First-Run Onboarding Flow

```
1. User runs install.sh (or double-clicks .dmg)
2. Daemon starts, opens http://localhost:8765 in browser
3. Setup wizard:
   a. "Choose a master password" — this encrypts everything
      (warn: if lost, data is unrecoverable — no reset)
   b. "Add your first cloud account" → select provider → OAuth browser flow
   c. Show quota discovered: "15 GB available on Google Drive (your.email@gmail.com)"
   d. "Add another account?" → repeat step b
   e. "Mount your drive" → shows total pooled capacity
   f. Done — drive appears in Finder/Files
4. System tray icon (optional v2) shows mount status + capacity used
```

---

## Phased Roadmap

### v0 — Proof of concept (personal, no UI)
- [ ] Go daemon skeleton: starts rclone RC as child process, kills it on exit
- [ ] Single provider (Google Drive) added via manual rclone config edit
- [ ] Minimal WebDAV server that intercepts writes
- [ ] Chunker: split file at 4 MB boundaries, SHA-256 hash each chunk
- [ ] Encryption: AES-256-GCM per chunk with hardcoded test key
- [ ] Upload: rclone RC `operations/putfile` for each chunk
- [ ] Metadata: SQLite write of chunk→remote_path mapping
- [ ] Read: metadata lookup → download chunks → decrypt → reassemble → stream
- [ ] **Test:** write a 20 MB file, read it back, verify SHA-256 matches

### v1 — Multi-account, distributable for self
- [ ] Space broker: real quota polling, route chunks by free space
- [ ] Multi-account support (2+ Google, 1+ Dropbox)
- [ ] Proper key derivation (PBKDF2, master password at startup)
- [ ] Metadata backup: periodic encrypted DB snapshot to cloud
- [ ] Basic web UI: capacity dashboard + add account (OAuth flow)
- [ ] macOS LaunchAgent: auto-start on login
- [ ] GoReleaser config: build macOS ARM + Intel binaries
- [ ] Rate limit handling: 429 detection, exponential backoff, provider skip
- [ ] Deletion: chunk cleanup across all accounts

### v2 — Polish for GitHub release
- [ ] First-run setup wizard in web UI
- [ ] Ship own OAuth credentials (Google + Dropbox + OneDrive apps registered)
- [ ] Background GC: orphan chunk detection and cleanup
- [ ] Integrity checker: background job verifies random chunks against SHA-256
- [ ] Graceful quota-full handling: clear error UX, suggest adding more accounts
- [ ] Linux FUSE support
- [ ] Windows WebDAV support
- [ ] Homebrew tap via GoReleaser

### v3 — Nice to have
- [ ] macFUSE for proper Finder sidebar integration on macOS
- [ ] Configurable chunk size (default 4 MB, range 1–32 MB)
- [ ] Bandwidth throttling (don't saturate upload during work hours)
- [ ] Rebalance command: redistribute chunks when you add a new account
- [ ] Selective sync: only some local folders backed by polydrive
- [ ] System tray icon with mount status

---

## Key Design Decisions & Rationale

**Why 4 MB chunks?**
Balances API call overhead vs granularity. A 1 GB file = 256 API calls (acceptable). Too small = thousands of calls for large files hitting rate limits. Too large = less flexible placement, small files waste quota. Configurable in v3.

**Why SQLite and not something embedded like bbolt?**
SQLite is transactional, supports WAL, is inspectable with standard tooling, and has a mature Go driver. The metadata DB needs to survive crashes cleanly — SQLite WAL mode is well-proven for this. bbolt/badger are faster but overkill and less crash-safe for relational data.

**Why WebDAV before FUSE on macOS?**
macFUSE requires a kernel extension and a reboot + security preference change. WebDAV works out of the box in macOS Ventura+. Better first-run experience for less technical users. FUSE is strictly better (performance, features) so it comes in v2 once the core is stable.

**Why not Python?**
PyInstaller bundles are 100MB+, trigger false-positive antivirus on Windows, have slow startup, and cross-compilation is painful. Go gives an 8 MB static binary with trivial `GOOS/GOARCH` cross-compilation.

**Why not use rclone's built-in Chunker + Union combo?**
rclone Union routes whole files to upstreams — a file must fit on one remote. This is the core gap this project fills. rclone Chunker + Union don't interoperate for cross-remote chunking of single files. We use rclone only as a provider transport layer, not for its VFS or chunking logic.

---

## Naming Ideas

- `tessera` — from tesserae, the individual tiles of a mosaic. Communicates the "many pieces, one surface" concept cleanly.
- `mosaicfs` — more explicit about the filesystem aspect
- `polydrive` — straightforward, descriptive
- `archipelago` — islands of storage unified into one

**Recommended:** `tessera` — memorable, unique enough to get a clean GitHub namespace, implies the concept without being literal about cloud storage.

---

## ToS & Legal Posture

**What's fine:**
- Building software that uses official cloud storage APIs
- Combining accounts you legitimately hold (work + personal + family)
- Open sourcing the tool on GitHub

**What to avoid in README/marketing:**
- Framing it as "create fake accounts to get free storage"
- Any suggestion to violate provider ToS

**Safe framing:**
> "Tessera lets you combine your existing cloud storage accounts — personal, work, shared — into a single unified drive with cross-account file chunking."

This is an accurate description of what the tool does and doesn't imply ToS circumvention. The tool itself is neutral; how users choose to use it is up to them.

---

## Critical Failure Modes to Handle

| Failure | Consequence | Mitigation |
|---|---|---|
| Metadata DB lost/corrupted | All cloud chunks unrecoverable | WAL mode + periodic encrypted DB backup to cloud |
| Upload succeeds, metadata write fails | Orphaned chunk | Write metadata only after upload confirmed; GC finds orphans weekly |
| Account full mid-upload | Partial file | Atomic chunk routing: check quota before assigning; rollback on failure |
| rclone crashes | Uploads stall | Daemon monitors rclone PID, restarts automatically |
| Rate limit hit mid-upload | Upload fails | 429 detection → mark provider as limited → retry with backoff on next available provider |
| Master password lost | All data permanently inaccessible | Warn clearly at setup: no recovery mechanism exists by design |

---

## Development Environment Setup

```bash
# Prerequisites
brew install go node rclone

# Clone and init
git clone https://github.com/yourname/tessera
cd tessera
go mod init github.com/yourname/tessera

# Key Go dependencies
go get golang.org/x/net/webdav         # WebDAV server
go get github.com/mattn/go-sqlite3     # SQLite driver (CGo)
go get bazil.org/fuse                  # FUSE (Linux/macOS later)

# Or if you want pure-Go SQLite (no CGo, easier cross-compile):
go get modernc.org/sqlite              # recommended for cross-platform builds

# Web UI
cd web && npm create vite@latest . -- --template react-ts
npm install && npm run build

# Run rclone in RC mode manually for dev/testing
rclone rcd --rc-addr localhost:5572 --rc-no-auth --config /tmp/test-rclone.conf

# Test an rclone RC call
curl http://localhost:5572/core/version
```

**Note on SQLite + CGo:** `modernc.org/sqlite` is a pure-Go SQLite port — strongly recommended over `go-sqlite3` for this project because it cross-compiles without a C toolchain. GoReleaser cross-compilation with CGo requires complex Docker-based cross-compilers. `modernc.org/sqlite` eliminates that entirely.

---

## Copilot Chat Starter Prompts

Once you've fed this document in as context, suggested starting points:

1. **"Scaffold the Go daemon skeleton with rclone RC child process management"** — gets you `internal/daemon/rclone_manager.go` with spawn, health check, and graceful shutdown logic.

2. **"Implement the chunker package — split an io.Reader into 4MB chunks, hash each, return []Chunk"** — pure logic, no dependencies, good first PR.

3. **"Implement the rclonerc client package with putfile, deletefile, and quota polling"** — wraps the rclone RC HTTP API.

4. **"Implement the SQLite metadata layer with the schema from this doc and typed query functions"** — gets you a working DB layer with WAL mode and the four tables.

5. **"Implement a minimal WebDAV server that intercepts writes and reads and calls the chunker + metadata layer"** — this is the integration point that ties everything together.

---

*End of planning document.*
