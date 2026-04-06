// Package engine is the core orchestrator for all pdrive file operations:
// upload, download, delete, rename, deduplication, and metadata management.
//
// Upload pipeline:
//  1. File is hashed (SHA-256) for content-hash deduplication.
//  2. If a matching file already exists, chunk metadata is cloned (zero upload).
//  3. Otherwise the file is split into chunks via [chunker.ChunkReader],
//     each chunk is AES-256-GCM encrypted, then uploaded concurrently with
//     retry and exponential backoff through the [CloudStorage] interface.
//  4. Files larger than [AsyncWriteThreshold] upload in the background so
//     WebDAV PUT returns quickly.
//
// Download pipeline:
//  1. Chunks are downloaded sequentially, decrypted, SHA-256 verified,
//     and written to a temp file.
//  2. A full-file hash check is performed before returning.
//
// The engine also manages:
//   - Debounced encrypted metadata DB backup to all providers
//   - Orphan GC (cloud objects with no DB record, and vice versa)
//   - Failed-deletion retry queue
//   - Telemetry counters (files/chunks/bytes uploaded, downloads, dedup hits)
package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/chunker"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/rclonerc"
)

// CloudStorage is the interface the Engine uses to talk to cloud providers.
// *rclonerc.Client satisfies this interface in production; tests inject a fake.
type CloudStorage interface {
	PutFile(remote, remotePath string, data io.Reader) error
	GetFile(remote, remotePath string) (io.ReadCloser, error)
	DeleteFile(remote, remotePath string) error
	ListDir(remote, remotePath string) ([]rclonerc.ListItem, error)
	Cleanup(remote string) error
	Mkdir(remote, remotePath string) error
}

const chunkRemoteDir = "pdrive-chunks"

// uploadProgress tracks in-flight async upload state.
type uploadProgress struct {
	VirtualPath    string
	TotalChunks    int
	ChunksUploaded int
	SizeBytes      int64
	StartedAt      time.Time
	Failed         bool
}

// UploadProgressInfo is the exported snapshot of an in-flight upload.
type UploadProgressInfo struct {
	VirtualPath    string
	TotalChunks    int
	ChunksUploaded int
	SizeBytes      int64
	StartedAt      time.Time
	Failed         bool
}

// Engine orchestrates file write and read operations.
type Engine struct {
	db           *metadata.DB
	dbPath       string
	rc           CloudStorage
	broker       *broker.Broker
	encKey       []byte        // AES-256 key (32 bytes)
	uploadTokens chan struct{} // token bucket: limits upload API calls per second
	fileGate     chan struct{} // serializes file-level uploads (only 1 file at a time)
	// maxChunkRetries overrides maxUploadRetries when > 0 (used by tests to
	// avoid long exponential-backoff delays).
	maxChunkRetries int

	// overrideChunkSize, when > 0, replaces the dynamic chunk-size calculation.
	overrideChunkSize int

	uploadsMu sync.RWMutex
	uploads   map[string]*uploadProgress // fileID → progress

	// asyncWG tracks in-flight async upload goroutines for graceful shutdown.
	asyncWG sync.WaitGroup

	// closeCh is closed by Close() to stop the rate-limit refill goroutine.
	closeCh chan struct{}

	// backupTimer/backupMu handle debounced metadata DB backups.
	backupTimer *time.Timer
	backupMu    sync.Mutex

	// uploading is nonzero while a file upload is in progress.
	// BackupDB defers work while uploading to avoid competing for provider quota.
	uploading atomic.Int32

	// saltPath is the local path to the Argon2id salt file (enc.salt).
	// When set, BackupDB uploads the salt alongside the encrypted DB.
	saltPath string

	// Telemetry counters (atomic).
	filesUploaded   atomic.Int64
	filesDownloaded atomic.Int64
	filesDeleted    atomic.Int64
	chunksUploaded  atomic.Int64
	bytesUploaded   atomic.Int64
	bytesDownloaded atomic.Int64
	dedupHits       atomic.Int64
}

const (
	// uploadRatePerSec is the maximum number of chunk-upload API calls per second
	// across all providers. With operations/copyfile + _async, each call just
	// starts a lightweight rclone job; the actual cloud API calls (with their
	// own backoff) run inside rclone.  6/s lets us saturate fast connections
	// without overwhelming the local rclone RC server.
	uploadRatePerSec = 6
	uploadRateBurst  = 10 // initial burst before the ticker kicks in
)

// NewEngine creates a new engine backed by an rclone RC client.
func NewEngine(db *metadata.DB, dbPath string, rc *rclonerc.Client, b *broker.Broker, encKey []byte) *Engine {
	const burst = 10
	e := newEngine(db, dbPath, rc, b, encKey, burst, uploadRatePerSec)
	return e
}

// NewEngineWithRate creates an Engine with a custom API rate limit (tokens per second).
// A ratePerSec of 0 or less uses the default (8/s).
func NewEngineWithRate(db *metadata.DB, dbPath string, rc *rclonerc.Client, b *broker.Broker, encKey []byte, ratePerSec int) *Engine {
	const burst = 10
	if ratePerSec <= 0 {
		ratePerSec = uploadRatePerSec
	}
	return newEngine(db, dbPath, rc, b, encKey, burst, ratePerSec)
}

// NewEngineWithCloud creates an Engine with any CloudStorage implementation.
// Intended for testing and tooling that needs an alternative storage backend.
// Uses a larger initial token burst (256) so that test-speed uploads are never
// token-starved.
func NewEngineWithCloud(db *metadata.DB, dbPath string, rc CloudStorage, b *broker.Broker, encKey []byte) *Engine {
	const burst = 256
	return newEngine(db, dbPath, rc, b, encKey, burst, uploadRatePerSec)
}

func newEngine(db *metadata.DB, dbPath string, rc CloudStorage, b *broker.Broker, encKey []byte, burst, ratePerSec int) *Engine {
	e := &Engine{
		db:           db,
		dbPath:       dbPath,
		rc:           rc,
		broker:       b,
		encKey:       encKey,
		uploadTokens: make(chan struct{}, burst),
		fileGate:     make(chan struct{}, 1),
		uploads:      make(map[string]*uploadProgress),
		closeCh:      make(chan struct{}),
	}
	for i := 0; i < burst; i++ {
		e.uploadTokens <- struct{}{}
	}
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(ratePerSec))
		defer ticker.Stop()
		for {
			select {
			case <-e.closeCh:
				return
			case <-ticker.C:
				select {
				case e.uploadTokens <- struct{}{}:
				default:
				}
			}
		}
	}()
	return e
}

// Close stops the rate-limit refill goroutine and waits up to 30 seconds for
// any in-flight async uploads to complete. Safe to call multiple times.
func (e *Engine) Close() {
	select {
	case <-e.closeCh:
		return // already closed
	default:
		close(e.closeCh)
	}

	// Wait for in-flight async uploads with a timeout.
	done := make(chan struct{})
	go func() {
		e.asyncWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		slog.Info("all async uploads finished")
	case <-time.After(30 * time.Second):
		slog.Warn("shutdown timeout: some async uploads may not have completed")
	}
}

// DB returns the underlying metadata database. Exposed for test helpers that
// need to inspect or mutate DB state alongside engine operations.
func (e *Engine) DB() *metadata.DB { return e.db }

// SetChunkSize overrides the dynamic chunk-size calculation with a fixed value.
// Pass 0 to revert to the default dynamic behaviour.
func (e *Engine) SetChunkSize(bytes int) { e.overrideChunkSize = bytes }

// SetSaltPath sets the local path to the Argon2id salt file.
// When set, BackupDB also uploads the salt to every provider.
func (e *Engine) SetSaltPath(p string) { e.saltPath = p }

// SetMaxChunkRetries overrides the default retry count for chunk uploads.

// EnsureRemoteDirs creates the pdrive-chunks and pdrive-meta directories on
// every configured provider. This is a no-op when the directories already
// exist and is essential after a user deletes the remote folders manually.
func (e *Engine) EnsureRemoteDirs() {
	providers, err := e.db.GetAllProviders()
	if err != nil || len(providers) == 0 {
		return
	}
	for _, p := range providers {
		if err := e.rc.Mkdir(p.RcloneRemote, "pdrive-chunks"); err != nil {
			slog.Warn("ensure remote dir failed", "provider", p.DisplayName, "dir", "pdrive-chunks", "error", err)
		}
		if err := e.rc.Mkdir(p.RcloneRemote, "pdrive-meta"); err != nil {
			slog.Warn("ensure remote dir failed", "provider", p.DisplayName, "dir", "pdrive-meta", "error", err)
		}
	}
	slog.Info("remote directories ensured on all providers")
}
func (e *Engine) SetMaxChunkRetries(n int) { e.maxChunkRetries = n }

// MetricsSnapshot is a point-in-time snapshot of engine telemetry counters.
type MetricsSnapshot struct {
	FilesUploaded   int64 `json:"files_uploaded"`
	FilesDownloaded int64 `json:"files_downloaded"`
	FilesDeleted    int64 `json:"files_deleted"`
	ChunksUploaded  int64 `json:"chunks_uploaded"`
	BytesUploaded   int64 `json:"bytes_uploaded"`
	BytesDownloaded int64 `json:"bytes_downloaded"`
	DedupHits       int64 `json:"dedup_hits"`
}

// Metrics returns a snapshot of the engine's telemetry counters.
func (e *Engine) Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		FilesUploaded:   e.filesUploaded.Load(),
		FilesDownloaded: e.filesDownloaded.Load(),
		FilesDeleted:    e.filesDeleted.Load(),
		ChunksUploaded:  e.chunksUploaded.Load(),
		BytesUploaded:   e.bytesUploaded.Load(),
		BytesDownloaded: e.bytesDownloaded.Load(),
		DedupHits:       e.dedupHits.Load(),
	}
}

// chunkSize returns the chunk size to use for a file of the given size.
func (e *Engine) chunkSize(fileSize int64) int {
	if e.overrideChunkSize > 0 {
		return e.overrideChunkSize
	}
	return chunker.ChunkSizeForFile(fileSize)
}

// workersForChunkSize returns an appropriate concurrency level for the given
// chunk size.  PutFile writes data to a temp file and delegates the actual
// upload to rclone (operations/copyfile + _async), so peak memory is low;
// the limit is tuned to saturate typical broadband connections.
func workersForChunkSize(chunkSize int) int {
	switch {
	case chunkSize >= 32*1024*1024: // ≥ 32 MB → 6 workers
		return 6
	case chunkSize >= 8*1024*1024: // ≥ 8 MB → 8 workers
		return 8
	default: // < 8 MB → 10 workers
		return maxUploadWorkers
	}
}

// WriteFile writes a file to the virtual filesystem, chunking and encrypting it.
// For small files or when data is already in memory.
func (e *Engine) WriteFile(virtualPath string, data []byte) error {
	return e.WriteFileStream(virtualPath, bytes.NewReader(data), int64(len(data)))
}

const (
	// maxUploadWorkers is the default (small-chunk) concurrency limit; see
	// workersForChunkSize for how this scales down for larger chunks.
	maxUploadWorkers = 10
	// maxUploadRetries is the number of retry attempts for a failed chunk upload.
	maxUploadRetries = 5
	// AsyncWriteThreshold: files larger than this are uploaded in the background
	// so the WebDAV PUT returns quickly and Finder doesn't time out.
	AsyncWriteThreshold = 4 * 1024 * 1024 // 4 MB
)

// chunkMeta holds metadata for a single uploaded chunk.
type chunkMeta struct {
	chunkID       string
	sequence      int
	size          int
	sha256        string
	encryptedSize int
	providerID    string
	remotePath    string
}

// WriteFileStream writes a file from a stream synchronously (hash + upload + metadata).
func (e *Engine) WriteFileStream(virtualPath string, r io.ReadSeeker, size int64) error {
	// Delete any existing record (complete or pending) at this path so the
	// INSERT below never hits a UNIQUE constraint on virtual_path.
	existing, _ := e.db.GetFileByPath(virtualPath)
	if existing != nil {
		locs, _ := e.db.GetChunkLocationsForFile(existing.ID)
		e.db.DeleteFile(existing.ID) //nolint:errcheck
		if len(locs) > 0 {
			go e.deleteCloudChunks(locs)
		}
	}

	fileID := uuid.New().String()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, r); err != nil {
		return err
	}
	fullHashStr := hex.EncodeToString(hasher.Sum(nil))
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Content-hash dedup: if a completed file with the same SHA256 already
	// exists, clone its chunk metadata instead of re-uploading.
	if donor, _ := e.db.GetCompleteFileByHash(fullHashStr); donor != nil {
		return e.cloneFileFromDonor(donor, fileID, virtualPath, size, fullHashStr)
	}

	metas, err := e.uploadChunks(r, fileID, size, nil)
	if err != nil {
		return err
	}
	// Insert the file record FIRST — chunk records have a FK to files.id so the
	// parent row must exist before we insert children.
	now := time.Now().Unix()
	if err := e.db.InsertFile(&metadata.File{
		ID:          fileID,
		VirtualPath: virtualPath,
		SizeBytes:   size,
		CreatedAt:   now,
		ModifiedAt:  now,
		SHA256Full:  fullHashStr,
		UploadState: "complete",
		TmpPath:     nil,
	}); err != nil {
		return err
	}
	if err := e.insertChunkMetadata(fileID, metas); err != nil {
		e.db.DeleteFile(fileID) //nolint:errcheck
		return err
	}
	slog.Info("file written", "path", virtualPath, "size", size, "chunks", len(metas))
	e.filesUploaded.Add(1)
	e.bytesUploaded.Add(size)
	e.scheduleBackup()
	return nil
}

// WriteFileAsync hashes the file synchronously, writes a pending DB record
// (so uploads survive a daemon restart via ResumeUploads), then uploads chunks
// in a background goroutine. The file stays invisible in the WebDAV listing
// until the upload completes (ListFiles/GetFileByPath filter pending records).
// The caller must NOT close or remove tmpFile; the engine takes ownership.
func (e *Engine) WriteFileAsync(virtualPath string, tmpFile *os.File, tmpPath string, size int64) error {
	// Hash synchronously so we can write the pending DB record now.
	hasher := sha256.New()
	if _, err := io.Copy(hasher, tmpFile); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("hashing file: %w", err)
	}
	fullHashStr := hex.EncodeToString(hasher.Sum(nil))
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("rewinding after hash: %w", err)
	}

	// Delete any existing file at this path.
	existing, _ := e.db.GetFileByPath(virtualPath)
	if existing != nil {
		locs, _ := e.db.GetChunkLocationsForFile(existing.ID)
		e.db.DeleteFile(existing.ID) //nolint:errcheck
		if len(locs) > 0 {
			go e.deleteCloudChunks(locs)
		}
	}

	fileID := uuid.New().String()
	now := time.Now().Unix()
	dbTmpPath := tmpPath
	if err := e.db.InsertFile(&metadata.File{
		ID:          fileID,
		VirtualPath: virtualPath,
		SizeBytes:   size,
		CreatedAt:   now,
		ModifiedAt:  now,
		SHA256Full:  fullHashStr,
		UploadState: "pending",
		TmpPath:     &dbTmpPath,
	}); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("inserting pending file record: %w", err)
	}

	e.asyncWG.Add(1)
	go func() {
		defer e.asyncWG.Done()
		defer tmpFile.Close()
		defer os.Remove(tmpPath)
		defer func() {
			e.uploadsMu.Lock()
			delete(e.uploads, fileID)
			e.uploadsMu.Unlock()
		}()

		metas, err := e.uploadChunksTracked(tmpFile, fileID, virtualPath, size)
		if err != nil {
			slog.Error("background upload failed", "path", virtualPath, "error", err)
			// Remove the pending record so the path is free for retry and
			// the file doesn't appear stuck/unreadable.
			if delErr := e.db.DeleteFile(fileID); delErr != nil {
				slog.Error("failed to remove pending record after upload failure",
					"path", virtualPath, "error", delErr)
			}
			return
		}
		if err := e.insertChunkMetadata(fileID, metas); err != nil {
			slog.Error("failed to insert chunk metadata", "path", virtualPath, "error", err)
			if delErr := e.db.DeleteFile(fileID); delErr != nil {
				slog.Error("failed to remove pending record after metadata failure",
					"path", virtualPath, "error", delErr)
			}
			return
		}
		if err := e.db.SetUploadComplete(fileID); err != nil {
			slog.Error("failed to mark upload complete", "path", virtualPath, "error", err)
			return
		}
		slog.Info("file written", "path", virtualPath, "size", size, "chunks", len(metas))
		e.filesUploaded.Add(1)
		e.bytesUploaded.Add(size)
		e.scheduleBackup()
	}()
	return nil
}

// uploadChunks reads, encrypts, and uploads chunks concurrently with retry.
// Chunk size is chosen dynamically based on fileSize to keep the total chunk
// count near ~100, reducing cloud API calls for large files.
// onChunkUploaded, if non-nil, is called after each successful chunk upload.
// Returns the ordered slice of chunk metadata on success.
func (e *Engine) uploadChunks(r io.ReadSeeker, fileID string, fileSize int64, onChunkUploaded func()) ([]chunkMeta, error) {
	chunkSize := e.chunkSize(fileSize)
	workers := workersForChunkSize(chunkSize)
	slog.Debug("upload plan", "fileSize", fileSize, "chunkSize", chunkSize, "workers", workers)
	cr := chunker.NewChunkReader(r, chunkSize)

	var (
		metas    []chunkMeta
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
		sem      = make(chan struct{}, workers)
	)

	for chunkCount := 0; ; chunkCount++ {
		mu.Lock()
		uploadErr := firstErr
		mu.Unlock()
		if uploadErr != nil {
			break
		}

		chunk, err := cr.Next()
		if err != nil {
			wg.Wait()
			return nil, fmt.Errorf("reading chunk %d: %w", chunkCount, err)
		}
		if chunk == nil {
			break
		}

		encrypted, err := chunker.Encrypt(e.encKey, chunk.Data)
		if err != nil {
			wg.Wait()
			return nil, fmt.Errorf("encrypting chunk %d: %w", chunk.Sequence, err)
		}
		chunk.Data = nil

		providerID, err := e.broker.AssignChunk(int64(len(encrypted)))
		if err != nil {
			wg.Wait()
			return nil, fmt.Errorf("assigning chunk %d: %w", chunk.Sequence, err)
		}

		provider, err := e.db.GetProvider(providerID)
		if err != nil || provider == nil {
			wg.Wait()
			return nil, fmt.Errorf("getting provider %s: %w", providerID, err)
		}

		remotePath := chunkRemoteDir + "/" + chunk.ID
		metas = append(metas, chunkMeta{
			chunkID:       chunk.ID,
			sequence:      chunk.Sequence,
			size:          chunk.Size,
			sha256:        chunk.SHA256,
			encryptedSize: len(encrypted),
			providerID:    providerID,
			remotePath:    remotePath,
		})

		sem <- struct{}{}
		wg.Add(1)
		go func(enc []byte, remote string, prov *metadata.Provider, seq int) {
			defer func() { <-sem; wg.Done() }()
			retries := maxUploadRetries
			if e.maxChunkRetries > 0 {
				retries = e.maxChunkRetries
			}
			var lastErr error
			for attempt := 0; attempt < retries; attempt++ {
				if attempt > 0 {
					backoff := time.Duration(1<<uint(attempt)) * time.Second
					// Triple backoff when the provider is rate-limiting us.
					if rclonerc.IsRateLimited(lastErr) {
						backoff *= 3
					}
					if backoff > 30*time.Second {
						backoff = 30 * time.Second
					}
					// Add up to 50% jitter to prevent thundering-herd retries.
					jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
					backoff += jitter
					slog.Warn("retrying chunk upload",
						"seq", seq, "attempt", attempt+1, "backoff", backoff)
					time.Sleep(backoff)
				}
				// Acquire a rate-limit token before each API call (blocks briefly
				// when the bucket is empty) to avoid bursting past provider quotas.
				<-e.uploadTokens
				if err := e.rc.PutFile(prov.RcloneRemote, remote, bytes.NewReader(enc)); err != nil {
					lastErr = err
					continue
				}
				slog.Debug("chunk uploaded", "seq", seq, "provider", prov.DisplayName)
				e.chunksUploaded.Add(1)
				if onChunkUploaded != nil {
					onChunkUploaded()
				}
				return // success
			}
			mu.Lock()
			if firstErr == nil {
				firstErr = fmt.Errorf("uploading chunk %d to %s after %d retries: %w",
					seq, prov.DisplayName, retries, lastErr)
			}
			mu.Unlock()
		}(encrypted, remotePath, provider, chunk.Sequence)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return metas, nil
}

// insertChunkMetadata writes chunk and chunk_location records inside a single
// transaction so that either all records are committed or none are.
func (e *Engine) insertChunkMetadata(fileID string, metas []chunkMeta) error {
	tx, err := e.db.Conn().Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	confirmTime := time.Now().Unix()
	for _, m := range metas {
		if _, err := tx.Exec(
			`INSERT INTO chunks (id, file_id, sequence, size_bytes, sha256, encrypted_size) VALUES (?, ?, ?, ?, ?, ?)`,
			m.chunkID, fileID, m.sequence, m.size, m.sha256, m.encryptedSize,
		); err != nil {
			return fmt.Errorf("inserting chunk record: %w", err)
		}
		if _, err := tx.Exec(
			`INSERT INTO chunk_locations (chunk_id, provider_id, remote_path, upload_confirmed_at) VALUES (?, ?, ?, ?)`,
			m.chunkID, m.providerID, m.remotePath, confirmTime,
		); err != nil {
			return fmt.Errorf("inserting chunk location: %w", err)
		}
	}
	return tx.Commit()
}

// cloneFileFromDonor creates a new file record that shares the same cloud
// chunks as the donor file (content-hash dedup). No data is uploaded.
// All inserts are wrapped in a single transaction for atomicity.
func (e *Engine) cloneFileFromDonor(donor *metadata.File, fileID, virtualPath string, size int64, sha256Full string) error {
	donorChunks, err := e.db.GetChunksForFile(donor.ID)
	if err != nil {
		return fmt.Errorf("getting donor chunks: %w", err)
	}
	if len(donorChunks) == 0 {
		return fmt.Errorf("donor file %s has no chunks", donor.VirtualPath)
	}

	// Pre-fetch all donor chunk locations before starting the transaction.
	// The tx holds the single SQLite connection, so db queries inside it would deadlock.
	donorLocs := make(map[string][]metadata.ChunkLocation, len(donorChunks))
	for _, dc := range donorChunks {
		locs, err := e.db.GetChunkLocations(dc.ID)
		if err != nil {
			return fmt.Errorf("getting donor chunk locations: %w", err)
		}
		donorLocs[dc.ID] = locs
	}

	tx, err := e.db.Conn().Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now().Unix()
	if _, err := tx.Exec(
		`INSERT INTO files (id, virtual_path, size_bytes, created_at, modified_at, sha256_full, upload_state) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		fileID, virtualPath, size, now, now, sha256Full, "complete",
	); err != nil {
		return err
	}

	for _, dc := range donorChunks {
		newChunkID := uuid.New().String()
		if _, err := tx.Exec(
			`INSERT INTO chunks (id, file_id, sequence, size_bytes, sha256, encrypted_size) VALUES (?, ?, ?, ?, ?, ?)`,
			newChunkID, fileID, dc.Sequence, dc.SizeBytes, dc.SHA256, dc.EncryptedSize,
		); err != nil {
			return fmt.Errorf("cloning chunk record: %w", err)
		}
		for _, loc := range donorLocs[dc.ID] {
			if _, err := tx.Exec(
				`INSERT INTO chunk_locations (chunk_id, provider_id, remote_path, upload_confirmed_at) VALUES (?, ?, ?, ?)`,
				newChunkID, loc.ProviderID, loc.RemotePath, loc.UploadConfirmedAt,
			); err != nil {
				return fmt.Errorf("cloning chunk location: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing clone transaction: %w", err)
	}

	slog.Info("file deduped (cloned from existing)", "path", virtualPath, "donor", donor.VirtualPath, "size", size)
	e.dedupHits.Add(1)
	e.filesUploaded.Add(1)
	e.bytesUploaded.Add(size)
	e.scheduleBackup()
	return nil
}

// uploadChunksTracked registers upload progress for fileID, then delegates to
// uploadChunks with a callback that increments the chunk counter.
// Acquires the file-level gate so only one file uploads at a time.
func (e *Engine) uploadChunksTracked(r io.ReadSeeker, fileID, virtualPath string, fileSize int64) ([]chunkMeta, error) {
	// Serialize file-level uploads: wait for the previous file to finish.
	e.fileGate <- struct{}{}
	defer func() { <-e.fileGate }()

	// Signal that an upload is active so BackupDB defers to avoid
	// competing for the same provider API quota.
	e.uploading.Add(1)
	defer e.uploading.Add(-1)

	chunkSize := e.chunkSize(fileSize)
	estimated := int(fileSize/int64(chunkSize)) + 1

	e.uploadsMu.Lock()
	e.uploads[fileID] = &uploadProgress{
		VirtualPath: virtualPath,
		TotalChunks: estimated,
		SizeBytes:   fileSize,
		StartedAt:   time.Now(),
	}
	e.uploadsMu.Unlock()

	callback := func() {
		e.uploadsMu.Lock()
		if p, ok := e.uploads[fileID]; ok {
			p.ChunksUploaded++
			if p.ChunksUploaded > p.TotalChunks {
				p.TotalChunks = p.ChunksUploaded
			}
		}
		e.uploadsMu.Unlock()
	}

	metas, err := e.uploadChunks(r, fileID, fileSize, callback)
	if err != nil {
		return nil, err
	}

	// Correct total once we know the actual chunk count.
	e.uploadsMu.Lock()
	if p, ok := e.uploads[fileID]; ok {
		p.TotalChunks = len(metas)
	}
	e.uploadsMu.Unlock()

	return metas, nil
}

// UploadProgress returns a snapshot of all currently in-flight async uploads.
func (e *Engine) UploadProgress() []UploadProgressInfo {
	e.uploadsMu.RLock()
	defer e.uploadsMu.RUnlock()

	out := make([]UploadProgressInfo, 0, len(e.uploads))
	for _, p := range e.uploads {
		out = append(out, UploadProgressInfo{
			VirtualPath:    p.VirtualPath,
			TotalChunks:    p.TotalChunks,
			ChunksUploaded: p.ChunksUploaded,
			SizeBytes:      p.SizeBytes,
			StartedAt:      p.StartedAt,
			Failed:         p.Failed,
		})
	}
	return out
}

// ResumeUploads re-queues any uploads that were interrupted by a prior daemon
// restart. It reads pending file records from the DB, checks that the tmp file
// still exists on disk, and hands each one back to WriteFileAsync.
func (e *Engine) ResumeUploads() {
	pending, err := e.db.GetPendingUploads()
	if err != nil {
		slog.Error("failed to query pending uploads", "error", err)
		return
	}
	for _, f := range pending {
		if f.TmpPath == nil {
			slog.Warn("pending file has no tmp_path, removing", "path", f.VirtualPath)
			e.db.DeleteFile(f.ID) //nolint:errcheck
			continue
		}
		tmpPath := *f.TmpPath
		if _, err := os.Stat(tmpPath); err != nil {
			slog.Warn("tmp file missing for pending upload, removing record",
				"path", f.VirtualPath, "tmpPath", tmpPath)
			e.db.DeleteFile(f.ID) //nolint:errcheck
			continue
		}
		tmpFile, err := os.Open(tmpPath)
		if err != nil {
			slog.Error("cannot open tmp file for pending upload",
				"path", f.VirtualPath, "tmpPath", tmpPath, "error", err)
			continue
		}
		slog.Info("resuming interrupted upload", "path", f.VirtualPath, "size", f.SizeBytes)
		// WriteFileAsync takes ownership of tmpFile and tmpPath.
		if err := e.WriteFileAsync(f.VirtualPath, tmpFile, tmpPath, f.SizeBytes); err != nil {
			slog.Error("failed to resume upload", "path", f.VirtualPath, "error", err)
			tmpFile.Close()
		}
	}
}

// ReadFile reads a file from the virtual filesystem, downloading and decrypting chunks.
// Returns an error if the file is still uploading (upload_state='pending').
// For large files, prefer ReadFileToTempFile to avoid holding the entire file in memory.
func (e *Engine) ReadFile(virtualPath string) ([]byte, error) {
	tmp, err := e.ReadFileToTempFile(virtualPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()
	return io.ReadAll(tmp)
}

// ReadFileToTempFile downloads a file to a temporary file, returning the open handle.
// The caller must close the file and remove it when done:
//
//	defer func() { f.Close(); os.Remove(f.Name()) }()
//
// Each chunk is downloaded, decrypted, verified, and written to disk sequentially
// so peak memory stays bounded to one chunk (~32–128 MB) regardless of file size.
func (e *Engine) ReadFileToTempFile(virtualPath string) (*os.File, error) {
	file, err := e.db.GetCompleteFileByPath(virtualPath)
	if err != nil {
		return nil, fmt.Errorf("looking up file: %w", err)
	}
	if file == nil {
		if any, _ := e.db.GetFileByPath(virtualPath); any != nil {
			return nil, fmt.Errorf("file upload in progress: %s", virtualPath)
		}
		return nil, fmt.Errorf("file not found: %s", virtualPath)
	}

	chunks, err := e.db.GetChunksForFile(file.ID)
	if err != nil {
		return nil, fmt.Errorf("getting chunks: %w", err)
	}

	// Validate chunk sequences are contiguous (0, 1, 2, ..., n-1).
	for i, c := range chunks {
		if c.Sequence != i {
			return nil, fmt.Errorf("chunk sequence gap at index %d: expected seq %d, got %d for %s",
				i, i, c.Sequence, virtualPath)
		}
	}

	tmp, err := os.CreateTemp("", "pdrive-read-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	abandon := func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}

	fullHasher := sha256.New()

	for _, chunk := range chunks {
		locs, err := e.db.GetChunkLocations(chunk.ID)
		if err != nil {
			abandon()
			return nil, fmt.Errorf("getting chunk locations: %w", err)
		}
		if len(locs) == 0 {
			abandon()
			return nil, fmt.Errorf("no locations for chunk %s", chunk.ID)
		}

		loc := locs[0]
		provider, err := e.db.GetProvider(loc.ProviderID)
		if err != nil || provider == nil {
			abandon()
			return nil, fmt.Errorf("getting provider for chunk %s: %w", chunk.ID, err)
		}

		rc, err := e.rc.GetFile(provider.RcloneRemote, loc.RemotePath)
		if err != nil {
			abandon()
			return nil, fmt.Errorf("downloading chunk %d from %s: %w", chunk.Sequence, provider.DisplayName, err)
		}
		encrypted, readErr := io.ReadAll(rc)
		rc.Close()
		if readErr != nil {
			abandon()
			return nil, fmt.Errorf("reading chunk %d: %w", chunk.Sequence, readErr)
		}

		decrypted, err := chunker.Decrypt(e.encKey, encrypted)
		if err != nil {
			abandon()
			return nil, fmt.Errorf("decrypting chunk %d: %w", chunk.Sequence, err)
		}

		chunkHash := sha256.Sum256(decrypted)
		if hex.EncodeToString(chunkHash[:]) != chunk.SHA256 {
			abandon()
			return nil, fmt.Errorf("chunk %d hash mismatch for %s", chunk.Sequence, virtualPath)
		}

		fullHasher.Write(decrypted)
		if _, err := tmp.Write(decrypted); err != nil {
			abandon()
			return nil, fmt.Errorf("writing chunk %d to temp: %w", chunk.Sequence, err)
		}
	}

	if hex.EncodeToString(fullHasher.Sum(nil)) != file.SHA256Full {
		abandon()
		return nil, fmt.Errorf("file hash mismatch for %s", virtualPath)
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		abandon()
		return nil, err
	}

	slog.Info("file read", "path", virtualPath, "size", file.SizeBytes)
	e.filesDownloaded.Add(1)
	e.bytesDownloaded.Add(file.SizeBytes)
	return tmp, nil
}

// DeleteFile removes a file, its chunks from the cloud, and all metadata.
// Cloud chunk cleanup happens in the background so the caller returns quickly.
// Idempotent: returns nil if the file doesn't exist.
func (e *Engine) DeleteFile(virtualPath string) error {
	file, err := e.db.GetFileByPath(virtualPath)
	if err != nil {
		return fmt.Errorf("looking up file: %w", err)
	}
	if file == nil {
		return nil // idempotent
	}

	// Collect chunk locations BEFORE deleting the DB record.
	locs, _ := e.db.GetChunkLocationsForFile(file.ID)

	// Delete DB record immediately (CASCADE removes chunks + locations).
	if err := e.db.DeleteFile(file.ID); err != nil {
		return fmt.Errorf("deleting file metadata: %w", err)
	}

	// Clean up cloud chunks in the background.
	if len(locs) > 0 {
		go e.deleteCloudChunks(locs)
	}

	slog.Info("file deleted", "path", virtualPath)
	e.filesDeleted.Add(1)
	// Immediate backup — deletions are irreversible and must be synced ASAP.
	go func() {
		if err := e.BackupDB(); err != nil {
			slog.Warn("post-delete backup failed", "error", err)
		}
	}()
	return nil
}

// MkDir creates an explicit directory record.
func (e *Engine) MkDir(dirPath string) error {
	return e.db.CreateDirectory(dirPath)
}

// DeleteDir recursively deletes a directory: all files, cloud chunks, and directory records.
// DB records are deleted immediately; cloud chunk cleanup runs in the background.
func (e *Engine) DeleteDir(dirPath string) error {
	files, err := e.db.GetFilesUnderDir(dirPath)
	if err != nil {
		return fmt.Errorf("listing files under %s: %w", dirPath, err)
	}

	// Collect all cloud chunk locations before deleting DB records.
	var allLocs []metadata.ChunkLocation
	for _, f := range files {
		locs, _ := e.db.GetChunkLocationsForFile(f.ID)
		allLocs = append(allLocs, locs...)
		if err := e.db.DeleteFile(f.ID); err != nil {
			return fmt.Errorf("deleting file record %s: %w", f.VirtualPath, err)
		}
	}
	if err := e.db.DeleteDirectoriesUnder(dirPath); err != nil {
		return fmt.Errorf("deleting directory records: %w", err)
	}

	// Clean up cloud chunks in the background.
	if len(allLocs) > 0 {
		go e.deleteCloudChunks(allLocs)
	}

	slog.Info("directory deleted", "path", dirPath)
	// Immediate backup — deletions are irreversible and must be synced ASAP.
	go func() {
		if err := e.BackupDB(); err != nil {
			slog.Warn("post-delete backup failed", "error", err)
		}
	}()
	return nil
}

// RenameFile updates a file's virtual path in the metadata DB without touching
// cloud storage. If the destination already exists it is deleted first (its
// chunks are removed from the cloud in the background).
func (e *Engine) RenameFile(oldPath, newPath string) error {
	existing, err := e.db.GetFileByPath(newPath)
	if err != nil {
		return fmt.Errorf("checking rename destination: %w", err)
	}
	if existing != nil {
		locs, _ := e.db.GetChunkLocationsForFile(existing.ID)
		if err := e.db.DeleteFile(existing.ID); err != nil {
			return fmt.Errorf("deleting existing destination file: %w", err)
		}
		if len(locs) > 0 {
			go e.deleteCloudChunks(locs)
		}
	}
	if err := e.db.RenameFileByPath(oldPath, newPath); err != nil {
		return fmt.Errorf("renaming file: %w", err)
	}
	slog.Info("file renamed", "old", oldPath, "new", newPath)
	e.scheduleBackup()
	return nil
}

// RenameDir renames a directory and all its contents in the metadata DB.
func (e *Engine) RenameDir(oldPath, newPath string) error {
	if err := e.db.RenameFilesUnderDir(oldPath, newPath); err != nil {
		return fmt.Errorf("renaming files: %w", err)
	}
	if err := e.db.RenameDirectoriesUnder(oldPath, newPath); err != nil {
		return fmt.Errorf("renaming directories: %w", err)
	}
	slog.Info("directory renamed", "old", oldPath, "new", newPath)
	e.scheduleBackup()
	return nil
}

// deleteCloudChunks removes chunks from cloud providers in the background.
// Skips cloud objects that are still referenced by other files (dedup clones).
// Failed deletions are persisted to DB for later retry.
func (e *Engine) deleteCloudChunks(locs []metadata.ChunkLocation) {
	if e.rc == nil {
		return
	}
	for _, loc := range locs {
		// Check if another chunk_location still references this cloud object
		// (happens when content-hash dedup cloned the chunks).
		if refCount, err := e.db.RemotePathRefCount(loc.RemotePath); err == nil && refCount > 0 {
			slog.Debug("skipping shared cloud chunk", "remotePath", loc.RemotePath, "refs", refCount)
			continue
		}

		// Rate-limit delete calls through the same token bucket as uploads
		// to avoid monopolizing the provider's API quota during GC.
		<-e.uploadTokens

		provider, err := e.db.GetProvider(loc.ProviderID)
		if err != nil || provider == nil {
			slog.Warn("could not get provider for chunk cleanup", "providerID", loc.ProviderID)
			continue
		}
		if err := e.rc.DeleteFile(provider.RcloneRemote, loc.RemotePath); err != nil {
			slog.Warn("failed to delete chunk from provider, queuing for retry",
				"chunk", loc.ChunkID, "provider", provider.DisplayName, "error", err)
			e.db.InsertFailedDeletion(loc.ProviderID, loc.RemotePath, err.Error()) //nolint:errcheck
		}
	}
	slog.Debug("cloud chunk cleanup done", "count", len(locs))
}

// Stat returns file metadata or nil if the file doesn't exist.
func (e *Engine) Stat(virtualPath string) (*metadata.File, error) {
	return e.db.GetFileByPath(virtualPath)
}

// ListDir returns files and subdirectory names directly under dirPath.
func (e *Engine) ListDir(dirPath string) ([]metadata.File, []string, error) {
	files, err := e.db.ListFiles(dirPath)
	if err != nil {
		return nil, nil, err
	}
	dirs, err := e.db.ListSubdirectories(dirPath)
	if err != nil {
		return nil, nil, err
	}
	return files, dirs, nil
}

// FileExists checks if a file exists at the given virtual path.
func (e *Engine) FileExists(virtualPath string) (bool, error) {
	return e.db.FileExists(virtualPath)
}

// IsDir checks if a path is a directory (has files underneath it).
func (e *Engine) IsDir(path string) (bool, error) {
	return e.db.PathIsDir(path)
}

// StorageStatus holds aggregate storage statistics.
type StorageStatus struct {
	TotalFiles    int64
	TotalBytes    int64
	Providers     []metadata.Provider
	ProviderBytes map[string]int64 // encrypted bytes stored per provider ID
}

// StorageStatus returns total file count, total bytes stored, and per-provider quota info.
func (e *Engine) StorageStatus() (StorageStatus, error) {
	var totalFiles, totalBytes int64
	if err := e.db.Conn().QueryRow(`SELECT COUNT(*), COALESCE(SUM(size_bytes),0) FROM files WHERE upload_state = 'complete'`).Scan(&totalFiles, &totalBytes); err != nil {
		return StorageStatus{}, fmt.Errorf("querying storage stats: %w", err)
	}
	providers, err := e.db.GetAllProviders()
	if err != nil {
		return StorageStatus{}, fmt.Errorf("getting providers: %w", err)
	}
	providerBytes, err := e.db.GetProviderChunkBytes()
	if err != nil {
		providerBytes = map[string]int64{}
	}
	return StorageStatus{
		TotalFiles:    totalFiles,
		TotalBytes:    totalBytes,
		Providers:     providers,
		ProviderBytes: providerBytes,
	}, nil
}

// RetryFailedDeletions retries cloud chunk deletions that previously failed.
// Called periodically by the daemon. Deletions that exceed maxRetries are abandoned.
func (e *Engine) RetryFailedDeletions() {
	const batchSize = 50
	const maxRetries = 10

	items, err := e.db.GetFailedDeletions(batchSize)
	if err != nil || len(items) == 0 {
		return
	}

	var retried, succeeded, abandoned int
	for _, item := range items {
		if item.RetryCount >= maxRetries {
			slog.Warn("abandoning chunk deletion after max retries",
				"remotePath", item.RemotePath, "retries", item.RetryCount)
			e.db.DeleteFailedDeletion(item.ID) //nolint:errcheck
			abandoned++
			continue
		}

		provider, err := e.db.GetProvider(item.ProviderID)
		if err != nil || provider == nil {
			e.db.DeleteFailedDeletion(item.ID) //nolint:errcheck
			abandoned++
			continue
		}

		<-e.uploadTokens
		if err := e.rc.DeleteFile(provider.RcloneRemote, item.RemotePath); err != nil {
			e.db.IncrementFailedDeletionRetry(item.ID, err.Error()) //nolint:errcheck
			retried++
		} else {
			e.db.DeleteFailedDeletion(item.ID) //nolint:errcheck
			succeeded++
		}
	}

	if retried+succeeded+abandoned > 0 {
		slog.Info("failed deletion retry complete",
			"succeeded", succeeded, "retried", retried, "abandoned", abandoned)
	}
}

// SearchFiles returns all completed files matching a pattern under the given root.
func (e *Engine) SearchFiles(root, pattern string) ([]metadata.File, error) {
	return e.db.SearchFiles(root, pattern)
}

// ListAllFiles returns all completed files under root, recursively.
func (e *Engine) ListAllFiles(root string) ([]metadata.File, error) {
	return e.db.ListAllFiles(root)
}

// DiskUsage returns (file_count, total_bytes) for all completed files under root.
func (e *Engine) DiskUsage(root string) (int64, int64, error) {
	return e.db.DiskUsage(root)
}

// FileInfo holds detailed information about a single file, including chunk distribution.
type FileInfo struct {
	File   metadata.File
	Chunks []ChunkInfo
}

// ChunkInfo describes one chunk and where it's stored.
type ChunkInfo struct {
	Sequence      int
	SizeBytes     int
	EncryptedSize int
	Providers     []string // provider display names
}

// GetFileInfo returns detailed metadata for a file, including chunk distribution.
func (e *Engine) GetFileInfo(virtualPath string) (*FileInfo, error) {
	f, err := e.db.GetFileByPath(virtualPath)
	if err != nil || f == nil {
		return nil, err
	}
	chunks, err := e.db.GetChunksForFile(f.ID)
	if err != nil {
		return nil, err
	}
	// Build provider name lookup.
	providers, _ := e.db.GetAllProviders()
	provNames := make(map[string]string)
	for _, p := range providers {
		provNames[p.ID] = p.DisplayName
	}
	var infos []ChunkInfo
	for _, c := range chunks {
		locs, _ := e.db.GetChunkLocations(c.ID)
		var names []string
		for _, loc := range locs {
			if n, ok := provNames[loc.ProviderID]; ok {
				names = append(names, n)
			} else {
				names = append(names, loc.ProviderID)
			}
		}
		infos = append(infos, ChunkInfo{
			Sequence:      c.Sequence,
			SizeBytes:     c.SizeBytes,
			EncryptedSize: c.EncryptedSize,
			Providers:     names,
		})
	}
	return &FileInfo{File: *f, Chunks: infos}, nil
}
