// Package engine is the core orchestrator for all pdrive file operations:
// upload, download, delete, rename, deduplication, and metadata management.
//
// Upload pipeline:
//  1. File is hashed (SHA-256) for content-hash deduplication.
//  2. If a matching file already exists, chunk metadata is cloned (zero upload).
//  3. Otherwise the file is split into chunks via [chunker.ChunkReader],
//     then uploaded concurrently with retry and exponential backoff through
//     the [CloudStorage] interface.
//  4. Files larger than [AsyncWriteThreshold] upload in the background so
//     WebDAV PUT returns quickly.
//
// Download pipeline:
//  1. Chunks are downloaded sequentially, SHA-256 verified,
//     and written to a temp file.
//  2. A full-file hash check is performed before returning.
//
// The engine also manages:
//   - Debounced metadata DB backup to all providers
//   - Orphan GC (cloud objects with no DB record, and vice versa)
//   - Failed-deletion retry queue
//   - Telemetry counters (files/chunks/bytes uploaded, downloads, dedup hits)
//
// File layout:
//   - engine.go — types, constructors, lifecycle, config, metrics, queries
//   - upload.go — upload pipeline (write, chunk, dedup, progress)
//   - download.go — download pipeline (read, stream, verify)
//   - delete.go — delete pipeline (file/dir delete, cloud cleanup, retry)
//   - dbsync.go — DB backup, restore, orphan GC
package engine

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/chunker"
	"github.com/smit-p/pdrive/internal/erasure"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/rclonerc"
)

// ErrInsufficientSpace is returned when the total file size exceeds the
// aggregate free space across all providers.
var ErrInsufficientSpace = errors.New("file size exceeds available storage space")

// CloudStorage is the interface the Engine uses to talk to cloud providers.
// *rclonerc.Client satisfies this interface in production; tests inject a fake.
type CloudStorage interface {
	PutFile(remote, remotePath string, data io.Reader) error
	GetFile(remote, remotePath string) (io.ReadCloser, error)
	StreamGetFile(remote, remotePath string) (io.ReadCloser, error)
	DeleteFile(remote, remotePath string) error
	ListDir(remote, remotePath string) ([]rclonerc.ListItem, error)
	Cleanup(remote string) error
	Mkdir(remote, remotePath string) error
	TransferStats() rclonerc.TransferProgress
}

const chunkRemoteDir = "pdrive-chunks"

// Engine orchestrates file write and read operations.
type Engine struct {
	db           *metadata.DB
	dbPath       string
	rc           CloudStorage
	broker       *broker.Broker
	uploadTokens chan struct{} // token bucket: limits upload API calls per second
	fileGate     chan struct{} // serializes file-level uploads (only 1 file at a time)
	// maxChunkRetries overrides maxUploadRetries when > 0 (used by tests to
	// avoid long exponential-backoff delays).
	maxChunkRetries int

	// overrideChunkSize, when > 0, replaces the dynamic chunk-size calculation.
	overrideChunkSize int

	// erasureEnc, when non-nil, enables Reed-Solomon erasure coding.
	// Each chunk is split into data+parity shards spread
	// across distinct providers.
	erasureEnc *erasure.Encoder

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
	// own backoff) run inside rclone.  12/s saturates high-bandwidth connections
	// while still being well within rclone RC limits.
	uploadRatePerSec = 12
	uploadRateBurst  = 20 // initial burst before the ticker kicks in
)

// NewEngine creates a new engine backed by an rclone RC client.
func NewEngine(db *metadata.DB, dbPath string, rc *rclonerc.Client, b *broker.Broker) *Engine {
	const burst = 20
	e := newEngine(db, dbPath, rc, b, burst, uploadRatePerSec)
	return e
}

// NewEngineWithRate creates an Engine with a custom API rate limit (tokens per second).
// A ratePerSec of 0 or less uses the default (6/s).
func NewEngineWithRate(db *metadata.DB, dbPath string, rc *rclonerc.Client, b *broker.Broker, ratePerSec int) *Engine {
	const burst = 20
	if ratePerSec <= 0 {
		ratePerSec = uploadRatePerSec
	}
	return newEngine(db, dbPath, rc, b, burst, ratePerSec)
}

// NewEngineWithCloud creates an Engine with any CloudStorage implementation.
// Intended for testing and tooling that needs an alternative storage backend.
// Uses a larger initial token burst (256) so that test-speed uploads are never
// token-starved.
func NewEngineWithCloud(db *metadata.DB, dbPath string, rc CloudStorage, b *broker.Broker) *Engine {
	const burst = 256
	return newEngine(db, dbPath, rc, b, burst, uploadRatePerSec)
}

func newEngine(db *metadata.DB, dbPath string, rc CloudStorage, b *broker.Broker, burst, ratePerSec int) *Engine {
	e := &Engine{
		db:           db,
		dbPath:       dbPath,
		rc:           rc,
		broker:       b,
		uploadTokens: make(chan struct{}, burst),
		fileGate:     make(chan struct{}, 1),
		uploads:      make(map[string]*uploadProgress),
		closeCh:      make(chan struct{}),
	}
	// Seed in-memory counters from persisted DB values.
	if counters, err := db.LoadCounters(); err == nil {
		e.filesUploaded.Store(counters["files_uploaded"])
		e.filesDownloaded.Store(counters["files_downloaded"])
		e.filesDeleted.Store(counters["files_deleted"])
		e.chunksUploaded.Store(counters["chunks_uploaded"])
		e.bytesUploaded.Store(counters["bytes_uploaded"])
		e.bytesDownloaded.Store(counters["bytes_downloaded"])
		e.dedupHits.Store(counters["dedup_hits"])
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

// WaitUploads blocks until all in-flight async uploads have completed.
// Intended for tests; production code should use Shutdown().
func (e *Engine) WaitUploads() { e.asyncWG.Wait() }

// SetChunkSize overrides the dynamic chunk-size calculation with a fixed value.
// Pass 0 to revert to the default dynamic behaviour.
func (e *Engine) SetChunkSize(bytes int) { e.overrideChunkSize = bytes }

// SetErasure configures Reed-Solomon erasure coding for new uploads.
// dataShards is the number of data shards, parityShards is the number
// of parity shards.  Existing files are unaffected — their shard counts
// are stored per-chunk and used automatically on download.
func (e *Engine) SetErasure(dataShards, parityShards int) error {
	enc, err := erasure.NewEncoder(dataShards, parityShards)
	if err != nil {
		return err
	}
	e.erasureEnc = enc
	return nil
}

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

// CheckSpace returns a non-nil error if fileSize exceeds the aggregate free
// space across all eligible providers.
func (e *Engine) CheckSpace(fileSize int64) error {
	free, err := e.broker.TotalFreeSpace()
	if err != nil {
		return fmt.Errorf("checking free space: %w", err)
	}
	if fileSize > free {
		return fmt.Errorf("%w: need ~%s but only %s available",
			ErrInsufficientSpace, fmtBytes(fileSize), fmtBytes(free))
	}
	return nil
}

// fmtBytes formats a byte count as a human-readable string.
func fmtBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

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

// incCounter increments both the in-memory atomic and the persisted DB counter.
func (e *Engine) incCounter(counter *atomic.Int64, key string, delta int64) {
	counter.Add(delta)
	e.db.IncrementCounter(key, delta) //nolint:errcheck
}

// chunkSchedule returns a variable chunk schedule for the given file size.
// When an override chunk size is set (tests, CLI flag) a single-tier schedule
// is returned so the override is respected.
func (e *Engine) chunkSchedule(fileSize int64) *chunker.ChunkSchedule {
	if e.overrideChunkSize > 0 {
		return &chunker.ChunkSchedule{Tiers: []chunker.ChunkTier{
			{Count: 0, Size: e.overrideChunkSize},
		}}
	}
	freeSpaces, err := e.broker.EligibleFreeSpaces()
	if err != nil || len(freeSpaces) == 0 {
		return chunker.ScheduleForFile(fileSize)
	}
	return chunker.PlanChunks(fileSize, freeSpaces)
}

// uploadWorkersForSchedule returns the number of concurrent upload workers,
// scaled inversely with the maximum chunk size to keep peak memory bounded.
// Peak in-flight memory ≈ workers × maxChunkSize.
func uploadWorkersForSchedule(s *chunker.ChunkSchedule) int {
	const memBudget = 6 << 30 // 6 GiB target for in-flight data
	maxChunk := int64(s.MaxSize())
	if maxChunk <= 0 {
		return maxUploadWorkers
	}
	w := int(memBudget / maxChunk)
	if w < 2 {
		w = 2
	}
	if w > maxUploadWorkers {
		w = maxUploadWorkers
	}
	return w
}
func (e *Engine) MkDir(dirPath string) error {
	return e.db.CreateDirectory(dirPath)
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
	ProviderBytes map[string]int64 // cloud bytes stored per provider ID
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
	// Account for the metadata DB backup that pdrive stores on every provider
	// (pdrive-meta/metadata.db = 16-byte header + raw DB file).
	var metaSize int64
	if e.dbPath != "" {
		if info, err := os.Stat(e.dbPath); err == nil {
			metaSize = info.Size() + 16 // backup header overhead
		}
	}
	if metaSize > 0 {
		for _, p := range providers {
			providerBytes[p.ID] += metaSize
		}
	}
	return StorageStatus{
		TotalFiles:    totalFiles,
		TotalBytes:    totalBytes,
		Providers:     providers,
		ProviderBytes: providerBytes,
	}, nil
}
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
	Sequence  int
	SizeBytes int
	CloudSize int
	Providers []string // provider display names
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
			Sequence:  c.Sequence,
			SizeBytes: c.SizeBytes,
			CloudSize: c.CloudSize,
			Providers: names,
		})
	}
	return &FileInfo{File: *f, Chunks: infos}, nil
}
