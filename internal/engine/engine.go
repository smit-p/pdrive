package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/chunker"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/rclonerc"
)

const chunkRemoteDir = "pdrive-chunks"

// Engine orchestrates file write and read operations.
type Engine struct {
	db           *metadata.DB
	dbPath       string
	rc           *rclonerc.Client
	broker       *broker.Broker
	encKey       []byte        // AES-256 key (32 bytes)
	uploadTokens chan struct{} // token bucket: limits upload API calls per second
}

const (
	// uploadRatePerSec is the maximum number of chunk-upload API calls per second
	// across all providers. Google Drive's per-user quota is ~10 req/100s; 8/s
	// gives comfortable headroom without stalling uploads.
	uploadRatePerSec = 8
	uploadRateBurst  = 4 // initial burst before the ticker kicks in
)

// NewEngine creates a new engine.
func NewEngine(db *metadata.DB, dbPath string, rc *rclonerc.Client, b *broker.Broker, encKey []byte) *Engine {
	e := &Engine{
		db:           db,
		dbPath:       dbPath,
		rc:           rc,
		broker:       b,
		encKey:       encKey,
		uploadTokens: make(chan struct{}, uploadRateBurst),
	}
	// Pre-fill the burst quota.
	for i := 0; i < uploadRateBurst; i++ {
		e.uploadTokens <- struct{}{}
	}
	// Refill one token every 1/uploadRatePerSec seconds.
	go func() {
		ticker := time.NewTicker(time.Second / uploadRatePerSec)
		for range ticker.C {
			select {
			case e.uploadTokens <- struct{}{}:
			default: // bucket full, discard
			}
		}
	}()
	return e
}

// workersForChunkSize returns an appropriate concurrency level for the given
// chunk size so that peak in-flight memory is bounded to roughly 256 MB.
func workersForChunkSize(chunkSize int) int {
	switch {
	case chunkSize >= 32*1024*1024: // ≥ 32 MB → 1 worker (≤64 MB in-flight)
		return 1
	case chunkSize >= 8*1024*1024: // ≥ 8 MB → 2 workers (≤32 MB in-flight)
		return 2
	default: // < 8 MB → 3 workers (≤24 MB in-flight)
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
	maxUploadWorkers = 3
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
	fileID, fullHashStr, err := e.prepareFileWrite(virtualPath, r, size)
	if err != nil {
		return err
	}
	_ = fullHashStr

	metas, err := e.uploadChunks(r, fileID, size)
	if err != nil {
		return err
	}

	if err := e.insertChunkMetadata(fileID, metas); err != nil {
		return err
	}

	slog.Info("file written", "path", virtualPath, "size", size, "chunks", len(metas))
	e.scheduleBackup()
	return nil
}

// WriteFileAsync hashes and creates the file record synchronously, then uploads
// chunks in a background goroutine. The caller must NOT close or remove tmpFile;
// the engine takes ownership and cleans up when done.
func (e *Engine) WriteFileAsync(virtualPath string, tmpFile *os.File, tmpPath string, size int64) error {
	fileID, _, err := e.prepareFileWrite(virtualPath, tmpFile, size)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return err
	}

	go func() {
		defer tmpFile.Close()
		defer os.Remove(tmpPath)

		metas, err := e.uploadChunks(tmpFile, fileID, size)
		if err != nil {
			// Keep the file record so the user can still see and delete the file.
			slog.Error("background upload failed",
				"path", virtualPath, "error", err)
			return
		}

		if err := e.insertChunkMetadata(fileID, metas); err != nil {
			slog.Error("failed to insert chunk metadata", "path", virtualPath, "error", err)
			return
		}

		slog.Info("file written", "path", virtualPath, "size", size, "chunks", len(metas))
		e.scheduleBackup()
	}()

	return nil
}

// prepareFileWrite hashes the data, deletes any existing file, and inserts the
// new file record. It returns the fileID and hash. The reader is rewound to the
// start, ready for chunk reading.
func (e *Engine) prepareFileWrite(virtualPath string, r io.ReadSeeker, size int64) (string, string, error) {
	hasher := sha256.New()
	if _, err := io.Copy(hasher, r); err != nil {
		return "", "", fmt.Errorf("hashing file: %w", err)
	}
	fullHashStr := hex.EncodeToString(hasher.Sum(nil))

	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return "", "", fmt.Errorf("rewinding reader: %w", err)
	}

	existing, err := e.db.GetFileByPath(virtualPath)
	if err != nil {
		return "", "", fmt.Errorf("checking existing file: %w", err)
	}
	if existing != nil {
		// Collect cloud locations before deleting DB record, clean up in background.
		locs, _ := e.db.GetChunkLocationsForFile(existing.ID)
		if err := e.db.DeleteFile(existing.ID); err != nil {
			return "", "", fmt.Errorf("deleting old file: %w", err)
		}
		if len(locs) > 0 {
			go e.deleteCloudChunks(locs)
		}
	}

	fileID := uuid.New().String()
	now := time.Now().Unix()
	if err := e.db.InsertFile(&metadata.File{
		ID:          fileID,
		VirtualPath: virtualPath,
		SizeBytes:   size,
		CreatedAt:   now,
		ModifiedAt:  now,
		SHA256Full:  fullHashStr,
	}); err != nil {
		return "", "", fmt.Errorf("inserting file record: %w", err)
	}

	return fileID, fullHashStr, nil
}

// uploadChunks reads, encrypts, and uploads chunks concurrently with retry.
// Chunk size is chosen dynamically based on fileSize to keep the total chunk
// count near ~100, reducing cloud API calls for large files.
// Returns the ordered slice of chunk metadata on success.
func (e *Engine) uploadChunks(r io.ReadSeeker, fileID string, fileSize int64) ([]chunkMeta, error) {
	chunkSize := chunker.ChunkSizeForFile(fileSize)
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
			var lastErr error
			for attempt := 0; attempt < maxUploadRetries; attempt++ {
				if attempt > 0 {
					backoff := time.Duration(1<<uint(attempt)) * time.Second
					if backoff > 30*time.Second {
						backoff = 30 * time.Second
					}
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
				return // success
			}
			mu.Lock()
			if firstErr == nil {
				firstErr = fmt.Errorf("uploading chunk %d to %s after %d retries: %w",
					seq, prov.DisplayName, maxUploadRetries, lastErr)
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

// insertChunkMetadata writes chunk and chunk_location records to the DB.
func (e *Engine) insertChunkMetadata(fileID string, metas []chunkMeta) error {
	for _, m := range metas {
		confirmTime := time.Now().Unix()
		if err := e.db.InsertChunk(&metadata.ChunkRecord{
			ID:            m.chunkID,
			FileID:        fileID,
			Sequence:      m.sequence,
			SizeBytes:     m.size,
			SHA256:        m.sha256,
			EncryptedSize: m.encryptedSize,
		}); err != nil {
			return fmt.Errorf("inserting chunk record: %w", err)
		}

		if err := e.db.InsertChunkLocation(&metadata.ChunkLocation{
			ChunkID:           m.chunkID,
			ProviderID:        m.providerID,
			RemotePath:        m.remotePath,
			UploadConfirmedAt: &confirmTime,
		}); err != nil {
			return fmt.Errorf("inserting chunk location: %w", err)
		}
	}
	return nil
}

// ReadFile reads a file from the virtual filesystem, downloading and decrypting chunks.
func (e *Engine) ReadFile(virtualPath string) ([]byte, error) {
	file, err := e.db.GetFileByPath(virtualPath)
	if err != nil {
		return nil, fmt.Errorf("looking up file: %w", err)
	}
	if file == nil {
		return nil, fmt.Errorf("file not found: %s", virtualPath)
	}

	chunks, err := e.db.GetChunksForFile(file.ID)
	if err != nil {
		return nil, fmt.Errorf("getting chunks: %w", err)
	}

	var decryptedChunks []chunker.DecryptedChunk

	for _, chunk := range chunks {
		locs, err := e.db.GetChunkLocations(chunk.ID)
		if err != nil {
			return nil, fmt.Errorf("getting chunk locations: %w", err)
		}
		if len(locs) == 0 {
			return nil, fmt.Errorf("no locations for chunk %s", chunk.ID)
		}

		loc := locs[0] // use first available location
		provider, err := e.db.GetProvider(loc.ProviderID)
		if err != nil || provider == nil {
			return nil, fmt.Errorf("getting provider for chunk %s: %w", chunk.ID, err)
		}

		encrypted, err := e.rc.GetFile(provider.RcloneRemote, loc.RemotePath)
		if err != nil {
			return nil, fmt.Errorf("downloading chunk %d from %s: %w", chunk.Sequence, provider.DisplayName, err)
		}

		decrypted, err := chunker.Decrypt(e.encKey, encrypted)
		if err != nil {
			return nil, fmt.Errorf("decrypting chunk %d: %w", chunk.Sequence, err)
		}

		decryptedChunks = append(decryptedChunks, chunker.DecryptedChunk{
			Sequence: chunk.Sequence,
			Data:     decrypted,
			SHA256:   chunk.SHA256,
		})
	}

	reader, err := chunker.Assemble(decryptedChunks)
	if err != nil {
		return nil, fmt.Errorf("assembling file: %w", err)
	}

	result, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading assembled file: %w", err)
	}

	// Verify full file hash.
	fullHash := sha256.Sum256(result)
	if hex.EncodeToString(fullHash[:]) != file.SHA256Full {
		return nil, fmt.Errorf("file hash mismatch for %s", virtualPath)
	}

	slog.Info("file read", "path", virtualPath, "size", len(result))
	return result, nil
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
	e.scheduleBackup()
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
			slog.Warn("failed to delete file record", "file", f.VirtualPath, "error", err)
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
// Best-effort: errors are logged but never propagated.
func (e *Engine) deleteCloudChunks(locs []metadata.ChunkLocation) {
	for _, loc := range locs {
		provider, err := e.db.GetProvider(loc.ProviderID)
		if err != nil || provider == nil {
			slog.Warn("could not get provider for chunk cleanup", "providerID", loc.ProviderID)
			continue
		}
		if err := e.rc.DeleteFile(provider.RcloneRemote, loc.RemotePath); err != nil {
			slog.Warn("failed to delete chunk from provider", "chunk", loc.ChunkID, "provider", provider.DisplayName, "error", err)
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
