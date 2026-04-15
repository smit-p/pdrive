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
	"path"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/smit-p/pdrive/internal/chunker"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/rclonerc"
)

const (
	// maxUploadWorkers is the concurrency limit for chunk upload goroutines.
	// Matches rclone --transfers=12 so every worker can have a transfer in
	// flight simultaneously.
	maxUploadWorkers = 12
	// maxUploadRetries is the number of retry attempts for a failed chunk upload.
	maxUploadRetries = 5
	// AsyncWriteThreshold: files larger than this are uploaded in the background
	// so the WebDAV PUT returns quickly and Finder doesn't time out.
	AsyncWriteThreshold = 4 * 1024 * 1024 // 4 MB
)

// uploadProgress tracks in-flight async upload state.
type uploadProgress struct {
	VirtualPath    string
	TotalChunks    int
	ChunksUploaded int
	SizeBytes      int64
	BytesDone      int64 // encrypted bytes of completed chunks
	BytesTotal     int64 // total encrypted size (all chunks)
	StartedAt      time.Time
	Failed         bool
	Preparing      bool // true while hashing / spooling, before chunks start
	// inFlightChunks tracks chunk remote paths currently being uploaded
	// so we can match them against rclone transfer stats.
	inFlightChunks map[string]struct{}
	// cancelCh is closed to signal cancellation of this upload.
	cancelCh chan struct{}
}

// UploadProgressInfo is the exported snapshot of an in-flight upload.
type UploadProgressInfo struct {
	VirtualPath    string    `json:"VirtualPath"`
	TotalChunks    int       `json:"TotalChunks"`
	ChunksUploaded int       `json:"ChunksUploaded"`
	SizeBytes      int64     `json:"SizeBytes"`
	BytesDone      int64     `json:"BytesDone"`
	BytesTotal     int64     `json:"BytesTotal"`
	SpeedBPS       float64   `json:"SpeedBPS"`
	StartedAt      time.Time `json:"StartedAt"`
	Failed         bool      `json:"Failed"`
	Preparing      bool      `json:"Preparing"`
}

// chunkMeta holds metadata for a single uploaded chunk.
type chunkMeta struct {
	chunkID       string
	sequence      int
	size          int
	sha256        string
	encryptedSize int
	dataShards    int
	parityShards  int
	shards        []shardMeta
}

// shardMeta holds metadata for a single erasure shard within a chunk.
type shardMeta struct {
	shardIndex int
	providerID string
	remotePath string
	size       int
}

// WriteFile writes a file to the virtual filesystem, chunking and encrypting it.
// For small files or when data is already in memory.
func (e *Engine) WriteFile(virtualPath string, data []byte) error {
	return e.WriteFileStream(virtualPath, bytes.NewReader(data), int64(len(data)))
}

// WriteFileStream writes a file from a stream synchronously (hash + upload + metadata).
func (e *Engine) WriteFileStream(virtualPath string, r io.ReadSeeker, size int64) error {
	// Pre-upload space check: reject early if the file won't fit.
	if err := e.CheckSpace(size); err != nil {
		return err
	}

	// Capture existing file info BEFORE the upload, but do NOT delete yet.
	// Deleting first would cause data loss if the upload fails.
	existing, _ := e.db.GetFileByPath(virtualPath)
	var oldID string
	var oldLocs []metadata.ChunkLocation
	if existing != nil {
		oldID = existing.ID
		oldLocs, _ = e.db.GetChunkLocationsForFile(existing.ID)
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
		err := e.cloneFileFromDonor(donor, fileID, virtualPath, size, fullHashStr, oldID)
		if err == nil && len(oldLocs) > 0 {
			go e.deleteCloudChunks(oldLocs)
		}
		return err
	}

	// Upload all chunks FIRST — if this fails the old file is preserved.
	metas, err := e.uploadChunks(r, fileID, size, nil, nil, nil)
	if err != nil {
		return err
	}

	// Upload succeeded — now atomically swap: delete old record, insert new.
	if oldID != "" {
		if err := e.db.DeleteFile(oldID); err != nil {
			slog.Warn("failed to delete old file record during overwrite", "oldID", oldID, "error", err)
		}
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
		if delErr := e.db.DeleteFile(fileID); delErr != nil {
			slog.Warn("failed to clean up file record after chunk metadata failure", "error", delErr)
		}
		return err
	}

	// Clean up old cloud chunks AFTER the new file is safely recorded.
	if len(oldLocs) > 0 {
		go e.deleteCloudChunks(oldLocs)
	}

	slog.Info("file written", "path", virtualPath, "size", size, "chunks", len(metas))
	e.incCounter(&e.filesUploaded, "files_uploaded", 1)
	e.incCounter(&e.bytesUploaded, "bytes_uploaded", size)
	e.scheduleBackup()
	return nil
}

// WriteFileAsync hashes the file synchronously, writes a pending DB record
// (so uploads survive a daemon restart via ResumeUploads), then uploads chunks
// in a background goroutine. The file stays invisible in the WebDAV listing
// until the upload completes (ListFiles/GetFileByPath filter pending records).
// The caller must NOT close or remove tmpFile; the engine takes ownership.
func (e *Engine) WriteFileAsync(virtualPath string, tmpFile *os.File, tmpPath string, size int64) error {
	// Generate the file ID early so we can register progress immediately.
	fileID := uuid.New().String()

	// Adopt the queued placeholder (set by SyncDir) or create a fresh entry.
	e.adoptQueuedUpload(virtualPath, fileID, size)

	// Pre-upload space check: reject early if the file won't fit.
	if err := e.CheckSpace(size); err != nil {
		e.removeUploadProgress(fileID)
		tmpFile.Close()
		os.Remove(tmpPath)
		return err
	}

	// Hash synchronously so we can write the pending DB record now.
	hasher := sha256.New()
	if _, err := io.Copy(hasher, tmpFile); err != nil {
		e.removeUploadProgress(fileID)
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("hashing file: %w", err)
	}
	fullHashStr := hex.EncodeToString(hasher.Sum(nil))
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		e.removeUploadProgress(fileID)
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("rewinding after hash: %w", err)
	}

	// Capture existing file info BEFORE the upload, but do NOT delete yet.
	// Deleting first would cause data loss if the async upload fails later.
	existing, _ := e.db.GetFileByPath(virtualPath)
	var oldID string
	var oldLocs []metadata.ChunkLocation
	if existing != nil {
		oldID = existing.ID
		oldLocs, _ = e.db.GetChunkLocationsForFile(existing.ID)
	}

	// Content-hash dedup: if a completed file with the same SHA256 already
	// exists, clone its chunk metadata instead of re-uploading.
	if donor, _ := e.db.GetCompleteFileByHash(fullHashStr); donor != nil {
		e.removeUploadProgress(fileID)
		tmpFile.Close()
		os.Remove(tmpPath)
		err := e.cloneFileFromDonor(donor, fileID, virtualPath, size, fullHashStr, oldID)
		if err == nil {
			if len(oldLocs) > 0 {
				go e.deleteCloudChunks(oldLocs)
			}
			if err := e.db.InsertActivity("upload", virtualPath, fmt.Sprintf("%d bytes", size)); err != nil {
				slog.Debug("activity log insert failed", "error", err)
			}
		}
		return err
	}

	// Delete existing file record now so the pending INSERT doesn't conflict.
	// The old cloud chunks are NOT cleaned up yet — that happens after the
	// background upload succeeds, preserving the old data if this upload fails.
	if oldID != "" {
		if err := e.db.DeleteFile(oldID); err != nil {
			slog.Warn("failed to delete old file record before async upload", "oldID", oldID, "error", err)
		}
	}

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
		e.removeUploadProgress(fileID)
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
		e.incCounter(&e.filesUploaded, "files_uploaded", 1)
		e.incCounter(&e.bytesUploaded, "bytes_uploaded", size)

		// Clean up old cloud chunks AFTER the new file is safely recorded.
		if len(oldLocs) > 0 {
			e.deleteCloudChunks(oldLocs)
		}

		if err := e.db.InsertActivity("upload", virtualPath, fmt.Sprintf("%d bytes", size)); err != nil {
			slog.Debug("activity log insert failed", "error", err)
		}
		e.scheduleBackup()
	}()
	return nil
}

// uploadChunks splits the file into chunks, writes each to a temp file,
// and uploads them concurrently.
// onChunkUploaded, if non-nil, is called with (remotePath, chunkSize, done):
//   - done=false at beginning of upload attempt (track as in-flight)
//   - done=true after successful upload (track bytes completed)
//
// cancelCh, if non-nil, is checked between chunks to allow cancellation.
// Returns the ordered slice of chunk metadata on success.
func (e *Engine) uploadChunks(r io.ReadSeeker, fileID string, fileSize int64, schedule *chunker.ChunkSchedule, onChunkUploaded func(remotePath string, encSize int64, done bool), cancelCh <-chan struct{}) ([]chunkMeta, error) {
	if schedule == nil {
		schedule = e.chunkSchedule(fileSize)
	}
	workers := uploadWorkersForSchedule(schedule)
	slog.Debug("upload plan", "fileSize", fileSize, "workers", workers,
		"tiers", len(schedule.Tiers), "maxChunk", schedule.MaxSize())

	var (
		metas    []chunkMeta
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
		sem      = make(chan struct{}, workers)
		// uploadedLocs tracks shard locations whose cloud upload succeeded,
		// so we can clean them up if a later chunk/shard fails.
		uploadedLocs []metadata.ChunkLocation
	)

	remaining := fileSize
	for seq := 0; remaining > 0; seq++ {
		// Check for cancellation.
		if cancelCh != nil {
			select {
			case <-cancelCh:
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("upload cancelled")
				}
				mu.Unlock()
			default:
			}
		}

		mu.Lock()
		uploadErr := firstErr
		mu.Unlock()
		if uploadErr != nil {
			break
		}

		chunkPlain := int64(schedule.SizeForSeq(seq))
		if chunkPlain > remaining {
			chunkPlain = remaining
		}

		// Read chunk data through a hasher into a temp file.
		chunkID := uuid.New().String()
		hasher := sha256.New()
		tee := io.TeeReader(io.LimitReader(r, chunkPlain), hasher)

		chunkTmp, err := os.CreateTemp("", "pdrive-chunk-*")
		if err != nil {
			wg.Wait()
			return nil, fmt.Errorf("creating temp file for chunk %d: %w", seq, err)
		}

		if _, err := io.Copy(chunkTmp, tee); err != nil {
			chunkTmp.Close()
			os.Remove(chunkTmp.Name())
			wg.Wait()
			return nil, fmt.Errorf("writing chunk %d to temp: %w", seq, err)
		}
		chunkHash := hex.EncodeToString(hasher.Sum(nil))
		remaining -= chunkPlain

		// Build shard plan: either RS-encoded shards or a single shard.
		cm := chunkMeta{
			chunkID:       chunkID,
			sequence:      seq,
			size:          int(chunkPlain),
			sha256:        chunkHash,
			encryptedSize: int(chunkPlain),
		}

		type shardFile struct {
			file *os.File
			size int64
			sm   shardMeta
		}
		var shardFiles []shardFile

		if e.erasureEnc != nil {
			// Reed-Solomon: read chunk data, encode into shards.
			if _, err := chunkTmp.Seek(0, io.SeekStart); err != nil {
				chunkTmp.Close()
				os.Remove(chunkTmp.Name())
				wg.Wait()
				return nil, fmt.Errorf("seeking temp for RS encode chunk %d: %w", seq, err)
			}
			chunkData, err := io.ReadAll(chunkTmp)
			if err != nil {
				chunkTmp.Close()
				os.Remove(chunkTmp.Name())
				wg.Wait()
				return nil, fmt.Errorf("reading chunk data for RS encode chunk %d: %w", seq, err)
			}
			chunkTmp.Close()
			os.Remove(chunkTmp.Name())

			rsShards, err := e.erasureEnc.Encode(chunkData)
			if err != nil {
				wg.Wait()
				return nil, fmt.Errorf("RS-encoding chunk %d: %w", seq, err)
			}

			totalShards := e.erasureEnc.TotalShards()
			cm.dataShards = e.erasureEnc.DataShards()
			cm.parityShards = e.erasureEnc.ParityShards()

			// Assign providers for all shards.
			shardSize := int64(len(rsShards[0]))
			providerIDs, err := e.broker.AssignShards(totalShards, shardSize)
			if err != nil {
				wg.Wait()
				return nil, fmt.Errorf("assigning shards for chunk %d: %w", seq, err)
			}

			// Write each shard to a temp file and reserve space.
			for i := 0; i < totalShards; i++ {
				sf, err := os.CreateTemp("", fmt.Sprintf("pdrive-shard-%d-*", i))
				if err != nil {
					// Clean up already-created shard files.
					for _, prev := range shardFiles {
						prev.file.Close()
						os.Remove(prev.file.Name())
					}
					wg.Wait()
					return nil, fmt.Errorf("creating shard temp file %d for chunk %d: %w", i, seq, err)
				}
				if _, err := sf.Write(rsShards[i]); err != nil {
					sf.Close()
					os.Remove(sf.Name())
					for _, prev := range shardFiles {
						prev.file.Close()
						os.Remove(prev.file.Name())
					}
					wg.Wait()
					return nil, fmt.Errorf("writing shard %d for chunk %d: %w", i, seq, err)
				}

				remotePath := fmt.Sprintf("%s/%s.s%d", chunkRemoteDir, chunkID, i)
				sm := shardMeta{
					shardIndex: i,
					providerID: providerIDs[i],
					remotePath: remotePath,
					size:       int(shardSize),
				}
				cm.shards = append(cm.shards, sm)
				shardFiles = append(shardFiles, shardFile{file: sf, size: shardSize, sm: sm})

				_ = e.db.DeductProviderFreeBytes(providerIDs[i], shardSize)
			}
		} else {
			// No erasure coding: single shard.
			cm.dataShards = 1
			cm.parityShards = 0

			providerID, err := e.broker.AssignChunk(chunkPlain)
			if err != nil {
				chunkTmp.Close()
				os.Remove(chunkTmp.Name())
				wg.Wait()
				return nil, fmt.Errorf("assigning chunk %d: %w", seq, err)
			}

			remotePath := chunkRemoteDir + "/" + chunkID
			cm.shards = []shardMeta{{
				shardIndex: 0,
				providerID: providerID,
				remotePath: remotePath,
				size:       int(chunkPlain),
			}}
			shardFiles = []shardFile{{file: chunkTmp, size: chunkPlain, sm: cm.shards[0]}}

			_ = e.db.DeductProviderFreeBytes(providerID, chunkPlain)
		}

		metas = append(metas, cm)

		sem <- struct{}{}
		wg.Add(1)
		go func(sFiles []shardFile, s int, cmRef chunkMeta) {
			defer func() {
				for _, sf := range sFiles {
					sf.file.Close()
					os.Remove(sf.file.Name())
				}
				<-sem
				wg.Done()
			}()
			retries := maxUploadRetries
			if e.maxChunkRetries > 0 {
				retries = e.maxChunkRetries
			}

			// Upload each shard sequentially within this chunk's goroutine.
			for _, sf := range sFiles {
				// Check for cancellation before starting each shard.
				if cancelCh != nil {
					select {
					case <-cancelCh:
						mu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("upload cancelled")
						}
						mu.Unlock()
						return
					default:
					}
				}

				provider, err := e.db.GetProvider(sf.sm.providerID)
				if err != nil || provider == nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("getting provider %s for shard %d of chunk %d: %w",
							sf.sm.providerID, sf.sm.shardIndex, s, err)
					}
					mu.Unlock()
					return
				}

				var lastErr error
				uploaded := false
				for attempt := 0; attempt < retries; attempt++ {
					// Check for cancellation before each attempt.
					if cancelCh != nil {
						select {
						case <-cancelCh:
							mu.Lock()
							if firstErr == nil {
								firstErr = fmt.Errorf("upload cancelled")
							}
							mu.Unlock()
							return
						default:
						}
					}

					if attempt > 0 {
						if rclonerc.IsQuotaExceeded(lastErr) {
							break
						}
						backoff := time.Duration(1<<uint(attempt)) * time.Second
						if rclonerc.IsRateLimited(lastErr) {
							backoff *= 3
						}
						if backoff > 30*time.Second {
							backoff = 30 * time.Second
						}
						jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
						backoff += jitter
						slog.Warn("retrying shard upload",
							"seq", s, "shard", sf.sm.shardIndex, "attempt", attempt+1, "backoff", backoff)
						time.Sleep(backoff)
					}
					if _, err := sf.file.Seek(0, io.SeekStart); err != nil {
						lastErr = err
						continue
					}
					if attempt == 0 && onChunkUploaded != nil {
						onChunkUploaded(sf.sm.remotePath, sf.size, false)
					}
					<-e.uploadTokens
					if err := e.rc.PutFile(provider.RcloneRemote, sf.sm.remotePath, sf.file); err != nil {
						lastErr = err
						continue
					}
					slog.Debug("shard uploaded", "seq", s, "shard", sf.sm.shardIndex,
						"provider", provider.DisplayName)
					uploaded = true
					mu.Lock()
					uploadedLocs = append(uploadedLocs, metadata.ChunkLocation{
						ChunkID:    cmRef.chunkID,
						ShardIndex: sf.sm.shardIndex,
						ProviderID: sf.sm.providerID,
						RemotePath: sf.sm.remotePath,
					})
					mu.Unlock()
					if onChunkUploaded != nil {
						onChunkUploaded(sf.sm.remotePath, sf.size, true)
					}
					break // success
				}
				if !uploaded {
					_ = e.db.CreditProviderFreeBytes(sf.sm.providerID, sf.size)
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("uploading shard %d of chunk %d after %d retries: %w",
							sf.sm.shardIndex, s, retries, lastErr)
					}
					mu.Unlock()
					return
				}
			}
			e.incCounter(&e.chunksUploaded, "chunks_uploaded", 1)
		}(shardFiles, seq, metas[len(metas)-1])
	}

	wg.Wait()

	if firstErr != nil {
		// Credit back space for shards that were successfully uploaded.
		for _, loc := range uploadedLocs {
			for _, m := range metas {
				for _, s := range m.shards {
					if s.remotePath == loc.RemotePath {
						_ = e.db.CreditProviderFreeBytes(s.providerID, int64(s.size))
					}
				}
			}
		}
		// Clean up shards that were successfully uploaded before the failure.
		if len(uploadedLocs) > 0 {
			slog.Info("cleaning up partial upload", "uploaded_shards", len(uploadedLocs))
			go e.deleteCloudChunks(uploadedLocs)
		}
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
	defer tx.Rollback() //nolint:errcheck — rollback after commit is a no-op

	confirmTime := time.Now().Unix()
	for _, m := range metas {
		if _, err := tx.Exec(
			`INSERT INTO chunks (id, file_id, sequence, size_bytes, sha256, encrypted_size, data_shards, parity_shards) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			m.chunkID, fileID, m.sequence, m.size, m.sha256, m.encryptedSize, m.dataShards, m.parityShards,
		); err != nil {
			return fmt.Errorf("inserting chunk record: %w", err)
		}
		for _, s := range m.shards {
			if _, err := tx.Exec(
				`INSERT INTO chunk_locations (chunk_id, shard_index, provider_id, remote_path, upload_confirmed_at) VALUES (?, ?, ?, ?, ?)`,
				m.chunkID, s.shardIndex, s.providerID, s.remotePath, confirmTime,
			); err != nil {
				return fmt.Errorf("inserting chunk location for shard %d: %w", s.shardIndex, err)
			}
		}
	}
	return tx.Commit()
}

// cloneFileFromDonor creates a new file record that shares the same cloud
// chunks as the donor file (content-hash dedup). No data is uploaded.
// All inserts are wrapped in a single transaction for atomicity.
// If replaceFileID is non-empty, that file record is deleted inside the same
// transaction to avoid a window where neither old nor new record exists.
func (e *Engine) cloneFileFromDonor(donor *metadata.File, fileID, virtualPath string, size int64, sha256Full, replaceFileID string) error {
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
	defer tx.Rollback() //nolint:errcheck — rollback after commit is a no-op

	// Verify the donor still exists inside the transaction. If it was deleted
	// concurrently, its cloud chunks may be getting cleaned up — using those
	// locations would create a file with orphaned references.
	var donorExists int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM files WHERE id = ?`, donor.ID).Scan(&donorExists); err != nil {
		return fmt.Errorf("verifying donor: %w", err)
	}
	if donorExists == 0 {
		return fmt.Errorf("donor file %s was deleted concurrently", donor.VirtualPath)
	}

	// Delete the old file record atomically within this transaction so
	// there's no window where the virtual_path is missing.
	if replaceFileID != "" {
		if _, err := tx.Exec(`DELETE FROM files WHERE id = ?`, replaceFileID); err != nil {
			return fmt.Errorf("removing old file record: %w", err)
		}
	}

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
			`INSERT INTO chunks (id, file_id, sequence, size_bytes, sha256, encrypted_size, data_shards, parity_shards) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			newChunkID, fileID, dc.Sequence, dc.SizeBytes, dc.SHA256, dc.EncryptedSize, dc.DataShards, dc.ParityShards,
		); err != nil {
			return fmt.Errorf("cloning chunk record: %w", err)
		}
		for _, loc := range donorLocs[dc.ID] {
			if _, err := tx.Exec(
				`INSERT INTO chunk_locations (chunk_id, shard_index, provider_id, remote_path, upload_confirmed_at) VALUES (?, ?, ?, ?, ?)`,
				newChunkID, loc.ShardIndex, loc.ProviderID, loc.RemotePath, loc.UploadConfirmedAt,
			); err != nil {
				return fmt.Errorf("cloning chunk location: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing clone transaction: %w", err)
	}

	slog.Info("file deduped (cloned from existing)", "path", virtualPath, "donor", donor.VirtualPath, "size", size)
	e.incCounter(&e.dedupHits, "dedup_hits", 1)
	e.incCounter(&e.filesUploaded, "files_uploaded", 1)
	e.incCounter(&e.bytesUploaded, "bytes_uploaded", size)
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

	schedule := e.chunkSchedule(fileSize)
	estimated := schedule.EstimateChunks(fileSize)
	if estimated == 0 {
		estimated = 1
	}

	// Total bytes to upload equals file size (no encryption overhead).
	bytesTotal := fileSize

	e.uploadsMu.Lock()
	var cancelCh chan struct{}
	if p, ok := e.uploads[fileID]; ok {
		// Entry was pre-registered by WriteFileAsync with Preparing=true;
		// transition to active upload.
		p.TotalChunks = estimated
		p.BytesTotal = bytesTotal
		p.Preparing = false
		p.inFlightChunks = make(map[string]struct{})
		cancelCh = p.cancelCh
	} else {
		// Fallback: WriteFileStream path (no pre-registration).
		cancelCh = make(chan struct{})
		e.uploads[fileID] = &uploadProgress{
			VirtualPath:    virtualPath,
			TotalChunks:    estimated,
			SizeBytes:      fileSize,
			BytesTotal:     bytesTotal,
			StartedAt:      time.Now(),
			inFlightChunks: make(map[string]struct{}),
			cancelCh:       cancelCh,
		}
	}
	e.uploadsMu.Unlock()

	callback := func(remotePath string, encSize int64, done bool) {
		e.uploadsMu.Lock()
		if p, ok := e.uploads[fileID]; ok {
			if done {
				p.ChunksUploaded++
				p.BytesDone += encSize
				delete(p.inFlightChunks, remotePath)
				if p.ChunksUploaded > p.TotalChunks {
					p.TotalChunks = p.ChunksUploaded
				}
			} else {
				p.inFlightChunks[remotePath] = struct{}{}
			}
		}
		e.uploadsMu.Unlock()
	}

	metas, err := e.uploadChunks(r, fileID, fileSize, schedule, callback, cancelCh)
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

// UploadProgress returns a snapshot of all currently in-flight async uploads,
// enriched with real-time transfer speed and in-flight byte counts from rclone.
func (e *Engine) UploadProgress() []UploadProgressInfo {
	var stats rclonerc.TransferProgress
	if e.rc != nil {
		stats = e.rc.TransferStats()
	}

	e.uploadsMu.RLock()
	defer e.uploadsMu.RUnlock()

	out := make([]UploadProgressInfo, 0, len(e.uploads))
	for _, p := range e.uploads {
		// Sum bytes of in-flight chunks from rclone transfer stats.
		// Try exact path match first; fall back to basename match if the
		// rclone stat name doesn't include directory prefixes.
		var inFlightBytes int64
		for rp := range p.inFlightChunks {
			if b, ok := stats.Transferring[rp]; ok {
				inFlightBytes += b
			} else {
				// Basename fallback: rclone sometimes reports just the
				// filename portion depending on the backend.
				base := path.Base(rp)
				if b2, ok := stats.Transferring[base]; ok {
					inFlightBytes += b2
				}
			}
		}

		bytesDone := p.BytesDone + inFlightBytes

		// Elapsed-time estimation fallback: if we have speed but no byte
		// progress yet (chunk-level gaps), estimate progress from elapsed
		// time so the progress bar moves smoothly.
		if bytesDone == 0 && !p.Preparing && stats.SpeedBytes > 0 && p.BytesTotal > 0 {
			elapsed := time.Since(p.StartedAt).Seconds()
			estimated := int64(stats.SpeedBytes * elapsed)
			// Cap at 95% of total to avoid overshooting.
			cap := int64(float64(p.BytesTotal) * 0.95)
			if estimated > cap {
				estimated = cap
			}
			bytesDone = estimated
		}

		out = append(out, UploadProgressInfo{
			VirtualPath:    p.VirtualPath,
			TotalChunks:    p.TotalChunks,
			ChunksUploaded: p.ChunksUploaded,
			SizeBytes:      p.SizeBytes,
			BytesDone:      bytesDone,
			BytesTotal:     p.BytesTotal,
			SpeedBPS:       stats.SpeedBytes,
			StartedAt:      p.StartedAt,
			Failed:         p.Failed,
			Preparing:      p.Preparing,
		})
	}
	return out
}

// removeUploadProgress removes the given file from the in-flight uploads map.
func (e *Engine) removeUploadProgress(fileID string) {
	e.uploadsMu.Lock()
	delete(e.uploads, fileID)
	e.uploadsMu.Unlock()
}

// CancelUpload cancels an in-progress upload by virtual path.
// Returns true if an upload was found and cancelled.
func (e *Engine) CancelUpload(virtualPath string) bool {
	e.uploadsMu.Lock()
	defer e.uploadsMu.Unlock()
	for _, p := range e.uploads {
		if p.VirtualPath == virtualPath {
			select {
			case <-p.cancelCh:
				// already cancelled
			default:
				close(p.cancelCh)
				p.Failed = true
			}
			return true
		}
	}
	return false
}

// RegisterQueuedUpload inserts a "Preparing" entry keyed by a path-based
// placeholder so the UI shows immediate feedback.  SyncDir calls this at the
// very start of upload() — before hashing or spooling.  WriteFileAsync later
// adopts the entry by calling adoptQueuedUpload.
// Returns the placeholder key (caller must pass it to UnregisterQueuedUpload
// if the upload is abandoned before WriteFileAsync is reached).
func (e *Engine) RegisterQueuedUpload(virtualPath string, size int64) string {
	key := "queued:" + virtualPath
	e.uploadsMu.Lock()
	e.uploads[key] = &uploadProgress{
		VirtualPath: virtualPath,
		SizeBytes:   size,
		StartedAt:   time.Now(),
		Preparing:   true,
		cancelCh:    make(chan struct{}),
	}
	e.uploadsMu.Unlock()
	return key
}

// UnregisterQueuedUpload removes a queued placeholder (e.g. when the upload is
// skipped due to dedup or error before WriteFileAsync is called).
func (e *Engine) UnregisterQueuedUpload(key string) {
	e.uploadsMu.Lock()
	delete(e.uploads, key)
	e.uploadsMu.Unlock()
}

// adoptQueuedUpload moves the queued entry (if any) to the real fileID key,
// keeping the original StartedAt timestamp so elapsed time is accurate.
func (e *Engine) adoptQueuedUpload(virtualPath, fileID string, size int64) {
	queueKey := "queued:" + virtualPath
	e.uploadsMu.Lock()
	if p, ok := e.uploads[queueKey]; ok {
		// Re-key from placeholder to real fileID.
		delete(e.uploads, queueKey)
		p.SizeBytes = size // update in case stat changed
		e.uploads[fileID] = p
	} else {
		// No queued entry (direct WriteFileAsync call, e.g. browser upload).
		e.uploads[fileID] = &uploadProgress{
			VirtualPath: virtualPath,
			SizeBytes:   size,
			StartedAt:   time.Now(),
			Preparing:   true,
			cancelCh:    make(chan struct{}),
		}
	}
	e.uploadsMu.Unlock()
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
			if err := e.db.DeleteFile(f.ID); err != nil {
				slog.Warn("failed to remove pending file without tmp_path", "id", f.ID, "error", err)
			}
			continue
		}
		tmpPath := *f.TmpPath
		if _, err := os.Stat(tmpPath); err != nil {
			slog.Warn("tmp file missing for pending upload, removing record",
				"path", f.VirtualPath, "tmpPath", tmpPath)
			if err := e.db.DeleteFile(f.ID); err != nil {
				slog.Warn("failed to remove pending file with missing tmp", "id", f.ID, "error", err)
			}
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
