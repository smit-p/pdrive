package engine

import (
	"fmt"
	"log/slog"

	"github.com/smit-p/pdrive/internal/metadata"
)

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

	// Compute per-provider credit BEFORE CASCADE deletes chunk rows.
	// Only non-shared cloud objects are counted (matches deleteCloudChunks logic).
	credits, _ := e.db.GetFileChunkCreditsByProvider(file.ID)

	// Delete DB record immediately (CASCADE removes chunks + locations).
	if err := e.db.DeleteFile(file.ID); err != nil {
		return fmt.Errorf("deleting file metadata: %w", err)
	}

	// Credit freed space immediately so the storage dashboard reflects the
	// deletion without waiting for the next full quota resync.
	for providerID, cloudBytes := range credits {
		_ = e.db.CreditProviderFreeBytes(providerID, cloudBytes)
	}

	// Clean up cloud chunks in the background.
	if len(locs) > 0 {
		go e.deleteCloudChunks(locs)
	}

	slog.Info("file deleted", "path", virtualPath)
	e.incCounter(&e.filesDeleted, "files_deleted", 1)
	// Immediate backup — deletions are irreversible and must be synced ASAP.
	go func() {
		if err := e.BackupDB(); err != nil {
			slog.Warn("post-delete backup failed", "error", err)
		}
	}()
	return nil
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
	allCredits := make(map[string]int64)
	for _, f := range files {
		locs, _ := e.db.GetChunkLocationsForFile(f.ID)
		allLocs = append(allLocs, locs...)
		// Accumulate per-provider credit for non-shared chunks.
		if credits, err := e.db.GetFileChunkCreditsByProvider(f.ID); err == nil {
			for providerID, cloudBytes := range credits {
				allCredits[providerID] += cloudBytes
			}
		}
		if err := e.db.DeleteFile(f.ID); err != nil {
			return fmt.Errorf("deleting file record %s: %w", f.VirtualPath, err)
		}
	}
	if err := e.db.DeleteDirectoriesUnder(dirPath); err != nil {
		return fmt.Errorf("deleting directory records: %w", err)
	}

	// Credit freed space immediately so the storage dashboard reflects the
	// deletion without waiting for the next full quota resync.
	for providerID, cloudBytes := range allCredits {
		_ = e.db.CreditProviderFreeBytes(providerID, cloudBytes)
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
			if insertErr := e.db.InsertFailedDeletion(loc.ProviderID, loc.RemotePath, err.Error()); insertErr != nil {
				slog.Warn("failed to queue failed deletion", "error", insertErr)
			}
		}
	}
	slog.Debug("cloud chunk cleanup done", "count", len(locs))
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
			if err := e.db.DeleteFailedDeletion(item.ID); err != nil {
				slog.Warn("failed to remove abandoned deletion record", "error", err)
			}
			abandoned++
			continue
		}

		provider, err := e.db.GetProvider(item.ProviderID)
		if err != nil || provider == nil {
			if err := e.db.DeleteFailedDeletion(item.ID); err != nil {
				slog.Warn("failed to remove orphan deletion record", "error", err)
			}
			abandoned++
			continue
		}

		<-e.uploadTokens
		if err := e.rc.DeleteFile(provider.RcloneRemote, item.RemotePath); err != nil {
			if retryErr := e.db.IncrementFailedDeletionRetry(item.ID, err.Error()); retryErr != nil {
				slog.Warn("failed to update retry count", "error", retryErr)
			}
			retried++
		} else {
			if err := e.db.DeleteFailedDeletion(item.ID); err != nil {
				slog.Warn("failed to remove successful deletion record", "error", err)
			}
			succeeded++
		}
	}

	if retried+succeeded+abandoned > 0 {
		slog.Info("failed deletion retry complete",
			"succeeded", succeeded, "retried", retried, "abandoned", abandoned)
	}
}
