package engine

import (
	"bytes"
	"log/slog"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

const dbSyncRemotePath = "pdrive-meta/metadata.db"

var (
	backupTimer *time.Timer
	backupMu    sync.Mutex
)

// scheduleBackup debounces metadata DB backup: waits 30s after the last mutation.
func (e *Engine) scheduleBackup() {
	if e.dbPath == "" {
		return
	}
	backupMu.Lock()
	defer backupMu.Unlock()
	if backupTimer != nil {
		backupTimer.Stop()
	}
	backupTimer = time.AfterFunc(30*time.Second, func() {
		if err := e.BackupDB(); err != nil {
			slog.Warn("metadata backup failed", "error", err)
		}
	})
}

// FlushBackup performs an immediate backup (called on shutdown).
func (e *Engine) FlushBackup() {
	backupMu.Lock()
	if backupTimer != nil {
		backupTimer.Stop()
		backupTimer = nil
	}
	backupMu.Unlock()

	if err := e.BackupDB(); err != nil {
		slog.Warn("final metadata backup failed", "error", err)
	}
}

// BackupDB uploads the metadata database to ALL configured cloud providers.
// Having a copy on every provider ensures it can be restored after a total
// loss of one account.
func (e *Engine) BackupDB() error {
	if e.dbPath == "" {
		return nil
	}
	providers, err := e.db.GetAllProviders()
	if err != nil || len(providers) == 0 {
		return nil
	}

	// Checkpoint WAL so the main DB file is complete.
	if _, err := e.db.Conn().Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		slog.Warn("WAL checkpoint failed", "error", err)
	}

	data, err := os.ReadFile(e.dbPath)
	if err != nil {
		return err
	}

	var lastErr error
	for _, provider := range providers {
		if err := e.rc.PutFile(provider.RcloneRemote, dbSyncRemotePath, bytes.NewReader(data)); err != nil {
			slog.Warn("metadata DB backup failed", "provider", provider.DisplayName, "error", err)
			lastErr = err
			continue
		}
		slog.Info("metadata DB backed up to cloud", "provider", provider.DisplayName, "size", len(data))
	}
	return lastErr
}

// GCOrphanedChunks reconciles cloud storage against the metadata DB:
//   - Cloud objects with no DB record → deleted (true orphans from failed uploads/crashes)
//   - DB records with no cloud object → logged as warnings (broken/unreadable files)
//
// Safe to call concurrently with uploads; it only touches objects in pdrive-chunks/
// and never removes anything that has a valid DB entry.
func (e *Engine) GCOrphanedChunks() {
	providers, err := e.db.GetAllProviders()
	if err != nil || len(providers) == 0 {
		return
	}

	// Build set of all remote_paths the DB knows about, keyed by provider.
	allLocs, err := e.db.GetAllChunkLocations()
	if err != nil {
		slog.Warn("gc: failed to load chunk locations", "error", err)
		return
	}
	// known[providerID][remotePath] = true
	known := make(map[string]map[string]bool, len(providers))
	for _, loc := range allLocs {
		if known[loc.ProviderID] == nil {
			known[loc.ProviderID] = make(map[string]bool)
		}
		known[loc.ProviderID][loc.RemotePath] = true
	}

	var orphansDeleted, brokenRecords int
	brokenFileIDs := make(map[string]bool)

	for _, p := range providers {
		items, err := e.rc.ListDir(p.RcloneRemote, chunkRemoteDir)
		if err != nil {
			// Folder may not exist yet (fresh install) — not an error.
			slog.Debug("gc: could not list cloud chunks", "provider", p.DisplayName, "error", err)
			continue
		}

		provKnown := known[p.ID]
		for _, item := range items {
			if item.IsDir {
				continue
			}
			// remote_path stored in DB is e.g. "pdrive-chunks/<uuid>"
			remotePath := path.Join(chunkRemoteDir, item.Name)
			// Normalise — rclone may return "pdrive-chunks/uuid" or just "uuid"
			if !strings.HasPrefix(item.Path, chunkRemoteDir) {
				remotePath = path.Join(chunkRemoteDir, item.Path)
			} else {
				remotePath = item.Path
			}

			if !provKnown[remotePath] {
				slog.Info("gc: deleting orphaned chunk",
					"provider", p.DisplayName, "path", remotePath)
				// Consume a rate-limit token before each cloud API call so GC
				// doesn't burst through the provider quota and starve uploads.
				<-e.uploadTokens
				if err := e.rc.DeleteFile(p.RcloneRemote, remotePath); err != nil {
					slog.Warn("gc: failed to delete orphaned chunk",
						"provider", p.DisplayName, "path", remotePath, "error", err)
				} else {
					orphansDeleted++
				}
			}
		}

		// Inverse check: DB records for this provider with no matching cloud object.
		// Collect the file IDs so we can purge them from the DB entirely — leaving
		// them in place means they show up in the WebDAV listing as readable files
		// but every read attempt fails, which can crash macOS Finder.
		cloudPaths := make(map[string]bool, len(items))
		for _, item := range items {
			cloudPaths[item.Path] = true
			cloudPaths[path.Join(chunkRemoteDir, item.Name)] = true
		}
		for remotePath := range provKnown {
			if !cloudPaths[remotePath] && !cloudPaths[path.Base(remotePath)] {
				brokenRecords++
				// Look up the file_id that owns this chunk location.
				var fileID string
				err := e.db.Conn().QueryRow(`
					SELECT c.file_id FROM chunk_locations cl
					JOIN chunks c ON cl.chunk_id = c.id
					WHERE cl.remote_path = ?
					LIMIT 1`, remotePath).Scan(&fileID)
				if err == nil {
					brokenFileIDs[fileID] = true
				}
			}
		}
	}

	// Remove all files whose cloud chunks are gone. These are unreadable and
	// must not be served over WebDAV.
	for fileID := range brokenFileIDs {
		// Get virtual_path for logging before deletion.
		var vpath string
		_ = e.db.Conn().QueryRow(`SELECT virtual_path FROM files WHERE id = ?`, fileID).Scan(&vpath)
		slog.Warn("gc: removing file with missing cloud chunks",
			"fileID", fileID, "path", vpath)
		if err := e.db.DeleteFile(fileID); err != nil {
			slog.Error("gc: failed to remove broken file record",
				"fileID", fileID, "error", err)
		}
		// If the files record was already gone (orphaned chunks), clean up directly.
		_, _ = e.db.Conn().Exec(`DELETE FROM chunk_locations WHERE chunk_id IN (SELECT id FROM chunks WHERE file_id = ?)`, fileID)
		_, _ = e.db.Conn().Exec(`DELETE FROM chunks WHERE file_id = ?`, fileID)
	}

	// Sweep any leftover orphaned chunk records where the parent file no longer
	// exists (can occur if a previous daemon crash left partial state).
	if _, err := e.db.Conn().Exec(`
		DELETE FROM chunk_locations WHERE chunk_id IN (
			SELECT id FROM chunks WHERE file_id NOT IN (SELECT id FROM files)
		)`); err != nil {
		slog.Warn("gc: failed to sweep orphaned chunk_locations", "error", err)
	}
	if _, err := e.db.Conn().Exec(`
		DELETE FROM chunks WHERE file_id NOT IN (SELECT id FROM files)`); err != nil {
		slog.Warn("gc: failed to sweep orphaned chunks", "error", err)
	}

	slog.Info("gc: orphan scan complete",
		"orphans_deleted", orphansDeleted,
		"broken_db_records", brokenRecords,
		"broken_files_removed", len(brokenFileIDs))
}
