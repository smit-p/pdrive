package engine

import (
	"bytes"
	"log/slog"
	"os"
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

// BackupDB uploads the metadata database to the first available cloud provider.
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

	provider := providers[0]
	if err := e.rc.PutFile(provider.RcloneRemote, dbSyncRemotePath, bytes.NewReader(data)); err != nil {
		return err
	}

	slog.Info("metadata DB backed up to cloud", "provider", provider.DisplayName, "size", len(data))
	return nil
}
