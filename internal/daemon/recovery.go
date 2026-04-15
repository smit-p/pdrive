package daemon

import (
	"io"
	"log/slog"
	"os"
	"path"

	"github.com/smit-p/pdrive/internal/engine"
)

// validateRestoredDB checks that the chunk records in the restored DB correspond
// to files that actually exist on the cloud. Returns false if the DB looks stale
// (i.e., has chunk locations pointing to cloud objects that no longer exist).
func (d *Daemon) validateRestoredDB() bool {
	var chunkCount int
	d.db.Conn().QueryRow("SELECT COUNT(*) FROM chunk_locations").Scan(&chunkCount)
	if chunkCount == 0 {
		// Empty or providers-only DB — always valid.
		return true
	}

	// Sample up to 3 chunk locations and verify they exist on cloud.
	rows, err := d.db.Conn().Query(
		`SELECT cl.provider_id, cl.remote_path, p.rclone_remote
		   FROM chunk_locations cl
		   JOIN providers p ON p.id = cl.provider_id
		  LIMIT 3`)
	if err != nil {
		return false
	}
	defer rows.Close()

	for rows.Next() {
		var provID, remotePath, rcloneRemote string
		if err := rows.Scan(&provID, &remotePath, &rcloneRemote); err != nil {
			continue
		}
		// Use a lightweight directory listing of the parent folder to check existence
		// without downloading the full chunk.
		dir := path.Dir(remotePath)
		items, err := d.rclone.Client().ListDir(rcloneRemote, dir)
		if err != nil || len(items) == 0 {
			slog.Warn("restored DB references missing cloud chunk — treating as stale",
				"provider", provID, "path", remotePath)
			return false
		}
	}
	return true
}

// tryRestoreDB downloads metadata DB backups from ALL configured rclone remotes,
// decrypts them, and writes the newest one (by embedded timestamp) to dbPath.
// Returns true if a backup was found and restored.
func (d *Daemon) tryRestoreDB(dbPath string) bool {
	remotes, err := d.rclone.Client().ListRemotes()
	if err != nil {
		slog.Debug("could not list rclone remotes for DB restore", "error", err)
		return false
	}

	var bestData []byte
	var bestTS int64
	var bestRemote string

	for _, remote := range remotes {
		if data, ts, ok := d.tryDownloadBackup(remote); ok {
			if ts > bestTS {
				bestTS = ts
				bestData = data
				bestRemote = remote
			}
		}
	}

	if bestData == nil {
		return false
	}

	if err := os.WriteFile(dbPath, bestData, 0600); err != nil {
		slog.Warn("failed to write restored DB", "error", err)
		return false
	}
	slog.Info("metadata DB restored from cloud", "remote", bestRemote, "size", len(bestData))
	return true
}

// tryDownloadBackup downloads the backup from a remote.
func (d *Daemon) tryDownloadBackup(remote string) (dbData []byte, timestamp int64, ok bool) {
	rc, err := d.rclone.Client().GetFile(remote, "pdrive-meta/metadata.db")
	if err != nil {
		return nil, 0, false
	}
	blob, readErr := io.ReadAll(rc)
	rc.Close()
	if readErr != nil || len(blob) == 0 {
		return nil, 0, false
	}

	// Parse header.
	ts, dbData, ok := engine.ParseBackupPayload(blob)
	if !ok {
		return nil, 0, false
	}
	return dbData, ts, true
}
