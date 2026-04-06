package daemon

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"

	"github.com/smit-p/pdrive/internal/chunker"
	"github.com/smit-p/pdrive/internal/engine"
)

const saltRemotePath = "pdrive-meta/enc.salt"

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

// resolveCloudSalt tries to fetch the Argon2id salt from any configured cloud remote.
// If found, it derives the encryption key and saves the salt locally.
// If no cloud salt exists, it generates a fresh salt (true first run).
// On success, d.config.EncKey and d.config.Password are set appropriately.
func (d *Daemon) resolveCloudSalt() error {
	saltPath := filepath.Join(d.config.ConfigDir, "enc.salt")

	// Try every remote for an existing salt.
	remotes, err := d.rclone.Client().ListRemotes()
	if err != nil {
		slog.Debug("could not list remotes for salt lookup", "error", err)
	}
	for _, remote := range remotes {
		rc, err := d.rclone.Client().GetFile(remote, saltRemotePath)
		if err != nil {
			continue
		}
		salt, readErr := io.ReadAll(rc)
		rc.Close()
		if readErr != nil || len(salt) != chunker.SaltSize {
			continue
		}
		// Found salt on cloud — derive key and save locally.
		d.config.EncKey = chunker.DeriveKey(d.config.Password, salt)
		if err := os.WriteFile(saltPath, salt, 0600); err != nil {
			slog.Warn("could not save cloud salt locally", "error", err)
		}
		slog.Info("encryption salt restored from cloud", "remote", remote)
		d.config.Password = "" // clear password from memory
		return nil
	}

	// No cloud salt — first run. Generate a fresh salt.
	salt, err := chunker.GenerateSalt()
	if err != nil {
		return fmt.Errorf("generating salt: %w", err)
	}
	if err := os.MkdirAll(d.config.ConfigDir, 0700); err != nil {
		return fmt.Errorf("creating config dir: %w", err)
	}
	if err := os.WriteFile(saltPath, salt, 0600); err != nil {
		return fmt.Errorf("saving salt: %w", err)
	}
	d.config.EncKey = chunker.DeriveKey(d.config.Password, salt)
	d.config.Password = "" // clear password from memory
	slog.Info("new encryption salt generated")
	return nil
}

// tryRestoreDB downloads metadata DB backups from ALL configured rclone remotes,
// decrypts them, and writes the newest one (by embedded timestamp) to dbPath.
// Falls back to the legacy unencrypted path ("pdrive-meta/metadata.db") for
// backward compatibility with backups created before encryption was added.
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
		// Try encrypted backup first.
		if data, ts, ok := d.tryDownloadEncrypted(remote); ok {
			if ts > bestTS {
				bestTS = ts
				bestData = data
				bestRemote = remote
			}
			continue
		}
		// Fall back to legacy unencrypted backup.
		if data, ok := d.tryDownloadLegacy(remote); ok {
			if bestTS == 0 && bestData == nil {
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
	slog.Info("metadata DB restored from cloud", "remote", bestRemote, "size", len(bestData), "encrypted", bestTS > 0)
	return true
}

// tryDownloadEncrypted downloads and decrypts the encrypted backup from a remote.
func (d *Daemon) tryDownloadEncrypted(remote string) (dbData []byte, timestamp int64, ok bool) {
	rc, err := d.rclone.Client().GetFile(remote, "pdrive-meta/metadata.db.enc")
	if err != nil {
		return nil, 0, false
	}
	blob, readErr := io.ReadAll(rc)
	rc.Close()
	if readErr != nil || len(blob) == 0 {
		return nil, 0, false
	}

	plain, err := chunker.Decrypt(d.config.EncKey, blob)
	if err != nil {
		slog.Warn("could not decrypt backup from remote", "remote", remote, "error", err)
		return nil, 0, false
	}

	// Parse header.
	ts, dbData, ok := engine.ParseBackupPayload(plain)
	if !ok {
		return nil, 0, false
	}
	return dbData, ts, true
}

// tryDownloadLegacy downloads a legacy unencrypted backup for backward compatibility.
func (d *Daemon) tryDownloadLegacy(remote string) ([]byte, bool) {
	rc, err := d.rclone.Client().GetFile(remote, "pdrive-meta/metadata.db")
	if err != nil {
		return nil, false
	}
	data, readErr := io.ReadAll(rc)
	rc.Close()
	if readErr != nil || len(data) == 0 {
		return nil, false
	}
	return data, true
}
