package metadata

import (
	"database/sql"
	"fmt"
	"path"
	"strings"
	"time"
)

// escapeLike escapes SQL LIKE wildcard characters (%, _) in a literal string
// so they are matched verbatim.  The caller must add ESCAPE '\' to the query.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

func escapeLike(s string) string { return likeEscaper.Replace(s) }

// File represents a virtual file in the pdrive filesystem.
type File struct {
	ID          string
	VirtualPath string
	SizeBytes   int64
	CreatedAt   int64
	ModifiedAt  int64
	SHA256Full  string
	UploadState string  // "pending" or "complete"
	TmpPath     *string // path to local tmp file while pending; nil when complete
}

// ChunkRecord represents a chunk row in the database.
type ChunkRecord struct {
	ID            string
	FileID        string
	Sequence      int
	SizeBytes     int
	SHA256        string
	EncryptedSize int
	DataShards    int
	ParityShards  int
}

// ChunkLocation represents a chunk_locations row.
type ChunkLocation struct {
	ChunkID           string
	ShardIndex        int
	ProviderID        string
	RemotePath        string
	UploadConfirmedAt *int64
}

// Provider represents a cloud storage provider account.
type Provider struct {
	ID               string
	Type             string
	DisplayName      string
	RcloneRemote     string
	AccountIdentity  string
	QuotaTotalBytes  *int64
	QuotaFreeBytes   *int64
	QuotaPolledAt    *int64
	RateLimitedUntil *int64
}

// GetProviderChunkBytes returns the total encrypted bytes pdrive has stored on
// each provider, keyed by provider ID.
func (db *DB) GetProviderChunkBytes() (map[string]int64, error) {
	rows, err := db.conn.Query(`
		SELECT cl.provider_id, COALESCE(SUM(c.encrypted_size), 0)
		FROM chunk_locations cl
		JOIN chunks c ON c.id = cl.chunk_id
		GROUP BY cl.provider_id`)
	if err != nil {
		return nil, fmt.Errorf("querying provider chunk bytes: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var providerID string
		var bytes int64
		if err := rows.Scan(&providerID, &bytes); err != nil {
			return nil, err
		}
		out[providerID] = bytes
	}
	return out, rows.Err()
}

// InsertFile inserts or replaces a file record.
func (db *DB) InsertFile(f *File) error {
	state := f.UploadState
	if state == "" {
		state = "complete"
	}
	_, err := db.conn.Exec(
		`INSERT OR REPLACE INTO files (id, virtual_path, size_bytes, created_at, modified_at, sha256_full, upload_state, tmp_path)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		f.ID, f.VirtualPath, f.SizeBytes, f.CreatedAt, f.ModifiedAt, f.SHA256Full, state, f.TmpPath,
	)
	return err
}

// SetUploadComplete marks a file as fully uploaded and clears the tmp_path.
func (db *DB) SetUploadComplete(fileID string) error {
	_, err := db.conn.Exec(
		`UPDATE files SET upload_state = 'complete', tmp_path = NULL WHERE id = ?`, fileID,
	)
	return err
}

// GetPendingUploads returns all files with upload_state = 'pending'.
func (db *DB) GetPendingUploads() ([]File, error) {
	rows, err := db.conn.Query(
		`SELECT id, virtual_path, size_bytes, created_at, modified_at, sha256_full, upload_state, tmp_path
		 FROM files WHERE upload_state = 'pending'`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.ID, &f.VirtualPath, &f.SizeBytes, &f.CreatedAt, &f.ModifiedAt, &f.SHA256Full, &f.UploadState, &f.TmpPath); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

// InsertChunk inserts a new chunk record.
// DataShards defaults to 1 when unset (zero).
func (db *DB) InsertChunk(c *ChunkRecord) error {
	ds := c.DataShards
	if ds == 0 {
		ds = 1
	}
	_, err := db.conn.Exec(
		`INSERT INTO chunks (id, file_id, sequence, size_bytes, sha256, encrypted_size, data_shards, parity_shards)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		c.ID, c.FileID, c.Sequence, c.SizeBytes, c.SHA256, c.EncryptedSize, ds, c.ParityShards,
	)
	return err
}

// InsertChunkLocation inserts a new chunk location record.
func (db *DB) InsertChunkLocation(cl *ChunkLocation) error {
	_, err := db.conn.Exec(
		`INSERT INTO chunk_locations (chunk_id, shard_index, provider_id, remote_path, upload_confirmed_at)
		 VALUES (?, ?, ?, ?, ?)`,
		cl.ChunkID, cl.ShardIndex, cl.ProviderID, cl.RemotePath, cl.UploadConfirmedAt,
	)
	return err
}

// ConfirmUpload sets the upload_confirmed_at timestamp for a chunk location.
func (db *DB) ConfirmUpload(chunkID string, shardIndex int) error {
	now := time.Now().Unix()
	res, err := db.conn.Exec(
		`UPDATE chunk_locations SET upload_confirmed_at = ? WHERE chunk_id = ? AND shard_index = ?`,
		now, chunkID, shardIndex,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("chunk location not found: chunk=%s shard=%d", chunkID, shardIndex)
	}
	return nil
}

// GetFileByPath retrieves a file by its virtual path.
func (db *DB) GetFileByPath(virtualPath string) (*File, error) {
	f := &File{}
	err := db.conn.QueryRow(
		`SELECT id, virtual_path, size_bytes, created_at, modified_at, sha256_full, upload_state, tmp_path
		 FROM files WHERE virtual_path = ?`, virtualPath,
	).Scan(&f.ID, &f.VirtualPath, &f.SizeBytes, &f.CreatedAt, &f.ModifiedAt, &f.SHA256Full, &f.UploadState, &f.TmpPath)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return f, err
}

// GetCompleteFileByPath returns the file record only if it is fully uploaded.
// Used for read operations (Stat, ReadFile) where pending files must appear
// "not yet visible" to the mount.
func (db *DB) GetCompleteFileByPath(virtualPath string) (*File, error) {
	f := &File{}
	err := db.conn.QueryRow(
		`SELECT id, virtual_path, size_bytes, created_at, modified_at, sha256_full, upload_state, tmp_path
		 FROM files WHERE virtual_path = ? AND upload_state = 'complete'`, virtualPath,
	).Scan(&f.ID, &f.VirtualPath, &f.SizeBytes, &f.CreatedAt, &f.ModifiedAt, &f.SHA256Full, &f.UploadState, &f.TmpPath)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return f, err
}

// GetCompleteFileByHash returns a completed file with the given content hash.
// Used for content-hash deduplication: if another file has the same SHA256,
// its chunks can be reused instead of re-uploading.
func (db *DB) GetCompleteFileByHash(sha256Full string) (*File, error) {
	f := &File{}
	err := db.conn.QueryRow(
		`SELECT id, virtual_path, size_bytes, created_at, modified_at, sha256_full, upload_state, tmp_path
		 FROM files WHERE sha256_full = ? AND upload_state = 'complete' LIMIT 1`, sha256Full,
	).Scan(&f.ID, &f.VirtualPath, &f.SizeBytes, &f.CreatedAt, &f.ModifiedAt, &f.SHA256Full, &f.UploadState, &f.TmpPath)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return f, err
}

// GetChunksForFile returns all chunks for a file, ordered by sequence.
func (db *DB) GetChunksForFile(fileID string) ([]ChunkRecord, error) {
	rows, err := db.conn.Query(
		`SELECT id, file_id, sequence, size_bytes, sha256, encrypted_size, data_shards, parity_shards
		 FROM chunks WHERE file_id = ? ORDER BY sequence`, fileID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []ChunkRecord
	for rows.Next() {
		var c ChunkRecord
		if err := rows.Scan(&c.ID, &c.FileID, &c.Sequence, &c.SizeBytes, &c.SHA256, &c.EncryptedSize, &c.DataShards, &c.ParityShards); err != nil {
			return nil, err
		}
		chunks = append(chunks, c)
	}
	return chunks, rows.Err()
}

// GetChunkLocations returns all locations for a given chunk.
func (db *DB) GetChunkLocations(chunkID string) ([]ChunkLocation, error) {
	rows, err := db.conn.Query(
		`SELECT chunk_id, shard_index, provider_id, remote_path, upload_confirmed_at
		 FROM chunk_locations WHERE chunk_id = ? ORDER BY shard_index`, chunkID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locs []ChunkLocation
	for rows.Next() {
		var cl ChunkLocation
		if err := rows.Scan(&cl.ChunkID, &cl.ShardIndex, &cl.ProviderID, &cl.RemotePath, &cl.UploadConfirmedAt); err != nil {
			return nil, err
		}
		locs = append(locs, cl)
	}
	return locs, rows.Err()
}

// DeleteFile deletes a file and all its associated chunks/locations (cascading).
func (db *DB) DeleteFile(fileID string) error {
	_, err := db.conn.Exec(`DELETE FROM files WHERE id = ?`, fileID)
	return err
}

// DeleteFileByPath deletes a file by virtual path.
func (db *DB) DeleteFileByPath(virtualPath string) error {
	_, err := db.conn.Exec(`DELETE FROM files WHERE virtual_path = ?`, virtualPath)
	return err
}

// ListFiles returns files whose virtual_path is directly inside dirPath.
// dirPath should be like "/" or "/subdir/".
// Filtering for direct children is done in SQL using INSTR to avoid fetching
// deeply nested files into Go.
func (db *DB) ListFiles(dirPath string) ([]File, error) {
	// Normalize: ensure dirPath ends with /
	if !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}

	rows, err := db.conn.Query(
		`SELECT id, virtual_path, size_bytes, created_at, modified_at, sha256_full, upload_state, tmp_path
		 FROM files
		 WHERE virtual_path LIKE ? ESCAPE '\'
		   AND upload_state = 'complete'
		   AND INSTR(SUBSTR(virtual_path, LENGTH(?) + 1), '/') = 0`, escapeLike(dirPath)+"%", dirPath,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.ID, &f.VirtualPath, &f.SizeBytes, &f.CreatedAt, &f.ModifiedAt, &f.SHA256Full, &f.UploadState, &f.TmpPath); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

// ListSubdirectories returns unique immediate subdirectory names under dirPath.
func (db *DB) ListSubdirectories(dirPath string) ([]string, error) {
	if !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}
	cleanDirPath := strings.TrimSuffix(dirPath, "/")

	seen := make(map[string]bool)

	// 1. Explicit directories that are immediate children.
	dirRows, err := db.conn.Query(`SELECT path FROM directories WHERE path LIKE ? ESCAPE '\'`, escapeLike(dirPath)+"%")
	if err != nil {
		return nil, err
	}
	for dirRows.Next() {
		var p string
		if err := dirRows.Scan(&p); err != nil {
			dirRows.Close()
			return nil, err
		}
		var rel string
		if cleanDirPath == "" {
			rel = strings.TrimPrefix(p, "/")
		} else {
			rel = strings.TrimPrefix(p, cleanDirPath+"/")
		}
		if idx := strings.Index(rel, "/"); idx >= 0 {
			rel = rel[:idx]
		}
		if rel != "" {
			seen[rel] = true
		}
	}
	dirRows.Close()
	if err := dirRows.Err(); err != nil {
		return nil, err
	}

	// 2. Implicit directories from file paths.
	fileRows, err := db.conn.Query(
		`SELECT DISTINCT virtual_path FROM files WHERE virtual_path LIKE ? ESCAPE '\'`, escapeLike(dirPath)+"%",
	)
	if err != nil {
		return nil, err
	}
	for fileRows.Next() {
		var vpath string
		if err := fileRows.Scan(&vpath); err != nil {
			fileRows.Close()
			return nil, err
		}
		rel := strings.TrimPrefix(vpath, dirPath)
		if idx := strings.Index(rel, "/"); idx >= 0 {
			seen[rel[:idx]] = true
		}
	}
	fileRows.Close()
	if err := fileRows.Err(); err != nil {
		return nil, err
	}

	var dirs []string
	for d := range seen {
		dirs = append(dirs, d)
	}
	return dirs, nil
}

// UpsertProvider inserts or updates a provider record.
func (db *DB) UpsertProvider(p *Provider) error {
	_, err := db.conn.Exec(
		`INSERT INTO providers (id, type, display_name, rclone_remote, account_identity, quota_total_bytes, quota_free_bytes, quota_polled_at, rate_limited_until)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   type = excluded.type,
		   display_name = excluded.display_name,
		   rclone_remote = excluded.rclone_remote,
		   account_identity = excluded.account_identity,
		   quota_total_bytes = excluded.quota_total_bytes,
		   quota_free_bytes = excluded.quota_free_bytes,
		   quota_polled_at = excluded.quota_polled_at,
		   rate_limited_until = excluded.rate_limited_until`,
		p.ID, p.Type, p.DisplayName, p.RcloneRemote, p.AccountIdentity, p.QuotaTotalBytes, p.QuotaFreeBytes, p.QuotaPolledAt, p.RateLimitedUntil,
	)
	return err
}

// GetProvider retrieves a provider by ID.
func (db *DB) GetProvider(id string) (*Provider, error) {
	p := &Provider{}
	err := db.conn.QueryRow(
		`SELECT id, type, display_name, rclone_remote, account_identity, quota_total_bytes, quota_free_bytes, quota_polled_at, rate_limited_until
		 FROM providers WHERE id = ?`, id,
	).Scan(&p.ID, &p.Type, &p.DisplayName, &p.RcloneRemote, &p.AccountIdentity, &p.QuotaTotalBytes, &p.QuotaFreeBytes, &p.QuotaPolledAt, &p.RateLimitedUntil)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return p, err
}

// GetAllProviders returns all registered providers.
func (db *DB) GetAllProviders() ([]Provider, error) {
	rows, err := db.conn.Query(
		`SELECT id, type, display_name, rclone_remote, account_identity, quota_total_bytes, quota_free_bytes, quota_polled_at, rate_limited_until
		 FROM providers ORDER BY display_name`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var providers []Provider
	for rows.Next() {
		var p Provider
		if err := rows.Scan(&p.ID, &p.Type, &p.DisplayName, &p.RcloneRemote, &p.AccountIdentity, &p.QuotaTotalBytes, &p.QuotaFreeBytes, &p.QuotaPolledAt, &p.RateLimitedUntil); err != nil {
			return nil, err
		}
		providers = append(providers, p)
	}
	return providers, rows.Err()
}

// GetProviderByRemote returns the provider matching the given rclone remote name, or nil if not found.
func (db *DB) GetProviderByRemote(rcloneRemote string) (*Provider, error) {
	p := &Provider{}
	err := db.conn.QueryRow(
		`SELECT id, type, display_name, rclone_remote, account_identity, quota_total_bytes, quota_free_bytes, quota_polled_at, rate_limited_until
		 FROM providers WHERE rclone_remote = ?`, rcloneRemote,
	).Scan(&p.ID, &p.Type, &p.DisplayName, &p.RcloneRemote, &p.AccountIdentity, &p.QuotaTotalBytes, &p.QuotaFreeBytes, &p.QuotaPolledAt, &p.RateLimitedUntil)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return p, err
}

// DeductProviderFreeBytes atomically decreases a provider's quota_free_bytes
// by delta. Used after each chunk upload to keep the broker's space view fresh
// between full quota syncs.
func (db *DB) DeductProviderFreeBytes(providerID string, delta int64) error {
	_, err := db.conn.Exec(
		`UPDATE providers SET quota_free_bytes = MAX(quota_free_bytes - ?, 0) WHERE id = ? AND quota_free_bytes IS NOT NULL`,
		delta, providerID,
	)
	return err
}

// CreditProviderFreeBytes atomically increases a provider's quota_free_bytes
// by delta.  Used to roll back an optimistic space reservation when a chunk
// upload fails.
func (db *DB) CreditProviderFreeBytes(providerID string, delta int64) error {
	_, err := db.conn.Exec(
		`UPDATE providers SET quota_free_bytes = quota_free_bytes + ? WHERE id = ? AND quota_free_bytes IS NOT NULL`,
		delta, providerID,
	)
	return err
}

// GetChunkLocationsForFile returns all chunk locations for every chunk belonging to a file.
func (db *DB) GetChunkLocationsForFile(fileID string) ([]ChunkLocation, error) {
	rows, err := db.conn.Query(
		`SELECT cl.chunk_id, cl.shard_index, cl.provider_id, cl.remote_path, cl.upload_confirmed_at
		 FROM chunk_locations cl
		 JOIN chunks c ON c.id = cl.chunk_id
		 WHERE c.file_id = ?
		 ORDER BY c.sequence, cl.shard_index`, fileID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locs []ChunkLocation
	for rows.Next() {
		var cl ChunkLocation
		if err := rows.Scan(&cl.ChunkID, &cl.ShardIndex, &cl.ProviderID, &cl.RemotePath, &cl.UploadConfirmedAt); err != nil {
			return nil, err
		}
		locs = append(locs, cl)
	}
	return locs, rows.Err()
}

// GetAllChunkLocations returns every chunk_location record in the database.
// Used by the orphan GC to build the set of cloud objects that should exist.
func (db *DB) GetAllChunkLocations() ([]ChunkLocation, error) {
	rows, err := db.conn.Query(
		`SELECT chunk_id, shard_index, provider_id, remote_path, upload_confirmed_at FROM chunk_locations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locs []ChunkLocation
	for rows.Next() {
		var cl ChunkLocation
		if err := rows.Scan(&cl.ChunkID, &cl.ShardIndex, &cl.ProviderID, &cl.RemotePath, &cl.UploadConfirmedAt); err != nil {
			return nil, err
		}
		locs = append(locs, cl)
	}
	return locs, rows.Err()
}

// RemotePathRefCount returns the number of chunk_location rows that reference
// the given remote_path. Used to avoid deleting shared cloud objects when a
// dedup-cloned file is removed.
func (db *DB) RemotePathRefCount(remotePath string) (int, error) {
	var count int
	err := db.conn.QueryRow(
		`SELECT COUNT(*) FROM chunk_locations WHERE remote_path = ?`, remotePath,
	).Scan(&count)
	return count, err
}

// FileExists checks if a virtual path exists in the database.
func (db *DB) FileExists(virtualPath string) (bool, error) {
	var count int
	err := db.conn.QueryRow(`SELECT COUNT(*) FROM files WHERE virtual_path = ?`, virtualPath).Scan(&count)
	return count > 0, err
}

// PathIsDir checks if a path is a directory (explicit or implicit from file paths).
func (db *DB) PathIsDir(dirPath string) (bool, error) {
	cleanDir := strings.TrimSuffix(dirPath, "/")
	if cleanDir == "" {
		return true, nil
	}

	// Check explicit directories.
	var dirCount int
	if err := db.conn.QueryRow(`SELECT COUNT(*) FROM directories WHERE path = ?`, cleanDir).Scan(&dirCount); err != nil {
		return false, err
	}
	if dirCount > 0 {
		return true, nil
	}

	// Check implicit directories (files under this path).
	prefix := cleanDir + "/"
	var fileCount int
	err := db.conn.QueryRow(`SELECT COUNT(*) FROM files WHERE virtual_path LIKE ? ESCAPE '\'`, escapeLike(prefix)+"%").Scan(&fileCount)
	return fileCount > 0, err
}

// VirtualDir returns the parent directory of a virtual path.
// Returns "/" for top-level paths.
func VirtualDir(virtualPath string) string {
	dir := path.Dir(virtualPath)
	if dir == "." {
		return "/"
	}
	return dir
}

// CreateDirectory records an explicit directory.
func (db *DB) CreateDirectory(dirPath string) error {
	dirPath = strings.TrimSuffix(dirPath, "/")
	if dirPath == "" {
		return nil // root always exists
	}
	now := time.Now().Unix()
	_, err := db.conn.Exec(
		`INSERT OR IGNORE INTO directories (path, created_at) VALUES (?, ?)`,
		dirPath, now,
	)
	return err
}

// DirectoryExists checks if an explicit directory record exists.
func (db *DB) DirectoryExists(dirPath string) (bool, error) {
	dirPath = strings.TrimSuffix(dirPath, "/")
	if dirPath == "" {
		return true, nil
	}
	var count int
	err := db.conn.QueryRow(`SELECT COUNT(*) FROM directories WHERE path = ?`, dirPath).Scan(&count)
	return count > 0, err
}

// DeleteDirectory deletes an explicit directory record.
func (db *DB) DeleteDirectory(dirPath string) error {
	dirPath = strings.TrimSuffix(dirPath, "/")
	_, err := db.conn.Exec(`DELETE FROM directories WHERE path = ?`, dirPath)
	return err
}

// DeleteDirectoriesUnder deletes all explicit directory records under (and including) a prefix.
func (db *DB) DeleteDirectoriesUnder(dirPath string) error {
	dirPath = strings.TrimSuffix(dirPath, "/")
	_, err := db.conn.Exec(`DELETE FROM directories WHERE path = ? OR path LIKE ? ESCAPE '\'`,
		dirPath, escapeLike(dirPath)+"/%")
	return err
}

// GetFilesUnderDir returns all files with virtual_path starting with dirPath/.
func (db *DB) GetFilesUnderDir(dirPath string) ([]File, error) {
	prefix := strings.TrimSuffix(dirPath, "/") + "/"
	rows, err := db.conn.Query(
		`SELECT id, virtual_path, size_bytes, created_at, modified_at, sha256_full, upload_state, tmp_path
		 FROM files WHERE virtual_path LIKE ? ESCAPE '\'`, escapeLike(prefix)+"%",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var files []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.ID, &f.VirtualPath, &f.SizeBytes, &f.CreatedAt, &f.ModifiedAt, &f.SHA256Full, &f.UploadState, &f.TmpPath); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

// RenameFileByPath updates the virtual_path of a single file record.
func (db *DB) RenameFileByPath(oldPath, newPath string) error {
	_, err := db.conn.Exec(
		`UPDATE files SET virtual_path = ?, modified_at = ? WHERE virtual_path = ?`,
		newPath, time.Now().Unix(), oldPath,
	)
	return err
}

// RenameFilesUnderDir renames all files under oldDir to be under newDir.
func (db *DB) RenameFilesUnderDir(oldDir, newDir string) error {
	oldPrefix := strings.TrimSuffix(oldDir, "/") + "/"
	newPrefix := strings.TrimSuffix(newDir, "/") + "/"
	_, err := db.conn.Exec(
		`UPDATE files SET virtual_path = ? || SUBSTR(virtual_path, ?)
		 WHERE virtual_path LIKE ? ESCAPE '\'`,
		newPrefix, len(oldPrefix)+1, escapeLike(oldPrefix)+"%",
	)
	return err
}

// RenameDirectoriesUnder renames directory records from oldDir to newDir.
func (db *DB) RenameDirectoriesUnder(oldDir, newDir string) error {
	oldDir = strings.TrimSuffix(oldDir, "/")
	newDir = strings.TrimSuffix(newDir, "/")
	// Rename the directory itself.
	_, err := db.conn.Exec(`UPDATE directories SET path = ? WHERE path = ?`, newDir, oldDir)
	if err != nil {
		return err
	}
	// Rename subdirectories.
	_, err = db.conn.Exec(
		`UPDATE directories SET path = ? || SUBSTR(path, ?)
		 WHERE path LIKE ? ESCAPE '\'`,
		newDir, len(oldDir)+1, escapeLike(oldDir)+"/%",
	)
	return err
}

// FailedDeletion represents a cloud chunk deletion that failed and must be retried.
// Each record lives in the failed_deletions table until the retry succeeds or
// the maximum retry count is reached.
type FailedDeletion struct {
	ID         int64
	ProviderID string
	RemotePath string
	FailedAt   int64
	RetryCount int
	LastError  string
}

// InsertFailedDeletion records a chunk deletion that failed for later retry.
func (db *DB) InsertFailedDeletion(providerID, remotePath, lastError string) error {
	_, err := db.conn.Exec(
		`INSERT INTO failed_deletions (provider_id, remote_path, failed_at, retry_count, last_error)
		 VALUES (?, ?, ?, 0, ?)`,
		providerID, remotePath, time.Now().Unix(), lastError,
	)
	return err
}

// GetFailedDeletions returns up to limit failed deletions ordered oldest first.
func (db *DB) GetFailedDeletions(limit int) ([]FailedDeletion, error) {
	rows, err := db.conn.Query(
		`SELECT id, provider_id, remote_path, failed_at, retry_count, last_error
		 FROM failed_deletions ORDER BY failed_at ASC LIMIT ?`, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []FailedDeletion
	for rows.Next() {
		var d FailedDeletion
		if err := rows.Scan(&d.ID, &d.ProviderID, &d.RemotePath, &d.FailedAt, &d.RetryCount, &d.LastError); err != nil {
			return nil, err
		}
		items = append(items, d)
	}
	return items, rows.Err()
}

// DeleteFailedDeletion removes a failed deletion record (after successful retry).
func (db *DB) DeleteFailedDeletion(id int64) error {
	_, err := db.conn.Exec(`DELETE FROM failed_deletions WHERE id = ?`, id)
	return err
}

// IncrementFailedDeletionRetry bumps the retry count and updates the error.
func (db *DB) IncrementFailedDeletionRetry(id int64, lastError string) error {
	_, err := db.conn.Exec(
		`UPDATE failed_deletions SET retry_count = retry_count + 1, last_error = ? WHERE id = ?`,
		lastError, id,
	)
	return err
}

// GetChunkLocationsByProvider returns all chunk_location records for a given provider.
// Used by GC to avoid loading the entire chunk_locations table into memory at once.
func (db *DB) GetChunkLocationsByProvider(providerID string) ([]ChunkLocation, error) {
	rows, err := db.conn.Query(
		`SELECT chunk_id, shard_index, provider_id, remote_path, upload_confirmed_at
		 FROM chunk_locations WHERE provider_id = ?`, providerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locs []ChunkLocation
	for rows.Next() {
		var cl ChunkLocation
		if err := rows.Scan(&cl.ChunkID, &cl.ShardIndex, &cl.ProviderID, &cl.RemotePath, &cl.UploadConfirmedAt); err != nil {
			return nil, err
		}
		locs = append(locs, cl)
	}
	return locs, rows.Err()
}

// SearchFiles returns all completed files whose virtual_path contains the
// given pattern (case-insensitive LIKE match) under the specified root.
func (db *DB) SearchFiles(root, pattern string) ([]File, error) {
	if !strings.HasSuffix(root, "/") {
		root += "/"
	}
	// Escape LIKE special characters in the user-provided pattern so that
	// literal '%' and '_' in the search term are not treated as wildcards.
	escaped := escapeLike(pattern)
	likePattern := "%" + escaped + "%"
	rows, err := db.conn.Query(
		`SELECT id, virtual_path, size_bytes, created_at, modified_at, sha256_full, upload_state, tmp_path
		 FROM files
		 WHERE virtual_path LIKE ? ESCAPE '\'
		   AND upload_state = 'complete'
		   AND virtual_path LIKE ? ESCAPE '\'`, escapeLike(root)+"%", likePattern,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.ID, &f.VirtualPath, &f.SizeBytes, &f.CreatedAt, &f.ModifiedAt, &f.SHA256Full, &f.UploadState, &f.TmpPath); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

// ListAllFiles returns all completed files under root, recursively.
func (db *DB) ListAllFiles(root string) ([]File, error) {
	if !strings.HasSuffix(root, "/") {
		root += "/"
	}
	rows, err := db.conn.Query(
		`SELECT id, virtual_path, size_bytes, created_at, modified_at, sha256_full, upload_state, tmp_path
		 FROM files
		 WHERE virtual_path LIKE ? ESCAPE '\'
		   AND upload_state = 'complete'
		 ORDER BY virtual_path`, escapeLike(root)+"%",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.ID, &f.VirtualPath, &f.SizeBytes, &f.CreatedAt, &f.ModifiedAt, &f.SHA256Full, &f.UploadState, &f.TmpPath); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

// DiskUsage returns (file_count, total_bytes) for all completed files under root.
func (db *DB) DiskUsage(root string) (int64, int64, error) {
	if !strings.HasSuffix(root, "/") {
		root += "/"
	}
	var count, total int64
	err := db.conn.QueryRow(
		`SELECT COUNT(*), COALESCE(SUM(size_bytes), 0)
		 FROM files
		 WHERE virtual_path LIKE ? ESCAPE '\'
		   AND upload_state = 'complete'`, escapeLike(root)+"%",
	).Scan(&count, &total)
	return count, total, err
}

// ActivityEntry represents a row in the activity_log table, which tracks
// user-visible actions (uploads, downloads, deletes, etc.) for the UI.
type ActivityEntry struct {
	ID        int64  `json:"id"`
	Action    string `json:"action"`
	Path      string `json:"path"`
	Detail    string `json:"detail"`
	CreatedAt int64  `json:"created_at"`
}

// InsertActivity logs an action to the activity_log table.
func (db *DB) InsertActivity(action, path, detail string) error {
	_, err := db.conn.Exec(
		`INSERT INTO activity_log (action, path, detail, created_at) VALUES (?, ?, ?, ?)`,
		action, path, detail, time.Now().Unix(),
	)
	return err
}

// RecentActivity returns the most recent N activity log entries.
func (db *DB) RecentActivity(limit int) ([]ActivityEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := db.conn.Query(
		`SELECT id, action, path, COALESCE(detail, ''), created_at
		 FROM activity_log ORDER BY created_at DESC, id DESC LIMIT ?`, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []ActivityEntry
	for rows.Next() {
		var e ActivityEntry
		if err := rows.Scan(&e.ID, &e.Action, &e.Path, &e.Detail, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// IncrementCounter atomically increments a named counter by delta.
func (db *DB) IncrementCounter(key string, delta int64) error {
	_, err := db.conn.Exec(
		`INSERT INTO counters (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = value + excluded.value`,
		key, delta,
	)
	return err
}

// LoadCounters returns all persisted counters as a key→value map.
func (db *DB) LoadCounters() (map[string]int64, error) {
	rows, err := db.conn.Query(`SELECT key, value FROM counters`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := make(map[string]int64)
	for rows.Next() {
		var k string
		var v int64
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, rows.Err()
}
