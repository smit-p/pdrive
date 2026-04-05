package daemon

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/chunker"
	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/vfs"
	"golang.org/x/net/webdav"
)

//go:embed browser_ui.html
var browserUIHTML string

// API response types.

type lsFile struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	ModifiedAt int64  `json:"modified_at"`
	LocalState string `json:"local_state"` // "local", "stub", or "uploading"
}

type lsResponse struct {
	Path  string   `json:"path"`
	Dirs  []string `json:"dirs"`
	Files []lsFile `json:"files"`
}

type statusProvider struct {
	Name              string `json:"name"`
	Type              string `json:"type,omitempty"`
	AccountIdentity   string `json:"account_identity,omitempty"`
	QuotaTotalBytes   *int64 `json:"quota_total_bytes"`
	QuotaFreeBytes    *int64 `json:"quota_free_bytes"`
	QuotaUsedByPdrive *int64 `json:"quota_used_by_pdrive"`
}

type statusResponse struct {
	TotalFiles int64            `json:"total_files"`
	TotalBytes int64            `json:"total_bytes"`
	Providers  []statusProvider `json:"providers"`
}

// Config holds daemon configuration.
type Config struct {
	ConfigDir    string   // ~/.pdrive/
	RcloneBin    string   // path to rclone binary
	RcloneAddr   string   // e.g., "127.0.0.1:5572"
	WebDAVAddr   string   // e.g., "127.0.0.1:8765"
	SyncDir      string   // local folder to sync (e.g. ~/pdrive); empty disables sync
	EncKey       []byte   // 32-byte AES-256 key (set when salt is available locally)
	Password     string   // raw password — when set and EncKey is nil, daemon derives key after cloud salt lookup
	BrokerPolicy string   // "pfrd" or "mfs"
	MinFreeSpace int64    // bytes to keep free on each provider
	SkipRestore  bool     // skip cloud DB restore on startup (useful after a manual wipe)
	ChunkSize    int      // override chunk size (bytes); 0 uses dynamic sizing
	RatePerSec   int      // API rate limit (tokens per second); 0 uses default (8/s)
	Remotes      []string // rclone remote names to use; empty means all
}

// Daemon is the main pdrive daemon that ties everything together.
type Daemon struct {
	config       Config
	db           *metadata.DB
	rclone       *RcloneManager
	engine       *engine.Engine
	webdavServer *http.Server
	syncDir      *vfs.SyncDir
}

// New creates a new daemon with the given configuration.
func New(cfg Config) *Daemon {
	return &Daemon{config: cfg}
}

// Start launches all daemon components.
func (d *Daemon) Start(ctx context.Context) error {
	// Ensure config directory exists.
	if err := os.MkdirAll(d.config.ConfigDir, 0700); err != nil {
		return fmt.Errorf("creating config directory: %w", err)
	}

	// Open metadata database.
	dbPath := filepath.Join(d.config.ConfigDir, "metadata.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		return fmt.Errorf("opening metadata db: %w", err)
	}
	d.db = db
	slog.Info("metadata database opened", "path", dbPath)

	// Start rclone RC — use existing rclone config if available.
	rcloneConf := filepath.Join(d.config.ConfigDir, "rclone.conf")
	if _, err := os.Stat(rcloneConf); os.IsNotExist(err) {
		// Fall back to default rclone config location.
		if home, _ := os.UserHomeDir(); home != "" {
			defaultConf := filepath.Join(home, ".config", "rclone", "rclone.conf")
			if _, err := os.Stat(defaultConf); err == nil {
				rcloneConf = defaultConf
			}
		}
	}
	d.rclone = NewRcloneManager(d.config.RcloneBin, rcloneConf, d.config.RcloneAddr)
	if err := d.rclone.Start(ctx); err != nil {
		d.db.Close()
		return fmt.Errorf("starting rclone: %w", err)
	}

	// If a password was given without a local salt, try to fetch the salt from
	// cloud so the same key is derived as on the original machine.
	if len(d.config.EncKey) == 0 && d.config.Password != "" {
		if err := d.resolveCloudSalt(); err != nil {
			d.rclone.Stop()
			d.db.Close()
			return fmt.Errorf("resolving encryption salt: %w", err)
		}
	}

	// If local DB is empty and restore is not disabled, try to restore from a cloud backup.
	var fileCount int
	if err := d.db.Conn().QueryRow("SELECT COUNT(*) FROM files").Scan(&fileCount); err != nil {
		slog.Warn("could not count files", "error", err)
	}
	var provCount int
	if err := d.db.Conn().QueryRow("SELECT COUNT(*) FROM providers").Scan(&provCount); err != nil {
		slog.Warn("could not count providers", "error", err)
	}
	if !d.config.SkipRestore && fileCount == 0 && provCount == 0 {
		if restored := d.tryRestoreDB(dbPath); restored {
			// Reopen the DB after restore.
			d.db.Close()
			db, err = metadata.Open(dbPath)
			if err != nil {
				return fmt.Errorf("reopening restored DB: %w", err)
			}
			d.db = db
			// Validate: if the restored DB has chunk records but no matching
			// cloud chunks exist, discard the restore to avoid ghost files.
			if !d.validateRestoredDB() {
				slog.Warn("restored DB failed cloud validation — discarding and starting fresh")
				d.db.Close()
				if err := os.Remove(dbPath); err == nil {
					db, err = metadata.Open(dbPath)
					if err != nil {
						return fmt.Errorf("reopening DB after discard: %w", err)
					}
					d.db = db
				}
			}
		}
	}

	// Sync rclone remotes → providers table. This ensures the broker has
	// providers to assign chunks to, and keeps quotas up to date.
	d.syncProviders()

	// After a DB restore, check whether we're missing cloud accounts that the
	// original machine had. Warn the user so they can add them.
	missing := d.checkMissingProviders()
	if len(missing) > 0 {
		slog.Warn("some files may be inaccessible until the missing providers are configured")
	}

	// Create persistent spool directory for in-progress upload temp files so
	// they survive a daemon restart and can be resumed automatically.
	spoolDir := filepath.Join(d.config.ConfigDir, "spool")
	if err := os.MkdirAll(spoolDir, 0700); err != nil {
		slog.Warn("could not create spool directory, falling back to os.TempDir", "error", err)
		spoolDir = ""
	}

	// Create engine.
	b := broker.NewBroker(d.db, broker.Policy(d.config.BrokerPolicy), d.config.MinFreeSpace)
	if d.config.RatePerSec > 0 {
		d.engine = engine.NewEngineWithRate(d.db, dbPath, d.rclone.Client(), b, d.config.EncKey, d.config.RatePerSec)
	} else {
		d.engine = engine.NewEngine(d.db, dbPath, d.rclone.Client(), b, d.config.EncKey)
	}
	if d.config.ChunkSize > 0 {
		d.engine.SetChunkSize(d.config.ChunkSize)
	}
	// If password-derived encryption is in use, tell the engine where the salt
	// lives so it can upload it alongside the encrypted DB backup.
	saltPath := filepath.Join(d.config.ConfigDir, "enc.salt")
	if _, err := os.Stat(saltPath); err == nil {
		d.engine.SetSaltPath(saltPath)
	}

	// Start local sync dir if configured.
	if d.config.SyncDir != "" {
		if err := os.MkdirAll(d.config.SyncDir, 0755); err != nil {
			return fmt.Errorf("creating sync directory: %w", err)
		}
		d.syncDir = vfs.NewSyncDir(d.config.SyncDir, d.engine, spoolDir)
		if err := d.syncDir.Start(ctx); err != nil {
			return fmt.Errorf("starting sync dir: %w", err)
		}
	}

	// Start HTTP server — includes WebDAV + status/upload APIs.
	davFS := vfs.NewWebDAVFS(d.engine, spoolDir)
	handler := &webdav.Handler{
		FileSystem: davFS,
		LockSystem: webdav.NewMemLS(),
		Logger: func(r *http.Request, err error) {
			if err != nil {
				slog.Debug("webdav request", "method", r.Method, "path", r.URL.Path, "error", err)
			}
		},
	}

	d.webdavServer = &http.Server{
		Addr: d.config.WebDAVAddr,
		Handler: &browserHandler{
			davHandler:    handler,
			engine:        d.engine,
			syncDir:       d.syncDir,
			startTime:     time.Now(),
			configDir:     d.config.ConfigDir,
			rcloneClient:  d.rclone.Client(),
			activeRemotes: d.config.Remotes,
		},
	}

	go func() {
		slog.Info("HTTP server starting", "addr", d.config.WebDAVAddr)
		if err := d.webdavServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
		}
	}()

	slog.Info("pdrive daemon started",
		"configDir", d.config.ConfigDir,
		"syncDir", d.config.SyncDir,
		"http", d.config.WebDAVAddr,
		"rclone", d.config.RcloneAddr,
	)

	// Resume any uploads interrupted by a prior daemon restart.
	go d.engine.ResumeUploads()

	// Run orphan GC: first pass after 60s (let any in-progress uploads settle),
	// then every 24h. Runs entirely in the background.
	go func() {
		timer := time.NewTimer(60 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		d.engine.GCOrphanedChunks()
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.engine.GCOrphanedChunks()
			}
		}
	}()

	// Retry failed chunk deletions every hour.
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.engine.RetryFailedDeletions()
			}
		}
	}()

	return nil
}

// Stop gracefully shuts down all daemon components.
func (d *Daemon) Stop() {
	slog.Info("pdrive daemon shutting down")

	if d.syncDir != nil {
		d.syncDir.Stop()
	}
	if d.webdavServer != nil {
		d.webdavServer.Close()
	}
	if d.engine != nil {
		d.engine.FlushBackup()
		d.engine.Close()
	}
	if d.rclone != nil {
		d.rclone.Stop()
	}
	if d.db != nil {
		d.db.Close()
	}

	slog.Info("pdrive daemon stopped")
}

// Engine returns the daemon's engine (useful for testing).
func (d *Daemon) Engine() *engine.Engine {
	return d.engine
}

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

const saltRemotePath = "pdrive-meta/enc.salt"

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

// syncProviders discovers rclone remotes and registers/updates them as providers
// in the metadata database. This is the bridge between "rclone config" and pdrive's
// provider-aware broker. Called on every startup to keep the DB in sync.
func (d *Daemon) syncProviders() {
	remotes, err := d.rclone.Client().ListRemotes()
	if err != nil {
		slog.Warn("could not list rclone remotes for provider sync", "error", err)
		return
	}
	if len(remotes) == 0 {
		slog.Warn("no rclone remotes configured — uploads will fail until a remote is added")
		return
	}

	// If the user specified --remotes, use that. Otherwise load remotes.json.
	allowedRemotes := d.config.Remotes
	if len(allowedRemotes) == 0 {
		remotesFile := filepath.Join(d.config.ConfigDir, "remotes.json")
		if data, err := os.ReadFile(remotesFile); err == nil {
			var saved []string
			if err := json.Unmarshal(data, &saved); err == nil && len(saved) > 0 {
				allowedRemotes = saved
				slog.Info("loaded remote selection from remotes.json", "remotes", saved)
			}
		}
	}

	if len(allowedRemotes) > 0 {
		allowed := make(map[string]bool, len(allowedRemotes))
		for _, r := range allowedRemotes {
			allowed[r] = true
		}
		filtered := make([]string, 0, len(remotes))
		for _, r := range remotes {
			if allowed[r] {
				filtered = append(filtered, r)
			} else {
				slog.Debug("skipping remote (not in selection)", "remote", r)
			}
		}
		remotes = filtered
		if len(remotes) == 0 {
			slog.Warn("none of the selected remotes match configured rclone remotes")
			return
		}
	}

	now := time.Now().Unix()
	for _, remote := range remotes {
		remoteType, err := d.rclone.Client().GetRemoteType(remote)
		if err != nil {
			slog.Debug("could not get remote type", "remote", remote, "error", err)
			remoteType = "unknown"
		}

		// Check if this remote is already registered (possibly from a restored DB).
		existing, _ := d.db.GetProviderByRemote(remote)

		providerID := remote // use remote name as stable ID
		if existing != nil {
			providerID = existing.ID
		}

		// Fetch quota from cloud.
		var quotaTotal, quotaFree *int64
		var polledAt *int64
		aboutResult, err := d.rclone.Client().About(remote)
		if err != nil {
			slog.Debug("could not fetch quota", "remote", remote, "error", err)
			// Keep existing quota values if available.
			if existing != nil {
				quotaTotal = existing.QuotaTotalBytes
				quotaFree = existing.QuotaFreeBytes
				polledAt = existing.QuotaPolledAt
			}
		} else {
			quotaTotal = &aboutResult.Total
			quotaFree = &aboutResult.Free
			polledAt = &now
		}

		p := &metadata.Provider{
			ID:              providerID,
			Type:            remoteType,
			DisplayName:     remote,
			RcloneRemote:    remote,
			QuotaTotalBytes: quotaTotal,
			QuotaFreeBytes:  quotaFree,
			QuotaPolledAt:   polledAt,
		}
		// Preserve rate-limit state from existing record.
		if existing != nil {
			p.RateLimitedUntil = existing.RateLimitedUntil
		}

		// Fetch account identity (email/username). The About() call above
		// forces rclone to refresh any expired OAuth tokens, so the
		// config/get call inside FetchAccountIdentity gets a fresh token.
		if existing != nil && existing.AccountIdentity != "" {
			p.AccountIdentity = existing.AccountIdentity
		} else {
			identity, err := d.rclone.Client().FetchAccountIdentity(remote)
			if err != nil {
				slog.Debug("could not fetch account identity", "remote", remote, "error", err)
			}
			p.AccountIdentity = identity
		}

		if err := d.db.UpsertProvider(p); err != nil {
			slog.Warn("failed to register provider", "remote", remote, "error", err)
			continue
		}
		slog.Info("provider synced", "remote", remote, "type", remoteType)
	}
}

// checkMissingProviders compares the providers in the restored DB with the
// currently available rclone remotes. If any DB providers are missing (the user
// has not yet configured those remotes on this machine), it logs warnings so the
// user knows which accounts to add.
// Returns the list of missing provider remote names.
func (d *Daemon) checkMissingProviders() []string {
	dbProviders, err := d.db.GetAllProviders()
	if err != nil || len(dbProviders) == 0 {
		return nil
	}

	remotes, err := d.rclone.Client().ListRemotes()
	if err != nil {
		slog.Warn("could not list remotes for missing-provider check", "error", err)
		return nil
	}

	available := make(map[string]bool, len(remotes))
	for _, r := range remotes {
		available[r] = true
	}

	var missing []string
	for _, p := range dbProviders {
		if !available[p.RcloneRemote] {
			label := p.RcloneRemote
			if p.AccountIdentity != "" {
				label += " (" + p.AccountIdentity + ", " + p.Type + ")"
			} else if p.Type != "" {
				label += " (" + p.Type + ")"
			}
			missing = append(missing, label)
		}
	}

	if len(missing) > 0 {
		slog.Warn("restored metadata references cloud providers not configured on this machine",
			"missing", missing,
			"total_db_providers", len(dbProviders),
			"available_remotes", len(remotes),
		)
		slog.Warn(fmt.Sprintf("pdrive needs %d additional cloud account(s) to access all your files: %s",
			len(missing), strings.Join(missing, ", ")))
		slog.Warn("add the missing remotes with: rclone config create <name> <type>")
	}

	return missing
}

// browserHandler wraps the WebDAV handler to serve HTML directory listings
// for browser GET requests, while passing WebDAV methods through normally.
type browserHandler struct {
	davHandler   http.Handler
	engine       *engine.Engine
	syncDir      *vfs.SyncDir // may be nil if sync is disabled
	startTime    time.Time
	configDir    string
	rcloneClient interface {
		ListRemotes() ([]string, error)
		GetRemoteType(string) (string, error)
	}
	activeRemotes []string // from --remotes flag; empty = all
}

func (h *browserHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/uploads":
		ups := h.engine.UploadProgress()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ups) //nolint:errcheck
		return
	case "/api/ls":
		h.serveAPILs(w, r)
		return
	case "/api/status":
		h.serveAPIStatus(w, r)
		return
	case "/api/remotes":
		h.serveAPIRemotes(w, r)
		return
	case "/api/pin":
		h.serveAPIPin(w, r)
		return
	case "/api/unpin":
		h.serveAPIUnpin(w, r)
		return
	case "/api/health":
		h.serveAPIHealth(w, r)
		return
	case "/api/metrics":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(h.engine.Metrics()) //nolint:errcheck
		return
	case "/api/download":
		h.serveAPIDownload(w, r)
		return
	case "/api/delete":
		h.serveAPIDelete(w, r)
		return
	case "/api/tree":
		h.serveAPITree(w, r)
		return
	case "/api/find":
		h.serveAPIFind(w, r)
		return
	case "/api/mv":
		h.serveAPIMv(w, r)
		return
	case "/api/mkdir":
		h.serveAPIMkdir(w, r)
		return
	case "/api/info":
		h.serveAPIInfo(w, r)
		return
	case "/api/du":
		h.serveAPIDu(w, r)
		return
	}

	// Only intercept GET/HEAD with a browser-like Accept header.
	if (r.Method == "GET" || r.Method == "HEAD") && strings.Contains(r.Header.Get("Accept"), "text/html") {
		h.serveBrowser(w, r)
		return
	}
	if h.davHandler == nil {
		http.Error(w, "WebDAV not configured", http.StatusInternalServerError)
		return
	}
	h.davHandler.ServeHTTP(w, r)
}

func (h *browserHandler) serveAPILs(w http.ResponseWriter, r *http.Request) {
	p := path.Clean(r.URL.Query().Get("path"))
	if p == "" || p == "." {
		p = "/"
	}
	dirPath := p
	if dirPath != "/" {
		dirPath += "/"
	}
	files, dirs, err := h.engine.ListDir(dirPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := lsResponse{
		Path:  p,
		Dirs:  make([]string, 0, len(dirs)),
		Files: make([]lsFile, 0, len(files)),
	}
	resp.Dirs = append(resp.Dirs, dirs...)
	for _, f := range files {
		state := "local"
		if h.syncDir != nil && h.syncDir.IsStub(f.VirtualPath) {
			state = "stub"
		}
		resp.Files = append(resp.Files, lsFile{
			Name:       path.Base(f.VirtualPath),
			Path:       f.VirtualPath,
			Size:       f.SizeBytes,
			ModifiedAt: f.ModifiedAt,
			LocalState: state,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp) //nolint:errcheck
}

func (h *browserHandler) serveAPIStatus(w http.ResponseWriter, r *http.Request) {
	st, err := h.engine.StorageStatus()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := statusResponse{
		TotalFiles: st.TotalFiles,
		TotalBytes: st.TotalBytes,
		Providers:  make([]statusProvider, 0, len(st.Providers)),
	}
	for _, p := range st.Providers {
		usedBytes := st.ProviderBytes[p.ID]
		resp.Providers = append(resp.Providers, statusProvider{
			Name:              p.DisplayName,
			Type:              p.Type,
			AccountIdentity:   p.AccountIdentity,
			QuotaTotalBytes:   p.QuotaTotalBytes,
			QuotaFreeBytes:    p.QuotaFreeBytes,
			QuotaUsedByPdrive: &usedBytes,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp) //nolint:errcheck
}

func (h *browserHandler) serveAPIRemotes(w http.ResponseWriter, r *http.Request) {
	allRemotes, err := h.rcloneClient.ListRemotes()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Determine active set: from config + remotes.json.
	activeRemotes := h.activeRemotes
	if len(activeRemotes) == 0 {
		remotesFile := filepath.Join(h.configDir, "remotes.json")
		if data, err := os.ReadFile(remotesFile); err == nil {
			var saved []string
			if err := json.Unmarshal(data, &saved); err == nil && len(saved) > 0 {
				activeRemotes = saved
			}
		}
	}

	activeSet := make(map[string]bool, len(activeRemotes))
	if len(activeRemotes) > 0 {
		for _, r := range activeRemotes {
			activeSet[r] = true
		}
	}

	type remoteInfo struct {
		Name   string `json:"name"`
		Type   string `json:"type"`
		Active bool   `json:"active"`
	}

	result := make([]remoteInfo, 0, len(allRemotes))
	for _, name := range allRemotes {
		active := len(activeRemotes) == 0 || activeSet[name]
		remoteType, _ := h.rcloneClient.GetRemoteType(name)
		result = append(result, remoteInfo{
			Name:   name,
			Type:   remoteType,
			Active: active,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"remotes": result}) //nolint:errcheck
}

func (h *browserHandler) serveAPIPin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if h.syncDir == nil {
		http.Error(w, "sync dir not configured", http.StatusServiceUnavailable)
		return
	}
	p := path.Clean(r.URL.Query().Get("path"))
	if p == "" || p == "." || p == "/" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}
	if err := h.syncDir.PinFile(p); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","path":%q,"state":"local"}`, p)
}

func (h *browserHandler) serveAPIUnpin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if h.syncDir == nil {
		http.Error(w, "sync dir not configured", http.StatusServiceUnavailable)
		return
	}
	p := path.Clean(r.URL.Query().Get("path"))
	if p == "" || p == "." || p == "/" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}
	if err := h.syncDir.UnpinFile(p); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","path":%q,"state":"stub"}`, p)
}

func (h *browserHandler) serveAPIDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	p := path.Clean(r.URL.Query().Get("path"))
	if p == "" || p == "." || p == "/" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}

	// Check if it's a directory first.
	isDir, err := h.engine.IsDir(p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if isDir {
		dirPath := p
		if !strings.HasSuffix(dirPath, "/") {
			dirPath += "/"
		}
		if err := h.engine.DeleteDir(dirPath); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if h.syncDir != nil {
			os.RemoveAll(filepath.Join(h.syncDir.Root(), p))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","path":%q,"type":"dir"}`, p)
		return
	}

	// Single file.
	exists, err := h.engine.FileExists(p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "file not found: "+p, http.StatusNotFound)
		return
	}
	if err := h.engine.DeleteFile(p); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if h.syncDir != nil {
		os.Remove(filepath.Join(h.syncDir.Root(), p))
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","path":%q,"type":"file"}`, p)
}

func (h *browserHandler) serveAPITree(w http.ResponseWriter, r *http.Request) {
	p := path.Clean(r.URL.Query().Get("path"))
	if p == "" || p == "." {
		p = "/"
	}
	files, err := h.engine.ListAllFiles(p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type treeEntry struct {
		Path string `json:"path"`
		Size int64  `json:"size"`
	}
	entries := make([]treeEntry, 0, len(files))
	for _, f := range files {
		entries = append(entries, treeEntry{Path: f.VirtualPath, Size: f.SizeBytes})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries) //nolint:errcheck
}

func (h *browserHandler) serveAPIFind(w http.ResponseWriter, r *http.Request) {
	root := path.Clean(r.URL.Query().Get("path"))
	if root == "" || root == "." {
		root = "/"
	}
	pattern := r.URL.Query().Get("pattern")
	if pattern == "" {
		http.Error(w, "pattern required", http.StatusBadRequest)
		return
	}
	files, err := h.engine.SearchFiles(root, pattern)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type findEntry struct {
		Path string `json:"path"`
		Size int64  `json:"size"`
	}
	entries := make([]findEntry, 0, len(files))
	for _, f := range files {
		entries = append(entries, findEntry{Path: f.VirtualPath, Size: f.SizeBytes})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries) //nolint:errcheck
}

func (h *browserHandler) serveAPIMv(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	src := path.Clean(r.URL.Query().Get("src"))
	dst := path.Clean(r.URL.Query().Get("dst"))
	if src == "" || src == "." || dst == "" || dst == "." {
		http.Error(w, "src and dst required", http.StatusBadRequest)
		return
	}

	// Check if source is a directory.
	isDir, err := h.engine.IsDir(src)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if isDir {
		srcDir := src
		if !strings.HasSuffix(srcDir, "/") {
			srcDir += "/"
		}
		dstDir := dst
		if !strings.HasSuffix(dstDir, "/") {
			dstDir += "/"
		}
		if err := h.engine.RenameDir(srcDir, dstDir); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if h.syncDir != nil {
			oldLocal := filepath.Join(h.syncDir.Root(), src)
			newLocal := filepath.Join(h.syncDir.Root(), dst)
			os.MkdirAll(filepath.Dir(newLocal), 0755)
			os.Rename(oldLocal, newLocal)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","src":%q,"dst":%q,"type":"dir"}`, src, dst)
		return
	}

	// Single file.
	exists, err := h.engine.FileExists(src)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "file not found: "+src, http.StatusNotFound)
		return
	}
	if err := h.engine.RenameFile(src, dst); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if h.syncDir != nil {
		oldLocal := filepath.Join(h.syncDir.Root(), src)
		newLocal := filepath.Join(h.syncDir.Root(), dst)
		os.MkdirAll(filepath.Dir(newLocal), 0755)
		os.Rename(oldLocal, newLocal)
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","src":%q,"dst":%q,"type":"file"}`, src, dst)
}

func (h *browserHandler) serveAPIMkdir(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	p := path.Clean(r.URL.Query().Get("path"))
	if p == "" || p == "." || p == "/" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}
	dirPath := p
	if !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}
	if err := h.engine.MkDir(dirPath); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if h.syncDir != nil {
		os.MkdirAll(filepath.Join(h.syncDir.Root(), p), 0755)
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","path":%q}`, p)
}

func (h *browserHandler) serveAPIInfo(w http.ResponseWriter, r *http.Request) {
	p := path.Clean(r.URL.Query().Get("path"))
	if p == "" || p == "." {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}
	info, err := h.engine.GetFileInfo(p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if info == nil {
		http.Error(w, "file not found: "+p, http.StatusNotFound)
		return
	}
	type chunkJSON struct {
		Sequence      int      `json:"sequence"`
		SizeBytes     int      `json:"size_bytes"`
		EncryptedSize int      `json:"encrypted_size"`
		Providers     []string `json:"providers"`
	}
	type infoJSON struct {
		Path        string      `json:"path"`
		SizeBytes   int64       `json:"size_bytes"`
		CreatedAt   int64       `json:"created_at"`
		ModifiedAt  int64       `json:"modified_at"`
		SHA256      string      `json:"sha256"`
		UploadState string      `json:"upload_state"`
		Chunks      []chunkJSON `json:"chunks"`
	}
	resp := infoJSON{
		Path:        info.File.VirtualPath,
		SizeBytes:   info.File.SizeBytes,
		CreatedAt:   info.File.CreatedAt,
		ModifiedAt:  info.File.ModifiedAt,
		SHA256:      info.File.SHA256Full,
		UploadState: info.File.UploadState,
		Chunks:      make([]chunkJSON, 0, len(info.Chunks)),
	}
	for _, c := range info.Chunks {
		resp.Chunks = append(resp.Chunks, chunkJSON{
			Sequence:      c.Sequence,
			SizeBytes:     c.SizeBytes,
			EncryptedSize: c.EncryptedSize,
			Providers:     c.Providers,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp) //nolint:errcheck
}

func (h *browserHandler) serveAPIDu(w http.ResponseWriter, r *http.Request) {
	p := path.Clean(r.URL.Query().Get("path"))
	if p == "" || p == "." {
		p = "/"
	}
	count, total, err := h.engine.DiskUsage(p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"path":%q,"file_count":%d,"total_bytes":%d}`, p, count, total)
}

func (h *browserHandler) serveAPIHealth(w http.ResponseWriter, _ *http.Request) {
	dbOK := true
	if err := h.engine.DB().Conn().Ping(); err != nil {
		dbOK = false
	}

	inFlight := len(h.engine.UploadProgress())

	status := "ok"
	if !dbOK {
		status = "degraded"
	}

	resp := struct {
		Status          string  `json:"status"`
		UptimeSeconds   float64 `json:"uptime_seconds"`
		InFlightUploads int     `json:"in_flight_uploads"`
		DBOK            bool    `json:"db_ok"`
	}{
		Status:          status,
		UptimeSeconds:   time.Since(h.startTime).Seconds(),
		InFlightUploads: inFlight,
		DBOK:            dbOK,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp) //nolint:errcheck
}

func (h *browserHandler) serveAPIDownload(w http.ResponseWriter, r *http.Request) {
	p := path.Clean(r.URL.Query().Get("path"))
	if p == "" || p == "." || p == "/" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}

	file, err := h.engine.Stat(p)
	if err != nil || file == nil {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}

	tmp, err := h.engine.ReadFileToTempFile(p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()

	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, path.Base(p)))
	http.ServeContent(w, r, path.Base(p), time.Unix(file.ModifiedAt, 0), tmp)
}

func (h *browserHandler) serveBrowser(w http.ResponseWriter, r *http.Request) {
	p := path.Clean(r.URL.Path)

	// Check if it's a file — stream content from a temp file to avoid
	// holding the entire file in memory.
	file, err := h.engine.Stat(p)
	if err == nil && file != nil {
		tmp, err := h.engine.ReadFileToTempFile(p)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			tmp.Close()
			os.Remove(tmp.Name())
		}()
		http.ServeContent(w, r, path.Base(p), time.Unix(file.ModifiedAt, 0), tmp)
		return
	}

	// Otherwise serve the SPA shell — JS handles listing and navigation.
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, browserUIHTML)
}
