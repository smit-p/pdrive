// Package daemon ties all pdrive subsystems together into a single long-running
// process: it starts the rclone RC child process, opens the metadata DB,
// creates the engine, launches the WebDAV + HTTP API server, watches the
// local sync directory, and runs periodic background tasks (orphan GC,
// failed-deletion retry, metadata backup).
//
// The daemon is started by the CLI (cmd/pdrive) and can be stopped via
// "pdrive stop" (SIGTERM to the PID file).
package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/fusefs"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/vfs"
	"golang.org/x/net/webdav"
)

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
	MountBackend string   // "webdav" (default) or "fuse"
	MountPoint   string   // FUSE mount point path (e.g. /Volumes/pdrive)
}

// Daemon is the main pdrive daemon that ties everything together.
type Daemon struct {
	config       Config
	db           *metadata.DB
	rclone       *RcloneManager
	engine       *engine.Engine
	webdavServer *http.Server
	syncDir      *vfs.SyncDir
	fuseServer   *fusefs.Server // non-nil when MountBackend == "fuse"
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

	// Purge OS-generated junk files (e.g. .DS_Store, ._* resource forks)
	// that may have been synced before the skip filter was in place.
	d.purgeJunkFiles()

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

	// Start FUSE mount if backend is "fuse".
	if d.config.MountBackend == "fuse" {
		mp := d.config.MountPoint
		if mp == "" {
			// Default mount point: ~/pdrive
			if home, err := os.UserHomeDir(); err == nil {
				mp = filepath.Join(home, "pdrive")
				slog.Info("no --mountpoint specified, using default", "mountpoint", mp)
			} else {
				return fmt.Errorf("--mountpoint is required (could not determine home directory)")
			}
		}
		srv, err := fusefs.Mount(mp, d.engine, spoolDir)
		if err != nil {
			return fmt.Errorf("mounting FUSE filesystem: %w", err)
		}
		d.fuseServer = srv
	}

	// Start HTTP server — includes WebDAV + status/upload APIs.
	// The HTTP API server always runs (for CLI commands), but WebDAV
	// file operations only serve when backend is "webdav" (default).
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
		Addr:              d.config.WebDAVAddr,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
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

	backend := d.config.MountBackend
	if backend == "" {
		backend = "webdav"
	}
	slog.Info("pdrive daemon started",
		"configDir", d.config.ConfigDir,
		"syncDir", d.config.SyncDir,
		"backend", backend,
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
	if d.fuseServer != nil {
		if err := d.fuseServer.Unmount(); err != nil {
			slog.Warn("FUSE unmount error", "error", err)
		}
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
