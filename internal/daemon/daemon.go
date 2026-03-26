package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/vfs"
	"golang.org/x/net/webdav"
)

// Config holds daemon configuration.
type Config struct {
	ConfigDir  string // ~/.pdrive/
	RcloneBin  string // path to rclone binary
	RcloneAddr string // e.g., "localhost:5572"
	WebDAVAddr string // e.g., "localhost:8765"
	EncKey     []byte // 32-byte AES-256 key
}

// Daemon is the main pdrive daemon that ties everything together.
type Daemon struct {
	config  Config
	db      *metadata.DB
	rclone  *RcloneManager
	engine  *engine.Engine
	webdavServer *http.Server
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

	// Start rclone RC.
	rcloneConf := filepath.Join(d.config.ConfigDir, "rclone.conf")
	d.rclone = NewRcloneManager(d.config.RcloneBin, rcloneConf, d.config.RcloneAddr)
	if err := d.rclone.Start(ctx); err != nil {
		d.db.Close()
		return fmt.Errorf("starting rclone: %w", err)
	}

	// Create engine.
	b := broker.NewBroker(d.db)
	d.engine = engine.NewEngine(d.db, d.rclone.Client(), b, d.config.EncKey)

	// Start WebDAV server.
	davFS := vfs.NewWebDAVFS(d.engine)
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
		Addr:    d.config.WebDAVAddr,
		Handler: handler,
	}

	go func() {
		slog.Info("WebDAV server starting", "addr", d.config.WebDAVAddr)
		if err := d.webdavServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("WebDAV server error", "error", err)
		}
	}()

	slog.Info("pdrive daemon started",
		"configDir", d.config.ConfigDir,
		"webdav", d.config.WebDAVAddr,
		"rclone", d.config.RcloneAddr,
	)
	return nil
}

// Stop gracefully shuts down all daemon components.
func (d *Daemon) Stop() {
	slog.Info("pdrive daemon shutting down")

	if d.webdavServer != nil {
		d.webdavServer.Close()
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
