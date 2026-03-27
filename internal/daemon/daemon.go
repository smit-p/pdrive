package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/vfs"
	"golang.org/x/net/webdav"
)

// Config holds daemon configuration.
type Config struct {
	ConfigDir    string // ~/.pdrive/
	RcloneBin    string // path to rclone binary
	RcloneAddr   string // e.g., "localhost:5572"
	WebDAVAddr   string // e.g., "localhost:8765"
	EncKey       []byte // 32-byte AES-256 key
	BrokerPolicy string // "pfrd" or "mfs"
	MinFreeSpace int64  // bytes to keep free on each provider
	SkipRestore  bool   // skip cloud DB restore on startup (useful after a manual wipe)
}

// Daemon is the main pdrive daemon that ties everything together.
type Daemon struct {
	config       Config
	db           *metadata.DB
	rclone       *RcloneManager
	engine       *engine.Engine
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

	// If local DB is empty and restore is not disabled, try to restore from a cloud backup.
	var fileCount int
	d.db.Conn().QueryRow("SELECT COUNT(*) FROM files").Scan(&fileCount)
	var provCount int
	d.db.Conn().QueryRow("SELECT COUNT(*) FROM providers").Scan(&provCount)
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
					db, _ = metadata.Open(dbPath)
					d.db = db
				}
			}
		}
	}

	// Create engine.
	b := broker.NewBroker(d.db, broker.Policy(d.config.BrokerPolicy), d.config.MinFreeSpace)
	d.engine = engine.NewEngine(d.db, dbPath, d.rclone.Client(), b, d.config.EncKey)

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
		Handler: &browserHandler{davHandler: handler, engine: d.engine},
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

	// Resume any uploads interrupted by a prior daemon restart.
	go d.engine.ResumeUploads()

	// Run orphan GC: first pass after 60s (let any in-progress uploads settle),
	// then every 24h. Runs entirely in the background.
	go func() {
		time.Sleep(60 * time.Second)
		d.engine.GCOrphanedChunks()
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			d.engine.GCOrphanedChunks()
		}
	}()

	return nil
}

// Stop gracefully shuts down all daemon components.
func (d *Daemon) Stop() {
	slog.Info("pdrive daemon shutting down")

	if d.webdavServer != nil {
		d.webdavServer.Close()
	}
	if d.engine != nil {
		d.engine.FlushBackup()
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

// tryRestoreDB attempts to download a metadata DB backup from any configured rclone remote.
// Returns true if a backup was found and restored.
func (d *Daemon) tryRestoreDB(dbPath string) bool {
	remotes, err := d.rclone.Client().ListRemotes()
	if err != nil {
		slog.Debug("could not list rclone remotes for DB restore", "error", err)
		return false
	}
	for _, remote := range remotes {
		data, err := d.rclone.Client().GetFile(remote, "pdrive-meta/metadata.db")
		if err != nil || len(data) == 0 {
			continue
		}
		if err := os.WriteFile(dbPath, data, 0600); err != nil {
			slog.Warn("failed to write restored DB", "error", err)
			continue
		}
		slog.Info("metadata DB restored from cloud", "remote", remote, "size", len(data))
		return true
	}
	return false
}

// browserHandler wraps the WebDAV handler to serve HTML directory listings
// for browser GET requests, while passing WebDAV methods through normally.
type browserHandler struct {
	davHandler http.Handler
	engine     *engine.Engine
}

func (h *browserHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// JSON API for upload progress (polled by the browser UI).
	if r.URL.Path == "/api/uploads" {
		ups := h.engine.UploadProgress()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ups) //nolint:errcheck
		return
	}

	// Only intercept GET/HEAD with a browser-like Accept header.
	if (r.Method == "GET" || r.Method == "HEAD") && strings.Contains(r.Header.Get("Accept"), "text/html") {
		h.serveBrowser(w, r)
		return
	}
	h.davHandler.ServeHTTP(w, r)
}

func (h *browserHandler) serveBrowser(w http.ResponseWriter, r *http.Request) {
	p := path.Clean(r.URL.Path)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}

	// Check if it's a file — serve the raw content.
	file, err := h.engine.Stat(p)
	if err == nil && file != nil {
		data, err := h.engine.ReadFile(p)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.ServeContent(w, r, path.Base(p), time.Unix(file.ModifiedAt, 0), strings.NewReader(string(data)))
		return
	}

	// Otherwise treat as directory listing.
	dirPath := p
	if dirPath != "/" && !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}

	files, dirs, err := h.engine.ListDir(dirPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if p != "/" {
		isDir, _ := h.engine.IsDir(dirPath)
		if !isDir && len(files) == 0 && len(dirs) == 0 {
			http.NotFound(w, r)
			return
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>pdrive — %s</title>
<style>
body { font-family: -apple-system, system-ui, sans-serif; max-width: 700px; margin: 40px auto; padding: 0 20px; color: #333; }
h1 { font-size: 1.4em; border-bottom: 1px solid #ddd; padding-bottom: 8px; }
a { color: #0366d6; text-decoration: none; }
a:hover { text-decoration: underline; }
table { width: 100%%; border-collapse: collapse; }
td { padding: 6px 12px 6px 0; border-bottom: 1px solid #eee; }
td.size { text-align: right; color: #666; font-variant-numeric: tabular-nums; }
.dir { font-weight: 500; }
.empty { color: #999; font-style: italic; padding: 20px 0; }
#uploads { margin-bottom: 24px; }
#uploads h2 { font-size: 1.1em; color: #555; margin: 0 0 10px; }
.upload-item { margin-bottom: 10px; }
.upload-name { font-size: 0.9em; margin-bottom: 3px; word-break: break-all; }
.upload-name.failed { color: #c0392b; }
.bar-bg { background: #eee; border-radius: 4px; height: 10px; width: 100%%; overflow: hidden; }
.bar-fg { background: #2ecc71; height: 10px; border-radius: 4px; transition: width 0.4s; }
.bar-fg.failed { background: #e74c3c; }
.pct { font-size: 0.8em; color: #666; margin-top: 2px; }
</style>
<script>
function fmtSize(b){if(b>=1073741824)return(b/1073741824).toFixed(1)+' GB';if(b>=1048576)return(b/1048576).toFixed(1)+' MB';if(b>=1024)return(b/1024).toFixed(1)+' KB';return b+' B';}
function refreshUploads(){
  fetch('/api/uploads').then(r=>r.json()).then(ups=>{
    var div=document.getElementById('uploads');
    if(!ups||ups.length===0){div.innerHTML='';return;}
    var html='<h2>⬆ Uploading…</h2>';
    ups.forEach(function(u){
      var pct=u.TotalChunks>0?Math.round(u.ChunksUploaded/u.TotalChunks*100):0;
      if(pct>100)pct=100;
      var failed=u.Failed;
      html+='<div class="upload-item">';
      html+='<div class="upload-name'+(failed?' failed':'')+'">'+u.VirtualPath+(failed?' ✗ failed':'')+'</div>';
      html+='<div class="bar-bg"><div class="bar-fg'+(failed?' failed':'')+'" style="width:'+pct+'%%"></div></div>';
      html+='<div class="pct">'+pct+'%%  ('+u.ChunksUploaded+'/'+u.TotalChunks+' chunks, '+fmtSize(u.SizeBytes)+')</div>';
      html+='</div>';
    });
    div.innerHTML=html;
  }).catch(function(){});
}
refreshUploads();
setInterval(refreshUploads,2000);
</script>
</head><body>
<div id="uploads"></div>
<h1>📁 %s</h1>`, html.EscapeString(p), html.EscapeString(p))

	if p != "/" {
		parent := path.Dir(strings.TrimSuffix(p, "/"))
		if parent == "" {
			parent = "/"
		}
		fmt.Fprintf(w, `<table><tr><td class="dir"><a href="%s">⬆ ..</a></td><td></td></tr>`, html.EscapeString(parent))
	} else {
		fmt.Fprint(w, `<table>`)
	}

	for _, d := range dirs {
		link := path.Join(p, d) + "/"
		fmt.Fprintf(w, `<tr><td class="dir"><a href="%s">📁 %s/</a></td><td class="size">—</td></tr>`,
			html.EscapeString(link), html.EscapeString(d))
	}
	for _, f := range files {
		name := path.Base(f.VirtualPath)
		link := path.Join(p, name)
		fmt.Fprintf(w, `<tr><td><a href="%s">📄 %s</a></td><td class="size">%s</td></tr>`,
			html.EscapeString(link), html.EscapeString(name), formatSize(f.SizeBytes))
	}

	if len(dirs) == 0 && len(files) == 0 {
		fmt.Fprint(w, `<tr><td colspan="2" class="empty">This directory is empty</td></tr>`)
	}

	fmt.Fprint(w, `</table></body></html>`)
}

func formatSize(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
