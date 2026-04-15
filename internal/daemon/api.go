package daemon

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/junkfile"
	"github.com/smit-p/pdrive/internal/logutil"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/vfs"
)

//go:embed web/index.html web/styles.css web/app.js
var webFS embed.FS

// staticFS serves files from the embedded web/ directory at /static/.
var staticFS http.Handler

func init() {
	sub, _ := fs.Sub(webFS, "web")
	staticFS = http.StripPrefix("/static/", http.FileServer(http.FS(sub)))
}

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

// browserHandler wraps the WebDAV handler to serve HTML directory listings
// for browser GET requests, while passing WebDAV methods through normally.
type browserHandler struct {
	davHandler   http.Handler
	engine       *engine.Engine
	syncDir      *vfs.SyncDir // may be nil if sync is disabled
	startTime    time.Time
	configDir    string
	spoolDir     string // temp dir for in-progress upload spool files
	rcloneClient interface {
		ListRemotes() ([]string, error)
		GetRemoteType(string) (string, error)
	}
	activeRemotes   []string             // from --remotes flag; empty = all
	resyncProviders func()               // triggers immediate provider re-sync
	logHandler      *logutil.RingHandler // live log ring buffer (nil safe)
}

// cleanPath sanitises a user-supplied virtual path: it cleans ".." segments
// and ensures the result is absolute (rooted at "/").  This prevents
// path-traversal attacks where a relative path like "../../etc" would escape
// the sync directory when joined with the filesystem root.
func cleanPath(raw string) string {
	p := path.Clean("/" + raw)
	return p
}

// isJunkFile returns true for OS-generated hidden files that should never be
// stored (macOS .DS_Store, AppleDouble resource forks, Windows Thumbs.db, etc.).
func isJunkFile(virtualPath string) bool {
	return junkfile.IsOSJunk(path.Base(virtualPath))
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
	case "/api/upload":
		h.serveAPIUpload(w, r)
		return
	case "/api/upload/cancel":
		h.serveAPIUploadCancel(w, r)
		return
	case "/api/verify":
		h.serveAPIVerify(w, r)
		return
	case "/api/activity":
		h.serveAPIActivity(w, r)
		return
	case "/api/resync":
		if h.resyncProviders != nil {
			go h.resyncProviders()
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`)) //nolint:errcheck
		return
	case "/api/logs":
		if h.logHandler != nil {
			h.logHandler.ServeRecentLogs(w, r)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[]`)) //nolint:errcheck
		}
		return
	case "/api/logs/stream":
		if h.logHandler != nil {
			h.logHandler.ServeLogStream(w, r)
		} else {
			http.Error(w, "logging not initialized", http.StatusServiceUnavailable)
		}
		return
	}

	// Only intercept GET/HEAD with a browser-like Accept header.
	if (r.Method == "GET" || r.Method == "HEAD") && strings.Contains(r.Header.Get("Accept"), "text/html") {
		h.serveBrowser(w, r)
		return
	}

	// Serve embedded static assets (CSS, JS) with no-cache so updates
	// are picked up immediately after a binary rebuild.
	if strings.HasPrefix(r.URL.Path, "/static/") {
		w.Header().Set("Cache-Control", "no-cache, must-revalidate")
		staticFS.ServeHTTP(w, r)
		return
	}
	if h.davHandler == nil {
		http.Error(w, "WebDAV not configured", http.StatusInternalServerError)
		return
	}
	h.davHandler.ServeHTTP(w, r)
}

func (h *browserHandler) serveAPILs(w http.ResponseWriter, r *http.Request) {
	p := cleanPath(r.URL.Query().Get("path"))
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
		if isJunkFile(f.VirtualPath) {
			continue
		}
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
	p := cleanPath(r.URL.Query().Get("path"))
	if p == "/" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}
	if err := h.syncDir.PinFile(p); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.engine.DB().InsertActivity("pin", p, "") //nolint:errcheck
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
	p := cleanPath(r.URL.Query().Get("path"))
	if p == "/" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}
	if err := h.syncDir.UnpinFile(p); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.engine.DB().InsertActivity("unpin", p, "") //nolint:errcheck
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","path":%q,"state":"stub"}`, p)
}

func (h *browserHandler) serveAPIDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	p := cleanPath(r.URL.Query().Get("path"))
	if p == "/" {
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
		h.engine.DB().InsertActivity("delete", p, "directory") //nolint:errcheck
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
	h.engine.DB().InsertActivity("delete", p, "file") //nolint:errcheck
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","path":%q,"type":"file"}`, p)
}

func (h *browserHandler) serveAPITree(w http.ResponseWriter, r *http.Request) {
	p := cleanPath(r.URL.Query().Get("path"))
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
	root := cleanPath(r.URL.Query().Get("path"))
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
	src := cleanPath(r.URL.Query().Get("src"))
	dst := cleanPath(r.URL.Query().Get("dst"))
	if src == "/" || dst == "/" {
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
		h.engine.DB().InsertActivity("move", src, dst) //nolint:errcheck
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
	h.engine.DB().InsertActivity("move", src, dst) //nolint:errcheck
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","src":%q,"dst":%q,"type":"file"}`, src, dst)
}

func (h *browserHandler) serveAPIMkdir(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	p := cleanPath(r.URL.Query().Get("path"))
	if p == "/" {
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
	h.engine.DB().InsertActivity("mkdir", p, "") //nolint:errcheck
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","path":%q}`, p)
}

func (h *browserHandler) serveAPIInfo(w http.ResponseWriter, r *http.Request) {
	p := cleanPath(r.URL.Query().Get("path"))
	if p == "/" {
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
		Sequence  int      `json:"sequence"`
		SizeBytes int      `json:"size_bytes"`
		CloudSize int      `json:"cloud_size"`
		Providers []string `json:"providers"`
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
			Sequence:  c.Sequence,
			SizeBytes: c.SizeBytes,
			CloudSize: c.CloudSize,
			Providers: c.Providers,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp) //nolint:errcheck
}

func (h *browserHandler) serveAPIDu(w http.ResponseWriter, r *http.Request) {
	p := cleanPath(r.URL.Query().Get("path"))
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
	p := cleanPath(r.URL.Query().Get("path"))
	if p == "/" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}

	file, err := h.engine.Stat(p)
	if err != nil || file == nil {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}

	slog.Info("API download request", "path", p, "size", file.SizeBytes)

	// Set headers and flush immediately so the browser sees the download
	// start right away instead of timing out waiting for the first byte.
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, path.Base(p)))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(file.SizeBytes, 10))
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	if r.Method == "HEAD" {
		return
	}

	h.engine.DB().InsertActivity("download", p, "") //nolint:errcheck

	if err := h.engine.StreamFile(r.Context(), p, w); err != nil {
		// Headers already sent — can't return an HTTP error. Log it.
		slog.Error("API download stream failed", "path", p, "error", err)
		return
	}
}

func (h *browserHandler) serveAPIUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		http.Error(w, "invalid multipart form: "+err.Error(), http.StatusBadRequest)
		return
	}
	dir := cleanPath(r.FormValue("dir"))
	if dir == "" || dir == "." {
		dir = "/"
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file field required: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Sanitise the filename — strip any directory components to prevent
	// path injection via a crafted multipart Content-Disposition header.
	baseName := filepath.Base(header.Filename)
	if baseName == "." || baseName == "/" || baseName == "" {
		http.Error(w, "invalid filename", http.StatusBadRequest)
		return
	}
	virtualPath := path.Join(dir, baseName)

	if isJunkFile(virtualPath) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"skipped","path":%q,"reason":"OS-generated junk file"}`, virtualPath)
		return
	}

	// Ensure parent directories exist (for folder uploads).
	if dir != "/" {
		h.ensureParentDirs(dir)
	}

	// Spool the multipart data to a temp file so WriteFileAsync can take
	// ownership and upload chunks in the background. This lets the HTTP
	// request finish quickly and gives server-side progress tracking +
	// resume on daemon restart.
	spoolDir := h.spoolDir
	if spoolDir == "" {
		spoolDir = os.TempDir()
	}
	tmpFile, err := os.CreateTemp(spoolDir, "upload-*")
	if err != nil {
		http.Error(w, "failed to create temp file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := io.Copy(tmpFile, file); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		http.Error(w, "failed to spool upload: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		http.Error(w, "failed to rewind temp file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Also place a copy in the sync directory so the file stays "local"
	// instead of becoming a cloud-only stub on the next daemon restart.
	if h.syncDir != nil {
		if err := h.syncDir.WriteLocalCopy(virtualPath, tmpFile); err != nil {
			slog.Error("upload: failed to write local copy", "path", virtualPath, "error", err)
			// Non-fatal — the cloud upload still proceeds.
		}
		if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			http.Error(w, "failed to rewind temp file: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// WriteFileAsync takes ownership of tmpFile (caller must not close it).
	if err := h.engine.WriteFileAsync(virtualPath, tmpFile, tmpFile.Name(), header.Size); err != nil {
		http.Error(w, "upload failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","path":%q,"size":%d}`, virtualPath, header.Size)
}

// ensureParentDirs creates all ancestor directory records for a path like
// "/photos/vacation/2024" → creates "/photos", "/photos/vacation",
// "/photos/vacation/2024".
func (h *browserHandler) ensureParentDirs(dirPath string) {
	parts := strings.Split(strings.Trim(dirPath, "/"), "/")
	cur := ""
	for _, p := range parts {
		cur += "/" + p
		h.engine.MkDir(cur) //nolint:errcheck
	}
}

func (h *browserHandler) serveAPIUploadCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	vp := cleanPath(r.URL.Query().Get("path"))
	if vp == "" || vp == "/" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}
	ok := h.engine.CancelUpload(vp)
	w.Header().Set("Content-Type", "application/json")
	if ok {
		fmt.Fprintf(w, `{"ok":true,"path":%q}`, vp)
	} else {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"ok":false,"error":"no active upload for path"}`)
	}
}

func (h *browserHandler) serveAPIVerify(w http.ResponseWriter, r *http.Request) {
	p := cleanPath(r.URL.Query().Get("path"))
	if p == "/" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}

	tmp, err := h.engine.ReadFileToTempFile(p)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"path":%q,"ok":false,"error":%q}`, p, err.Error())
		return
	}
	tmp.Close()
	os.Remove(tmp.Name())

	h.engine.DB().InsertActivity("verify", p, "integrity check passed") //nolint:errcheck
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"path":%q,"ok":true}`, p)
}

func (h *browserHandler) serveAPIActivity(w http.ResponseWriter, r *http.Request) {
	entries, err := h.engine.DB().RecentActivity(100)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if entries == nil {
		entries = []metadata.ActivityEntry{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries) //nolint:errcheck
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
	indexHTML, _ := webFS.ReadFile("web/index.html")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML) //nolint:errcheck
}
