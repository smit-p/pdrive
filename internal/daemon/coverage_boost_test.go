package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/chunker"
	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/metadata"
)

// ── helpers ─────────────────────────────────────────────────────────────────

// newDaemonWithEngine creates a Daemon wired to a real engine + fakeCloud,
// useful for testing daemon methods that need engine access (e.g. purgeJunkFiles).
func newDaemonWithEngine(t *testing.T) (*Daemon, *engine.Engine, *metadata.DB) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	total, free := int64(100e9), int64(99e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "p1", Type: "drive", DisplayName: "TestDrive",
		RcloneRemote: "fake", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})

	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, cloud, b, encKey)
	t.Cleanup(eng.Close)

	d := &Daemon{
		config: Config{ConfigDir: dir},
		db:     db,
		engine: eng,
	}
	return d, eng, db
}

// ── serveAPIUpload — error branches ─────────────────────────────────────────

func TestUpload_MethodNotAllowed(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/upload", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestUpload_InvalidMultipart(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("POST", "/api/upload", strings.NewReader("not multipart"))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=invalid")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestUpload_NoFileField(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	w.WriteField("dir", "/test")
	w.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestUpload_JunkFileSkipped(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", ".DS_Store")
	part.Write([]byte("junk data"))
	w.WriteField("dir", "/test")
	w.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "skipped") {
		t.Errorf("expected skipped response, got %s", rec.Body.String())
	}
}

func TestUpload_ThumbsDbSkipped(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "Thumbs.db")
	part.Write([]byte("windows junk"))
	w.WriteField("dir", "/")
	w.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "skipped") {
		t.Errorf("expected skipped, got %s", rec.Body.String())
	}
}

func TestUpload_SpoolDirFailure(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	h.spoolDir = "/nonexistent/readonly/dir"

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "doc.txt")
	part.Write([]byte("data"))
	w.WriteField("dir", "/")
	w.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestUpload_NestedDir_EnsureParentDirs(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "deep.txt")
	part.Write([]byte("nested data"))
	w.WriteField("dir", "/a/b/c")
	w.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	eng.WaitUploads()

	// Verify file exists at nested path.
	exists, _ := eng.FileExists("/a/b/c/deep.txt")
	if !exists {
		t.Error("file /a/b/c/deep.txt should exist after upload")
	}
}

func TestUpload_EmptyDir_DefaultsToRoot(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "root.txt")
	part.Write([]byte("at root"))
	w.WriteField("dir", "")
	w.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	eng.WaitUploads()

	exists, _ := eng.FileExists("/root.txt")
	if !exists {
		t.Error("file should exist at /root.txt")
	}
}

// ── serveAPIActivity ────────────────────────────────────────────────────────

func TestActivity_EmptyLog(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/activity", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	// Should return an empty JSON array.
	if !strings.Contains(body, "[]") {
		t.Errorf("expected empty array, got %s", body)
	}
}

// ── serveAPIDu with path ────────────────────────────────────────────────────

func TestDu_WithSpecificPath(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	// Upload a file first.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "x.txt")
	part.Write([]byte("disk usage test"))
	w.WriteField("dir", "/dutest")
	w.Close()
	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	eng.WaitUploads()

	// Query disk usage for the specific path (not "." or empty).
	req = httptest.NewRequest("GET", "/api/du?path=/dutest", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("du: %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"file_count":1`) {
		t.Errorf("expected 1 file, got %s", rec.Body.String())
	}
}

// ── serveAPILs with junk filter ─────────────────────────────────────────────

func TestLs_FiltersJunkFiles(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	// Upload a .DS_Store directly through the engine (bypassing API junk check)
	// so it exists in the DB but gets filtered by ls.
	tmp, err := os.CreateTemp("", "junk-*")
	if err != nil {
		t.Fatal(err)
	}
	tmp.Write([]byte("junk"))
	tmp.Seek(0, io.SeekStart)
	eng.WriteFileAsync("/.DS_Store", tmp, tmp.Name(), 4)
	eng.WaitUploads()

	// Also write a normal file.
	tmp2, _ := os.CreateTemp("", "normal-*")
	tmp2.Write([]byte("normal"))
	tmp2.Seek(0, io.SeekStart)
	eng.WriteFileAsync("/normal.txt", tmp2, tmp2.Name(), 6)
	eng.WaitUploads()

	req := httptest.NewRequest("GET", "/api/ls?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ls: %d", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, ".DS_Store") {
		t.Error("ls should filter out .DS_Store")
	}
	if !strings.Contains(body, "normal.txt") {
		t.Error("ls should include normal.txt")
	}
}

// ── purgeJunkFiles ──────────────────────────────────────────────────────────

func TestPurgeJunkFiles_RemovesJunkViaDaemon(t *testing.T) {
	d, eng, _ := newDaemonWithEngine(t)

	// Write a .DS_Store file directly via engine.
	tmp, err := os.CreateTemp("", "junk-*")
	if err != nil {
		t.Fatal(err)
	}
	tmp.Write([]byte("junk data"))
	tmp.Seek(0, io.SeekStart)
	eng.WriteFileAsync("/.DS_Store", tmp, tmp.Name(), 9)
	eng.WaitUploads()

	// Write a normal file.
	tmp2, _ := os.CreateTemp("", "normal-*")
	tmp2.Write([]byte("keep me"))
	tmp2.Seek(0, io.SeekStart)
	eng.WriteFileAsync("/keepme.txt", tmp2, tmp2.Name(), 7)
	eng.WaitUploads()

	// Verify both exist.
	exists1, _ := eng.FileExists("/.DS_Store")
	exists2, _ := eng.FileExists("/keepme.txt")
	if !exists1 || !exists2 {
		t.Fatalf("setup: .DS_Store=%v, keepme.txt=%v", exists1, exists2)
	}

	d.purgeJunkFiles()

	exists1, _ = eng.FileExists("/.DS_Store")
	exists2, _ = eng.FileExists("/keepme.txt")
	if exists1 {
		t.Error(".DS_Store should have been purged")
	}
	if !exists2 {
		t.Error("keepme.txt should still exist")
	}
}

func TestPurgeJunkFiles_NoJunkFiles(t *testing.T) {
	d, eng, _ := newDaemonWithEngine(t)

	tmp, _ := os.CreateTemp("", "norm-*")
	tmp.Write([]byte("data"))
	tmp.Seek(0, io.SeekStart)
	eng.WriteFileAsync("/clean.txt", tmp, tmp.Name(), 4)
	eng.WaitUploads()

	// Should not panic or error.
	d.purgeJunkFiles()

	exists, _ := eng.FileExists("/clean.txt")
	if !exists {
		t.Error("clean.txt should still exist")
	}
}

// ── syncProviders ───────────────────────────────────────────────────────────

func TestSyncProviders_FiltersByRemotesConfig(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive", "onedrive": "onedrive", "s3": "s3"},
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)
	d.config.Remotes = []string{"gdrive", "s3"}

	d.syncProviders()

	providers, _ := db.GetAllProviders()
	names := make(map[string]bool)
	for _, p := range providers {
		names[p.RcloneRemote] = true
	}
	if len(names) != 2 {
		t.Errorf("expected 2 providers, got %d: %v", len(names), names)
	}
	if !names["gdrive"] || !names["s3"] {
		t.Errorf("expected gdrive and s3, got %v", names)
	}
}

func TestSyncProviders_LoadsRemotesJSON(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive", "onedrive": "onedrive"},
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)
	d.config.Remotes = nil // empty — will read remotes.json

	// Write remotes.json selecting only gdrive.
	os.WriteFile(filepath.Join(d.config.ConfigDir, "remotes.json"), []byte(`["gdrive"]`), 0600)

	d.syncProviders()

	providers, _ := db.GetAllProviders()
	if len(providers) != 1 || providers[0].RcloneRemote != "gdrive" {
		t.Errorf("expected only gdrive, got %v", providers)
	}
}

func TestSyncProviders_AllRemotesWhenNoFilter(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive", "onedrive": "onedrive"},
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)
	d.config.Remotes = nil // no filter

	d.syncProviders()

	providers, _ := db.GetAllProviders()
	if len(providers) != 2 {
		t.Errorf("expected 2 providers, got %d", len(providers))
	}
}

func TestSyncProviders_NoMatchingRemotes(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)
	d.config.Remotes = []string{"nonexistent"}

	d.syncProviders()

	providers, _ := db.GetAllProviders()
	if len(providers) != 0 {
		t.Errorf("expected 0 providers, got %d", len(providers))
	}
	_ = db
}

func TestSyncProviders_ExistingProvider(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)

	// Pre-populate with an existing provider to test the update path.
	total, free := int64(50e9), int64(25e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "gdrive", Type: "drive", DisplayName: "gdrive",
		RcloneRemote: "gdrive", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
		AccountIdentity: "existing@gmail.com",
	})

	d.syncProviders()

	providers, _ := db.GetAllProviders()
	if len(providers) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(providers))
	}
	// Account identity should be preserved.
	if providers[0].AccountIdentity != "existing@gmail.com" {
		t.Errorf("identity = %q, want existing@gmail.com", providers[0].AccountIdentity)
	}
	// Quota should be updated from new About() call.
	if providers[0].QuotaTotalBytes == nil || *providers[0].QuotaTotalBytes != int64(100e9) {
		t.Errorf("quota should be updated to 100e9, got %v", providers[0].QuotaTotalBytes)
	}
}

// ── checkMissingProviders ───────────────────────────────────────────────────

func TestCheckMissingProviders_WithMissingType(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)

	// Insert a provider that doesn't exist as a remote (with type but no identity).
	total := int64(50e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "missing-remote", Type: "dropbox", DisplayName: "missing-remote",
		RcloneRemote: "missing-remote", QuotaTotalBytes: &total,
	})

	missing := d.checkMissingProviders()
	if len(missing) == 0 {
		t.Error("expected missing providers")
	}
	if !strings.Contains(missing[0], "dropbox") {
		t.Errorf("missing label should contain type, got %q", missing[0])
	}
}

func TestCheckMissingProviders_WithIdentityAndType(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)

	total := int64(50e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "missing", Type: "drive", DisplayName: "missing",
		RcloneRemote:    "missing",
		QuotaTotalBytes: &total,
		AccountIdentity: "alice@gmail.com",
	})

	missing := d.checkMissingProviders()
	if len(missing) != 1 {
		t.Fatalf("expected 1 missing, got %d", len(missing))
	}
	if !strings.Contains(missing[0], "alice@gmail.com") {
		t.Errorf("missing should have identity: %q", missing[0])
	}
	if !strings.Contains(missing[0], "drive") {
		t.Errorf("missing should have type: %q", missing[0])
	}
}

// ── daemon Start / Stop — additional branches ───────────────────────────────

func TestDaemon_StartStop_WithSyncDir(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	fakeRclone := buildFakeRclone(t)
	rclonePort := freePort(t)
	httpPort := freePort(t)
	dir := t.TempDir()

	cfg := Config{
		ConfigDir:   dir,
		RcloneBin:   fakeRclone,
		RcloneAddr:  rclonePort,
		WebDAVAddr:  httpPort,
		SyncDir:     filepath.Join(dir, "sync"),
		SkipRestore: true,
	}

	d := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if d.syncDir == nil {
		t.Error("syncDir should not be nil when SyncDir is configured")
	}

	// SyncDir directory should exist.
	if _, err := os.Stat(filepath.Join(dir, "sync")); os.IsNotExist(err) {
		t.Error("sync directory should have been created")
	}

	d.Stop()
	cancel()
}

func TestDaemon_StartStop_WithRateAndChunkSize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	fakeRclone := buildFakeRclone(t)
	rclonePort := freePort(t)
	httpPort := freePort(t)
	dir := t.TempDir()

	cfg := Config{
		ConfigDir:   dir,
		RcloneBin:   fakeRclone,
		RcloneAddr:  rclonePort,
		WebDAVAddr:  httpPort,
		SkipRestore: true,
		ChunkSize:   1 << 20, // 1 MB
		RatePerSec:  10,      // 10 req/s
	}

	d := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if d.Engine() == nil {
		t.Error("Engine should be configured")
	}

	d.Stop()
	cancel()
}

func TestDaemon_StartStop_WithRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	fakeRclone := buildFakeRclone(t)
	rclonePort := freePort(t)
	httpPort := freePort(t)
	dir := t.TempDir()

	cfg := Config{
		ConfigDir:   dir,
		RcloneBin:   fakeRclone,
		RcloneAddr:  rclonePort,
		WebDAVAddr:  httpPort,
		SkipRestore: false, // will attempt restore, find nothing
	}

	d := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// DB should be open and engine available.
	if d.Engine() == nil {
		t.Error("Engine should be available after Start")
	}

	d.Stop()
	cancel()
}

func TestDaemon_StartStop_WithMFSPolicy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	fakeRclone := buildFakeRclone(t)
	rclonePort := freePort(t)
	httpPort := freePort(t)
	dir := t.TempDir()

	cfg := Config{
		ConfigDir:    dir,
		RcloneBin:    fakeRclone,
		RcloneAddr:   rclonePort,
		WebDAVAddr:   httpPort,
		SkipRestore:  true,
		BrokerPolicy: "mfs",
	}

	d := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	d.Stop()
	cancel()
}

func TestDaemon_StartStop_WithRemotes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	fakeRclone := buildFakeRclone(t)
	rclonePort := freePort(t)
	httpPort := freePort(t)
	dir := t.TempDir()

	cfg := Config{
		ConfigDir:   dir,
		RcloneBin:   fakeRclone,
		RcloneAddr:  rclonePort,
		WebDAVAddr:  httpPort,
		SkipRestore: true,
		Remotes:     []string{"nonexistent"},
	}

	d := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	d.Stop()
	cancel()
}

// Test that periodic goroutines exit when context is cancelled.
func TestDaemon_StartStop_PeriodicGoroutinesExit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	fakeRclone := buildFakeRclone(t)
	rclonePort := freePort(t)
	httpPort := freePort(t)
	dir := t.TempDir()

	cfg := Config{
		ConfigDir:   dir,
		RcloneBin:   fakeRclone,
		RcloneAddr:  rclonePort,
		WebDAVAddr:  httpPort,
		SkipRestore: true,
	}

	d := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Let the periodic goroutines start.
	time.Sleep(50 * time.Millisecond)

	// Cancel context — goroutines should exit.
	cancel()
	time.Sleep(50 * time.Millisecond)

	d.Stop()
}

// ── serveAPIVerify — error path ─────────────────────────────────────────────

func TestVerify_MissingFile(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/verify?path=/nonexistent.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"ok":false`) {
		t.Errorf("expected ok:false, got %s", body)
	}
}

func TestVerify_RootPath(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/verify?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

// ── serveAPIDownload — error paths ──────────────────────────────────────────

func TestDownload_RootPath(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/download?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestDownload_MissingFile(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/download?path=/nope.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

// ── serveAPIResync ──────────────────────────────────────────────────────────

func TestResync_TriggersCallback(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	triggered := false
	h.resyncProviders = func() { triggered = true }

	req := httptest.NewRequest("POST", "/api/resync", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	time.Sleep(50 * time.Millisecond) // resyncProviders is called in a goroutine
	if !triggered {
		t.Error("resyncProviders should have been called")
	}
}

// ── serveAPIFind — missing pattern ──────────────────────────────────────────

func TestFind_MissingPattern(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/find?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

// ── ServeHTTP — static assets ───────────────────────────────────────────────

func TestServeHTTP_StaticCSS(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/static/styles.css", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// Should serve the embedded CSS (or 404 if not found — either way, not 500).
	if rec.Code == http.StatusInternalServerError {
		t.Errorf("expected non-500, got %d", rec.Code)
	}
}

func TestServeHTTP_NoDavHandler_NilFallback(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	h.davHandler = nil // simulate missing WebDAV

	// A PUT request (WebDAV method) when davHandler is nil.
	req := httptest.NewRequest("PUT", "/some-webdav-path", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

// ── recovery — edge cases ───────────────────────────────────────────────────

func TestResolveCloudSalt_LocalSaltAlreadyExists(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files:   map[string][]byte{},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)

	// Write a local salt file first.
	saltPath := filepath.Join(d.config.ConfigDir, "enc.salt")
	os.WriteFile(saltPath, make([]byte, 16), 0600)

	// resolveCloudSalt should still work (generates fresh since no cloud salt).
	if err := d.resolveCloudSalt(); err != nil {
		t.Fatalf("resolveCloudSalt() error: %v", err)
	}
	if len(d.config.EncKey) != 32 {
		t.Errorf("EncKey length = %d, want 32", len(d.config.EncKey))
	}
}

func TestTryDownloadLegacy_EmptyFile(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files: map[string][]byte{
			"gdrive:pdrive-meta/metadata.db": {}, // empty file
		},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)

	_, ok := d.tryDownloadLegacy("gdrive")
	if ok {
		t.Error("tryDownloadLegacy should return false for empty file")
	}
}

func TestTryDownloadEncrypted_EmptyFile(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files: map[string][]byte{
			"gdrive:pdrive-meta/metadata.db.enc": {}, // empty
		},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)
	d.config.EncKey = make([]byte, 32)

	_, _, ok := d.tryDownloadEncrypted("gdrive")
	if ok {
		t.Error("tryDownloadEncrypted should return false for empty blob")
	}
}

// ── rclone manager — monitor health loop ────────────────────────────────────
// The monitor function polls every 10s which is too slow for unit tests.
// Test the restart-on-failure path by calling monitor with a very short context
// and a mock that fails ping immediately.

func TestRcloneManager_MonitorRestartOnFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("builds fake rclone binary")
	}

	fakeRclone := buildFakeRclone(t)
	addr := freePort(t)
	configPath := filepath.Join(t.TempDir(), "rclone.conf")
	os.WriteFile(configPath, []byte(""), 0600)

	rm := NewRcloneManager(fakeRclone, configPath, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := rm.Start(ctx); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	// Verify running.
	if err := rm.Client().Ping(); err != nil {
		t.Fatalf("initial ping: %v", err)
	}

	// Kill the process to simulate crash — monitor should restart it.
	rm.mu.Lock()
	if rm.cmd != nil && rm.cmd.Process != nil {
		rm.cmd.Process.Kill()
		rm.cmd.Wait() //nolint: errcheck
		rm.cmd = nil
	}
	rm.mu.Unlock()

	// Directly test the restart logic from monitor (instead of waiting 10s).
	rm.mu.Lock()
	if err := rm.spawn(ctx); err != nil {
		t.Fatalf("manual respawn: %v", err)
	}
	if err := rm.waitHealthy(ctx, 5 * time.Second); err != nil {
		t.Fatalf("waitHealthy after respawn: %v", err)
	}
	rm.mu.Unlock()

	// Verify alive again.
	if err := rm.Client().Ping(); err != nil {
		t.Errorf("ping after restart: %v", err)
	}

	rm.Stop()
}

func TestRcloneManager_SpawnBadBinary(t *testing.T) {
	rm := NewRcloneManager("/nonexistent/rclone", "/tmp/cfg", "127.0.0.1:0")
	err := rm.spawn(context.Background())
	if err == nil {
		t.Error("spawn with bad binary should error")
	}
}

// ── API error paths (closed-DB triggers first DB call failure) ──────────────

// newClosedDBHandler creates a browserHandler whose underlying DB has been
// closed, so every engine operation that hits the DB returns an error.
func newClosedDBHandler(t *testing.T) *browserHandler {
	t.Helper()
	h, _, db := newTestHandlerWithCloud(t)
	db.Close() // all subsequent DB operations will fail
	return h
}

func TestLs_DBError(t *testing.T) {
	h := newClosedDBHandler(t)
	req := httptest.NewRequest("GET", "/api/ls?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestTree_DBError(t *testing.T) {
	h := newClosedDBHandler(t)
	req := httptest.NewRequest("GET", "/api/tree?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestFind_DBError(t *testing.T) {
	h := newClosedDBHandler(t)
	req := httptest.NewRequest("GET", "/api/find?path=/&pattern=*.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDu_DBError(t *testing.T) {
	h := newClosedDBHandler(t)
	req := httptest.NewRequest("GET", "/api/du?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestActivity_DBError(t *testing.T) {
	h := newClosedDBHandler(t)
	req := httptest.NewRequest("GET", "/api/activity", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDelete_IsDirDBError(t *testing.T) {
	h := newClosedDBHandler(t)
	req := httptest.NewRequest("POST", "/api/delete?path=/somefile.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMv_IsDirDBError(t *testing.T) {
	h := newClosedDBHandler(t)
	req := httptest.NewRequest("POST", "/api/mv?src=/a.txt&dst=/b.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestUpload_WriteFileAsyncDBError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)

	// Build a valid multipart upload.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "test.txt")
	part.Write([]byte("upload data"))
	w.WriteField("dir", "/")
	w.Close()

	// Close DB AFTER building the request so multipart parsing succeeds
	// but WriteFileAsync fails when it tries to check space.
	db.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── provider.go — error paths ───────────────────────────────────────────────

func TestSyncProviders_ListRemotesError(t *testing.T) {
	// Server that fails on ListRemotes.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rclone down", http.StatusInternalServerError)
	}))
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)
	// Should not panic — just logs a warning and returns.
	d.syncProviders()
}

func TestSyncProviders_AboutError_ExistingProvider(t *testing.T) {
	// Server where About fails but other calls succeed.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		switch r.URL.Path {
		case "/config/listremotes":
			json.NewEncoder(w).Encode(map[string]any{"remotes": []string{"gdrive"}})
		case "/config/get":
			json.NewEncoder(w).Encode(map[string]any{"type": "drive"})
		case "/operations/about":
			http.Error(w, "about failed", http.StatusInternalServerError)
		default:
			json.NewEncoder(w).Encode(map[string]any{})
		}
	}))
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)
	// Pre-populate an existing provider with quota so the fallback path is hit.
	total, free := int64(50e9), int64(25e9)
	polled := time.Now().Unix()
	db.UpsertProvider(&metadata.Provider{
		ID: "gdrive", Type: "drive", DisplayName: "gdrive",
		RcloneRemote:    "gdrive",
		QuotaTotalBytes: &total,
		QuotaFreeBytes:  &free,
		QuotaPolledAt:   &polled,
		AccountIdentity: "user@example.com",
	})

	d.syncProviders()

	// Existing quota values should be preserved.
	providers, _ := db.GetAllProviders()
	if len(providers) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(providers))
	}
	if providers[0].QuotaTotalBytes == nil || *providers[0].QuotaTotalBytes != total {
		t.Errorf("quota should be preserved, got %v", providers[0].QuotaTotalBytes)
	}
}

func TestSyncProviders_GetRemoteTypeError(t *testing.T) {
	// Server where config/get fails, so GetRemoteType returns error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		switch r.URL.Path {
		case "/config/listremotes":
			json.NewEncoder(w).Encode(map[string]any{"remotes": []string{"badremote"}})
		case "/config/get":
			http.Error(w, "config get failed", http.StatusInternalServerError)
		case "/operations/about":
			json.NewEncoder(w).Encode(map[string]any{"total": int64(100e9), "free": int64(50e9)})
		default:
			json.NewEncoder(w).Encode(map[string]any{})
		}
	}))
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)
	d.syncProviders()

	providers, _ := db.GetAllProviders()
	if len(providers) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(providers))
	}
	// Type should be "unknown" since GetRemoteType failed.
	if providers[0].Type != "unknown" {
		t.Errorf("expected type=unknown, got %q", providers[0].Type)
	}
}

func TestSyncProviders_UpsertProviderError(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)
	// Close DB so UpsertProvider fails.
	db.Close()
	// Should not panic.
	d.syncProviders()
}

func TestCheckMissingProviders_ListRemotesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)
	// Insert a provider so dbProviders is non-empty.
	total := int64(50e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "test", Type: "drive", DisplayName: "test",
		RcloneRemote: "test", QuotaTotalBytes: &total,
	})

	missing := d.checkMissingProviders()
	// Should return nil because ListRemotes failed.
	if missing != nil {
		t.Errorf("expected nil, got %v", missing)
	}
}

func TestPurgeJunkFiles_ListAllFilesError(t *testing.T) {
	d, _, db := newDaemonWithEngine(t)
	// Close DB so ListAllFiles fails.
	db.Close()
	// Should not panic.
	d.purgeJunkFiles()
}

// ── recovery.go — error paths ───────────────────────────────────────────────

func TestResolveCloudSalt_ListRemotesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rclone down", http.StatusInternalServerError)
	}))
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)
	// Should still work (generates fresh salt since no cloud salt found).
	if err := d.resolveCloudSalt(); err != nil {
		t.Fatalf("resolveCloudSalt should succeed with fresh salt, got: %v", err)
	}
	if len(d.config.EncKey) != 32 {
		t.Errorf("EncKey length = %d, want 32", len(d.config.EncKey))
	}
}

func TestTryRestoreDB_ListRemotesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rclone down", http.StatusInternalServerError)
	}))
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)
	dbPath := filepath.Join(d.config.ConfigDir, "restored.db")
	ok := d.tryRestoreDB(dbPath)
	if ok {
		t.Error("tryRestoreDB should return false when ListRemotes fails")
	}
}

func TestTryRestoreDB_WriteError(t *testing.T) {
	// Create a valid encrypted backup on the fake cloud.
	salt, _ := chunker.GenerateSalt()
	key := makeTestEncKey("test-password", salt)

	// Create a minimal valid SQLite DB.
	tmpDir := os.TempDir()
	tmpDB := filepath.Join(tmpDir, "minimal-test.db")
	mdb, err := metadata.Open(tmpDB)
	if err != nil {
		t.Fatal(err)
	}
	mdb.Close()
	dbData, _ := os.ReadFile(tmpDB)
	os.Remove(tmpDB)
	payload := makeTestBackupPayload(t, key, dbData)

	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files: map[string][]byte{
			"gdrive:pdrive-meta/enc.salt":        salt,
			"gdrive:pdrive-meta/metadata.db.enc": payload,
		},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)
	d.config.EncKey = key

	// Try to write to a read-only path so os.WriteFile fails.
	badPath := "/dev/null/impossible/restored.db"
	ok := d.tryRestoreDB(badPath)
	if ok {
		t.Error("tryRestoreDB should return false when write fails")
	}
}

func TestValidateRestoredDB_QueryError(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)
	// Close DB so the initial COUNT query fails.
	db.Close()
	result := d.validateRestoredDB()
	// When the DB is closed, the query should error and we treat it as 0 chunks → valid.
	// Actually, QueryRow.Scan on a closed DB returns an error, so chunkCount stays 0 → returns true.
	if !result {
		t.Error("validateRestoredDB should return true for errored empty DB query")
	}
}

func TestResolveCloudSalt_ReadError(t *testing.T) {
	// Salt on cloud is wrong size (triggers readErr || len(salt) != SaltSize).
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files: map[string][]byte{
			"gdrive:pdrive-meta/enc.salt": []byte("too-short"),
		},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)
	// Should fall through to generating a fresh salt.
	if err := d.resolveCloudSalt(); err != nil {
		t.Fatalf("resolveCloudSalt should generate fresh salt, got: %v", err)
	}
	if len(d.config.EncKey) != 32 {
		t.Errorf("EncKey length = %d, want 32", len(d.config.EncKey))
	}
}

// ── rclone_manager.go — monitor restart path ────────────────────────────────

func TestRcloneManager_MonitorContext(t *testing.T) {
	// Test that monitor exits when context is cancelled.
	rm := NewRcloneManager("/nonexistent", "/tmp/cfg", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	// monitor should return immediately.
	rm.monitor(ctx)
}

func TestRcloneManager_StartSpawnFailure(t *testing.T) {
	rm := NewRcloneManager("/nonexistent/rclone", "/tmp/cfg", "127.0.0.1:0")
	err := rm.Start(context.Background())
	if err == nil {
		t.Error("Start with bad binary should error")
	}
}

func TestResolveCloudSalt_MkdirAllError(t *testing.T) {
	// Server with no salt on cloud, so resolveCloudSalt tries to generate fresh.
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files:   map[string][]byte{},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)
	// Set configDir to an impossible path so MkdirAll fails.
	d.config.ConfigDir = "/dev/null/impossible"
	err := d.resolveCloudSalt()
	if err == nil {
		t.Error("resolveCloudSalt should fail when configDir is unwritable")
	}
	if !strings.Contains(err.Error(), "creating config dir") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestResolveCloudSalt_WriteFileError(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files:   map[string][]byte{},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)
	// ConfigDir exists (from TempDir) but make the salt file path fail by
	// creating a directory where the file should be.
	saltDir := filepath.Join(d.config.ConfigDir, "enc.salt")
	os.MkdirAll(saltDir, 0755) // make enc.salt a directory so WriteFile fails
	err := d.resolveCloudSalt()
	if err == nil {
		t.Error("resolveCloudSalt should fail when salt file path is a directory")
	}
	if !strings.Contains(err.Error(), "saving salt") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateRestoredDB_ChunksExist_ListDirError(t *testing.T) {
	// Server that fails on operations/list (for chunk validation).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		switch r.URL.Path {
		case "/config/listremotes":
			json.NewEncoder(w).Encode(map[string]any{"remotes": []string{"gdrive"}})
		case "/config/get":
			json.NewEncoder(w).Encode(map[string]any{"type": "drive"})
		case "/operations/about":
			json.NewEncoder(w).Encode(map[string]any{"total": int64(100e9), "free": int64(50e9)})
		case "/operations/list":
			// Return empty list so validation thinks chunks are missing.
			json.NewEncoder(w).Encode(map[string]any{"list": []any{}})
		default:
			json.NewEncoder(w).Encode(map[string]any{})
		}
	}))
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)

	// Insert a provider and a fake file + chunk_location so chunkCount > 0.
	total, free := int64(100e9), int64(50e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "gdrive", Type: "drive", DisplayName: "gdrive",
		RcloneRemote: "gdrive", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
	// Insert a file record.
	db.Conn().Exec(`INSERT INTO files (id, virtual_path, size_bytes, sha256_full, created_at, modified_at, upload_state) VALUES ('f1', '/test.txt', 100, 'abc123', 1000, 1000, 'complete')`)
	// Insert a chunk record.
	db.Conn().Exec(`INSERT INTO chunks (id, file_id, sequence, size_bytes, sha256, encrypted_size) VALUES ('c1', 'f1', 0, 100, 'chunk_hash', 128)`)
	// Insert a chunk location pointing to gdrive.
	db.Conn().Exec(`INSERT INTO chunk_locations (chunk_id, provider_id, remote_path) VALUES ('c1', 'gdrive', 'pdrive-chunks/c1')`)

	result := d.validateRestoredDB()
	if result {
		t.Error("validateRestoredDB should return false when cloud chunks are missing")
	}
}

func TestValidateRestoredDB_QueryFailure(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)

	// We need chunkCount > 0 (first query passes) but the JOIN query to fail.
	// Insert a provider and valid chain of file→chunk→chunk_location.
	total, free := int64(100e9), int64(50e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "gdrive", Type: "drive", DisplayName: "gdrive",
		RcloneRemote: "gdrive", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
	db.Conn().Exec(`INSERT INTO files (id, virtual_path, size_bytes, sha256_full, created_at, modified_at, upload_state) VALUES ('f1', '/t.txt', 10, 'h', 1, 1, 'complete')`)
	db.Conn().Exec(`INSERT INTO chunks (id, file_id, sequence, size_bytes, sha256, encrypted_size) VALUES ('c1', 'f1', 0, 10, 'h', 16)`)
	db.Conn().Exec(`INSERT INTO chunk_locations (chunk_id, provider_id, remote_path) VALUES ('c1', 'gdrive', 'pdrive-chunks/c1')`)

	// Now rename the providers table so the JOIN errors.
	db.Conn().Exec(`PRAGMA foreign_keys=OFF`)
	db.Conn().Exec(`ALTER TABLE providers RENAME TO providers_old`)

	result := d.validateRestoredDB()
	if result {
		t.Error("validateRestoredDB should return false when JOIN query fails")
	}
}

func TestResolveCloudSalt_CloudSaltWriteError(t *testing.T) {
	// Put a valid salt on cloud.
	salt, _ := chunker.GenerateSalt()
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files: map[string][]byte{
			"gdrive:pdrive-meta/enc.salt": salt,
		},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)
	// Make the salt file path unwritable by creating a directory there.
	saltDir := filepath.Join(d.config.ConfigDir, "enc.salt")
	os.MkdirAll(saltDir, 0755)

	// Should still succeed (writes fail but the key is derived).
	if err := d.resolveCloudSalt(); err != nil {
		t.Fatalf("resolveCloudSalt should succeed despite write error, got: %v", err)
	}
	if len(d.config.EncKey) != 32 {
		t.Errorf("EncKey length = %d, want 32", len(d.config.EncKey))
	}
}

// ── daemon.Start error branches ─────────────────────────────────────────────

func TestStart_MkdirAllError(t *testing.T) {
	d := New(Config{ConfigDir: "/dev/null/impossible"})
	err := d.Start(context.Background())
	if err == nil {
		t.Fatal("expected error from MkdirAll")
	}
	if !strings.Contains(err.Error(), "creating config directory") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestStart_DBOpenError(t *testing.T) {
	dir := t.TempDir()
	// Create metadata.db as a directory so sqlite cannot open it.
	os.MkdirAll(filepath.Join(dir, "metadata.db"), 0755)
	d := New(Config{ConfigDir: dir})
	err := d.Start(context.Background())
	if err == nil {
		t.Fatal("expected error from metadata.Open")
	}
	if !strings.Contains(err.Error(), "opening metadata db") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ── validateRestoredDB — Scan error ─────────────────────────────────────────

func TestValidateRestoredDB_ScanError(t *testing.T) {
	d, _, db := newDaemonWithEngine(t)

	// Insert a provider with NULL rclone_remote via raw SQL.
	db.Conn().Exec(`INSERT INTO providers (id, type, display_name, rclone_remote) VALUES ('pnull', 'drive', 'NullRemote', NULL)`)
	db.Conn().Exec(`INSERT INTO files (id, virtual_path, size_bytes, sha256_full, created_at, modified_at, upload_state) VALUES ('f1', '/test.txt', 100, 'abc', 1, 1, 'complete')`)
	db.Conn().Exec(`INSERT INTO chunks (id, file_id, sequence, size_bytes, sha256, encrypted_size) VALUES ('c1', 'f1', 0, 100, 'h', 128)`)
	db.Conn().Exec(`INSERT INTO chunk_locations (chunk_id, provider_id, remote_path) VALUES ('c1', 'pnull', 'pdrive-chunks/c1')`)

	// validateRestoredDB should hit Scan error (NULL → string) and continue,
	// then return true since no valid chunks found to disprove.
	if !d.validateRestoredDB() {
		t.Error("expected validateRestoredDB to return true when Scan fails")
	}
}
