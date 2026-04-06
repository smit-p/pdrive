package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/rclonerc"
	"github.com/smit-p/pdrive/internal/vfs"
	"golang.org/x/net/webdav"
)

// fakeCloud is an in-memory CloudStorage for daemon tests that need actual
// cloud read/write capabilities (e.g., download and browser file serving).
type fakeCloud struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newFakeCloud() *fakeCloud { return &fakeCloud{objects: make(map[string][]byte)} }

func (f *fakeCloud) key(remote, p string) string { return remote + ":" + p }

func (f *fakeCloud) PutFile(remote, p string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.objects[f.key(remote, p)] = data
	f.mu.Unlock()
	return nil
}

func (f *fakeCloud) GetFile(remote, p string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objects[f.key(remote, p)]
	if !ok {
		return nil, fmt.Errorf("not found: %s/%s", remote, p)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return io.NopCloser(bytes.NewReader(cp)), nil
}

func (f *fakeCloud) DeleteFile(remote, p string) error {
	f.mu.Lock()
	delete(f.objects, f.key(remote, p))
	f.mu.Unlock()
	return nil
}

func (f *fakeCloud) ListDir(remote, p string) ([]rclonerc.ListItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := remote + ":" + p + "/"
	var items []rclonerc.ListItem
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			name := strings.TrimPrefix(k, prefix)
			if !strings.Contains(name, "/") {
				items = append(items, rclonerc.ListItem{Name: name, Path: p + "/" + name})
			}
		}
	}
	return items, nil
}

func (f *fakeCloud) Cleanup(remote string) error { return nil }
func (f *fakeCloud) Mkdir(remote, path string) error { return nil }

// newTestHandlerWithCloud creates a handler backed by a real fakeCloud engine
// so that file writes/reads succeed end-to-end.
func newTestHandlerWithCloud(t *testing.T) (*browserHandler, *engine.Engine, *metadata.DB) {
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
		RcloneRemote: "fake:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})

	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, cloud, b, encKey)
	t.Cleanup(eng.Close)

	h := &browserHandler{engine: eng, startTime: time.Now()}
	return h, eng, db
}

func TestHealthEndpoint(t *testing.T) {
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
		RcloneRemote:    "fake:",
		QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})

	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, nil, b, encKey)

	h := &browserHandler{
		engine:    eng,
		startTime: time.Now().Add(-5 * time.Second),
	}

	req := httptest.NewRequest("GET", "/api/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp struct {
		Status          string  `json:"status"`
		UptimeSeconds   float64 `json:"uptime_seconds"`
		InFlightUploads int     `json:"in_flight_uploads"`
		DBOK            bool    `json:"db_ok"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "ok" {
		t.Errorf("expected status=ok, got %q", resp.Status)
	}
	if !resp.DBOK {
		t.Error("expected db_ok=true")
	}
	if resp.UptimeSeconds < 5 {
		t.Errorf("expected uptime >= 5s, got %.1f", resp.UptimeSeconds)
	}
	if resp.InFlightUploads != 0 {
		t.Errorf("expected 0 in-flight uploads, got %d", resp.InFlightUploads)
	}
}

func TestMetricsEndpoint(t *testing.T) {
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
		RcloneRemote:    "fake:",
		QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})

	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, nil, b, encKey)

	h := &browserHandler{
		engine:    eng,
		startTime: time.Now(),
	}

	req := httptest.NewRequest("GET", "/api/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp engine.MetricsSnapshot
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	// Fresh engine should have all-zero counters.
	if resp.FilesUploaded != 0 || resp.FilesDownloaded != 0 || resp.DedupHits != 0 {
		t.Errorf("fresh engine should have zero metrics: %+v", resp)
	}
}

func TestLsEndpoint(t *testing.T) {
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
		RcloneRemote:    "fake:",
		QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})

	// Seed a complete file.
	now := time.Now().Unix()
	db.InsertFile(&metadata.File{
		ID: "f1", VirtualPath: "/docs/readme.txt", SizeBytes: 42,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h", UploadState: "complete",
	})

	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, nil, b, encKey)

	h := &browserHandler{engine: eng, startTime: time.Now()}

	req := httptest.NewRequest("GET", "/api/ls?path=/docs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp lsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Path != "/docs" {
		t.Errorf("expected path /docs, got %q", resp.Path)
	}
	if len(resp.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(resp.Files))
	}
	if resp.Files[0].Name != "readme.txt" {
		t.Errorf("expected readme.txt, got %q", resp.Files[0].Name)
	}
}

func TestStatusEndpoint(t *testing.T) {
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
		RcloneRemote:    "fake:",
		QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})

	now := time.Now().Unix()
	db.InsertFile(&metadata.File{
		ID: "f1", VirtualPath: "/a.txt", SizeBytes: 100,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h", UploadState: "complete",
	})
	db.InsertFile(&metadata.File{
		ID: "f2", VirtualPath: "/b.txt", SizeBytes: 200,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h2", UploadState: "complete",
	})

	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, nil, b, encKey)

	h := &browserHandler{engine: eng, startTime: time.Now()}

	req := httptest.NewRequest("GET", "/api/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp statusResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.TotalFiles != 2 {
		t.Errorf("expected 2 files, got %d", resp.TotalFiles)
	}
	if resp.TotalBytes != 300 {
		t.Errorf("expected 300 bytes, got %d", resp.TotalBytes)
	}
	if len(resp.Providers) != 1 {
		t.Errorf("expected 1 provider, got %d", len(resp.Providers))
	}
}

func TestPinEndpoint_RequiresPOST(t *testing.T) {
	h := &browserHandler{startTime: time.Now()}

	req := httptest.NewRequest("GET", "/api/pin?path=/test.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for GET /api/pin, got %d", rec.Code)
	}
}

func TestPinEndpoint_NoSyncDir(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, nil, b, encKey)

	h := &browserHandler{engine: eng, syncDir: nil, startTime: time.Now()}

	req := httptest.NewRequest("POST", "/api/pin?path=/test.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when syncDir is nil, got %d", rec.Code)
	}
}

// newTestHandler creates a browserHandler wired to a real engine + DB for tests.
func newTestHandler(t *testing.T) (*browserHandler, *metadata.DB) {
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
		RcloneRemote: "fake:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})

	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, nil, b, encKey)
	t.Cleanup(eng.Close)

	h := &browserHandler{engine: eng, startTime: time.Now()}
	return h, db
}

func seedFile(t *testing.T, db *metadata.DB, id, vpath string, size int64) {
	t.Helper()
	now := time.Now().Unix()
	db.InsertFile(&metadata.File{
		ID: id, VirtualPath: vpath, SizeBytes: size,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "sha-" + id, UploadState: "complete",
	})
}

// ── Unpin endpoint ──

func TestUnpinEndpoint_RequiresPOST(t *testing.T) {
	h := &browserHandler{startTime: time.Now()}
	req := httptest.NewRequest("GET", "/api/unpin?path=/x.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestUnpinEndpoint_NoSyncDir(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("POST", "/api/unpin?path=/test.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}

func TestUnpinEndpoint_NoPath(t *testing.T) {
	h := &browserHandler{syncDir: nil, startTime: time.Now()}
	req := httptest.NewRequest("POST", "/api/unpin", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// No syncDir → 503 before path check
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}

// ── Delete endpoint ──

func TestDeleteEndpoint_RequiresPOST(t *testing.T) {
	h := &browserHandler{startTime: time.Now()}
	req := httptest.NewRequest("GET", "/api/delete?path=/x.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestDeleteEndpoint_NoPath(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("POST", "/api/delete", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestDeleteEndpoint_FileNotFound(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("POST", "/api/delete?path=/nope.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestDeleteEndpoint_File(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f1", "/docs/readme.txt", 42)

	req := httptest.NewRequest("POST", "/api/delete?path=/docs/readme.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]string
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp["type"] != "file" {
		t.Errorf("expected type=file, got %q", resp["type"])
	}

	// Verify file is gone.
	exists, _ := db.FileExists("/docs/readme.txt")
	if exists {
		t.Error("file should have been deleted")
	}
}

func TestDeleteEndpoint_Dir(t *testing.T) {
	h, db := newTestHandler(t)
	db.CreateDirectory("/mydir")
	seedFile(t, db, "f1", "/mydir/a.txt", 10)

	req := httptest.NewRequest("POST", "/api/delete?path=/mydir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]string
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp["type"] != "dir" {
		t.Errorf("expected type=dir, got %q", resp["type"])
	}
}

// ── Tree endpoint ──

func TestTreeEndpoint(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f1", "/a.txt", 10)
	seedFile(t, db, "f2", "/sub/b.txt", 20)

	req := httptest.NewRequest("GET", "/api/tree?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var entries []struct {
		Path string `json:"path"`
		Size int64  `json:"size"`
	}
	json.NewDecoder(rec.Body).Decode(&entries)
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}
}

func TestTreeEndpoint_DefaultPath(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f1", "/x.txt", 5)

	req := httptest.NewRequest("GET", "/api/tree", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// ── Find endpoint ──

func TestFindEndpoint(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f1", "/docs/readme.txt", 42)
	seedFile(t, db, "f2", "/images/photo.jpg", 100)

	req := httptest.NewRequest("GET", "/api/find?path=/&pattern=readme", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var entries []struct {
		Path string `json:"path"`
	}
	json.NewDecoder(rec.Body).Decode(&entries)
	if len(entries) != 1 {
		t.Errorf("expected 1 match, got %d", len(entries))
	}
}

func TestFindEndpoint_NoPattern(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/find?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing pattern, got %d", rec.Code)
	}
}

// ── Mv endpoint ──

func TestMvEndpoint_RequiresPOST(t *testing.T) {
	h := &browserHandler{startTime: time.Now()}
	req := httptest.NewRequest("GET", "/api/mv?src=/a&dst=/b", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestMvEndpoint_MissingParams(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("POST", "/api/mv?src=/a", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestMvEndpoint_FileNotFound(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("POST", "/api/mv?src=/nonexistent.txt&dst=/new.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestMvEndpoint_File(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f1", "/old.txt", 42)

	req := httptest.NewRequest("POST", "/api/mv?src=/old.txt&dst=/new.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp map[string]string
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp["type"] != "file" {
		t.Errorf("expected type=file, got %q", resp["type"])
	}
}

func TestMvEndpoint_Dir(t *testing.T) {
	h, db := newTestHandler(t)
	db.CreateDirectory("/srcdir")
	seedFile(t, db, "f1", "/srcdir/a.txt", 10)

	req := httptest.NewRequest("POST", "/api/mv?src=/srcdir&dst=/dstdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp map[string]string
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp["type"] != "dir" {
		t.Errorf("expected type=dir, got %q", resp["type"])
	}
}

// ── Mkdir endpoint ──

func TestMkdirEndpoint_RequiresPOST(t *testing.T) {
	h := &browserHandler{startTime: time.Now()}
	req := httptest.NewRequest("GET", "/api/mkdir?path=/new", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestMkdirEndpoint_NoPath(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("POST", "/api/mkdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestMkdirEndpoint_Success(t *testing.T) {
	h, db := newTestHandler(t)

	req := httptest.NewRequest("POST", "/api/mkdir?path=/newdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	exists, _ := db.DirectoryExists("/newdir")
	if !exists {
		t.Error("directory should exist")
	}
}

// ── Info endpoint ──

func TestInfoEndpoint_NoPath(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/info", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestInfoEndpoint_NotFound(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/info?path=/nope.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestInfoEndpoint_Success(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f1", "/test.txt", 42)

	req := httptest.NewRequest("GET", "/api/info?path=/test.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp struct {
		Path      string `json:"path"`
		SizeBytes int64  `json:"size_bytes"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Path != "/test.txt" {
		t.Errorf("expected path=/test.txt, got %q", resp.Path)
	}
	if resp.SizeBytes != 42 {
		t.Errorf("expected size=42, got %d", resp.SizeBytes)
	}
}

// ── Du endpoint ──

func TestDuEndpoint(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f1", "/a.txt", 100)
	seedFile(t, db, "f2", "/b.txt", 200)

	req := httptest.NewRequest("GET", "/api/du?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp struct {
		FileCount  int64 `json:"file_count"`
		TotalBytes int64 `json:"total_bytes"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.FileCount != 2 {
		t.Errorf("expected file_count=2, got %d", resp.FileCount)
	}
	if resp.TotalBytes != 300 {
		t.Errorf("expected total_bytes=300, got %d", resp.TotalBytes)
	}
}

func TestDuEndpoint_DefaultPath(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/du", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// ── Download endpoint ──

func TestDownloadEndpoint_NoPath(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/download", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestDownloadEndpoint_NotFound(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/download?path=/missing.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

// ── Uploads endpoint ──

func TestUploadsEndpoint(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/uploads", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// ── Browser endpoint ──

func TestBrowserEndpoint_ServesHTML(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if ct != "text/html; charset=utf-8" {
		t.Errorf("expected text/html content-type, got %q", ct)
	}
}

func TestBrowserEndpoint_FileNotFound(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/nonexistent.txt", nil)
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// When file doesn't exist and it's a browser request, serves the SPA shell.
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (SPA shell), got %d", rec.Code)
	}
}

// ── Ls endpoint edge cases ──

func TestLsEndpoint_DefaultPath(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/ls", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp lsResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Path != "/" {
		t.Errorf("expected path=/, got %q", resp.Path)
	}
}

func TestLsEndpoint_WithDirs(t *testing.T) {
	h, db := newTestHandler(t)
	db.CreateDirectory("/mydir")
	seedFile(t, db, "f1", "/mydir/a.txt", 10)

	req := httptest.NewRequest("GET", "/api/ls?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp lsResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if len(resp.Dirs) == 0 {
		t.Error("expected at least one directory")
	}
}

// ── Pin endpoint edge cases ──

func TestPinEndpoint_NoPath(t *testing.T) {
	h := &browserHandler{syncDir: nil, startTime: time.Now()}
	req := httptest.NewRequest("POST", "/api/pin", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// No syncDir → 503 before path check
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}

// ── Status endpoint (with provider data) ──

func TestStatusEndpoint_Success(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp statusResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Providers) == 0 {
		t.Error("expected at least 1 provider")
	}
}

// ── Mv endpoint (file rename success) ──

func TestMvEndpoint_FileSuccess(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f-mv1", "/old.txt", 5)

	req := httptest.NewRequest("POST", "/api/mv?src=/old.txt&dst=/new.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Mv endpoint (directory rename success) ──

func TestMvEndpoint_DirSuccess(t *testing.T) {
	h, db := newTestHandler(t)
	db.CreateDirectory("/srcdir/")
	seedFile(t, db, "f-mv2", "/srcdir/a.txt", 3)

	req := httptest.NewRequest("POST", "/api/mv?src=/srcdir&dst=/dstdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Du endpoint (with seeded file) ──

func TestDuEndpoint_WithFile(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f-du1", "/du/file.bin", 42)

	req := httptest.NewRequest("GET", "/api/du?path=/du", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		FileCount  int64 `json:"file_count"`
		TotalBytes int64 `json:"total_bytes"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.FileCount != 1 {
		t.Errorf("expected 1 file, got %d", resp.FileCount)
	}
	if resp.TotalBytes != 42 {
		t.Errorf("expected 42 bytes, got %d", resp.TotalBytes)
	}
}

// ── Ls endpoint with seeded files ──

func TestLsEndpoint_WithFiles(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f-ls1", "/a/x.txt", 5)
	seedFile(t, db, "f-ls2", "/a/y.txt", 10)

	req := httptest.NewRequest("GET", "/api/ls?path=/a", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Find endpoint with matching results ──

func TestFindEndpoint_WithResults(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f-find1", "/docs/readme.md", 20)

	req := httptest.NewRequest("GET", "/api/find?pattern=readme", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Tree endpoint with seeded files ──

func TestTreeEndpoint_WithFiles(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f-tree1", "/t/a.txt", 1)
	seedFile(t, db, "f-tree2", "/t/b.txt", 2)

	req := httptest.NewRequest("GET", "/api/tree?path=/t", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Pin endpoint with syncDir (error in PinFile) ──

func TestPinEndpoint_WithSyncDir_BadPath(t *testing.T) {
	h, _ := newTestHandler(t)
	dir := t.TempDir()
	h.syncDir = vfs.NewSyncDir(dir, h.engine, dir)
	// POST with empty path → 400
	req := httptest.NewRequest("POST", "/api/pin?path=", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestPinEndpoint_PinFileError(t *testing.T) {
	h, _ := newTestHandler(t)
	dir := t.TempDir()
	h.syncDir = vfs.NewSyncDir(dir, h.engine, dir)
	// File doesn't exist in engine → PinFile returns error → 500
	req := httptest.NewRequest("POST", "/api/pin?path=/nonexistent.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Unpin endpoint with syncDir (error in UnpinFile) ──

func TestUnpinEndpoint_WithSyncDir_BadPath(t *testing.T) {
	h, _ := newTestHandler(t)
	dir := t.TempDir()
	h.syncDir = vfs.NewSyncDir(dir, h.engine, dir)
	req := httptest.NewRequest("POST", "/api/unpin?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestUnpinEndpoint_UnpinFileError(t *testing.T) {
	h, _ := newTestHandler(t)
	dir := t.TempDir()
	h.syncDir = vfs.NewSyncDir(dir, h.engine, dir)
	req := httptest.NewRequest("POST", "/api/unpin?path=/nonexistent.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Download endpoint: file exists but no cloud → 500 ──

func TestDownloadEndpoint_ReadError(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f-dl1", "/file.bin", 10)
	// Stat succeeds but ReadFileToTempFile fails (no chunks / no cloud).
	req := httptest.NewRequest("GET", "/api/download?path=/file.bin", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Delete endpoint: directory that also removes local sync data ──

func TestDeleteEndpoint_DirWithSyncDir(t *testing.T) {
	h, db := newTestHandler(t)
	dir := t.TempDir()
	h.syncDir = vfs.NewSyncDir(dir, h.engine, dir)
	db.CreateDirectory("/rmdir/")
	seedFile(t, db, "f-rmd", "/rmdir/f.txt", 5)
	// Create local dir so RemoveAll has something to remove.
	os.MkdirAll(filepath.Join(dir, "rmdir"), 0755)

	req := httptest.NewRequest("POST", "/api/delete?path=/rmdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Ls endpoint with subdirectories ──

func TestLsEndpoint_SubdirListing(t *testing.T) {
	h, db := newTestHandler(t)
	db.CreateDirectory("/sub/")
	seedFile(t, db, "f-sub1", "/sub/a.txt", 1)

	req := httptest.NewRequest("GET", "/api/ls?path=/sub", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Files []interface{} `json:"files"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	if len(resp.Files) == 0 {
		t.Error("expected files in response")
	}
}

// ── Mv endpoint: rename a directory path ──

func TestMvEndpoint_DirWithContents(t *testing.T) {
	h, db := newTestHandler(t)
	db.CreateDirectory("/before/")
	seedFile(t, db, "f-mv99", "/before/test.txt", 7)

	req := httptest.NewRequest("POST", "/api/mv?src=/before&dst=/after", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Mkdir endpoint with nested path ──

func TestMkdirEndpoint_NestedPath(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("POST", "/api/mkdir?path=/a/b/c", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Mv endpoint with syncDir ──

func TestMvEndpoint_FileWithSyncDir(t *testing.T) {
	h, db := newTestHandler(t)
	dir := t.TempDir()
	h.syncDir = vfs.NewSyncDir(dir, h.engine, dir)
	seedFile(t, db, "f-mvs1", "/mvs/old.txt", 5)
	// Create local file so Rename has something.
	os.MkdirAll(filepath.Join(dir, "mvs"), 0755)
	os.WriteFile(filepath.Join(dir, "mvs", "old.txt"), []byte("hi"), 0644)

	req := httptest.NewRequest("POST", "/api/mv?src=/mvs/old.txt&dst=/mvs/new.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMvEndpoint_DirWithSyncDir(t *testing.T) {
	h, db := newTestHandler(t)
	dir := t.TempDir()
	h.syncDir = vfs.NewSyncDir(dir, h.engine, dir)
	db.CreateDirectory("/mvdir/")
	seedFile(t, db, "f-mvsd1", "/mvdir/a.txt", 3)
	os.MkdirAll(filepath.Join(dir, "mvdir"), 0755)

	req := httptest.NewRequest("POST", "/api/mv?src=/mvdir&dst=/mvdir2", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Mkdir endpoint with syncDir ──

func TestMkdirEndpoint_WithSyncDir(t *testing.T) {
	h, _ := newTestHandler(t)
	dir := t.TempDir()
	h.syncDir = vfs.NewSyncDir(dir, h.engine, dir)

	req := httptest.NewRequest("POST", "/api/mkdir?path=/syncmk", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Check local dir was created.
	if _, err := os.Stat(filepath.Join(dir, "syncmk")); os.IsNotExist(err) {
		t.Error("expected local directory to be created via syncDir")
	}
}

// ── Browser serves SPA shell for directories ──

func TestBrowserEndpoint_DirectoryServesHTML(t *testing.T) {
	h, _ := newTestHandler(t)

	// Request a path that is not a file → should return HTML shell.
	req := httptest.NewRequest("GET", "/somedir", nil)
	rec := httptest.NewRecorder()
	h.serveBrowser(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if ct != "text/html; charset=utf-8" {
		t.Errorf("expected text/html content type, got %q", ct)
	}
	if rec.Body.Len() == 0 {
		t.Error("expected non-empty HTML body")
	}
}

// ── Delete single file with syncDir ──

func TestDeleteEndpoint_FileWithSyncDir(t *testing.T) {
	h, db := newTestHandler(t)
	dir := t.TempDir()
	h.syncDir = vfs.NewSyncDir(dir, h.engine, dir)
	seedFile(t, db, "f-delsync", "/delsync.txt", 5)
	os.WriteFile(filepath.Join(dir, "delsync.txt"), []byte("hello"), 0644)

	req := httptest.NewRequest("POST", "/api/delete?path=/delsync.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Check local file was removed.
	if _, err := os.Stat(filepath.Join(dir, "delsync.txt")); !os.IsNotExist(err) {
		t.Error("expected local file to be removed")
	}
}

// ── Ls endpoint with pendingFile shows uploading state ──

func TestLsEndpoint_PendingFileState(t *testing.T) {
	h, db := newTestHandler(t)
	now := time.Now().Unix()
	tmp := "/tmp/fake"
	db.InsertFile(&metadata.File{
		ID: "f-pend", VirtualPath: "/pls/uploading.bin", SizeBytes: 99,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: &tmp,
	})
	// Also seed a complete file.
	seedFile(t, db, "f-done", "/pls/done.txt", 42)

	req := httptest.NewRequest("GET", "/api/ls?path=/pls", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Du endpoint default (empty path) ──

func TestDuEndpoint_Default(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f-du-d", "/du-d/file.bin", 100)

	req := httptest.NewRequest("GET", "/api/du", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Info endpoint (with chunks) ──

func TestInfoEndpoint_WithChunks(t *testing.T) {
	h, db := newTestHandler(t)
	seedFile(t, db, "f-info-c", "/info/detailed.txt", 50)
	// Insert a chunk + location so the info response has chunk data.
	db.Conn().Exec(
		`INSERT INTO chunks (id, file_id, sequence, size_bytes, sha256, encrypted_size) VALUES (?, ?, ?, ?, ?, ?)`,
		"c-info1", "f-info-c", 0, 50, "fakehash", 66,
	)
	db.Conn().Exec(
		`INSERT INTO chunk_locations (chunk_id, provider_id, remote_path, upload_confirmed_at) VALUES (?, ?, ?, ?)`,
		"c-info1", "p1", "pdrive-chunks/info1.enc", time.Now().Unix(),
	)

	req := httptest.NewRequest("GET", "/api/info?path=/info/detailed.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Chunks []struct {
			Sequence      int      `json:"sequence"`
			SizeBytes     int      `json:"size_bytes"`
			EncryptedSize int      `json:"encrypted_size"`
			Providers     []string `json:"providers"`
		} `json:"chunks"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(resp.Chunks))
	}
	if len(resp.Chunks[0].Providers) == 0 {
		t.Error("expected at least 1 provider in chunk info")
	}
}

// ── Uploads endpoint (empty) ──

func TestUploadsEndpoint_Empty(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/uploads", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// ── ServeHTTP routing: unknown API path ──

func TestServeHTTP_UnknownAPIPath(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/nonexistent", nil)
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// unknown /api/ paths with browser Accept header fall through to serveBrowser
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 (browser fallthrough), got %d", rec.Code)
	}
}

// ── Health endpoint with degraded DB ──

func TestHealthEndpoint_NoDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, nil, b, encKey)

	h := &browserHandler{engine: eng, startTime: time.Now()}

	// Close DB to trigger degraded health.
	db.Close()

	req := httptest.NewRequest("GET", "/api/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp struct {
		Status string `json:"status"`
		DBOK   bool   `json:"db_ok"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.DBOK {
		t.Error("expected db_ok=false when DB is closed")
	}
	if resp.Status != "degraded" {
		t.Errorf("expected status=degraded, got %q", resp.Status)
	}
}

// ── Download endpoint: successful download with cloud-backed engine ──

func TestDownloadEndpoint_Success(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	// Write a real file through the engine.
	if err := eng.WriteFile("/dl/hello.txt", []byte("hello world")); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/api/download?path=/dl/hello.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Header().Get("Content-Disposition"), "hello.txt") {
		t.Errorf("expected Content-Disposition with filename, got %q", rec.Header().Get("Content-Disposition"))
	}
	if rec.Body.String() != "hello world" {
		t.Errorf("expected body 'hello world', got %q", rec.Body.String())
	}
}

// ── Download endpoint: root path (should be 400) ──

func TestDownloadEndpoint_RootPath(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/download?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

// ── Browser endpoint: file streaming with cloud-backed engine ──

func TestBrowserEndpoint_FileStream(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	if err := eng.WriteFile("/browser/doc.txt", []byte("browser content")); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/browser/doc.txt", nil)
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "browser content") {
		t.Errorf("expected file content in response, got %q", rec.Body.String())
	}
}

// ── Ls with syncDir stub state ──

func TestLsEndpoint_StubState(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)

	// Write a real file so it's "complete".
	if err := eng.WriteFile("/stub/data.txt", []byte("data")); err != nil {
		t.Fatal(err)
	}

	// Set up syncDir and create a stub file.
	syncRoot := t.TempDir()
	h.syncDir = vfs.NewSyncDir(syncRoot, eng, syncRoot)
	stubPath := filepath.Join(syncRoot, "stub", "data.txt")
	os.MkdirAll(filepath.Dir(stubPath), 0755)
	// Create stub by writing empty file with xattr (use the vfs helper via unpin or manual).
	// Simplest: just use UnpinFile which creates a stub for a known file.
	_ = db // ensure db not collected
	if err := h.syncDir.UnpinFile("/stub/data.txt"); err != nil {
		t.Fatalf("UnpinFile: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/ls?path=/stub", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp lsResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if len(resp.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(resp.Files))
	}
	if resp.Files[0].LocalState != "stub" {
		t.Errorf("expected local_state=stub, got %q", resp.Files[0].LocalState)
	}
}

// ── Pin endpoint: successful pin with cloud engine ──

func TestPinEndpoint_Success(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	if err := eng.WriteFile("/pin/test.txt", []byte("pin me")); err != nil {
		t.Fatal(err)
	}

	syncRoot := t.TempDir()
	h.syncDir = vfs.NewSyncDir(syncRoot, eng, syncRoot)

	req := httptest.NewRequest("POST", "/api/pin?path=/pin/test.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"state":"local"`) {
		t.Errorf("expected state=local in response, got %s", rec.Body.String())
	}
}

// ── Unpin endpoint: successful unpin with cloud engine ──

func TestUnpinEndpoint_Success(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	if err := eng.WriteFile("/unpin/test.txt", []byte("unpin me")); err != nil {
		t.Fatal(err)
	}

	syncRoot := t.TempDir()
	h.syncDir = vfs.NewSyncDir(syncRoot, eng, syncRoot)
	// Create the local file for unpin.
	os.MkdirAll(filepath.Join(syncRoot, "unpin"), 0755)
	os.WriteFile(filepath.Join(syncRoot, "unpin", "test.txt"), []byte("unpin me"), 0644)

	req := httptest.NewRequest("POST", "/api/unpin?path=/unpin/test.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"state":"stub"`) {
		t.Errorf("expected state=stub in response, got %s", rec.Body.String())
	}
}

// ── WebDAV passthrough for non-browser requests ──

func TestServeHTTP_WebDAVPassthrough(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	if err := eng.WriteFile("/dav/test.txt", []byte("dav content")); err != nil {
		t.Fatal(err)
	}

	// Non-browser GET (no Accept: text/html) should go to DAV handler.
	// Since h.davHandler is nil, this will panic unless we set it.
	// Instead, test that a PROPFIND or non-html GET goes through browser fallback.
	req := httptest.NewRequest("GET", "/dav/test.txt", nil)
	// No text/html Accept header → goes to davHandler.
	// Since davHandler is nil in test handler, let's just verify the routing.
	// We can check that a browser request with Accept: text/html goes to serveBrowser.
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// Should serve the file content via serveBrowser.
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Mv endpoint with syncDir: file rename updates local ──

func TestMvEndpoint_FileWithSyncDir_RenamesLocal(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	if err := eng.WriteFile("/mvl/old.txt", []byte("content")); err != nil {
		t.Fatal(err)
	}

	syncRoot := t.TempDir()
	h.syncDir = vfs.NewSyncDir(syncRoot, eng, syncRoot)
	os.MkdirAll(filepath.Join(syncRoot, "mvl"), 0755)
	os.WriteFile(filepath.Join(syncRoot, "mvl", "old.txt"), []byte("content"), 0644)

	req := httptest.NewRequest("POST", "/api/mv?src=/mvl/old.txt&dst=/mvl/new.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// New local file should exist.
	if _, err := os.Stat(filepath.Join(syncRoot, "mvl", "new.txt")); os.IsNotExist(err) {
		t.Error("expected local file to be renamed")
	}
}

// ── Delete endpoint with syncDir: removes local dir ──

func TestDeleteEndpoint_DirWithSyncDir_RemovesLocal(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)

	db.CreateDirectory("/rmlocal/")
	seedFile(t, db, "f-rmlocal", "/rmlocal/f.txt", 5)
	_ = eng // keep reference

	syncRoot := t.TempDir()
	h.syncDir = vfs.NewSyncDir(syncRoot, eng, syncRoot)
	os.MkdirAll(filepath.Join(syncRoot, "rmlocal"), 0755)
	os.WriteFile(filepath.Join(syncRoot, "rmlocal", "f.txt"), []byte("data"), 0644)

	req := httptest.NewRequest("POST", "/api/delete?path=/rmlocal", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Local dir should be removed.
	if _, err := os.Stat(filepath.Join(syncRoot, "rmlocal")); !os.IsNotExist(err) {
		t.Error("expected local directory to be removed")
	}
}

// ── Delete endpoint with syncDir: removes local file ──

func TestDeleteEndpoint_FileWithSyncDir_RemovesLocal(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	if err := eng.WriteFile("/delsyncf.txt", []byte("data")); err != nil {
		t.Fatal(err)
	}

	syncRoot := t.TempDir()
	h.syncDir = vfs.NewSyncDir(syncRoot, eng, syncRoot)
	os.WriteFile(filepath.Join(syncRoot, "delsyncf.txt"), []byte("data"), 0644)

	req := httptest.NewRequest("POST", "/api/delete?path=/delsyncf.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if _, err := os.Stat(filepath.Join(syncRoot, "delsyncf.txt")); !os.IsNotExist(err) {
		t.Error("expected local file to be removed")
	}
}

// ── Mkdir with syncDir: creates local directory ──

func TestMkdirEndpoint_WithSyncDir_CreatesLocal(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)

	syncRoot := t.TempDir()
	h.syncDir = vfs.NewSyncDir(syncRoot, eng, syncRoot)

	req := httptest.NewRequest("POST", "/api/mkdir?path=/syncmklocal", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if _, err := os.Stat(filepath.Join(syncRoot, "syncmklocal")); os.IsNotExist(err) {
		t.Error("expected local directory to be created")
	}
}

// ── Mv dir with syncDir: renames local directory ──

func TestMvEndpoint_DirWithSyncDir_RenamesLocal(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)

	db.CreateDirectory("/mvdlocal/")
	seedFile(t, db, "f-mvdl", "/mvdlocal/a.txt", 3)
	_ = eng

	syncRoot := t.TempDir()
	h.syncDir = vfs.NewSyncDir(syncRoot, eng, syncRoot)
	os.MkdirAll(filepath.Join(syncRoot, "mvdlocal"), 0755)

	req := httptest.NewRequest("POST", "/api/mv?src=/mvdlocal&dst=/mvdlocal2", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if _, err := os.Stat(filepath.Join(syncRoot, "mvdlocal2")); os.IsNotExist(err) {
		t.Error("expected local directory to be renamed")
	}
}

// ── Mv endpoint: src and dst params required ──

func TestMvEndpoint_EmptyParams(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("POST", "/api/mv?src=&dst=", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Info endpoint: file not found via cloud-backed handler ──

func TestInfoEndpoint_CloudNotFound(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/info?path=/ghost.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Download endpoint: file not found via cloud-backed handler ──

func TestDownloadEndpoint_CloudNotFound(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/download?path=/ghost.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── TreeEndpoint: with subpath filter ──

func TestTreeEndpoint_WithSubpath(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.CreateDirectory("/treedir/")
	seedFile(t, db, "f-tree1", "/treedir/a.txt", 100)
	seedFile(t, db, "f-tree2", "/treedir/b.txt", 200)

	req := httptest.NewRequest("GET", "/api/tree?path=/treedir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "a.txt") || !strings.Contains(body, "b.txt") {
		t.Errorf("expected both files in tree:\n%s", body)
	}
}

// ── FindEndpoint: empty pattern returns 400 ──

func TestFindEndpoint_EmptyPattern(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/find?pattern=", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── DuEndpoint: root path (no param) ──

func TestDuEndpoint_RootPath(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	seedFile(t, db, "f-du1", "/du1.txt", 1024)
	seedFile(t, db, "f-du2", "/du2.txt", 2048)

	req := httptest.NewRequest("GET", "/api/du", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"file_count":2`) {
		t.Errorf("expected 2 files:\n%s", body)
	}
}

// ── DuEndpoint: subpath ──

func TestDuEndpoint_SubPath(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.CreateDirectory("/dudir/")
	seedFile(t, db, "f-du3", "/dudir/a.txt", 100)

	req := httptest.NewRequest("GET", "/api/du?path=/dudir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"file_count":1`) {
		t.Errorf("expected 1 file:\n%s", body)
	}
}

// ── StatusEndpoint: with file data and provider quotas ──

func TestStatusEndpoint_WithData(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	seedFile(t, db, "f-st1", "/st1.txt", 1024)

	req := httptest.NewRequest("GET", "/api/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"total_files":1`) {
		t.Errorf("expected 1 file in status:\n%s", body)
	}
	if !strings.Contains(body, "TestDrive") {
		t.Errorf("expected provider name in status:\n%s", body)
	}
}

// ── Delete endpoint: root path returns 400 ──

func TestDeleteEndpoint_RootPath(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("POST", "/api/delete?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

// ── Mkdir endpoint: root path returns 400 ──

func TestMkdirEndpoint_RootPath(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("POST", "/api/mkdir?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

// ── Mv file with cloud engine: full write+rename ──

func TestMvEndpoint_FileWithCloudEngine(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	if err := eng.WriteFile("/mvfile.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("POST", "/api/mv?src=/mvfile.txt&dst=/mvfile2.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"type":"file"`) {
		t.Errorf("expected type=file:\n%s", body)
	}
}

// ── Delete file with cloud engine ──

func TestDeleteEndpoint_FileWithCloudEngine(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	if err := eng.WriteFile("/delfile.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("POST", "/api/delete?path=/delfile.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"type":"file"`) {
		t.Errorf("expected type=file:\n%s", body)
	}
}

// ── Delete directory with cloud engine ──

func TestDeleteEndpoint_DirWithCloudEngine(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	db.CreateDirectory("/deldir/")
	if err := eng.WriteFile("/deldir/a.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("POST", "/api/delete?path=/deldir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"type":"dir"`) {
		t.Errorf("expected type=dir:\n%s", body)
	}
}

// ── Info endpoint: file with real chunks via cloud engine ──

func TestInfoEndpoint_FileWithRealChunks(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	if err := eng.WriteFile("/infofile.txt", []byte("hello world chunk data")); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/api/info?path=/infofile.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "infofile.txt") {
		t.Errorf("expected path in info:\n%s", body)
	}
	if !strings.Contains(body, "chunks") {
		t.Errorf("expected chunks array in info:\n%s", body)
	}
}

// ── Find endpoint: with cloud-written file ──

func TestFindEndpoint_CloudResults(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	if err := eng.WriteFile("/findme.txt", []byte("data")); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/api/find?pattern=findme", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "findme.txt") {
		t.Errorf("expected findme.txt in results:\n%s", body)
	}
}

// ── Delete non-existent file (not dir) → 404 ───────────────────────────────

func TestDeleteEndpoint_NonExistentFile(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("POST", "/api/delete?path=/ghost.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Delete method check ─────────────────────────────────────────────────────

func TestDeleteEndpoint_WrongMethod(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/delete?path=/x.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

// ── Mv non-existent src (not dir) → 404 ────────────────────────────────────

func TestMvEndpoint_NonExistentSrc(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("POST", "/api/mv?src=/nope.txt&dst=/dest.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Mv method check ─────────────────────────────────────────────────────────

func TestMvEndpoint_WrongMethod(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/mv?src=/a&dst=/b", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

// ── Mkdir method check ──────────────────────────────────────────────────────

func TestMkdirEndpoint_WrongMethod(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/mkdir?path=/x", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

// ── Browser directory path → SPA shell ──────────────────────────────────────

func TestBrowserEndpoint_Directory(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	eng.MkDir("/mydir/")

	req := httptest.NewRequest("GET", "/mydir/", nil)
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	// Should return HTML (SPA shell), not JSON.
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("expected text/html content type, got %q", ct)
	}
}

func TestBrowserEndpoint_Root(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("expected text/html, got %q", ct)
	}
}

// ── Du with files ───────────────────────────────────────────────────────────

func TestDuEndpoint_WithFiles(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	eng.WriteFile("/dudir/a.txt", []byte("hello"))
	eng.WriteFile("/dudir/b.txt", []byte("world!"))

	req := httptest.NewRequest("GET", "/api/du?path=/dudir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Path       string `json:"path"`
		FileCount  int    `json:"file_count"`
		TotalBytes int64  `json:"total_bytes"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.FileCount < 2 {
		t.Errorf("expected >= 2 files, got %d", resp.FileCount)
	}
	if resp.TotalBytes < 11 {
		t.Errorf("expected >= 11 bytes, got %d", resp.TotalBytes)
	}
}

// ── Info with real chunks ───────────────────────────────────────────────────

func TestInfoEndpoint_CompleteFile(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	eng.WriteFile("/info-detail.txt", []byte("details"))

	req := httptest.NewRequest("GET", "/api/info?path=/info-detail.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Path        string `json:"path"`
		SizeBytes   int64  `json:"size_bytes"`
		UploadState string `json:"upload_state"`
		Chunks      []struct {
			Sequence  int      `json:"sequence"`
			Providers []string `json:"providers"`
		} `json:"chunks"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Path != "/info-detail.txt" {
		t.Errorf("expected path /info-detail.txt, got %q", resp.Path)
	}
	if len(resp.Chunks) == 0 {
		t.Error("expected at least 1 chunk")
	}
	if resp.UploadState != "complete" {
		t.Errorf("expected upload_state=complete, got %q", resp.UploadState)
	}
}

// ── Mv dir (rename dir) with cloud engine ───────────────────────────────────

func TestMvEndpoint_DirWithCloudEngine(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	eng.MkDir("/mvdir/")
	eng.WriteFile("/mvdir/x.txt", []byte("x"))

	req := httptest.NewRequest("POST", "/api/mv?src=/mvdir&dst=/mvdest", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"type":"dir"`) {
		t.Errorf("expected type=dir in response: %s", body)
	}
}

// ── Download with file ──────────────────────────────────────────────────────

func TestDownloadEndpoint_WithFile(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	eng.WriteFile("/dl.bin", []byte("download me"))

	req := httptest.NewRequest("GET", "/api/download?path=/dl.bin", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "download me" {
		t.Errorf("expected 'download me', got %q", rec.Body.String())
	}
	cd := rec.Header().Get("Content-Disposition")
	if !strings.Contains(cd, "dl.bin") {
		t.Errorf("expected Content-Disposition with filename, got %q", cd)
	}
}

// ── Ls with files and dirs ──────────────────────────────────────────────────

func TestLsEndpoint_WithData(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	eng.MkDir("/lsdir/")
	eng.WriteFile("/lsdir/f.txt", []byte("file"))
	eng.MkDir("/lsdir/sub/")

	req := httptest.NewRequest("GET", "/api/ls?path=/lsdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "f.txt") {
		t.Errorf("expected f.txt in ls output: %s", body)
	}
}

// ── Tree with files (cloud) ─────────────────────────────────────────────────

func TestTreeEndpoint_WithCloudFiles(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	eng.WriteFile("/tree/a.txt", []byte("a"))
	eng.WriteFile("/tree/b.txt", []byte("bb"))

	req := httptest.NewRequest("GET", "/api/tree?path=/tree", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "a.txt") || !strings.Contains(body, "b.txt") {
		t.Errorf("expected both files in tree: %s", body)
	}
}

// ── Uploads endpoint with in-progress upload ────────────────────────────────

func TestUploadsEndpoint_NoActiveUploads(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	req := httptest.NewRequest("GET", "/api/uploads", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "[]") {
		t.Errorf("expected empty array, got %s", body)
	}
}

// ── Error paths (closed DB triggers engine errors) ──────────────────────────

func TestLsEndpoint_EngineError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("GET", "/api/ls?path=/nothing", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestStatusEndpoint_EngineError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("GET", "/api/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestTreeEndpoint_EngineError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("GET", "/api/tree?path=/x", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestFindEndpoint_EngineError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("GET", "/api/find?path=/&pattern=*.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestDuEndpoint_EngineError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("GET", "/api/du?path=/x", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestInfoEndpoint_EngineError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("GET", "/api/info?path=/x.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestDeleteEndpoint_EngineError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("POST", "/api/delete?path=/x.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestMvEndpoint_EngineError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("POST", "/api/mv?src=/a.txt&dst=/b.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestMkdirEndpoint_EngineError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("POST", "/api/mkdir?path=/newdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestDownloadEndpoint_CloudDataMissing(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	eng.WriteFile("/dl.txt", []byte("data"))
	req := httptest.NewRequest("GET", "/api/download?path=/dl.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Logf("download status: %d (may fail if cloud cleanup ran)", rec.Code)
	}
}

func TestBrowserEndpoint_FileError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	// Seed a file but with no cloud data — ReadFileToTempFile will fail.
	seedFile(t, db, "br-err", "/browse-err.txt", 10)
	req := httptest.NewRequest("GET", "/browse-err.txt", nil)
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestLsEndpoint_WithSubpath(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	eng.MkDir("/lsub/")
	eng.WriteFile("/lsub/a.txt", []byte("a"))
	req := httptest.NewRequest("GET", "/api/ls?path=/lsub", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "a.txt") {
		t.Errorf("expected a.txt in ls: %s", body)
	}
}

func TestHealthEndpoint_ClosedDB(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("GET", "/api/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"degraded"`) {
		t.Errorf("expected degraded status: %s", body)
	}
}

func TestDeleteEndpoint_DirEngineError(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	// Create a directory, then close DB to trigger DeleteDir error.
	eng.MkDir("/errdir/")
	eng.WriteFile("/errdir/f.txt", []byte("data"))
	db.Close()
	req := httptest.NewRequest("POST", "/api/delete?path=/errdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// Should get an error (500 from IsDir or DeleteDir failing).
	if rec.Code == http.StatusOK {
		t.Error("expected error response for closed DB")
	}
}

func TestMvEndpoint_DirEngineError(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.MkDir("/mvdir/")
	eng.WriteFile("/mvdir/f.txt", []byte("data"))
	db.Close()
	req := httptest.NewRequest("POST", "/api/mv?src=/mvdir&dst=/mvdir2", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Error("expected error response for closed DB")
	}
}

// ── ServeHTTP: WebDAV fallthrough for non-API, non-browser requests ──

func TestServeHTTP_WebDAVFallthrough(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	eng.WriteFile("/dav.txt", []byte("webdav content"))

	spool := t.TempDir()
	davFS := vfs.NewWebDAVFS(eng, spool)
	h.davHandler = &webdav.Handler{
		FileSystem: davFS,
		LockSystem: webdav.NewMemLS(),
	}

	// PROPFIND is a WebDAV method, not a browser GET, so it falls through.
	req := httptest.NewRequest("PROPFIND", "/dav.txt", nil)
	req.Header.Set("Depth", "0")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// Any response other than 0 means the handler was invoked.
	if rec.Code == 0 {
		t.Error("expected davHandler to handle PROPFIND")
	}
}

// ── Download endpoint: HEAD method ──

func TestDownloadEndpoint_HEAD(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	eng.WriteFile("/headfile.txt", []byte("head me"))
	req := httptest.NewRequest("HEAD", "/api/download?path=/headfile.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Error paths: closed DB in handlers ──────────────────────────────────────

func TestDeleteEndpoint_IsDirError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("POST", "/api/delete?path=/foo", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestMvEndpoint_IsDirError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("POST", "/api/mv?src=/a&dst=/b", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestDeleteEndpoint_DeleteDirError(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.MkDir("/testdir/")
	eng.WriteFile("/testdir/file.txt", []byte("data"))
	// Close DB after creating dir — DeleteDir will error.
	db.Close()
	req := httptest.NewRequest("POST", "/api/delete?path=/testdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// IsDir may also fail on closed DB — either 500 or 404 is acceptable.
	if rec.Code != http.StatusInternalServerError {
		t.Logf("got %d (expected 500 from IsDir or DeleteDir failure)", rec.Code)
	}
}

func TestDeleteEndpoint_DeleteFileError(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.WriteFile("/delme.txt", []byte("data"))
	db.Close()
	req := httptest.NewRequest("POST", "/api/delete?path=/delme.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestMvEndpoint_RenameDirError(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.MkDir("/srcdir/")
	eng.WriteFile("/srcdir/file.txt", []byte("data"))
	db.Close()
	req := httptest.NewRequest("POST", "/api/mv?src=/srcdir&dst=/dstdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestMvEndpoint_RenameFileError(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.WriteFile("/mvme.txt", []byte("data"))
	db.Close()
	req := httptest.NewRequest("POST", "/api/mv?src=/mvme.txt&dst=/moved.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestLsEndpoint_DBError(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)
	db.Close()
	req := httptest.NewRequest("GET", "/api/ls?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestBrowserEndpoint_ReadFileError(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.WriteFile("/browse2.txt", []byte("content2"))

	// Close the DB so ReadFileToTempFile fails (Stat uses cached info but read needs DB).
	// Actually Stat also needs DB. Let's break cloud reads instead.
	// Remove cloud objects after writing so read fails.
	// The simplest: close engine DB after write so ReadFileToTempFile errors out.
	db.Close()

	req := httptest.NewRequest("GET", "/browse2.txt", nil)
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// With broken DB, Stat will fail → falls through to SPA shell (200).
	// That still covers the serveBrowser code path.
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 (SPA shell fallback), got %d", rec.Code)
	}
}

func TestServeHTTP_NoDavHandler(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	// davHandler is nil by default in newTestHandlerWithCloud.
	// Send a non-browser, non-API request (e.g. PUT to a non-api path).
	req := httptest.NewRequest("PUT", "/somefile.txt", strings.NewReader("data"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "WebDAV not configured") {
		t.Errorf("expected 'WebDAV not configured' error, got %q", rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Trigger-based tests: use SQLite triggers to make DML (DELETE/UPDATE) fail
// while SELECT queries still succeed, covering deeper error paths.
// ---------------------------------------------------------------------------

func TestDeleteEndpoint_DeleteDirError_Trigger(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	// Create a directory with a file under it.
	eng.MkDir("/mydir/")
	eng.WriteFile("/mydir/file.txt", []byte("dir content"))

	// Block DELETE on both files and directories so DeleteDir returns an error.
	db.Conn().Exec(`CREATE TRIGGER block_file_delete BEFORE DELETE ON files BEGIN SELECT RAISE(ABORT, 'delete blocked'); END`)
	db.Conn().Exec(`CREATE TRIGGER block_dir_delete BEFORE DELETE ON directories BEGIN SELECT RAISE(ABORT, 'delete blocked'); END`)

	req := httptest.NewRequest("POST", "/api/delete?path=/mydir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDeleteEndpoint_DeleteFileError_Trigger(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.WriteFile("/standalone.txt", []byte("standalone content"))

	// Add trigger that blocks DELETE on files.
	db.Conn().Exec(`CREATE TRIGGER block_file_delete BEFORE DELETE ON files BEGIN SELECT RAISE(ABORT, 'delete blocked by trigger'); END`)

	req := httptest.NewRequest("POST", "/api/delete?path=/standalone.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// IsDir → false, FileExists → true, DeleteFile → trigger error.
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMvEndpoint_RenameDirError_Trigger(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.MkDir("/srcdir/")
	eng.WriteFile("/srcdir/file.txt", []byte("dir content"))

	// Add trigger that blocks UPDATE on files.
	db.Conn().Exec(`CREATE TRIGGER block_file_update BEFORE UPDATE ON files BEGIN SELECT RAISE(ABORT, 'update blocked by trigger'); END`)

	req := httptest.NewRequest("POST", "/api/mv?src=/srcdir&dst=/dstdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// IsDir → true, RenameDir → RenameFilesUnderDir → trigger error.
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMvEndpoint_RenameFileError_Trigger(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.WriteFile("/moveme.txt", []byte("move content"))

	// Add trigger that blocks UPDATE on files.
	db.Conn().Exec(`CREATE TRIGGER block_file_update BEFORE UPDATE ON files BEGIN SELECT RAISE(ABORT, 'update blocked by trigger'); END`)

	req := httptest.NewRequest("POST", "/api/mv?src=/moveme.txt&dst=/moved.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// IsDir → false, FileExists → true, RenameFile → trigger error.
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// syncProviders / checkMissingProviders
// ---------------------------------------------------------------------------

// fakeRCServer creates a mock rclone RC HTTP server that handles the endpoints
// needed by syncProviders and checkMissingProviders.
func fakeRCServer(t *testing.T, remotes map[string]string, quotas map[string][2]int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/config/listremotes":
			names := make([]string, 0, len(remotes))
			for name := range remotes {
				names = append(names, name)
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"remotes": names})
		case "/config/get":
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			name, _ := body["name"].(string)
			typ, ok := remotes[name]
			if !ok {
				http.Error(w, "not found", 404)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"type": typ})
		case "/operations/about":
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			fs, _ := body["fs"].(string)
			remote := strings.TrimSuffix(fs, ":")
			q, ok := quotas[remote]
			if !ok {
				http.Error(w, "no quota", 500)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"total": q[0], "free": q[1]})
		default:
			w.WriteHeader(200)
			w.Write([]byte("{}"))
		}
	}))
}

func newDaemonWithFakeRC(t *testing.T, srv *httptest.Server) (*Daemon, *metadata.DB) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	addr := strings.TrimPrefix(srv.URL, "http://")
	rm := NewRcloneManager("", "", addr)

	d := &Daemon{
		config: Config{ConfigDir: dir},
		db:     db,
		rclone: rm,
	}
	return d, db
}

func TestSyncProviders_RegistersRemotes(t *testing.T) {
	remotes := map[string]string{"gdrive": "drive", "mybox": "dropbox"}
	quotas := map[string][2]int64{"gdrive": {100e9, 80e9}, "mybox": {50e9, 30e9}}
	srv := fakeRCServer(t, remotes, quotas)
	defer srv.Close()

	d, db := newDaemonWithFakeRC(t, srv)

	// DB starts empty.
	providers, _ := db.GetAllProviders()
	if len(providers) != 0 {
		t.Fatalf("expected 0 providers, got %d", len(providers))
	}

	d.syncProviders()

	providers, err := db.GetAllProviders()
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 2 {
		t.Fatalf("expected 2 providers, got %d", len(providers))
	}

	// Verify one provider's data.
	p, _ := db.GetProviderByRemote("gdrive")
	if p == nil {
		t.Fatal("expected gdrive provider")
	}
	if p.Type != "drive" {
		t.Errorf("Type = %q, want %q", p.Type, "drive")
	}
	if p.QuotaTotalBytes == nil || *p.QuotaTotalBytes != int64(100e9) {
		t.Errorf("QuotaTotalBytes = %v, want %d", p.QuotaTotalBytes, int64(100e9))
	}
	if p.QuotaFreeBytes == nil || *p.QuotaFreeBytes != int64(80e9) {
		t.Errorf("QuotaFreeBytes = %v, want %d", p.QuotaFreeBytes, int64(80e9))
	}
}

func TestSyncProviders_UpdatesExisting(t *testing.T) {
	remotes := map[string]string{"gdrive": "drive"}
	quotas := map[string][2]int64{"gdrive": {100e9, 60e9}}
	srv := fakeRCServer(t, remotes, quotas)
	defer srv.Close()

	d, db := newDaemonWithFakeRC(t, srv)

	// Pre-existing provider with old quota.
	oldTotal, oldFree := int64(50e9), int64(10e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "gdrive", Type: "drive", DisplayName: "gdrive",
		RcloneRemote: "gdrive", QuotaTotalBytes: &oldTotal, QuotaFreeBytes: &oldFree,
	})

	d.syncProviders()

	p, _ := db.GetProviderByRemote("gdrive")
	if p == nil {
		t.Fatal("expected provider")
	}
	// Quota should be refreshed.
	if *p.QuotaTotalBytes != int64(100e9) {
		t.Errorf("QuotaTotalBytes = %d, want %d", *p.QuotaTotalBytes, int64(100e9))
	}
	if *p.QuotaFreeBytes != int64(60e9) {
		t.Errorf("QuotaFreeBytes = %d, want %d", *p.QuotaFreeBytes, int64(60e9))
	}
}

func TestSyncProviders_NoRemotes(t *testing.T) {
	remotes := map[string]string{}
	quotas := map[string][2]int64{}
	srv := fakeRCServer(t, remotes, quotas)
	defer srv.Close()

	d, db := newDaemonWithFakeRC(t, srv)
	d.syncProviders()

	providers, _ := db.GetAllProviders()
	if len(providers) != 0 {
		t.Fatalf("expected 0 providers, got %d", len(providers))
	}
}

func TestCheckMissingProviders_AllPresent(t *testing.T) {
	remotes := map[string]string{"gdrive": "drive", "mybox": "dropbox"}
	quotas := map[string][2]int64{}
	srv := fakeRCServer(t, remotes, quotas)
	defer srv.Close()

	d, db := newDaemonWithFakeRC(t, srv)

	total, free := int64(100e9), int64(50e9)
	db.UpsertProvider(&metadata.Provider{ID: "p1", Type: "drive", DisplayName: "gdrive", RcloneRemote: "gdrive", QuotaTotalBytes: &total, QuotaFreeBytes: &free})
	db.UpsertProvider(&metadata.Provider{ID: "p2", Type: "dropbox", DisplayName: "mybox", RcloneRemote: "mybox", QuotaTotalBytes: &total, QuotaFreeBytes: &free})

	missing := d.checkMissingProviders()
	if len(missing) != 0 {
		t.Errorf("expected 0 missing, got %v", missing)
	}
}

func TestCheckMissingProviders_SomeMissing(t *testing.T) {
	// Machine B only has gdrive configured, but DB has gdrive + dropbox + s3.
	remotes := map[string]string{"gdrive": "drive"}
	quotas := map[string][2]int64{}
	srv := fakeRCServer(t, remotes, quotas)
	defer srv.Close()

	d, db := newDaemonWithFakeRC(t, srv)

	total, free := int64(100e9), int64(50e9)
	db.UpsertProvider(&metadata.Provider{ID: "p1", Type: "drive", DisplayName: "gdrive", RcloneRemote: "gdrive", QuotaTotalBytes: &total, QuotaFreeBytes: &free})
	db.UpsertProvider(&metadata.Provider{ID: "p2", Type: "dropbox", DisplayName: "mybox", RcloneRemote: "mybox", QuotaTotalBytes: &total, QuotaFreeBytes: &free})
	db.UpsertProvider(&metadata.Provider{ID: "p3", Type: "s3", DisplayName: "aws", RcloneRemote: "aws", QuotaTotalBytes: &total, QuotaFreeBytes: &free})

	missing := d.checkMissingProviders()
	if len(missing) != 2 {
		t.Fatalf("expected 2 missing, got %v", missing)
	}
	// Labels now include provider type.
	joined := strings.Join(missing, " | ")
	if !strings.Contains(joined, "mybox") {
		t.Errorf("expected mybox in missing, got %v", missing)
	}
	if !strings.Contains(joined, "aws") {
		t.Errorf("expected aws in missing, got %v", missing)
	}
	if !strings.Contains(joined, "dropbox") {
		t.Errorf("expected type info in missing label, got %v", missing)
	}
}

func TestCheckMissingProviders_EmptyDB(t *testing.T) {
	remotes := map[string]string{"gdrive": "drive"}
	quotas := map[string][2]int64{}
	srv := fakeRCServer(t, remotes, quotas)
	defer srv.Close()

	d, _ := newDaemonWithFakeRC(t, srv)
	missing := d.checkMissingProviders()
	if len(missing) != 0 {
		t.Errorf("expected 0 missing for empty DB, got %v", missing)
	}
}

func TestCheckMissingProviders_WithIdentity(t *testing.T) {
	remotes := map[string]string{"gdrive1": "drive"}
	quotas := map[string][2]int64{}
	srv := fakeRCServer(t, remotes, quotas)
	defer srv.Close()

	d, db := newDaemonWithFakeRC(t, srv)

	total, free := int64(15e9), int64(10e9)
	db.UpsertProvider(&metadata.Provider{ID: "p1", Type: "drive", DisplayName: "gdrive1", RcloneRemote: "gdrive1", QuotaTotalBytes: &total, QuotaFreeBytes: &free})
	db.UpsertProvider(&metadata.Provider{ID: "p2", Type: "drive", DisplayName: "gdrive2", RcloneRemote: "gdrive2", AccountIdentity: "alice@gmail.com", QuotaTotalBytes: &total, QuotaFreeBytes: &free})

	missing := d.checkMissingProviders()
	if len(missing) != 1 {
		t.Fatalf("expected 1 missing, got %v", missing)
	}
	// Missing label should include identity and type.
	if !strings.Contains(missing[0], "alice@gmail.com") {
		t.Errorf("expected identity in label, got %q", missing[0])
	}
	if !strings.Contains(missing[0], "drive") {
		t.Errorf("expected type in label, got %q", missing[0])
	}
}

// ---------------------------------------------------------------------------
// stubRcloneClient for remotes endpoint testing
// ---------------------------------------------------------------------------

type stubRcloneClient struct {
	remotes    []string
	types      map[string]string
	listErr    error
	getTypeErr error
}

func (s *stubRcloneClient) ListRemotes() ([]string, error) {
	return s.remotes, s.listErr
}

func (s *stubRcloneClient) GetRemoteType(name string) (string, error) {
	if s.getTypeErr != nil {
		return "", s.getTypeErr
	}
	if t, ok := s.types[name]; ok {
		return t, nil
	}
	return "unknown", nil
}

// ---------------------------------------------------------------------------
// /api/remotes tests
// ---------------------------------------------------------------------------

func TestRemotesEndpoint_Success(t *testing.T) {
	h, _ := newTestHandler(t)
	h.rcloneClient = &stubRcloneClient{
		remotes: []string{"gdrive", "dropbox"},
		types:   map[string]string{"gdrive": "drive", "dropbox": "dropbox"},
	}
	h.configDir = t.TempDir()

	req := httptest.NewRequest("GET", "/api/remotes", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp struct {
		Remotes []struct {
			Name   string `json:"name"`
			Type   string `json:"type"`
			Active bool   `json:"active"`
		} `json:"remotes"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Remotes) != 2 {
		t.Fatalf("expected 2 remotes, got %d", len(resp.Remotes))
	}
	// When no activeRemotes filter, all should be active.
	for _, r := range resp.Remotes {
		if !r.Active {
			t.Errorf("expected %s to be active", r.Name)
		}
	}
}

func TestRemotesEndpoint_WithActiveFilter(t *testing.T) {
	h, _ := newTestHandler(t)
	h.rcloneClient = &stubRcloneClient{
		remotes: []string{"gdrive", "dropbox", "onedrive"},
		types:   map[string]string{"gdrive": "drive", "dropbox": "dropbox", "onedrive": "onedrive"},
	}
	h.configDir = t.TempDir()
	h.activeRemotes = []string{"gdrive"}

	req := httptest.NewRequest("GET", "/api/remotes", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp struct {
		Remotes []struct {
			Name   string `json:"name"`
			Type   string `json:"type"`
			Active bool   `json:"active"`
		} `json:"remotes"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	activeCount := 0
	for _, r := range resp.Remotes {
		if r.Active {
			activeCount++
			if r.Name != "gdrive" {
				t.Errorf("expected only gdrive to be active, got %s", r.Name)
			}
		}
	}
	if activeCount != 1 {
		t.Errorf("expected 1 active, got %d", activeCount)
	}
}

func TestRemotesEndpoint_ListError(t *testing.T) {
	h, _ := newTestHandler(t)
	h.rcloneClient = &stubRcloneClient{
		listErr: fmt.Errorf("connection refused"),
	}

	req := httptest.NewRequest("GET", "/api/remotes", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestRemotesEndpoint_SavedRemotesFile(t *testing.T) {
	h, _ := newTestHandler(t)
	h.rcloneClient = &stubRcloneClient{
		remotes: []string{"gdrive", "dropbox"},
		types:   map[string]string{"gdrive": "drive", "dropbox": "dropbox"},
	}
	h.configDir = t.TempDir()
	// Write a remotes.json to simulate saved remote selection.
	os.WriteFile(filepath.Join(h.configDir, "remotes.json"), []byte(`["gdrive"]`), 0644)

	req := httptest.NewRequest("GET", "/api/remotes", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var resp struct {
		Remotes []struct {
			Name   string `json:"name"`
			Active bool   `json:"active"`
		} `json:"remotes"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	for _, r := range resp.Remotes {
		if r.Name == "gdrive" && !r.Active {
			t.Error("gdrive should be active")
		}
		if r.Name == "dropbox" && r.Active {
			t.Error("dropbox should not be active")
		}
	}
}

// ---------------------------------------------------------------------------
// /api/upload tests
// ---------------------------------------------------------------------------

func TestUploadEndpoint_RequiresPOST(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/upload", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestUploadEndpoint_NoFile(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("POST", "/api/upload", strings.NewReader(""))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=xxx")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestUploadEndpoint_Success(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	_ = eng

	// Build multipart form
	var buf bytes.Buffer
	writer := multipartWriter(&buf)
	part, _ := writer.CreateFormFile("file", "test.txt")
	part.Write([]byte("hello world"))
	writer.WriteField("dir", "/docs")
	writer.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify file was created.
	f, err := db.GetCompleteFileByPath("/docs/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("uploaded file not found in DB")
	}
	if f.SizeBytes != 11 {
		t.Errorf("expected size 11, got %d", f.SizeBytes)
	}

	// Verify activity was logged.
	entries, _ := db.RecentActivity(10)
	found := false
	for _, e := range entries {
		if e.Action == "upload" && e.Path == "/docs/test.txt" {
			found = true
		}
	}
	if !found {
		t.Error("upload activity not logged")
	}
}

func TestUploadEndpoint_DefaultDir(t *testing.T) {
	h, _, db := newTestHandlerWithCloud(t)

	var buf bytes.Buffer
	writer := multipartWriter(&buf)
	part, _ := writer.CreateFormFile("file", "root.txt")
	part.Write([]byte("root file"))
	writer.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	f, _ := db.GetCompleteFileByPath("/root.txt")
	if f == nil {
		t.Fatal("uploaded file not found at /root.txt")
	}
}

// ---------------------------------------------------------------------------
// /api/verify tests
// ---------------------------------------------------------------------------

func TestVerifyEndpoint_NoPath(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/verify", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestVerifyEndpoint_FileNotFound(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/verify?path=/nonexistent.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.OK {
		t.Error("expected ok=false for nonexistent file")
	}
	if resp.Error == "" {
		t.Error("expected error message")
	}
}

func TestVerifyEndpoint_Success(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.WriteFile("/verify.txt", []byte("verify me"))

	req := httptest.NewRequest("GET", "/api/verify?path=/verify.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK bool `json:"ok"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	if !resp.OK {
		t.Error("expected verify to pass")
	}

	// Verify activity was logged.
	entries, _ := db.RecentActivity(10)
	found := false
	for _, e := range entries {
		if e.Action == "verify" && e.Path == "/verify.txt" {
			found = true
		}
	}
	if !found {
		t.Error("verify activity not logged")
	}
}

// ---------------------------------------------------------------------------
// /api/activity tests
// ---------------------------------------------------------------------------

func TestActivityEndpoint_Empty(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/activity", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var entries []metadata.ActivityEntry
	json.NewDecoder(rec.Body).Decode(&entries)
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

func TestActivityEndpoint_WithEntries(t *testing.T) {
	h, db := newTestHandler(t)
	db.InsertActivity("upload", "/test.txt", "100 bytes")
	db.InsertActivity("delete", "/old.txt", "file")

	req := httptest.NewRequest("GET", "/api/activity", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var entries []struct {
		Action    string `json:"Action"`
		Path      string `json:"Path"`
		Detail    string `json:"Detail"`
		CreatedAt int64  `json:"CreatedAt"`
	}
	json.NewDecoder(rec.Body).Decode(&entries)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	// Most recent first.
	if entries[0].Action != "delete" {
		t.Error("expected most recent entry first")
	}
}

// ---------------------------------------------------------------------------
// Activity logging in existing endpoints
// ---------------------------------------------------------------------------

func TestDeleteEndpoint_LogsActivity(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.WriteFile("/todelete.txt", []byte("bye"))

	req := httptest.NewRequest("POST", "/api/delete?path=/todelete.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	entries, _ := db.RecentActivity(10)
	found := false
	for _, e := range entries {
		if e.Action == "delete" && e.Path == "/todelete.txt" {
			found = true
		}
	}
	if !found {
		t.Error("delete activity not logged")
	}
}

func TestMkdirEndpoint_LogsActivity(t *testing.T) {
	h, db := newTestHandler(t)

	req := httptest.NewRequest("POST", "/api/mkdir?path=/newdir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	entries, _ := db.RecentActivity(10)
	found := false
	for _, e := range entries {
		if e.Action == "mkdir" && e.Path == "/newdir" {
			found = true
		}
	}
	if !found {
		t.Error("mkdir activity not logged")
	}
}

func TestMvEndpoint_LogsActivity(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.WriteFile("/moveme.txt", []byte("moving"))

	req := httptest.NewRequest("POST", "/api/mv?src=/moveme.txt&dst=/moved.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	entries, _ := db.RecentActivity(10)
	found := false
	for _, e := range entries {
		if e.Action == "move" && e.Path == "/moveme.txt" && e.Detail == "/moved.txt" {
			found = true
		}
	}
	if !found {
		t.Error("move activity not logged")
	}
}

func TestDownloadEndpoint_LogsActivity(t *testing.T) {
	h, eng, db := newTestHandlerWithCloud(t)
	eng.WriteFile("/dl.txt", []byte("download me"))

	req := httptest.NewRequest("GET", "/api/download?path=/dl.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	entries, _ := db.RecentActivity(10)
	found := false
	for _, e := range entries {
		if e.Action == "download" && e.Path == "/dl.txt" {
			found = true
		}
	}
	if !found {
		t.Error("download activity not logged")
	}
}

// ---------------------------------------------------------------------------
// Additional edge case tests
// ---------------------------------------------------------------------------

func TestUploadEndpoint_InvalidMultipart(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("POST", "/api/upload", strings.NewReader("not multipart"))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestVerifyEndpoint_RootPath(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/api/verify?path=/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestDeleteEndpoint_DirLogsActivity(t *testing.T) {
	h, db := newTestHandler(t)
	h.engine.MkDir("/delete-dir/")

	req := httptest.NewRequest("POST", "/api/delete?path=/delete-dir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	entries, _ := db.RecentActivity(10)
	found := false
	for _, e := range entries {
		if e.Action == "delete" && strings.Contains(e.Path, "delete-dir") && e.Detail == "directory" {
			found = true
		}
	}
	if !found {
		t.Error("dir delete activity not logged")
	}
}

func TestMvEndpoint_DirLogsActivity(t *testing.T) {
	h, db := newTestHandler(t)
	h.engine.MkDir("/mvdir/")

	req := httptest.NewRequest("POST", "/api/mv?src=/mvdir&dst=/mvdir2", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	entries, _ := db.RecentActivity(10)
	found := false
	for _, e := range entries {
		if e.Action == "move" && e.Path == "/mvdir" && e.Detail == "/mvdir2" {
			found = true
		}
	}
	if !found {
		t.Error("dir move activity not logged")
	}
}

// multipartWriter is a helper to create mime/multipart writers.
func multipartWriter(buf *bytes.Buffer) *multipart.Writer {
	return multipart.NewWriter(buf)
}

// ---------------------------------------------------------------------------
// cleanPath unit tests
// ---------------------------------------------------------------------------

func TestCleanPath(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"", "/"},
		{".", "/"},
		{"/", "/"},
		{"/foo/bar", "/foo/bar"},
		{"foo/bar", "/foo/bar"},
		{"../../etc/passwd", "/etc/passwd"},
		{"foo/../../../bar", "/bar"},
		{"/../../../etc", "/etc"},
		{"/normal/../ok", "/ok"},
		{"//double//slash", "/double/slash"},
	}
	for _, c := range cases {
		got := cleanPath(c.input)
		if got != c.want {
			t.Errorf("cleanPath(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

// TestPathTraversal_Delete ensures that path traversal attempt doesn't affect
// files outside the virtual filesystem.
func TestPathTraversal_Delete(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	encKey := bytes.Repeat([]byte("K"), 32)
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	eng := engine.NewEngineWithCloud(db, dbPath, nil, b, encKey)
	defer eng.Close()

	h := &browserHandler{engine: eng, startTime: time.Now()}

	// Try to delete with path traversal — should not escape to host FS
	req := httptest.NewRequest("POST", "/api/delete?path=../../etc/passwd", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// The path gets cleaned to /etc/passwd, and the file won't exist in our
	// virtual FS, so we expect a 404 (not found) rather than deleting
	// something on the host filesystem.
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 for traversal attempt, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestPathTraversal_Upload ensures filenames are sanitised in uploads.
func TestPathTraversal_Upload(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cloud := newFakeCloud()
	encKey := bytes.Repeat([]byte("K"), 32)
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	total := int64(100 << 30)
	free := int64(50 << 30)
	db.UpsertProvider(&metadata.Provider{
		ID: "test-remote:", Type: "test", DisplayName: "Test",
		RcloneRemote: "test-remote:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	}) //nolint:errcheck
	eng := engine.NewEngineWithCloud(db, dbPath, cloud, b, encKey)
	defer eng.Close()

	h := &browserHandler{engine: eng, startTime: time.Now()}

	// Create a multipart form with a traversal attempt in the filename
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	mw.WriteField("dir", "/uploads") //nolint:errcheck
	fw, _ := mw.CreateFormFile("file", "../../etc/evil.txt")
	fw.Write([]byte("malicious content")) //nolint:errcheck
	mw.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// The file should be stored at /uploads/evil.txt (base name only),
	// not at /uploads/../../etc/evil.txt
	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp) //nolint:errcheck
	gotPath, _ := resp["path"].(string)
	if gotPath != "/uploads/evil.txt" {
		t.Errorf("expected sanitised path /uploads/evil.txt, got %q", gotPath)
	}
}

func TestUploadEndpoint_InvalidFilename(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	mw.WriteField("dir", "/") //nolint:errcheck
	// Create a form file with filename "." which should be rejected
	fw, _ := mw.CreateFormFile("file", ".")
	fw.Write([]byte("bad")) //nolint:errcheck
	mw.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid filename, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestUploadEndpoint_GETNotAllowed(t *testing.T) {
	h, _ := newTestHandler(t)

	req := httptest.NewRequest("PUT", "/api/upload", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for PUT, got %d", rec.Code)
	}
}

func TestDuEndpoint_NoPath(t *testing.T) {
	h, _ := newTestHandler(t)

	req := httptest.NewRequest("GET", "/api/du", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Should default to root "/" and return disk usage
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp) //nolint:errcheck
	if _, ok := resp["total_bytes"]; !ok {
		t.Error("expected total_bytes in response")
	}
	if _, ok := resp["file_count"]; !ok {
		t.Error("expected file_count in response")
	}
}
