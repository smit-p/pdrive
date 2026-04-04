package daemon

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/metadata"
)

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
