package engine

// Integration tests exercise multi-step engine workflows that span several
// subsystems: upload → GC, backup → restore, dedup → delete, and concurrent
// lifecycle flows.

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/metadata"
)

// ── Upload → Delete → GC removes orphaned cloud chunks ─────────────────────

func TestIntegration_UploadDeleteGCCleansChunks(t *testing.T) {
	eng, cloud := newTestEngine(t)
	content := []byte("data that will be orphaned after delete")

	if err := eng.WriteFileStream("/gc-target.txt", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatal(err)
	}

	// Verify chunks exist in cloud.
	chunksBeforeDelete := countNonMeta(cloud)
	if chunksBeforeDelete == 0 {
		t.Fatal("expected cloud chunks after upload")
	}

	// Delete the file — this marks chunks for deletion.
	if err := eng.DeleteFile("/gc-target.txt"); err != nil {
		t.Fatal(err)
	}

	// Run GC to clean up orphaned cloud objects.
	eng.GCOrphanedChunks()

	// Cloud should have fewer objects (chunks removed).
	chunksAfterGC := countNonMeta(cloud)
	if chunksAfterGC >= chunksBeforeDelete {
		t.Errorf("GC didn't clean up: before=%d, after=%d", chunksBeforeDelete, chunksAfterGC)
	}
}

// ── Upload → Backup → New engine from backup → Read ─────────────────────────

func TestIntegration_BackupRestoreReadFile(t *testing.T) {
	// Create first engine, upload a file, backup.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	total, free := int64(100e9), int64(99e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "p1", Type: "drive", DisplayName: "TestDrive",
		RcloneRemote: "fake:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	eng := NewEngineWithCloud(db, dbPath, cloud, b)

	content := []byte("backup round-trip content 123")
	if err := eng.WriteFileStream("/backup-test.txt", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatal(err)
	}

	// Trigger backup.
	if err := eng.BackupDB(); err != nil {
		t.Fatal(err)
	}
	eng.Close()
	db.Close()

	// Verify backup exists in cloud.
	cloud.mu.Lock()
	var backupKey string
	for k := range cloud.objects {
		if strings.Contains(k, "pdrive-meta/") {
			backupKey = k
		}
	}
	cloud.mu.Unlock()
	if backupKey == "" {
		t.Fatal("backup not found in cloud")
	}

	// Create a fresh DB path and restore the backup into it.
	dbPath2 := filepath.Join(dir, "restored.db")

	// Restore from backup: read the bytes, parse, write to DB file.
	rc, err := cloud.GetFile("fake:", "pdrive-meta/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	backupBytes, _ := io.ReadAll(rc)
	rc.Close()

	// Parse backup payload.
	_, dbData, ok := ParseBackupPayload(backupBytes)
	if !ok {
		t.Fatal("bad backup payload")
	}
	// Write restored DB file before opening it.
	if err := os.WriteFile(dbPath2, dbData, 0600); err != nil {
		t.Fatal(err)
	}

	// Open the restored DB.
	db2, err := metadata.Open(dbPath2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db2.Close() })

	b2 := broker.NewBroker(db2, broker.PolicyPFRD, 0)
	eng2 := NewEngineWithCloud(db2, dbPath2, cloud, b2)
	t.Cleanup(eng2.Close)

	// Read the file from the restored engine.
	got, err := eng2.ReadFile("/backup-test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("restored content mismatch: got %q, want %q", got, content)
	}
}

// ── Concurrent uploads to different paths ───────────────────────────────────

func TestIntegration_ConcurrentUploadsDistinctPaths(t *testing.T) {
	eng, _ := newTestEngine(t)

	var wg sync.WaitGroup
	paths := []string{"/concurrent/a.txt", "/concurrent/b.txt", "/concurrent/c.txt", "/concurrent/d.txt"}
	contents := make(map[string][]byte)
	for _, p := range paths {
		contents[p] = []byte("content for " + p)
	}

	for _, p := range paths {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			data := contents[path]
			if err := eng.WriteFileStream(path, bytes.NewReader(data), int64(len(data))); err != nil {
				t.Errorf("WriteFileStream %s: %v", path, err)
			}
		}(p)
	}
	wg.Wait()

	// Each file should be independently readable.
	for _, p := range paths {
		got, err := eng.ReadFile(p)
		if err != nil {
			t.Errorf("ReadFile %s: %v", p, err)
			continue
		}
		if !bytes.Equal(got, contents[p]) {
			t.Errorf("content mismatch for %s: got %q", p, got)
		}
	}
}

// ── Deep directory structure: create, list, rename, delete ──────────────────

func TestIntegration_DeepDirectoryLifecycle(t *testing.T) {
	eng, _ := newTestEngine(t)

	// Create nested structure.
	eng.MkDir("/a")
	eng.MkDir("/a/b")
	eng.MkDir("/a/b/c")
	eng.WriteFile("/a/b/c/deep.txt", []byte("deep file"))
	eng.WriteFile("/a/b/mid.txt", []byte("mid file"))
	eng.WriteFile("/a/top.txt", []byte("top file"))

	// ListDir at each level.
	_, dirs, err := eng.ListDir("/a")
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 1 || dirs[0] != "b" {
		t.Errorf("ListDir /a dirs = %v, want [b]", dirs)
	}

	files, _, err := eng.ListDir("/a/b/c")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Errorf("ListDir /a/b/c files = %d, want 1", len(files))
	}

	// Rename directory.
	if err := eng.RenameDir("/a/b", "/a/renamed"); err != nil {
		t.Fatal(err)
	}

	// Old path should be gone, new path should work.
	_, err = eng.ReadFile("/a/renamed/c/deep.txt")
	if err != nil {
		t.Errorf("renamed deep file: %v", err)
	}
	_, err = eng.ReadFile("/a/renamed/mid.txt")
	if err != nil {
		t.Errorf("renamed mid file: %v", err)
	}
	_, err = eng.ReadFile("/a/b/c/deep.txt")
	if err == nil {
		t.Error("old path should not exist after rename")
	}

	// Delete the whole tree.
	if err := eng.DeleteDir("/a"); err != nil {
		t.Fatal(err)
	}

	// Everything should be gone.
	for _, p := range []string{"/a/top.txt", "/a/renamed/mid.txt", "/a/renamed/c/deep.txt"} {
		exists, _ := eng.FileExists(p)
		if exists {
			t.Errorf("file %s still exists after DeleteDir /a", p)
		}
	}
}

// ── Dedup → delete original → delete clone → GC cleans all ─────────────────

func TestIntegration_DedupDeleteBothThenGC(t *testing.T) {
	eng, cloud := newTestEngine(t)
	content := []byte("shared chunk data for dedup GC test")

	eng.WriteFileStream("/dedup-gc/first.txt", bytes.NewReader(content), int64(len(content)))
	eng.WriteFileStream("/dedup-gc/second.txt", bytes.NewReader(content), int64(len(content)))

	chunksBefore := countNonMeta(cloud)

	// Delete both files.
	eng.DeleteFile("/dedup-gc/first.txt")
	eng.DeleteFile("/dedup-gc/second.txt")

	// GC should remove orphaned chunks.
	eng.GCOrphanedChunks()

	chunksAfterGC := countNonMeta(cloud)

	if chunksAfterGC >= chunksBefore {
		t.Errorf("GC didn't clean chunks after both dedup files deleted: before=%d, after=%d",
			chunksBefore, chunksAfterGC)
	}
}

// ── EnsureRemoteDirs creates missing remote directories ─────────────────────

func TestIntegration_EnsureRemoteDirs(t *testing.T) {
	eng, _ := newTestEngine(t)

	// EnsureRemoteDirs is a no-op on our fakeCloud but should not panic.

	eng.EnsureRemoteDirs()

	// After EnsureRemoteDirs, we should be able to upload and list without error.
	content := []byte("after ensure dirs")
	if err := eng.WriteFileStream("/ensure-test.txt", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("write after EnsureRemoteDirs: %v", err)
	}
	got, err := eng.ReadFile("/ensure-test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch")
	}
}

// ── Search across many files ────────────────────────────────────────────────

func TestIntegration_SearchAcrossFiles(t *testing.T) {
	eng, _ := newTestEngine(t)

	files := map[string][]byte{
		"/docs/readme.md":     []byte("# Readme"),
		"/docs/guide.md":      []byte("# Guide"),
		"/src/main.go":        []byte("package main"),
		"/src/util.go":        []byte("package main"),
		"/images/photo.jpg":   []byte{0xFF, 0xD8},
		"/images/logo.png":    []byte{0x89, 0x50},
		"/project/readme.txt": []byte("project readme"),
	}
	for p, data := range files {
		eng.WriteFile(p, data)
	}

	// Search for "readme".
	results, err := eng.SearchFiles("/", "readme")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("search 'readme': expected 2 results, got %d", len(results))
	}

	// Search for ".go".
	results, err = eng.SearchFiles("/", ".go")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("search '.go': expected 2 results, got %d", len(results))
	}

	// DiskUsage.
	fileCount, totalBytes, err := eng.DiskUsage("/")
	if err != nil {
		t.Fatal(err)
	}
	if fileCount != 7 {
		t.Errorf("DiskUsage files = %d, want 7", fileCount)
	}
	if totalBytes <= 0 {
		t.Error("DiskUsage bytes should be > 0")
	}
}

// ── Failed deletion retry ───────────────────────────────────────────────────

func TestIntegration_FailedDeletionRetrySucceeds(t *testing.T) {
	eng, cloud := newTestEngine(t)
	content := []byte("retry delete test")

	eng.WriteFileStream("/retry.txt", bytes.NewReader(content), int64(len(content)))

	// Make cloud delete fail.
	cloud.mu.Lock()
	cloud.deleteErr = os.ErrPermission
	cloud.mu.Unlock()

	// Delete file — cloud delete will fail and be queued.
	eng.DeleteFile("/retry.txt")

	// Fix cloud.
	cloud.mu.Lock()
	cloud.deleteErr = nil
	cloud.mu.Unlock()

	// Retry should succeed.
	eng.RetryFailedDeletions()

	// File should be gone from DB.
	exists, _ := eng.FileExists("/retry.txt")
	if exists {
		t.Error("file should be deleted from DB")
	}
}

// ── Metrics counters track all lifecycle operations ─────────────────────────

func TestIntegration_MetricsFullLifecycle(t *testing.T) {
	eng, _ := newTestEngine(t)

	m0 := eng.Metrics()

	// Upload.
	eng.WriteFile("/metrics-test.txt", []byte("metrics data"))
	m1 := eng.Metrics()
	if m1.FilesUploaded != m0.FilesUploaded+1 {
		t.Errorf("FilesUploaded: %d -> %d", m0.FilesUploaded, m1.FilesUploaded)
	}
	if m1.ChunksUploaded <= m0.ChunksUploaded {
		t.Error("ChunksUploaded should increase")
	}

	// Dedup upload.
	eng.WriteFile("/metrics-dup.txt", []byte("metrics data"))
	m2 := eng.Metrics()
	if m2.DedupHits != m1.DedupHits+1 {
		t.Errorf("DedupHits: %d -> %d", m1.DedupHits, m2.DedupHits)
	}

	// Download.
	eng.ReadFile("/metrics-test.txt")
	m3 := eng.Metrics()
	if m3.FilesDownloaded != m2.FilesDownloaded+1 {
		t.Errorf("FilesDownloaded: %d -> %d", m2.FilesDownloaded, m3.FilesDownloaded)
	}
}

// ── StorageStatus accurate after multi-file upload ──────────────────────────

func TestIntegration_StorageStatusMultiFile(t *testing.T) {
	eng, _ := newTestEngine(t)

	eng.WriteFile("/s1.txt", []byte("aaaa"))
	eng.WriteFile("/s2.txt", []byte("bbbb"))
	eng.WriteFile("/sub/s3.txt", []byte("cccc"))

	status, err := eng.StorageStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.TotalFiles != 3 {
		t.Errorf("TotalFiles = %d, want 3", status.TotalFiles)
	}
	if status.TotalBytes <= 0 {
		t.Error("TotalBytes should be > 0")
	}
}

// ── GetFileInfo returns complete file details ───────────────────────────────

func TestIntegration_FileInfoComplete(t *testing.T) {
	eng, _ := newTestEngine(t)

	eng.WriteFile("/info-test.txt", []byte("file info integration test"))

	info, err := eng.GetFileInfo("/info-test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.File.VirtualPath != "/info-test.txt" {
		t.Errorf("VirtualPath = %q", info.File.VirtualPath)
	}
	if info.File.SizeBytes != 26 {
		t.Errorf("SizeBytes = %d, want 26", info.File.SizeBytes)
	}
	if info.File.SHA256Full == "" {
		t.Error("SHA256Full should not be empty")
	}
	if len(info.Chunks) < 1 {
		t.Errorf("Chunks = %d, want >= 1", len(info.Chunks))
	}
	if len(info.Chunks[0].Providers) == 0 {
		t.Error("chunk should have at least one provider")
	}
}
