package engine

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/rclonerc"
)

// ── fake cloud ──────────────────────────────────────────────────────────────

// fakeCloud is an in-memory cloudStorage implementation for tests.
// It stores uploaded bytes in a map keyed by "remote:path".
type fakeCloud struct {
	mu      sync.Mutex
	objects map[string][]byte
	// putErr, if non-nil, is returned for every PutFile call.
	putErr error
}

func newFakeCloud() *fakeCloud {
	return &fakeCloud{objects: make(map[string][]byte)}
}

func (f *fakeCloud) key(remote, path string) string { return remote + ":" + path }

func (f *fakeCloud) PutFile(remote, path string, r io.Reader) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErr != nil {
		return f.putErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.objects[f.key(remote, path)] = data
	return nil
}

func (f *fakeCloud) GetFile(remote, path string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objects[f.key(remote, path)]
	if !ok {
		return nil, fmt.Errorf("object not found: %s/%s", remote, path)
	}
	return data, nil
}

func (f *fakeCloud) DeleteFile(remote, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, f.key(remote, path))
	return nil
}

func (f *fakeCloud) ListDir(remote, path string) ([]rclonerc.ListItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := remote + ":" + path + "/"
	var items []rclonerc.ListItem
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			name := strings.TrimPrefix(k, prefix)
			if !strings.Contains(name, "/") {
				items = append(items, rclonerc.ListItem{Name: name, Path: path + "/" + name})
			}
		}
	}
	return items, nil
}

// ── test helpers ─────────────────────────────────────────────────────────────

// newTestEngine creates a fully wired Engine backed by a temp-dir SQLite DB
// and fakeCloud. encKey is 32 zero bytes (fine for tests).
func newTestEngine(t *testing.T) (*Engine, *fakeCloud) {
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
		ID:              "p1",
		Type:            "drive",
		DisplayName:     "TestDrive",
		RcloneRemote:    "fake:",
		QuotaTotalBytes: &total,
		QuotaFreeBytes:  &free,
	})

	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32) // all-zero key — fine for tests

	eng := &Engine{
		db:              db,
		dbPath:          dbPath,
		rc:              cloud,
		broker:          b,
		encKey:          encKey,
		maxChunkRetries: 1, // no retries in tests — avoids long backoff delays
		uploadTokens:    make(chan struct{}, uploadRateBurst+100),
		uploads:         make(map[string]*uploadProgress),
	}
	// Pre-fill all tokens so uploads never block on the rate limiter in tests.
	for i := 0; i < uploadRateBurst+100; i++ {
		eng.uploadTokens <- struct{}{}
	}
	return eng, cloud
}

// writeTmpFile writes data to a temp file, rewinds it, and returns the open
// *os.File plus its path. The file is scheduled for cleanup via t.Cleanup.
func writeTmpFile(t *testing.T, data []byte) (*os.File, string) {
	t.Helper()
	f, err := os.CreateTemp("", "pdrive-test-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	return f, f.Name()
}

// ── tests ────────────────────────────────────────────────────────────────────

// TestWriteFileStream_SmallFile verifies a synchronous write round-trips correctly.
func TestWriteFileStream_SmallFile(t *testing.T) {
	eng, _ := newTestEngine(t)
	content := []byte("hello pdrive")

	if err := eng.WriteFileStream("/hello.txt", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("WriteFileStream: %v", err)
	}
	got, err := eng.ReadFile("/hello.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch: got %q, want %q", got, content)
	}
}

// TestWriteFileStream_OverwriteComplete checks that writing the same path
// twice does not cause a UNIQUE constraint violation.
func TestWriteFileStream_OverwriteComplete(t *testing.T) {
	eng, _ := newTestEngine(t)
	path := "/overwrite.txt"

	if err := eng.WriteFileStream(path, bytes.NewReader([]byte("v1")), 2); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := eng.WriteFileStream(path, bytes.NewReader([]byte("v222")), 4); err != nil {
		t.Fatalf("second write (overwrite): %v", err)
	}
	got, err := eng.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "v222" {
		t.Errorf("expected 'v222', got %q", got)
	}
}

// TestWriteFileStream_OverwritePending checks that a synchronous write can
// replace a stuck pending record without a UNIQUE constraint violation.
func TestWriteFileStream_OverwritePending(t *testing.T) {
	eng, _ := newTestEngine(t)
	path := "/video.mkv"

	// Simulate a stuck pending record.
	now := time.Now().Unix()
	tmp := "/tmp/stuck"
	eng.db.InsertFile(&metadata.File{
		ID: "stuck", VirtualPath: path, SizeBytes: 999,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: &tmp,
	})

	content := []byte("fresh content")
	if err := eng.WriteFileStream(path, bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("WriteFileStream over stuck pending: %v", err)
	}
	got, err := eng.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch: got %q, want %q", got, content)
	}
}

// TestWriteFileAsync_PendingRecordCreated verifies that WriteFileAsync writes
// a pending DB record synchronously (before the goroutine finishes) so that
// ResumeUploads can pick it up on daemon restart.
func TestWriteFileAsync_PendingRecordCreated(t *testing.T) {
	eng, _ := newTestEngine(t)
	content := make([]byte, 512)
	tmpFile, tmpPath := writeTmpFile(t, content)

	if err := eng.WriteFileAsync("/async.bin", tmpFile, tmpPath, int64(len(content))); err != nil {
		t.Fatalf("WriteFileAsync: %v", err)
	}

	// The record must be in the DB immediately (before the goroutine completes).
	f, err := eng.db.GetFileByPath("/async.bin")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("pending DB record must exist immediately after WriteFileAsync returns")
	}
}

// TestWriteFileAsync_OverwritePending checks that calling WriteFileAsync when
// a pending record already exists at the same path does not cause a UNIQUE
// constraint violation.
func TestWriteFileAsync_OverwritePending(t *testing.T) {
	eng, _ := newTestEngine(t)
	path := "/video.mkv"
	now := time.Now().Unix()

	// Insert a stuck pending record.
	stuckTmp := "/tmp/stuck"
	eng.db.InsertFile(&metadata.File{
		ID: "stuck", VirtualPath: path, SizeBytes: 999,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: &stuckTmp,
	})

	content := make([]byte, 512)
	tmpFile, tmpPath := writeTmpFile(t, content)
	if err := eng.WriteFileAsync(path, tmpFile, tmpPath, int64(len(content))); err != nil {
		t.Fatalf("WriteFileAsync over stuck pending: %v", err)
	}

	// New pending record should exist; old "stuck" ID should be gone.
	f, _ := eng.db.GetFileByPath(path)
	if f == nil {
		t.Fatal("new pending record must exist")
	}
	if f.ID == "stuck" {
		t.Error("old stuck record was not replaced")
	}
}

// TestWriteFileAsync_GoroutineCleanupOnUploadFailure verifies that if the
// background upload fails, the pending DB record is removed so the path is
// free for retry.
func TestWriteFileAsync_GoroutineCleanupOnUploadFailure(t *testing.T) {
	eng, cloud := newTestEngine(t)
	// All PutFile calls will fail.
	cloud.putErr = fmt.Errorf("simulated cloud error")

	content := make([]byte, 512)
	tmpFile, tmpPath := writeTmpFile(t, content)

	if err := eng.WriteFileAsync("/fail.bin", tmpFile, tmpPath, int64(len(content))); err != nil {
		t.Fatalf("WriteFileAsync returned an unexpected error: %v", err)
	}

	// Wait for the goroutine to finish and clean up.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		f, _ := eng.db.GetFileByPath("/fail.bin")
		if f == nil {
			return // pending record cleaned up — pass
		}
		if f.UploadState == "complete" {
			t.Fatal("upload should have failed but file is marked complete")
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("pending DB record was not cleaned up after upload failure")
}

// TestWriteFileAsync_CompletesAndIsReadable verifies the happy path: async
// upload finishes, record is marked complete, file is readable with correct content.
func TestWriteFileAsync_CompletesAndIsReadable(t *testing.T) {
	eng, _ := newTestEngine(t)
	content := []byte("async upload content - testing 1 2 3")
	tmpFile, tmpPath := writeTmpFile(t, content)

	if err := eng.WriteFileAsync("/async.txt", tmpFile, tmpPath, int64(len(content))); err != nil {
		t.Fatalf("WriteFileAsync: %v", err)
	}

	// Wait for the goroutine to mark the file complete.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		f, _ := eng.db.GetCompleteFileByPath("/async.txt")
		if f != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	got, err := eng.ReadFile("/async.txt")
	if err != nil {
		t.Fatalf("ReadFile after async upload: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch: got %q, want %q", got, content)
	}
}

// TestReadFile_PendingReturnsError verifies that ReadFile returns a clear error
// (not a hang or panic) when the file is still uploading.
func TestReadFile_PendingReturnsError(t *testing.T) {
	eng, _ := newTestEngine(t)
	now := time.Now().Unix()
	tmp := "/tmp/pending"
	eng.db.InsertFile(&metadata.File{
		ID: "f1", VirtualPath: "/uploading.mkv", SizeBytes: 655 * 1024 * 1024,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: &tmp,
	})

	_, err := eng.ReadFile("/uploading.mkv")
	if err == nil {
		t.Fatal("ReadFile on a pending file must return an error")
	}
	if !strings.Contains(err.Error(), "upload in progress") {
		t.Errorf("expected 'upload in progress' error, got: %v", err)
	}
}

// TestDeleteFile verifies that deleting a file removes the DB record and
// schedules cloud chunk cleanup.
func TestDeleteFile(t *testing.T) {
	eng, cloud := newTestEngine(t)
	content := []byte("delete me")

	if err := eng.WriteFileStream("/del.txt", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatal(err)
	}

	cloud.mu.Lock()
	n := len(cloud.objects)
	cloud.mu.Unlock()
	if n == 0 {
		t.Fatal("expected at least one cloud chunk after write")
	}

	if err := eng.DeleteFile("/del.txt"); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}

	// DB record gone.
	if f, _ := eng.db.GetFileByPath("/del.txt"); f != nil {
		t.Error("file record should be deleted")
	}

	// Cloud chunks cleaned up (async — give it 200ms).
	time.Sleep(200 * time.Millisecond)
	cloud.mu.Lock()
	remaining := len(cloud.objects)
	cloud.mu.Unlock()
	if remaining != 0 {
		t.Errorf("expected 0 cloud objects after delete, got %d", remaining)
	}
}

// TestRenameFile_DBOnlyNoReupload verifies that renaming a file is a pure DB
// operation and does not re-upload any chunks.
func TestRenameFile_DBOnlyNoReupload(t *testing.T) {
	eng, cloud := newTestEngine(t)
	content := []byte("rename me")

	if err := eng.WriteFileStream("/old.txt", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatal(err)
	}

	cloud.mu.Lock()
	uploadCount := len(cloud.objects)
	cloud.mu.Unlock()

	if err := eng.RenameFile("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("RenameFile: %v", err)
	}

	if f, _ := eng.db.GetFileByPath("/old.txt"); f != nil {
		t.Error("old path must not exist after rename")
	}
	if f, _ := eng.db.GetFileByPath("/new.txt"); f == nil {
		t.Error("new path must exist after rename")
	}

	// No new cloud uploads.
	cloud.mu.Lock()
	afterCount := len(cloud.objects)
	cloud.mu.Unlock()
	if afterCount != uploadCount {
		t.Errorf("rename must not upload chunks: before=%d after=%d", uploadCount, afterCount)
	}

	// Content still readable at the new path.
	got, err := eng.ReadFile("/new.txt")
	if err != nil {
		t.Fatalf("ReadFile after rename: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Error("content mismatch after rename")
	}
}

// TestListDir_HidesPendingFiles ensures ListDir does not surface pending files.
func TestListDir_HidesPendingFiles(t *testing.T) {
	eng, _ := newTestEngine(t)

	eng.WriteFileStream("/movies/complete.txt", bytes.NewReader([]byte("ok")), 2)

	now := time.Now().Unix()
	tmp := "/tmp/p"
	eng.db.InsertFile(&metadata.File{
		ID: "pend", VirtualPath: "/movies/pending.mkv", SizeBytes: 999,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: &tmp,
	})

	files, _, err := eng.ListDir("/movies")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file (complete only), got %d", len(files))
	}
}

// TestResumeUploads_FindsAndRequeues verifies that ResumeUploads picks up
// pending records whose tmp file still exists and completes them.
func TestResumeUploads_FindsAndRequeues(t *testing.T) {
	eng, _ := newTestEngine(t)

	content := []byte("resume me please")
	tmpFile, tmpPath := writeTmpFile(t, content)
	tmpFile.Close() // ResumeUploads will re-open the file

	now := time.Now().Unix()
	eng.db.InsertFile(&metadata.File{
		ID: "pend", VirtualPath: "/resume.bin", SizeBytes: int64(len(content)),
		CreatedAt: now, ModifiedAt: now, SHA256Full: "placeholder",
		UploadState: "pending", TmpPath: &tmpPath,
	})

	eng.ResumeUploads()

	// Wait for the upload goroutine to complete.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.db.GetCompleteFileByPath("/resume.bin"); f != nil {
			return // success
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("ResumeUploads did not complete the pending upload within timeout")
}
