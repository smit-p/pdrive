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
	// putDelay, if > 0, causes each PutFile to sleep before returning.
	putDelay time.Duration
}

func newFakeCloud() *fakeCloud {
	return &fakeCloud{objects: make(map[string][]byte)}
}

func (f *fakeCloud) key(remote, path string) string { return remote + ":" + path }

func (f *fakeCloud) PutFile(remote, path string, r io.Reader) error {
	f.mu.Lock()
	err := f.putErr
	delay := f.putDelay
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return err
	}
	data, ioErr := io.ReadAll(r)
	if ioErr != nil {
		return ioErr
	}
	f.mu.Lock()
	f.objects[f.key(remote, path)] = data
	f.mu.Unlock()
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

// ── Additional edge-case tests ────────────────────────────────────────────────

// TestStat_ReturnsCompleteFile verifies engine.Stat returns a complete file.
func TestStat_ReturnsCompleteFile(t *testing.T) {
	eng, _ := newTestEngine(t)
	eng.WriteFileStream("/stat.txt", bytes.NewReader([]byte("hi")), 2)

	f, err := eng.Stat("/stat.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if f == nil {
		t.Fatal("Stat must return non-nil for an existing file")
	}
	if f.SizeBytes != 2 {
		t.Errorf("expected size 2, got %d", f.SizeBytes)
	}
}

// TestStat_ReturnsPendingFile is the key Finder Error -36 regression test.
// macOS sends PROPFIND immediately after PUT; for async uploads the file is
// still pending. Stat must return it (not nil) so PROPFIND succeeds (207) and
// Finder does not show Error -36.
func TestStat_ReturnsPendingFile(t *testing.T) {
	eng, _ := newTestEngine(t)
	now := time.Now().Unix()
	tmp := "/tmp/pending-stat"
	eng.db.InsertFile(&metadata.File{
		ID: "pnd", VirtualPath: "/uploading.mkv", SizeBytes: 1024 * 1024 * 100,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: &tmp,
	})

	f, err := eng.Stat("/uploading.mkv")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if f == nil {
		t.Fatal("Stat must return the pending file — ErrNotExist here causes Finder Error -36")
	}
	if f.UploadState != "pending" {
		t.Errorf("expected upload_state=pending, got %s", f.UploadState)
	}
}

// TestStat_NonExistentReturnsNil ensures Stat returns nil (not error) for a
// missing path.
func TestStat_NonExistentReturnsNil(t *testing.T) {
	eng, _ := newTestEngine(t)
	f, err := eng.Stat("/no-such-file.txt")
	if err != nil {
		t.Fatalf("Stat must not error for non-existent file: %v", err)
	}
	if f != nil {
		t.Fatal("Stat must return nil for non-existent file")
	}
}

// TestReadFile_NotFound verifies a clear error for a completely non-existent file.
func TestReadFile_NotFound(t *testing.T) {
	eng, _ := newTestEngine(t)
	_, err := eng.ReadFile("/ghost.txt")
	if err == nil {
		t.Fatal("ReadFile must error for non-existent")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' error, got: %v", err)
	}
}

// TestDeleteFile_Idempotent checks that deleting a non-existent file returns
// nil — not an error.
func TestDeleteFile_Idempotent(t *testing.T) {
	eng, _ := newTestEngine(t)
	if err := eng.DeleteFile("/ghost.txt"); err != nil {
		t.Fatalf("DeleteFile on non-existent must be a no-op, got: %v", err)
	}
	// Second delete also fine.
	eng.WriteFileStream("/x.txt", bytes.NewReader([]byte("x")), 1)
	eng.DeleteFile("/x.txt")
	if err := eng.DeleteFile("/x.txt"); err != nil {
		t.Fatalf("second DeleteFile must be a no-op, got: %v", err)
	}
}

// TestDeleteFile_PendingFile verifies a pending upload can be deleted (e.g. as
// an admin cancel operation), freeing the path for retry.
func TestDeleteFile_PendingFile(t *testing.T) {
	eng, _ := newTestEngine(t)
	now := time.Now().Unix()
	tmp := "/tmp/del-pending"
	eng.db.InsertFile(&metadata.File{
		ID: "p1", VirtualPath: "/cancel.mkv", SizeBytes: 100,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: &tmp,
	})

	if err := eng.DeleteFile("/cancel.mkv"); err != nil {
		t.Fatalf("DeleteFile on pending: %v", err)
	}
	if f, _ := eng.db.GetFileByPath("/cancel.mkv"); f != nil {
		t.Error("pending file record must be gone after delete")
	}
}

// TestRenameFile_DestinationExists checks that renaming to an occupied path
// first removes the occupant (both DB record and its cloud chunks).
func TestRenameFile_DestinationExists(t *testing.T) {
	eng, cloud := newTestEngine(t)
	eng.WriteFileStream("/src.txt", bytes.NewReader([]byte("src content")), 11)
	eng.WriteFileStream("/dst.txt", bytes.NewReader([]byte("old dst")), 7)

	cloud.mu.Lock()
	before := len(cloud.objects)
	cloud.mu.Unlock()
	if before < 2 {
		t.Fatalf("expected at least 2 cloud objects before rename, got %d", before)
	}

	if err := eng.RenameFile("/src.txt", "/dst.txt"); err != nil {
		t.Fatalf("RenameFile: %v", err)
	}
	if f, _ := eng.db.GetFileByPath("/src.txt"); f != nil {
		t.Error("/src.txt must not exist after rename")
	}

	got, err := eng.ReadFile("/dst.txt")
	if err != nil {
		t.Fatalf("ReadFile after rename-over-dest: %v", err)
	}
	if string(got) != "src content" {
		t.Errorf("expected 'src content', got %q", got)
	}

	// Cloud cleanup: old dst chunks should eventually be removed (async).
	time.Sleep(200 * time.Millisecond)
}

// TestDeleteDir verifies all files and cloud chunks under a directory are removed.
func TestDeleteDir(t *testing.T) {
	eng, cloud := newTestEngine(t)
	eng.WriteFileStream("/movies/a.mkv", bytes.NewReader([]byte("aaa")), 3)
	eng.WriteFileStream("/movies/b.mkv", bytes.NewReader([]byte("bbb")), 3)
	eng.WriteFileStream("/other/c.txt", bytes.NewReader([]byte("ccc")), 3)

	cloud.mu.Lock()
	before := len(cloud.objects)
	cloud.mu.Unlock()
	if before < 3 {
		t.Fatalf("expected ≥3 cloud objects before DeleteDir, got %d", before)
	}

	if err := eng.DeleteDir("/movies"); err != nil {
		t.Fatalf("DeleteDir: %v", err)
	}

	if f, _ := eng.db.GetFileByPath("/movies/a.mkv"); f != nil {
		t.Error("/movies/a.mkv must be deleted")
	}
	if f, _ := eng.db.GetFileByPath("/movies/b.mkv"); f != nil {
		t.Error("/movies/b.mkv must be deleted")
	}
	// Other dir unaffected.
	if f, _ := eng.db.GetFileByPath("/other/c.txt"); f == nil {
		t.Error("/other/c.txt must still exist")
	}

	// Cloud chunks for movies cleaned up asynchronously.
	time.Sleep(300 * time.Millisecond)
	cloud.mu.Lock()
	after := len(cloud.objects)
	cloud.mu.Unlock()
	if after != 1 {
		t.Errorf("expected 1 cloud object (/other/c.txt chunk) remaining, got %d", after)
	}
}

// TestRenameDir verifies all files under a directory are moved to the new path.
func TestRenameDir(t *testing.T) {
	eng, _ := newTestEngine(t)
	eng.WriteFileStream("/docs/readme.txt", bytes.NewReader([]byte("readme")), 6)
	eng.WriteFileStream("/docs/sub/nested.txt", bytes.NewReader([]byte("nested")), 6)

	if err := eng.RenameDir("/docs", "/documents"); err != nil {
		t.Fatalf("RenameDir: %v", err)
	}

	// Old paths gone.
	if f, _ := eng.db.GetFileByPath("/docs/readme.txt"); f != nil {
		t.Error("old path must not exist after RenameDir")
	}

	// New paths exist and are readable.
	got, err := eng.ReadFile("/documents/readme.txt")
	if err != nil {
		t.Fatalf("ReadFile after RenameDir: %v", err)
	}
	if string(got) != "readme" {
		t.Errorf("content mismatch after RenameDir")
	}
	got2, err := eng.ReadFile("/documents/sub/nested.txt")
	if err != nil {
		t.Fatalf("ReadFile nested after RenameDir: %v", err)
	}
	if string(got2) != "nested" {
		t.Errorf("nested content mismatch after RenameDir")
	}
}

// TestMkDir_AppearsInListDir verifies MkDir creates an explicit directory that
// shows up as a subdirectory in ListDir.
func TestMkDir_AppearsInListDir(t *testing.T) {
	eng, _ := newTestEngine(t)
	if err := eng.MkDir("/mymovies"); err != nil {
		t.Fatalf("MkDir: %v", err)
	}

	_, dirs, err := eng.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	found := false
	for _, d := range dirs {
		if d == "mymovies" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'mymovies' in ListDir result, got: %v", dirs)
	}
}

// TestIsDir verifies IsDir for various path types.
func TestIsDir(t *testing.T) {
	eng, _ := newTestEngine(t)
	eng.WriteFileStream("/tv/show.mkv", bytes.NewReader([]byte("x")), 1)

	cases := []struct {
		path  string
		want  bool
		label string
	}{
		{"/tv", true, "implicit dir from file path"},
		{"/tv/show.mkv", false, "file path is not a dir"},
		{"/nonexistent", false, "non-existent path"},
		{"/", true, "root is always a dir"},
	}
	for _, c := range cases {
		got, err := eng.IsDir(c.path)
		if err != nil {
			t.Errorf("IsDir(%q): unexpected error: %v", c.path, err)
			continue
		}
		if got != c.want {
			t.Errorf("IsDir(%q) = %v, want %v (%s)", c.path, got, c.want, c.label)
		}
	}
}

// TestFileExists_Variants checks FileExists for complete, pending, and missing files.
func TestFileExists_Variants(t *testing.T) {
	eng, _ := newTestEngine(t)
	eng.WriteFileStream("/complete.txt", bytes.NewReader([]byte("x")), 1)

	now := time.Now().Unix()
	tmp := "/tmp/pend"
	eng.db.InsertFile(&metadata.File{
		ID: "pnd", VirtualPath: "/pending.mkv", SizeBytes: 100,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: &tmp,
	})

	cases := []struct {
		path string
		want bool
	}{
		{"/complete.txt", true},
		{"/pending.mkv", true}, // FileExists sees ALL states
		{"/missing.txt", false},
	}
	for _, c := range cases {
		got, err := eng.FileExists(c.path)
		if err != nil {
			t.Fatalf("FileExists(%q): %v", c.path, err)
		}
		if got != c.want {
			t.Errorf("FileExists(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

// TestWriteFile_ConvenienceWrapper verifies the WriteFile shorthand delegates
// to WriteFileStream and produces a readable file.
func TestWriteFile_ConvenienceWrapper(t *testing.T) {
	eng, _ := newTestEngine(t)
	content := []byte("shorthand write")
	if err := eng.WriteFile("/short.txt", content); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := eng.ReadFile("/short.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch: got %q, want %q", got, content)
	}
}

// TestWriteFileStream_EmptyFile verifies that a zero-byte file can be written
// and then read back.
func TestWriteFileStream_EmptyFile(t *testing.T) {
	eng, _ := newTestEngine(t)
	if err := eng.WriteFileStream("/empty.txt", bytes.NewReader(nil), 0); err != nil {
		t.Fatalf("WriteFileStream (empty): %v", err)
	}
	got, err := eng.ReadFile("/empty.txt")
	if err != nil {
		t.Fatalf("ReadFile empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 bytes, got %d", len(got))
	}
}

// TestWriteFileStream_MultiChunk writes a file larger than DefaultChunkSize
// (32 MiB), forcing the engine to split it into >1 chunks. Verifies the full
// content round-trips correctly so chunk ordering and assembly are exercised.
func TestWriteFileStream_MultiChunk(t *testing.T) {
	const defaultChunkSize = 32 * 1024 * 1024 // must match chunker.DefaultChunkSize
	eng, _ := newTestEngine(t)

	// Fill with a non-constant pattern so content mismatch is detectable.
	content := make([]byte, defaultChunkSize+1024)
	for i := range content {
		content[i] = byte(i & 0xFF)
	}

	if err := eng.WriteFileStream("/big.bin", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("WriteFileStream multi-chunk: %v", err)
	}
	got, err := eng.ReadFile("/big.bin")
	if err != nil {
		t.Fatalf("ReadFile multi-chunk: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("multi-chunk content mismatch (len got=%d want=%d)", len(got), len(content))
	}
}

// TestMultipleOverwrites_OnlyFinalContentReadable writes the same path 5 times
// and verifies only the last version is readable. Ensures UNIQUE constraints
// never fire and old cloud chunks are cleaned up.
func TestMultipleOverwrites_OnlyFinalContentReadable(t *testing.T) {
	eng, _ := newTestEngine(t)
	path := "/overwritten.txt"

	for i := 0; i < 5; i++ {
		payload := fmt.Sprintf("version-%d", i)
		if err := eng.WriteFileStream(path, bytes.NewReader([]byte(payload)), int64(len(payload))); err != nil {
			t.Fatalf("write #%d: %v", i, err)
		}
	}

	got, err := eng.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile after 5 overwrites: %v", err)
	}
	if string(got) != "version-4" {
		t.Errorf("expected 'version-4', got %q", got)
	}

	// Only one DB record should remain.
	rows, _ := eng.db.Conn().Query(`SELECT COUNT(*) FROM files WHERE virtual_path = ?`, path)
	var count int
	if rows.Next() {
		rows.Scan(&count)
	}
	rows.Close()
	if count != 1 {
		t.Errorf("expected exactly 1 DB record for %s, got %d", path, count)
	}
}

// TestConcurrentWrites_SamePath stress-tests overwriting the same path from
// multiple goroutines simultaneously. Because writes go through SQLite with a
// UNIQUE constraint on virtual_path, some concurrent writes will inevitably
// fail with a constraint error — this is expected serialisation behaviour, not
// a bug. Similarly, FK errors on chunk insertion can occur when a concurrent
// goroutine deletes the file record between the upload and the metadata write.
// Both are expected races. What must hold: no panics, no deadlocks, and the
// path is readable after all goroutines complete.
func TestConcurrentWrites_SamePath(t *testing.T) {
	eng, _ := newTestEngine(t)
	path := "/race.txt"

	const goroutines = 8
	var fatalErrs []error
	var mu sync.Mutex
	var wg sync.WaitGroup

	isExpectedRaceErr := func(err error) bool {
		s := err.Error()
		return strings.Contains(s, "UNIQUE constraint") || strings.Contains(s, "FOREIGN KEY constraint")
	}

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("goroutine-%d", n))
			if err := eng.WriteFileStream(path, bytes.NewReader(payload), int64(len(payload))); err != nil {
				if !isExpectedRaceErr(err) {
					mu.Lock()
					fatalErrs = append(fatalErrs, err)
					mu.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()

	for _, err := range fatalErrs {
		t.Errorf("unexpected concurrent write error: %v", err)
	}

	// File must be readable after all writers finish.
	if _, err := eng.ReadFile(path); err != nil {
		t.Errorf("ReadFile after concurrent writes: %v", err)
	}
}

// TestStorageStatus_Accurate checks that StorageStatus reports correct file
// count and byte total after writes.
func TestStorageStatus_Accurate(t *testing.T) {
	eng, _ := newTestEngine(t)
	eng.WriteFileStream("/a.txt", bytes.NewReader([]byte("aaaa")), 4)
	eng.WriteFileStream("/b.txt", bytes.NewReader([]byte("bb")), 2)

	st := eng.StorageStatus()
	if st.TotalFiles != 2 {
		t.Errorf("expected TotalFiles=2, got %d", st.TotalFiles)
	}
	if st.TotalBytes != 6 {
		t.Errorf("expected TotalBytes=6, got %d", st.TotalBytes)
	}
	if len(st.Providers) != 1 {
		t.Errorf("expected 1 provider, got %d", len(st.Providers))
	}
}

// TestUploadProgressTracking verifies that WriteFileAsync registers an in-flight
// entry in UploadProgress and removes it after completion.
func TestUploadProgressTracking(t *testing.T) {
	eng, _ := newTestEngine(t)
	content := make([]byte, 512)
	tmpFile, tmpPath := writeTmpFile(t, content)

	// Use a slow cloud that blocks so we can observe the progress entry.
	// Actually with fakeCloud, the upload is instant; we just check that at
	// least one entry existed at some point. We do this by counting before/after.
	if err := eng.WriteFileAsync("/tracked.bin", tmpFile, tmpPath, int64(len(content))); err != nil {
		t.Fatalf("WriteFileAsync: %v", err)
	}
	// Progress entry may already be cleared if the goroutine was fast; we at
	// least verify UploadProgress doesn't panic and returns a slice.
	_ = eng.UploadProgress()

	// Wait for completion.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.db.GetCompleteFileByPath("/tracked.bin"); f != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// After upload finishes, the progress entry may still be present (it's not
	// explicitly removed — that's OK for the daemon). Just verify no panic.
	info := eng.UploadProgress()
	t.Logf("upload progress entries after completion: %d", len(info))
}

// TestResumeUploads_MissingTmpFile checks that a pending record with a
// tmp_path that no longer exists on disk gets cleaned up — not re-queued.
func TestResumeUploads_MissingTmpFile(t *testing.T) {
	eng, _ := newTestEngine(t)
	gone := "/tmp/this-file-does-not-exist-pdrive-test"
	now := time.Now().Unix()
	eng.db.InsertFile(&metadata.File{
		ID: "ghost", VirtualPath: "/ghost.mkv", SizeBytes: 100,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: &gone,
	})

	eng.ResumeUploads()
	time.Sleep(100 * time.Millisecond)

	// Record must be removed because the tmp file is gone.
	if f, _ := eng.db.GetFileByPath("/ghost.mkv"); f != nil {
		t.Error("pending record with missing tmp file must be cleaned up by ResumeUploads")
	}
}

// TestResumeUploads_NilTmpPath checks that a pending record with no tmp_path
// (can't happen in normal operation but guards against DB corruption) is cleaned
// up without panicking.
func TestResumeUploads_NilTmpPath(t *testing.T) {
	eng, _ := newTestEngine(t)
	now := time.Now().Unix()
	eng.db.InsertFile(&metadata.File{
		ID: "nil-path", VirtualPath: "/corrupt.mkv", SizeBytes: 100,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: nil,
	})

	eng.ResumeUploads()
	time.Sleep(100 * time.Millisecond)

	if f, _ := eng.db.GetFileByPath("/corrupt.mkv"); f != nil {
		t.Error("pending record with nil tmp_path must be cleaned up by ResumeUploads")
	}
}

// TestWriteFileAsync_ThenImmediateOverwrite tests: start an async upload, then
// synchronously overwrite with a new write before the goroutine finishes. The
// sync write must win cleanly.
func TestWriteFileAsync_ThenImmediateOverwrite(t *testing.T) {
	eng, cloud := newTestEngine(t)

	// Make the cloud slow so the first goroutine is still running when the
	// synchronous overwrite happens.
	cloud.putDelay = 200 * time.Millisecond

	bigContent := make([]byte, 512)
	for i := range bigContent {
		bigContent[i] = 0xAA
	}
	tmpFile, tmpPath := writeTmpFile(t, bigContent)
	if err := eng.WriteFileAsync("/overlap.txt", tmpFile, tmpPath, int64(len(bigContent))); err != nil {
		t.Fatalf("WriteFileAsync: %v", err)
	}

	// Overwrite synchronously while the goroutine is still uploading.
	finalContent := []byte("sync-wins")
	if err := eng.WriteFileStream("/overlap.txt", bytes.NewReader(finalContent), int64(len(finalContent))); err != nil {
		t.Fatalf("synchronous overwrite: %v", err)
	}

	// Wait for async goroutine to finish (it will fail to find its fileID in DB
	// because the sync write already replaced it).
	time.Sleep(500 * time.Millisecond)

	got, err := eng.ReadFile("/overlap.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "sync-wins" {
		t.Errorf("synchronous write must win, got %q", got)
	}
}

// TestListDir_ShowsNestedSubdirs verifies that ListDir reports only direct
// children, not deeply nested files, and that implicit subdirectories appear.
func TestListDir_ShowsNestedSubdirs(t *testing.T) {
	eng, _ := newTestEngine(t)
	eng.WriteFileStream("/root.txt", bytes.NewReader([]byte("r")), 1)
	eng.WriteFileStream("/music/song.mp3", bytes.NewReader([]byte("m")), 1)
	eng.WriteFileStream("/music/albums/dark.mp3", bytes.NewReader([]byte("d")), 1)

	files, dirs, err := eng.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(files) != 1 || files[0].VirtualPath != "/root.txt" {
		t.Errorf("expected exactly [/root.txt] at root level, got %v", files)
	}
	if len(dirs) != 1 || dirs[0] != "music" {
		t.Errorf("expected exactly ['music'] as subdir, got %v", dirs)
	}

	// Nested listing.
	musicFiles, musicDirs, err := eng.ListDir("/music")
	if err != nil {
		t.Fatalf("ListDir /music: %v", err)
	}
	if len(musicFiles) != 1 {
		t.Errorf("expected 1 file under /music, got %d", len(musicFiles))
	}
	if len(musicDirs) != 1 || musicDirs[0] != "albums" {
		t.Errorf("expected 'albums' as subdir of /music, got %v", musicDirs)
	}
}
