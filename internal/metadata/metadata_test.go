package metadata

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestOpenAndWAL(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Verify WAL mode.
	var mode string
	db.Conn().QueryRow("PRAGMA journal_mode").Scan(&mode)
	if mode != "wal" {
		t.Errorf("expected WAL mode, got %s", mode)
	}

	// Verify DB file exists.
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("database file was not created")
	}
}

func TestProviderCRUD(t *testing.T) {
	db := testDB(t)
	now := time.Now().Unix()
	total := int64(15e9)
	free := int64(10e9)

	p := &Provider{
		ID:              "p1",
		Type:            "drive",
		DisplayName:     "My Google Drive",
		RcloneRemote:    "gdrive",
		QuotaTotalBytes: &total,
		QuotaFreeBytes:  &free,
		QuotaPolledAt:   &now,
	}
	if err := db.UpsertProvider(p); err != nil {
		t.Fatal(err)
	}

	got, err := db.GetProvider("p1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("provider not found")
	}
	if got.DisplayName != "My Google Drive" {
		t.Errorf("unexpected display name: %s", got.DisplayName)
	}
	if *got.QuotaFreeBytes != 10e9 {
		t.Errorf("unexpected free bytes: %d", *got.QuotaFreeBytes)
	}

	// Test GetAllProviders.
	all, err := db.GetAllProviders()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(all))
	}
}

func TestFileCRUD(t *testing.T) {
	db := testDB(t)

	// Seed a provider.
	total := int64(15e9)
	free := int64(10e9)
	db.UpsertProvider(&Provider{ID: "p1", Type: "drive", DisplayName: "GDrive", RcloneRemote: "gdrive", QuotaTotalBytes: &total, QuotaFreeBytes: &free})

	now := time.Now().Unix()
	f := &File{
		ID:          "f1",
		VirtualPath: "/test.txt",
		SizeBytes:   1024,
		CreatedAt:   now,
		ModifiedAt:  now,
		SHA256Full:  "abc123",
	}
	if err := db.InsertFile(f); err != nil {
		t.Fatal(err)
	}

	got, err := db.GetFileByPath("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("file not found")
	}
	if got.SizeBytes != 1024 {
		t.Errorf("unexpected size: %d", got.SizeBytes)
	}

	// FileExists
	exists, err := db.FileExists("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("file should exist")
	}

	// Delete.
	if err := db.DeleteFile("f1"); err != nil {
		t.Fatal(err)
	}
	got, err = db.GetFileByPath("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("file should have been deleted")
	}
}

func TestChunkAndLocationCRUD(t *testing.T) {
	db := testDB(t)

	total := int64(15e9)
	free := int64(10e9)
	db.UpsertProvider(&Provider{ID: "p1", Type: "drive", DisplayName: "GDrive", RcloneRemote: "gdrive", QuotaTotalBytes: &total, QuotaFreeBytes: &free})

	now := time.Now().Unix()
	db.InsertFile(&File{ID: "f1", VirtualPath: "/data.bin", SizeBytes: 8 * 1024 * 1024, CreatedAt: now, ModifiedAt: now, SHA256Full: "fullhash"})

	// Insert chunks.
	db.InsertChunk(&ChunkRecord{ID: "c1", FileID: "f1", Sequence: 0, SizeBytes: 4 * 1024 * 1024, SHA256: "hash1", EncryptedSize: 4*1024*1024 + 28})
	db.InsertChunk(&ChunkRecord{ID: "c2", FileID: "f1", Sequence: 1, SizeBytes: 4 * 1024 * 1024, SHA256: "hash2", EncryptedSize: 4*1024*1024 + 28})

	chunks, err := db.GetChunksForFile("f1")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
	if chunks[0].Sequence != 0 || chunks[1].Sequence != 1 {
		t.Error("chunks not ordered correctly")
	}

	// Insert locations.
	db.InsertChunkLocation(&ChunkLocation{ChunkID: "c1", ProviderID: "p1", RemotePath: "pdrive-chunks/c1"})
	db.InsertChunkLocation(&ChunkLocation{ChunkID: "c2", ProviderID: "p1", RemotePath: "pdrive-chunks/c2"})

	locs, err := db.GetChunkLocations("c1")
	if err != nil {
		t.Fatal(err)
	}
	if len(locs) != 1 {
		t.Fatalf("expected 1 location, got %d", len(locs))
	}
	if locs[0].UploadConfirmedAt != nil {
		t.Error("upload should not be confirmed yet")
	}

	// Confirm upload.
	if err := db.ConfirmUpload("c1", "p1"); err != nil {
		t.Fatal(err)
	}
	locs, _ = db.GetChunkLocations("c1")
	if locs[0].UploadConfirmedAt == nil {
		t.Error("upload should be confirmed now")
	}

	// GetChunkLocationsForFile.
	allLocs, err := db.GetChunkLocationsForFile("f1")
	if err != nil {
		t.Fatal(err)
	}
	if len(allLocs) != 2 {
		t.Fatalf("expected 2 locations, got %d", len(allLocs))
	}
}

func TestCascadeDelete(t *testing.T) {
	db := testDB(t)

	total := int64(15e9)
	free := int64(10e9)
	db.UpsertProvider(&Provider{ID: "p1", Type: "drive", DisplayName: "GDrive", RcloneRemote: "gdrive", QuotaTotalBytes: &total, QuotaFreeBytes: &free})

	now := time.Now().Unix()
	db.InsertFile(&File{ID: "f1", VirtualPath: "/cascade.bin", SizeBytes: 100, CreatedAt: now, ModifiedAt: now, SHA256Full: "h"})
	db.InsertChunk(&ChunkRecord{ID: "c1", FileID: "f1", Sequence: 0, SizeBytes: 100, SHA256: "ch", EncryptedSize: 128})
	db.InsertChunkLocation(&ChunkLocation{ChunkID: "c1", ProviderID: "p1", RemotePath: "pdrive-chunks/c1"})

	// Delete file — should cascade to chunks and locations.
	if err := db.DeleteFile("f1"); err != nil {
		t.Fatal(err)
	}

	chunks, _ := db.GetChunksForFile("f1")
	if len(chunks) != 0 {
		t.Error("chunks should have been cascaded")
	}

	locs, _ := db.GetChunkLocations("c1")
	if len(locs) != 0 {
		t.Error("locations should have been cascaded")
	}
}

func TestListFilesAndDirs(t *testing.T) {
	db := testDB(t)
	now := time.Now().Unix()

	db.InsertFile(&File{ID: "f1", VirtualPath: "/a.txt", SizeBytes: 10, CreatedAt: now, ModifiedAt: now, SHA256Full: "h1"})
	db.InsertFile(&File{ID: "f2", VirtualPath: "/sub/b.txt", SizeBytes: 20, CreatedAt: now, ModifiedAt: now, SHA256Full: "h2"})
	db.InsertFile(&File{ID: "f3", VirtualPath: "/sub/deep/c.txt", SizeBytes: 30, CreatedAt: now, ModifiedAt: now, SHA256Full: "h3"})

	// List root.
	files, err := db.ListFiles("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 root file, got %d", len(files))
	}

	// List subdirectories of root.
	dirs, err := db.ListSubdirectories("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 1 || dirs[0] != "sub" {
		t.Errorf("unexpected root subdirectories: %v", dirs)
	}

	// List /sub/ files.
	subFiles, err := db.ListFiles("/sub/")
	if err != nil {
		t.Fatal(err)
	}
	if len(subFiles) != 1 {
		t.Fatalf("expected 1 file in /sub/, got %d", len(subFiles))
	}

	// PathIsDir
	isDir, err := db.PathIsDir("/sub/")
	if err != nil {
		t.Fatal(err)
	}
	if !isDir {
		t.Error("/sub/ should be a directory")
	}
}

// ── Upload-state tests ──────────────────────────────────────────────────────

func seedProvider(t *testing.T, db *DB) {
	t.Helper()
	total, free := int64(15e9), int64(10e9)
	db.UpsertProvider(&Provider{
		ID: "p1", Type: "drive", DisplayName: "GDrive", RcloneRemote: "gdrive",
		QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
}

func newCompleteFile(id, path string) *File {
	now := time.Now().Unix()
	return &File{ID: id, VirtualPath: path, SizeBytes: 1024,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "hash", UploadState: "complete"}
}

func newPendingFile(id, path, tmpPath string) *File {
	now := time.Now().Unix()
	f := &File{ID: id, VirtualPath: path, SizeBytes: 655 * 1024 * 1024,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "hash", UploadState: "pending"}
	f.TmpPath = &tmpPath
	return f
}

// TestGetFileByPath_ReturnsPendingRecords verifies that GetFileByPath returns
// both pending and complete records (required so Stat after a PUT succeeds and
// overwrite logic can find and delete existing pending records).
func TestGetFileByPath_ReturnsPendingRecords(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)

	if err := db.InsertFile(newPendingFile("f1", "/movie.mkv", "/tmp/x")); err != nil {
		t.Fatal(err)
	}

	got, err := db.GetFileByPath("/movie.mkv")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("GetFileByPath must return pending records")
	}
	if got.UploadState != "pending" {
		t.Errorf("expected pending, got %q", got.UploadState)
	}
}

// TestGetCompleteFileByPath_HidesPendingRecords verifies that the read-path
// query does NOT return pending files.
func TestGetCompleteFileByPath_HidesPendingRecords(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)

	if err := db.InsertFile(newPendingFile("f1", "/movie.mkv", "/tmp/x")); err != nil {
		t.Fatal(err)
	}

	got, err := db.GetCompleteFileByPath("/movie.mkv")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("GetCompleteFileByPath must not return pending records")
	}

	// Mark complete; now it should be visible.
	if err := db.SetUploadComplete("f1"); err != nil {
		t.Fatal(err)
	}
	got, err = db.GetCompleteFileByPath("/movie.mkv")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("GetCompleteFileByPath must return complete records")
	}
}

// TestListFiles_HidesPendingFiles verifies that directory listings only show
// complete files so that partially-uploaded files are invisible in the mount.
func TestListFiles_HidesPendingFiles(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)

	db.InsertFile(newCompleteFile("f1", "/dir/complete.txt"))
	db.InsertFile(newPendingFile("f2", "/dir/pending.mkv", "/tmp/p"))

	files, err := db.ListFiles("/dir/")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file in listing, got %d (pending must be hidden)", len(files))
	}
	if files[0].ID != "f1" {
		t.Errorf("expected complete file f1, got %q", files[0].ID)
	}
}

// TestGetPendingUploads verifies that GetPendingUploads finds pending records
// for ResumeUploads to pick up on daemon restart.
func TestGetPendingUploads(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)

	db.InsertFile(newCompleteFile("f1", "/ready.txt"))
	db.InsertFile(newPendingFile("f2", "/uploading.mkv", "/tmp/u"))

	pending, err := db.GetPendingUploads()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending upload, got %d", len(pending))
	}
	if pending[0].ID != "f2" {
		t.Errorf("expected f2, got %q", pending[0].ID)
	}
}

// TestUploadStateLifecycle tests the complete pending→complete transition and
// verifies visibility at each stage.
func TestUploadStateLifecycle(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)

	// Insert as pending.
	tmp := "/tmp/lifecycle"
	if err := db.InsertFile(newPendingFile("f1", "/video.mkv", tmp)); err != nil {
		t.Fatal(err)
	}

	// Not visible in listing.
	files, _ := db.ListFiles("/")
	if len(files) != 0 {
		t.Error("pending file must not show in directory listing")
	}

	// Visible via direct lookup.
	f, _ := db.GetFileByPath("/video.mkv")
	if f == nil || f.UploadState != "pending" {
		t.Error("GetFileByPath must find the pending record")
	}
	if f.TmpPath == nil || *f.TmpPath != tmp {
		t.Error("TmpPath must be stored")
	}

	// Not visible to read operations.
	if c, _ := db.GetCompleteFileByPath("/video.mkv"); c != nil {
		t.Error("GetCompleteFileByPath must hide the pending record")
	}

	// Mark complete.
	if err := db.SetUploadComplete("f1"); err != nil {
		t.Fatal(err)
	}

	// Now visible everywhere.
	files, _ = db.ListFiles("/")
	if len(files) != 1 {
		t.Error("complete file must show in directory listing")
	}
	if c, _ := db.GetCompleteFileByPath("/video.mkv"); c == nil {
		t.Error("GetCompleteFileByPath must find complete record")
	}
	// TmpPath cleared.
	f, _ = db.GetFileByPath("/video.mkv")
	if f.TmpPath != nil {
		t.Error("TmpPath must be cleared after SetUploadComplete")
	}
}

// TestOverwritePendingWithNewWrite verifies that writing a file to a path that
// already has a pending record succeeds (the old record must be replaced, not
// cause a UNIQUE constraint violation).
func TestOverwritePendingWithNewWrite(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)

	// Insert a stuck pending record.
	if err := db.InsertFile(newPendingFile("old", "/movie.mkv", "/tmp/old")); err != nil {
		t.Fatal(err)
	}

	// Simulate what WriteFileAsync does: find + delete the old record, insert new.
	existing, err := db.GetFileByPath("/movie.mkv")
	if err != nil || existing == nil {
		t.Fatal("must find the existing pending record before overwrite")
	}
	if err := db.DeleteFile(existing.ID); err != nil {
		t.Fatal("must be able to delete the old record")
	}

	// Insert the new record — must not fail with UNIQUE constraint.
	if err := db.InsertFile(newPendingFile("new", "/movie.mkv", "/tmp/new")); err != nil {
		t.Fatalf("inserting new record after delete must succeed: %v", err)
	}

	f, _ := db.GetFileByPath("/movie.mkv")
	if f == nil || f.ID != "new" {
		t.Error("new record should be the active one")
	}
}

// TestFKConstraintEnforced verifies that foreign key constraints are enforced
// (requires SetMaxOpenConns(1) so the PRAGMA applies to all connections).
func TestFKConstraintEnforced(t *testing.T) {
	db := testDB(t)

	// Inserting a chunk that references a non-existent file must fail.
	err := db.InsertChunk(&ChunkRecord{
		ID:            "c1",
		FileID:        "nonexistent-file-id",
		Sequence:      0,
		SizeBytes:     100,
		SHA256:        "hash",
		EncryptedSize: 128,
	})
	if err == nil {
		t.Error("InsertChunk with non-existent file_id must fail (FK constraint)")
	}
}

// TestConcurrentWritesFK verifies FK constraints hold under concurrent writes
// from multiple goroutines (catches the multi-connection PRAGMA bug).
func TestConcurrentWritesFK(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)

	now := time.Now().Unix()
	// Insert 10 files and their chunks concurrently.
	errs := make(chan error, 20)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("f%d", i)
		cid := fmt.Sprintf("c%d", i)
		go func(fileID, chunkID string) {
			if err := db.InsertFile(&File{
				ID: fileID, VirtualPath: "/" + fileID + ".bin",
				SizeBytes: 100, CreatedAt: now, ModifiedAt: now,
				SHA256Full: "h", UploadState: "complete",
			}); err != nil {
				errs <- err
				return
			}
			if err := db.InsertChunk(&ChunkRecord{
				ID: chunkID, FileID: fileID, Sequence: 0,
				SizeBytes: 100, SHA256: "h", EncryptedSize: 128,
			}); err != nil {
				errs <- err
				return
			}
			errs <- nil
		}(id, cid)
	}
	for i := 0; i < 10; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent write error: %v", err)
		}
	}
}

// TestPathIsDir_CountsPendingFiles ensures that a directory with only pending
// files still reports as a directory (so the parent folder is visible in mount).
func TestPathIsDir_CountsPendingFiles(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)

	// Only a pending file under /movies/
	db.InsertFile(newPendingFile("f1", "/movies/clip.mkv", "/tmp/clip"))

	isDir, err := db.PathIsDir("/movies/")
	if err != nil {
		t.Fatal(err)
	}
	if !isDir {
		t.Error("directory with only pending files must still appear as a directory")
	}

	// But the listing is empty.
	files, _ := db.ListFiles("/movies/")
	if len(files) != 0 {
		t.Error("listing must be empty while file is pending")
	}
}
