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

// ── Directory CRUD tests ────────────────────────────────────────────────────

func TestCreateDirectory_AndExists(t *testing.T) {
	db := testDB(t)

	if err := db.CreateDirectory("/movies"); err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}
	exists, err := db.DirectoryExists("/movies")
	if err != nil {
		t.Fatalf("DirectoryExists: %v", err)
	}
	if !exists {
		t.Error("directory should exist after creation")
	}
}

func TestDirectoryExists_NonExistent(t *testing.T) {
	db := testDB(t)
	exists, err := db.DirectoryExists("/nope")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("non-existent directory should not exist")
	}
}

func TestDirectoryExists_RootAlwaysTrue(t *testing.T) {
	db := testDB(t)
	exists, err := db.DirectoryExists("")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("root directory must always exist")
	}
}

func TestDeleteDirectory(t *testing.T) {
	db := testDB(t)
	db.CreateDirectory("/movies")
	db.DeleteDirectory("/movies")

	exists, _ := db.DirectoryExists("/movies")
	if exists {
		t.Error("directory should be gone after deletion")
	}
}

func TestDeleteDirectoriesUnder(t *testing.T) {
	db := testDB(t)
	db.CreateDirectory("/movies")
	db.CreateDirectory("/movies/action")
	db.CreateDirectory("/movies/comedy")
	db.CreateDirectory("/other")

	if err := db.DeleteDirectoriesUnder("/movies"); err != nil {
		t.Fatalf("DeleteDirectoriesUnder: %v", err)
	}

	exists, _ := db.DirectoryExists("/movies")
	if exists {
		t.Error("/movies should be deleted")
	}
	exists, _ = db.DirectoryExists("/movies/action")
	if exists {
		t.Error("/movies/action should be deleted")
	}
	exists, _ = db.DirectoryExists("/other")
	if !exists {
		t.Error("/other should NOT be deleted")
	}
}

func TestGetFilesUnderDir(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)
	db.InsertFile(newCompleteFile("f1", "/docs/a.txt"))
	db.InsertFile(newCompleteFile("f2", "/docs/sub/b.txt"))
	db.InsertFile(newCompleteFile("f3", "/other/c.txt"))

	files, err := db.GetFilesUnderDir("/docs")
	if err != nil {
		t.Fatalf("GetFilesUnderDir: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files under /docs, got %d", len(files))
	}
	paths := map[string]bool{}
	for _, f := range files {
		paths[f.VirtualPath] = true
	}
	if !paths["/docs/a.txt"] || !paths["/docs/sub/b.txt"] {
		t.Errorf("unexpected files: %v", paths)
	}
}

// ── Rename tests ────────────────────────────────────────────────────────────

func TestRenameFileByPath(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)
	db.InsertFile(newCompleteFile("f1", "/old.txt"))

	if err := db.RenameFileByPath("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("RenameFileByPath: %v", err)
	}
	f, _ := db.GetFileByPath("/new.txt")
	if f == nil {
		t.Fatal("file must exist at new path")
	}
	old, _ := db.GetFileByPath("/old.txt")
	if old != nil {
		t.Error("file must not exist at old path")
	}
}

func TestRenameFilesUnderDir(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)
	db.InsertFile(newCompleteFile("f1", "/docs/a.txt"))
	db.InsertFile(newCompleteFile("f2", "/docs/sub/b.txt"))
	db.InsertFile(newCompleteFile("f3", "/other/c.txt"))

	if err := db.RenameFilesUnderDir("/docs", "/documents"); err != nil {
		t.Fatalf("RenameFilesUnderDir: %v", err)
	}

	f1, _ := db.GetFileByPath("/documents/a.txt")
	if f1 == nil {
		t.Error("/documents/a.txt must exist after rename")
	}
	f2, _ := db.GetFileByPath("/documents/sub/b.txt")
	if f2 == nil {
		t.Error("/documents/sub/b.txt must exist after rename")
	}
	f3, _ := db.GetFileByPath("/other/c.txt")
	if f3 == nil {
		t.Error("/other/c.txt must be untouched")
	}
	old, _ := db.GetFileByPath("/docs/a.txt")
	if old != nil {
		t.Error("/docs/a.txt must be gone after rename")
	}
}

func TestRenameDirectoriesUnder(t *testing.T) {
	db := testDB(t)
	db.CreateDirectory("/docs")
	db.CreateDirectory("/docs/sub")
	db.CreateDirectory("/other")

	if err := db.RenameDirectoriesUnder("/docs", "/documents"); err != nil {
		t.Fatalf("RenameDirectoriesUnder: %v", err)
	}

	exists, _ := db.DirectoryExists("/documents")
	if !exists {
		t.Error("/documents should exist")
	}
	exists, _ = db.DirectoryExists("/documents/sub")
	if !exists {
		t.Error("/documents/sub should exist")
	}
	exists, _ = db.DirectoryExists("/docs")
	if exists {
		t.Error("/docs should be gone")
	}
	exists, _ = db.DirectoryExists("/other")
	if !exists {
		t.Error("/other should be untouched")
	}
}

// TestListFiles_DirectChildrenOnly verifies that ListFiles returns only direct
// children and excludes deeply nested files (SQL-only filtering via INSTR).
func TestListFiles_DirectChildrenOnly(t *testing.T) {
	db := testDB(t)
	now := time.Now().Unix()

	files := []struct {
		id string
		vp string
	}{
		{"f1", "/docs/readme.txt"},
		{"f2", "/docs/license.txt"},
		{"f3", "/docs/sub/nested.txt"},
		{"f4", "/docs/sub/deep/very.txt"},
		{"f5", "/other/file.txt"},
	}
	for _, f := range files {
		db.InsertFile(&File{
			ID: f.id, VirtualPath: f.vp, SizeBytes: 100,
			CreatedAt: now, ModifiedAt: now, SHA256Full: "h" + f.id,
		})
	}

	got, err := db.ListFiles("/docs")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 direct children of /docs, got %d", len(got))
	}
	for _, f := range got {
		if f.VirtualPath != "/docs/readme.txt" && f.VirtualPath != "/docs/license.txt" {
			t.Errorf("unexpected file in ListFiles: %s", f.VirtualPath)
		}
	}
}

// TestRemotePathRefCount verifies the refcount query used by dedup-safe deletes.
func TestRemotePathRefCount(t *testing.T) {
	db := testDB(t)
	seedProvider(t, db)

	now := time.Now().Unix()
	db.InsertFile(&File{ID: "f1", VirtualPath: "/a.txt", SizeBytes: 100, CreatedAt: now, ModifiedAt: now, SHA256Full: "h1"})
	db.InsertFile(&File{ID: "f2", VirtualPath: "/b.txt", SizeBytes: 100, CreatedAt: now, ModifiedAt: now, SHA256Full: "h2"})
	db.InsertChunk(&ChunkRecord{ID: "c1", FileID: "f1", Sequence: 0, SizeBytes: 100, SHA256: "ch", EncryptedSize: 128})
	db.InsertChunk(&ChunkRecord{ID: "c2", FileID: "f2", Sequence: 0, SizeBytes: 100, SHA256: "ch", EncryptedSize: 128})

	// Both chunks point to the same remote_path (dedup scenario).
	db.InsertChunkLocation(&ChunkLocation{ChunkID: "c1", ProviderID: "p1", RemotePath: "pdrive-chunks/shared"})
	db.InsertChunkLocation(&ChunkLocation{ChunkID: "c2", ProviderID: "p1", RemotePath: "pdrive-chunks/shared"})

	count, err := db.RemotePathRefCount("pdrive-chunks/shared")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("expected refcount 2, got %d", count)
	}

	// Delete one file — CASCADE removes its chunk_location.
	db.DeleteFile("f1")

	count, err = db.RemotePathRefCount("pdrive-chunks/shared")
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected refcount 1 after deleting one file, got %d", count)
	}

	// Non-existent path should return 0.
	count, err = db.RemotePathRefCount("pdrive-chunks/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected refcount 0 for non-existent path, got %d", count)
	}
}

// TestFailedDeletions verifies insert, query, retry increment, and delete of
// failed deletion records.
func TestFailedDeletions(t *testing.T) {
	db := testDB(t)

	// Insert a failed deletion.
	if err := db.InsertFailedDeletion("prov1", "pdrive-chunks/abc123", "timeout"); err != nil {
		t.Fatalf("InsertFailedDeletion: %v", err)
	}

	// Fetch it.
	items, err := db.GetFailedDeletions(10)
	if err != nil {
		t.Fatalf("GetFailedDeletions: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 failed deletion, got %d", len(items))
	}
	if items[0].ProviderID != "prov1" || items[0].RemotePath != "pdrive-chunks/abc123" {
		t.Errorf("unexpected item: %+v", items[0])
	}
	if items[0].RetryCount != 0 {
		t.Errorf("expected retry_count=0, got %d", items[0].RetryCount)
	}

	// Increment retry.
	if err := db.IncrementFailedDeletionRetry(items[0].ID, "retry error"); err != nil {
		t.Fatalf("IncrementFailedDeletionRetry: %v", err)
	}
	items, _ = db.GetFailedDeletions(10)
	if items[0].RetryCount != 1 {
		t.Errorf("expected retry_count=1 after increment, got %d", items[0].RetryCount)
	}
	if items[0].LastError != "retry error" {
		t.Errorf("expected last_error='retry error', got %q", items[0].LastError)
	}

	// Delete it.
	if err := db.DeleteFailedDeletion(items[0].ID); err != nil {
		t.Fatalf("DeleteFailedDeletion: %v", err)
	}
	items, _ = db.GetFailedDeletions(10)
	if len(items) != 0 {
		t.Errorf("expected 0 failed deletions after delete, got %d", len(items))
	}
}

func TestGetChunkLocationsByProvider(t *testing.T) {
	db := testDB(t)

	// Create two providers.
	db.UpsertProvider(&Provider{ID: "p1", Type: "gdrive", DisplayName: "GD1", RcloneRemote: "gd1"})
	db.UpsertProvider(&Provider{ID: "p2", Type: "dropbox", DisplayName: "DB1", RcloneRemote: "db1"})

	// Create a file with two chunks on different providers.
	now := time.Now().Unix()
	db.InsertFile(&File{ID: "f1", VirtualPath: "/test.txt", SizeBytes: 100, CreatedAt: now, ModifiedAt: now, SHA256Full: "abc", UploadState: "complete"})
	db.InsertChunk(&ChunkRecord{ID: "c1", FileID: "f1", Sequence: 0, SizeBytes: 50, SHA256: "h1", EncryptedSize: 60})
	db.InsertChunk(&ChunkRecord{ID: "c2", FileID: "f1", Sequence: 1, SizeBytes: 50, SHA256: "h2", EncryptedSize: 60})
	conf := now
	db.InsertChunkLocation(&ChunkLocation{ChunkID: "c1", ProviderID: "p1", RemotePath: "pdrive-chunks/c1", UploadConfirmedAt: &conf})
	db.InsertChunkLocation(&ChunkLocation{ChunkID: "c2", ProviderID: "p2", RemotePath: "pdrive-chunks/c2", UploadConfirmedAt: &conf})

	// Query by provider — should only get chunks for that provider.
	locs, err := db.GetChunkLocationsByProvider("p1")
	if err != nil {
		t.Fatalf("GetChunkLocationsByProvider: %v", err)
	}
	if len(locs) != 1 || locs[0].ChunkID != "c1" {
		t.Errorf("expected 1 location for p1 (chunk c1), got %d", len(locs))
	}

	locs, err = db.GetChunkLocationsByProvider("p2")
	if err != nil {
		t.Fatalf("GetChunkLocationsByProvider: %v", err)
	}
	if len(locs) != 1 || locs[0].ChunkID != "c2" {
		t.Errorf("expected 1 location for p2 (chunk c2), got %d", len(locs))
	}

	// Non-existent provider returns empty.
	locs, err = db.GetChunkLocationsByProvider("p999")
	if err != nil {
		t.Fatalf("GetChunkLocationsByProvider: %v", err)
	}
	if len(locs) != 0 {
		t.Errorf("expected 0 locations for non-existent provider, got %d", len(locs))
	}
}
