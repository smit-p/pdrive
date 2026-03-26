package metadata

import (
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
