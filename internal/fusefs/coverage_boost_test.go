package fusefs

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/metadata"
)

// mockParent implements fs.InodeEmbedder but is NOT *Root. Used to exercise the
// Rename fallback branch for non-Root parents.
type mockParent struct {
	fs.Inode
}

// newTestEngineWithDB is like newTestEngine but also returns the DB so tests
// can close it to trigger error paths.
func newTestEngineWithDB(t *testing.T) (*engine.Engine, *fakeCloud, *metadata.DB, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	total, free := int64(200e9), int64(199e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "p1", Type: "drive", DisplayName: "TestDrive",
		RcloneRemote: "fake:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	eng := engine.NewEngineWithCloud(db, dbPath, cloud, b)
	spoolDir := filepath.Join(dir, "spool")
	os.MkdirAll(spoolDir, 0755)
	return eng, cloud, db, spoolDir
}

// ── Error path tests: close DB to break engine operations ──────────────────

func TestRoot_Lookup_DBError(t *testing.T) {
	eng, _, db, spoolDir := newTestEngineWithDB(t)
	defer eng.Close()
	root := NewRoot(eng, spoolDir)
	db.Close()
	var out fuse.EntryOut
	_, errno := root.Lookup(context.Background(), "file.txt", &out)
	if errno == 0 {
		t.Error("expected error from Lookup with closed DB")
	}
}

func TestRoot_Readdir_DBError(t *testing.T) {
	eng, _, db, spoolDir := newTestEngineWithDB(t)
	defer eng.Close()
	root := NewRoot(eng, spoolDir)
	db.Close()
	_, errno := root.Readdir(context.Background())
	if errno == 0 {
		t.Error("expected error from Readdir with closed DB")
	}
}

func TestRoot_Mkdir_DBError(t *testing.T) {
	eng, _, db, spoolDir := newTestEngineWithDB(t)
	defer eng.Close()
	root := NewRoot(eng, spoolDir)
	db.Close()
	var out fuse.EntryOut
	func() {
		defer func() { recover() }() // NewInode may panic
		_, errno := root.Mkdir(context.Background(), "newdir", 0755, &out)
		if errno == 0 {
			t.Error("expected error from Mkdir with closed DB")
		}
	}()
}

func TestRoot_Unlink_DBError(t *testing.T) {
	eng, _, db, spoolDir := newTestEngineWithDB(t)
	defer eng.Close()
	root := NewRoot(eng, spoolDir)
	db.Close()
	errno := root.Unlink(context.Background(), "file.txt")
	if errno == 0 {
		t.Error("expected error from Unlink with closed DB")
	}
}

func TestRoot_Rmdir_DBError(t *testing.T) {
	eng, _, db, spoolDir := newTestEngineWithDB(t)
	defer eng.Close()
	root := NewRoot(eng, spoolDir)
	db.Close()
	errno := root.Rmdir(context.Background(), "mydir")
	if errno == 0 {
		t.Error("expected error from Rmdir with closed DB")
	}
}

func TestRoot_Rename_DBError(t *testing.T) {
	eng, _, db, spoolDir := newTestEngineWithDB(t)
	defer eng.Close()
	root := NewRoot(eng, spoolDir)
	db.Close()
	// With closed DB, IsDir fails (ignored, returns false) → goes to file rename → RenameFile fails
	errno := root.Rename(context.Background(), "old.txt", root, "new.txt", 0)
	if errno == 0 {
		t.Error("expected error from Rename with closed DB")
	}
}

// ── Rename with non-Root parent (exercises else branch of type assertion) ──

func TestRoot_Rename_NonRootParent(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	uploadTestFile(t, eng, "/moveme.txt", []byte("data"))
	root := NewRoot(eng, spoolDir)
	parent := &mockParent{}
	// Non-Root parent → else branch; orphaned Inode → Path(nil) == "" → newParentPath = "/"
	errno := root.Rename(context.Background(), "moveme.txt", parent, "moved.txt", 0)
	if errno != 0 {
		t.Fatalf("Rename nonroot parent errno = %d", errno)
	}
	stat, _ := eng.Stat("/moved.txt")
	if stat == nil {
		t.Error("renamed file not found")
	}
}

// ── Open with writable flag (exercises fuseFlags = FOPEN_DIRECT_IO) ──

func TestRoot_Open_Writable(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	root := NewRoot(eng, spoolDir)
	fh, flags, errno := root.Open(context.Background(), syscall.O_WRONLY)
	if errno != 0 {
		t.Fatalf("Open writable errno = %d", errno)
	}
	if flags&fuse.FOPEN_DIRECT_IO == 0 {
		t.Error("expected FOPEN_DIRECT_IO for writable Open")
	}
	_ = fh
}

// ── FileHandle Getattr error (DB closed → Stat fails) ──

func TestFileHandle_Getattr_DBError(t *testing.T) {
	eng, _, db, spoolDir := newTestEngineWithDB(t)
	defer eng.Close()
	fh := &fuseFileHandle{
		eng: eng, vpath: "/test.txt",
		writable: false, spoolDir: spoolDir,
	}
	db.Close()
	var out fuse.AttrOut
	errno := fh.Getattr(context.Background(), &out)
	if errno == 0 {
		t.Error("expected error from Getattr with closed DB")
	}
}

// ── Flush upload error (DB closed → WriteFileStream fails at metadata step) ──

func TestFileHandle_Flush_UploadDBError(t *testing.T) {
	eng, _, db, spoolDir := newTestEngineWithDB(t)
	defer eng.Close()
	fh := &fuseFileHandle{
		eng: eng, vpath: "/upload-fail.txt",
		writable: true, spoolDir: spoolDir,
	}
	ctx := context.Background()
	fh.Write(ctx, []byte("hello world"), 0)
	db.Close()
	errno := fh.Flush(ctx)
	if errno == 0 {
		t.Error("expected error from Flush with closed DB")
	}
}
