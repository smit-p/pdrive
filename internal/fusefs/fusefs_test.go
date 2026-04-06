package fusefs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/rclonerc"
)

type fakeCloud struct {
	mu      sync.Mutex
	objects map[string][]byte
	putErr  error
}

func newFakeCloud() *fakeCloud { return &fakeCloud{objects: make(map[string][]byte)} }

func (f *fakeCloud) key(remote, p string) string { return remote + ":" + p }

func (f *fakeCloud) PutFile(remote, p string, r io.Reader) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErr != nil {
		return f.putErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.objects[f.key(remote, p)] = data
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
	defer f.mu.Unlock()
	delete(f.objects, f.key(remote, p))
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

func (f *fakeCloud) Cleanup(remote string) error     { return nil }
func (f *fakeCloud) Mkdir(remote, path string) error { return nil }
func (f *fakeCloud) TransferStats() rclonerc.TransferProgress {
	return rclonerc.TransferProgress{}
}

func newTestEngine(t *testing.T) (*engine.Engine, *fakeCloud, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	total, free := int64(200e9), int64(199e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "p1", Type: "drive", DisplayName: "TestDrive",
		RcloneRemote: "fake:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, cloud, b, encKey)
	t.Cleanup(eng.Close)
	spoolDir := filepath.Join(dir, "spool")
	os.MkdirAll(spoolDir, 0755)
	return eng, cloud, spoolDir
}

func uploadTestFile(t *testing.T, eng *engine.Engine, vpath string, data []byte) {
	t.Helper()
	tmp, err := os.CreateTemp("", "fusefs-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		t.Fatal(err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		tmp.Close()
		t.Fatal(err)
	}
	err = eng.WriteFileStream(vpath, tmp, int64(len(data)))
	tmp.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewRoot(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	root := NewRoot(eng, spoolDir)
	if root == nil {
		t.Fatal("NewRoot returned nil")
	}
	if root.eng != eng {
		t.Error("engine not set")
	}
	if root.spoolDir != spoolDir {
		t.Error("spoolDir not set")
	}
}

func TestRoot_Getattr(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	root := NewRoot(eng, spoolDir)
	var out fuse.AttrOut
	errno := root.Getattr(context.Background(), nil, &out)
	if errno != 0 {
		t.Fatalf("Getattr errno = %d", errno)
	}
	if out.Mode&0777 != 0755 {
		t.Errorf("mode = %o, want 0755", out.Mode&0777)
	}
}

func TestRoot_Statfs(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	root := NewRoot(eng, spoolDir)
	var out fuse.StatfsOut
	errno := root.Statfs(context.Background(), &out)
	if errno != 0 {
		t.Fatalf("Statfs errno = %d", errno)
	}
	if out.Bsize != 4096 {
		t.Errorf("Bsize = %d, want 4096", out.Bsize)
	}
	if out.NameLen != 255 {
		t.Errorf("NameLen = %d, want 255", out.NameLen)
	}
}

func TestRoot_Setattr(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	root := NewRoot(eng, spoolDir)
	var out fuse.AttrOut
	var in fuse.SetAttrIn
	errno := root.Setattr(context.Background(), nil, &in, &out)
	if errno != 0 {
		t.Fatalf("Setattr errno = %d", errno)
	}
}

func TestFileHandle_WriteAndFlush(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/test-write.txt",
		writable: true, spoolDir: spoolDir,
	}
	ctx := context.Background()
	data := []byte("hello world from FUSE write test")
	n, errno := fh.Write(ctx, data, 0)
	if errno != 0 {
		t.Fatalf("Write errno = %d", errno)
	}
	if int(n) != len(data) {
		t.Errorf("Write n = %d, want %d", n, len(data))
	}
	errno = fh.Flush(ctx)
	if errno != 0 {
		t.Fatalf("Flush errno = %d", errno)
	}
	stat, err := eng.Stat("/test-write.txt")
	if err != nil {
		t.Fatal(err)
	}
	if stat == nil {
		t.Fatal("file not found after Flush")
	}
	if stat.SizeBytes != int64(len(data)) {
		t.Errorf("file size = %d, want %d", stat.SizeBytes, len(data))
	}
}

func TestFileHandle_WriteAtOffset(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/offset-write.txt",
		writable: true, spoolDir: spoolDir,
	}
	data := []byte("XYZ")
	n, errno := fh.Write(context.Background(), data, 10)
	if errno != 0 {
		t.Fatalf("Write errno = %d", errno)
	}
	if int(n) != 3 {
		t.Errorf("Write n = %d, want 3", n)
	}
	if fh.writeSize != 13 {
		t.Errorf("writeSize = %d, want 13", fh.writeSize)
	}
}

func TestFileHandle_WriteNotWritable(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/readonly.txt",
		writable: false, spoolDir: spoolDir,
	}
	_, errno := fh.Write(context.Background(), []byte("nope"), 0)
	if errno == 0 {
		t.Error("expected EBADF for non-writable handle")
	}
}

func TestFileHandle_ReadExistingFile(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("content for read test - this is some data")
	uploadTestFile(t, eng, "/read-me.txt", content)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/read-me.txt",
		writable: false, spoolDir: spoolDir,
	}
	buf := make([]byte, 100)
	result, errno := fh.Read(context.Background(), buf, 0)
	if errno != 0 {
		t.Fatalf("Read errno = %d", errno)
	}
	got, _ := result.Bytes(make([]byte, result.Size()))
	if !bytes.Equal(got, content) {
		t.Errorf("Read got %q, want %q", got, content)
	}
}

func TestFileHandle_ReadAtOffset(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("0123456789ABCDEF")
	uploadTestFile(t, eng, "/offset-read.txt", content)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/offset-read.txt",
		writable: false, spoolDir: spoolDir,
	}
	buf := make([]byte, 4)
	result, errno := fh.Read(context.Background(), buf, 10)
	if errno != 0 {
		t.Fatalf("Read errno = %d", errno)
	}
	got, _ := result.Bytes(make([]byte, result.Size()))
	if string(got) != "ABCD" {
		t.Errorf("Read offset 10 got %q, want ABCD", got)
	}
}

func TestFileHandle_ReadNonexistent(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/no-such-file.txt",
		writable: false, spoolDir: spoolDir,
	}
	buf := make([]byte, 100)
	_, errno := fh.Read(context.Background(), buf, 0)
	if errno == 0 {
		t.Error("expected error reading non-existent file")
	}
}

func TestFileHandle_Release(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("release me")
	uploadTestFile(t, eng, "/release-test.txt", content)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/release-test.txt",
		writable: false, spoolDir: spoolDir,
	}
	buf := make([]byte, 100)
	fh.Read(context.Background(), buf, 0)
	if fh.readFile == nil {
		t.Fatal("readFile should be set after Read")
	}
	fh.writable = true
	fh.Write(context.Background(), []byte("x"), 0)
	if fh.tmpFile == nil {
		t.Fatal("tmpFile should be set after Write")
	}
	tmpPath := fh.tmpPath
	errno := fh.Release(context.Background())
	if errno != 0 {
		t.Fatalf("Release errno = %d", errno)
	}
	if fh.readFile != nil {
		t.Error("readFile should be nil after Release")
	}
	if fh.tmpFile != nil {
		t.Error("tmpFile should be nil after Release")
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("tmp file should be deleted after Release")
	}
}

func TestFileHandle_FlushNoWrite(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/no-write.txt",
		writable: true, spoolDir: spoolDir,
	}
	errno := fh.Flush(context.Background())
	if errno != 0 {
		t.Fatalf("Flush errno = %d (expected no-op)", errno)
	}
}

func TestFileHandle_FlushReadOnly(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/readonly.txt",
		writable: false, spoolDir: spoolDir,
	}
	errno := fh.Flush(context.Background())
	if errno != 0 {
		t.Fatalf("Flush errno = %d (expected no-op)", errno)
	}
}

func TestFileHandle_GetattrWritable(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/getattr-write.txt",
		writable: true, spoolDir: spoolDir,
	}
	fh.Write(context.Background(), []byte("hello attrs"), 0)
	var out fuse.AttrOut
	errno := fh.Getattr(context.Background(), &out)
	if errno != 0 {
		t.Fatalf("Getattr errno = %d", errno)
	}
	if out.Size != 11 {
		t.Errorf("Size = %d, want 11", out.Size)
	}
}

func TestFileHandle_GetattrExistingFile(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("stat me please")
	uploadTestFile(t, eng, "/stat-test.txt", content)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/stat-test.txt",
		writable: false, spoolDir: spoolDir,
	}
	var out fuse.AttrOut
	errno := fh.Getattr(context.Background(), &out)
	if errno != 0 {
		t.Fatalf("Getattr errno = %d", errno)
	}
	if out.Size != uint64(len(content)) {
		t.Errorf("Size = %d, want %d", out.Size, len(content))
	}
}

func TestFileHandle_GetattrNonexistent(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/no-such.txt",
		writable: false, spoolDir: spoolDir,
	}
	var out fuse.AttrOut
	errno := fh.Getattr(context.Background(), &out)
	if errno != 0 {
		t.Fatalf("Getattr errno = %d", errno)
	}
}

func TestServer_UnmountNilServer(t *testing.T) {
	s := &Server{mountPoint: "/tmp/test"}
	err := s.Unmount()
	if err != nil {
		t.Errorf("Unmount nil server = %v, want nil", err)
	}
}

func TestServer_WaitNilServer(t *testing.T) {
	s := &Server{mountPoint: "/tmp/test"}
	s.Wait()
}

func TestServer_MountPoint(t *testing.T) {
	s := &Server{mountPoint: "/mnt/pdrive"}
	if got := s.MountPoint(); got != "/mnt/pdrive" {
		t.Errorf("MountPoint() = %q, want /mnt/pdrive", got)
	}
}

func TestMount_NoFUSEDriver(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	dir := t.TempDir()
	mountPoint := filepath.Join(dir, "mnt")
	srv, err := Mount(mountPoint, eng, spoolDir)
	if err == nil {
		// FUSE mounted successfully — unmount before skipping so TempDir cleanup works.
		srv.Unmount()
		srv.Wait()
		t.Skip("FUSE is available; skipping no-driver test")
	}
	// Error could come from CheckFUSEAvailable or from the mount call itself.
	errMsg := err.Error()
	if !strings.Contains(errMsg, "FUSE") && !strings.Contains(errMsg, "macFUSE") && !strings.Contains(errMsg, "fuse") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCleanup(t *testing.T) {
	dir := t.TempDir()
	tmp, err := os.CreateTemp(dir, "cleanup-test-*")
	if err != nil {
		t.Fatal(err)
	}
	name := tmp.Name()
	fh := &fuseFileHandle{tmpFile: tmp, tmpPath: name}
	fh.cleanup()
	if fh.tmpFile != nil {
		t.Error("tmpFile not nil after cleanup")
	}
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Error("temp file not removed by cleanup")
	}
}

func TestCleanup_NilTmpFile(t *testing.T) {
	fh := &fuseFileHandle{}
	fh.cleanup()
}

func TestAttrTimeout(t *testing.T) {
	if attrTimeout != 5*time.Second {
		t.Errorf("attrTimeout = %v, want 5s", attrTimeout)
	}
}

func TestEnsureReadFile_Caching(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("cached read")
	uploadTestFile(t, eng, "/cached.txt", content)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/cached.txt",
		writable: false, spoolDir: spoolDir,
	}
	if err := fh.ensureReadFile(); err != nil {
		t.Fatal(err)
	}
	first := fh.readFile
	if first == nil {
		t.Fatal("readFile should be set")
	}
	if err := fh.ensureReadFile(); err != nil {
		t.Fatal(err)
	}
	if fh.readFile != first {
		t.Error("ensureReadFile should reuse existing file handle")
	}
}

func TestFileHandle_FlushAsyncLargeFile(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/large-async.bin",
		writable: true, spoolDir: spoolDir,
	}
	ctx := context.Background()
	// Write > 4MB to trigger async upload path.
	size := 5 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	n, errno := fh.Write(ctx, data, 0)
	if errno != 0 {
		t.Fatalf("Write errno = %d", errno)
	}
	if int(n) != size {
		t.Errorf("Write n = %d, want %d", n, size)
	}
	errno = fh.Flush(ctx)
	if errno != 0 {
		t.Fatalf("Flush errno = %d", errno)
	}
	// After async flush the tmpFile should be transferred to the engine.
	if fh.tmpFile != nil {
		t.Error("tmpFile should be nil after async flush (engine owns it)")
	}
	// Wait a moment for async upload to complete and verify.
	time.Sleep(500 * time.Millisecond)
	stat, err := eng.Stat("/large-async.bin")
	if err != nil {
		t.Fatal(err)
	}
	if stat == nil {
		t.Fatal("file not found after async Flush")
	}
}

func TestFileHandle_ReadEOFBeyondEnd(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("short")
	uploadTestFile(t, eng, "/short.txt", content)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/short.txt",
		writable: false, spoolDir: spoolDir,
	}
	// Read beyond end of file — should return 0 bytes, no error.
	buf := make([]byte, 100)
	result, errno := fh.Read(context.Background(), buf, 1000)
	if errno != 0 {
		t.Fatalf("Read beyond EOF errno = %d", errno)
	}
	got, _ := result.Bytes(make([]byte, result.Size()))
	if len(got) != 0 {
		t.Errorf("Read beyond EOF: got %d bytes, want 0", len(got))
	}
}

func TestFileHandle_GetattrStatError(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	// Writable handle without tmpFile, non-existent path.
	fh := &fuseFileHandle{
		eng: eng, vpath: "/nofile-getattr.txt",
		writable: true, spoolDir: spoolDir,
	}
	var out fuse.AttrOut
	errno := fh.Getattr(context.Background(), &out)
	if errno != 0 {
		t.Fatalf("Getattr errno = %d", errno)
	}
	// File doesn't exist and no tmpFile: returns empty attr.
	if out.Mode&0777 != 0644 {
		t.Errorf("mode = %o, want 0644", out.Mode&0777)
	}
}

func TestFileHandle_WriteBadSpoolDir(t *testing.T) {
	eng, _, _ := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/fail.txt",
		writable: true, spoolDir: "/nonexistent-dir-xyz",
	}
	_, errno := fh.Write(context.Background(), []byte("data"), 0)
	if errno == 0 {
		t.Error("expected error when spoolDir doesn't exist")
	}
}

func TestFileHandle_ReadEnsureReadFileError(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	// File doesn't exist — ReadFileToTempFile should fail.
	fh := &fuseFileHandle{
		eng: eng, vpath: "/does-not-exist-read.txt",
		writable: false, spoolDir: spoolDir,
	}
	_, errno := fh.Read(context.Background(), make([]byte, 100), 0)
	if errno == 0 {
		t.Error("expected error reading nonexistent file")
	}
}

func TestFileHandle_ReleaseWithReadFile(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("release-test-content")
	uploadTestFile(t, eng, "/release-test.txt", content)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/release-test.txt",
		writable: false, spoolDir: spoolDir,
	}
	// Trigger ensureReadFile by doing a Read first.
	buf := make([]byte, len(content))
	_, errno := fh.Read(context.Background(), buf, 0)
	if errno != 0 {
		t.Fatalf("Read errno = %d", errno)
	}
	// Now readFile is open. Release should close and remove it.
	errno = fh.Release(context.Background())
	if errno != 0 {
		t.Errorf("Release errno = %d", errno)
	}
	if fh.readFile != nil {
		t.Error("readFile should be nil after Release")
	}
}

func TestFileHandle_FlushSyncWriteError(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/sync-flush.txt",
		writable: true, spoolDir: spoolDir,
	}
	// Write small data (< 4MB) so it takes the sync path.
	_, errno := fh.Write(context.Background(), []byte("hello"), 0)
	if errno != 0 {
		t.Fatalf("Write errno = %d", errno)
	}
	// Normal sync flush should succeed.
	errno = fh.Flush(context.Background())
	if errno != 0 {
		t.Errorf("Flush errno = %d", errno)
	}
}

func TestFileHandle_DoubleFlush(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/double-flush.txt",
		writable: true, spoolDir: spoolDir,
	}
	_, errno := fh.Write(context.Background(), []byte("data"), 0)
	if errno != 0 {
		t.Fatalf("Write errno = %d", errno)
	}
	errno = fh.Flush(context.Background())
	if errno != 0 {
		t.Fatalf("First Flush errno = %d", errno)
	}
	// Second flush with no tmpFile — should be a no-op.
	errno = fh.Flush(context.Background())
	if errno != 0 {
		t.Errorf("Second Flush (no-op) errno = %d", errno)
	}
}

func TestCheckFUSEAvailable(t *testing.T) {
	// Just verify it returns without panicking. It will return nil or an error
	// depending on whether macFUSE/FUSE is installed on the test machine.
	err := CheckFUSEAvailable()
	if err != nil {
		t.Logf("FUSE not available (expected in CI): %v", err)
	}
}

func TestServer_MountBadDir(t *testing.T) {
	eng, _, _ := newTestEngine(t)
	// Mount should fail — either FUSE is unavailable or the path is unwritable.
	_, err := Mount("/proc/fake-no-create/mount", eng, t.TempDir())
	if err == nil {
		t.Error("expected error mounting at unwritable path")
	}
}

func TestFileHandle_ReadAtError(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("readat-error-test")
	uploadTestFile(t, eng, "/readat-err.txt", content)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/readat-err.txt",
		writable: false, spoolDir: spoolDir,
	}
	// Read once to download the file.
	buf := make([]byte, len(content))
	_, errno := fh.Read(context.Background(), buf, 0)
	if errno != 0 {
		t.Fatalf("initial Read errno = %d", errno)
	}
	// Close the underlying file to make ReadAt fail.
	fh.readFile.Close()
	_, errno = fh.Read(context.Background(), buf, 0)
	if errno == 0 {
		t.Error("expected error after closing readFile")
	}
}

func TestFileHandle_FlushSyncError(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/flush-sync-err.txt",
		writable: true, spoolDir: spoolDir,
	}
	_, errno := fh.Write(context.Background(), []byte("data"), 0)
	if errno != 0 {
		t.Fatalf("Write errno = %d", errno)
	}
	// Close the tmpFile to make Sync fail.
	fh.tmpFile.Close()
	errno = fh.Flush(context.Background())
	if errno == 0 {
		t.Error("expected error when Sync fails on closed file")
	}
}

func TestFileHandle_WriteAtError(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{
		eng: eng, vpath: "/writeat-err.txt",
		writable: true, spoolDir: spoolDir,
	}
	// Write initially to create tmpFile.
	_, errno := fh.Write(context.Background(), []byte("init"), 0)
	if errno != 0 {
		t.Fatalf("initial Write errno = %d", errno)
	}
	// Close tmpFile to make next WriteAt fail.
	fh.tmpFile.Close()
	_, errno = fh.Write(context.Background(), []byte("fail"), 0)
	if errno == 0 {
		t.Error("expected error after closing tmpFile")
	}
}
