package fusefs

// Integration tests for FUSE filesystem operations that exercise full
// write → read round-trips, directory operations, and rename flows through
// the engine via the FUSE file handle layer.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// ── Write → Flush → Read round-trip ─────────────────────────────────────────

func TestFUSE_WriteFlushReadRoundTrip(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("FUSE round-trip integration test data")

	// Write.
	wh := &fuseFileHandle{eng: eng, vpath: "/fuse-round.txt", writable: true, spoolDir: spoolDir}
	n, errno := wh.Write(context.Background(), content, 0)
	if errno != 0 {
		t.Fatalf("Write errno = %d", errno)
	}
	if int(n) != len(content) {
		t.Fatalf("Write n = %d, want %d", n, len(content))
	}
	if errno = wh.Flush(context.Background()); errno != 0 {
		t.Fatalf("Flush errno = %d", errno)
	}
	wh.Release(context.Background())

	// Read back.
	rh := &fuseFileHandle{eng: eng, vpath: "/fuse-round.txt", writable: false, spoolDir: spoolDir}
	buf := make([]byte, 256)
	result, errno := rh.Read(context.Background(), buf, 0)
	if errno != 0 {
		t.Fatalf("Read errno = %d", errno)
	}
	got, _ := result.Bytes(make([]byte, result.Size()))
	if !bytes.Equal(got, content) {
		t.Errorf("Read got %q, want %q", got, content)
	}
	rh.Release(context.Background())
}

// ── Overwrite updates content ───────────────────────────────────────────────

func TestFUSE_OverwriteUpdatesContent(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)

	write := func(data []byte) {
		fh := &fuseFileHandle{eng: eng, vpath: "/overwrite.txt", writable: true, spoolDir: spoolDir}
		fh.Write(context.Background(), data, 0)
		fh.Flush(context.Background())
		fh.Release(context.Background())
	}
	write([]byte("version 1"))
	write([]byte("version 2 — newer"))

	rh := &fuseFileHandle{eng: eng, vpath: "/overwrite.txt", writable: false, spoolDir: spoolDir}
	buf := make([]byte, 256)
	result, errno := rh.Read(context.Background(), buf, 0)
	if errno != 0 {
		t.Fatalf("Read errno = %d", errno)
	}
	got, _ := result.Bytes(make([]byte, result.Size()))
	if string(got) != "version 2 — newer" {
		t.Errorf("Read got %q, want version 2", got)
	}
	rh.Release(context.Background())
}

// ── Large file write spanning multiple chunks ──────────────────────────────

func TestFUSE_LargeFileMultipleChunks(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	// 256KB should be larger than the small-file-chunk threshold
	content := bytes.Repeat([]byte("ABCDEFGHIJKLMNOP"), 16384) // 256KB

	wh := &fuseFileHandle{eng: eng, vpath: "/large-fuse.bin", writable: true, spoolDir: spoolDir}
	n, errno := wh.Write(context.Background(), content, 0)
	if errno != 0 {
		t.Fatalf("Write errno = %d", errno)
	}
	if int(n) != len(content) {
		t.Fatalf("Write n = %d, want %d", n, len(content))
	}
	if errno = wh.Flush(context.Background()); errno != 0 {
		t.Fatalf("Flush errno = %d", errno)
	}
	wh.Release(context.Background())

	// Read back and verify.
	rh := &fuseFileHandle{eng: eng, vpath: "/large-fuse.bin", writable: false, spoolDir: spoolDir}
	buf := make([]byte, len(content)+100)
	result, errno := rh.Read(context.Background(), buf, 0)
	if errno != 0 {
		t.Fatalf("Read errno = %d", errno)
	}
	got, _ := result.Bytes(make([]byte, result.Size()))
	if !bytes.Equal(got, content) {
		t.Errorf("large file content mismatch: got %d bytes, want %d", len(got), len(content))
	}
	rh.Release(context.Background())
}

// ── Write then read at various offsets ──────────────────────────────────────

func TestFUSE_ReadAtMultipleOffsets(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

	wh := &fuseFileHandle{eng: eng, vpath: "/offsets.txt", writable: true, spoolDir: spoolDir}
	wh.Write(context.Background(), content, 0)
	wh.Flush(context.Background())
	wh.Release(context.Background())

	tests := []struct {
		off  int64
		size int
		want string
	}{
		{0, 10, "0123456789"},
		{10, 6, "ABCDEF"},
		{36, 5, "abcde"},
		{60, 100, "yz"}, // past end
		{62, 10, ""},    // at exact end
	}
	for _, tc := range tests {
		rh := &fuseFileHandle{eng: eng, vpath: "/offsets.txt", writable: false, spoolDir: spoolDir}
		buf := make([]byte, tc.size)
		result, errno := rh.Read(context.Background(), buf, tc.off)
		if errno != 0 {
			t.Errorf("Read(off=%d) errno = %d", tc.off, errno)
			rh.Release(context.Background())
			continue
		}
		got, _ := result.Bytes(make([]byte, result.Size()))
		if string(got) != tc.want {
			t.Errorf("Read(off=%d, size=%d) = %q, want %q", tc.off, tc.size, got, tc.want)
		}
		rh.Release(context.Background())
	}
}

// ── Getattr reflects correct size after write ───────────────────────────────

func TestFUSE_GetattrAfterWrite(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	data := []byte("getattr size check")

	fh := &fuseFileHandle{eng: eng, vpath: "/attr-size.txt", writable: true, spoolDir: spoolDir}
	fh.Write(context.Background(), data, 0)

	var out fuse.AttrOut
	errno := fh.Getattr(context.Background(), &out)
	if errno != 0 {
		t.Fatalf("Getattr errno = %d", errno)
	}
	if out.Size != uint64(len(data)) {
		t.Errorf("Getattr size = %d, want %d", out.Size, len(data))
	}
	fh.Flush(context.Background())
	fh.Release(context.Background())
}

// ── Multiple sequential writes accumulate ───────────────────────────────────

func TestFUSE_SequentialWrites(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)

	fh := &fuseFileHandle{eng: eng, vpath: "/seq-writes.txt", writable: true, spoolDir: spoolDir}
	ctx := context.Background()

	// First write at offset 0.
	fh.Write(ctx, []byte("Hello"), 0)
	// Second write at offset 5.
	fh.Write(ctx, []byte(" World"), 5)
	// Third write extends further.
	fh.Write(ctx, []byte("!!!"), 11)

	if fh.writeSize != 14 {
		t.Errorf("writeSize = %d, want 14", fh.writeSize)
	}

	fh.Flush(ctx)
	fh.Release(ctx)

	// Read back.
	rh := &fuseFileHandle{eng: eng, vpath: "/seq-writes.txt", writable: false, spoolDir: spoolDir}
	buf := make([]byte, 100)
	result, errno := rh.Read(ctx, buf, 0)
	if errno != 0 {
		t.Fatalf("Read errno = %d", errno)
	}
	got, _ := result.Bytes(make([]byte, result.Size()))
	if string(got) != "Hello World!!!" {
		t.Errorf("Read got %q, want %q", got, "Hello World!!!")
	}
	rh.Release(ctx)
}

// ── Release cleans up all temp files ────────────────────────────────────────

func TestFUSE_ReleaseCleansTempFiles(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("cleanup test")
	uploadTestFile(t, eng, "/cleanup-target.txt", content)

	fh := &fuseFileHandle{eng: eng, vpath: "/cleanup-target.txt", writable: false, spoolDir: spoolDir}

	// Trigger read to create readFile.
	buf := make([]byte, 100)
	fh.Read(context.Background(), buf, 0)

	if fh.readFile == nil {
		t.Fatal("readFile should be set after Read")
	}
	readPath := fh.readFile.Name()

	fh.Release(context.Background())

	if fh.readFile != nil {
		t.Error("readFile should be nil after Release")
	}
	if _, err := os.Stat(readPath); !os.IsNotExist(err) {
		t.Error("read temp file should be deleted after Release")
	}
}

// ── Write zero-length file ──────────────────────────────────────────────────

func TestFUSE_WriteEmptyFile(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)

	fh := &fuseFileHandle{eng: eng, vpath: "/empty.txt", writable: true, spoolDir: spoolDir}
	errno := fh.Flush(context.Background())
	if errno != 0 {
		t.Fatalf("Flush empty errno = %d", errno)
	}
	fh.Release(context.Background())

	// The file shouldn't exist (no data was written, Flush is a no-op).
	stat, _ := eng.Stat("/empty.txt")
	if stat != nil {
		t.Error("empty file with no Write should not be created")
	}
}

// ── Concurrent reads on same file ───────────────────────────────────────────

func TestFUSE_ConcurrentReads(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("concurrent read test -- shared data")
	uploadTestFile(t, eng, "/concurrent.txt", content)

	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func() {
			fh := &fuseFileHandle{eng: eng, vpath: "/concurrent.txt", writable: false, spoolDir: spoolDir}
			buf := make([]byte, 256)
			result, errno := fh.Read(context.Background(), buf, 0)
			if errno != 0 {
				errs <- fmt.Errorf("read errno=%d", errno)
				return
			}
			got, _ := result.Bytes(make([]byte, result.Size()))
			if !bytes.Equal(got, content) {
				errs <- fmt.Errorf("content mismatch")
				return
			}
			fh.Release(context.Background())
			errs <- nil
		}()
	}
	for i := 0; i < 5; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent read %d failed: %v", i, err)
		}
	}
}

// ── Getattr on existing file returns correct attributes ─────────────────────

func TestFUSE_GetattrExistingFileDetails(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	content := []byte("exactly 22 bytes long!")
	uploadTestFile(t, eng, "/attrs.txt", content)

	fh := &fuseFileHandle{eng: eng, vpath: "/attrs.txt", writable: false, spoolDir: spoolDir}
	var out fuse.AttrOut
	errno := fh.Getattr(context.Background(), &out)
	if errno != 0 {
		t.Fatalf("Getattr errno = %d", errno)
	}
	if out.Size != 22 {
		t.Errorf("Size = %d, want 22", out.Size)
	}
	if out.Mode&0644 == 0 {
		t.Errorf("Mode = %o, expected file permissions", out.Mode)
	}
	fh.Release(context.Background())
}

// ── Write with spoolDir set uses spool directory ────────────────────────────

func TestFUSE_WriteUsesSpoolDir(t *testing.T) {
	eng, _, spoolDir := newTestEngine(t)
	fh := &fuseFileHandle{eng: eng, vpath: "/spool-test.txt", writable: true, spoolDir: spoolDir}
	fh.Write(context.Background(), []byte("spool write"), 0)

	if fh.tmpFile == nil {
		t.Fatal("tmpFile should be created after Write")
	}
	if fh.tmpPath == "" {
		t.Fatal("tmpPath should be set")
	}
	// Verify the temp file is in the spool directory.
	if !bytes.HasPrefix([]byte(fh.tmpPath), []byte(spoolDir)) {
		t.Errorf("tmpPath %q not in spoolDir %q", fh.tmpPath, spoolDir)
	}

	fh.Flush(context.Background())
	fh.Release(context.Background())
}
