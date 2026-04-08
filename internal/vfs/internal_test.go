package vfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// ── shouldSkipPath ──

func TestShouldSkipPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/foo/bar.txt", false},
		{"/.DS_Store", true},
		{"/dir/.DS_Store", true},
		{"/._something", true},
		{"/dir/._foo", true},
		{"/.pdrive", true},
		{"/.pdrive-backup", true},
		{"/normal.txt", false},
	}
	for _, tt := range tests {
		if got := shouldSkipPath(tt.path); got != tt.want {
			t.Errorf("shouldSkipPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

// ── shouldSkipDir ──

func TestShouldSkipDir(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{".pdrive", true},
		{".Trash", true},
		{".Trashes", true},
		{".Spotlight-V100", true},
		{".fseventsd", true},
		{"Documents", false},
		{"src", false},
	}
	for _, tt := range tests {
		if got := shouldSkipDir(tt.name); got != tt.want {
			t.Errorf("shouldSkipDir(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// ── createStubFile / isStubFile / clearStubMarker ──

func TestCreateStubFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.stub")

	if err := createStubFile(path, 42); err != nil {
		t.Fatal(err)
	}

	// File should exist and be empty.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Errorf("expected 0-byte file, got %d", info.Size())
	}

	// Xattr should mark it as stub.
	buf := make([]byte, 8)
	n, err := unix.Getxattr(path, xattrStub, buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "1" {
		t.Errorf("expected xattr stub=1, got %q", string(buf[:n]))
	}

	// Size xattr should be "42".
	n, err = unix.Getxattr(path, xattrSize, buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "42" {
		t.Errorf("expected xattr size=42, got %q", string(buf[:n]))
	}
}

func TestIsStubFile_True(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.stub")
	if err := createStubFile(path, 100); err != nil {
		t.Fatal(err)
	}
	if !isStubFile(path) {
		t.Error("expected isStubFile=true for a stub file")
	}
}

func TestIsStubFile_False_RegularFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "regular.txt")
	os.WriteFile(path, []byte("hello"), 0644)
	if isStubFile(path) {
		t.Error("expected isStubFile=false for regular file")
	}
}

func TestIsStubFile_False_NonExistent(t *testing.T) {
	if isStubFile("/nonexistent/path/file.txt") {
		t.Error("expected isStubFile=false for nonexistent file")
	}
}

func TestClearStubMarker(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.stub")
	if err := createStubFile(path, 99); err != nil {
		t.Fatal(err)
	}

	clearStubMarker(path)

	if isStubFile(path) {
		t.Error("expected isStubFile=false after clearStubMarker")
	}
	// Size xattr should also be gone.
	buf := make([]byte, 8)
	_, err := unix.Getxattr(path, xattrSize, buf)
	if err == nil {
		t.Error("expected error reading size xattr after clear")
	}
}

func TestCreateStubFile_BadPath(t *testing.T) {
	err := createStubFile("/nonexistent/dir/file.stub", 10)
	if err == nil {
		t.Error("expected error for bad path")
	}
}

// ── NewSyncDir ──

func TestNewSyncDir(t *testing.T) {
	sd := NewSyncDir("/tmp/test", nil, "/tmp/spool")
	if sd.root != "/tmp/test" {
		t.Errorf("unexpected root: %s", sd.root)
	}
	if sd.spoolDir != "/tmp/spool" {
		t.Errorf("unexpected spoolDir: %s", sd.spoolDir)
	}
	if sd.pending == nil || sd.removals == nil || sd.suppress == nil {
		t.Error("expected maps to be initialized")
	}
}

func TestSyncDir_Root(t *testing.T) {
	sd := NewSyncDir("/my/root", nil, "")
	if sd.Root() != "/my/root" {
		t.Errorf("Root() = %q, want %q", sd.Root(), "/my/root")
	}
}

func TestSyncDir_VirtualPath(t *testing.T) {
	sd := NewSyncDir("/sync", nil, "")
	tests := []struct {
		abs  string
		want string
	}{
		{"/sync/foo.txt", "/foo.txt"},
		{"/sync/a/b/c.txt", "/a/b/c.txt"},
		{"/sync", "/."},
	}
	for _, tt := range tests {
		got := sd.virtualPath(tt.abs)
		if got != tt.want {
			t.Errorf("virtualPath(%q) = %q, want %q", tt.abs, got, tt.want)
		}
	}
}

func TestSyncDir_SuppressEvent(t *testing.T) {
	sd := NewSyncDir("/sync", nil, "")
	sd.suppressEvent("/sync/file.txt")
	sd.supMu.Lock()
	expiry, ok := sd.suppress["/sync/file.txt"]
	sd.supMu.Unlock()
	if !ok {
		t.Error("expected file to be in suppress map")
	}
	if expiry.Before(time.Now()) {
		t.Error("expected suppress expiry to be in the future")
	}
}

// ── hashLocalFile ──

func TestHashLocalFile_Valid(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "data.bin")
	os.WriteFile(p, []byte("hello"), 0644)
	h, err := hashLocalFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// SHA256 of "hello"
	want := "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if h != want {
		t.Errorf("hashLocalFile = %q, want %q", h, want)
	}
}

func TestHashLocalFile_NonExistent(t *testing.T) {
	_, err := hashLocalFile("/nonexistent/file.txt")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestHashLocalFile_Empty(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "empty.bin")
	os.WriteFile(p, nil, 0644)
	h, err := hashLocalFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// SHA256 of empty string
	want := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if h != want {
		t.Errorf("hashLocalFile(empty) = %q, want %q", h, want)
	}
}
