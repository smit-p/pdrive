package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_MissingFile(t *testing.T) {
	dir := t.TempDir()
	f, err := Load(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f != (File{}) {
		t.Fatalf("expected zero-value File, got %+v", f)
	}
}

func TestLoad_ValidTOML(t *testing.T) {
	dir := t.TempDir()
	toml := `
sync_dir       = "/tmp/sync"
rclone_addr    = "127.0.0.1:9999"
webdav_addr    = "127.0.0.1:7777"
rclone_bin     = "/usr/local/bin/rclone"
broker_policy  = "mfs"
min_free_space = 1000000
chunk_size     = 64
rate_limit     = 10
debug          = true
remotes        = "gdrive,dropbox"
mount_backend  = "fuse"
mount_point    = "/mnt/pdrive"
`
	if err := os.WriteFile(filepath.Join(dir, "config.toml"), []byte(toml), 0644); err != nil {
		t.Fatal(err)
	}

	f, err := Load(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.SyncDir != "/tmp/sync" {
		t.Errorf("SyncDir = %q, want /tmp/sync", f.SyncDir)
	}
	if f.RcloneAddr != "127.0.0.1:9999" {
		t.Errorf("RcloneAddr = %q, want 127.0.0.1:9999", f.RcloneAddr)
	}
	if f.WebDAVAddr != "127.0.0.1:7777" {
		t.Errorf("WebDAVAddr = %q, want 127.0.0.1:7777", f.WebDAVAddr)
	}
	if f.RcloneBin != "/usr/local/bin/rclone" {
		t.Errorf("RcloneBin = %q", f.RcloneBin)
	}
	if f.BrokerPolicy != "mfs" {
		t.Errorf("BrokerPolicy = %q", f.BrokerPolicy)
	}
	if f.MinFreeSpace != 1000000 {
		t.Errorf("MinFreeSpace = %d", f.MinFreeSpace)
	}
	if f.ChunkSize != 64 {
		t.Errorf("ChunkSize = %d", f.ChunkSize)
	}
	if f.RateLimit != 10 {
		t.Errorf("RateLimit = %d", f.RateLimit)
	}
	if !f.Debug {
		t.Error("Debug = false, want true")
	}
	if f.Remotes != "gdrive,dropbox" {
		t.Errorf("Remotes = %q", f.Remotes)
	}
	if f.MountBackend != "fuse" {
		t.Errorf("MountBackend = %q, want fuse", f.MountBackend)
	}
	if f.MountPoint != "/mnt/pdrive" {
		t.Errorf("MountPoint = %q, want /mnt/pdrive", f.MountPoint)
	}
}

func TestLoad_PartialTOML(t *testing.T) {
	dir := t.TempDir()
	toml := `mount_backend = "webdav"`
	if err := os.WriteFile(filepath.Join(dir, "config.toml"), []byte(toml), 0644); err != nil {
		t.Fatal(err)
	}

	f, err := Load(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.MountBackend != "webdav" {
		t.Errorf("MountBackend = %q, want webdav", f.MountBackend)
	}
	if f.SyncDir != "" {
		t.Errorf("SyncDir = %q, want empty", f.SyncDir)
	}
	if f.Debug {
		t.Error("Debug should be false")
	}
}

func TestLoad_InvalidTOML(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.toml"), []byte("{{invalid"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(dir)
	if err == nil {
		t.Fatal("expected parse error for invalid TOML")
	}
}

func TestLoad_UnreadableDir(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	// Create a directory where the file should be — ReadFile will fail.
	if err := os.MkdirAll(configPath, 0755); err != nil {
		t.Fatal(err)
	}

	_, err := Load(dir)
	if err == nil {
		t.Fatal("expected error when config.toml is a directory")
	}
}
