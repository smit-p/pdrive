package daemon

import (
	"context"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// New
// ---------------------------------------------------------------------------

func TestDaemon_New(t *testing.T) {
	cfg := Config{ConfigDir: "/tmp/test", WebDAVAddr: "127.0.0.1:0"}
	d := New(cfg)
	if d == nil {
		t.Fatal("New returned nil")
	}
	if d.config.ConfigDir != "/tmp/test" {
		t.Errorf("config.ConfigDir = %q", d.config.ConfigDir)
	}
}

// ---------------------------------------------------------------------------
// Engine before Start
// ---------------------------------------------------------------------------

func TestDaemon_Engine_BeforeStart(t *testing.T) {
	d := New(Config{})
	if d.Engine() != nil {
		t.Error("Engine() should return nil before Start()")
	}
}

// ---------------------------------------------------------------------------
// Stop before Start (should not panic)
// ---------------------------------------------------------------------------

func TestDaemon_Stop_BeforeStart(t *testing.T) {
	d := New(Config{})
	d.Stop() // should not panic (all fields are nil)
}

// ---------------------------------------------------------------------------
// Full Start / Stop lifecycle using fake rclone binary
// ---------------------------------------------------------------------------

func TestDaemon_StartStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	fakeRclone := buildFakeRclone(t)
	rclonePort := freePort(t)
	httpPort := freePort(t)
	dir := t.TempDir()

	cfg := Config{
		ConfigDir:   dir,
		RcloneBin:   fakeRclone,
		RcloneAddr:  rclonePort,
		WebDAVAddr:  httpPort,
		SkipRestore: true,
	}

	d := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Engine should be available now.
	if d.Engine() == nil {
		t.Error("Engine() should not be nil after Start()")
	}

	// DB should be open.
	if d.db == nil {
		t.Error("db should not be nil after Start()")
	}

	// Rclone should be running.
	if d.rclone == nil {
		t.Error("rclone should not be nil")
	}

	// Give the HTTP server a moment to start.
	time.Sleep(50 * time.Millisecond)

	// Stop should succeed.
	d.Stop()
	cancel()

	// Engine should still be non-nil (it's not zeroed, just closed).
	// But db closure won't panic on repeated stop.
}

func TestDaemon_Start_BadRclone(t *testing.T) {
	dir := t.TempDir()
	httpPort := freePort(t)

	cfg := Config{
		ConfigDir:  dir,
		RcloneBin:  "/nonexistent/rclone",
		RcloneAddr: "127.0.0.1:19999",
		WebDAVAddr: httpPort,
	}

	d := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := d.Start(ctx)
	if err == nil {
		d.Stop()
		t.Fatal("Start should fail with bad rclone binary")
	}
}
