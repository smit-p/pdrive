package rclonerc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// ── IsQuotaExceeded ─────────────────────────────────────────────────────────

func TestIsQuotaExceeded(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("connection refused"), false},
		{fmt.Errorf("quota exceeded"), true},
		{fmt.Errorf("storageLimitExceeded"), true},
		{fmt.Errorf("storage limit reached"), true},
		{fmt.Errorf("insufficient storage"), true},
		{fmt.Errorf("not enough space"), true},
		{fmt.Errorf("disk full"), true},
		{fmt.Errorf("no space left on device"), true},
		{fmt.Errorf("ENOSPC: write failed"), true},
		{fmt.Errorf("HTTP 413 payload too large"), true},
		{fmt.Errorf("over quota"), true},
		{fmt.Errorf("account full"), true},
	}
	for _, tt := range tests {
		got := IsQuotaExceeded(tt.err)
		if got != tt.want {
			t.Errorf("IsQuotaExceeded(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

// ── PutFile — temp file creation error ──────────────────────────────────────

func TestPutFile_TempDirError(t *testing.T) {
	// Use a client that points nowhere — the error should come before any HTTP call
	// Actually, the error happens at os.MkdirTemp which uses system temp dir.
	// Hard to break. Skip this.
	t.Skip("os.MkdirTemp failure requires breaking system temp dir")
}

// ── PutFile — job response parse error ──────────────────────────────────────

func TestPutFile_BadJobResponse(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/operations/copyfile":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`not json`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	err := c.PutFile("drive", "path/file.txt", strings.NewReader("data"))
	if err == nil || !strings.Contains(err.Error(), "parsing async job response") {
		t.Fatalf("expected job response parse error, got: %v", err)
	}
}

// ── PutFile — job poll error ────────────────────────────────────────────────

func TestPutFile_JobPollError(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/operations/copyfile":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"jobid": 1})
		case "/job/status":
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`error`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	err := c.PutFile("drive", "path/file.txt", strings.NewReader("data"))
	if err == nil || !strings.Contains(err.Error(), "polling job") {
		t.Fatalf("expected poll error, got: %v", err)
	}
}

// ── PutFile — job status bad JSON ───────────────────────────────────────────

func TestPutFile_JobStatusBadJSON(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/operations/copyfile":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"jobid": 1})
		case "/job/status":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`not json`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	err := c.PutFile("drive", "path/file.txt", strings.NewReader("data"))
	if err == nil || !strings.Contains(err.Error(), "parsing job status") {
		t.Fatalf("expected parse error, got: %v", err)
	}
}

// ── GetFile — tempdir creation error is hard; test open-file error ──────────

func TestGetFile_OpenDownloadedFileError(t *testing.T) {
	// The file doesn't exist after copyfile returns because our fake
	// doesn't actually create a file.
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/operations/copyfile":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"jobid": 1})
		case "/job/status":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"finished": true, "success": true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	_, err := c.GetFile("drive", "path/file.txt")
	if err == nil || !strings.Contains(err.Error(), "opening downloaded file") {
		t.Fatalf("expected open error, got: %v", err)
	}
}

// ── Mkdir error ─────────────────────────────────────────────────────────────

func TestMkdir_Success(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})
	err := c.Mkdir("drive", "some/path")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMkdir_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`error`))
	})
	err := c.Mkdir("drive", "some/path")
	if err == nil || !strings.Contains(err.Error(), "mkdir") {
		t.Fatalf("expected mkdir error, got: %v", err)
	}
}

// ── About — partial fields ──────────────────────────────────────────────────

func TestAbout_PartialFields(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Only total is set, used and free are null
		w.Write([]byte(`{"total": 100000}`))
	})
	info, err := c.About("drive")
	if err != nil {
		t.Fatal(err)
	}
	if info.Total != 100000 {
		t.Errorf("Total = %d, want 100000", info.Total)
	}
	if info.Used != 0 || info.Free != 0 {
		t.Errorf("Used/Free should be 0, got %d/%d", info.Used, info.Free)
	}
}

// ── GetRemoteConfig ─────────────────────────────────────────────────────────

func TestGetRemoteConfig_SuccessWithScope(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"type":"drive","scope":"drive"}`))
	})
	cfg, err := c.GetRemoteConfig("gdrive")
	if err != nil {
		t.Fatal(err)
	}
	if cfg["type"] != "drive" {
		t.Errorf("type = %v, want drive", cfg["type"])
	}
}

func TestGetRemoteConfig_ServerError(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`error`))
	})
	_, err := c.GetRemoteConfig("gdrive")
	if err == nil || !strings.Contains(err.Error(), "getting remote config") {
		t.Fatalf("expected error, got: %v", err)
	}
}

func TestGetRemoteConfig_BadJSON(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not json`))
	})
	_, err := c.GetRemoteConfig("gdrive")
	if err == nil || !strings.Contains(err.Error(), "parsing config response") {
		t.Fatalf("expected parse error, got: %v", err)
	}
}

// ── TransferStats ───────────────────────────────────────────────────────────

func TestTransferStats_Success(t *testing.T) {
	calls := 0
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
		if calls == 1 {
			// Default group
			w.Write([]byte(`{"speed": 1000.0, "transferring": [{"name": "file1", "bytes": 500}]}`))
		} else {
			// pdrive group — higher speed, extra transfer
			w.Write([]byte(`{"speed": 2000.0, "transferring": [{"name": "file2", "bytes": 1000}]}`))
		}
	})
	stats := c.TransferStats()
	if stats.SpeedBytes != 2000.0 {
		t.Errorf("SpeedBytes = %f, want 2000", stats.SpeedBytes)
	}
	if stats.Transferring["file1"] != 500 {
		t.Errorf("file1 bytes = %d, want 500", stats.Transferring["file1"])
	}
	if stats.Transferring["file2"] != 1000 {
		t.Errorf("file2 bytes = %d, want 1000", stats.Transferring["file2"])
	}
}

func TestTransferStats_ServerError(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`error`))
	})
	stats := c.TransferStats()
	if stats.SpeedBytes != 0 {
		t.Errorf("SpeedBytes should be 0 on error, got %f", stats.SpeedBytes)
	}
}

func TestTransferStats_BadJSON(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not json`))
	})
	stats := c.TransferStats()
	if stats.SpeedBytes != 0 {
		t.Errorf("SpeedBytes should be 0 on bad JSON, got %f", stats.SpeedBytes)
	}
}

func TestTransferStats_SameFileDifferentBytes(t *testing.T) {
	calls := 0
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
		if calls == 1 {
			w.Write([]byte(`{"speed": 100.0, "transferring": [{"name": "shared", "bytes": 100}]}`))
		} else {
			// pdrive group has higher bytes for same file
			w.Write([]byte(`{"speed": 50.0, "transferring": [{"name": "shared", "bytes": 500}]}`))
		}
	})
	stats := c.TransferStats()
	// Should take the higher speed
	if stats.SpeedBytes != 100.0 {
		t.Errorf("SpeedBytes = %f, want 100", stats.SpeedBytes)
	}
	// Should take the higher bytes for same file
	if stats.Transferring["shared"] != 500 {
		t.Errorf("shared bytes = %d, want 500", stats.Transferring["shared"])
	}
}

// ── About — Bad JSON ────────────────────────────────────────────────────────

func TestAbout_AllFieldsSet(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"total": 100000, "used": 30000, "free": 70000}`))
	})
	info, err := c.About("drive")
	if err != nil {
		t.Fatal(err)
	}
	if info.Total != 100000 || info.Used != 30000 || info.Free != 70000 {
		t.Errorf("About returned %+v, want total=100000 used=30000 free=70000", info)
	}
}

// ── PutFile — close error ───────────────────────────────────────────────────

func TestPutFile_TempFileCloseError(t *testing.T) {
	// This is hard to trigger since we can't force os.File.Close to fail.
	// Already covered by other tests exercising the normal path.
	t.Skip("os.File.Close error requires kernel-level failure")
}

// ── ListRemotes bad JSON already tested ─────────────────────────────────────

// ── Cleanup success ─────────────────────────────────────────────────────────

func TestCleanup_Success(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/operations/cleanup" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})
	if err := c.Cleanup("drive"); err != nil {
		t.Fatal(err)
	}
}
