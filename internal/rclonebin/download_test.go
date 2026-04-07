package rclonebin

import (
	"archive/zip"
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureRclone_Cached(t *testing.T) {
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	os.MkdirAll(binDir, 0755)
	dest := filepath.Join(binDir, "rclone")
	os.WriteFile(dest, []byte("#!/bin/sh\necho fake"), 0755)

	got, err := EnsureRclone(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != dest {
		t.Errorf("got %q, want %q", got, dest)
	}
}

func TestEnsureRclone_Download(t *testing.T) {
	zipData := buildRcloneZip(t, "rclone-current-test-amd64/rclone", "#!/bin/sh\necho rclone")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(zipData)
	}))
	defer ts.Close()

	old := downloadBaseURL
	downloadBaseURL = ts.URL
	defer func() { downloadBaseURL = old }()

	dir := t.TempDir()
	got, err := EnsureRclone(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, "/bin/rclone") {
		t.Errorf("unexpected path: %s", got)
	}
	data, _ := os.ReadFile(got)
	if !strings.Contains(string(data), "echo rclone") {
		t.Error("extracted binary has wrong content")
	}
}

func TestEnsureRclone_HTTPError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	old := downloadBaseURL
	downloadBaseURL = ts.URL
	defer func() { downloadBaseURL = old }()

	_, err := EnsureRclone(t.TempDir())
	if err == nil {
		t.Fatal("expected error for HTTP 404")
	}
	if !strings.Contains(err.Error(), "HTTP 404") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEnsureRclone_TooSmall(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("tiny"))
	}))
	defer ts.Close()

	old := downloadBaseURL
	downloadBaseURL = ts.URL
	defer func() { downloadBaseURL = old }()

	_, err := EnsureRclone(t.TempDir())
	if err == nil {
		t.Fatal("expected error for suspiciously small download")
	}
	if !strings.Contains(err.Error(), "suspiciously small") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEnsureRclone_BadZip(t *testing.T) {
	garbage := make([]byte, 1024*1024+1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(garbage)
	}))
	defer ts.Close()

	old := downloadBaseURL
	downloadBaseURL = ts.URL
	defer func() { downloadBaseURL = old }()

	_, err := EnsureRclone(t.TempDir())
	if err == nil {
		t.Fatal("expected error for invalid zip")
	}
	if !strings.Contains(err.Error(), "zip") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEnsureRclone_MissingBinaryInZip(t *testing.T) {
	zipData := buildRcloneZip(t, "somedir/notRclone", strings.Repeat("x", 1024*1024))
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(zipData)
	}))
	defer ts.Close()

	old := downloadBaseURL
	downloadBaseURL = ts.URL
	defer func() { downloadBaseURL = old }()

	_, err := EnsureRclone(t.TempDir())
	if err == nil {
		t.Fatal("expected error when rclone not in zip")
	}
	if !strings.Contains(err.Error(), "not found in zip") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEnsureRclone_MkdirAllError(t *testing.T) {
	zipData := buildRcloneZip(t, "rclone-current/rclone", strings.Repeat("x", 1024*1024))
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(zipData)
	}))
	defer ts.Close()

	old := downloadBaseURL
	downloadBaseURL = ts.URL
	defer func() { downloadBaseURL = old }()

	badDir := filepath.Join(t.TempDir(), "config")
	os.WriteFile(badDir, []byte("x"), 0644)

	_, err := EnsureRclone(badDir)
	if err == nil {
		t.Fatal("expected error from MkdirAll")
	}
	if !strings.Contains(err.Error(), "bin directory") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEnsureRclone_HTTPGetError(t *testing.T) {
	old := downloadBaseURL
	downloadBaseURL = "://\x00invalid" // malformed URL → http.Get fails
	defer func() { downloadBaseURL = old }()

	_, err := EnsureRclone(t.TempDir())
	if err == nil {
		t.Fatal("expected error for malformed URL")
	}
	if !strings.Contains(err.Error(), "downloading rclone") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEnsureRclone_ReadBodyError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Declare a large Content-Length but send only a few bytes, then close.
		w.Header().Set("Content-Length", "10000000")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("partial"))
		// Handler returns → connection closes → client gets unexpected EOF
	}))
	defer ts.Close()

	old := downloadBaseURL
	downloadBaseURL = ts.URL
	defer func() { downloadBaseURL = old }()

	_, err := EnsureRclone(t.TempDir())
	if err == nil {
		t.Fatal("expected error for partial body")
	}
	if !strings.Contains(err.Error(), "reading rclone download") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEnsureRclone_DestIsDirectory(t *testing.T) {
	zipData := buildRcloneZip(t, "rclone-current/rclone", strings.Repeat("x", 1024*1024))
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(zipData)
	}))
	defer ts.Close()

	old := downloadBaseURL
	downloadBaseURL = ts.URL
	defer func() { downloadBaseURL = old }()

	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	os.MkdirAll(binDir, 0755)
	// Make dest a directory so os.OpenFile fails with "is a directory".
	dest := filepath.Join(binDir, "rclone")
	os.MkdirAll(dest, 0755)

	_, err := EnsureRclone(dir)
	if err == nil {
		t.Fatal("expected error when dest is a directory")
	}
	if !strings.Contains(err.Error(), "writing rclone binary") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEnsureRclone_FileOpenCorruptZip(t *testing.T) {
	// Build a valid zip, then corrupt the local file header so file.Open() fails.
	zipData := buildRcloneZip(t, "rclone-current/rclone", strings.Repeat("x", 1024*1024))
	// Corrupt the local file header magic bytes (first 4 bytes: PK\x03\x04).
	zipData[0] = 0xFF
	zipData[1] = 0xFF
	zipData[2] = 0xFF
	zipData[3] = 0xFF

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(zipData)
	}))
	defer ts.Close()

	old := downloadBaseURL
	downloadBaseURL = ts.URL
	defer func() { downloadBaseURL = old }()

	_, err := EnsureRclone(t.TempDir())
	if err == nil {
		t.Fatal("expected error from corrupted zip file entry")
	}
	if !strings.Contains(err.Error(), "extracting rclone") {
		t.Errorf("unexpected error: %v", err)
	}
}

func buildRcloneZip(t *testing.T, name, content string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	// Use Store method (no compression) so the zip size matches the content size.
	fw, err := zw.CreateHeader(&zip.FileHeader{
		Name:   name,
		Method: zip.Store,
	})
	if err != nil {
		t.Fatal(err)
	}
	fw.Write([]byte(content))
	// Pad to >1 MB to pass the size check (stored, not compressed).
	fw.Write(make([]byte, 1024*1024+100))
	zw.Close()
	return buf.Bytes()
}
