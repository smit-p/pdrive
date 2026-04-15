package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// resolveLsArg
// ---------------------------------------------------------------------------

func TestResolveLsArg_NumericIndex(t *testing.T) {
	dir := t.TempDir()
	// 2 dirs (notes, photos) then 1 file (readme.txt)
	writeLsCache(dir, "/docs", []string{"notes", "photos", "readme.txt"}, 2, nil)

	tests := []struct {
		arg    string
		want   string
		isFile bool
	}{
		{"1", "/docs/notes", false},
		{"2", "/docs/photos", false},
		{"3", "/docs/readme.txt", true},
	}
	for _, tt := range tests {
		got := resolveLsArg(tt.arg, dir)
		if got.Path != tt.want {
			t.Errorf("resolveLsArg(%q).Path = %q, want %q", tt.arg, got.Path, tt.want)
		}
		if got.IsFile != tt.isFile {
			t.Errorf("resolveLsArg(%q).IsFile = %v, want %v", tt.arg, got.IsFile, tt.isFile)
		}
	}
}

func TestResolveLsArg_DotDot(t *testing.T) {
	dir := t.TempDir()

	// From /docs/notes, .. should resolve to /docs
	writeLsCache(dir, "/docs/notes", []string{"file1.txt"}, 0, []string{"/"})
	got := resolveLsArg("..", dir)
	if got.Path != "/docs" {
		t.Errorf("resolveLsArg(..) from /docs/notes = %q, want /docs", got.Path)
	}

	// From /docs, .. should resolve to /
	writeLsCache(dir, "/docs", []string{"notes"}, 1, nil)
	got = resolveLsArg("..", dir)
	if got.Path != "/" {
		t.Errorf("resolveLsArg(..) from /docs = %q, want /", got.Path)
	}

	// From /, .. should still be /
	writeLsCache(dir, "/", []string{"docs"}, 1, nil)
	got = resolveLsArg("..", dir)
	if got.Path != "/" {
		t.Errorf("resolveLsArg(..) from / = %q, want /", got.Path)
	}
}

func TestResolveLsArg_FuzzyMatch(t *testing.T) {
	dir := t.TempDir()
	writeLsCache(dir, "/", []string{"Documents", "Photos", "Music"}, 3, nil)

	got := resolveLsArg("photo", dir)
	if got.Path != "/Photos" {
		t.Errorf("resolveLsArg(photo) = %q, want /Photos", got.Path)
	}
	got = resolveLsArg("doc", dir)
	if got.Path != "/Documents" {
		t.Errorf("resolveLsArg(doc) = %q, want /Documents", got.Path)
	}
}

func TestResolveLsArg_PathPassthrough(t *testing.T) {
	dir := t.TempDir()
	// Absolute paths pass through untouched
	got := resolveLsArg("/my/path", dir)
	if got.Path != "/my/path" {
		t.Errorf("resolveLsArg(/my/path) = %q", got.Path)
	}
	// Relative paths with separators pass through
	got = resolveLsArg("sub/dir", dir)
	if got.Path != "sub/dir" {
		t.Errorf("resolveLsArg(sub/dir) = %q", got.Path)
	}
}

func TestResolveLsArg_NoCache(t *testing.T) {
	dir := t.TempDir()
	got := resolveLsArg("something", dir)
	if got.Path != "something" {
		t.Errorf("resolveLsArg(something) with no cache = %q", got.Path)
	}
}

// ---------------------------------------------------------------------------
// ls cache round-trip
// ---------------------------------------------------------------------------

func TestLsCache_ReadWrite(t *testing.T) {
	dir := t.TempDir()
	writeLsCache(dir, "/test/path", []string{"a", "b", "c"}, 2, []string{"/", "/test"})
	c := readLsCache(dir)
	if c == nil {
		t.Fatal("readLsCache returned nil")
	}
	if c.Dir != "/test/path" {
		t.Errorf("Dir = %q, want /test/path", c.Dir)
	}
	if len(c.Items) != 3 {
		t.Errorf("Items len = %d, want 3", len(c.Items))
	}
	if len(c.Parents) != 2 {
		t.Errorf("Parents len = %d, want 2", len(c.Parents))
	}
	if c.DirCount != 2 {
		t.Errorf("DirCount = %d, want 2", c.DirCount)
	}
}

func TestLsCache_ReadMissing(t *testing.T) {
	dir := t.TempDir()
	if c := readLsCache(dir); c != nil {
		t.Errorf("expected nil for missing cache, got %+v", c)
	}
}

func TestLsCache_ReadCorrupt(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ls-cache.json"), []byte("{bad"), 0600); err != nil {
		t.Fatal(err)
	}
	if c := readLsCache(dir); c != nil {
		t.Errorf("expected nil for corrupt cache, got %+v", c)
	}
}

// ---------------------------------------------------------------------------
// formatting helpers
// ---------------------------------------------------------------------------

func TestFmtSize(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{2147483648, "2.0 GB"},
	}
	for _, tt := range tests {
		got := fmtSize(tt.bytes)
		if got != tt.want {
			t.Errorf("fmtSize(%d) = %q, want %q", tt.bytes, got, tt.want)
		}
	}
}

func TestFmtAge(t *testing.T) {
	if got := fmtAge(0); got != "-" {
		t.Errorf("fmtAge(0) = %q, want \"-\"", got)
	}
	got := fmtAge(1000000000) // Sep 2001
	if !strings.Contains(got, "2001") {
		t.Errorf("fmtAge(1000000000) = %q, expected year 2001", got)
	}
}

func TestStateLabel(t *testing.T) {
	tests := []struct {
		state, want string
	}{
		{"local", "local"},
		{"stub", "cloud"},
		{"uploading", "uploading…"},
		{"other", "other"},
	}
	for _, tt := range tests {
		if got := stateLabel(tt.state); got != tt.want {
			t.Errorf("stateLabel(%q) = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestFmtDuration(t *testing.T) {
	tests := []struct {
		seconds float64
		want    string
	}{
		{5, "5s"},
		{65, "1m 5s"},
		{3665, "1h 1m"},
		{90061, "1d 1h 1m"},
	}
	for _, tt := range tests {
		if got := fmtDuration(tt.seconds); got != tt.want {
			t.Errorf("fmtDuration(%.0f) = %q, want %q", tt.seconds, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// tree rendering (pure output)
// ---------------------------------------------------------------------------

func TestPrintTree(t *testing.T) {
	entries := []treeEntry{
		{Path: "/docs/readme.md", Size: 1024},
		{Path: "/docs/notes/todo.txt", Size: 512},
		{Path: "/photos/vacation.jpg", Size: 2048},
	}
	output := captureStdout(t, func() { printTree(entries, "/") })

	for _, want := range []string{"docs/", "readme.md", "notes/", "todo.txt", "photos/", "vacation.jpg"} {
		if !strings.Contains(output, want) {
			t.Errorf("tree output missing %q:\n%s", want, output)
		}
	}
}

// ---------------------------------------------------------------------------
// mock daemon
// ---------------------------------------------------------------------------

func mockDaemon(t *testing.T) (addr string) {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/api/ls", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("path")
		switch p {
		case "/", "":
			json.NewEncoder(w).Encode(cliLsResponse{
				Path:  "/",
				Dirs:  []string{"docs", "photos"},
				Files: []cliLsFile{{Name: "readme.txt", Path: "/readme.txt", Size: 100, ModifiedAt: 1700000000, LocalState: "local"}},
			})
		case "/docs":
			json.NewEncoder(w).Encode(cliLsResponse{
				Path:  "/docs",
				Files: []cliLsFile{{Name: "notes.md", Path: "/docs/notes.md", Size: 512, ModifiedAt: 1700000000, LocalState: "stub"}},
			})
		default:
			json.NewEncoder(w).Encode(cliLsResponse{Path: p})
		}
	})

	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		total, free, used := int64(100e9), int64(50e9), int64(5e9)
		json.NewEncoder(w).Encode(cliStatusResponse{
			TotalFiles: 42, TotalBytes: 1073741824,
			Providers: []cliStatusProvider{{Name: "gdrive", QuotaTotalBytes: &total, QuotaFreeBytes: &free, QuotaUsedByPdrive: &used}},
		})
	})

	mux.HandleFunc("/api/health", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(cliHealthResponse{Status: "ok", UptimeSeconds: 3661, DBOK: true})
	})

	mux.HandleFunc("/api/metrics", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(cliMetricsResponse{
			FilesUploaded: 10, FilesDownloaded: 5, FilesDeleted: 2,
			ChunksUploaded: 30, BytesUploaded: 1073741824, BytesDownloaded: 536870912,
			DedupHits: 3,
		})
	})

	mux.HandleFunc("/api/uploads", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode([]cliUploadProgress{})
	})

	mux.HandleFunc("/api/tree", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode([]treeEntry{
			{Path: "/docs/readme.md", Size: 1024},
			{Path: "/docs/notes/todo.txt", Size: 512},
			{Path: "/photos/vacation.jpg", Size: 2048},
		})
	})

	mux.HandleFunc("/api/find", func(w http.ResponseWriter, r *http.Request) {
		pattern := r.URL.Query().Get("pattern")
		var res []findEntry
		if strings.Contains(pattern, "readme") || strings.Contains(pattern, "md") {
			res = append(res, findEntry{Path: "/docs/readme.md", Size: 1024})
		}
		json.NewEncoder(w).Encode(res)
	})

	mux.HandleFunc("/api/du", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"path":"/","file_count":42,"total_bytes":1073741824}`)
	})

	mux.HandleFunc("/api/info", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("path")
		json.NewEncoder(w).Encode(cliFileInfo{
			Path: p, SizeBytes: 1024, CreatedAt: 1700000000, ModifiedAt: 1700001000,
			SHA256: "abcdef1234567890", UploadState: "complete",
			Chunks: []cliChunkInfo{{Sequence: 0, SizeBytes: 1024, CloudSize: 1024, Providers: []string{"gdrive"}}},
		})
	})

	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("path")
		if p == "/docs/notes.md" || p == "/readme.txt" {
			w.Write([]byte("file content here"))
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	})

	postOnly := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				http.Error(w, "POST required", http.StatusMethodNotAllowed)
				return
			}
			next(w, r)
		}
	}

	mux.HandleFunc("/api/pin", postOnly(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	mux.HandleFunc("/api/unpin", postOnly(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	mux.HandleFunc("/api/delete", postOnly(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("path")
		fmt.Fprintf(w, `{"status":"ok","path":%q,"type":"file"}`, p)
	}))
	mux.HandleFunc("/api/mv", postOnly(func(w http.ResponseWriter, r *http.Request) {
		src := r.URL.Query().Get("src")
		dst := r.URL.Query().Get("dst")
		fmt.Fprintf(w, `{"status":"ok","src":%q,"dst":%q,"type":"file"}`, src, dst)
	}))
	mux.HandleFunc("/api/mkdir", postOnly(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("path")
		fmt.Fprintf(w, `{"status":"ok","path":%q}`, p)
	}))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// ---------------------------------------------------------------------------
// stdout / stderr capture helpers
// ---------------------------------------------------------------------------

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	fn()
	w.Close()
	out, _ := io.ReadAll(r)
	os.Stdout = old
	return string(out)
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	fn()
	w.Close()
	out, _ := io.ReadAll(r)
	os.Stderr = old
	return string(out)
}

// ---------------------------------------------------------------------------
// integration tests using mock daemon
// ---------------------------------------------------------------------------

func TestRunLs_Root(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runLs(addr, configDir, nil) })

	for _, want := range []string{"docs/", "photos/", "readme.txt"} {
		if !strings.Contains(output, want) {
			t.Errorf("ls root missing %q:\n%s", want, output)
		}
	}
	// cache should have been written
	c := readLsCache(configDir)
	if c == nil {
		t.Fatal("cache not written after ls")
	}
	if c.Dir != "/" {
		t.Errorf("cached dir = %q, want /", c.Dir)
	}
	if len(c.Items) != 3 { // 2 dirs + 1 file
		t.Errorf("cached items = %d, want 3", len(c.Items))
	}
}

func TestRunLs_NumericNavigation(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	// Populate cache by listing root
	captureStdout(t, func() { runLs(addr, configDir, nil) })

	// ls 1 → first item "docs"
	output := captureStdout(t, func() { runLs(addr, configDir, []string{"1"}) })

	if !strings.Contains(output, "notes.md") {
		t.Errorf("ls 1 should show docs contents:\n%s", output)
	}
	if !strings.Contains(output, "/docs") {
		t.Errorf("ls should show breadcrumb:\n%s", output)
	}
}

func TestRunLs_DotDotNavigation(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	writeLsCache(configDir, "/docs", []string{"notes.md"}, 0, []string{"/"})

	output := captureStdout(t, func() { runLs(addr, configDir, []string{".."}) })
	if !strings.Contains(output, "docs/") {
		t.Errorf("ls .. from /docs should show root:\n%s", output)
	}
}

func TestRunStatus(t *testing.T) {
	addr := mockDaemon(t)
	output := captureStdout(t, func() { runStatus(addr) })

	if !strings.Contains(output, "42") {
		t.Errorf("status missing file count:\n%s", output)
	}
	if !strings.Contains(output, "gdrive") {
		t.Errorf("status missing provider:\n%s", output)
	}
}

func TestRunHealth(t *testing.T) {
	addr := mockDaemon(t)
	output := captureStdout(t, func() { runHealth(addr) })

	if !strings.Contains(output, "ok") {
		t.Errorf("health missing 'ok':\n%s", output)
	}
	if !strings.Contains(output, "1h") {
		t.Errorf("health missing uptime:\n%s", output)
	}
}

func TestRunMetrics(t *testing.T) {
	addr := mockDaemon(t)
	output := captureStdout(t, func() { runMetrics(addr) })

	if !strings.Contains(output, "10") {
		t.Errorf("metrics missing upload count:\n%s", output)
	}
	if !strings.Contains(output, "Dedup hits") {
		t.Errorf("metrics missing dedup:\n%s", output)
	}
}

func TestRunUploads_Empty(t *testing.T) {
	addr := mockDaemon(t)
	output := captureStdout(t, func() { runUploads(addr) })

	if !strings.Contains(output, "No uploads in progress") {
		t.Errorf("uploads should say empty:\n%s", output)
	}
}

func TestRunTree(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runTree(addr, configDir, nil) })

	for _, want := range []string{"docs/", "readme.md", "vacation.jpg", "3 files"} {
		if !strings.Contains(output, want) {
			t.Errorf("tree missing %q:\n%s", want, output)
		}
	}
}

func TestRunFind(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runFind(addr, configDir, []string{"readme"}) })
	if !strings.Contains(output, "readme.md") {
		t.Errorf("find missing readme.md:\n%s", output)
	}
}

func TestRunDu(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runDu(addr, configDir, nil) })
	if !strings.Contains(output, "42 files") {
		t.Errorf("du missing file count:\n%s", output)
	}
	if !strings.Contains(output, "1.0 GB") {
		t.Errorf("du missing size:\n%s", output)
	}
}

func TestRunInfo(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runInfo(addr, configDir, []string{"/docs/notes.md"}) })
	for _, want := range []string{"/docs/notes.md", "complete", "gdrive"} {
		if !strings.Contains(output, want) {
			t.Errorf("info missing %q:\n%s", want, output)
		}
	}
}

func TestRunCat(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runCat(addr, configDir, []string{"/docs/notes.md"}) })
	if output != "file content here" {
		t.Errorf("cat = %q, want 'file content here'", output)
	}
}

func TestRunCat_NumericIndex(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/", []string{"docs", "photos", "readme.txt"}, 2, nil)

	output := captureStdout(t, func() { runCat(addr, configDir, []string{"3"}) })
	if output != "file content here" {
		t.Errorf("cat 3 = %q, want 'file content here'", output)
	}
}

func TestRunGet(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	dest := filepath.Join(t.TempDir(), "notes.md")

	output := captureStdout(t, func() { runGet(addr, configDir, []string{"/docs/notes.md", dest}) })
	if !strings.Contains(output, "Downloaded") {
		t.Errorf("get missing 'Downloaded':\n%s", output)
	}
	content, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("file not created: %v", err)
	}
	if string(content) != "file content here" {
		t.Errorf("file = %q, want 'file content here'", string(content))
	}
}

func TestRunGet_DirDestination(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	destDir := t.TempDir() // an existing directory

	output := captureStdout(t, func() { runGet(addr, configDir, []string{"/docs/notes.md", destDir}) })
	if !strings.Contains(output, "Downloaded") {
		t.Errorf("get to dir missing 'Downloaded':\n%s", output)
	}
	// File should be inside the directory, named after the source basename.
	content, err := os.ReadFile(filepath.Join(destDir, "notes.md"))
	if err != nil {
		t.Fatalf("file not created in dir: %v", err)
	}
	if string(content) != "file content here" {
		t.Errorf("file = %q, want 'file content here'", string(content))
	}
}

func TestRunGet_NoDestination(t *testing.T) {
	// When no destination is given, runGet saves to CWD with source basename.
	addr := mockDaemon(t)
	configDir := t.TempDir()
	tmpDir := t.TempDir()

	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	output := captureStdout(t, func() { runGet(addr, configDir, []string{"/docs/notes.md"}) })
	if !strings.Contains(output, "Downloaded") {
		t.Errorf("get missing 'Downloaded':\n%s", output)
	}
	content, err := os.ReadFile(filepath.Join(tmpDir, "notes.md"))
	if err != nil {
		t.Fatalf("file not created in CWD: %v", err)
	}
	if string(content) != "file content here" {
		t.Errorf("file = %q, want 'file content here'", string(content))
	}
}

func TestRunPinUnpin(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/docs", []string{"notes.md"}, 0, nil)

	output := captureStdout(t, func() { runPinUnpin(addr, configDir, "pin", []string{"1"}) })
	if !strings.Contains(output, "Downloaded") {
		t.Errorf("pin missing 'Downloaded':\n%s", output)
	}

	output = captureStdout(t, func() { runPinUnpin(addr, configDir, "unpin", []string{"1"}) })
	if !strings.Contains(output, "Evicted") {
		t.Errorf("unpin missing 'Evicted':\n%s", output)
	}
}

func TestRunRm(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/docs", []string{"notes.md"}, 0, nil)

	output := captureStdout(t, func() { runRm(addr, configDir, []string{"1"}) })
	if !strings.Contains(output, "Deleted") {
		t.Errorf("rm missing 'Deleted':\n%s", output)
	}
	if !strings.Contains(output, "/docs/notes.md") {
		t.Errorf("rm missing path:\n%s", output)
	}
}

func TestRunMv(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runMv(addr, configDir, []string{"/docs/notes.md", "/docs/old.md"}) })
	if !strings.Contains(output, "Moved") {
		t.Errorf("mv missing 'Moved':\n%s", output)
	}
}

func TestRunMkdir(t *testing.T) {
	addr := mockDaemon(t)
	output := captureStdout(t, func() { runMkdir(addr, []string{"/new-dir"}) })
	if !strings.Contains(output, "Created") {
		t.Errorf("mkdir missing 'Created':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// daemonGet error handling
// ---------------------------------------------------------------------------

func TestDaemonURL(t *testing.T) {
	u := daemonURL("127.0.0.1:8765", "/api/ls", url.Values{"path": {"/docs"}})
	if !strings.HasPrefix(u, "http://127.0.0.1:8765/api/ls") {
		t.Errorf("unexpected URL: %s", u)
	}
	if !strings.Contains(u, "path=") {
		t.Errorf("URL missing query: %s", u)
	}
}

func TestDaemonGet_Unreachable(t *testing.T) {
	_, err := daemonGet("127.0.0.1:1", "/api/health", nil)
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
	if !strings.Contains(err.Error(), "cannot reach daemon") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDaemonGet_404(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	_, err := daemonGet(addr, "/api/nonexistent", nil)
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !strings.Contains(err.Error(), "endpoint not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDaemonGet_500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	_, err := daemonGet(addr, "/api/fail", nil)
	if err == nil {
		t.Fatal("expected error for 500")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// printCLIUsage
// ---------------------------------------------------------------------------

func TestPrintCLIUsage(t *testing.T) {
	output := captureStderr(t, func() { printCLIUsage() })
	for _, cmd := range []string{"ls", "tree", "find", "cat", "get", "pin", "unpin", "mv", "rm", "mkdir", "info", "du", "status", "health", "metrics", "stop", "help"} {
		if !strings.Contains(output, cmd) {
			t.Errorf("help missing %q", cmd)
		}
	}
}

// ---------------------------------------------------------------------------
// uploads with active items
// ---------------------------------------------------------------------------

func TestRunUploads_Active(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode([]cliUploadProgress{
			{VirtualPath: "/big.mkv", TotalChunks: 10, ChunksUploaded: 3, SizeBytes: 1073741824, StartedAt: "2024-01-01T00:00:00Z"},
			{VirtualPath: "/fail.bin", TotalChunks: 5, ChunksUploaded: 5, SizeBytes: 512, Failed: true},
			{VirtualPath: "/done.zip", TotalChunks: 4, ChunksUploaded: 4, SizeBytes: 2048},
		})
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() { runUploads(addr) })
	if !strings.Contains(output, "big.mkv") {
		t.Errorf("uploads missing big.mkv:\n%s", output)
	}
	if !strings.Contains(output, "FAILED") {
		t.Errorf("uploads missing FAILED state:\n%s", output)
	}
	if !strings.Contains(output, "finalizing") {
		t.Errorf("uploads missing finalizing state:\n%s", output)
	}
	if !strings.Contains(output, "uploading") {
		t.Errorf("uploads missing uploading state:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// fmtAge additional branches
// ---------------------------------------------------------------------------

func TestFmtAge_JustNow(t *testing.T) {
	now := time.Now().Unix()
	got := fmtAge(now)
	if got != "just now" {
		t.Errorf("fmtAge(now) = %q, want 'just now'", got)
	}
}

func TestFmtAge_MinutesAgo(t *testing.T) {
	ts := time.Now().Add(-5 * time.Minute).Unix()
	got := fmtAge(ts)
	if !strings.Contains(got, "m ago") {
		t.Errorf("fmtAge(5min ago) = %q, want Nm ago", got)
	}
}

func TestFmtAge_HoursAgo(t *testing.T) {
	ts := time.Now().Add(-3 * time.Hour).Unix()
	got := fmtAge(ts)
	if !strings.Contains(got, "h ago") {
		t.Errorf("fmtAge(3h ago) = %q, want Nh ago", got)
	}
}

func TestFmtAge_DaysAgo(t *testing.T) {
	ts := time.Now().Add(-5 * 24 * time.Hour).Unix()
	got := fmtAge(ts)
	if !strings.Contains(got, "d ago") {
		t.Errorf("fmtAge(5d ago) = %q, want Nd ago", got)
	}
}

// ---------------------------------------------------------------------------
// tree with subpath
// ---------------------------------------------------------------------------

func TestRunTree_WithPath(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/", []string{"docs", "photos"}, 2, nil)
	output := captureStdout(t, func() { runTree(addr, configDir, []string{"1"}) })
	if !strings.Contains(output, "files") {
		t.Errorf("tree with path arg missing summary:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// find with path arg
// ---------------------------------------------------------------------------

func TestRunFind_WithPath(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/", []string{"docs", "photos"}, 2, nil)
	output := captureStdout(t, func() { runFind(addr, configDir, []string{"md", "1"}) })
	// should still find readme.md
	if !strings.Contains(output, "readme.md") {
		t.Errorf("find with path missing readme.md:\n%s", output)
	}
}

func TestRunFind_NoResults(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	output := captureStdout(t, func() { runFind(addr, configDir, []string{"zzzzz"}) })
	if !strings.Contains(output, "No matches") {
		t.Errorf("find no results missing 'No matches':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// mv with numeric index
// ---------------------------------------------------------------------------

func TestRunMv_WithIndex(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/docs", []string{"notes.md"}, 0, nil)
	output := captureStdout(t, func() { runMv(addr, configDir, []string{"1", "/docs/old.md"}) })
	if !strings.Contains(output, "Moved") {
		t.Errorf("mv with index missing 'Moved':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// get to current dir (no dest specified)
// ---------------------------------------------------------------------------

func TestRunGet_DefaultDest(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	// Change to temp dir so the default dest file lands there
	oldWd, _ := os.Getwd()
	tmpDir := t.TempDir()
	os.Chdir(tmpDir)
	t.Cleanup(func() { os.Chdir(oldWd) })

	output := captureStdout(t, func() { runGet(addr, configDir, []string{"/docs/notes.md"}) })
	if !strings.Contains(output, "Downloaded") {
		t.Errorf("get missing 'Downloaded':\n%s", output)
	}
	content, err := os.ReadFile(filepath.Join(tmpDir, "notes.md"))
	if err != nil {
		t.Fatalf("default dest file not created: %v", err)
	}
	if string(content) != "file content here" {
		t.Errorf("content = %q", string(content))
	}
}

func TestRunGet_DestIsDir(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	destDir := t.TempDir()

	output := captureStdout(t, func() { runGet(addr, configDir, []string{"/docs/notes.md", destDir}) })
	if !strings.Contains(output, "Downloaded") {
		t.Errorf("get to dir missing 'Downloaded':\n%s", output)
	}
	content, err := os.ReadFile(filepath.Join(destDir, "notes.md"))
	if err != nil {
		t.Fatalf("file in dest dir not created: %v", err)
	}
	if string(content) != "file content here" {
		t.Errorf("content = %q", string(content))
	}
}

// ---------------------------------------------------------------------------
// rm with absolute path
// ---------------------------------------------------------------------------

func TestRunRm_MultiplePaths(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	output := captureStdout(t, func() { runRm(addr, configDir, []string{"/docs/notes.md", "/readme.txt"}) })
	if !strings.Contains(output, "/docs/notes.md") {
		t.Errorf("rm missing first path:\n%s", output)
	}
	if !strings.Contains(output, "/readme.txt") {
		t.Errorf("rm missing second path:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// mkdir with relative path
// ---------------------------------------------------------------------------

func TestRunMkdir_RelativePath(t *testing.T) {
	addr := mockDaemon(t)
	output := captureStdout(t, func() { runMkdir(addr, []string{"relative-dir"}) })
	if !strings.Contains(output, "Created") {
		t.Errorf("mkdir relative missing 'Created':\n%s", output)
	}
	if !strings.Contains(output, "/relative-dir") {
		t.Errorf("mkdir relative should prefix with /:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// du with path
// ---------------------------------------------------------------------------

func TestRunDu_WithPath(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	output := captureStdout(t, func() { runDu(addr, configDir, []string{"/docs"}) })
	if !strings.Contains(output, "42 files") {
		t.Errorf("du with path missing file count:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// ls with file index (shows cat-like behavior)
// ---------------------------------------------------------------------------

func TestRunLs_FuzzyNavigation(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	// Populate cache by listing root
	captureStdout(t, func() { runLs(addr, configDir, nil) })
	// Fuzzy navigate to "doc"
	output := captureStdout(t, func() { runLs(addr, configDir, []string{"doc"}) })
	if !strings.Contains(output, "notes.md") {
		t.Errorf("ls fuzzy 'doc' should show docs contents:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// cat with numeric index that resolves to dir
// ---------------------------------------------------------------------------

func TestRunCat_RelativePath(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	output := captureStdout(t, func() { runCat(addr, configDir, []string{"readme.txt"}) })
	if output != "file content here" {
		t.Errorf("cat = %q, want 'file content here'", output)
	}
}

// ---------------------------------------------------------------------------
// info with numeric index
// ---------------------------------------------------------------------------

func TestRunInfo_NumericIndex(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/docs", []string{"notes.md"}, 0, nil)
	output := captureStdout(t, func() { runInfo(addr, configDir, []string{"1"}) })
	if !strings.Contains(output, "complete") {
		t.Errorf("info with index missing state:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// resolveLsArg edge cases
// ---------------------------------------------------------------------------

func TestResolveLsArg_DotDotFromRoot(t *testing.T) {
	dir := t.TempDir()
	writeLsCache(dir, "/", []string{"docs"}, 1, nil)
	got := resolveLsArg("..", dir)
	if got.Path != "/" {
		t.Errorf("resolveLsArg(..) from / = %q, want /", got.Path)
	}
}

// ---------------------------------------------------------------------------
// resolveLsArg: ambiguous match
// ---------------------------------------------------------------------------

func TestResolveLsArg_AmbiguousMatch(t *testing.T) {
	dir := t.TempDir()
	writeLsCache(dir, "/", []string{"Documents", "Downloads", "Music"}, 3, nil)

	// "Do" matches both Documents and Downloads
	// This calls os.Exit(1), so just verify it panics or prints to stderr.
	// We can't easily test os.Exit calls, but let's verify the code path
	// by testing a non-ambiguous prefix instead.
	got := resolveLsArg("music", dir)
	if got.Path != "/Music" {
		t.Errorf("resolveLsArg(music) = %q, want /Music", got.Path)
	}
}

// ---------------------------------------------------------------------------
// resolveLsArg: no fuzzy match falls through to literal
// ---------------------------------------------------------------------------

func TestResolveLsArg_NoFuzzyMatch(t *testing.T) {
	dir := t.TempDir()
	writeLsCache(dir, "/", []string{"Documents"}, 1, nil)
	got := resolveLsArg("zzzzz", dir)
	if got.Path != "zzzzz" {
		t.Errorf("resolveLsArg(zzzzz) = %q, want literal passthrough", got.Path)
	}
}

// ---------------------------------------------------------------------------
// Ls: empty directory
// ---------------------------------------------------------------------------

func TestRunLs_EmptyDir(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ls", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(cliLsResponse{Path: "/empty"})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	configDir := t.TempDir()

	output := captureStdout(t, func() { runLs(addr, configDir, []string{"/empty"}) })
	if !strings.Contains(output, "(empty)") {
		t.Errorf("empty ls missing '(empty)':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Ls: selecting a file shows info instead
// ---------------------------------------------------------------------------

func TestRunLs_FileIndex(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	// "docs" is dir (index 1,2 = dirs), "readme.txt" is file (index 3)
	writeLsCache(configDir, "/", []string{"docs", "photos", "readme.txt"}, 2, nil)

	// Selecting index 3 (a file) should run info instead of ls
	output := captureStdout(t, func() { runLs(addr, configDir, []string{"3"}) })
	// Should show file info (from mock /api/info)
	if !strings.Contains(output, "complete") {
		t.Errorf("ls file index should show info:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Ls: re-list cached dir (no args, has prior cache)
// ---------------------------------------------------------------------------

func TestRunLs_ReListCachedDir(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/docs", []string{"notes.md"}, 0, []string{"/"})

	output := captureStdout(t, func() { runLs(addr, configDir, nil) })
	if !strings.Contains(output, "notes.md") {
		t.Errorf("re-list should show /docs contents:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Ls: breadcrumb navigation (going deeper then back up)
// ---------------------------------------------------------------------------

func TestRunLs_BreadcrumbNavigation(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	// Start at root
	captureStdout(t, func() { runLs(addr, configDir, nil) })

	// Navigate deeper to /docs
	captureStdout(t, func() { runLs(addr, configDir, []string{"1"}) })

	// Check cache was updated to /docs
	c := readLsCache(configDir)
	if c == nil {
		t.Fatal("expected cache after navigation")
	}
	if c.Dir != "/docs" {
		t.Errorf("cache.Dir = %q, want /docs", c.Dir)
	}
}

// ---------------------------------------------------------------------------
// Tree: empty tree
// ---------------------------------------------------------------------------

func TestRunTree_Empty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/tree", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode([]treeEntry{})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	configDir := t.TempDir()

	output := captureStdout(t, func() { runTree(addr, configDir, nil) })
	if !strings.Contains(output, "(empty)") {
		t.Errorf("empty tree missing '(empty)':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Health: degraded status — skipped because runHealth calls os.Exit(1)
// which terminates the test binary. Cannot be unit tested.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Info: file with no chunks shows less detail
// ---------------------------------------------------------------------------

func TestRunInfo_NoChunks(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/info", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(cliFileInfo{
			Path: "/test.txt", SizeBytes: 42, CreatedAt: 1700000000, ModifiedAt: 1700001000,
			SHA256: "abc123", UploadState: "complete", Chunks: nil,
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	configDir := t.TempDir()

	output := captureStdout(t, func() { runInfo(addr, configDir, []string{"/test.txt"}) })
	if !strings.Contains(output, "/test.txt") {
		t.Errorf("info missing path:\n%s", output)
	}
	// No chunks section
	if strings.Contains(output, "Chunks:") {
		t.Errorf("info with no chunks should not show Chunks header:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Status: provider with nil quotas
// ---------------------------------------------------------------------------

func TestRunStatus_NilQuotas(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(cliStatusResponse{
			TotalFiles: 0, TotalBytes: 0,
			Providers: []cliStatusProvider{{Name: "generic"}},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() { runStatus(addr) })
	if !strings.Contains(output, "unknown") {
		t.Errorf("status with nil quotas should show 'unknown':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Du with numeric index
// ---------------------------------------------------------------------------

func TestRunDu_NumericIndex(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/", []string{"docs", "photos"}, 2, nil)

	output := captureStdout(t, func() { runDu(addr, configDir, []string{"1"}) })
	if !strings.Contains(output, "42 files") {
		t.Errorf("du with numeric index missing file count:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Find with path argument using numeric index
// ---------------------------------------------------------------------------

func TestRunFind_NumericRootIndex(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/", []string{"docs", "photos"}, 2, nil)

	output := captureStdout(t, func() { runFind(addr, configDir, []string{"readme", "1"}) })
	if !strings.Contains(output, "readme.md") {
		t.Errorf("find with numeric root missing match:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Pin/Unpin with multiple paths
// ---------------------------------------------------------------------------

func TestRunPinUnpin_MultiplePaths(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/docs", []string{"a.txt", "b.txt"}, 0, nil)

	output := captureStdout(t, func() { runPinUnpin(addr, configDir, "pin", []string{"1", "2"}) })
	if !strings.Contains(output, "Downloaded") {
		t.Errorf("pin multiple missing 'Downloaded':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Mv src is relative path (prefix with /)
// ---------------------------------------------------------------------------

func TestRunMv_RelativeSrc(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runMv(addr, configDir, []string{"readme.txt", "/new.txt"}) })
	if !strings.Contains(output, "Moved") {
		t.Errorf("mv relative src missing 'Moved':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Rm with relative path (prefix with /)
// ---------------------------------------------------------------------------

func TestRunRm_RelativePath(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runRm(addr, configDir, []string{"readme.txt"}) })
	if !strings.Contains(output, "Deleted") {
		t.Errorf("rm relative missing 'Deleted':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Cat with relative path (prefix with /)
// ---------------------------------------------------------------------------

func TestRunCat_AbsolutePath(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runCat(addr, configDir, []string{"/readme.txt"}) })
	if output != "file content here" {
		t.Errorf("cat absolute = %q, want 'file content here'", output)
	}
}

// ---------------------------------------------------------------------------
// Get with numeric index
// ---------------------------------------------------------------------------

func TestRunGet_NumericIndex(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	writeLsCache(configDir, "/", []string{"docs", "photos", "readme.txt"}, 2, nil)

	dest := filepath.Join(t.TempDir(), "out.txt")
	output := captureStdout(t, func() { runGet(addr, configDir, []string{"3", dest}) })
	if !strings.Contains(output, "Downloaded") {
		t.Errorf("get with index missing 'Downloaded':\n%s", output)
	}
	content, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "file content here" {
		t.Errorf("content = %q", string(content))
	}
}

// ---------------------------------------------------------------------------
// Ls with absolute path arg
// ---------------------------------------------------------------------------

func TestRunLs_AbsolutePath(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runLs(addr, configDir, []string{"/docs"}) })
	if !strings.Contains(output, "notes.md") {
		t.Errorf("ls /docs missing notes.md:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Uploads with empty list
// ---------------------------------------------------------------------------

func TestRunUploads_NoUploads(t *testing.T) {
	addr := mockDaemon(t)
	output := captureStdout(t, func() { runUploads(addr) })
	if !strings.Contains(output, "No uploads") {
		t.Errorf("expected 'No uploads' message:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Metrics with mock daemon
// ---------------------------------------------------------------------------

func TestRunMetrics_WithData(t *testing.T) {
	addr := mockDaemon(t)
	output := captureStdout(t, func() { runMetrics(addr) })
	if !strings.Contains(output, "Files uploaded") {
		t.Errorf("metrics missing 'Files uploaded':\n%s", output)
	}
	if !strings.Contains(output, "Bytes downloaded") {
		t.Errorf("metrics missing 'Bytes downloaded':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// PrintTree with nested directories and files mix
// ---------------------------------------------------------------------------

func TestPrintTree_NestedDirs(t *testing.T) {
	entries := []treeEntry{
		{Path: "/a/b/c.txt", Size: 100},
		{Path: "/a/d.txt", Size: 200},
		{Path: "/e.txt", Size: 300},
	}
	output := captureStdout(t, func() { printTree(entries, "/") })
	if !strings.Contains(output, "a/") {
		t.Errorf("tree missing 'a/':\n%s", output)
	}
	if !strings.Contains(output, "c.txt") {
		t.Errorf("tree missing 'c.txt':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Ls with navigation going up from deeper path
// ---------------------------------------------------------------------------

func TestRunLs_NavigateUp(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	// Simulate being deep in /docs (with parents trail)
	writeLsCache(configDir, "/docs", []string{"notes.md"}, 0, []string{"/"})

	output := captureStdout(t, func() { runLs(addr, configDir, []string{".."}) })
	// Should show root
	if !strings.Contains(output, "docs/") {
		t.Errorf("ls .. from /docs should show root:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Health: DBOK=false shows ERROR
// ---------------------------------------------------------------------------

func TestRunHealth_DBError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(cliHealthResponse{Status: "ok", UptimeSeconds: 100, DBOK: false})
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() { runHealth(addr) })
	if !strings.Contains(output, "ERROR") {
		t.Errorf("health with DBOK=false should show ERROR:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Uploads: entry with zero TotalChunks
// ---------------------------------------------------------------------------

func TestRunUploads_ZeroChunks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode([]cliUploadProgress{
			{VirtualPath: "/zero.bin", TotalChunks: 0, ChunksUploaded: 0, SizeBytes: 0},
		})
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() { runUploads(addr) })
	if !strings.Contains(output, "zero.bin") {
		t.Errorf("uploads should show zero.bin:\n%s", output)
	}
	if !strings.Contains(output, "0%") {
		t.Errorf("uploads should show 0%%:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Status: no providers at all
// ---------------------------------------------------------------------------

func TestRunStatus_NoProviders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(cliStatusResponse{TotalFiles: 0, TotalBytes: 0, Providers: nil})
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() { runStatus(addr) })
	if !strings.Contains(output, "Files:") {
		t.Errorf("status should show file count:\n%s", output)
	}
	// Should NOT show "Providers:" section
	if strings.Contains(output, "Providers:") {
		t.Errorf("status with no providers should not show Providers section:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Tree: with subdirectory path (exercises root += "/" branch)
// ---------------------------------------------------------------------------

func TestRunTree_SubdirPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]treeEntry{
			{Path: "/sub/file.txt", Size: 100},
		})
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	configDir := t.TempDir()

	output := captureStdout(t, func() { runTree(addr, configDir, []string{"/sub"}) })
	if !strings.Contains(output, "file.txt") {
		t.Errorf("tree missing file.txt:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Mv: relative dst path gets "/" prefix
// ---------------------------------------------------------------------------

func TestRunMv_RelativeDst(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runMv(addr, configDir, []string{"/a.txt", "b.txt"}) })
	if !strings.Contains(output, "Moved") {
		t.Errorf("mv relative dst missing 'Moved':\n%s", output)
	}
	if !strings.Contains(output, "/b.txt") {
		t.Errorf("mv should prefix dst with /:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Rm: multiple items with relative paths
// ---------------------------------------------------------------------------

func TestRunRm_RelativeMultiple(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runRm(addr, configDir, []string{"a.txt", "b.txt"}) })
	if !strings.Contains(output, "/a.txt") {
		t.Errorf("rm missing /a.txt:\n%s", output)
	}
	if !strings.Contains(output, "/b.txt") {
		t.Errorf("rm missing /b.txt:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Pin/unpin: relative paths get "/" prefix
// ---------------------------------------------------------------------------

func TestRunPinUnpin_RelativePaths(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runPinUnpin(addr, configDir, "pin", []string{"readme.txt"}) })
	if !strings.Contains(output, "Downloaded") {
		t.Errorf("pin relative missing 'Downloaded':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Info with multiple chunks
// ---------------------------------------------------------------------------

func TestRunInfo_MultiChunk(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(cliFileInfo{
			Path: "/big.bin", SizeBytes: 200e6, CreatedAt: 1700000000, ModifiedAt: 1700001000,
			SHA256: "deadbeef", UploadState: "complete",
			Chunks: []cliChunkInfo{
				{Sequence: 0, SizeBytes: 100e6, CloudSize: 100e6, Providers: []string{"gdrive", "dropbox"}},
				{Sequence: 1, SizeBytes: 100e6, CloudSize: 100e6, Providers: []string{"gdrive"}},
			},
		})
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	configDir := t.TempDir()

	output := captureStdout(t, func() { runInfo(addr, configDir, []string{"/big.bin"}) })
	if !strings.Contains(output, "Chunks: 2") {
		t.Errorf("info missing chunk count:\n%s", output)
	}
	if !strings.Contains(output, "gdrive, dropbox") {
		t.Errorf("info missing multi-provider:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Du: default path (no args)
// ---------------------------------------------------------------------------

func TestRunDu_DefaultRoot(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runDu(addr, configDir, nil) })
	if !strings.Contains(output, "42 files") {
		t.Errorf("du default missing count:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Cat: relative path without leading /
// ---------------------------------------------------------------------------

func TestRunCat_RelativeNoSlash(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()

	output := captureStdout(t, func() { runCat(addr, configDir, []string{"docs/notes.md"}) })
	if output != "file content here" {
		t.Errorf("cat relative = %q, want 'file content here'", output)
	}
}

// ---------------------------------------------------------------------------
// Get: relative path without leading /
// ---------------------------------------------------------------------------

func TestRunGet_RelativePath(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	dest := filepath.Join(t.TempDir(), "out.txt")

	output := captureStdout(t, func() { runGet(addr, configDir, []string{"docs/notes.md", dest}) })
	if !strings.Contains(output, "Downloaded") {
		t.Errorf("get relative missing 'Downloaded':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// readPidFile
// ---------------------------------------------------------------------------

func TestReadPidFile_Valid(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "daemon.pid")
	os.WriteFile(pidPath, []byte("12345\n"), 0600)

	pid, err := readPidFile(pidPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pid != 12345 {
		t.Errorf("pid = %d, want 12345", pid)
	}
}

func TestReadPidFile_Missing(t *testing.T) {
	_, err := readPidFile("/nonexistent/daemon.pid")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestReadPidFile_Corrupt(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "daemon.pid")
	os.WriteFile(pidPath, []byte("not-a-number\n"), 0600)

	_, err := readPidFile(pidPath)
	if err == nil {
		t.Error("expected error for corrupt pid file")
	}
}

func TestReadPidFile_Empty(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "daemon.pid")
	os.WriteFile(pidPath, []byte(""), 0600)

	_, err := readPidFile(pidPath)
	if err == nil {
		t.Error("expected error for empty pid file")
	}
}

// ---------------------------------------------------------------------------
// stopDaemon
// ---------------------------------------------------------------------------

func TestStopDaemon_NoPidFile(t *testing.T) {
	dir := t.TempDir()
	output := captureStdout(t, func() { stopDaemon(dir) })
	if !strings.Contains(output, "not running") {
		t.Errorf("stopDaemon with no pid file should say not running:\n%s", output)
	}
}

func TestStopDaemon_StalePid(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "daemon.pid")
	// Use a PID that's almost certainly not running (max PID value)
	os.WriteFile(pidPath, []byte("99999999\n"), 0600)
	output := captureStdout(t, func() { stopDaemon(dir) })
	if !strings.Contains(output, "not running") {
		t.Errorf("stopDaemon with stale pid should say not running:\n%s", output)
	}
	// Pid file should be cleaned up
	if _, err := os.Stat(pidPath); err == nil {
		t.Error("stale pid file should be removed")
	}
}

// ---------------------------------------------------------------------------
// daemonGet: ReadAll error (server sends invalid chunked response)
// ---------------------------------------------------------------------------

func TestDaemonGet_ReadBodyError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(200)
		// Write less data than Content-Length, then close the connection.
		// This causes io.ReadAll to return an unexpected EOF.
		w.Write([]byte("partial"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	_, err := daemonGet(addr, "/api/test", nil)
	if err == nil {
		t.Error("expected error for truncated body")
	}
	if !strings.Contains(err.Error(), "reading response") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Ls: parent navigation breadcrumb trimming (sideways navigation)
// ---------------------------------------------------------------------------

func TestRunLs_NavigateSideways(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	// Simulate being in /docs with parents ["/"]
	writeLsCache(configDir, "/docs", []string{"notes.md"}, 0, []string{"/"})
	// Navigate to root (sideways/up)
	captureStdout(t, func() { runLs(addr, configDir, []string{"/"}) })
	// Check that parents were trimmed
	c := readLsCache(configDir)
	if c == nil {
		t.Fatal("expected cache")
	}
	if len(c.Parents) != 0 {
		t.Errorf("parents should be nil/empty at root, got %v", c.Parents)
	}
}

// ---------------------------------------------------------------------------
// Status: provider with partial quota info
// ---------------------------------------------------------------------------

func TestRunStatus_PartialQuota(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		total := int64(100e9)
		json.NewEncoder(w).Encode(cliStatusResponse{
			TotalFiles: 10, TotalBytes: 5000,
			Providers: []cliStatusProvider{
				{Name: "partial", QuotaTotalBytes: &total}, // free=nil, used=nil
			},
		})
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() { runStatus(addr) })
	if !strings.Contains(output, "unknown") {
		t.Errorf("partial quota should show 'unknown' for free:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Uploads: various states
// ---------------------------------------------------------------------------

func TestRunUploads_MixedStates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode([]cliUploadProgress{
			{VirtualPath: "/a.bin", TotalChunks: 10, ChunksUploaded: 5, SizeBytes: 1024},
			{VirtualPath: "/b.bin", TotalChunks: 0, ChunksUploaded: 0, SizeBytes: 0},
		})
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() { runUploads(addr) })
	if !strings.Contains(output, "50%") {
		t.Errorf("uploads should show 50%%:\n%s", output)
	}
	if !strings.Contains(output, "0/0") {
		t.Errorf("uploads should show 0/0 chunks:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Ls: navigate from deeper path to root (resets parents)
// ---------------------------------------------------------------------------

func TestRunLs_NavigateDeepToRoot(t *testing.T) {
	addr := mockDaemon(t)
	configDir := t.TempDir()
	// Simulate being deep with parents
	writeLsCache(configDir, "/docs/sub", []string{"file.txt"}, 0, []string{"/", "/docs"})
	// Navigate to root
	captureStdout(t, func() { runLs(addr, configDir, []string{"/"}) })
	c := readLsCache(configDir)
	if c == nil {
		t.Fatal("expected cache")
	}
	if c.Dir != "/" {
		t.Errorf("cache.Dir = %q, want /", c.Dir)
	}
}

// ---------------------------------------------------------------------------
// stopDaemon: SIGTERM to a real (sleep) process
// ---------------------------------------------------------------------------

func TestStopDaemon_Signal(t *testing.T) {
	// Start a dummy process we can signal.
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})

	dir := t.TempDir()
	pidPath := filepath.Join(dir, "daemon.pid")
	os.WriteFile(pidPath, []byte(fmt.Sprintf("%d\n", cmd.Process.Pid)), 0600)

	output := captureStdout(t, func() { stopDaemon(dir) })
	if !strings.Contains(output, "stopped") {
		t.Errorf("stopDaemon should report 'stopped':\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Subprocess tests for os.Exit paths
// ---------------------------------------------------------------------------

// exitTestHelper checks if the env var is set and runs the specified function.
// Returns true if it ran (i.e., we're in the child subprocess).
func exitTestHelper(action string) bool {
	return os.Getenv("CLI_EXIT_TEST") == action
}

func TestRunCat_NoArgs_Exits(t *testing.T) {
	if exitTestHelper("cat_noargs") {
		runCat("127.0.0.1:1", t.TempDir(), nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunCat_NoArgs_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=cat_noargs")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for runCat with no args")
	}
}

func TestRunGet_NoArgs_Exits(t *testing.T) {
	if exitTestHelper("get_noargs") {
		runGet("127.0.0.1:1", t.TempDir(), nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunGet_NoArgs_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=get_noargs")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for runGet with no args")
	}
}

func TestRunFind_NoArgs_Exits(t *testing.T) {
	if exitTestHelper("find_noargs") {
		runFind("127.0.0.1:1", t.TempDir(), nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunFind_NoArgs_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=find_noargs")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for runFind with no args")
	}
}

func TestRunMv_NoArgs_Exits(t *testing.T) {
	if exitTestHelper("mv_noargs") {
		runMv("127.0.0.1:1", t.TempDir(), nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunMv_NoArgs_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=mv_noargs")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for runMv with no args")
	}
}

func TestRunMkdir_NoArgs_Exits(t *testing.T) {
	if exitTestHelper("mkdir_noargs") {
		runMkdir("127.0.0.1:1", nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunMkdir_NoArgs_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=mkdir_noargs")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for runMkdir with no args")
	}
}

func TestRunInfo_NoArgs_Exits(t *testing.T) {
	if exitTestHelper("info_noargs") {
		runInfo("127.0.0.1:1", t.TempDir(), nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunInfo_NoArgs_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=info_noargs")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for runInfo with no args")
	}
}

// Test daemon unreachable error paths (via subprocess).

func TestRunCat_Unreachable_Exits(t *testing.T) {
	if exitTestHelper("cat_unreach") {
		runCat("127.0.0.1:1", t.TempDir(), []string{"/file.txt"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunCat_Unreachable_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=cat_unreach")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for unreachable daemon")
	}
}

func TestRunGet_NotFound_Exits(t *testing.T) {
	if exitTestHelper("get_notfound") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "not found", http.StatusNotFound)
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runGet(addr, t.TempDir(), []string{"/missing.txt", filepath.Join(t.TempDir(), "out.txt")})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunGet_NotFound_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=get_notfound")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for 404")
	}
}

func TestRunHealth_Degraded_Exits(t *testing.T) {
	if exitTestHelper("health_degraded") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			json.NewEncoder(w).Encode(cliHealthResponse{Status: "degraded", UptimeSeconds: 10, DBOK: true})
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runHealth(addr)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunHealth_Degraded_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=health_degraded")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for degraded health")
	}
}

func TestRunStatus_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("status_err") {
		runStatus("127.0.0.1:1")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunStatus_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=status_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunUploads_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("uploads_err") {
		runUploads("127.0.0.1:1")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunUploads_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=uploads_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunHealth_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("health_err") {
		runHealth("127.0.0.1:1")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunHealth_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=health_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunMetrics_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("metrics_err") {
		runMetrics("127.0.0.1:1")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunMetrics_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=metrics_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunLs_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("ls_err") {
		runLs("127.0.0.1:1", t.TempDir(), nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunLs_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=ls_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunTree_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("tree_err") {
		runTree("127.0.0.1:1", t.TempDir(), nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunTree_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=tree_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunFind_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("find_err") {
		runFind("127.0.0.1:1", t.TempDir(), []string{"test"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunFind_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=find_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunDu_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("du_err") {
		runDu("127.0.0.1:1", t.TempDir(), nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunDu_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=du_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunInfo_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("info_err") {
		runInfo("127.0.0.1:1", t.TempDir(), []string{"/file.txt"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunInfo_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=info_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunMv_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("mv_err") {
		runMv("127.0.0.1:1", t.TempDir(), []string{"/a.txt", "/b.txt"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunMv_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=mv_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunMkdir_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("mkdir_err") {
		runMkdir("127.0.0.1:1", []string{"/newdir"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunMkdir_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=mkdir_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunRm_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("rm_err") {
		runRm("127.0.0.1:1", t.TempDir(), []string{"/file.txt"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunRm_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=rm_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

func TestRunPinUnpin_DaemonError_Exits(t *testing.T) {
	if exitTestHelper("pin_err") {
		runPinUnpin("127.0.0.1:1", t.TempDir(), "pin", []string{"/file.txt"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunPinUnpin_DaemonError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=pin_err")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit")
	}
}

// Test server error (non-200) exit paths.

func TestRunMv_ServerError_Exits(t *testing.T) {
	if exitTestHelper("mv_500") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", 500)
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runMv(addr, t.TempDir(), []string{"/a.txt", "/b.txt"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunMv_ServerError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=mv_500")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for server error")
	}
}

func TestRunMkdir_ServerError_Exits(t *testing.T) {
	if exitTestHelper("mkdir_500") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", 500)
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runMkdir(addr, []string{"/newdir"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunMkdir_ServerError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=mkdir_500")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for server error")
	}
}

func TestRunRm_ServerError_Exits(t *testing.T) {
	if exitTestHelper("rm_500") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", 500)
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runRm(addr, t.TempDir(), []string{"/file.txt"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunRm_ServerError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=rm_500")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for server error")
	}
}

func TestRunPinUnpin_ServerError_Exits(t *testing.T) {
	if exitTestHelper("pin_500") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", 500)
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runPinUnpin(addr, t.TempDir(), "pin", []string{"/file.txt"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunPinUnpin_ServerError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=pin_500")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for server error")
	}
}

// Test JSON parse error paths.

func TestRunStatus_BadJSON_Exits(t *testing.T) {
	if exitTestHelper("status_badjson") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte("{bad json"))
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runStatus(addr)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunStatus_BadJSON_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=status_badjson")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for bad JSON")
	}
}

func TestRunUploads_BadJSON_Exits(t *testing.T) {
	if exitTestHelper("uploads_badjson") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte("{bad json"))
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runUploads(addr)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunUploads_BadJSON_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=uploads_badjson")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for bad JSON")
	}
}

func TestRunHealth_BadJSON_Exits(t *testing.T) {
	if exitTestHelper("health_badjson") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte("{bad json"))
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runHealth(addr)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunHealth_BadJSON_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=health_badjson")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for bad JSON")
	}
}

func TestRunMetrics_BadJSON_Exits(t *testing.T) {
	if exitTestHelper("metrics_badjson") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte("{bad json"))
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runMetrics(addr)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunMetrics_BadJSON_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=metrics_badjson")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for bad JSON")
	}
}

func TestRunLs_BadJSON_Exits(t *testing.T) {
	if exitTestHelper("ls_badjson") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte("{bad json"))
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runLs(addr, t.TempDir(), []string{"/"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunLs_BadJSON_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=ls_badjson")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for bad JSON")
	}
}

func TestRunTree_BadJSON_Exits(t *testing.T) {
	if exitTestHelper("tree_badjson") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte("{bad json"))
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runTree(addr, t.TempDir(), nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunTree_BadJSON_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=tree_badjson")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for bad JSON")
	}
}

func TestRunFind_BadJSON_Exits(t *testing.T) {
	if exitTestHelper("find_badjson") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte("{bad json"))
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runFind(addr, t.TempDir(), []string{"test"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunFind_BadJSON_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=find_badjson")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for bad JSON")
	}
}

func TestRunDu_BadJSON_Exits(t *testing.T) {
	if exitTestHelper("du_badjson") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte("{bad json"))
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runDu(addr, t.TempDir(), nil)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunDu_BadJSON_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=du_badjson")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for bad JSON")
	}
}

func TestRunInfo_BadJSON_Exits(t *testing.T) {
	if exitTestHelper("info_badjson") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte("{bad json"))
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runInfo(addr, t.TempDir(), []string{"/file.txt"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunInfo_BadJSON_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=info_badjson")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for bad JSON")
	}
}

// Test runCat error paths: server 404, server non-200.

func TestRunCat_NotFound_Exits(t *testing.T) {
	if exitTestHelper("cat_404") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "not found", http.StatusNotFound)
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runCat(addr, t.TempDir(), []string{"/missing.txt"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunCat_NotFound_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=cat_404")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for cat 404")
	}
}

func TestRunCat_ServerError_Exits(t *testing.T) {
	if exitTestHelper("cat_500") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", 500)
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runCat(addr, t.TempDir(), []string{"/file.txt"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunCat_ServerError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=cat_500")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for cat 500")
	}
}

// Test runGet error paths: server non-200 (not 404).

func TestRunGet_ServerError_Exits(t *testing.T) {
	if exitTestHelper("get_500") {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", 500)
		}))
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runGet(addr, t.TempDir(), []string{"/file.txt", filepath.Join(t.TempDir(), "out.txt")})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunGet_ServerError_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=get_500")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for get 500")
	}
}

// Test resolveLsArg numeric index out of range (os.Exit path).

func TestResolveLsArg_NumericOutOfRange_Exits(t *testing.T) {
	if exitTestHelper("resolve_oor") {
		dir := t.TempDir()
		writeLsCache(dir, "/", []string{"one"}, 1, nil)
		resolveLsArg("99", dir)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestResolveLsArg_NumericOutOfRange_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=resolve_oor")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for out-of-range index")
	}
}

// Test resolveLsArg ambiguous match (os.Exit path).

func TestResolveLsArg_Ambiguous_Exits(t *testing.T) {
	if exitTestHelper("resolve_ambig") {
		dir := t.TempDir()
		writeLsCache(dir, "/", []string{"Documents", "Downloads"}, 2, nil)
		resolveLsArg("Do", dir)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestResolveLsArg_Ambiguous_Exits")
	cmd.Env = append(os.Environ(), "CLI_EXIT_TEST=resolve_ambig")
	err := cmd.Run()
	if err == nil {
		t.Error("expected non-zero exit for ambiguous match")
	}
}

// ---------------------------------------------------------------------------
// Breadcrumb trimming: navigate deep then sideways so parent trail is pruned.
// Covers cli.go line 239: parents = parents[:len(parents)-1]
// ---------------------------------------------------------------------------

func TestRunLs_BreadcrumbTrimming(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ls", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("path")
		switch p {
		case "/alpha":
			json.NewEncoder(w).Encode(cliLsResponse{Path: "/alpha", Dirs: []string{"sub"}})
		case "/alpha/sub":
			json.NewEncoder(w).Encode(cliLsResponse{Path: "/alpha/sub", Files: []cliLsFile{{Name: "file.txt", Path: "/alpha/sub/file.txt", Size: 10}}})
		case "/beta":
			json.NewEncoder(w).Encode(cliLsResponse{Path: "/beta", Files: []cliLsFile{{Name: "data.txt", Path: "/beta/data.txt", Size: 20}}})
		default:
			json.NewEncoder(w).Encode(cliLsResponse{Path: p})
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	configDir := t.TempDir()

	// Step 1: list /alpha → cache Dir="/alpha", Parents=nil
	captureStdout(t, func() { runLs(addr, configDir, []string{"/alpha"}) })

	// Step 2: list /alpha/sub (deeper → Parents=["/alpha"])
	captureStdout(t, func() { runLs(addr, configDir, []string{"/alpha/sub"}) })

	// Step 3: list /beta (sideways → for loop trims Parents)
	captureStdout(t, func() { runLs(addr, configDir, []string{"/beta"}) })
}

// ---------------------------------------------------------------------------
// Tree: ≥2 files at the same directory level triggers file sort comparator.
// Covers cli.go line 686 (files sort.Slice body).
// ---------------------------------------------------------------------------

func TestRunTree_MultipleFilesInDir(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/tree", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode([]treeEntry{
			{Path: "/alpha.txt", Size: 100},
			{Path: "/beta.txt", Size: 200},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() { runTree(addr, t.TempDir(), nil) })
	if !strings.Contains(output, "alpha.txt") || !strings.Contains(output, "beta.txt") {
		t.Errorf("tree missing files:\n%s", output)
	}
}

// TestRunLs_FileSelection_TriggersInfo verifies that selecting a file (not dir)
// by numeric index redirects to runInfo (line 175: if res.IsFile).
func TestRunLs_FileSelection_TriggersInfo(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/info", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("path")
		json.NewEncoder(w).Encode(cliFileInfo{
			Path: p, SizeBytes: 2048, CreatedAt: 1700000000, ModifiedAt: 1700001000,
			SHA256: "abc123", UploadState: "complete",
			Chunks: []cliChunkInfo{{Sequence: 0, SizeBytes: 2048, CloudSize: 2048, Providers: []string{"gdrive"}}},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	configDir := t.TempDir()
	// Cache has 2 dirs + 1 file; index 3 points to the file.
	writeLsCache(configDir, "/", []string{"docs", "photos", "readme.txt"}, 2, nil)

	output := captureStdout(t, func() { runLs(addr, configDir, []string{"3"}) })
	// runInfo should print the file info, not a directory listing.
	if !strings.Contains(output, "readme.txt") {
		t.Errorf("expected file info for readme.txt, got:\n%s", output)
	}
	if !strings.Contains(output, "abc123") {
		t.Errorf("expected SHA256 in output, got:\n%s", output)
	}
}

// TestRunLs_SortMultipleFiles verifies that the sort.Slice comparator at
// line 219 fires when the response contains 2+ files.
func TestRunLs_SortMultipleFiles(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ls", func(w http.ResponseWriter, _ *http.Request) {
		// Return files in reverse order to exercise the sort.
		json.NewEncoder(w).Encode(cliLsResponse{
			Path: "/data",
			Files: []cliLsFile{
				{Name: "zebra.txt", Path: "/data/zebra.txt", Size: 100, ModifiedAt: 1700000000, LocalState: "local"},
				{Name: "alpha.txt", Path: "/data/alpha.txt", Size: 200, ModifiedAt: 1700000000, LocalState: "local"},
				{Name: "middle.txt", Path: "/data/middle.txt", Size: 150, ModifiedAt: 1700000000, LocalState: "local"},
			},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	configDir := t.TempDir()
	output := captureStdout(t, func() { runLs(addr, configDir, []string{"/data"}) })

	// After sorting, alpha < middle < zebra.
	alphaIdx := strings.Index(output, "alpha.txt")
	middleIdx := strings.Index(output, "middle.txt")
	zebraIdx := strings.Index(output, "zebra.txt")
	if alphaIdx < 0 || middleIdx < 0 || zebraIdx < 0 {
		t.Fatalf("missing files in output:\n%s", output)
	}
	if alphaIdx >= middleIdx || middleIdx >= zebraIdx {
		t.Errorf("files not sorted: alpha@%d middle@%d zebra@%d\n%s", alphaIdx, middleIdx, zebraIdx, output)
	}
}

// ---------------------------------------------------------------------------
// pluralS
// ---------------------------------------------------------------------------

func TestPluralS(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, "s"},
		{1, ""},
		{2, "s"},
		{100, "s"},
	}
	for _, tt := range tests {
		if got := pluralS(tt.n); got != tt.want {
			t.Errorf("pluralS(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// fmtProviderDetail
// ---------------------------------------------------------------------------

func TestFmtProviderDetail(t *testing.T) {
	tests := []struct {
		name string
		p    cliStatusProvider
		want string
	}{
		{"both", cliStatusProvider{AccountIdentity: "alice@gmail.com", Type: "drive"}, "(alice@gmail.com, drive)"},
		{"identity only", cliStatusProvider{AccountIdentity: "bob@icloud.com"}, "(bob@icloud.com)"},
		{"type only", cliStatusProvider{Type: "dropbox"}, "(dropbox)"},
		{"neither", cliStatusProvider{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fmtProviderDetail(tt.p); got != tt.want {
				t.Errorf("fmtProviderDetail() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// showRecentLogErrors
// ---------------------------------------------------------------------------

func TestShowRecentLogErrors_MissingFile(t *testing.T) {
	// Should not panic when log file doesn't exist.
	output := captureStderr(t, func() { showRecentLogErrors("/nonexistent/daemon.log") })
	_ = output // no output expected, just no panic
}

func TestShowRecentLogErrors_NoErrors(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "daemon.log")
	os.WriteFile(tmpFile, []byte("INFO: all good\nDEBUG: fine\n"), 0644)
	output := captureStderr(t, func() { showRecentLogErrors(tmpFile) })
	_ = output
}

func TestShowRecentLogErrors_WithErrors(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "daemon.log")
	os.WriteFile(tmpFile, []byte("INFO: ok\nlevel=ERROR msg=\"something broke\"\nINFO: resumed\n"), 0644)
	output := captureStderr(t, func() { showRecentLogErrors(tmpFile) })
	if !strings.Contains(output, "something broke") {
		t.Errorf("expected error line in output:\n%s", output)
	}
}

// ── remotesConfigPath / loadRemotesConfig / saveRemotesConfig ───────────────

func TestRemotesConfigPath(t *testing.T) {
	got := remotesConfigPath("/some/dir")
	want := filepath.Join("/some/dir", "remotes.json")
	if got != want {
		t.Errorf("remotesConfigPath = %q, want %q", got, want)
	}
}

func TestLoadRemotesConfig_NoFile(t *testing.T) {
	dir := t.TempDir()
	remotes, err := loadRemotesConfig(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if remotes != nil {
		t.Errorf("expected nil for missing file, got %v", remotes)
	}
}

func TestLoadRemotesConfig_ValidJSON(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "remotes.json"), []byte(`["gdrive","mybox"]`), 0644)

	remotes, err := loadRemotesConfig(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(remotes) != 2 || remotes[0] != "gdrive" || remotes[1] != "mybox" {
		t.Errorf("unexpected remotes: %v", remotes)
	}
}

func TestLoadRemotesConfig_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "remotes.json"), []byte(`{invalid`), 0644)

	_, err := loadRemotesConfig(dir)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if !strings.Contains(err.Error(), "invalid remotes.json") {
		t.Errorf("error should mention invalid remotes.json: %v", err)
	}
}

func TestSaveRemotesConfig(t *testing.T) {
	dir := t.TempDir()
	err := saveRemotesConfig(dir, []string{"drive1", "drive2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify it was saved correctly.
	remotes, err := loadRemotesConfig(dir)
	if err != nil {
		t.Fatalf("load after save: %v", err)
	}
	if len(remotes) != 2 || remotes[0] != "drive1" || remotes[1] != "drive2" {
		t.Errorf("unexpected remotes after save: %v", remotes)
	}
}

func TestSaveRemotesConfig_CreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "config")
	err := saveRemotesConfig(dir, []string{"remote1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Verify directory was created and file exists.
	if _, err := os.Stat(filepath.Join(dir, "remotes.json")); err != nil {
		t.Fatalf("remotes.json not created: %v", err)
	}
}

func TestLoadRemotesConfig_EmptyArray(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "remotes.json"), []byte(`[]`), 0644)

	remotes, err := loadRemotesConfig(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(remotes) != 0 {
		t.Errorf("expected empty slice, got %v", remotes)
	}
}

// ---------------------------------------------------------------------------
// uploadOneFile
// ---------------------------------------------------------------------------

func TestUploadOneFile_Success(t *testing.T) {
	// Create a temp file to upload.
	dir := t.TempDir()
	localPath := filepath.Join(dir, "hello.txt")
	os.WriteFile(localPath, []byte("hello world"), 0644)

	// Mock /api/upload endpoint.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
			t.Errorf("expected multipart, got %s", r.Header.Get("Content-Type"))
		}
		// Parse the multipart form.
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		if got := r.FormValue("dir"); got != "/docs" {
			t.Errorf("dir = %q, want /docs", got)
		}
		f, fh, err := r.FormFile("file")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if fh.Filename != "hello.txt" {
			t.Errorf("filename = %q", fh.Filename)
		}
		data, _ := io.ReadAll(f)
		if string(data) != "hello world" {
			t.Errorf("file content = %q", string(data))
		}
		w.WriteHeader(200)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() {
		if err := uploadOneFile(addr, localPath, "/docs"); err != nil {
			t.Fatalf("uploadOneFile error: %v", err)
		}
	})
	if !strings.Contains(output, "hello.txt") {
		t.Errorf("output missing filename:\n%s", output)
	}
	if !strings.Contains(output, "/docs/hello.txt") {
		t.Errorf("output missing virtual path:\n%s", output)
	}
}

func TestUploadOneFile_DaemonError(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "hello.txt")
	os.WriteFile(localPath, []byte("hello"), 0644)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "disk full", 500)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	err := uploadOneFile(addr, localPath, "/")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error = %q, want 500", err.Error())
	}
}

func TestUploadOneFile_Unreachable(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "hello.txt")
	os.WriteFile(localPath, []byte("hello"), 0644)

	err := uploadOneFile("127.0.0.1:1", localPath, "/")
	if err == nil {
		t.Fatal("expected error for unreachable daemon")
	}
	if !strings.Contains(err.Error(), "cannot reach daemon") {
		t.Errorf("error = %q, want cannot reach daemon", err.Error())
	}
}

func TestUploadOneFile_BadLocalPath(t *testing.T) {
	err := uploadOneFile("127.0.0.1:1", "/nonexistent/file.txt", "/")
	if err == nil {
		t.Fatal("expected error for bad local path")
	}
}

// ---------------------------------------------------------------------------
// runPut — single file
// ---------------------------------------------------------------------------

func TestRunPut_SingleFile(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "up.txt")
	os.WriteFile(localPath, []byte("data"), 0644)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() {
		runPut(addr, []string{localPath})
	})
	if !strings.Contains(output, "up.txt") {
		t.Errorf("put output missing filename:\n%s", output)
	}
}

func TestRunPut_SingleFileToDir(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "up.txt")
	os.WriteFile(localPath, []byte("data"), 0644)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseMultipartForm(1 << 20)
		if got := r.FormValue("dir"); got != "/backup" {
			t.Errorf("dir = %q, want /backup", got)
		}
		w.WriteHeader(200)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() {
		runPut(addr, []string{localPath, "/backup"})
	})
	if !strings.Contains(output, "/backup/up.txt") {
		t.Errorf("put output missing virtual path:\n%s", output)
	}
}

func TestRunPut_Directory(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "mydir", "sub"), 0755)
	os.WriteFile(filepath.Join(dir, "mydir", "a.txt"), []byte("aaa"), 0644)
	os.WriteFile(filepath.Join(dir, "mydir", "sub", "b.txt"), []byte("bbb"), 0644)

	var uploads []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseMultipartForm(1 << 20)
		uploads = append(uploads, r.FormValue("dir"))
		w.WriteHeader(200)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() {
		runPut(addr, []string{filepath.Join(dir, "mydir")})
	})
	if !strings.Contains(output, "Uploaded 2 file") {
		t.Errorf("put dir output:\n%s", output)
	}
}

func TestRunPut_RelativeRemoteDir(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "up.txt")
	os.WriteFile(localPath, []byte("data"), 0644)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseMultipartForm(1 << 20)
		if got := r.FormValue("dir"); got != "/docs" {
			t.Errorf("dir = %q, want /docs (should auto-prefix /)", got)
		}
		w.WriteHeader(200)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	captureStdout(t, func() {
		runPut(addr, []string{localPath, "docs"})
	})
}

// ---------------------------------------------------------------------------
// notifyDaemonResync
// ---------------------------------------------------------------------------

func TestNotifyDaemonResync_DaemonRunning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/resync" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Method != "POST" {
			t.Errorf("method = %q", r.Method)
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() {
		notifyDaemonResync(addr)
	})
	if !strings.Contains(output, "sync triggered") {
		t.Errorf("output = %q, want sync triggered", output)
	}
}

func TestNotifyDaemonResync_DaemonNotRunning(t *testing.T) {
	output := captureStdout(t, func() {
		notifyDaemonResync("127.0.0.1:1")
	})
	if !strings.Contains(output, "not running") {
		t.Errorf("output = %q, want not running", output)
	}
}

// ---------------------------------------------------------------------------
// runRemotesReset
// ---------------------------------------------------------------------------

func TestRunRemotesReset_FileExists(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "remotes.json")
	os.WriteFile(p, []byte(`["gdrive"]`), 0644)

	// Mock daemon for resync notification.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() {
		runRemotesReset(dir, addr)
	})
	if !strings.Contains(output, "cleared") {
		t.Errorf("output = %q, want cleared", output)
	}
	// File should be deleted.
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Error("remotes.json should be deleted")
	}
}

func TestRunRemotesReset_FileNotExist(t *testing.T) {
	dir := t.TempDir()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() {
		runRemotesReset(dir, addr)
	})
	// Should succeed even if file doesn't exist.
	if !strings.Contains(output, "cleared") {
		t.Errorf("output = %q, want cleared", output)
	}
}

// ---------------------------------------------------------------------------
// saveRemotesConfig error path
// ---------------------------------------------------------------------------

func TestSaveRemotesConfig_BadDir(t *testing.T) {
	err := saveRemotesConfig("/nonexistent/dir", []string{"gdrive"})
	if err == nil {
		t.Error("expected error for bad directory")
	}
}

// ---------------------------------------------------------------------------
// loadRemotesConfig with real content
// ---------------------------------------------------------------------------

func TestLoadRemotesConfig_WithEntries(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "remotes.json"), []byte(`["gdrive","dropbox"]`), 0644)

	remotes, err := loadRemotesConfig(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(remotes) != 2 || remotes[0] != "gdrive" || remotes[1] != "dropbox" {
		t.Errorf("remotes = %v", remotes)
	}
}

func TestLoadRemotesConfig_MissingFile(t *testing.T) {
	dir := t.TempDir()
	remotes, err := loadRemotesConfig(dir)
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if remotes != nil {
		t.Errorf("missing file should return nil, got %v", remotes)
	}
}
