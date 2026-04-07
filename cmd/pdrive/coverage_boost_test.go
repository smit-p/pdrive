package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// ── helpers ─────────────────────────────────────────────────────────────────

// overrideListRemotes replaces listRemotesFn for the test duration.
func overrideListRemotes(t *testing.T, fn func() ([]string, error)) {
	t.Helper()
	old := listRemotesFn
	listRemotesFn = fn
	t.Cleanup(func() { listRemotesFn = old })
}

// mockResyncServer returns a daemon addr that records whether /api/resync was called.
func mockResyncServer(t *testing.T) (addr string, called *bool) {
	t.Helper()
	c := false
	mux := http.NewServeMux()
	mux.HandleFunc("/api/resync", func(w http.ResponseWriter, _ *http.Request) {
		c = true
		w.WriteHeader(200)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://"), &c
}

// ── runRemotes dispatch ─────────────────────────────────────────────────────

func TestRunRemotes_NoArgs_ListsRemotes(t *testing.T) {
	overrideListRemotes(t, func() ([]string, error) {
		return []string{"gdrive", "onedrive"}, nil
	})
	dir := t.TempDir()
	output := captureStdout(t, func() {
		runRemotes(dir, "127.0.0.1:0", nil)
	})
	if !strings.Contains(output, "gdrive") || !strings.Contains(output, "onedrive") {
		t.Errorf("expected remotes list, got:\n%s", output)
	}
}

func TestRunRemotes_Reset(t *testing.T) {
	dir := t.TempDir()
	addr, _ := mockResyncServer(t)

	// Pre-create a remotes.json file.
	saveRemotesConfig(dir, []string{"gdrive"})

	output := captureStdout(t, func() {
		runRemotes(dir, addr, []string{"reset"})
	})
	if !strings.Contains(output, "cleared") {
		t.Errorf("expected 'cleared' message, got:\n%s", output)
	}
}

func TestRunRemotes_UnknownSubcommand(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		runRemotes(t.TempDir(), "127.0.0.1:0", []string{"bogus"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunRemotes_UnknownSubcommand")
	if !strings.Contains(stderr, "Unknown remotes subcommand") {
		t.Errorf("expected error about unknown subcommand, got:\n%s", stderr)
	}
}

func TestRunRemotes_AddMissingName(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		runRemotes(t.TempDir(), "127.0.0.1:0", []string{"add"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunRemotes_AddMissingName")
	if !strings.Contains(stderr, "Usage") {
		t.Errorf("expected usage message, got:\n%s", stderr)
	}
}

func TestRunRemotes_RemoveMissingName(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		runRemotes(t.TempDir(), "127.0.0.1:0", []string{"remove"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunRemotes_RemoveMissingName")
	if !strings.Contains(stderr, "Usage") {
		t.Errorf("expected usage message, got:\n%s", stderr)
	}
}

// ── runRemotesList ──────────────────────────────────────────────────────────

func TestRunRemotesList_AllEnabled(t *testing.T) {
	overrideListRemotes(t, func() ([]string, error) {
		return []string{"gdrive", "s3"}, nil
	})
	dir := t.TempDir()
	output := captureStdout(t, func() {
		runRemotesList(dir)
	})
	if !strings.Contains(output, "* gdrive") || !strings.Contains(output, "* s3") {
		t.Errorf("expected all remotes starred, got:\n%s", output)
	}
	if !strings.Contains(output, "all remotes are used") {
		t.Errorf("expected 'all remotes' message, got:\n%s", output)
	}
}

func TestRunRemotesList_SomeEnabled(t *testing.T) {
	overrideListRemotes(t, func() ([]string, error) {
		return []string{"gdrive", "s3", "onedrive"}, nil
	})
	dir := t.TempDir()
	saveRemotesConfig(dir, []string{"gdrive", "s3"})

	output := captureStdout(t, func() {
		runRemotesList(dir)
	})
	if !strings.Contains(output, "* gdrive") || !strings.Contains(output, "* s3") {
		t.Errorf("expected gdrive and s3 starred, got:\n%s", output)
	}
	// onedrive should NOT be starred.
	if strings.Contains(output, "* onedrive") {
		t.Error("onedrive should not be starred")
	}
}

func TestRunRemotesList_NoRemotes(t *testing.T) {
	overrideListRemotes(t, func() ([]string, error) {
		return nil, nil
	})
	dir := t.TempDir()
	output := captureStdout(t, func() {
		runRemotesList(dir)
	})
	if !strings.Contains(output, "No rclone remotes configured") {
		t.Errorf("expected no remotes message, got:\n%s", output)
	}
}

func TestRunRemotesList_Error(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		listRemotesFn = func() ([]string, error) {
			return nil, fmt.Errorf("rclone not found")
		}
		runRemotesList(t.TempDir())
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunRemotesList_Error")
	if !strings.Contains(stderr, "rclone not found") {
		t.Errorf("expected error, got:\n%s", stderr)
	}
}

// ── runRemotesAdd ───────────────────────────────────────────────────────────

func TestRunRemotesAdd_ValidRemote(t *testing.T) {
	overrideListRemotes(t, func() ([]string, error) {
		return []string{"gdrive", "s3", "onedrive"}, nil
	})
	dir := t.TempDir()
	addr, called := mockResyncServer(t)

	output := captureStdout(t, func() {
		runRemotesAdd(dir, addr, []string{"gdrive", "s3"})
	})
	if !strings.Contains(output, "Enabled gdrive") || !strings.Contains(output, "Enabled s3") {
		t.Errorf("expected 'Enabled' messages, got:\n%s", output)
	}
	if !*called {
		t.Error("daemon resync should have been triggered")
	}
	// Verify saved config.
	stored, _ := loadRemotesConfig(dir)
	if len(stored) != 2 {
		t.Errorf("expected 2 stored remotes, got %d", len(stored))
	}
}

func TestRunRemotesAdd_AlreadyEnabled(t *testing.T) {
	overrideListRemotes(t, func() ([]string, error) {
		return []string{"gdrive", "s3"}, nil
	})
	dir := t.TempDir()
	saveRemotesConfig(dir, []string{"gdrive"})
	addr, _ := mockResyncServer(t)

	output := captureStdout(t, func() {
		runRemotesAdd(dir, addr, []string{"gdrive"})
	})
	if !strings.Contains(output, "already enabled") {
		t.Errorf("expected 'already enabled' message, got:\n%s", output)
	}
}

func TestRunRemotesAdd_InvalidRemote(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		listRemotesFn = func() ([]string, error) {
			return []string{"gdrive"}, nil
		}
		runRemotesAdd(t.TempDir(), "127.0.0.1:0", []string{"bogus"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunRemotesAdd_InvalidRemote")
	if !strings.Contains(stderr, "not a configured rclone remote") {
		t.Errorf("expected validation error, got:\n%s", stderr)
	}
}

func TestRunRemotesAdd_ListError(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		listRemotesFn = func() ([]string, error) {
			return nil, fmt.Errorf("rclone broken")
		}
		runRemotesAdd(t.TempDir(), "127.0.0.1:0", []string{"gdrive"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunRemotesAdd_ListError")
	if !strings.Contains(stderr, "rclone broken") {
		t.Errorf("expected error, got:\n%s", stderr)
	}
}

// ── runRemotesRemove ────────────────────────────────────────────────────────

func TestRunRemotesRemove_FromExplicitList(t *testing.T) {
	overrideListRemotes(t, func() ([]string, error) {
		return []string{"gdrive", "s3", "onedrive"}, nil
	})
	dir := t.TempDir()
	saveRemotesConfig(dir, []string{"gdrive", "s3", "onedrive"})
	addr, called := mockResyncServer(t)

	output := captureStdout(t, func() {
		runRemotesRemove(dir, addr, []string{"s3"})
	})
	if !strings.Contains(output, "Disabled s3") {
		t.Errorf("expected 'Disabled' message, got:\n%s", output)
	}
	if !*called {
		t.Error("daemon resync should have been triggered")
	}
	stored, _ := loadRemotesConfig(dir)
	if len(stored) != 2 {
		t.Errorf("expected 2 remaining, got %d", len(stored))
	}
}

func TestRunRemotesRemove_FromAllRemotes(t *testing.T) {
	// When no config exists yet, remove populates from all remotes first.
	overrideListRemotes(t, func() ([]string, error) {
		return []string{"gdrive", "s3", "onedrive"}, nil
	})
	dir := t.TempDir()
	addr, _ := mockResyncServer(t)

	output := captureStdout(t, func() {
		runRemotesRemove(dir, addr, []string{"onedrive"})
	})
	if !strings.Contains(output, "Disabled onedrive") {
		t.Errorf("expected 'Disabled' message, got:\n%s", output)
	}
	stored, _ := loadRemotesConfig(dir)
	if len(stored) != 2 {
		t.Errorf("expected 2 remaining, got %d", len(stored))
	}
}

func TestRunRemotesRemove_NotEnabled(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		listRemotesFn = func() ([]string, error) {
			return []string{"gdrive", "s3"}, nil
		}
		dir := t.TempDir()
		saveRemotesConfig(dir, []string{"gdrive"})
		runRemotesRemove(dir, "127.0.0.1:0", []string{"s3"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunRemotesRemove_NotEnabled")
	if !strings.Contains(stderr, "not an enabled remote") {
		t.Errorf("expected error, got:\n%s", stderr)
	}
}

func TestRunRemotesRemove_AllWouldBeRemoved(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		listRemotesFn = func() ([]string, error) {
			return []string{"gdrive"}, nil
		}
		dir := t.TempDir()
		saveRemotesConfig(dir, []string{"gdrive"})
		runRemotesRemove(dir, "127.0.0.1:0", []string{"gdrive"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunRemotesRemove_AllWouldBeRemoved")
	if !strings.Contains(stderr, "cannot remove all remotes") {
		t.Errorf("expected error about removing all remotes, got:\n%s", stderr)
	}
}

func TestRunRemotesRemove_LoadError(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		listRemotesFn = func() ([]string, error) {
			return []string{"gdrive"}, nil
		}
		dir := t.TempDir()
		// Write invalid JSON to remotes.json.
		os.WriteFile(filepath.Join(dir, "remotes.json"), []byte("{bad"), 0600)
		runRemotesRemove(dir, "127.0.0.1:0", []string{"gdrive"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunRemotesRemove_LoadError")
	if !strings.Contains(stderr, "Error") {
		t.Errorf("expected load error, got:\n%s", stderr)
	}
}

func TestRunRemotesRemove_ListError(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		listRemotesFn = func() ([]string, error) {
			return nil, fmt.Errorf("rclone not found")
		}
		dir := t.TempDir()
		// No existing config → will call listRemotesFn
		runRemotesRemove(dir, "127.0.0.1:0", []string{"gdrive"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunRemotesRemove_ListError")
	if !strings.Contains(stderr, "rclone not found") {
		t.Errorf("expected error, got:\n%s", stderr)
	}
}

// ── runGet — additional error paths ─────────────────────────────────────────

func TestRunGet_404_Exits(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		addr := mockDaemon(t)
		runGet(addr, t.TempDir(), []string{"/nonexistent.txt"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunGet_404_Exits")
	if !strings.Contains(stderr, "not found") {
		t.Errorf("expected 'not found' error, got:\n%s", stderr)
	}
}

func TestRunGet_WriteError_Exits(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		addr := mockDaemon(t)
		// Change to a read-only directory so file creation fails.
		readOnly := filepath.Join(t.TempDir(), "readonly")
		os.MkdirAll(readOnly, 0500)
		os.Chdir(readOnly)
		runGet(addr, t.TempDir(), []string{"/readme.txt"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunGet_WriteError_Exits")
	if !strings.Contains(stderr, "Error") {
		t.Errorf("expected error, got:\n%s", stderr)
	}
}

func TestRunGet_Unreachable_Exits(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		runGet("127.0.0.1:1", t.TempDir(), []string{"/file.txt"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunGet_Unreachable_Exits")
	if !strings.Contains(stderr, "cannot reach") {
		t.Errorf("expected reach error, got:\n%s", stderr)
	}
}

// ── runPut — additional error paths ─────────────────────────────────────────

func TestRunPut_NoArgs_Exits(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		runPut("127.0.0.1:0", nil)
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunPut_NoArgs_Exits")
	if !strings.Contains(stderr, "Usage") {
		t.Errorf("expected usage, got:\n%s", stderr)
	}
}

func TestRunPut_BadLocalPath_Exits(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		runPut("127.0.0.1:0", []string{"/nonexistent/path/that/does/not/exist"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunPut_BadLocalPath_Exits")
	if !strings.Contains(stderr, "Error") {
		t.Errorf("expected error, got:\n%s", stderr)
	}
}

func TestRunPut_DirectoryWalk(t *testing.T) {
	// Create a temp directory with nested files.
	dir := t.TempDir()
	sub := filepath.Join(dir, "upload-test")
	os.MkdirAll(filepath.Join(sub, "nested"), 0755)
	os.WriteFile(filepath.Join(sub, "a.txt"), []byte("aaa"), 0600)
	os.WriteFile(filepath.Join(sub, "nested", "b.txt"), []byte("bbb"), 0600)

	// Use a mock server that accepts uploads.
	mux := http.NewServeMux()
	mux.HandleFunc("/api/upload", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, `{"status":"ok"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	output := captureStdout(t, func() {
		runPut(addr, []string{sub, "/remote"})
	})
	if !strings.Contains(output, "Uploaded 2") {
		t.Errorf("expected 2 files uploaded, got:\n%s", output)
	}
}

// ── readPassword — via pipe ─────────────────────────────────────────────────

func TestReadPassword_PipeWithNewline(t *testing.T) {
	// Create a pipe with password + newline
	r, w, _ := os.Pipe()
	w.WriteString("my-secret-pw\n")
	w.Close()

	old := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = old }()

	pw, err := readPassword()
	if err != nil {
		t.Fatalf("readPassword() error: %v", err)
	}
	if pw != "my-secret-pw" {
		t.Errorf("readPassword() = %q, want %q", pw, "my-secret-pw")
	}
}

// ── runCat — additional error path ──────────────────────────────────────────

func TestRunCat_ServerError_Exits_StatusCode(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/download", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "server broke", 503)
		})
		srv := httptest.NewServer(mux)
		defer srv.Close()
		addr := strings.TrimPrefix(srv.URL, "http://")
		runCat(addr, t.TempDir(), []string{"/file.txt"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunCat_ServerError_Exits_StatusCode")
	if !strings.Contains(stderr, "server broke") {
		t.Errorf("expected server error, got:\n%s", stderr)
	}
}

// ── browse.go — additional coverage ─────────────────────────────────────────

func TestBrowseModel_FetchDir_BadJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ls", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("{invalid"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	m := newBrowseModel(strings.TrimPrefix(srv.URL, "http://"), t.TempDir())
	cmd := m.fetchDir("/")
	msg := cmd().(lsResultMsg)
	if msg.err == nil {
		t.Error("expected error for bad JSON")
	}
}

func TestBrowseModel_FetchDir_BreadcrumbTrimming(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ls", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("path")
		json.NewEncoder(w).Encode(cliLsResponse{
			Path: p,
			Dirs: []string{"child"},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	// Start at /a/b with parents [/a]
	m := newBrowseModel(addr, t.TempDir())
	m.path = "/a/b"
	m.parents = []string{"/", "/a"}

	// Navigate sideways to /a/c — should trim /a/b from parents.
	cmd := m.fetchDir("/a/c")
	msg := cmd().(lsResultMsg)
	if msg.err != nil {
		t.Fatalf("fetchDir: %v", msg.err)
	}
	// Parents should trim down since /a/c is not a child of /a/b.
	if len(msg.parents) > 2 {
		t.Errorf("parents should have been trimmed, got %v", msg.parents)
	}
}

func TestBrowseModel_FetchInfo_BadJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/info", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("{bad json"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	m := newBrowseModel(addr, t.TempDir())
	m.path = "/"
	m.items = []browseItem{{Name: "file.txt", IsDir: false}}

	cmd := m.fetchInfo(0)
	msg := cmd().(fileInfoMsg)
	if msg.err == nil {
		t.Error("expected error for bad JSON")
	}
}

func TestBrowseModel_FetchInfo_Error(t *testing.T) {
	m := newBrowseModel("127.0.0.1:1", t.TempDir())
	m.path = "/"
	m.items = []browseItem{{Name: "file.txt", IsDir: false}}

	cmd := m.fetchInfo(0)
	msg := cmd().(fileInfoMsg)
	if msg.err == nil {
		t.Error("expected error for unreachable daemon")
	}
}

func TestBrowseModel_DoAction_Unpin(t *testing.T) {
	addr := browseMockDaemon(t)
	m := newBrowseModel(addr, t.TempDir())
	m.path = "/docs"
	m.items = []browseItem{
		{Name: "file.txt", IsDir: false, SizeBytes: 100},
	}

	cmd := m.doAction("unpin", 0)
	msg := cmd().(actionDoneMsg)
	if msg.err != nil {
		t.Fatalf("doAction unpin: %v", msg.err)
	}
	if !strings.Contains(msg.msg, "unpin") {
		t.Errorf("expected 'unpin' in message, got %q", msg.msg)
	}
}

func TestBrowseModel_DoAction_Unreachable(t *testing.T) {
	m := newBrowseModel("127.0.0.1:1", t.TempDir())
	m.path = "/"
	m.items = []browseItem{{Name: "file.txt", IsDir: false}}

	cmd := m.doAction("pin", 0)
	msg := cmd().(actionDoneMsg)
	if msg.err == nil {
		t.Error("expected error for unreachable daemon")
	}
}

func TestBrowseModel_DoAction_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/pin", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "quota exceeded", 507)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	m := newBrowseModel(addr, t.TempDir())
	m.path = "/"
	m.items = []browseItem{{Name: "file.txt", IsDir: false}}

	cmd := m.doAction("pin", 0)
	msg := cmd().(actionDoneMsg)
	if msg.err == nil {
		t.Error("expected error for server error")
	}
	if !strings.Contains(msg.err.Error(), "quota exceeded") {
		t.Errorf("expected quota error, got: %v", msg.err)
	}
}

func TestBrowseModel_FetchDir_RootBreadcrumb(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ls", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(cliLsResponse{
			Path: "/",
			Dirs: []string{"docs"},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	// Navigate from /a back to / — parents should be nil.
	m := newBrowseModel(addr, t.TempDir())
	m.path = "/a"
	m.parents = []string{"/"}

	cmd := m.fetchDir("/")
	msg := cmd().(lsResultMsg)
	if msg.err != nil {
		t.Fatalf("fetchDir: %v", msg.err)
	}
	if msg.parents != nil {
		t.Errorf("parents should be nil at root, got %v", msg.parents)
	}
}

func TestEnsureVisible_WithInfoPanel(t *testing.T) {
	m := browseModel{
		height:   20,
		cursor:   15,
		offset:   0,
		items:    make([]browseItem, 20),
		fileInfo: &cliFileInfo{Path: "/test"},
	}
	m.ensureVisible()
	// listHeight = 20 - 14 = 6 (min 3), cursor=15 should need offset adjustment
	if m.offset == 0 {
		t.Error("offset should be non-zero when cursor is past viewport with info panel")
	}
}

func TestEnsureVisible_SmallHeight(t *testing.T) {
	m := browseModel{
		height: 5, // very small — listHeight = max(5-6, 3) = 3
		cursor: 5,
		offset: 0,
		items:  make([]browseItem, 10),
	}
	m.ensureVisible()
	if m.offset == 0 {
		t.Error("offset should be non-zero with small height and cursor at 5")
	}
}

func TestEnsureVisible_InfoErr(t *testing.T) {
	m := browseModel{
		height:  20,
		cursor:  15,
		offset:  0,
		items:   make([]browseItem, 20),
		infoErr: fmt.Errorf("some error"),
	}
	m.ensureVisible()
	if m.offset == 0 {
		t.Error("offset should be non-zero with infoErr and cursor past viewport")
	}
}

// ── showRecentLogErrors ─────────────────────────────────────────────────────

func TestShowRecentLogErrors_LargeFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	var lines []string
	for i := 0; i < 50; i++ {
		lines = append(lines, fmt.Sprintf("log line %d", i))
	}
	os.WriteFile(logPath, []byte(strings.Join(lines, "\n")), 0600)

	stderr := captureStderr(t, func() {
		showRecentLogErrors(logPath)
	})
	// Should only show last ~10 lines.
	if !strings.Contains(stderr, "log line 49") {
		t.Error("expected last log lines")
	}
	if strings.Contains(stderr, "log line 0") {
		t.Error("should not show very early log lines")
	}
}

// ── runUnmount ──────────────────────────────────────────────────────────────

func TestRunUnmount_CallsStopDaemon(t *testing.T) {
	dir := t.TempDir()
	output := captureStdout(t, func() {
		runUnmount("127.0.0.1:0", dir)
	})
	if !strings.Contains(output, "not running") {
		t.Errorf("expected 'not running' message, got:\n%s", output)
	}
}

// ── browse View — with various model states ─────────────────────────────────

func TestBrowseModel_View_ActionDoneRefetch(t *testing.T) {
	m := testModelWithItems()
	result, cmd := m.Update(actionDoneMsg{msg: "delete /docs/file.txt"})
	model := result.(browseModel)
	if !model.loading {
		t.Error("expected loading=true after successful action")
	}
	if cmd == nil {
		t.Error("expected non-nil cmd to refetch directory")
	}
}

func TestBrowseModel_View_ActionError(t *testing.T) {
	m := testModelWithItems()
	result, _ := m.Update(actionDoneMsg{err: fmt.Errorf("delete failed")})
	model := result.(browseModel)
	if model.err == nil {
		t.Error("expected error to be set after failed action")
	}
}

func TestBrowseModel_DownloadKey(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 1 // move to a file (not a directory)
	// The download key triggers a pin confirmation.
	result, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'d'}})
	model := result.(browseModel)
	if model.confirmMsg == "" {
		t.Error("expected confirm prompt for download")
	}
	if model.confirmAction != "pin" {
		t.Errorf("expected pin action, got %q", model.confirmAction)
	}
}

// ── helpers for subprocess exit testing ──────────────────────────────────────

// captureSubprocessStderr runs the named test in a subprocess and returns stderr.
func captureSubprocessStderr(t *testing.T, testName string) string {
	t.Helper()
	cmd := captureSubprocess(t, testName)
	stderr, _ := io.ReadAll(cmd)
	return string(stderr)
}

func captureSubprocess(t *testing.T, testName string) io.Reader {
	t.Helper()
	//nolint:gosec // test helper
	c := exec.Command(os.Args[0], "-test.run=^"+testName+"$")
	c.Env = append(os.Environ(), "BE_CRASHER=1")
	pipe, _ := c.StderrPipe()
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Wait() }) //nolint:errcheck
	return pipe
}
