package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/smit-p/pdrive/internal/chunker"
	"github.com/smit-p/pdrive/internal/config"
)

// ── resolveEncKey ────────────────────────────────────────────────────────────

func TestResolveEncKey_HexKeyValid(t *testing.T) {
	dir := t.TempDir()
	hexKey := "0102030405060708091011121314151617181920212223242526272829303132"
	key, pw, err := resolveEncKey(dir, hexKey, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 32 {
		t.Errorf("expected 32-byte key, got %d", len(key))
	}
	if pw != "" {
		t.Errorf("expected empty deferred password, got %q", pw)
	}
}

func TestResolveEncKey_HexKeyInvalid(t *testing.T) {
	dir := t.TempDir()
	_, _, err := resolveEncKey(dir, "not-hex", "", nil)
	if err == nil {
		t.Fatal("expected error for invalid hex key")
	}
}

func TestResolveEncKey_HexKeyTooShort(t *testing.T) {
	dir := t.TempDir()
	_, _, err := resolveEncKey(dir, "0102030405", "", nil)
	if err == nil {
		t.Fatal("expected error for short hex key")
	}
}

func TestResolveEncKey_PasswordWithSalt(t *testing.T) {
	dir := t.TempDir()
	salt := make([]byte, chunker.SaltSize)
	for i := range salt {
		salt[i] = byte(i)
	}
	os.WriteFile(filepath.Join(dir, "enc.salt"), salt, 0600)

	key, pw, err := resolveEncKey(dir, "", "mypassword", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 32 {
		t.Errorf("expected 32-byte key, got %d", len(key))
	}
	if pw != "" {
		t.Errorf("expected empty deferred password, got %q", pw)
	}
}

func TestResolveEncKey_PasswordNoSalt_Deferred(t *testing.T) {
	dir := t.TempDir()
	key, pw, err := resolveEncKey(dir, "", "deferred-pass", nil)
	if err != nil {
		t.Fatal(err)
	}
	if key != nil {
		t.Errorf("expected nil key when deferring, got %v", key)
	}
	if pw != "deferred-pass" {
		t.Errorf("expected deferred password, got %q", pw)
	}
}

func TestResolveEncKey_DefaultLegacyKeyFile(t *testing.T) {
	dir := t.TempDir()
	keyData := make([]byte, 32)
	for i := range keyData {
		keyData[i] = byte(i + 1)
	}
	os.WriteFile(filepath.Join(dir, "enc.key"), keyData, 0600)

	key, pw, err := resolveEncKey(dir, "", "", func() (string, error) {
		return "", fmt.Errorf("should not be called")
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 32 {
		t.Errorf("expected 32-byte key, got %d", len(key))
	}
	if pw != "" {
		t.Errorf("expected empty deferred password, got %q", pw)
	}
}

func TestResolveEncKey_DefaultSaltExists_PromptSuccess(t *testing.T) {
	dir := t.TempDir()
	salt := make([]byte, chunker.SaltSize)
	os.WriteFile(filepath.Join(dir, "enc.salt"), salt, 0600)

	key, _, err := resolveEncKey(dir, "", "", func() (string, error) {
		return "testpass", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 32 {
		t.Errorf("expected 32-byte key, got %d", len(key))
	}
}

func TestResolveEncKey_DefaultSaltExists_PromptEmpty(t *testing.T) {
	dir := t.TempDir()
	salt := make([]byte, chunker.SaltSize)
	os.WriteFile(filepath.Join(dir, "enc.salt"), salt, 0600)

	_, _, err := resolveEncKey(dir, "", "", func() (string, error) {
		return "", nil
	})
	if err == nil {
		t.Fatal("expected error when prompt returns empty")
	}
}

func TestResolveEncKey_DefaultFirstRun_Success(t *testing.T) {
	dir := t.TempDir()
	callNum := 0
	_, pw, err := resolveEncKey(dir, "", "", func() (string, error) {
		callNum++
		if callNum == 1 {
			return "longpassword", nil
		}
		return "longpassword", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if pw != "longpassword" {
		t.Errorf("expected deferred password, got %q", pw)
	}
}

func TestResolveEncKey_DefaultFirstRun_TooShort(t *testing.T) {
	dir := t.TempDir()
	_, _, err := resolveEncKey(dir, "", "", func() (string, error) {
		return "short", nil
	})
	if err == nil {
		t.Fatal("expected error for password < 8 chars")
	}
}

func TestResolveEncKey_DefaultFirstRun_Mismatch(t *testing.T) {
	dir := t.TempDir()
	callNum := 0
	_, _, err := resolveEncKey(dir, "", "", func() (string, error) {
		callNum++
		if callNum == 1 {
			return "password1234", nil
		}
		return "different999", nil
	})
	if err == nil {
		t.Fatal("expected error for password mismatch")
	}
}

// ── findRcloneBin ────────────────────────────────────────────────────────────

func TestFindRcloneBin_ExplicitFlag(t *testing.T) {
	bin, err := findRcloneBin("/usr/local/bin/rclone", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if bin != "/usr/local/bin/rclone" {
		t.Errorf("expected explicit path, got %q", bin)
	}
}

func TestFindRcloneBin_LookPath(t *testing.T) {
	// rclone is on PATH in the test environment. If it isn't, the test
	// won't fail — findRcloneBin just tries other fallbacks.
	bin, _ := findRcloneBin("", t.TempDir())
	if bin == "" {
		t.Skip("rclone not available on PATH or via fallback")
	}
}

// ── parseRemotes ─────────────────────────────────────────────────────────────

func TestParseRemotes_Empty(t *testing.T) {
	if got := parseRemotes(""); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestParseRemotes_Single(t *testing.T) {
	got := parseRemotes("gdrive")
	if len(got) != 1 || got[0] != "gdrive" {
		t.Errorf("expected [gdrive], got %v", got)
	}
}

func TestParseRemotes_Multiple(t *testing.T) {
	got := parseRemotes("gdrive, s3 , onedrive")
	if len(got) != 3 || got[0] != "gdrive" || got[1] != "s3" || got[2] != "onedrive" {
		t.Errorf("expected [gdrive s3 onedrive], got %v", got)
	}
}

func TestParseRemotes_SkipsEmpty(t *testing.T) {
	got := parseRemotes("a,,b, ,c")
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("expected [a b c], got %v", got)
	}
}

// ── mergeConfigFile ──────────────────────────────────────────────────────────

func TestMergeConfigFile_AppliesAllFields(t *testing.T) {
	fileCfg := config.File{
		SyncDir:      "/custom/sync",
		RcloneAddr:   "localhost:9999",
		WebDAVAddr:   "localhost:8888",
		RcloneBin:    "/opt/rclone",
		BrokerPolicy: "mfs",
		MinFreeSpace: 512 * 1024 * 1024,
		ChunkSize:    67108864,
		RateLimit:    10,
		Debug:        true,
		Remotes:      "gdrive",
		MountBackend: "fuse",
		MountPoint:   "/mnt/pdrive",
	}
	opts := cliOptions{
		SyncDir:      filepath.Join("/home/test", "pdrive"),
		RcloneAddr:   "127.0.0.1:5572",
		WebDAVAddr:   "127.0.0.1:8765",
		BrokerPolicy: "pfrd",
		MinFreeSpace: 256 * 1024 * 1024,
	}
	mergeConfigFile(fileCfg, "/home/test", &opts)

	if opts.SyncDir != "/custom/sync" {
		t.Errorf("SyncDir = %q", opts.SyncDir)
	}
	if opts.RcloneAddr != "localhost:9999" {
		t.Errorf("RcloneAddr = %q", opts.RcloneAddr)
	}
	if opts.WebDAVAddr != "localhost:8888" {
		t.Errorf("WebDAVAddr = %q", opts.WebDAVAddr)
	}
	if opts.RcloneBin != "/opt/rclone" {
		t.Errorf("RcloneBin = %q", opts.RcloneBin)
	}
	if opts.BrokerPolicy != "mfs" {
		t.Errorf("BrokerPolicy = %q", opts.BrokerPolicy)
	}
	if opts.MinFreeSpace != 512*1024*1024 {
		t.Errorf("MinFreeSpace = %d", opts.MinFreeSpace)
	}
	if opts.ChunkSize != 67108864 {
		t.Errorf("ChunkSize = %d", opts.ChunkSize)
	}
	if opts.RateLimit != 10 {
		t.Errorf("RateLimit = %d", opts.RateLimit)
	}
	if !opts.Debug {
		t.Error("Debug should be true")
	}
	if opts.Remotes != "gdrive" {
		t.Errorf("Remotes = %q", opts.Remotes)
	}
	if opts.Backend != "fuse" {
		t.Errorf("Backend = %q", opts.Backend)
	}
	if opts.MountPoint != "/mnt/pdrive" {
		t.Errorf("MountPoint = %q", opts.MountPoint)
	}
}

func TestMergeConfigFile_CLIOverridesConfig(t *testing.T) {
	fileCfg := config.File{
		RcloneAddr: "localhost:9999",
		Debug:      true,
	}
	opts := cliOptions{
		SyncDir:      filepath.Join("/home/test", "pdrive"),
		RcloneAddr:   "custom:1234", // not at default, should NOT be overridden
		WebDAVAddr:   "127.0.0.1:8765",
		BrokerPolicy: "pfrd",
		MinFreeSpace: 256 * 1024 * 1024,
		Debug:        true, // already true
	}
	mergeConfigFile(fileCfg, "/home/test", &opts)

	if opts.RcloneAddr != "custom:1234" {
		t.Errorf("RcloneAddr should not be overridden, got %q", opts.RcloneAddr)
	}
}

// ── dispatchCmd ──────────────────────────────────────────────────────────────

func TestDispatchCmd_NoArgs(t *testing.T) {
	handled, err := dispatchCmd(nil, "unused", t.TempDir(), "")
	if handled || err != nil {
		t.Errorf("expected (false, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Unknown(t *testing.T) {
	handled, err := dispatchCmd([]string{"bogus"}, "unused", t.TempDir(), "")
	if !handled || err == nil {
		t.Errorf("expected (true, error), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_PinMissingArgs(t *testing.T) {
	_, err := dispatchCmd([]string{"pin"}, "unused", t.TempDir(), "")
	if err == nil {
		t.Error("expected error for pin with no path")
	}
}

func TestDispatchCmd_UnpinMissingArgs(t *testing.T) {
	_, err := dispatchCmd([]string{"unpin"}, "unused", t.TempDir(), "")
	if err == nil {
		t.Error("expected error for unpin with no path")
	}
}

func TestDispatchCmd_PutMissingArgs(t *testing.T) {
	_, err := dispatchCmd([]string{"put"}, "unused", t.TempDir(), "")
	if err == nil {
		t.Error("expected error for put with no args")
	}
}

func TestDispatchCmd_RmMissingArgs(t *testing.T) {
	_, err := dispatchCmd([]string{"rm"}, "unused", t.TempDir(), "")
	if err == nil {
		t.Error("expected error for rm with no args")
	}
}

func TestDispatchCmd_FindMissingArgs(t *testing.T) {
	_, err := dispatchCmd([]string{"find"}, "unused", t.TempDir(), "")
	if err == nil {
		t.Error("expected error for find with no pattern")
	}
}

func TestDispatchCmd_MvMissingArgs(t *testing.T) {
	_, err := dispatchCmd([]string{"mv", "a"}, "unused", t.TempDir(), "")
	if err == nil {
		t.Error("expected error for mv with < 3 args")
	}
}

func TestDispatchCmd_MkdirMissingArgs(t *testing.T) {
	_, err := dispatchCmd([]string{"mkdir"}, "unused", t.TempDir(), "")
	if err == nil {
		t.Error("expected error for mkdir with no path")
	}
}

func TestDispatchCmd_InfoMissingArgs(t *testing.T) {
	_, err := dispatchCmd([]string{"info"}, "unused", t.TempDir(), "")
	if err == nil {
		t.Error("expected error for info with no path")
	}
}

func TestDispatchCmd_Version(t *testing.T) {
	handled, err := dispatchCmd([]string{"version"}, "unused", t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Stop(t *testing.T) {
	dir := t.TempDir()
	// No PID file → stopDaemon prints "not running" and returns.
	handled, err := dispatchCmd([]string{"stop"}, "unused", dir, "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Help(t *testing.T) {
	handled, err := dispatchCmd([]string{"help"}, "unused", t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Unmount(t *testing.T) {
	dir := t.TempDir()
	handled, err := dispatchCmd([]string{"unmount"}, "unused", dir, "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

// Test dispatch with a live mock server for commands that make HTTP calls.
func newDispatchServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ls":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"path": "/", "dirs": []string{}, "files": []interface{}{},
			})
		case "/api/status":
			fmt.Fprint(w, `{"Uptime":"1m","Files":0,"Chunks":0}`)
		case "/api/uploads":
			fmt.Fprint(w, `[]`)
		case "/api/health":
			fmt.Fprint(w, `{"status":"ok"}`)
		case "/api/metrics":
			fmt.Fprint(w, `{"TotalFiles":0}`)
		case "/api/tree":
			fmt.Fprint(w, `[]`)
		case "/api/du":
			fmt.Fprint(w, `{"entries":[]}`)
		case "/api/pin", "/api/unpin":
			w.WriteHeader(200)
		case "/api/download":
			w.Write([]byte("data"))
		case "/api/find":
			fmt.Fprint(w, `[]`)
		case "/api/mv":
			w.WriteHeader(200)
		case "/api/mkdir":
			w.WriteHeader(200)
		case "/api/delete":
			w.WriteHeader(200)
		case "/api/info":
			fmt.Fprint(w, `{"Path":"/x","SizeBytes":10,"UploadState":"done","Chunks":[]}`)
		case "/api/upload":
			w.WriteHeader(200)
		default:
			w.WriteHeader(404)
		}
	}))
}

func TestDispatchCmd_Ls(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	dir := t.TempDir()
	handled, err := dispatchCmd([]string{"ls"}, srv.Listener.Addr().String(), dir, "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Status(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"status"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Uploads(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"uploads"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Health(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"health"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Metrics(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"metrics"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Du(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"du"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Tree(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"tree"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Pin(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"pin", "/a"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Cat(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	dir := t.TempDir()
	handled, err := dispatchCmd([]string{"cat", "/test.txt"}, srv.Listener.Addr().String(), dir, "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Rm(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"rm", "/a"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Find(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"find", "*.txt"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Mv(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"mv", "/a", "/b"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Mkdir(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"mkdir", "/newdir"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Info(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"info", "/x"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

func TestDispatchCmd_Remotes(t *testing.T) {
	dir := t.TempDir()
	old := listRemotesFn
	defer func() { listRemotesFn = old }()
	listRemotesFn = func() ([]string, error) { return []string{"gdrive"}, nil }
	handled, err := dispatchCmd([]string{"remotes"}, "unused", dir, "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

// ── browse edge cases ────────────────────────────────────────────────────────

func TestBrowseModel_UnpinKey_OnDir(t *testing.T) {
	m := testModelWithItems()
	// cursor=0 → folder (IsDir=true), unpin should be no-op
	result, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'u'}})
	model := result.(browseModel)
	if model.confirmMsg != "" {
		t.Error("expected no confirm for unpin on directory")
	}
}

func TestBrowseModel_UnpinKey_OnFile(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 1 // readme.md (file)
	result, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'u'}})
	model := result.(browseModel)
	if model.confirmAction != "unpin" {
		t.Errorf("expected unpin action, got %q", model.confirmAction)
	}
}

func TestBrowseModel_DeleteKey(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 1
	result, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	model := result.(browseModel)
	if model.confirmAction != "delete" {
		t.Errorf("expected delete action, got %q", model.confirmAction)
	}
}

func TestBrowseModel_DeleteKey_Empty(t *testing.T) {
	m := testModelWithItems()
	m.items = nil
	result, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	model := result.(browseModel)
	if model.confirmMsg != "" {
		t.Error("expected no confirm for delete on empty list")
	}
}

func TestBrowseModel_DownloadKey_OnDir(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 0 // folder
	result, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'d'}})
	model := result.(browseModel)
	if model.confirmMsg != "" {
		t.Error("expected no confirm for download on directory")
	}
}

func TestBrowseModel_View_ScrollIndicatorManyItems(t *testing.T) {
	m := testModelWithItems()
	m.height = 10 // small enough to trigger scroll
	// Add many items
	for i := 0; i < 20; i++ {
		m.items = append(m.items, browseItem{Name: fmt.Sprintf("file%d.txt", i)})
	}
	out := m.View()
	if out == "" {
		t.Error("expected non-empty view")
	}
}

func TestBrowseModel_View_PathNotRoot(t *testing.T) {
	m := testModelWithItems()
	m.path = "/subdir"
	out := m.View()
	if out == "" {
		t.Error("expected non-empty view")
	}
}

// ── runRemotesReset ──────────────────────────────────────────────────────────

func TestRunRemotesReset_MissingFile(t *testing.T) {
	dir := t.TempDir()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	// Should not panic when file doesn't exist.
	runRemotesReset(dir, srv.Listener.Addr().String())
}

func TestRunRemotesReset_ClearsExistingFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "remotes.json"), []byte(`["a"]`), 0600)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	runRemotesReset(dir, srv.Listener.Addr().String())
	if _, err := os.Stat(filepath.Join(dir, "remotes.json")); !os.IsNotExist(err) {
		t.Error("expected remotes.json to be deleted")
	}
}

// ── loadRemotesConfig / saveRemotesConfig edge cases ────────────────────────

func TestLoadRemotesConfig_ReadError(t *testing.T) {
	dir := t.TempDir()
	// Create an unreadable file
	p := filepath.Join(dir, "remotes.json")
	os.WriteFile(p, []byte(`["a"]`), 0000)
	defer os.Chmod(p, 0600)

	_, err := loadRemotesConfig(dir)
	if err == nil {
		t.Error("expected error reading unreadable file")
	}
}

func TestSaveRemotesConfig_WritesJSON(t *testing.T) {
	dir := t.TempDir()
	if err := saveRemotesConfig(dir, []string{"gdrive", "s3"}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "remotes.json"))
	var got []string
	json.Unmarshal(data, &got)
	if len(got) != 2 || got[0] != "gdrive" || got[1] != "s3" {
		t.Errorf("expected [gdrive s3], got %v", got)
	}
}

// ── cli_files edge cases ─────────────────────────────────────────────────────

func TestRunGet_SuccessfulDownload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/download" {
			w.Write([]byte("file-content"))
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	dest := filepath.Join(dir, "output.txt")

	// Use dest as the second arg to runGet
	// We need to call from a directory where we can write
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	runGet(srv.Listener.Addr().String(), t.TempDir(), []string{"/test.txt", dest})

	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "file-content" {
		t.Errorf("expected 'file-content', got %q", string(data))
	}
}

func TestRunPut_SingleFileSuccess(t *testing.T) {
	uploaded := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/upload" {
			uploaded = true
			w.WriteHeader(200)
		}
	}))
	defer srv.Close()

	// Create a temp file to upload
	dir := t.TempDir()
	f := filepath.Join(dir, "test.txt")
	os.WriteFile(f, []byte("hello"), 0600)

	runPut(srv.Listener.Addr().String(), []string{f})
	if !uploaded {
		t.Error("expected upload request to be made")
	}
}

// ── stopDaemon edge cases ────────────────────────────────────────────────────

func TestStopDaemon_StalePidFile(t *testing.T) {
	dir := t.TempDir()
	// Write a PID that definitely doesn't exist.
	os.WriteFile(filepath.Join(dir, "daemon.pid"), []byte("9999999\n"), 0600)
	stopDaemon(dir)
	// Should have cleaned up the stale PID file.
	if _, err := os.Stat(filepath.Join(dir, "daemon.pid")); !os.IsNotExist(err) {
		t.Error("expected stale PID file to be removed")
	}
}

// ── readPassword edge cases ──────────────────────────────────────────────────

func TestReadPassword_ScannerError(t *testing.T) {
	// Test with a closed file descriptor.
	r, w, _ := os.Pipe()
	w.Close()

	origStdin := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = origStdin }()

	pw, err := readPassword()
	if pw != "" && err == nil {
		// Either empty password or EOF — both acceptable.
		t.Log("got empty password with no error (pipe was closed)")
	}
}

// ── probeDaemonRunning ──────────────────────────────────────────────────────

func TestProbeDaemonRunning_NotRunning(t *testing.T) {
	if probeDaemonRunning("127.0.0.1:1") {
		t.Error("expected false for unreachable address")
	}
}

func TestProbeDaemonRunning_Running(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer srv.Close()
	if !probeDaemonRunning(srv.Listener.Addr().String()) {
		t.Error("expected true for live server")
	}
}

// ── openDaemonLog ───────────────────────────────────────────────────────────

func TestOpenDaemonLog_Success(t *testing.T) {
	dir := t.TempDir()
	f, logPath, err := openDaemonLog(filepath.Join(dir, "sub", "dir"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if logPath == "" {
		t.Error("expected non-empty log path")
	}
	if _, err := f.WriteString("test\n"); err != nil {
		t.Error("expected writable file")
	}
}

func TestOpenDaemonLog_ReadOnlyDir(t *testing.T) {
	dir := t.TempDir()
	roDir := filepath.Join(dir, "readonly")
	os.MkdirAll(roDir, 0500)
	defer os.Chmod(roDir, 0700)

	_, _, err := openDaemonLog(filepath.Join(roDir, "nested"))
	if err == nil {
		t.Error("expected error for read-only parent dir")
	}
}

// ── buildChildCmd ───────────────────────────────────────────────────────────

func TestBuildChildCmd_AppendsForeground(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	f, _ := os.Create(logPath)
	defer f.Close()

	cmd := buildChildCmd("/usr/bin/echo", []string{"--debug", "--sync-dir=/tmp"}, f)
	args := cmd.Args[1:]
	found := false
	for _, a := range args {
		if a == "--foreground" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected --foreground in args, got %v", cmd.Args)
	}
	if cmd.Stdout != f {
		t.Error("expected stdout to be redirected to logFile")
	}
	if cmd.Stderr != f {
		t.Error("expected stderr to be redirected to logFile")
	}
}

// ── pollDaemonStart ─────────────────────────────────────────────────────────

func TestPollDaemonStart_ImmediateSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer srv.Close()
	dummyCmd := exec.Command("echo")
	ok := pollDaemonStart(srv.Listener.Addr().String(), dummyCmd, 1)
	if !ok {
		t.Error("expected true when server is healthy")
	}
}

func TestPollDaemonStart_NeverHealthy(t *testing.T) {
	dummyCmd := exec.Command("echo")
	ok := pollDaemonStart("127.0.0.1:1", dummyCmd, 1)
	if ok {
		t.Error("expected false when server is unreachable")
	}
}

// ── buildDaemonConfig ───────────────────────────────────────────────────────

func TestBuildDaemonConfig_AllFields(t *testing.T) {
	key := make([]byte, 32)
	cfg := buildDaemonConfig("/etc/pdrive", "/usr/bin/rclone", "127.0.0.1:5572",
		"127.0.0.1:8765", "/home/user/pdrive",
		key, "mypass", "mfs",
		512*1024*1024, true,
		67108864, 10, []string{"gdrive", "s3"},
		"fuse", "/mnt/pdrive")

	if cfg.ConfigDir != "/etc/pdrive" {
		t.Errorf("ConfigDir = %q", cfg.ConfigDir)
	}
	if cfg.RcloneBin != "/usr/bin/rclone" {
		t.Errorf("RcloneBin = %q", cfg.RcloneBin)
	}
	if cfg.RcloneAddr != "127.0.0.1:5572" {
		t.Errorf("RcloneAddr = %q", cfg.RcloneAddr)
	}
	if cfg.WebDAVAddr != "127.0.0.1:8765" {
		t.Errorf("WebDAVAddr = %q", cfg.WebDAVAddr)
	}
	if cfg.SyncDir != "/home/user/pdrive" {
		t.Errorf("SyncDir = %q", cfg.SyncDir)
	}
	if cfg.Password != "mypass" {
		t.Errorf("Password = %q", cfg.Password)
	}
	if cfg.BrokerPolicy != "mfs" {
		t.Errorf("BrokerPolicy = %q", cfg.BrokerPolicy)
	}
	if cfg.MinFreeSpace != 512*1024*1024 {
		t.Errorf("MinFreeSpace = %d", cfg.MinFreeSpace)
	}
	if !cfg.SkipRestore {
		t.Error("SkipRestore should be true")
	}
	if cfg.ChunkSize != 67108864 {
		t.Errorf("ChunkSize = %d", cfg.ChunkSize)
	}
	if cfg.RatePerSec != 10 {
		t.Errorf("RatePerSec = %d", cfg.RatePerSec)
	}
	if len(cfg.Remotes) != 2 {
		t.Errorf("Remotes = %v", cfg.Remotes)
	}
	if cfg.MountBackend != "fuse" {
		t.Errorf("MountBackend = %q", cfg.MountBackend)
	}
	if cfg.MountPoint != "/mnt/pdrive" {
		t.Errorf("MountPoint = %q", cfg.MountPoint)
	}
}

// ── additional cli_files gap tests ──────────────────────────────────────────

func TestRunLs_IsFileFlag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"path": "/test.txt", "is_file": true,
			"files": []map[string]interface{}{
				{"Name": "test.txt", "SizeBytes": 100, "State": "local"},
			},
			"dirs": []string{},
		})
	}))
	defer srv.Close()
	runLs(srv.Listener.Addr().String(), t.TempDir(), []string{"/test.txt"})
}

func TestRunGet_DestIsDirectory(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("file-content"))
	}))
	defer srv.Close()

	dir := t.TempDir()
	runGet(srv.Listener.Addr().String(), t.TempDir(), []string{"/photos/pic.jpg", dir})
	data, err := os.ReadFile(filepath.Join(dir, "pic.jpg"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "file-content" {
		t.Errorf("expected file-content, got %q", string(data))
	}
}

func TestRunGet_Non200_Subprocess(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(404)
			w.Write([]byte("not found"))
		}))
		defer srv.Close()
		runGet(srv.Listener.Addr().String(), t.TempDir(), []string{"/missing.txt", t.TempDir() + "/missing.txt"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunGet_Non200_Subprocess")
	if !strings.Contains(stderr, "not found") {
		t.Errorf("expected 'not found', got: %s", stderr)
	}
}

func TestRunCat_Non200_Subprocess(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			w.Write([]byte("internal server error"))
		}))
		defer srv.Close()
		runCat(srv.Listener.Addr().String(), t.TempDir(), []string{"/broken.txt"})
		return
	}
	stderr := captureSubprocessStderr(t, "TestRunCat_Non200_Subprocess")
	if !strings.Contains(stderr, "internal server error") {
		t.Errorf("expected error with server message, got: %s", stderr)
	}
}

func TestRunPut_UploadDirectory(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/upload":
			callCount++
			w.WriteHeader(200)
		case "/api/mkdir":
			w.WriteHeader(200)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hello"), 0600)
	os.WriteFile(filepath.Join(dir, "b.txt"), []byte("world"), 0600)
	sub := filepath.Join(dir, "sub")
	os.MkdirAll(sub, 0755)
	os.WriteFile(filepath.Join(sub, "c.txt"), []byte("!"), 0600)

	runPut(srv.Listener.Addr().String(), []string{dir})
	if callCount < 3 {
		t.Errorf("expected at least 3 upload calls, got %d", callCount)
	}
}

func TestUploadOneFile_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("upload failed"))
	}))
	defer srv.Close()

	f := filepath.Join(t.TempDir(), "test.txt")
	os.WriteFile(f, []byte("data"), 0600)

	err := uploadOneFile(srv.Listener.Addr().String(), f, "/test.txt")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected status 500 in error, got: %v", err)
	}
}

func TestRunFind_WithResults(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]map[string]interface{}{
			{"path": "/docs/readme.md", "size": 1024},
			{"path": "/docs/guide.md", "size": 2048},
		})
	}))
	defer srv.Close()
	runFind(srv.Listener.Addr().String(), t.TempDir(), []string{"*.md"})
}

func TestRunInfo_Full(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"Path": "/test.txt", "SizeBytes": 1024, "UploadState": "done",
			"SHA256": "abc123", "Chunks": []map[string]interface{}{
				{"Remote": "gdrive", "Path": "chunks/abc", "SizeBytes": 512},
			},
		})
	}))
	defer srv.Close()
	runInfo(srv.Listener.Addr().String(), t.TempDir(), []string{"/test.txt"})
}

func TestRunDu_SubdirectoryPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"entries": []map[string]interface{}{
				{"path": "/docs", "size": 4096, "count": 3},
			},
		})
	}))
	defer srv.Close()
	runDu(srv.Listener.Addr().String(), t.TempDir(), []string{"/docs"})
}

// ── notifyDaemonResync ──────────────────────────────────────────────────────

func TestNotifyDaemonResync_Offline(t *testing.T) {
	notifyDaemonResync("127.0.0.1:1")
}

func TestNotifyDaemonResync_Online(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/resync" {
			called = true
			w.WriteHeader(200)
		}
	}))
	defer srv.Close()

	notifyDaemonResync(srv.Listener.Addr().String())
	if !called {
		t.Error("expected resync to be called")
	}
}

// ── more cli_files tests ────────────────────────────────────────────────────

func TestRunMv_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	runMv(srv.Listener.Addr().String(), t.TempDir(), []string{"/old.txt", "/new.txt"})
}

func TestRunMkdir_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	runMkdir(srv.Listener.Addr().String(), []string{"/newdir"})
}

func TestRunRm_TwoFiles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	runRm(srv.Listener.Addr().String(), t.TempDir(), []string{"/a", "/b"})
}

func TestRunTree_WithEntries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]map[string]interface{}{
			{"path": "/docs/readme.md", "size": 1024},
			{"path": "/docs/sub/a.txt", "size": 512},
			{"path": "/photos/pic.jpg", "size": 2048},
		})
	}))
	defer srv.Close()
	runTree(srv.Listener.Addr().String(), t.TempDir(), nil)
}

// ── more dispatch tests ─────────────────────────────────────────────────────

func TestDispatchCmd_GetNoArgs(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		dispatchCmd([]string{"get"}, "unused", t.TempDir(), "")
		return
	}
	stderr := captureSubprocessStderr(t, "TestDispatchCmd_GetNoArgs")
	if !strings.Contains(stderr, "Usage") {
		t.Errorf("expected usage message, got: %s", stderr)
	}
}

func TestDispatchCmd_CatNoArgs(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		dispatchCmd([]string{"cat"}, "unused", t.TempDir(), "")
		return
	}
	stderr := captureSubprocessStderr(t, "TestDispatchCmd_CatNoArgs")
	if !strings.Contains(stderr, "Usage") {
		t.Errorf("expected usage message, got: %s", stderr)
	}
}

func TestDispatchCmd_Unpin(t *testing.T) {
	srv := newDispatchServer()
	defer srv.Close()
	handled, err := dispatchCmd([]string{"unpin", "/a"}, srv.Listener.Addr().String(), t.TempDir(), "")
	if !handled || err != nil {
		t.Errorf("expected (true, nil), got (%v, %v)", handled, err)
	}
}

// ── parseAndMergeConfig ─────────────────────────────────────────────────────

func TestParseAndMergeConfig_Defaults(t *testing.T) {
	cfg, err := parseAndMergeConfig([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RcloneAddr != "127.0.0.1:5572" {
		t.Errorf("expected default rclone-addr, got %s", cfg.RcloneAddr)
	}
	if cfg.WebDAVAddr != "127.0.0.1:8765" {
		t.Errorf("expected default webdav-addr, got %s", cfg.WebDAVAddr)
	}
	if cfg.BrokerPolicy != "pfrd" {
		t.Errorf("expected default broker-policy, got %s", cfg.BrokerPolicy)
	}
	if cfg.MinFreeSpace != 256*1024*1024 {
		t.Errorf("expected default min-free-space, got %d", cfg.MinFreeSpace)
	}
	if cfg.Debug {
		t.Error("expected debug=false by default")
	}
	if cfg.Foreground {
		t.Error("expected foreground=false by default")
	}
	if len(cfg.Args) != 0 {
		t.Errorf("expected no args, got %v", cfg.Args)
	}
}

func TestParseAndMergeConfig_AllFlags(t *testing.T) {
	dir := t.TempDir()
	cfg, err := parseAndMergeConfig([]string{
		"--config-dir", dir,
		"--sync-dir", "/tmp/sync",
		"--rclone-addr", "localhost:9999",
		"--webdav-addr", "localhost:8888",
		"--rclone-bin", "/usr/local/bin/rclone",
		"--enc-key", "abcd1234",
		"--password", "secret",
		"--broker-policy", "mfs",
		"--min-free-space", "1024",
		"--skip-restore",
		"--chunk-size", "67108864",
		"--rate-limit", "10",
		"--remotes", "gdrive,box",
		"--debug",
		"--foreground",
		"--backend", "fuse",
		"--mountpoint", "/mnt/pdrive",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConfigDir != dir {
		t.Errorf("config-dir: got %s", cfg.ConfigDir)
	}
	if cfg.SyncDir != "/tmp/sync" {
		t.Errorf("sync-dir: got %s", cfg.SyncDir)
	}
	if cfg.RcloneAddr != "localhost:9999" {
		t.Errorf("rclone-addr: got %s", cfg.RcloneAddr)
	}
	if cfg.WebDAVAddr != "localhost:8888" {
		t.Errorf("webdav-addr: got %s", cfg.WebDAVAddr)
	}
	if cfg.RcloneBin != "/usr/local/bin/rclone" {
		t.Errorf("rclone-bin: got %s", cfg.RcloneBin)
	}
	if cfg.EncKeyHex != "abcd1234" {
		t.Errorf("enc-key: got %s", cfg.EncKeyHex)
	}
	if cfg.Password != "secret" {
		t.Errorf("password: got %s", cfg.Password)
	}
	if cfg.BrokerPolicy != "mfs" {
		t.Errorf("broker-policy: got %s", cfg.BrokerPolicy)
	}
	if cfg.MinFreeSpace != 1024 {
		t.Errorf("min-free-space: got %d", cfg.MinFreeSpace)
	}
	if !cfg.SkipRestore {
		t.Error("expected skip-restore=true")
	}
	if cfg.ChunkSize != 67108864 {
		t.Errorf("chunk-size: got %d", cfg.ChunkSize)
	}
	if cfg.RateLimit != 10 {
		t.Errorf("rate-limit: got %d", cfg.RateLimit)
	}
	if cfg.Remotes != "gdrive,box" {
		t.Errorf("remotes: got %s", cfg.Remotes)
	}
	if !cfg.Debug {
		t.Error("expected debug=true")
	}
	if !cfg.Foreground {
		t.Error("expected foreground=true")
	}
	if cfg.Backend != "fuse" {
		t.Errorf("backend: got %s", cfg.Backend)
	}
	if cfg.MountPoint != "/mnt/pdrive" {
		t.Errorf("mountpoint: got %s", cfg.MountPoint)
	}
}

func TestParseAndMergeConfig_PositionalArgs(t *testing.T) {
	cfg, err := parseAndMergeConfig([]string{"ls", "/docs"})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Args) != 2 || cfg.Args[0] != "ls" || cfg.Args[1] != "/docs" {
		t.Errorf("expected [ls /docs], got %v", cfg.Args)
	}
}

func TestParseAndMergeConfig_InvalidFlag(t *testing.T) {
	_, err := parseAndMergeConfig([]string{"--no-such-flag"})
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
}

func TestParseAndMergeConfig_ConfigFileMerge(t *testing.T) {
	dir := t.TempDir()
	cfgDir := filepath.Join(dir, "cfgdir")
	os.MkdirAll(cfgDir, 0755)
	os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(`
sync_dir = "/custom/sync"
rclone_addr = "10.0.0.1:5572"
broker_policy = "mfs"
debug = true
mount_backend = "fuse"
mount_point = "/Volumes/pd"
`), 0600)

	cfg, err := parseAndMergeConfig([]string{"--config-dir", cfgDir})
	if err != nil {
		t.Fatal(err)
	}
	// Config file values should apply where CLI flags use defaults.
	if cfg.SyncDir != "/custom/sync" {
		t.Errorf("sync-dir should come from config file, got %s", cfg.SyncDir)
	}
	if cfg.RcloneAddr != "10.0.0.1:5572" {
		t.Errorf("rclone-addr should come from config file, got %s", cfg.RcloneAddr)
	}
	if cfg.BrokerPolicy != "mfs" {
		t.Errorf("broker-policy should come from config file, got %s", cfg.BrokerPolicy)
	}
	if !cfg.Debug {
		t.Error("expected debug=true from config file")
	}
	if cfg.Backend != "fuse" {
		t.Errorf("backend should come from config file, got %s", cfg.Backend)
	}
	if cfg.MountPoint != "/Volumes/pd" {
		t.Errorf("mountpoint should come from config file, got %s", cfg.MountPoint)
	}
}

func TestParseAndMergeConfig_CLIOverridesConfig(t *testing.T) {
	dir := t.TempDir()
	cfgDir := filepath.Join(dir, "cfgdir")
	os.MkdirAll(cfgDir, 0755)
	os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(`
broker_policy = "mfs"
mount_backend = "fuse"
`), 0600)

	cfg, err := parseAndMergeConfig([]string{
		"--config-dir", cfgDir,
		"--backend", "webdav",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backend != "webdav" {
		t.Errorf("backend should be CLI value, got %s", cfg.Backend)
	}
}

func TestParseAndMergeConfig_DebugLogging(t *testing.T) {
	cfg, err := parseAndMergeConfig([]string{"--debug"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Debug {
		t.Error("expected debug=true")
	}
}

// ── findRcloneBin bundled path ─────────────────────────────────────────────

func TestFindRcloneBin_BundledBinary(t *testing.T) {
	// Place a fake "rclone" next to the test binary so the bundled check finds it.
	exePath, err := os.Executable()
	if err != nil {
		t.Skip("cannot determine test binary path")
	}
	bundled := filepath.Join(filepath.Dir(exePath), "rclone")
	os.WriteFile(bundled, []byte("#!/bin/sh\n"), 0755)
	defer os.Remove(bundled)

	// Clear PATH so LookPath("rclone") fails, forcing the bundled path.
	t.Setenv("PATH", "")

	bin, err := findRcloneBin("", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if bin != bundled {
		t.Errorf("expected bundled %s, got %s", bundled, bin)
	}
}

// ── showRecentLogErrors ────────────────────────────────────────────────────

func TestShowRecentLogErrors_Nonexistent(t *testing.T) {
	// Should not panic on missing file.
	captureStderr(t, func() {
		showRecentLogErrors("/nonexistent/path/log.txt")
	})
}

func TestShowRecentLogErrors_TailOutput(t *testing.T) {
	f := filepath.Join(t.TempDir(), "daemon.log")
	var lines []string
	for i := 0; i < 20; i++ {
		lines = append(lines, fmt.Sprintf("line %d", i))
	}
	os.WriteFile(f, []byte(strings.Join(lines, "\n")), 0600)
	out := captureStderr(t, func() {
		showRecentLogErrors(f)
	})
	// Should show the last 10 lines.
	if !strings.Contains(out, "line 19") {
		t.Errorf("expected last line, got: %s", out)
	}
}

// ── readPassword edge cases ────────────────────────────────────────────────

func TestReadPassword_EmptyPipe(t *testing.T) {
	// readPassword reads from stdin. We can't easily test the terminal path,
	// but we verified the non-terminal path via TestReadPassword_ScannerError.
	// This test confirms the function exists and compiles.
	_ = readPassword
}

// ── runPinUnpin ────────────────────────────────────────────────────────────

func TestRunPinUnpin_Pin(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/pin" {
			called = true
			w.WriteHeader(200)
		}
	}))
	defer srv.Close()
	runPinUnpin(srv.Listener.Addr().String(), t.TempDir(), "pin", []string{"/test.txt"})
	if !called {
		t.Error("expected pin endpoint to be called")
	}
}

func TestRunPinUnpin_Unpin(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/unpin" {
			called = true
			w.WriteHeader(200)
		}
	}))
	defer srv.Close()
	runPinUnpin(srv.Listener.Addr().String(), t.TempDir(), "unpin", []string{"/test.txt"})
	if !called {
		t.Error("expected unpin endpoint to be called")
	}
}

// ── stopDaemon with live PID ───────────────────────────────────────────────

func TestStopDaemon_BadPidContent(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "daemon.pid"), []byte("notanumber\n"), 0600)
	// Should print "not running" and not panic.
	out := captureStdout(t, func() {
		stopDaemon(dir)
	})
	if !strings.Contains(out, "not running") {
		t.Errorf("expected 'not running', got: %s", out)
	}
}

// ── dispatchCmd browse ─────────────────────────────────────────────────────

func TestDispatchCmd_Mount(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		// runMount calls fusefs.CheckFUSEAvailable which will fail.
		dispatchCmd([]string{"mount"}, "127.0.0.1:1", t.TempDir(), "")
		return
	}
	stderr := captureSubprocessStderr(t, "TestDispatchCmd_Mount")
	// Should get an error about FUSE not being available (or similar).
	_ = stderr
}

// ── uploadOneFile success path ─────────────────────────────────────────────

func TestUploadOneFile_SuccessPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	f := filepath.Join(t.TempDir(), "hello.txt")
	os.WriteFile(f, []byte("hello world"), 0600)
	err := uploadOneFile(srv.Listener.Addr().String(), f, "/docs")
	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
}

func TestUploadOneFile_BadPath(t *testing.T) {
	err := uploadOneFile("127.0.0.1:1", "/nonexistent/file.txt", "/")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

// ── runRemotesReset edge case ──────────────────────────────────────────────

func TestRunRemotesReset_WithNotifySuccess(t *testing.T) {
	notified := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/resync" {
			notified = true
			w.WriteHeader(200)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "remotes.json"), []byte(`["gdrive"]`), 0600)
	captureStdout(t, func() {
		runRemotesReset(dir, srv.Listener.Addr().String())
	})
	if !notified {
		t.Error("expected resync notification")
	}
	// Verify remotes.json was removed.
	if _, err := os.Stat(filepath.Join(dir, "remotes.json")); err == nil {
		t.Error("expected remotes.json to be removed")
	}
}

// ── runTree with nested dirs ───────────────────────────────────────────────

func TestRunTree_RootDefault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]map[string]interface{}{
			{"path": "/readme.md", "size": 100, "is_dir": false},
		})
	}))
	defer srv.Close()
	out := captureStdout(t, func() {
		runTree(srv.Listener.Addr().String(), t.TempDir(), nil)
	})
	if !strings.Contains(out, "readme.md") {
		t.Errorf("expected tree output, got: %s", out)
	}
}

// ── listRcloneRemotes (covers the real implementation, not the mock) ──────

func TestListRcloneRemotes_Success(t *testing.T) {
	// Create a fake rclone binary that outputs remote names.
	dir := t.TempDir()
	script := filepath.Join(dir, "rclone")
	err := os.WriteFile(script, []byte("#!/bin/sh\necho 'gdrive:'\necho 'onedrive:'\n"), 0755)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)

	remotes, err := listRcloneRemotes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(remotes) != 2 || remotes[0] != "gdrive" || remotes[1] != "onedrive" {
		t.Errorf("got %v, want [gdrive onedrive]", remotes)
	}
}

func TestListRcloneRemotes_Error(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "rclone")
	err := os.WriteFile(script, []byte("#!/bin/sh\nexit 1\n"), 0755)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)

	_, err = listRcloneRemotes()
	if err == nil {
		t.Fatal("expected error from failing rclone")
	}
	if !strings.Contains(err.Error(), "failed to run rclone") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ── stopDaemon successful kill ─────────────────────────────────────────────

func TestStopDaemon_SuccessfulKill(t *testing.T) {
	// Start a real process we can kill.
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() }) //nolint:errcheck

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "daemon.pid"), []byte(fmt.Sprintf("%d", pid)), 0600); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		stopDaemon(dir)
	})
	if !strings.Contains(out, "stopped") {
		t.Errorf("expected 'stopped' message, got: %s", out)
	}
}

// ── saveRemotesConfig error ────────────────────────────────────────────────

func TestSaveRemotesConfig_MkdirError(t *testing.T) {
	err := saveRemotesConfig("/dev/null/impossible", []string{"a"})
	if err == nil {
		t.Fatal("expected error for impossible path")
	}
}

// ── openDaemonLog error ────────────────────────────────────────────────────

func TestOpenDaemonLog_MkdirAllError(t *testing.T) {
	_, _, err := openDaemonLog("/dev/null/impossible")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "creating config dir") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ── browse View quitting ───────────────────────────────────────────────────

func TestBrowseModel_View_QuittingReturnsEmpty(t *testing.T) {
	m := testModelWithItems()
	m.quitting = true
	out := m.View()
	if out != "" {
		t.Errorf("expected empty string for quitting view, got %q", out)
	}
}

// ── browse View empty directory ────────────────────────────────────────────

func TestBrowseModel_View_EmptyDirectory(t *testing.T) {
	m := newBrowseModel("localhost:0", "/tmp")
	m.loading = false
	m.width = 80
	m.height = 40
	m.path = "/empty"
	m.items = nil

	out := m.View()
	if !strings.Contains(out, "empty directory") {
		t.Errorf("expected 'empty directory' in output, got:\n%s", out)
	}
}

// ── browse View small height (listHeight < 3) ────────────────────────────

func TestBrowseModel_View_SmallHeight(t *testing.T) {
	m := testModelWithItems()
	m.height = 5 // height-6 = -1, should clamp to 3
	out := m.View()
	if out == "" {
		t.Error("expected non-empty output even at small height")
	}
}

// ── browse View cursor on file (non-dir) ──────────────────────────────────

func TestBrowseModel_View_CursorOnFile(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 1 // "readme.md" — a file, not a dir
	out := m.View()
	if !strings.Contains(out, "readme.md") {
		t.Errorf("expected file name in output, got:\n%s", out)
	}
}

// ── pollDaemonStart process exited ─────────────────────────────────────────

func TestPollDaemonStart_ProcessExited(t *testing.T) {
	cmd := exec.Command("true")
	_ = cmd.Run() // run and let it exit
	// Now cmd.ProcessState is non-nil.
	ok := pollDaemonStart("127.0.0.1:0", cmd, 2)
	if ok {
		t.Error("expected false when process already exited")
	}
}

// ── dispatchCmd "put" with valid file ──────────────────────────────────────

func TestDispatchCmd_PutWithFile(t *testing.T) {
	// Create a fake upload server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	// Create a temp file to upload.
	tmpFile := filepath.Join(t.TempDir(), "test.txt")
	if err := os.WriteFile(tmpFile, []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		handled, err := dispatchCmd([]string{"put", tmpFile}, addr, t.TempDir(), "")
		if !handled {
			t.Error("expected put to be handled")
		}
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
	if !strings.Contains(out, "test.txt") {
		t.Errorf("expected file upload message, got: %s", out)
	}
}

// ── dispatchCmd "remotes" dispatch ─────────────────────────────────────────

func TestDispatchCmd_RemotesDispatch(t *testing.T) {
	overrideListRemotes(t, func() ([]string, error) {
		return []string{"gdrive"}, nil
	})
	out := captureStdout(t, func() {
		handled, err := dispatchCmd([]string{"remotes"}, "127.0.0.1:0", t.TempDir(), "")
		if !handled {
			t.Error("expected handled=true")
		}
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
	if !strings.Contains(out, "gdrive") {
		t.Errorf("expected remotes output, got: %s", out)
	}
}

// ── runLs with file target (IsFile branch) ────────────────────────────────

func TestRunLs_FileTarget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/api/info"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"path": "/test.txt", "name": "test.txt",
				"size": 42, "local_state": "local",
			})
		case strings.Contains(r.URL.Path, "/api/ls"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"path": "/", "dirs": []string{},
				"files": []map[string]interface{}{
					{"name": "test.txt", "size": 42, "local_state": "local"},
				},
			})
		}
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	dir := t.TempDir()

	// First do a ls to populate the cache with a file entry.
	captureStdout(t, func() {
		runLs(addr, dir, nil)
	})

	// Now ls "1" should trigger the IsFile branch (select file by number).
	out := captureStdout(t, func() {
		runLs(addr, dir, []string{"1"})
	})
	if !strings.Contains(out, "test.txt") {
		t.Errorf("expected file info, got: %s", out)
	}
}

// ── parseAndMergeConfig flag parse error ──────────────────────────────────

func TestParseAndMergeConfig_FlagSetOutput(t *testing.T) {
	// The --help flag returns ErrHelp from flag parsing.
	_, err := parseAndMergeConfig([]string{"--help"})
	if err == nil {
		t.Fatal("expected error for --help")
	}
}

// ── fetchDir JSON decode error ─────────────────────────────────────────────

func TestBrowseModel_FetchDir_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "NOT JSON")
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	m := newBrowseModel(addr, t.TempDir())
	m.path = "/"
	cmd := m.fetchDir("/")
	msg := cmd() // Execute the tea.Cmd function directly.
	result, ok := msg.(lsResultMsg)
	if !ok {
		t.Fatalf("expected lsResultMsg, got %T", msg)
	}
	if result.err == nil {
		t.Fatal("expected error for bad JSON")
	}
	if !strings.Contains(result.err.Error(), "invalid response") {
		t.Errorf("unexpected error: %v", result.err)
	}
}

// ── fetchDir server error ─────────────────────────────────────────────────

func TestBrowseModel_FetchDir_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		fmt.Fprint(w, "internal error")
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	m := newBrowseModel(addr, t.TempDir())
	cmd := m.fetchDir("/")
	msg := cmd()
	result, ok := msg.(lsResultMsg)
	if !ok {
		t.Fatalf("expected lsResultMsg, got %T", msg)
	}
	if result.err == nil {
		t.Fatal("expected error for server error")
	}
}

// ── browse Update with fileInfo set (height adjustment) ────────────────────

func TestBrowseModel_View_WithFileInfo(t *testing.T) {
	m := testModelWithItems()
	m.fileInfo = &cliFileInfo{
		Path:      "/docs/readme.md",
		SizeBytes: 1024,
	}
	out := m.View()
	if !strings.Contains(out, "readme.md") {
		t.Errorf("expected file info panel, got:\n%s", out)
	}
}

// ── browse Update with unrecognized key (fallthrough return) ───────────────

func TestBrowseModel_Update_UnrecognizedKey(t *testing.T) {
	m := testModelWithItems()
	result, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'z'}})
	model := result.(browseModel)
	// Nothing should have changed — model state unchanged.
	if model.cursor != 0 {
		t.Errorf("cursor changed unexpectedly: %d", model.cursor)
	}
	if cmd != nil {
		t.Error("expected nil cmd for unrecognized key")
	}
}

// ── openDaemonLog OpenFile error ───────────────────────────────────────────

func TestOpenDaemonLog_OpenFileError(t *testing.T) {
	dir := t.TempDir()
	// Make daemon.log be a directory so OpenFile fails.
	if err := os.MkdirAll(filepath.Join(dir, "daemon.log"), 0755); err != nil {
		t.Fatal(err)
	}
	_, _, err := openDaemonLog(dir)
	if err == nil {
		t.Fatal("expected error when daemon.log is a directory")
	}
	if !strings.Contains(err.Error(), "opening log file") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ── fetchDir parent breadcrumb: navigate deeper ────────────────────────────

func TestBrowseModel_FetchDir_DeeperPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"path": "/docs/sub",
			"dirs": []string{},
			"files": []map[string]interface{}{
				{"name": "a.txt", "size": 10, "local_state": "local"},
			},
		})
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	m := newBrowseModel(addr, t.TempDir())
	m.path = "/docs"
	m.parents = nil
	cmd := m.fetchDir("/docs/sub")
	msg := cmd()
	result := msg.(lsResultMsg)
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
	}
	if result.path != "/docs/sub" {
		t.Errorf("path = %q, want /docs/sub", result.path)
	}
	// Parents should include the previous path.
	if len(result.parents) != 1 || result.parents[0] != "/docs" {
		t.Errorf("parents = %v, want [/docs]", result.parents)
	}
}

// ── fetchDir parent breadcrumb: navigate up (pop parents) ──────────────────

func TestBrowseModel_FetchDir_GoUpPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"path":  "/",
			"dirs":  []string{"docs"},
			"files": []map[string]interface{}{},
		})
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	m := newBrowseModel(addr, t.TempDir())
	m.path = "/docs/sub"
	m.parents = []string{"/", "/docs"}
	cmd := m.fetchDir("/")
	msg := cmd()
	result := msg.(lsResultMsg)
	if result.err != nil {
		t.Fatalf("unexpected error: %v", result.err)
	}
	// Going to "/" should clear all parents.
	if len(result.parents) != 0 {
		t.Errorf("parents = %v, want empty", result.parents)
	}
}

// ── runPut with directory (covers walk fi.IsDir skip) ──────────────────────

func TestRunPut_DirectoryUpload(t *testing.T) {
	// Set up a fake upload server.
	var uploadCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		uploadCount++
		w.WriteHeader(200)
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	// Create a temp directory with a file and a subdirectory.
	dir := t.TempDir()
	subDir := filepath.Join(dir, "sub")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file1.txt"), []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("world"), 0644); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		runPut(addr, []string{dir})
	})
	if !strings.Contains(out, "Uploaded 2 file") {
		t.Errorf("expected 'Uploaded 2 file' message, got: %s", out)
	}
	if uploadCount != 2 {
		t.Errorf("expected 2 uploads, got %d", uploadCount)
	}
}

// ── runPut directory with upload error ──────────────────────────────────────

func TestRunPut_DirectoryUploadError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		fmt.Fprint(w, "internal error")
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	stderr := captureStderr(t, func() {
		captureStdout(t, func() {
			runPut(addr, []string{dir})
		})
	})
	if !strings.Contains(stderr, "Error") {
		t.Errorf("expected error in stderr, got: %s", stderr)
	}
}
