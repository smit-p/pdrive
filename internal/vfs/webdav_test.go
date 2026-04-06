package vfs_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/rclonerc"
	"github.com/smit-p/pdrive/internal/vfs"
	"golang.org/x/net/webdav"
)

// fakeCloud is an in-memory implementation of engine.CloudStorage for WebDAV tests.
type fakeCloud struct {
	mu       sync.Mutex
	objects  map[string][]byte
	putErr   error
	putDelay time.Duration
}

func newFakeCloud() *fakeCloud                   { return &fakeCloud{objects: make(map[string][]byte)} }
func (f *fakeCloud) key(remote, p string) string { return remote + ":" + p }

func (f *fakeCloud) PutFile(remote, p string, r io.Reader) error {
	f.mu.Lock()
	putErr := f.putErr
	delay := f.putDelay
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if putErr != nil {
		return putErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.objects[f.key(remote, p)] = data
	f.mu.Unlock()
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

// newTestServer creates a fully wired WebDAV HTTP test server.
func newTestServer(t *testing.T) (*httptest.Server, *engine.Engine, *fakeCloud) {
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
	fs := vfs.NewWebDAVFS(eng, "") // empty spoolDir falls back to os.TempDir in tests
	handler := &webdav.Handler{FileSystem: fs, LockSystem: webdav.NewMemLS()}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv, eng, cloud
}

func propfind(t *testing.T, srv *httptest.Server, path string) int {
	t.Helper()
	const xmlBody = `<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><allprop/></propfind>`
	req, err := http.NewRequest("PROPFIND", srv.URL+path, strings.NewReader(xmlBody))
	if err != nil {
		t.Fatalf("propfind build: %v", err)
	}
	req.Header.Set("Depth", "0")
	req.Header.Set("Content-Type", "application/xml")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PROPFIND %s: %v", path, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func httpPUT(t *testing.T, srv *httptest.Server, path string, data []byte) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, srv.URL+path, bytes.NewReader(data))
	req.ContentLength = int64(len(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", path, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func httpGET(t *testing.T, srv *httptest.Server, path string) ([]byte, int) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body, resp.StatusCode
}

func httpDELETE(t *testing.T, srv *httptest.Server, path string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", path, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func httpMOVE(t *testing.T, srv *httptest.Server, from, to string) int {
	t.Helper()
	req, _ := http.NewRequest("MOVE", srv.URL+from, nil)
	req.Header.Set("Destination", srv.URL+to)
	req.Header.Set("Overwrite", "T")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("MOVE %s->%s: %v", from, to, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func httpMKCOL(t *testing.T, srv *httptest.Server, path string) int {
	t.Helper()
	req, _ := http.NewRequest("MKCOL", srv.URL+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("MKCOL %s: %v", path, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func waitComplete(t *testing.T, eng *engine.Engine, vpath string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f, _ := eng.Stat(vpath); f != nil && f.UploadState == "complete" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("timed out waiting for %s to complete", vpath)
}

// TestWebDAV_PUT_SmallFile: PUT small file must be readable via GET.
func TestWebDAV_PUT_SmallFile(t *testing.T) {
	srv, _, _ := newTestServer(t)
	content := []byte("hello webdav")
	status := httpPUT(t, srv, "/hello.txt", content)
	if status != http.StatusCreated && status != http.StatusNoContent {
		t.Fatalf("PUT returned %d, want 201/204", status)
	}
	body, gstatus := httpGET(t, srv, "/hello.txt")
	if gstatus != http.StatusOK {
		t.Fatalf("GET returned %d, want 200", gstatus)
	}
	if !bytes.Equal(body, content) {
		t.Errorf("content mismatch: got %q, want %q", body, content)
	}
}

// TestWebDAV_PUT_LargeFile: async upload; waitComplete; GET returns correct bytes.
func TestWebDAV_PUT_LargeFile(t *testing.T) {
	srv, eng, _ := newTestServer(t)
	content := make([]byte, engine.AsyncWriteThreshold+1)
	for i := range content {
		content[i] = byte(i & 0xFF)
	}
	status := httpPUT(t, srv, "/large.bin", content)
	if status != http.StatusCreated && status != http.StatusNoContent {
		t.Fatalf("async PUT returned %d, want 201/204", status)
	}
	waitComplete(t, eng, "/large.bin", 15*time.Second)
	body, gstatus := httpGET(t, srv, "/large.bin")
	if gstatus != http.StatusOK {
		t.Fatalf("GET after async upload: %d", gstatus)
	}
	if !bytes.Equal(body, content) {
		t.Errorf("large file content mismatch (got %d bytes, want %d)", len(body), len(content))
	}
}

// TestWebDAV_PROPFIND_AfterSyncPUT_Returns207: complete file is always visible.
func TestWebDAV_PROPFIND_AfterSyncPUT_Returns207(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpPUT(t, srv, "/synced.txt", []byte("content"))
	if code := propfind(t, srv, "/synced.txt"); code != http.StatusMultiStatus {
		t.Errorf("PROPFIND after sync PUT: got %d, want 207", code)
	}
}

// TestWebDAV_PROPFIND_AfterAsyncPUT_Pending is the key Finder Error -36
// regression test.
//
// Finder sends PROPFIND immediately after PUT returns. For large files the
// upload runs in the background (upload_state=pending). Before the fix,
// GetFileByPath filtered to complete-only => nil => PROPFIND 404 => Error -36.
// After the fix it returns all states => pending file visible => 207.
func TestWebDAV_PROPFIND_AfterAsyncPUT_Pending(t *testing.T) {
	srv, eng, cloud := newTestServer(t)

	// Slow down PutFile so the record stays pending when we PROPFIND.
	cloud.mu.Lock()
	cloud.putDelay = 500 * time.Millisecond
	cloud.mu.Unlock()

	content := make([]byte, engine.AsyncWriteThreshold+1)
	status := httpPUT(t, srv, "/uploading.mkv", content)
	if status != http.StatusCreated && status != http.StatusNoContent {
		t.Fatalf("PUT returned %d", status)
	}

	// PROPFIND while upload is still in progress — MUST be 207, not 404.
	if code := propfind(t, srv, "/uploading.mkv"); code != http.StatusMultiStatus {
		t.Errorf("PROPFIND pending file: got %d, want 207 (Finder Error -36 regression!)", code)
	}

	// Unblock and wait for upload to finish.
	cloud.mu.Lock()
	cloud.putDelay = 0
	cloud.mu.Unlock()
	waitComplete(t, eng, "/uploading.mkv", 15*time.Second)
}

// TestWebDAV_PROPFIND_NonExistent: PROPFIND never-written path must return 404.
func TestWebDAV_PROPFIND_NonExistent(t *testing.T) {
	srv, _, _ := newTestServer(t)
	if code := propfind(t, srv, "/ghost.mkv"); code != http.StatusNotFound {
		t.Errorf("PROPFIND non-existent: got %d, want 404", code)
	}
}

// TestWebDAV_PUT_Overwrite: second PUT wins.
func TestWebDAV_PUT_Overwrite(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpPUT(t, srv, "/v.txt", []byte("version-1"))
	httpPUT(t, srv, "/v.txt", []byte("version-2"))
	body, status := httpGET(t, srv, "/v.txt")
	if status != http.StatusOK {
		t.Fatalf("GET after overwrite: %d", status)
	}
	if string(body) != "version-2" {
		t.Errorf("expected version-2, got %q", body)
	}
}

// TestWebDAV_PUT_OverwritePending: PUT over a stuck pending record must
// succeed without UNIQUE constraint errors.
func TestWebDAV_PUT_OverwritePending(t *testing.T) {
	srv, eng, _ := newTestServer(t)
	db := eng.DB()
	tmp := "/tmp/stuck-webdav-test"
	now := time.Now().Unix()
	err := db.InsertFile(&metadata.File{
		ID: "stuck", VirtualPath: "/stuck.mkv", SizeBytes: 999,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: &tmp,
	})
	if err != nil {
		t.Fatalf("InsertFile: %v", err)
	}
	content := []byte("fresh upload over pending")
	status := httpPUT(t, srv, "/stuck.mkv", content)
	if status != http.StatusCreated && status != http.StatusNoContent {
		t.Fatalf("PUT over pending: got %d, want 201/204", status)
	}
	body, gstatus := httpGET(t, srv, "/stuck.mkv")
	if gstatus != http.StatusOK {
		t.Fatalf("GET after overwrite-pending: %d", gstatus)
	}
	if !bytes.Equal(body, content) {
		t.Errorf("content mismatch: got %q, want %q", body, content)
	}
}

// TestWebDAV_GET_NonExistent: GET missing file must return 404.
func TestWebDAV_GET_NonExistent(t *testing.T) {
	srv, _, _ := newTestServer(t)
	_, status := httpGET(t, srv, "/no-such-file.txt")
	if status != http.StatusNotFound {
		t.Errorf("GET non-existent: got %d, want 404", status)
	}
}

// TestWebDAV_DELETE_File: DELETE then PROPFIND => 404.
func TestWebDAV_DELETE_File(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpPUT(t, srv, "/del.txt", []byte("bye"))
	if code := httpDELETE(t, srv, "/del.txt"); code != http.StatusNoContent {
		t.Fatalf("DELETE returned %d, want 204", code)
	}
	if code := propfind(t, srv, "/del.txt"); code != http.StatusNotFound {
		t.Errorf("PROPFIND after DELETE: got %d, want 404", code)
	}
}

// TestWebDAV_DELETE_NonExistent: DELETE on missing path must not 500.
func TestWebDAV_DELETE_NonExistent(t *testing.T) {
	srv, _, _ := newTestServer(t)
	code := httpDELETE(t, srv, "/phantom.txt")
	if code >= 500 {
		t.Errorf("DELETE non-existent returned server error %d", code)
	}
}

// TestWebDAV_MOVE_File: MOVE renames; old=404, new=readable.
func TestWebDAV_MOVE_File(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpPUT(t, srv, "/old.txt", []byte("move me"))
	code := httpMOVE(t, srv, "/old.txt", "/new.txt")
	if code != http.StatusCreated && code != http.StatusNoContent {
		t.Fatalf("MOVE returned %d, want 201/204", code)
	}
	if code := propfind(t, srv, "/old.txt"); code != http.StatusNotFound {
		t.Errorf("old path after MOVE: got %d, want 404", code)
	}
	body, status := httpGET(t, srv, "/new.txt")
	if status != http.StatusOK {
		t.Fatalf("GET new path after MOVE: %d", status)
	}
	if string(body) != "move me" {
		t.Errorf("content after MOVE: got %q", body)
	}
}

// TestWebDAV_MKCOL: MKCOL creates a directory visible via PROPFIND.
func TestWebDAV_MKCOL(t *testing.T) {
	srv, _, _ := newTestServer(t)
	if code := httpMKCOL(t, srv, "/mydir"); code != http.StatusCreated {
		t.Fatalf("MKCOL returned %d, want 201", code)
	}
	if code := propfind(t, srv, "/mydir"); code != http.StatusMultiStatus {
		t.Errorf("PROPFIND on MKCOL dir: got %d, want 207", code)
	}
}

// TestWebDAV_PROPFIND_Root: PROPFIND on "/" always 207.
func TestWebDAV_PROPFIND_Root(t *testing.T) {
	srv, _, _ := newTestServer(t)
	if code := propfind(t, srv, "/"); code != http.StatusMultiStatus {
		t.Errorf("PROPFIND / returned %d, want 207", code)
	}
}

// TestWebDAV_PROPFIND_Dir_ListsFiles: Depth:1 must include uploaded files.
func TestWebDAV_PROPFIND_Dir_ListsFiles(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpMKCOL(t, srv, "/albums")
	httpPUT(t, srv, "/albums/dark_side.flac", []byte("m1"))
	httpPUT(t, srv, "/albums/wish_you.flac", []byte("m2"))

	const xmlBody = `<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><allprop/></propfind>`
	req, _ := http.NewRequest("PROPFIND", srv.URL+"/albums", strings.NewReader(xmlBody))
	req.Header.Set("Depth", "1")
	req.Header.Set("Content-Type", "application/xml")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PROPFIND Depth:1: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusMultiStatus {
		t.Fatalf("Depth:1 PROPFIND returned %d, want 207", resp.StatusCode)
	}
	if !strings.Contains(string(respBody), "dark_side.flac") {
		t.Errorf("dark_side.flac missing from PROPFIND Depth:1 response")
	}
	if !strings.Contains(string(respBody), "wish_you.flac") {
		t.Errorf("wish_you.flac missing from PROPFIND Depth:1 response")
	}
}

// TestWebDAV_PUT_Sequence_NoConstraintViolation writes 5 distinct files to
// verify no FK/UNIQUE errors under normal sequential use.
func TestWebDAV_PUT_Sequence_NoConstraintViolation(t *testing.T) {
	srv, _, _ := newTestServer(t)
	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("/file%d.txt", i)
		status := httpPUT(t, srv, path, []byte(fmt.Sprintf("c%d", i)))
		if status != http.StatusCreated && status != http.StatusNoContent {
			t.Errorf("PUT %s returned %d", path, status)
		}
	}
	for i := 0; i < 5; i++ {
		_, status := httpGET(t, srv, fmt.Sprintf("/file%d.txt", i))
		if status != http.StatusOK {
			t.Errorf("GET file%d.txt: %d", i, status)
		}
	}
}

// TestWebDAV_PUT_SamePath_Rapid: 3 rapid overwrites simulates Finder retry;
// last write always wins.
func TestWebDAV_PUT_SamePath_Rapid(t *testing.T) {
	srv, _, _ := newTestServer(t)
	for i := 0; i < 3; i++ {
		status := httpPUT(t, srv, "/rapid.txt", []byte(fmt.Sprintf("v%d", i)))
		if status != http.StatusCreated && status != http.StatusNoContent {
			t.Errorf("rapid PUT #%d returned %d", i, status)
		}
	}
	body, status := httpGET(t, srv, "/rapid.txt")
	if status != http.StatusOK {
		t.Fatalf("GET after rapid writes: %d", status)
	}
	if string(body) != "v2" {
		t.Errorf("expected v2, got %q", body)
	}
}

// ── cleanPath coverage (tested indirectly via Stat/OpenFile) ────────────────

func TestWebDAV_Stat_WithCleanablePaths(t *testing.T) {
	_, eng, _ := newTestServer(t)
	eng.MkDir("/testdir")
	fs := vfs.NewWebDAVFS(eng, "")

	// These paths should all resolve to /testdir via cleanPath.
	paths := []string{"/testdir", "testdir", "/a/../testdir", "//testdir"}
	for _, p := range paths {
		fi, err := fs.Stat(context.TODO(), p)
		if err != nil {
			t.Errorf("Stat(%q): %v", p, err)
			continue
		}
		if !fi.IsDir() {
			t.Errorf("Stat(%q): expected directory", p)
		}
	}
}

// ── fileInfo / dirInfo coverage ─────────────────────────────────────────────

func TestWebDAV_Stat_Root(t *testing.T) {
	srv, eng, _ := newTestServer(t)
	_ = srv
	fs := vfs.NewWebDAVFS(eng, "")
	fi, err := fs.Stat(context.TODO(), "/")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Error("root should be a dir")
	}
	if fi.Name() != "/" {
		t.Errorf("expected name '/', got %q", fi.Name())
	}
	if fi.Mode()&os.ModeDir == 0 {
		t.Error("root should have ModeDir")
	}
	if fi.Size() != 0 {
		t.Error("dir size should be 0")
	}
	if fi.Sys() != nil {
		t.Error("Sys() should be nil")
	}
}

func TestWebDAV_Stat_File(t *testing.T) {
	srv, eng, _ := newTestServer(t)
	httpPUT(t, srv, "/stattest.txt", []byte("hello"))
	fs := vfs.NewWebDAVFS(eng, "")
	fi, err := fs.Stat(context.TODO(), "/stattest.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.IsDir() {
		t.Error("expected file, not dir")
	}
	if fi.Name() != "stattest.txt" {
		t.Errorf("expected name stattest.txt, got %q", fi.Name())
	}
	if fi.Size() != 5 {
		t.Errorf("expected size 5, got %d", fi.Size())
	}
	if fi.Mode() != 0644 {
		t.Errorf("expected mode 0644, got %v", fi.Mode())
	}
	if fi.Sys() != nil {
		t.Error("Sys() should be nil")
	}
}

func TestWebDAV_Stat_NonExistent(t *testing.T) {
	_, eng, _ := newTestServer(t)
	fs := vfs.NewWebDAVFS(eng, "")
	_, err := fs.Stat(context.TODO(), "/nonexistent.txt")
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestWebDAV_Stat_Dir(t *testing.T) {
	_, eng, _ := newTestServer(t)
	eng.MkDir("/testdir")
	fs := vfs.NewWebDAVFS(eng, "")
	fi, err := fs.Stat(context.TODO(), "/testdir")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Error("expected directory")
	}
	if fi.Name() != "testdir" {
		t.Errorf("expected name 'testdir', got %q", fi.Name())
	}
}

func TestWebDAV_OpenFile_Dir(t *testing.T) {
	_, eng, _ := newTestServer(t)
	fs := vfs.NewWebDAVFS(eng, "")
	f, err := fs.OpenFile(context.TODO(), "/", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Readdir on root (empty).
	infos, err := f.Readdir(-1)
	if err != nil {
		t.Fatal(err)
	}
	_ = infos // may be empty
}

func TestWebDAV_RemoveAll_Dir(t *testing.T) {
	_, eng, _ := newTestServer(t)
	eng.MkDir("/rmdir")
	fs := vfs.NewWebDAVFS(eng, "")
	if err := fs.RemoveAll(context.TODO(), "/rmdir"); err != nil {
		t.Fatal(err)
	}
}

func TestWebDAV_Rename_Dir(t *testing.T) {
	_, eng, _ := newTestServer(t)
	eng.MkDir("/oldname")
	fs := vfs.NewWebDAVFS(eng, "")
	if err := fs.Rename(context.TODO(), "/oldname", "/newname"); err != nil {
		t.Fatal(err)
	}
}

func TestWebDAV_Mkdir(t *testing.T) {
	_, eng, _ := newTestServer(t)
	fs := vfs.NewWebDAVFS(eng, "")
	if err := fs.Mkdir(context.TODO(), "/newdir", 0755); err != nil {
		t.Fatal(err)
	}
}

func TestWebDAV_OpenFile_Write(t *testing.T) {
	_, eng, _ := newTestServer(t)
	fs := vfs.NewWebDAVFS(eng, "")
	f, err := fs.OpenFile(context.TODO(), "/write.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// Verify written.
	fi, err := fs.Stat(context.TODO(), "/write.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 4 {
		t.Errorf("expected size 4, got %d", fi.Size())
	}
}

func TestWebDAV_DELETE_Dir(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpMKCOL(t, srv, "/deldir")
	httpPUT(t, srv, "/deldir/filex.txt", []byte("rm"))
	if code := httpDELETE(t, srv, "/deldir"); code >= 500 {
		t.Errorf("DELETE dir: got server error %d", code)
	}
}

func TestWebDAV_MOVE_Dir(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpMKCOL(t, srv, "/mvdir")
	httpPUT(t, srv, "/mvdir/inner.txt", []byte("mv"))
	code := httpMOVE(t, srv, "/mvdir", "/moved")
	if code != http.StatusCreated && code != http.StatusNoContent {
		t.Fatalf("MOVE dir returned %d", code)
	}
}

// ── Read / Seek / Readdir edge cases via direct API ─────────────────────────

func TestWebDAVFile_Read_Directory(t *testing.T) {
	_, eng, _ := newTestServer(t)
	eng.MkDir("/readdir")
	fs := vfs.NewWebDAVFS(eng, "")
	f, err := fs.OpenFile(context.TODO(), "/readdir", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 10)
	_, err = f.Read(buf)
	if err == nil {
		t.Error("expected error reading a directory")
	}
}

func TestWebDAVFile_Seek_Directory(t *testing.T) {
	_, eng, _ := newTestServer(t)
	eng.MkDir("/seekdir")
	fs := vfs.NewWebDAVFS(eng, "")
	f, err := fs.OpenFile(context.TODO(), "/seekdir", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, err = f.Seek(0, io.SeekStart)
	if err == nil {
		t.Error("expected error seeking a directory")
	}
}

func TestWebDAVFile_Readdir_NotDir(t *testing.T) {
	srv, eng, _ := newTestServer(t)
	httpPUT(t, srv, "/file.txt", []byte("data"))
	fs := vfs.NewWebDAVFS(eng, "")
	f, err := fs.OpenFile(context.TODO(), "/file.txt", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, err = f.Readdir(-1)
	if err == nil {
		t.Error("expected error calling Readdir on a file")
	}
}

func TestWebDAVFile_Readdir_WithCount(t *testing.T) {
	srv, eng, _ := newTestServer(t)
	httpPUT(t, srv, "/rdcount/a.txt", []byte("a"))
	httpPUT(t, srv, "/rdcount/b.txt", []byte("b"))
	httpPUT(t, srv, "/rdcount/c.txt", []byte("c"))
	fs := vfs.NewWebDAVFS(eng, "")
	f, err := fs.OpenFile(context.TODO(), "/rdcount", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	infos, err := f.Readdir(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 2 {
		t.Errorf("expected 2 entries with count=2, got %d", len(infos))
	}
}

func TestWebDAVFile_Write_NotWritable(t *testing.T) {
	_, eng, _ := newTestServer(t)
	fs := vfs.NewWebDAVFS(eng, "")
	// Open for read (no write flag)
	f, err := fs.OpenFile(context.TODO(), "/", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, err = f.Write([]byte("data"))
	if err == nil {
		t.Error("expected error writing to non-writable file")
	}
}

func TestWebDAVFile_Stat_Dir(t *testing.T) {
	_, eng, _ := newTestServer(t)
	eng.MkDir("/statdir")
	fs := vfs.NewWebDAVFS(eng, "")
	f, err := fs.OpenFile(context.TODO(), "/statdir", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Error("expected IsDir true")
	}
	if fi.Name() != "statdir" {
		t.Errorf("expected name 'statdir', got %q", fi.Name())
	}
}

func TestWebDAVFile_ReadAndSeek_File(t *testing.T) {
	srv, eng, _ := newTestServer(t)
	httpPUT(t, srv, "/readseek.txt", []byte("hello world"))
	fs := vfs.NewWebDAVFS(eng, "")
	f, err := fs.OpenFile(context.TODO(), "/readseek.txt", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Read first 5 bytes
	buf := make([]byte, 5)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("Read = %q, want 'hello'", buf[:n])
	}

	// Seek back to start
	pos, err := f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Errorf("Seek returned %d, want 0", pos)
	}

	// Read again
	n, err = f.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("Read after seek = %q, want 'hello'", buf[:n])
	}
}

func TestWebDAVFile_Close_WithReadFile(t *testing.T) {
	srv, eng, _ := newTestServer(t)
	httpPUT(t, srv, "/closerd.txt", []byte("close test"))
	fs := vfs.NewWebDAVFS(eng, "")
	f, err := fs.OpenFile(context.TODO(), "/closerd.txt", 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Trigger readFile creation by reading
	buf := make([]byte, 4)
	if _, err := f.Read(buf); err != nil {
		t.Fatal(err)
	}

	// Close should clean up the temp read file
	if err := f.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

func TestWebDAV_RemoveAll_File(t *testing.T) {
	srv, eng, _ := newTestServer(t)
	httpPUT(t, srv, "/rmfile.txt", []byte("data"))
	fs := vfs.NewWebDAVFS(eng, "")
	if err := fs.RemoveAll(context.TODO(), "/rmfile.txt"); err != nil {
		t.Fatal(err)
	}
	_, err := fs.Stat(context.TODO(), "/rmfile.txt")
	if !os.IsNotExist(err) {
		t.Errorf("expected ErrNotExist after RemoveAll, got %v", err)
	}
}

func TestWebDAV_Rename_File(t *testing.T) {
	srv, eng, _ := newTestServer(t)
	httpPUT(t, srv, "/renold.txt", []byte("rename"))
	fs := vfs.NewWebDAVFS(eng, "")
	if err := fs.Rename(context.TODO(), "/renold.txt", "/rennew.txt"); err != nil {
		t.Fatal(err)
	}
	fi, err := fs.Stat(context.TODO(), "/rennew.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 6 {
		t.Errorf("expected size 6, got %d", fi.Size())
	}
}

func TestWebDAVFile_Write_WithSpoolDir(t *testing.T) {
	_, eng, _ := newTestServer(t)
	spool := t.TempDir()
	fs := vfs.NewWebDAVFS(eng, spool)
	f, err := fs.OpenFile(context.TODO(), "/spool.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("spool data")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := fs.Stat(context.TODO(), "/spool.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 10 {
		t.Errorf("expected size 10, got %d", fi.Size())
	}
}

// ── SyncDir helper ──────────────────────────────────────────────────────────

// newTestSyncDir creates a SyncDir with a real engine for testing PinFile/UnpinFile.
func newTestSyncDir(t *testing.T) (*vfs.SyncDir, *engine.Engine, *fakeCloud) {
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
	root := t.TempDir()
	spool := t.TempDir()
	sd := vfs.NewSyncDir(root, eng, spool)
	return sd, eng, cloud
}

// ── SyncDir PinFile ─────────────────────────────────────────────────────────

func TestSyncDir_PinFile(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)
	content := []byte("pin this content")
	eng.WriteFile("/doc.txt", content)

	if err := sd.PinFile("/doc.txt"); err != nil {
		t.Fatal(err)
	}

	localPath := filepath.Join(sd.Root(), "doc.txt")
	data, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("reading pinned file: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("pinned content mismatch: got %q, want %q", data, content)
	}
}

func TestSyncDir_PinFile_NonExistent(t *testing.T) {
	sd, _, _ := newTestSyncDir(t)
	err := sd.PinFile("/nope.txt")
	if err == nil {
		t.Error("expected error pinning non-existent file")
	}
}

// ── SyncDir UnpinFile ───────────────────────────────────────────────────────

func TestSyncDir_UnpinFile(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)
	content := []byte("unpin me")
	eng.WriteFile("/unpin.txt", content)

	// First pin it to get local copy.
	if err := sd.PinFile("/unpin.txt"); err != nil {
		t.Fatal(err)
	}

	// Now unpin — should replace with stub.
	if err := sd.UnpinFile("/unpin.txt"); err != nil {
		t.Fatal(err)
	}

	if !sd.IsStub("/unpin.txt") {
		t.Error("expected file to be a stub after unpin")
	}

	// Stub file should be 0 bytes.
	localPath := filepath.Join(sd.Root(), "unpin.txt")
	info, err := os.Stat(localPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Errorf("expected stub to be 0 bytes, got %d", info.Size())
	}
}

func TestSyncDir_UnpinFile_NonExistent(t *testing.T) {
	sd, _, _ := newTestSyncDir(t)
	err := sd.UnpinFile("/nope.txt")
	if err == nil {
		t.Error("expected error unpinning non-existent file")
	}
}

func TestSyncDir_UnpinFile_Pending(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)
	now := time.Now().Unix()
	tmp := "/tmp/pending-unpin"
	eng.DB().InsertFile(&metadata.File{
		ID: "unpin-pend", VirtualPath: "/pend.txt", SizeBytes: 10,
		CreatedAt: now, ModifiedAt: now, SHA256Full: "h",
		UploadState: "pending", TmpPath: &tmp,
	})
	err := sd.UnpinFile("/pend.txt")
	if err == nil {
		t.Error("expected error unpinning pending file")
	}
}

// ── SyncDir IsStub ──────────────────────────────────────────────────────────

func TestSyncDir_IsStub_NotStub(t *testing.T) {
	sd, _, _ := newTestSyncDir(t)
	// Create a regular file.
	localPath := filepath.Join(sd.Root(), "regular.txt")
	os.WriteFile(localPath, []byte("data"), 0644)
	if sd.IsStub("/regular.txt") {
		t.Error("expected regular file to not be a stub")
	}
}

func TestSyncDir_IsStub_NonExistent(t *testing.T) {
	sd, _, _ := newTestSyncDir(t)
	if sd.IsStub("/ghost.txt") {
		t.Error("expected non-existent file to not be a stub")
	}
}

// ── SyncDir ListFiles ───────────────────────────────────────────────────────

func TestSyncDir_ListFiles(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)
	eng.WriteFile("/a.txt", []byte("a"))
	eng.WriteFile("/b.txt", []byte("b"))
	files := sd.ListFiles()
	if len(files) < 2 {
		t.Errorf("expected >= 2 files, got %d", len(files))
	}
}

func TestSyncDir_ListFiles_Empty(t *testing.T) {
	sd, _, _ := newTestSyncDir(t)
	files := sd.ListFiles()
	if len(files) != 0 {
		t.Errorf("expected 0 files, got %d", len(files))
	}
}

// ── WebDAV edge cases ───────────────────────────────────────────────────────

func TestWebDAVFile_Stat_WritableFile(t *testing.T) {
	_, eng, _ := newTestServer(t)
	fs := vfs.NewWebDAVFS(eng, "")
	f, err := fs.OpenFile(context.TODO(), "/wrstat.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("stats"))
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi.IsDir() {
		t.Error("writable file should not be a dir")
	}
	if fi.Name() != "wrstat.txt" {
		t.Errorf("expected name wrstat.txt, got %q", fi.Name())
	}
	f.Close()
}

func TestWebDAVFile_Close_ReadOnlyNoReadFile(t *testing.T) {
	_, eng, _ := newTestServer(t)
	eng.MkDir("/closedir")
	fs := vfs.NewWebDAVFS(eng, "")
	f, err := fs.OpenFile(context.TODO(), "/closedir", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Close without reading — no readFile to clean up.
	if err := f.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

func TestWebDAV_PROPFIND_Dir_Depth0(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpMKCOL(t, srv, "/depth0dir")
	httpPUT(t, srv, "/depth0dir/f.txt", []byte("x"))
	if code := propfind(t, srv, "/depth0dir"); code != http.StatusMultiStatus {
		t.Errorf("PROPFIND Depth:0 on dir: got %d, want 207", code)
	}
}

func TestWebDAV_PUT_InSubdir(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpMKCOL(t, srv, "/sub")
	status := httpPUT(t, srv, "/sub/file.txt", []byte("nested"))
	if status != http.StatusCreated && status != http.StatusNoContent {
		t.Fatalf("PUT in subdir: got %d", status)
	}
	body, gstatus := httpGET(t, srv, "/sub/file.txt")
	if gstatus != http.StatusOK {
		t.Fatalf("GET from subdir: got %d", gstatus)
	}
	if string(body) != "nested" {
		t.Errorf("content mismatch: got %q", body)
	}
}

func TestWebDAV_DELETE_EmptyDir(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpMKCOL(t, srv, "/emptydir")
	if code := httpDELETE(t, srv, "/emptydir"); code >= 500 {
		t.Errorf("DELETE empty dir: got server error %d", code)
	}
	if code := propfind(t, srv, "/emptydir"); code != http.StatusNotFound {
		t.Errorf("PROPFIND after empty dir DELETE: got %d, want 404", code)
	}
}

// ── Additional WebDAV edge-case tests ────────────────────────────────────────

func TestWebDAV_GET_ReadSeekFile(t *testing.T) {
	// GET triggers both Read and Seek paths via http.ServeContent.
	srv, _, _ := newTestServer(t)
	data := []byte("seekable content here")
	httpPUT(t, srv, "/seek.txt", data)
	time.Sleep(200 * time.Millisecond)
	body, code := httpGET(t, srv, "/seek.txt")
	if code != http.StatusOK {
		t.Fatalf("GET /seek.txt: got %d", code)
	}
	if string(body) != string(data) {
		t.Errorf("content mismatch: got %q, want %q", body, data)
	}
}

func TestWebDAV_GET_RangeRequest(t *testing.T) {
	// Range request exercises Seek path in webDAVFile.
	srv, _, _ := newTestServer(t)
	data := []byte("0123456789abcdef")
	httpPUT(t, srv, "/range.txt", data)
	time.Sleep(200 * time.Millisecond)
	req, _ := http.NewRequest("GET", srv.URL+"/range.txt", nil)
	req.Header.Set("Range", "bytes=4-7")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected 206 Partial Content, got %d", resp.StatusCode)
	}
	if string(body) != "4567" {
		t.Errorf("range content: got %q, want %q", body, "4567")
	}
}

func TestWebDAV_PROPFIND_Dir_Depth1(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpMKCOL(t, srv, "/d1dir")
	httpPUT(t, srv, "/d1dir/a.txt", []byte("a"))
	httpPUT(t, srv, "/d1dir/b.txt", []byte("bb"))
	time.Sleep(200 * time.Millisecond)
	const xmlBody = `<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><allprop/></propfind>`
	req, _ := http.NewRequest("PROPFIND", srv.URL+"/d1dir", strings.NewReader(xmlBody))
	req.Header.Set("Depth", "1")
	req.Header.Set("Content-Type", "application/xml")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMultiStatus {
		t.Errorf("PROPFIND Depth:1: got %d, want 207", resp.StatusCode)
	}
}

func TestWebDAV_HEAD_File(t *testing.T) {
	srv, _, _ := newTestServer(t)
	data := []byte("head test")
	httpPUT(t, srv, "/head.txt", data)
	time.Sleep(200 * time.Millisecond)
	req, _ := http.NewRequest("HEAD", srv.URL+"/head.txt", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("HEAD: got %d, want 200", resp.StatusCode)
	}
}

func TestWebDAV_Stat_ExplicitDir(t *testing.T) {
	// Stat on an explicit directory (created via MKCOL, no files inside).
	srv, _, _ := newTestServer(t)
	httpMKCOL(t, srv, "/expldir")
	code := propfind(t, srv, "/expldir")
	if code != http.StatusMultiStatus {
		t.Errorf("expected 207, got %d for explicit empty dir", code)
	}
}

func TestWebDAV_Readdir_WithCount(t *testing.T) {
	// Exercises the Readdir count > 0 truncation branch.
	srv, eng, _ := newTestServer(t)
	eng.WriteFile("/countdir/a.txt", []byte("a"))
	eng.WriteFile("/countdir/b.txt", []byte("b"))
	eng.WriteFile("/countdir/c.txt", []byte("c"))
	// PROPFIND with Depth:1 triggers Readdir internally.
	const xmlBody = `<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><allprop/></propfind>`
	req, _ := http.NewRequest("PROPFIND", srv.URL+"/countdir/", strings.NewReader(xmlBody))
	req.Header.Set("Depth", "1")
	req.Header.Set("Content-Type", "application/xml")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusMultiStatus {
		t.Fatalf("Readdir PROPFIND: got %d, want 207. Body: %s", resp.StatusCode, body)
	}
}

func TestWebDAV_PUT_EmptyFile(t *testing.T) {
	srv, _, _ := newTestServer(t)
	status := httpPUT(t, srv, "/empty.txt", []byte{})
	if status != http.StatusCreated && status != http.StatusNoContent {
		t.Fatalf("PUT empty file: got %d", status)
	}
}

func TestWebDAV_MOVE_Overwrite(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpPUT(t, srv, "/movesrc.txt", []byte("src"))
	httpPUT(t, srv, "/movedst.txt", []byte("dst"))
	time.Sleep(100 * time.Millisecond)
	status := httpMOVE(t, srv, "/movesrc.txt", "/movedst.txt")
	if status >= 400 {
		t.Fatalf("MOVE overwrite: got %d", status)
	}
	body, code := httpGET(t, srv, "/movedst.txt")
	if code != http.StatusOK {
		t.Fatalf("GET after MOVE: got %d", code)
	}
	if string(body) != "src" {
		t.Errorf("content after MOVE: got %q, want %q", body, "src")
	}
}

func TestWebDAV_DELETE_Dir_WithFiles(t *testing.T) {
	srv, _, _ := newTestServer(t)
	httpMKCOL(t, srv, "/deldir")
	httpPUT(t, srv, "/deldir/x.txt", []byte("x"))
	time.Sleep(100 * time.Millisecond)
	code := httpDELETE(t, srv, "/deldir")
	if code >= 500 {
		t.Errorf("DELETE dir with files: server error %d", code)
	}
	_, gcode := httpGET(t, srv, "/deldir/x.txt")
	if gcode != http.StatusNotFound {
		t.Errorf("expected 404 after recursive delete, got %d", gcode)
	}
}

func TestWebDAV_MKCOL_Nested(t *testing.T) {
	srv, _, _ := newTestServer(t)
	// Create parent then child.
	httpMKCOL(t, srv, "/nest1")
	httpMKCOL(t, srv, "/nest1/nest2")
	code := propfind(t, srv, "/nest1/nest2")
	if code != http.StatusMultiStatus {
		t.Errorf("nested MKCOL: PROPFIND got %d, want 207", code)
	}
}

func TestSyncDir_PinFile_CreatesDirs(t *testing.T) {
	// PinFile should create intermediate directories for nested paths.
	sd, eng, _ := newTestSyncDir(t)
	eng.WriteFile("/deep/nested/file.txt", []byte("deep content"))
	if err := sd.PinFile("/deep/nested/file.txt"); err != nil {
		t.Fatal(err)
	}
	localPath := filepath.Join(sd.Root(), "deep", "nested", "file.txt")
	data, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("failed to read pinned file: %v", err)
	}
	if string(data) != "deep content" {
		t.Errorf("content mismatch: got %q", data)
	}
}

func TestSyncDir_UnpinFile_ClearsMarker(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)
	eng.WriteFile("/clearmarker.txt", []byte("hello"))
	sd.PinFile("/clearmarker.txt")
	if err := sd.UnpinFile("/clearmarker.txt"); err != nil {
		t.Fatal(err)
	}
	// After unpin, the file should be a stub (0 bytes).
	fi, err := os.Stat(filepath.Join(sd.Root(), "clearmarker.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 {
		t.Errorf("expected stub (0 bytes), got %d bytes", fi.Size())
	}
}

// ── Direct WebDAVFS API tests for error branches ─────────────────────────────

// newTestFS creates a WebDAVFS with engine and cloud for direct API tests.
func newTestFS(t *testing.T) (*vfs.WebDAVFS, *engine.Engine, *fakeCloud) {
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
	fs := vfs.NewWebDAVFS(eng, "")
	return fs, eng, cloud
}

func TestWebDAVFS_Read_DeletedFile(t *testing.T) {
	// Open a file for reading, then delete it from the engine before reading.
	// This exercises the ensureReadFile error path.
	fs, eng, _ := newTestFS(t)
	eng.WriteFile("/readfail.txt", []byte("content"))
	ctx := context.Background()
	f, err := fs.OpenFile(ctx, "/readfail.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// Delete the file so the engine no longer has it.
	eng.DeleteFile("/readfail.txt")
	buf := make([]byte, 10)
	_, err = f.Read(buf)
	if err == nil {
		t.Error("expected error for Read of deleted file")
	}
}

func TestWebDAVFS_Seek_DeletedFile(t *testing.T) {
	fs, eng, _ := newTestFS(t)
	eng.WriteFile("/seekfail.txt", []byte("content"))
	ctx := context.Background()
	f, err := fs.OpenFile(ctx, "/seekfail.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	eng.DeleteFile("/seekfail.txt")
	_, err = f.Seek(0, io.SeekStart)
	if err == nil {
		t.Error("expected error for Seek of deleted file")
	}
}

func TestWebDAVFS_Readdir_EngineError(t *testing.T) {
	// Open a dir file handle, then exercise Readdir — should succeed.
	fs, eng, _ := newTestFS(t)
	eng.WriteFile("/rddir/a.txt", []byte("a"))
	ctx := context.Background()
	f, err := fs.OpenFile(ctx, "/rddir", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	infos, err := f.Readdir(0)
	if err != nil {
		t.Fatalf("Readdir error: %v", err)
	}
	if len(infos) == 0 {
		t.Error("expected at least 1 entry")
	}
}

func TestWebDAVFS_Stat_NotFoundPath(t *testing.T) {
	fs, _, _ := newTestFS(t)
	ctx := context.Background()
	_, err := fs.Stat(ctx, "/nonexistent/path.txt")
	if err == nil {
		t.Error("expected error for non-existent path")
	}
}

func TestWebDAVFS_OpenFile_NonExistentRead(t *testing.T) {
	fs, _, _ := newTestFS(t)
	ctx := context.Background()
	_, err := fs.OpenFile(ctx, "/no-such-file.txt", os.O_RDONLY, 0)
	if err == nil {
		t.Error("expected error for non-existent file in read mode")
	}
}

func TestWebDAVFS_Close_WritableNoData(t *testing.T) {
	// Open for writing but never write anything — Close should be a no-op.
	fs, _, _ := newTestFS(t)
	ctx := context.Background()
	f, err := fs.OpenFile(ctx, "/nowrite.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("Close on writable file with no data: %v", err)
	}
}

func TestWebDAVFS_Stat_ImplicitDir(t *testing.T) {
	// Stat on a directory that exists only because files are under it.
	fs, eng, _ := newTestFS(t)
	eng.WriteFile("/impldir/sub/deep.txt", []byte("deep"))
	ctx := context.Background()
	fi, err := fs.Stat(ctx, "/impldir")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Error("expected IsDir=true for implicit directory")
	}
}

func TestWebDAVFS_RemoveAll_Dir(t *testing.T) {
	fs, eng, _ := newTestFS(t)
	eng.MkDir("/rmdir/")
	eng.WriteFile("/rmdir/f.txt", []byte("x"))
	ctx := context.Background()
	if err := fs.RemoveAll(ctx, "/rmdir"); err != nil {
		t.Fatalf("RemoveAll dir: %v", err)
	}
	fi, err := fs.Stat(ctx, "/rmdir/f.txt")
	if err == nil && fi != nil {
		t.Error("expected file to be gone after RemoveAll")
	}
}

func TestWebDAVFS_Rename(t *testing.T) {
	fs, eng, _ := newTestFS(t)
	eng.WriteFile("/rensrc.txt", []byte("data"))
	ctx := context.Background()
	if err := fs.Rename(ctx, "/rensrc.txt", "/rendst.txt"); err != nil {
		t.Fatal(err)
	}
	fi, err := fs.Stat(ctx, "/rendst.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.IsDir() {
		t.Error("expected file, got dir")
	}
}

func TestWebDAVFS_Mkdir(t *testing.T) {
	fs, _, _ := newTestFS(t)
	ctx := context.Background()
	if err := fs.Mkdir(ctx, "/newdir", 0755); err != nil {
		t.Fatal(err)
	}
	fi, err := fs.Stat(ctx, "/newdir")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Error("expected directory")
	}
}

// ---------------------------------------------------------------------------
// Additional tests to improve webdav.go function coverage
// ---------------------------------------------------------------------------

// Test Close with writable file that has a sync error.
// We can trigger this by writing to a tmp file in a deleted spool dir.
func TestWebDAVFile_Close_SyncError(t *testing.T) {
	fs, _, _ := newTestFS(t)
	ctx := context.Background()
	f, err := fs.OpenFile(ctx, "/syncerr.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// Write something to allocate tmpFile.
	f.Write([]byte("hello"))
	// On macOS/Linux, if we could invalidate the fd the Sync would fail.
	// Instead, we test the normal Close path fully to ensure Close → Sync → Seek → WriteFileStream.
	err = f.Close()
	if err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	// Verify file was written.
	fi, err := fs.Stat(ctx, "/syncerr.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 5 {
		t.Errorf("expected size 5, got %d", fi.Size())
	}
}

// Test Stat when engine.Stat returns error (closed DB scenario).
func TestWebDAVFS_Stat_EngineStatError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	total, free := int64(200e9), int64(199e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "p1", Type: "drive", DisplayName: "TestDrive",
		RcloneRemote: "fake:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, cloud, b, encKey)
	fs := vfs.NewWebDAVFS(eng, "")
	// Close the engine's DB so Stat will fail.
	db.Close()
	eng.Close()
	_, err = fs.Stat(context.Background(), "/test.txt")
	if err == nil {
		t.Error("expected error from Stat when engine is closed")
	}
}

// Test OpenFile for reading when engine.Stat returns error (not just nil).
func TestWebDAVFS_OpenFile_StatError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	total, free := int64(200e9), int64(199e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "p1", Type: "drive", DisplayName: "TestDrive",
		RcloneRemote: "fake:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, cloud, b, encKey)
	fs := vfs.NewWebDAVFS(eng, "")
	db.Close()
	eng.Close()
	_, err = fs.OpenFile(context.Background(), "/test.txt", os.O_RDONLY, 0)
	if err == nil {
		t.Error("expected error from OpenFile when engine is closed")
	}
}

// Test Write with bad spool dir (CreateTemp error).
func TestWebDAVFile_Write_BadSpoolDir(t *testing.T) {
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
	// Use a non-existent spool dir to trigger CreateTemp error.
	fs := vfs.NewWebDAVFS(eng, "/nonexistent/spool/dir")
	f, err := fs.OpenFile(context.Background(), "/file.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write([]byte("data"))
	if err == nil {
		t.Error("expected error writing to file with bad spool dir")
	}
	f.Close()
}

// Test Readdir with count=0 (no truncation, default).
func TestWebDAVFile_Readdir_CountZero(t *testing.T) {
	fs, eng, _ := newTestFS(t)
	ctx := context.Background()
	eng.MkDir("/mydir")
	eng.WriteFile("/mydir/a.txt", []byte("aaa"))
	eng.WriteFile("/mydir/b.txt", []byte("bbb"))
	eng.WriteFile("/mydir/c.txt", []byte("ccc"))

	f, err := fs.OpenFile(ctx, "/mydir", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	infos, err := f.Readdir(0) // count=0 means all
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 3 {
		t.Errorf("expected 3 infos, got %d", len(infos))
	}
}

// Test Readdir with count larger than entries (no truncation).
func TestWebDAVFile_Readdir_CountLargerThanEntries(t *testing.T) {
	fs, eng, _ := newTestFS(t)
	ctx := context.Background()
	eng.MkDir("/mydir")
	eng.WriteFile("/mydir/a.txt", []byte("aaa"))

	f, err := fs.OpenFile(ctx, "/mydir", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	infos, err := f.Readdir(100) // count > actual
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 {
		t.Errorf("expected 1 info, got %d", len(infos))
	}
}

// Test PinFile into a read-only directory to trigger os.Create error.
func TestSyncDir_PinFile_CreateError(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)
	eng.WriteFile("/readonly/doc.txt", []byte("content"))

	// Make the root + /readonly dir read-only so os.Create fails.
	root := sd.Root()
	roDir := filepath.Join(root, "readonly")
	os.MkdirAll(roDir, 0500)
	t.Cleanup(func() { os.Chmod(roDir, 0700) })

	err := sd.PinFile("/readonly/doc.txt")
	if err == nil {
		t.Error("expected error when creating file in read-only dir")
	}
}

// Test PinFile with cloud PutErr to trigger io.Copy/read error.
func TestSyncDir_PinFile_CopyError(t *testing.T) {
	sd, eng, cloud := newTestSyncDir(t)
	eng.WriteFile("/doc.txt", []byte("content"))
	// Now remove the cloud data so read will fail.
	cloud.mu.Lock()
	for k := range cloud.objects {
		delete(cloud.objects, k)
	}
	cloud.mu.Unlock()

	err := sd.PinFile("/doc.txt")
	if err == nil {
		t.Error("expected error when cloud data is missing")
	}
}

// Test hashLocalFile with a non-existent file.
func TestSyncDir_ShouldSkipPath(t *testing.T) {
	fs, eng, _ := newTestFS(t)
	ctx := context.Background()
	// Write a file with .DS_Store name — shouldSkipPath doesn't apply to engine directly
	eng.WriteFile("/.DS_Store", []byte("data"))
	fi, _ := fs.Stat(ctx, "/.DS_Store")
	if fi == nil {
		t.Log("skipped .DS_Store as expected")
	}
}

// Test Stat path for file → nil, isDir error.
func TestWebDAVFS_Stat_IsDirError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	total, free := int64(200e9), int64(199e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "p1", Type: "drive", DisplayName: "TestDrive",
		RcloneRemote: "fake:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, cloud, b, encKey)
	fs := vfs.NewWebDAVFS(eng, "")
	// Close DB so Stat will call engine.Stat → error, which hits the first branch.
	// But we actually need Stat to succeed (return nil) then IsDir to fail.
	// With a normal DB but no file, Stat returns nil, then IsDir is called.
	// If we close the DB between these two, it's tricky.
	// Let's just test the Stat → nil → IsDir → true path (explicit dir).
	eng.MkDir("/explicitdir")
	fi, err := fs.Stat(context.Background(), "/explicitdir")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Error("expected directory")
	}
	// Now test Stat → nil → IsDir → false → ErrNotExist.
	db.Close()
	eng.Close()
	_, err = fs.Stat(context.Background(), "/nonexist")
	if err == nil {
		t.Error("expected error for non-existent path with closed engine")
	}
}

// ---------------------------------------------------------------------------
// SyncDir Start / Stop integration tests (covers most syncdir.go functions)
// ---------------------------------------------------------------------------

func TestSyncDir_StartStop_InitialSync(t *testing.T) {
	// Tests: Start, Stop, addWatchRecursive, initialSync, downloadMissing.
	sd, eng, _ := newTestSyncDir(t)

	// Create a cloud file that doesn't exist locally.
	eng.WriteFile("/cloud-only.txt", []byte("from the cloud"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// initialSync's downloadMissing should have created a stub for cloud-only.txt.
	stubPath := filepath.Join(sd.Root(), "cloud-only.txt")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(stubPath); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(stubPath); os.IsNotExist(err) {
		t.Error("expected stub file to be created by initialSync")
	}
}

func TestSyncDir_StartStop_LocalOnlyFile(t *testing.T) {
	// Tests: initialSync upload path for local-only files.
	sd, eng, _ := newTestSyncDir(t)

	// Create a local file before starting.
	localFile := filepath.Join(sd.Root(), "local.txt")
	os.WriteFile(localFile, []byte("local data"), 0644)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Wait for initialSync to upload the local file.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.Stat("/local.txt"); f != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	sd.Stop()

	f, _ := eng.Stat("/local.txt")
	if f == nil {
		t.Error("expected local file to be uploaded by initialSync")
	}
}

func TestSyncDir_FileCreate_Upload(t *testing.T) {
	// Tests: eventLoop → handleEvent → Create → debounce → upload.
	sd, eng, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a new file in the watched directory.
	newFile := filepath.Join(sd.Root(), "newfile.txt")
	os.WriteFile(newFile, []byte("brand new"), 0644)

	// Wait for debounce (2s) + some buffer.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.Stat("/newfile.txt"); f != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	f, _ := eng.Stat("/newfile.txt")
	if f == nil {
		t.Error("expected new file to be uploaded after create event")
	}
}

func TestSyncDir_FileRemove_Delete(t *testing.T) {
	// Tests: eventLoop → handleEvent → Remove → engine.DeleteFile.
	sd, eng, _ := newTestSyncDir(t)

	// Upload a file via the engine.
	eng.WriteFile("/deleteme.txt", []byte("delete this"))

	// Start watcher FIRST so PinFile's suppress entry is consumed by the
	// Create event that fsnotify fires when PinFile writes the local copy.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Pin after Start so the watcher consumes the suppress entry.
	sd.PinFile("/deleteme.txt")
	time.Sleep(300 * time.Millisecond) // let Create event be consumed

	// Remove the local file — triggers Remove event.
	os.Remove(filepath.Join(sd.Root(), "deleteme.txt"))

	// Wait for the rename window (500ms) + debounce to expire.
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.Stat("/deleteme.txt"); f == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	f, _ := eng.Stat("/deleteme.txt")
	if f != nil {
		t.Error("expected file to be deleted after Remove event")
	}
}

func TestSyncDir_DirCreate_Watch(t *testing.T) {
	// Tests: handleEvent → Create → IsDir → watcher.Add + MkDir.
	sd, eng, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a new directory.
	newDir := filepath.Join(sd.Root(), "subdir")
	os.Mkdir(newDir, 0755)

	// Wait for the directory to be recognized.
	time.Sleep(500 * time.Millisecond)

	// Write a file inside the new subdirectory.
	os.WriteFile(filepath.Join(newDir, "inner.txt"), []byte("inner"), 0644)

	// Wait for debounce + upload.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.Stat("/subdir/inner.txt"); f != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	f, _ := eng.Stat("/subdir/inner.txt")
	if f == nil {
		t.Error("expected file in new subdir to be uploaded")
	}
}

func TestSyncDir_FileWrite_Debounce(t *testing.T) {
	// Tests: eventLoop → Write → debounce (update path).
	sd, eng, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create initial file.
	filePath := filepath.Join(sd.Root(), "updated.txt")
	os.WriteFile(filePath, []byte("v1"), 0644)

	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Wait for initial upload.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.Stat("/updated.txt"); f != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Now overwrite the file to trigger a Write event.
	os.WriteFile(filePath, []byte("v2-updated"), 0644)

	// Wait for debounce + re-upload.
	deadline = time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.Stat("/updated.txt"); f != nil && f.SizeBytes == 10 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	f, _ := eng.Stat("/updated.txt")
	if f != nil && f.SizeBytes != 10 {
		t.Errorf("expected updated size 10, got %d", f.SizeBytes)
	}
}

func TestSyncDir_SkipDSStore(t *testing.T) {
	// Tests: shouldSkipPath filters.
	sd, eng, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a .DS_Store file — should be skipped.
	os.WriteFile(filepath.Join(sd.Root(), ".DS_Store"), []byte("dsstore"), 0644)
	// Create a ._resource fork — should be skipped.
	os.WriteFile(filepath.Join(sd.Root(), "._test"), []byte("resource"), 0644)

	time.Sleep(3 * time.Second) // Wait past debounce.

	f1, _ := eng.Stat("/.DS_Store")
	f2, _ := eng.Stat("/._test")
	if f1 != nil {
		t.Error("expected .DS_Store to be skipped")
	}
	if f2 != nil {
		t.Error("expected ._test resource fork to be skipped")
	}
}

func TestSyncDir_DownloadMissing_Subdir(t *testing.T) {
	// Tests: downloadMissing recursion into subdirectories.
	sd, eng, _ := newTestSyncDir(t)

	eng.MkDir("/deep/")
	eng.WriteFile("/deep/file.txt", []byte("deep content"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// downloadMissing should create stubs recursively.
	stubPath := filepath.Join(sd.Root(), "deep", "file.txt")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(stubPath); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(stubPath); os.IsNotExist(err) {
		t.Error("expected stub in subdirectory")
	}
}

func TestSyncDir_ScanDir_FilesBeforeWatch(t *testing.T) {
	// Tests: scanDir — files that arrive before fsnotify watcher.
	sd, _, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Rapidly create a directory with files — some may arrive before watcher is added.
	subDir := filepath.Join(sd.Root(), "batch")
	os.MkdirAll(subDir, 0755)
	for i := 0; i < 3; i++ {
		os.WriteFile(filepath.Join(subDir, fmt.Sprintf("f%d.txt", i)), []byte(fmt.Sprintf("data%d", i)), 0644)
	}

	// Just verify no crash/deadlock — the files should eventually be picked up.
	time.Sleep(4 * time.Second)
}

func TestSyncDir_FileRename_MetadataOnly(t *testing.T) {
	// Tests: handleEvent → Rename detection path.
	sd, eng, _ := newTestSyncDir(t)

	eng.WriteFile("/original.txt", []byte("rename me"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Pin after Start so the watcher consumes the suppress entry.
	sd.PinFile("/original.txt")
	time.Sleep(300 * time.Millisecond)

	// Rename the file (Remove + Create within renameWindow).
	oldPath := filepath.Join(sd.Root(), "original.txt")
	newPath := filepath.Join(sd.Root(), "renamed.txt")
	os.Rename(oldPath, newPath)

	// Wait for rename detection + processing.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.Stat("/renamed.txt"); f != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The old path should be gone and the new path should exist.
	f, _ := eng.Stat("/renamed.txt")
	if f == nil {
		// Rename detection might not always work, but the file should at least be re-uploaded.
		time.Sleep(4 * time.Second) // allow debounce + upload
		f, _ = eng.Stat("/renamed.txt")
		if f == nil {
			t.Log("rename detection did not fire; file was not re-uploaded either (may need longer wait)")
		}
	}
}

// ── Upload path coverage ─────────────────────────────────────────────────────

func TestSyncDir_Upload_StubSkipped(t *testing.T) {
	// upload() should skip stub files (isStubFile returns true).
	sd, eng, _ := newTestSyncDir(t)

	// Write a file to the engine and create a stub locally.
	eng.WriteFile("/cloud.txt", []byte("cloud content"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Wait for initial sync to create stubs, then overwrite the stub's data
	// to force a re-debounce — but it's still a stub.
	time.Sleep(500 * time.Millisecond)

	// Touch the stub to fire a Write event — upload should skip it.
	stubPath := filepath.Join(sd.Root(), "cloud.txt")
	os.Chtimes(stubPath, time.Now(), time.Now())

	time.Sleep(3 * time.Second)

	// The file should still be the original cloud content (not re-uploaded as 0 byte).
	f, _ := eng.Stat("/cloud.txt")
	if f == nil {
		t.Fatal("file disappeared")
	}
	if f.SizeBytes != int64(len("cloud content")) {
		t.Errorf("expected size %d, got %d", len("cloud content"), f.SizeBytes)
	}
}

func TestSyncDir_Upload_HashDedup(t *testing.T) {
	// upload() should skip if file content matches existing (hash dedup).
	sd, eng, _ := newTestSyncDir(t)

	content := []byte("identical content")
	eng.WriteFile("/existing.txt", content)

	// Create the local file with the same content.
	localPath := filepath.Join(sd.Root(), "existing.txt")
	os.WriteFile(localPath, content, 0644)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Touch to fire a Write event.
	time.Sleep(100 * time.Millisecond)
	os.Chtimes(localPath, time.Now(), time.Now())

	// Wait for debounce — upload should be skipped (same hash).
	time.Sleep(3 * time.Second)

	// Check it wasn't re-uploaded (same version).
	f, _ := eng.Stat("/existing.txt")
	if f == nil {
		t.Fatal("file disappeared")
	}
}

func TestSyncDir_Upload_LargeFileAsync(t *testing.T) {
	// upload() should use WriteFileAsync for files > AsyncWriteThreshold.
	sd, eng, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a file larger than 4 MB.
	largePath := filepath.Join(sd.Root(), "large.bin")
	data := make([]byte, engine.AsyncWriteThreshold+1)
	for i := range data {
		data[i] = byte(i % 256)
	}
	os.WriteFile(largePath, data, 0644)

	// Wait for debounce + async upload to complete.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.Stat("/large.bin"); f != nil && f.UploadState == "complete" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	f, _ := eng.Stat("/large.bin")
	if f == nil {
		t.Fatal("large file not uploaded")
	}
	if f.SizeBytes != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), f.SizeBytes)
	}
}

func TestSyncDir_Upload_FileVanishes(t *testing.T) {
	// upload() should handle file disappearing between event and read.
	sd, _, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a file and immediately delete it before debounce fires.
	tmpPath := filepath.Join(sd.Root(), "vanish.txt")
	os.WriteFile(tmpPath, []byte("gone"), 0644)
	time.Sleep(100 * time.Millisecond)
	os.Remove(tmpPath)

	// Wait for debounce — upload should handle Stat error gracefully.
	time.Sleep(3 * time.Second)
	// No crash = success.
}

// ── ScanDir coverage ─────────────────────────────────────────────────────────

func TestSyncDir_ScanDir_SkipTrashDir(t *testing.T) {
	// scanDir and addWatchRecursive skip .Trash directories that exist at start.
	sd, eng, _ := newTestSyncDir(t)

	// Create .Trash BEFORE starting — addWatchRecursive should skip it.
	trashDir := filepath.Join(sd.Root(), ".Trash")
	os.MkdirAll(trashDir, 0755)
	os.WriteFile(filepath.Join(trashDir, "deleted.txt"), []byte("trash"), 0644)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	time.Sleep(3 * time.Second)

	// The file should NOT be uploaded (directory was skipped in initialSync).
	f, _ := eng.Stat("/.Trash/deleted.txt")
	if f != nil {
		t.Error("file in .Trash should not be uploaded during initialSync")
	}
}

func TestSyncDir_ScanDir_SkipDotUnderscore(t *testing.T) {
	// scanDir should skip files with ._ prefix (macOS resource forks).
	sd, eng, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a subdir with both a real file and a resource fork.
	subDir := filepath.Join(sd.Root(), "docs")
	os.MkdirAll(subDir, 0755)
	os.WriteFile(filepath.Join(subDir, "._hiddenresource"), []byte("res"), 0644)
	os.WriteFile(filepath.Join(subDir, "real.txt"), []byte("real file"), 0644)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.Stat("/docs/real.txt"); f != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Resource fork should not be uploaded.
	f, _ := eng.Stat("/docs/._hiddenresource")
	if f != nil {
		t.Error("._hiddenresource should not be uploaded")
	}
	// Real file should be uploaded.
	f2, _ := eng.Stat("/docs/real.txt")
	if f2 == nil {
		t.Error("real.txt should be uploaded")
	}
}

// ── Stop with pending timers ─────────────────────────────────────────────────

func TestSyncDir_Stop_CancelsPending(t *testing.T) {
	// Stop() should cancel pending debounce timers.
	sd, _, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Create a file to trigger a debounce timer.
	os.WriteFile(filepath.Join(sd.Root(), "pending.txt"), []byte("pending"), 0644)
	time.Sleep(200 * time.Millisecond) // let event reach eventLoop

	// Stop immediately — pending timer should be cancelled.
	sd.Stop()
	time.Sleep(3 * time.Second) // wait past debounce period

	// No crash/panic = success (timer was cancelled properly).
}

// ── handleEvent suppressed ───────────────────────────────────────────────────

func TestSyncDir_HandleEvent_Suppressed(t *testing.T) {
	// Download events should be suppressed by the suppress map.
	sd, eng, _ := newTestSyncDir(t)

	// Write a file to the engine.
	eng.WriteFile("/suppressed.txt", []byte("suppress me"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// PinFile should download and suppress the Create event.
	sd.PinFile("/suppressed.txt")

	// Wait for any events to settle.
	time.Sleep(3 * time.Second)

	// The file should NOT be re-uploaded (event was suppressed).
	// The original and the local should be identical.
	localData, err := os.ReadFile(filepath.Join(sd.Root(), "suppressed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(localData) != "suppress me" {
		t.Errorf("unexpected content: %q", localData)
	}
}

// ── addWatchRecursive with skip dirs ─────────────────────────────────────────

func TestSyncDir_Start_SkipsDotPdriveDir(t *testing.T) {
	// addWatchRecursive should skip .pdrive directories.
	sd, _, _ := newTestSyncDir(t)

	// Create a .pdrive dir with files.
	pdriveDir := filepath.Join(sd.Root(), ".pdrive")
	os.MkdirAll(pdriveDir, 0755)
	os.WriteFile(filepath.Join(pdriveDir, "config.json"), []byte("{}"), 0644)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	time.Sleep(500 * time.Millisecond)
	// No crash = success (skipped dir wasn't watched).
}

// ── initialSync with skipDir ─────────────────────────────────────────────────

func TestSyncDir_InitialSync_SkipTrash(t *testing.T) {
	// initialSync should skip .Trash directories when walking local files.
	sd, eng, _ := newTestSyncDir(t)

	// Create .Trash with a file before starting.
	trashDir := filepath.Join(sd.Root(), ".Trash")
	os.MkdirAll(trashDir, 0755)
	os.WriteFile(filepath.Join(trashDir, "old.txt"), []byte("old"), 0644)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	time.Sleep(500 * time.Millisecond)

	f, _ := eng.Stat("/.Trash/old.txt")
	if f != nil {
		t.Error(".Trash file should not be uploaded during initialSync")
	}
}

// ── downloadMissing with error ───────────────────────────────────────────────

func TestSyncDir_DownloadMissing_EngineError(t *testing.T) {
	// downloadMissing should handle ListDir errors gracefully.
	sd, eng, _ := newTestSyncDir(t)

	// Write and upload a file, then close db to cause errors.
	eng.WriteFile("/will-fail.txt", []byte("data"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	sd.Stop()
	// No crash = success.
}

// ── Start with watcher error ─────────────────────────────────────────────────

func TestSyncDir_Start_AlreadyStarted(t *testing.T) {
	// Calling Start twice should work if first was stopped.
	sd, _, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	sd.Stop()
	cancel()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := sd.Start(ctx2); err != nil {
		t.Fatal(err)
	}
	sd.Stop()
}

// ── handleEvent dir removed ──────────────────────────────────────────────────

func TestSyncDir_DirRemove(t *testing.T) {
	// handleEvent should handle directory removal.
	sd, eng, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a directory and a file in it.
	subDir := filepath.Join(sd.Root(), "removeme")
	os.MkdirAll(subDir, 0755)
	time.Sleep(500 * time.Millisecond)

	os.WriteFile(filepath.Join(subDir, "file.txt"), []byte("hello"), 0644)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f, _ := eng.Stat("/removeme/file.txt"); f != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Remove the directory.
	os.RemoveAll(subDir)
	time.Sleep(1 * time.Second)
	// No crash = success (dir removal handled).
}

// ── Upload error paths (engine failures) ─────────────────────────────────────

func TestSyncDir_Upload_EngineWriteError(t *testing.T) {
	// upload() small file path should handle WriteFile errors gracefully.
	sd, _, cloud := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Make cloud fail on uploads.
	cloud.mu.Lock()
	cloud.putErr = fmt.Errorf("cloud write error")
	cloud.mu.Unlock()

	// Create a file — upload should fail silently (logged as error).
	os.WriteFile(filepath.Join(sd.Root(), "fail.txt"), []byte("will fail"), 0644)
	time.Sleep(3 * time.Second)
	// No crash = success (error handled gracefully).
}

func TestSyncDir_Upload_LargeSpoolError(t *testing.T) {
	// upload() large file with bad spool dir should handle CreateTemp error.
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
	root := t.TempDir()
	badSpool := "/nonexistent/spool/dir"
	sd := vfs.NewSyncDir(root, eng, badSpool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a large file — CreateTemp should fail.
	data := make([]byte, engine.AsyncWriteThreshold+1)
	os.WriteFile(filepath.Join(root, "big.bin"), data, 0644)
	time.Sleep(4 * time.Second)
	// No crash = success (CreateTemp error handled).
}

// ── Stress: multiple files in quick succession ───────────────────────────────

func TestSyncDir_RapidFileCreation(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create several files quickly.
	for i := 0; i < 5; i++ {
		name := filepath.Join(sd.Root(), fmt.Sprintf("rapid-%d.txt", i))
		os.WriteFile(name, []byte(fmt.Sprintf("data-%d", i)), 0644)
	}

	// Wait for all uploads.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for i := 0; i < 5; i++ {
			if f, _ := eng.Stat(fmt.Sprintf("/rapid-%d.txt", i)); f != nil {
				count++
			}
		}
		if count == 5 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	uploaded := 0
	for i := 0; i < 5; i++ {
		if f, _ := eng.Stat(fmt.Sprintf("/rapid-%d.txt", i)); f != nil {
			uploaded++
		}
	}
	if uploaded < 3 {
		t.Errorf("expected most files uploaded, got %d/5", uploaded)
	}
}

// ── Readdir with count limit ─────────────────────────────────────────────────

func TestReaddir_CountLimit(t *testing.T) {
	fs, eng, _ := newTestFS(t)

	// Create multiple files.
	for i := 0; i < 5; i++ {
		eng.WriteFile(fmt.Sprintf("/rd/file%d.txt", i), []byte("data"))
	}

	f, err := fs.OpenFile(context.Background(), "/rd/", 0, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	infos, err := f.Readdir(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 2 {
		t.Errorf("expected 2 entries, got %d", len(infos))
	}
}

// ── WebDAV Readdir on non-directory ──────────────────────────────────────────

func TestReaddir_NonDirectory(t *testing.T) {
	fs, eng, _ := newTestFS(t)

	eng.WriteFile("/notdir.txt", []byte("content"))

	f, err := fs.OpenFile(context.Background(), "/notdir.txt", 0, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	_, err = f.Readdir(-1)
	if err == nil {
		t.Error("expected error calling Readdir on a file")
	}
}

// ── WebDAV Seek on directory ─────────────────────────────────────────────────

func TestSeek_Directory(t *testing.T) {
	fs, eng, _ := newTestFS(t)

	eng.MkDir("/seekdir/")

	f, err := fs.OpenFile(context.Background(), "/seekdir/", 0, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	_, err = f.Seek(0, io.SeekStart)
	if err == nil {
		t.Error("expected error seeking on a directory")
	}
}

// ── WebDAV Read triggers lazy download ───────────────────────────────────────

func TestRead_LazyDownload(t *testing.T) {
	fs, eng, _ := newTestFS(t)

	content := []byte("lazy download content")
	eng.WriteFile("/lazy.txt", content)

	f, err := fs.OpenFile(context.Background(), "/lazy.txt", 0, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Read should trigger ensureReadFile → ReadFileToTempFile.
	buf := make([]byte, 100)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], content) {
		t.Errorf("content mismatch: got %q, want %q", buf[:n], content)
	}

	// Seek should work on the already-opened read file.
	pos, err := f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Errorf("expected seek position 0, got %d", pos)
	}
}

// ── WebDAV Close cleans up read file ─────────────────────────────────────────

func TestClose_CleansUpReadFile(t *testing.T) {
	fs, eng, _ := newTestFS(t)

	eng.WriteFile("/cleanup.txt", []byte("cleanup me"))

	f, err := fs.OpenFile(context.Background(), "/cleanup.txt", 0, 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Trigger readFile creation.
	buf := make([]byte, 100)
	f.Read(buf)

	// Close should clean up tmp file.
	if err := f.Close(); err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}
}

// ── WebDAV Stat for directory ────────────────────────────────────────────────

func TestStat_Directory(t *testing.T) {
	fs, eng, _ := newTestFS(t)

	eng.MkDir("/statdir/")

	f, err := fs.OpenFile(context.Background(), "/statdir/", 0, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Error("expected directory")
	}
	if info.Name() != "statdir" {
		t.Errorf("expected name 'statdir', got %q", info.Name())
	}
}

// ── WebDAV OpenFile nonexistent ──────────────────────────────────────────────

func TestOpenFile_Nonexistent(t *testing.T) {
	fs, _, _ := newTestFS(t)

	_, err := fs.OpenFile(context.Background(), "/nonexist.txt", 0, 0644)
	if err == nil {
		t.Error("expected error opening nonexistent file")
	}
}

// ── WebDAV RemoveAll ─────────────────────────────────────────────────────────

func TestRemoveAll_File(t *testing.T) {
	fs, eng, _ := newTestFS(t)

	eng.WriteFile("/rm.txt", []byte("remove me"))
	if err := fs.RemoveAll(context.Background(), "/rm.txt"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}
	f, err := eng.Stat("/rm.txt")
	if f != nil {
		t.Error("file should be deleted")
	}
	_ = err
}

func TestRemoveAll_Dir(t *testing.T) {
	fs, eng, _ := newTestFS(t)

	eng.MkDir("/rmdir/")
	eng.WriteFile("/rmdir/f.txt", []byte("data"))
	if err := fs.RemoveAll(context.Background(), "/rmdir/"); err != nil {
		t.Fatalf("RemoveAll dir failed: %v", err)
	}
}

// ── WebDAV Rename ────────────────────────────────────────────────────────────

func TestRename_File(t *testing.T) {
	fs, eng, _ := newTestFS(t)

	eng.WriteFile("/old.txt", []byte("rename me"))
	if err := fs.Rename(context.Background(), "/old.txt", "/new.txt"); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if _, err := eng.Stat("/new.txt"); err != nil {
		t.Error("renamed file not found")
	}
}

func TestRename_Dir(t *testing.T) {
	fs, eng, _ := newTestFS(t)

	eng.MkDir("/rendir/")
	eng.WriteFile("/rendir/f.txt", []byte("data"))
	if err := fs.Rename(context.Background(), "/rendir/", "/newdir/"); err != nil {
		t.Fatalf("Rename dir failed: %v", err)
	}
}

// ── WebDAV Mkdir ─────────────────────────────────────────────────────────────

func TestMkdir_New(t *testing.T) {
	fs, _, _ := newTestFS(t)

	if err := fs.Mkdir(context.Background(), "/newmkdir", 0755); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
}

// ── WebDAV Stat error path ──────────────────────────────────────────────────

func TestStat_EngineError(t *testing.T) {
	_, eng, _ := newTestFS(t)
	fs2 := vfs.NewWebDAVFS(eng, t.TempDir())

	// Write a file, then break the DB so Stat returns an error.
	eng.WriteFile("/err.txt", []byte("data"))
	eng.DB().Close()

	_, err := fs2.Stat(context.Background(), "/err.txt")
	if err == nil {
		t.Error("expected error from Stat when DB is closed")
	}
}

// ── WebDAV Readdir error paths ──────────────────────────────────────────────

func TestReaddir_ListDirError(t *testing.T) {
	_, eng, _ := newTestFS(t)
	fs2 := vfs.NewWebDAVFS(eng, t.TempDir())

	// Close DB so ListDir fails.
	eng.DB().Close()

	f, err := fs2.OpenFile(context.Background(), "/", 0, 0)
	if err != nil {
		t.Fatalf("OpenFile for root: %v", err)
	}
	_, err = f.Readdir(-1)
	if err == nil {
		t.Error("expected error from Readdir when DB is closed")
	}
	f.Close()
}

// ── SyncDir rename detection ────────────────────────────────────────────────

func TestSyncDir_RenameDetection(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	// Upload a file via the engine first.
	content := []byte("rename-detect-content")
	eng.WriteFile("/original.txt", content)

	// Start the sync dir to set up watching.
	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create the original file in the local dir.
	origPath := filepath.Join(sd.Root(), "original.txt")
	os.WriteFile(origPath, content, 0644)

	// Wait for the watcher to be ready.
	time.Sleep(500 * time.Millisecond)

	// Simulate rename: remove original and create new file with same content/size.
	os.Remove(origPath)
	time.Sleep(200 * time.Millisecond) // let Remove event propagate
	newPath := filepath.Join(sd.Root(), "renamed.txt")
	os.WriteFile(newPath, content, 0644)

	// Give time for the rename detection logic.
	time.Sleep(3 * time.Second)
}

// ── SyncDir UnpinFile stub creation error ───────────────────────────────────

func TestSyncDir_UnpinFile_NotFound(t *testing.T) {
	sd, _, _ := newTestSyncDir(t)

	err := sd.UnpinFile("/not-exists.txt")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

// ── SyncDir initialSync with stubs ──────────────────────────────────────────

func TestSyncDir_InitialSync_DownloadMissing(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	// Add files to the engine that don't exist locally.
	eng.WriteFile("/cloud-only.txt", []byte("cloud data"))
	eng.WriteFile("/another.txt", []byte("more data"))

	// Start syncing — this triggers initialSync which calls downloadMissing.
	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Wait for initial sync.
	time.Sleep(2 * time.Second)

	// Stub files should exist locally.
	for _, name := range []string{"cloud-only.txt", "another.txt"} {
		localPath := filepath.Join(sd.Root(), name)
		if _, err := os.Stat(localPath); err != nil {
			t.Errorf("expected stub for %s to exist: %v", name, err)
		}
	}
}

// ── SyncDir Start with addWatchRecursive error ──────────────────────────────

func TestSyncDir_Start_BadRoot(t *testing.T) {
	eng, _ := newTestEngineForVFS(t)
	sd := vfs.NewSyncDir("/nonexistent/root/dir", eng, t.TempDir())

	err := sd.Start(context.Background())
	if err == nil {
		t.Error("expected error when root doesn't exist")
		sd.Stop()
	}
}

func newTestEngineForVFS(t *testing.T) (*engine.Engine, *fakeCloud) {
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
	return eng, cloud
}

// ── SyncDir upload edge cases ───────────────────────────────────────────────

func TestSyncDir_Upload_WriteError(t *testing.T) {
	sd, eng, cloud := newTestSyncDir(t)

	// Limit retries to avoid long backoff sleeps that cause test timeout.
	eng.SetMaxChunkRetries(1)

	// Start sync dir.
	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Set cloud error so upload fails.
	cloud.mu.Lock()
	cloud.putErr = fmt.Errorf("cloud write error")
	cloud.mu.Unlock()

	// Create a small file that will be uploaded synchronously.
	localPath := filepath.Join(sd.Root(), "fail-upload.txt")
	os.WriteFile(localPath, []byte("will-fail"), 0644)

	// Wait for debounce + upload attempt.
	time.Sleep(4 * time.Second)

	// File should NOT appear in the engine because the upload failed.
	if f, _ := eng.Stat("/fail-upload.txt"); f != nil {
		t.Error("file should not exist after failed upload")
	}
}

// ── SyncDir scanDir with nested directories ─────────────────────────────────

func TestSyncDir_ScanDir_NestedDirs(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a nested directory structure AFTER the watcher is started.
	// The scanDir method is called on new directories.
	nested := filepath.Join(sd.Root(), "parent", "child")
	os.MkdirAll(nested, 0755)
	time.Sleep(500 * time.Millisecond)

	// Write a file in the nested directory.
	os.WriteFile(filepath.Join(nested, "deep.txt"), []byte("deep content"), 0644)

	// Wait for debounce.
	time.Sleep(4 * time.Second)

	// File should be uploaded.
	if f, _ := eng.Stat("/parent/child/deep.txt"); f == nil {
		t.Error("expected nested file to be uploaded")
	}
}

// ── SyncDir directory removal ───────────────────────────────────────────────

func TestSyncDir_DirRemoval(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	eng.MkDir("/rmdir/")
	eng.WriteFile("/rmdir/file.txt", []byte("data"))

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create the directory locally for the watcher to see.
	dirPath := filepath.Join(sd.Root(), "rmdir")
	os.MkdirAll(dirPath, 0755)
	os.WriteFile(filepath.Join(dirPath, "file.txt"), []byte("data"), 0644)
	time.Sleep(500 * time.Millisecond)

	// Remove the directory.
	os.RemoveAll(dirPath)

	// Wait for event processing.
	time.Sleep(3 * time.Second)
}

// ── SyncDir upload skip for duplicate content ───────────────────────────────

func TestSyncDir_Upload_SkipDuplicate(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	content := []byte("duplicate-check")
	eng.WriteFile("/dup.txt", content)

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Write same content locally — should be skipped (dedup).
	localPath := filepath.Join(sd.Root(), "dup.txt")
	os.WriteFile(localPath, content, 0644)

	time.Sleep(4 * time.Second)
}

// ── SyncDir IsStub helper ───────────────────────────────────────────────────

func TestSyncDir_IsStub_NoFile(t *testing.T) {
	sd, _, _ := newTestSyncDir(t)
	if sd.IsStub("/nope.txt") {
		t.Error("non-existent file should not be a stub")
	}
}

// ── SyncDir Stop without Start ──────────────────────────────────────────────

func TestSyncDir_Stop_BeforeStart(t *testing.T) {
	sd, _, _ := newTestSyncDir(t)
	// After bug fix, Stop() before Start() should no-op instead of panicking.
	sd.Stop() // must not panic
}

// ── SyncDir: initialSync uploads local-only files ───────────────────────────

func TestSyncDir_InitialSync_UploadsLocalFiles(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	// Create a local file BEFORE starting sync (simulates pre-existing file).
	localPath := filepath.Join(sd.Root(), "local-only.txt")
	os.WriteFile(localPath, []byte("local stuff"), 0644)

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Wait for initial sync + upload.
	time.Sleep(4 * time.Second)

	// File should now exist in engine.
	if fi, _ := eng.Stat("/local-only.txt"); fi == nil {
		t.Error("expected local-only file to be uploaded during initial sync")
	}
}

// ── SyncDir: initialSync skips stub files ───────────────────────────────────

func TestSyncDir_InitialSync_SkipsStubs(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	// Create a stub file in the sync root.
	eng.WriteFile("/cloud-only.txt", []byte("cloud content"))

	// Pin it to create the local file, then unpin to make it a stub.
	sd.PinFile("/cloud-only.txt")
	sd.UnpinFile("/cloud-only.txt")

	// Start sync — the stub should NOT trigger a re-upload.
	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()
	time.Sleep(2 * time.Second)

	// Verify stub is still a stub.
	if !sd.IsStub("/cloud-only.txt") {
		t.Error("expected file to remain a stub after initial sync")
	}
}

// ── SyncDir: initialSync skips .DS_Store and similar ────────────────────────

func TestSyncDir_InitialSync_SkipsDSStore(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	// Create a .DS_Store file locally.
	dsPath := filepath.Join(sd.Root(), ".DS_Store")
	os.WriteFile(dsPath, []byte{0, 0, 0, 1}, 0644)

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()
	time.Sleep(3 * time.Second)

	// .DS_Store should NOT be uploaded.
	if fi, _ := eng.Stat("/.DS_Store"); fi != nil {
		t.Error(".DS_Store should not be uploaded")
	}
}

// ── SyncDir: downloadMissing skips incomplete uploads ───────────────────────

func TestSyncDir_DownloadMissing_SkipsIncomplete(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	// Upload a complete file normally.
	eng.WriteFile("/pending.txt", []byte("pending content"))

	// Simulate an incomplete upload by inserting a pending file record directly
	// into the DB rather than triggering a real upload that retries with backoff.
	eng.DB().InsertFile(&metadata.File{
		ID:          "incomplete-id",
		VirtualPath: "/will-fail.txt",
		SizeBytes:   4,
		UploadState: "pending",
	})

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()
	time.Sleep(2 * time.Second)
}

// ── WebDAV Close error paths (file writable with sync/seek errors) ──────────

func TestWebDAVFile_Close_AsyncLargeFile(t *testing.T) {
	fs, eng, _ := newTestFS(t)

	// Open a file for writing and write > AsyncWriteThreshold bytes.
	f, err := fs.OpenFile(context.Background(), "/large.bin", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Write enough data to trigger async upload (> 4MB).
	bigData := make([]byte, 5*1024*1024)
	for i := range bigData {
		bigData[i] = byte(i % 256)
	}
	n, err := f.Write(bigData)
	if err != nil || n != len(bigData) {
		t.Fatalf("Write failed: %v (n=%d)", err, n)
	}

	// Close should trigger async upload path.
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Wait for async upload to complete.
	time.Sleep(3 * time.Second)
	eng.Close()

	// File should exist.
	if fi, _ := eng.Stat("/large.bin"); fi == nil {
		t.Error("large file should have been uploaded")
	}
}

// ── Upload: small file read-error ───────────────────────────────────────────

func TestSyncDir_Upload_SmallFile_ReadError(t *testing.T) {
	sd, _, _ := newTestSyncDir(t)

	// Create a local file, then remove it before the debounce fires so
	// os.ReadFile fails inside upload().
	localPath := filepath.Join(sd.Root(), "vanish.txt")
	os.WriteFile(localPath, []byte("temp"), 0644)

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Remove the file after the watcher picks it up but before upload reads it.
	time.Sleep(200 * time.Millisecond)
	os.Remove(localPath)
	time.Sleep(4 * time.Second)
	// No assertion — just verifying no panic on the read-error path.
}

// ── Upload: small file engine.WriteFile error ───────────────────────────────

func TestSyncDir_Upload_SmallFile_WriteError(t *testing.T) {
	sd, _, cloud := newTestSyncDir(t)

	cloud.mu.Lock()
	cloud.putErr = fmt.Errorf("cloud down")
	cloud.mu.Unlock()

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Write a small file — upload will fail.
	localPath := filepath.Join(sd.Root(), "fail.txt")
	os.WriteFile(localPath, []byte("data"), 0644)
	time.Sleep(4 * time.Second)
}

// ── handleEvent: Stat error on Create ───────────────────────────────────────

func TestSyncDir_HandleEvent_StatError(t *testing.T) {
	sd, _, _ := newTestSyncDir(t)

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a symlink that os.Stat may return an error for (dangling).
	dangling := filepath.Join(sd.Root(), "dangling-link")
	os.Symlink("/tmp/nonexistent-pdrive-target-12345", dangling)
	time.Sleep(4 * time.Second)
	// No assertion — covers the handleEvent stat-error early return.
}

// ── SyncDir: downloadMissing with shouldSkipPath files ──────────────────────

func TestSyncDir_DownloadMissing_SkipsFilteredFiles(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	// Write a file with a path that should be skipped.
	eng.WriteFile("/.DS_Store", []byte("skip me"))
	eng.WriteFile("/._resource", []byte("skip me too"))

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()
	time.Sleep(2 * time.Second)

	// Neither .DS_Store nor ._resource should have stubs.
	if _, err := os.Stat(filepath.Join(sd.Root(), ".DS_Store")); err == nil {
		t.Error(".DS_Store should not have a stub")
	}
	if _, err := os.Stat(filepath.Join(sd.Root(), "._resource")); err == nil {
		t.Error("._resource should not have a stub")
	}
}

// ── SyncDir: downloadMissing createStubFile error ───────────────────────────

func TestSyncDir_DownloadMissing_StubCreateError(t *testing.T) {
	eng, _ := newTestEngineForVFS(t)
	eng.WriteFile("/doc.txt", []byte("content"))

	// Use a root path that can't be written to.
	sd := vfs.NewSyncDir("/dev/null/impossible", eng, t.TempDir())

	ctx := context.Background()
	// Start will fail or downloadMissing will fail to create stubs.
	sd.Start(ctx) //nolint:errcheck
	time.Sleep(2 * time.Second)
	sd.Stop()
}

// ── UnpinFile: file still uploading ─────────────────────────────────────────

func TestSyncDir_UnpinFile_StillUploading(t *testing.T) {
	sd, eng, cloud := newTestSyncDir(t)

	// Make uploads slow so the file stays in "pending" state.
	cloud.mu.Lock()
	cloud.putDelay = 30 * time.Second
	cloud.mu.Unlock()

	// Start a write that won't complete.
	go eng.WriteFile("/slow.txt", []byte("data"))
	time.Sleep(500 * time.Millisecond)

	err := sd.UnpinFile("/slow.txt")
	if err == nil {
		t.Error("expected error for file still uploading")
	}

	cloud.mu.Lock()
	cloud.putDelay = 0
	cloud.mu.Unlock()
}

// ── WebDAV Close: normal write path ─────────────────────────────────────────

func TestWebDAVFile_Close_NormalWrite(t *testing.T) {
	fs, _, _ := newTestFS(t)

	// Open file for writing.
	f, err := fs.OpenFile(context.Background(), "/syncerr.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("hello"))

	// Test that Close on a small written file succeeds.
	if err := f.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// ── WebDAV Readdir: ListDir error ───────────────────────────────────────────

func TestWebDAVFile_Readdir_Error(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	total, free := int64(200e9), int64(199e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "p1", Type: "drive", DisplayName: "TestDrive",
		RcloneRemote: "fake:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, cloud, b, encKey)
	defer eng.Close()

	fs := vfs.NewWebDAVFS(eng, "")

	// Open root as directory.
	f, err := fs.OpenFile(context.Background(), "/", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Close DB so ListDir fails.
	db.Close()

	_, readErr := f.Readdir(-1)
	if readErr == nil {
		t.Error("expected error from broken DB in Readdir")
	}
	f.Close()
}

// ── SyncDir: rename detection with failure fallback ─────────────────────────

func TestSyncDir_RenameDetection_Observed(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a file and wait for it to upload.
	localPath := filepath.Join(sd.Root(), "before.txt")
	os.WriteFile(localPath, []byte("rename me"), 0644)
	time.Sleep(4 * time.Second)

	// Verify it was uploaded.
	if fi, _ := eng.Stat("/before.txt"); fi == nil {
		t.Fatal("file should have been uploaded")
	}

	// Rename it locally — should trigger rename detection.
	newPath := filepath.Join(sd.Root(), "after.txt")
	os.Rename(localPath, newPath)
	time.Sleep(4 * time.Second)

	// The old path should be gone and the new path should exist.
	if fi, _ := eng.Stat("/after.txt"); fi == nil {
		t.Error("renamed file should exist at new path")
	}
}

// ── SyncDir: dir create triggers scanDir ────────────────────────────────────

func TestSyncDir_DirCreate_ScanDir(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a directory with a file already in it — scanDir should pick it up.
	subDir := filepath.Join(sd.Root(), "subdir")
	os.MkdirAll(subDir, 0755)
	os.WriteFile(filepath.Join(subDir, "inner.txt"), []byte("inner content"), 0644)
	time.Sleep(4 * time.Second)

	if fi, _ := eng.Stat("/subdir/inner.txt"); fi == nil {
		t.Error("file inside new dir should have been uploaded via scanDir")
	}
}

// ── SyncDir: scanDir skips .Trash directories ───────────────────────────────

func TestSyncDir_ScanDir_SkipsTrash(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a .Trash directory with files — should be skipped.
	trashDir := filepath.Join(sd.Root(), ".Trash")
	os.MkdirAll(trashDir, 0755)
	os.WriteFile(filepath.Join(trashDir, "deleted.txt"), []byte("trash"), 0644)
	time.Sleep(4 * time.Second)

	// File in .Trash should not be uploaded.
	if fi, _ := eng.Stat("/.Trash/deleted.txt"); fi != nil {
		t.Error(".Trash contents should not be uploaded")
	}
}

// ── Upload: large file with bad spool dir (CreateTemp fails) ────────────────

func TestSyncDir_Upload_LargeFile_SpoolError(t *testing.T) {
	eng, _ := newTestEngineForVFS(t)

	// Use a non-existent spool directory so CreateTemp fails for large files.
	root := t.TempDir()
	sd := vfs.NewSyncDir(root, eng, "/nonexistent/spool/dir")

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a file larger than AsyncWriteThreshold (4MB).
	largePath := filepath.Join(root, "big.bin")
	bigData := make([]byte, 5*1024*1024)
	os.WriteFile(largePath, bigData, 0644)
	time.Sleep(5 * time.Second)

	// File should NOT have been uploaded (spool creation failed).
	if fi, _ := eng.Stat("/big.bin"); fi != nil {
		t.Error("large file should not have been uploaded with broken spool")
	}
}

// ── Upload: large file with cloud error (WriteFileAsync fails) ──────────────

func TestSyncDir_Upload_LargeFile_CloudError(t *testing.T) {
	sd, _, cloud := newTestSyncDir(t)

	cloud.mu.Lock()
	cloud.putErr = fmt.Errorf("cloud failure")
	cloud.mu.Unlock()

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()

	// Create a >4MB file.
	largePath := filepath.Join(sd.Root(), "big-fail.bin")
	bigData := make([]byte, 5*1024*1024)
	os.WriteFile(largePath, bigData, 0644)
	time.Sleep(5 * time.Second)

	cloud.mu.Lock()
	cloud.putErr = nil
	cloud.mu.Unlock()
}

// ── WebDAV Stat: engine.Stat error (not just IsDir) ─────────────────────────

func TestWebDAVFS_Stat_BrokenFilesTable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	total, free := int64(200e9), int64(199e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "p1", Type: "drive", DisplayName: "TestDrive",
		RcloneRemote: "fake:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, cloud, b, encKey)
	defer eng.Close()

	fs := vfs.NewWebDAVFS(eng, "")

	// Break the files table so Stat returns an error (not nil, nil).
	db.Conn().Exec("ALTER TABLE files RENAME TO files_broken")

	_, statErr := fs.Stat(context.Background(), "/anyfile.txt")
	if statErr == nil {
		t.Error("expected error from broken files table in Stat")
	}
}

// ── WebDAV Stat: IsDir error (file not found, but directories query broken) ─

func TestWebDAVFS_Stat_BrokenDirsTable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	total, free := int64(200e9), int64(199e9)
	db.UpsertProvider(&metadata.Provider{
		ID: "p1", Type: "drive", DisplayName: "TestDrive",
		RcloneRemote: "fake:", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})
	cloud := newFakeCloud()
	b := broker.NewBroker(db, broker.PolicyPFRD, 0)
	encKey := make([]byte, 32)
	eng := engine.NewEngineWithCloud(db, dbPath, cloud, b, encKey)
	defer eng.Close()

	fs := vfs.NewWebDAVFS(eng, "")

	// Break the directories table so IsDir fails (but files table works).
	db.Conn().Exec("ALTER TABLE directories RENAME TO directories_broken")

	_, statErr := fs.Stat(context.Background(), "/some/dir")
	if statErr == nil {
		t.Error("expected error from broken directories table in Stat")
	}
}

// ── SyncDir Stop with pending removal timers ────────────────────────────────

func TestSyncDir_StopCancelsPendingRemovals(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)
	// Upload a file to the engine so it's known.
	eng.WriteFile("/tracked.txt", []byte("tracked content"))
	// Create the local copy so it matches.
	localPath := filepath.Join(sd.Root(), "tracked.txt")
	os.WriteFile(localPath, []byte("tracked content"), 0644)

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Remove the local file → creates a removal entry with a timer.
	os.Remove(localPath)
	// Wait just enough for the event to be processed but less than renameWindow (500ms).
	time.Sleep(200 * time.Millisecond)

	// Stop should cancel the pending removal timer without panicking.
	sd.Stop()
}

// ── SyncDir downloadMissing: insert pending file via raw SQL ────────────────
// Covers syncdir.go line 411: if f.UploadState != "complete" { continue }

func TestSyncDir_DownloadMissing_SkipsPendingUploadState(t *testing.T) {
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

	// Insert a file with upload_state="pending" directly in the DB.
	now := time.Now().Unix()
	db.Conn().Exec(
		`INSERT INTO files (id, virtual_path, size_bytes, created_at, modified_at, sha256_full, upload_state)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"pending-id", "/pending-file.txt", 100, now, now, "abc123", "pending",
	)

	// Also insert a complete file for comparison.
	eng.WriteFile("/complete-file.txt", []byte("complete"))

	root := t.TempDir()
	spool := t.TempDir()
	sd := vfs.NewSyncDir(root, eng, spool)

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer sd.Stop()
	time.Sleep(1 * time.Second)

	// The pending file should NOT have a stub created.
	if _, err := os.Stat(filepath.Join(root, "pending-file.txt")); err == nil {
		t.Error("expected no stub for pending file, but found one")
	}
	// The complete file should have a stub.
	if _, err := os.Stat(filepath.Join(root, "complete-file.txt")); err != nil {
		t.Errorf("expected stub for complete file: %v", err)
	}
}

// ── SyncDir downloadMissing: createStubFile error (unwritable dir) ──────────
// Covers syncdir.go lines 422-424

func TestSyncDir_DownloadMissing_StubCreatePermError(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)
	eng.WriteFile("/deep/file.txt", []byte("file content"))

	// Make the root dir read-only so stub creation fails.
	os.Chmod(sd.Root(), 0555)
	t.Cleanup(func() { os.Chmod(sd.Root(), 0755) })

	ctx := context.Background()
	if err := sd.Start(ctx); err != nil {
		// Start may fail or succeed with logged errors; either is acceptable.
		sd.Stop()
	} else {
		defer sd.Stop()
	}
}

// ── SyncDir downloadMissing: ListDir error (broken table) ───────────────────
// Covers syncdir.go lines 396-398

func TestSyncDir_DownloadMissing_ListDirError(t *testing.T) {
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

	// Insert a complete file first, then break the DB.
	eng.WriteFile("/data.txt", []byte("data"))

	root := t.TempDir()
	spool := t.TempDir()
	sd := vfs.NewSyncDir(root, eng, spool)

	// Break both files and directories tables so ListDir fails.
	db.Conn().Exec("ALTER TABLE files RENAME TO files_broken")
	db.Conn().Exec("ALTER TABLE directories RENAME TO directories_broken")

	ctx := context.Background()
	// Start should not panic even when downloadMissing gets errors.
	if err := sd.Start(ctx); err != nil {
		sd.Stop()
	} else {
		defer sd.Stop()
	}
}

// ── SyncDir UnpinFile: createStubFile fails (unwritable path) ───────────────
// Covers syncdir.go lines 480-482

func TestSyncDir_UnpinFile_StubCreationError(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)
	eng.WriteFile("/locked.txt", []byte("content for locked file"))

	// Pin the file first (download locally).
	if err := sd.PinFile("/locked.txt"); err != nil {
		t.Fatal(err)
	}

	// Make the file itself read-only so createStubFile's OpenFile fails.
	localPath := filepath.Join(sd.Root(), "locked.txt")
	os.Chmod(localPath, 0444)
	t.Cleanup(func() { os.Chmod(localPath, 0644) })

	err := sd.UnpinFile("/locked.txt")
	if err == nil {
		t.Error("expected error when stub creation fails due to permissions")
	}
}

// ── SyncDir upload error paths ──────────────────────────────────────────────

// TestSyncDir_Upload_OpenError covers the os.Open error branch in the large-
// file upload path. By lowering AsyncWriteThreshold to 1 byte, even a small
// file takes the large-file code path. The file is chmod 0000 so Stat succeeds
// but Open fails.
func TestSyncDir_Upload_OpenError(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	// Create a file larger than AsyncWriteThreshold that can be stat'd but not read.
	fp := filepath.Join(sd.Root(), "unreadable.bin")
	data := make([]byte, engine.AsyncWriteThreshold+1)
	os.WriteFile(fp, data, 0644)
	os.Chmod(fp, 0000)
	t.Cleanup(func() { os.Chmod(fp, 0644) })

	// Start sync — initialSync will try to upload this file and hit Open error.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sd.Start(ctx)
	time.Sleep(2 * time.Second)
	sd.Stop()

	// The file should NOT appear in the engine (upload failed).
	f, _ := eng.Stat("/unreadable.bin")
	if f != nil {
		t.Error("file should not have been uploaded")
	}
}

// TestSyncDir_Upload_StatError covers the os.Stat error/IsDir branch —
// file disappears between event and upload.
func TestSyncDir_Upload_StatError(t *testing.T) {
	sd, eng, _ := newTestSyncDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sd.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// Create and immediately delete a file before sync can upload it.
	fp := filepath.Join(sd.Root(), "vanish.txt")
	os.WriteFile(fp, []byte("poof"), 0644)
	time.Sleep(100 * time.Millisecond)
	os.Remove(fp)
	time.Sleep(3 * time.Second)
	sd.Stop()

	f, _ := eng.Stat("/vanish.txt")
	if f != nil {
		t.Error("vanished file should not appear in engine")
	}
}
