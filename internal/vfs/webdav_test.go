package vfs_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

func newFakeCloud() *fakeCloud { return &fakeCloud{objects: make(map[string][]byte)} }
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

func (f *fakeCloud) GetFile(remote, p string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objects[f.key(remote, p)]
	if !ok {
		return nil, fmt.Errorf("not found: %s/%s", remote, p)
	}
	return data, nil
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
