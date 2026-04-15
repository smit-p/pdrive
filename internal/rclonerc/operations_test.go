package rclonerc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestIsRateLimited(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("connection refused"), false},
		{fmt.Errorf("rclone RC operations/uploadfile: HTTP 500: 429 Too Many Requests"), true},
		{fmt.Errorf("rateLimitExceeded"), true},
		{fmt.Errorf("upload failed: userRateLimitExceeded"), true},
		{fmt.Errorf("too many requests"), true},
		{fmt.Errorf("rate limit exceeded"), true},
		{fmt.Errorf("upload failed: HTTP 500"), false},
	}
	for _, tt := range tests {
		got := IsRateLimited(tt.err)
		if got != tt.want {
			t.Errorf("IsRateLimited(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestEnsureColon(t *testing.T) {
	tests := []struct{ in, want string }{
		{"drive", "drive:"},
		{"drive:", "drive:"},
		{"remote:", "remote:"},
		{"remote", "remote:"},
	}
	for _, tt := range tests {
		if got := ensureColon(tt.in); got != tt.want {
			t.Errorf("ensureColon(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNewClient(t *testing.T) {
	c := NewClient("127.0.0.1:5572")
	if c.baseURL != "http://127.0.0.1:5572" {
		t.Errorf("unexpected baseURL: %s", c.baseURL)
	}
}

// fakeRclone creates a mock rclone RC server and returns a Client pointing at it.
func fakeRclone(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c := NewClient(strings.TrimPrefix(srv.URL, "http://"))
	return c
}

func TestPing_Success(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"version":"v1.60"}`))
	})
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestPing_Failure(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`server error`))
	})
	if err := c.Ping(); err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestCall_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad"}`))
	})
	_, err := c.call("some/endpoint", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should mention status code: %v", err)
	}
}

func TestPutFile(t *testing.T) {
	var copyfileBody map[string]interface{}
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		switch r.URL.Path {
		case "/operations/copyfile":
			json.NewDecoder(r.Body).Decode(&copyfileBody)
			if copyfileBody["dstFs"] != "drive:" {
				t.Errorf("expected dstFs=drive:, got %v", copyfileBody["dstFs"])
			}
			if copyfileBody["dstRemote"] != "pdrive-chunks/chunk001" {
				t.Errorf("expected dstRemote=pdrive-chunks/chunk001, got %v", copyfileBody["dstRemote"])
			}
			if copyfileBody["_async"] != true {
				t.Error("expected _async=true")
			}
			// Verify the temp file was created with correct content.
			srcFs, _ := copyfileBody["srcFs"].(string)
			srcRemote, _ := copyfileBody["srcRemote"].(string)
			if srcFs != "" && srcRemote != "" {
				data, err := os.ReadFile(srcFs + srcRemote)
				if err != nil {
					t.Fatalf("temp file not readable: %v", err)
				}
				if string(data) != "hello" {
					t.Errorf("expected temp file content 'hello', got %q", string(data))
				}
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"jobid": 42})
		case "/job/status":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"finished": true, "success": true})
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	if err := c.PutFile("drive", "pdrive-chunks/chunk001", strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
}

func TestPutFile_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/operations/copyfile":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"jobid": 1})
		case "/job/status":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"finished": true, "success": false, "error": "upload error 500"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	err := c.PutFile("drive", "pdrive-chunks/chunk001", strings.NewReader("data"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "upload error 500") {
		t.Errorf("error should contain rclone error message: %v", err)
	}
}

func TestPutFile_CopyfileStartError(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	})
	err := c.PutFile("drive", "pdrive-chunks/chunk001", strings.NewReader("data"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetFile(t *testing.T) {
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
	// GetFile launches async copyfile then opens the local file — the mock
	// returns success but since no local file is written, we expect an error
	// when trying to open the temp file.
	rc, err := c.GetFile("drive", "pdrive-chunks/chunk001")
	if err == nil {
		rc.Close()
		t.Fatal("expected error because temp file won't exist from mock")
	}
}

func TestGetFile_Success(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/operations/copyfile":
			// Parse the request body to find dstFs and dstRemote.
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			dstFs, _ := body["dstFs"].(string)
			dstRemote, _ := body["dstRemote"].(string)
			// Simulate rclone writing the file to dstFs/dstRemote.
			if dstFs != "" && dstRemote != "" {
				os.WriteFile(dstFs+dstRemote, []byte("hello"), 0644)
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"jobid": 42})
		case "/job/status":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"finished": true, "success": true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	rc, err := c.GetFile("drive", "pdrive-chunks/chunk001")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Errorf("expected 'hello', got %q", string(data))
	}
}

func TestGetFile_CloseCleansUp(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/operations/copyfile":
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			dstFs, _ := body["dstFs"].(string)
			dstRemote, _ := body["dstRemote"].(string)
			if dstFs != "" && dstRemote != "" {
				os.WriteFile(dstFs+dstRemote, []byte("data"), 0644)
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"jobid": 7})
		case "/job/status":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"finished": true, "success": true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	rc, err := c.GetFile("drive", "test/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	// Get the temp dir path before closing.
	tfrc := rc.(*tempFileReadCloser)
	tmpDir := tfrc.tmpDir

	rc.Close()
	// After Close, temp dir should be removed.
	if _, err := os.Stat(tmpDir); err == nil {
		t.Error("expected temp dir to be removed after Close")
	}
}

func TestDeleteFile(t *testing.T) {
	var called bool
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		called = true
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		if body["fs"] != "drive:" {
			t.Errorf("expected fs=drive:, got %v", body["fs"])
		}
		if body["remote"] != "pdrive-chunks/chunk001" {
			t.Errorf("unexpected remote: %v", body["remote"])
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})
	if err := c.DeleteFile("drive", "pdrive-chunks/chunk001"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("handler was not called")
	}
}

func TestDeleteFile_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`delete failed`))
	})
	err := c.DeleteFile("drive", "chunk")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCleanup(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})
	if err := c.Cleanup("drive"); err != nil {
		t.Fatal(err)
	}
}

func TestCleanup_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`fail`))
	})
	if err := c.Cleanup("drive"); err == nil {
		t.Fatal("expected error")
	}
}

func TestListDir(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"list": []map[string]interface{}{
				{"Path": "file1.txt", "Name": "file1.txt", "Size": 42, "IsDir": false},
				{"Path": "subdir", "Name": "subdir", "Size": 0, "IsDir": true},
			},
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	})
	items, err := c.ListDir("drive", "pdrive-chunks")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].Name != "file1.txt" {
		t.Errorf("expected file1.txt, got %q", items[0].Name)
	}
	if items[1].IsDir != true {
		t.Error("expected second item to be a directory")
	}
}

func TestListDir_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`fail`))
	})
	_, err := c.ListDir("drive", "path")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestListRemotes(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"remotes": []string{"drive:", "onedrive:"},
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	})
	remotes, err := c.ListRemotes()
	if err != nil {
		t.Fatal(err)
	}
	if len(remotes) != 2 {
		t.Fatalf("expected 2 remotes, got %d", len(remotes))
	}
}

func TestListRemotes_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`fail`))
	})
	_, err := c.ListRemotes()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTempFileReadCloser_Close(t *testing.T) {
	dir := t.TempDir()
	f, err := io.NopCloser(strings.NewReader("data")), error(nil)
	_ = f
	if err != nil {
		t.Fatal(err)
	}
	// We can't easily test tempFileReadCloser without creating real temp files,
	// but we can verify the struct embeds work properly by creating one.
	_ = &tempFileReadCloser{tmpDir: dir}
}

func TestListDir_BadJSON(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not-json`))
	})
	_, err := c.ListDir("drive", "path")
	if err == nil {
		t.Fatal("expected error for bad JSON")
	}
}

func TestListRemotes_BadJSON(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not-json`))
	})
	_, err := c.ListRemotes()
	if err == nil {
		t.Fatal("expected error for bad JSON")
	}
}

// ---------------------------------------------------------------------------
// call: json.Marshal error (unmarshalable params)
// ---------------------------------------------------------------------------

func TestCall_MarshalError(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// Channels cannot be marshaled to JSON.
	_, err := c.call("test", map[string]interface{}{"bad": make(chan int)})
	if err == nil {
		t.Fatal("expected marshal error")
	}
	if !strings.Contains(err.Error(), "marshaling") {
		t.Errorf("expected 'marshaling' in error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// call: unreachable server (httpClient.Post error)
// ---------------------------------------------------------------------------

func TestCall_Unreachable(t *testing.T) {
	c := NewClient("127.0.0.1:1") // port 1 should be unreachable
	_, err := c.call("test/endpoint", nil)
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
	if !strings.Contains(err.Error(), "calling") {
		t.Errorf("expected 'calling' in error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetFile: call error cleans up temp dir
// ---------------------------------------------------------------------------

func TestGetFile_CallError(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`copyfile failed`))
	})
	_, err := c.GetFile("drive", "pdrive-chunks/chunk001")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "starting async download") {
		t.Errorf("expected 'starting async download' in error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// PutFile: reader error during io.Copy
// ---------------------------------------------------------------------------

func TestPutFile_ReaderError(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	err := c.PutFile("drive", "pdrive-chunks/chunk001", &errReaderRC{})
	if err == nil {
		t.Fatal("expected error from bad reader")
	}
	if !strings.Contains(err.Error(), "writing temp file") {
		t.Errorf("expected 'writing temp file' in error, got: %v", err)
	}
}

type errReaderRC struct{}

func (e *errReaderRC) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("disk read failed")
}

// ---------------------------------------------------------------------------
// PutFile: unreachable server
// ---------------------------------------------------------------------------

func TestPutFile_Unreachable(t *testing.T) {
	c := NewClient("127.0.0.1:1")
	err := c.PutFile("drive", "path/file", strings.NewReader("data"))
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
}

// ---------------------------------------------------------------------------
// About (operations/about)
// ---------------------------------------------------------------------------

func TestAbout_Success(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"total": 15e9,
			"used":  5e9,
			"free":  10e9,
		})
	})
	info, err := c.About("drive")
	if err != nil {
		t.Fatal(err)
	}
	if info.Total != int64(15e9) {
		t.Errorf("Total = %d, want %d", info.Total, int64(15e9))
	}
	if info.Free != int64(10e9) {
		t.Errorf("Free = %d, want %d", info.Free, int64(10e9))
	}
}

func TestAbout_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`error`))
	})
	_, err := c.About("drive")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAbout_BadJSON(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`not json`))
	})
	_, err := c.About("drive")
	if err == nil {
		t.Fatal("expected error for bad JSON")
	}
}

// ---------------------------------------------------------------------------
// GetRemoteType (config/get)
// ---------------------------------------------------------------------------

func TestGetRemoteType_Success(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"type":      "drive",
			"client_id": "xxx",
		})
	})
	rt, err := c.GetRemoteType("gdrive")
	if err != nil {
		t.Fatal(err)
	}
	if rt != "drive" {
		t.Errorf("GetRemoteType = %q, want %q", rt, "drive")
	}
}

func TestGetRemoteType_NoType(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{"client_id": "xxx"})
	})
	rt, err := c.GetRemoteType("gdrive")
	if err != nil {
		t.Fatal(err)
	}
	if rt != "unknown" {
		t.Errorf("GetRemoteType = %q, want %q", rt, "unknown")
	}
}

func TestGetRemoteType_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`not found`))
	})
	_, err := c.GetRemoteType("gdrive")
	if err == nil {
		t.Fatal("expected error")
	}
}

// ── StreamGetFile tests ─────────────────────────────────────────────────────

func TestStreamGetFile_Success(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("expected GET, got %s", r.Method)
		}
		// Verify the URL path uses the bracket format.
		want := "/[gdrive:]pdrive-chunks/test.enc"
		if r.URL.Path != want {
			t.Errorf("expected path %q, got %q", want, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("encrypted-data-bytes"))
	})

	rc, err := c.StreamGetFile("gdrive", "pdrive-chunks/test.enc")
	if err != nil {
		t.Fatalf("StreamGetFile: %v", err)
	}
	defer rc.Close()

	body, _ := io.ReadAll(rc)
	if string(body) != "encrypted-data-bytes" {
		t.Errorf("body = %q, want %q", body, "encrypted-data-bytes")
	}
}

func TestStreamGetFile_RemoteWithColon(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		// Remote already has colon — should not double it.
		want := "/[gdrive:]path/file.bin"
		if r.URL.Path != want {
			t.Errorf("expected path %q, got %q", want, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	rc, err := c.StreamGetFile("gdrive:", "path/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
}

func TestStreamGetFile_HTTPError(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found on remote"))
	})

	_, err := c.StreamGetFile("gdrive", "missing/file.enc")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error should mention status code: %v", err)
	}
	if !strings.Contains(err.Error(), "file not found on remote") {
		t.Errorf("error should contain body: %v", err)
	}
}

func TestStreamGetFile_ServerError(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	})

	_, err := c.StreamGetFile("gdrive", "chunk.enc")
	if err == nil {
		t.Fatal("expected error for 500")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should mention 500: %v", err)
	}
}

func TestStreamGetFile_ConnectionError(t *testing.T) {
	// Client pointing to a closed server.
	c := NewClient("127.0.0.1:1") // Port 1 — should fail to connect.
	c.httpClient.Timeout = 1 * time.Second

	_, err := c.StreamGetFile("gdrive", "chunk.enc")
	if err == nil {
		t.Fatal("expected connection error")
	}
	if !strings.Contains(err.Error(), "streaming GET") {
		t.Errorf("error should mention streaming GET: %v", err)
	}
}

func TestStreamGetFile_LargeBody(t *testing.T) {
	data := bytes.Repeat([]byte("A"), 1024*1024) // 1 MB
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	})

	rc, err := c.StreamGetFile("gdrive", "big-chunk.enc")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if len(got) != len(data) {
		t.Errorf("expected %d bytes, got %d", len(data), len(got))
	}
}

func TestStreamGetFile_EmptyBody(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Empty body
	})

	rc, err := c.StreamGetFile("gdrive", "empty.enc")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if len(got) != 0 {
		t.Errorf("expected empty body, got %d bytes", len(got))
	}
}

func TestStreamGetFile_URLFormat(t *testing.T) {
	// Verify the URL construction with various remote names.
	tests := []struct {
		remote string
		path   string
		want   string
	}{
		{"gdrive", "chunks/a.enc", "/[gdrive:]chunks/a.enc"},
		{"gdrive:", "chunks/b.enc", "/[gdrive:]chunks/b.enc"},
		{"dropbox", "pdrive-chunks/c.enc", "/[dropbox:]pdrive-chunks/c.enc"},
		{"s3:", "bucket/key.enc", "/[s3:]bucket/key.enc"},
	}

	for _, tt := range tests {
		t.Run(tt.remote+"_"+tt.path, func(t *testing.T) {
			c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tt.want {
					t.Errorf("path = %q, want %q", r.URL.Path, tt.want)
				}
				w.WriteHeader(http.StatusOK)
			})
			rc, err := c.StreamGetFile(tt.remote, tt.path)
			if err != nil {
				t.Fatal(err)
			}
			rc.Close()
		})
	}
}
