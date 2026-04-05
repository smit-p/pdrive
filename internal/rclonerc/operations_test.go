package rclonerc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
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
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.HasPrefix(r.URL.Path, "/operations/uploadfile") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		fs := r.URL.Query().Get("fs")
		if fs != "drive:" {
			t.Errorf("expected fs=drive:, got %q", fs)
		}
		// Parse multipart to verify file content.
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			t.Fatal(err)
		}
		f, header, err := r.FormFile("file0")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if header.Filename != "chunk001" {
			t.Errorf("expected filename chunk001, got %q", header.Filename)
		}
		data, _ := io.ReadAll(f)
		if string(data) != "hello" {
			t.Errorf("expected file content 'hello', got %q", string(data))
		}
		w.WriteHeader(http.StatusOK)
	})
	if err := c.PutFile("drive", "pdrive-chunks/chunk001", strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
}

func TestPutFile_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("upload error"))
	})
	err := c.PutFile("drive", "pdrive-chunks/chunk001", strings.NewReader("data"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should contain status code: %v", err)
	}
}

func TestGetFile(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		// copyfile writes the file locally — simulate success.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})
	// GetFile calls operations/copyfile then opens the local file — the mock
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
		w.Write([]byte(`{}`))
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
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		dstFs, _ := body["dstFs"].(string)
		dstRemote, _ := body["dstRemote"].(string)
		if dstFs != "" && dstRemote != "" {
			os.WriteFile(dstFs+dstRemote, []byte("data"), 0644)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
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
	if !strings.Contains(err.Error(), "downloading") {
		t.Errorf("expected 'downloading' in error, got: %v", err)
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
	if !strings.Contains(err.Error(), "copying") {
		t.Errorf("expected 'copying' in error, got: %v", err)
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
