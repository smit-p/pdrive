package daemon

// Integration tests exercise full request lifecycles through the HTTP API
// backed by a real engine + fakeCloud, verifying that upload → list → download →
// delete → verify chains work correctly end-to-end.

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── Upload → Download round-trip ────────────────────────────────────────────

func TestIntegration_UploadDownloadRoundTrip(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	content := []byte("integration test content — round trip")

	// Upload via multipart API.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "roundtrip.txt")
	part.Write(content)
	w.WriteField("dir", "/integration")
	w.Close()

	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Download the same file.
	req = httptest.NewRequest("GET", "/api/download?path=/integration/roundtrip.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("download: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	got, _ := io.ReadAll(rec.Body)
	if !bytes.Equal(got, content) {
		t.Errorf("download content mismatch: got %q, want %q", got, content)
	}

	// Verify integrity.
	req = httptest.NewRequest("GET", "/api/verify?path=/integration/roundtrip.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("verify: expected 200, got %d", rec.Code)
	}
	var vr struct {
		OK bool `json:"ok"`
	}
	json.NewDecoder(rec.Body).Decode(&vr)
	if !vr.OK {
		t.Error("verify: expected ok=true")
	}
	_ = eng // keep eng reference alive
}

// ── Upload → List → Delete → List (file disappears) ────────────────────────

func TestIntegration_UploadListDeleteList(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	content := []byte("file to be deleted")

	// Upload.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "ephemeral.txt")
	part.Write(content)
	w.WriteField("dir", "/temp")
	w.Close()
	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload: %d", rec.Code)
	}

	// List directory — should contain the file.
	req = httptest.NewRequest("GET", "/api/ls?path=/temp", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ls: %d", rec.Code)
	}
	var ls lsResponse
	json.NewDecoder(rec.Body).Decode(&ls)
	if len(ls.Files) != 1 || ls.Files[0].Name != "ephemeral.txt" {
		t.Fatalf("ls: expected [ephemeral.txt], got %+v", ls.Files)
	}

	// Delete the file.
	req = httptest.NewRequest("POST", "/api/delete?path=/temp/ephemeral.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete: %d", rec.Code)
	}

	// List again — should be empty (or dir gone).
	req = httptest.NewRequest("GET", "/api/ls?path=/temp", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ls after delete: %d", rec.Code)
	}
	json.NewDecoder(rec.Body).Decode(&ls)
	if len(ls.Files) != 0 {
		t.Errorf("ls after delete: expected 0 files, got %d", len(ls.Files))
	}
}

// ── Upload duplicate (dedup) → no extra cloud objects ───────────────────────

func TestIntegration_DedupSameContentTwice(t *testing.T) {
	h, eng, _ := newTestHandlerWithCloud(t)
	content := []byte("dedup me — identical content")

	upload := func(dir, name string) {
		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		part, _ := w.CreateFormFile("file", name)
		part.Write(content)
		w.WriteField("dir", dir)
		w.Close()
		req := httptest.NewRequest("POST", "/api/upload", &buf)
		req.Header.Set("Content-Type", w.FormDataContentType())
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("upload %s/%s: %d – %s", dir, name, rec.Code, rec.Body.String())
		}
	}

	upload("/dedup", "first.txt")
	m1 := eng.Metrics()

	upload("/dedup", "second.txt")
	m2 := eng.Metrics()

	// The second upload should register a dedup hit with zero additional chunks.
	if m2.DedupHits <= m1.DedupHits {
		t.Errorf("expected dedup hit: before=%d, after=%d", m1.DedupHits, m2.DedupHits)
	}
	if m2.ChunksUploaded != m1.ChunksUploaded {
		t.Errorf("second upload should not upload new chunks: before=%d, after=%d",
			m1.ChunksUploaded, m2.ChunksUploaded)
	}

	// Both files should be independently downloadable.
	for _, path := range []string{"/dedup/first.txt", "/dedup/second.txt"} {
		req := httptest.NewRequest("GET", "/api/download?path="+path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("download %s: %d", path, rec.Code)
		}
		got, _ := io.ReadAll(rec.Body)
		if !bytes.Equal(got, content) {
			t.Errorf("download %s: content mismatch", path)
		}
	}
}

// ── Upload → Rename → Download at new path ─────────────────────────────────

func TestIntegration_UploadRenameThenDownload(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	content := []byte("move me around")

	// Upload.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "old-name.txt")
	part.Write(content)
	w.WriteField("dir", "/mv-test")
	w.Close()
	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload: %d", rec.Code)
	}

	// Rename via API.
	req = httptest.NewRequest("POST", "/api/mv?src=/mv-test/old-name.txt&dst=/mv-test/new-name.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("mv: %d – %s", rec.Code, rec.Body.String())
	}

	// Download from new path.
	req = httptest.NewRequest("GET", "/api/download?path=/mv-test/new-name.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("download new: %d", rec.Code)
	}
	got, _ := io.ReadAll(rec.Body)
	if !bytes.Equal(got, content) {
		t.Errorf("download content mismatch after rename")
	}

	// Old path should 404.
	req = httptest.NewRequest("GET", "/api/download?path=/mv-test/old-name.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("download old: expected 404, got %d", rec.Code)
	}
}

// ── Upload multiple files → Tree → DiskUsage ───────────────────────────────

func TestIntegration_MultiFileTreeAndDu(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)

	files := []struct{ dir, name, body string }{
		{"/project", "readme.md", "# Readme"},
		{"/project/src", "main.go", "package main"},
		{"/project/src", "util.go", "package main // util"},
	}
	for _, f := range files {
		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		part, _ := w.CreateFormFile("file", f.name)
		part.Write([]byte(f.body))
		w.WriteField("dir", f.dir)
		w.Close()
		req := httptest.NewRequest("POST", "/api/upload", &buf)
		req.Header.Set("Content-Type", w.FormDataContentType())
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("upload %s/%s: %d", f.dir, f.name, rec.Code)
		}
	}

	// Tree should return all 3 files.
	req := httptest.NewRequest("GET", "/api/tree?path=/project", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("tree: %d", rec.Code)
	}
	var entries []struct {
		Path string `json:"path"`
	}
	json.NewDecoder(rec.Body).Decode(&entries)
	if len(entries) != 3 {
		t.Errorf("tree: expected 3 entries, got %d", len(entries))
	}

	// DiskUsage.
	req = httptest.NewRequest("GET", "/api/du?path=/project", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("du: %d", rec.Code)
	}
	var du struct {
		FileCount  int64 `json:"file_count"`
		TotalBytes int64 `json:"total_bytes"`
	}
	json.NewDecoder(rec.Body).Decode(&du)
	if du.FileCount != 3 {
		t.Errorf("du: expected 3 files, got %d", du.FileCount)
	}
	if du.TotalBytes <= 0 {
		t.Error("du: expected positive byte count")
	}
}

// ── Mkdir → Upload into it → Ls ─────────────────────────────────────────────

func TestIntegration_MkdirUploadLs(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)

	// Create directory via API.
	req := httptest.NewRequest("POST", "/api/mkdir?path=/new-dir", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("mkdir: %d – %s", rec.Code, rec.Body.String())
	}

	// Upload into it.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "child.txt")
	part.Write([]byte("inside new-dir"))
	w.WriteField("dir", "/new-dir")
	w.Close()
	req = httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload: %d", rec.Code)
	}

	// Ls the directory.
	req = httptest.NewRequest("GET", "/api/ls?path=/new-dir", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ls: %d", rec.Code)
	}
	var ls lsResponse
	json.NewDecoder(rec.Body).Decode(&ls)
	if len(ls.Files) != 1 || ls.Files[0].Name != "child.txt" {
		t.Errorf("ls: expected [child.txt], got %+v", ls.Files)
	}
}

// ── Upload → Delete dedup original → clone survives ────────────────────────

func TestIntegration_DeleteDedupOriginal_CloneSurvives(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	content := []byte("clone survivor test data")

	// Upload original.
	upload := func(dir, name string) {
		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		part, _ := w.CreateFormFile("file", name)
		part.Write(content)
		w.WriteField("dir", dir)
		w.Close()
		req := httptest.NewRequest("POST", "/api/upload", &buf)
		req.Header.Set("Content-Type", w.FormDataContentType())
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("upload %s/%s: %d", dir, name, rec.Code)
		}
	}

	upload("/clone", "original.txt")
	upload("/clone", "copy.txt")

	// Delete the original.
	req := httptest.NewRequest("POST", "/api/delete?path=/clone/original.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete: %d", rec.Code)
	}

	// Clone should still be downloadable.
	req = httptest.NewRequest("GET", "/api/download?path=/clone/copy.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("download clone: %d", rec.Code)
	}
	got, _ := io.ReadAll(rec.Body)
	if !bytes.Equal(got, content) {
		t.Errorf("clone content mismatch")
	}
}

// ── Upload → Overwrite → Download (latest version) ─────────────────────────

func TestIntegration_OverwriteReturnsLatest(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)

	upload := func(body string) {
		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		part, _ := w.CreateFormFile("file", "version.txt")
		part.Write([]byte(body))
		w.WriteField("dir", "/overwrite")
		w.Close()
		req := httptest.NewRequest("POST", "/api/upload", &buf)
		req.Header.Set("Content-Type", w.FormDataContentType())
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("upload: %d", rec.Code)
		}
	}

	upload("version 1")
	upload("version 2 — updated")

	req := httptest.NewRequest("GET", "/api/download?path=/overwrite/version.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("download: %d", rec.Code)
	}
	got := rec.Body.String()
	if got != "version 2 — updated" {
		t.Errorf("expected latest version, got %q", got)
	}
}

// ── Find across directories ─────────────────────────────────────────────────

func TestIntegration_FindAcrossDirectories(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)

	files := []struct{ dir, name string }{
		{"/a", "report.pdf"},
		{"/b", "report.csv"},
		{"/c", "photo.jpg"},
	}
	for _, f := range files {
		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		part, _ := w.CreateFormFile("file", f.name)
		part.Write([]byte("data for " + f.name))
		w.WriteField("dir", f.dir)
		w.Close()
		req := httptest.NewRequest("POST", "/api/upload", &buf)
		req.Header.Set("Content-Type", w.FormDataContentType())
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("upload %s/%s: %d", f.dir, f.name, rec.Code)
		}
	}

	req := httptest.NewRequest("GET", "/api/find?path=/&pattern=report", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("find: %d", rec.Code)
	}
	var entries []struct {
		Path string `json:"path"`
	}
	json.NewDecoder(rec.Body).Decode(&entries)
	if len(entries) != 2 {
		t.Errorf("find: expected 2 matches, got %d", len(entries))
	}
}

// ── Metrics reflect upload/download activity ────────────────────────────────

func TestIntegration_MetricsTrackActivity(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)

	// Check initial metrics.
	req := httptest.NewRequest("GET", "/api/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var m0 struct {
		FilesUploaded   int64 `json:"files_uploaded"`
		FilesDownloaded int64 `json:"files_downloaded"`
	}
	json.NewDecoder(rec.Body).Decode(&m0)
	if m0.FilesUploaded != 0 {
		t.Fatalf("initial files_uploaded: %d", m0.FilesUploaded)
	}

	// Upload a file.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "m.txt")
	part.Write([]byte("metrics"))
	w.WriteField("dir", "/metrics")
	w.Close()
	req = httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload: %d", rec.Code)
	}

	// Download the file.
	req = httptest.NewRequest("GET", "/api/download?path=/metrics/m.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("download: %d", rec.Code)
	}

	// Check metrics increased.
	req = httptest.NewRequest("GET", "/api/metrics", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var m1 struct {
		FilesUploaded   int64 `json:"files_uploaded"`
		FilesDownloaded int64 `json:"files_downloaded"`
	}
	json.NewDecoder(rec.Body).Decode(&m1)
	if m1.FilesUploaded <= m0.FilesUploaded {
		t.Errorf("files_uploaded did not increase: %d -> %d", m0.FilesUploaded, m1.FilesUploaded)
	}
	if m1.FilesDownloaded <= m0.FilesDownloaded {
		t.Errorf("downloads did not increase: %d -> %d", m0.FilesDownloaded, m1.FilesDownloaded)
	}
}

// ── Delete directory cleans up all children ─────────────────────────────────

func TestIntegration_DeleteDirCleansChildren(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)

	// Upload 3 files into /cleanup/.
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		part, _ := w.CreateFormFile("file", name)
		part.Write([]byte("content of " + name))
		w.WriteField("dir", "/cleanup")
		w.Close()
		req := httptest.NewRequest("POST", "/api/upload", &buf)
		req.Header.Set("Content-Type", w.FormDataContentType())
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("upload %s: %d", name, rec.Code)
		}
	}

	// Delete the whole directory.
	req := httptest.NewRequest("POST", "/api/delete?path=/cleanup", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete dir: %d – %s", rec.Code, rec.Body.String())
	}

	// Verify every child is gone.
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		req = httptest.NewRequest("GET", "/api/download?path=/cleanup/"+name, nil)
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("download /cleanup/%s after dir delete: expected 404, got %d", name, rec.Code)
		}
	}
}

// ── Info endpoint returns chunk details for uploaded file ────────────────────

func TestIntegration_InfoAfterUpload(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)

	// Upload.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "info-check.txt")
	part.Write([]byte("info endpoint integration test"))
	w.WriteField("dir", "/info")
	w.Close()
	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload: %d", rec.Code)
	}

	// Info.
	req = httptest.NewRequest("GET", "/api/info?path=/info/info-check.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("info: %d", rec.Code)
	}
	var info struct {
		Path      string `json:"path"`
		SizeBytes int64  `json:"size_bytes"`
		Chunks    []struct {
			Sequence int `json:"sequence"`
		} `json:"chunks"`
		SHA256 string `json:"sha256"`
	}
	json.NewDecoder(rec.Body).Decode(&info)
	if info.Path != "/info/info-check.txt" {
		t.Errorf("info path = %q", info.Path)
	}
	if info.SizeBytes != 30 {
		t.Errorf("info size = %d, want 30", info.SizeBytes)
	}
	if info.SHA256 == "" {
		t.Error("info SHA256 should not be empty")
	}
	if len(info.Chunks) < 1 {
		t.Errorf("info chunks = %d, want >= 1", len(info.Chunks))
	}
}

// ── Activity log records all operations ─────────────────────────────────────

func TestIntegration_ActivityLogChain(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)

	// Upload.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "activity.txt")
	part.Write([]byte("activity log test"))
	w.WriteField("dir", "/act")
	w.Close()
	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload: %d", rec.Code)
	}

	// Download.
	req = httptest.NewRequest("GET", "/api/download?path=/act/activity.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Verify.
	req = httptest.NewRequest("GET", "/api/verify?path=/act/activity.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Delete.
	req = httptest.NewRequest("POST", "/api/delete?path=/act/activity.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Activity should log all of these.
	req = httptest.NewRequest("GET", "/api/activity", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("activity: %d", rec.Code)
	}
	var activities []struct {
		Action string `json:"action"`
		Path   string `json:"path"`
	}
	json.NewDecoder(rec.Body).Decode(&activities)

	actions := make(map[string]bool)
	for _, a := range activities {
		if strings.Contains(a.Path, "activity.txt") {
			actions[a.Action] = true
		}
	}
	for _, expected := range []string{"upload", "download", "verify", "delete"} {
		if !actions[expected] {
			t.Errorf("activity log missing %q action for activity.txt", expected)
		}
	}
}

// ── Upload → HEAD download (no body) ────────────────────────────────────────

func TestIntegration_HeadDownload(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)
	content := []byte("head request test data — 123")

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "head.txt")
	part.Write(content)
	w.WriteField("dir", "/head")
	w.Close()
	req := httptest.NewRequest("POST", "/api/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload: %d", rec.Code)
	}

	// HEAD should return headers but no body.
	req = httptest.NewRequest("HEAD", "/api/download?path=/head/head.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD download: %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD should have empty body, got %d bytes", rec.Body.Len())
	}
}

// ── Status endpoint lists providers ─────────────────────────────────────────

func TestIntegration_StatusShowsProviders(t *testing.T) {
	h, _, _ := newTestHandlerWithCloud(t)

	req := httptest.NewRequest("GET", "/api/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d", rec.Code)
	}
	var status struct {
		Providers []struct {
			Name string `json:"name"`
		} `json:"providers"`
	}
	json.NewDecoder(rec.Body).Decode(&status)
	if len(status.Providers) != 1 {
		t.Errorf("expected 1 provider, got %d", len(status.Providers))
	}
}
