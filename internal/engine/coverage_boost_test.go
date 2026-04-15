package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/smit-p/pdrive/internal/metadata"
)

// ── fmtBytes ────────────────────────────────────────────────────────────────

func TestFmtBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1048576, "1.0 MiB"},
		{1073741824, "1.0 GiB"},
		{1099511627776, "1.0 TiB"},
		{1125899906842624, "1.0 PiB"},
		{1152921504606846976, "1.0 EiB"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.input), func(t *testing.T) {
			got := fmtBytes(tt.input)
			if got != tt.want {
				t.Errorf("fmtBytes(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ── CheckSpace ──────────────────────────────────────────────────────────────

func TestCheckSpace_Exceeded(t *testing.T) {
	eng, _ := newTestEngine(t)
	defer eng.Close()

	// The test provider has ~99 GB free. Ask for something larger.
	err := eng.CheckSpace(200e9)
	if err == nil {
		t.Fatal("expected insufficient space error")
	}
	if !strings.Contains(err.Error(), "exceeds available") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ── EnsureRemoteDirs ────────────────────────────────────────────────────────

func TestEnsureRemoteDirs(t *testing.T) {
	eng, _ := newTestEngine(t)
	defer eng.Close()

	// Should not panic or error with a valid provider set up.
	eng.EnsureRemoteDirs()
}

// ── RetryFailedDeletions ────────────────────────────────────────────────────

func TestRetryFailedDeletions_SuccessfulRetry(t *testing.T) {
	eng, fc := newTestEngine(t)
	defer eng.Close()

	// Put a chunk in the cloud, then record it as a failed deletion.
	fc.setObject("fake:", "pdrive-chunks/orphan1", []byte("data"))
	eng.db.InsertFailedDeletion("p1", "pdrive-chunks/orphan1", "transient")

	eng.RetryFailedDeletions()

	// Chunk should be deleted from cloud.
	if fc.hasObject("fake:", "pdrive-chunks/orphan1") {
		t.Error("expected orphan1 to be deleted")
	}
	// Failed deletion record should be removed.
	items, _ := eng.db.GetFailedDeletions(10)
	if len(items) != 0 {
		t.Errorf("expected 0 failed deletions, got %d", len(items))
	}
}

func TestRetryFailedDeletions_AbandonAfterMaxRetriesCount(t *testing.T) {
	eng, _ := newTestEngine(t)
	defer eng.Close()

	eng.db.InsertFailedDeletion("p1", "pdrive-chunks/old", "transient")
	// Bump retry count above the threshold (maxRetries=10).
	for i := 0; i < 11; i++ {
		eng.db.IncrementFailedDeletionRetry(1, "still failing")
	}

	eng.RetryFailedDeletions()

	// Should be abandoned (removed despite not being successfully deleted).
	items, _ := eng.db.GetFailedDeletions(10)
	if len(items) != 0 {
		t.Errorf("expected abandoned record to be removed, got %d", len(items))
	}
}

func TestRetryFailedDeletions_ProviderNotFound(t *testing.T) {
	eng, _ := newTestEngine(t)
	defer eng.Close()

	// Insert a failed deletion for a non-existent provider.
	eng.db.InsertFailedDeletion("nonexistent_provider", "pdrive-chunks/x", "err")

	eng.RetryFailedDeletions()

	// Should be abandoned since the provider doesn't exist.
	items, _ := eng.db.GetFailedDeletions(10)
	if len(items) != 0 {
		t.Errorf("expected record to be abandoned, got %d", len(items))
	}
}

func TestRetryFailedDeletions_DeleteStillFails(t *testing.T) {
	eng, fc := newTestEngine(t)
	defer eng.Close()

	eng.db.InsertFailedDeletion("p1", "pdrive-chunks/stuck", "err")
	fc.deleteErr = fmt.Errorf("cloud unavailable")

	eng.RetryFailedDeletions()

	// Should still be in the failed_deletions table with incremented retry.
	items, _ := eng.db.GetFailedDeletions(10)
	if len(items) != 1 {
		t.Fatalf("expected 1 failed deletion, got %d", len(items))
	}
	if items[0].RetryCount < 1 {
		t.Errorf("retry_count should be incremented, got %d", items[0].RetryCount)
	}
}

// ── deleteCloudChunks shared-chunk skip ─────────────────────────────────────

func TestDeleteCloudChunks_SkipsSharedChunk(t *testing.T) {
	eng, fc := newTestEngine(t)
	defer eng.Close()

	data := []byte("hello shared world")
	f, p := writeTmpFile(t, data)
	if err := eng.WriteFileAsync("/a.txt", f, p, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	eng.WaitUploads()

	// Write same content to /b.txt — should dedup (clone chunks).
	f2, p2 := writeTmpFile(t, data)
	if err := eng.WriteFileAsync("/b.txt", f2, p2, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	eng.WaitUploads()

	chunksBefore := countNonMeta(fc)

	// Delete the first file — shared chunk should NOT be removed from cloud.
	if err := eng.DeleteFile("/a.txt"); err != nil {
		t.Fatal(err)
	}
	// Give background deletion a moment.
	time.Sleep(100 * time.Millisecond)

	chunksAfter := countNonMeta(fc)
	if chunksAfter < chunksBefore {
		t.Errorf("shared chunk was deleted: before=%d, after=%d", chunksBefore, chunksAfter)
	}

	// Second file should still be readable.
	got2, err := eng.ReadFile("/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, data) {
		t.Errorf("data mismatch after shared-chunk delete")
	}
}

// ── ResumeUploads edge cases ────────────────────────────────────────────────

func TestResumeUploads_OpenError(t *testing.T) {
	eng, _ := newTestEngine(t)
	defer eng.Close()

	// Insert a pending file with a tmp_path that exists but is unreadable.
	tmpDir := t.TempDir()
	badPath := tmpDir + "/noperm"
	os.WriteFile(badPath, []byte("data"), 0000)

	now := time.Now().Unix()
	eng.db.InsertFile(&metadata.File{
		ID:          "resume-open-err",
		VirtualPath: "/resume-test.txt",
		SizeBytes:   4,
		CreatedAt:   now,
		ModifiedAt:  now,
		SHA256Full:  "0000000000000000000000000000000000000000000000000000000000000000",
		UploadState: "pending",
		TmpPath:     &badPath,
	})

	// Should not panic — just logs the error.
	eng.ResumeUploads()
}

// ── WriteFileAsync dedup short-circuit ──────────────────────────────────────

func TestWriteFileAsync_DedupActivityLog(t *testing.T) {
	eng, _ := newTestEngine(t)
	defer eng.Close()

	data := []byte("dedup activity data")
	f1, p1 := writeTmpFile(t, data)
	if err := eng.WriteFileAsync("/dedup1.txt", f1, p1, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	eng.WaitUploads()

	// Write same content to a different path — triggers dedup.
	f2, p2 := writeTmpFile(t, data)
	if err := eng.WriteFileAsync("/dedup2.txt", f2, p2, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	eng.WaitUploads()

	// Both files should exist and be readable.
	for _, path := range []string{"/dedup1.txt", "/dedup2.txt"} {
		got, err := eng.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) failed: %v", path, err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("%s data mismatch", path)
		}
	}
}

// ── Upload retry: all attempts fail ─────────────────────────────────────────

func TestWriteFileStream_AllRetriesFail(t *testing.T) {
	eng, fc := newTestEngine(t)
	defer eng.Close()
	eng.SetMaxChunkRetries(1) // 1 attempt, no retries
	eng.SetChunkSize(1 << 20)

	fc.putErr = fmt.Errorf("permanent cloud error")

	data := []byte("doomed upload")
	err := eng.WriteFileStream("/fail.txt", bytes.NewReader(data), int64(len(data)))
	if err == nil {
		t.Fatal("expected upload to fail when all retries exhausted")
	}
}

// ── RegisterQueuedUpload / UnregisterQueuedUpload ───────────────────────────

func TestRegisterQueuedUpload(t *testing.T) {
	eng, _ := newTestEngine(t)
	defer eng.Close()

	key := eng.RegisterQueuedUpload("/q.txt", 1234)
	progress := eng.UploadProgress()
	found := false
	for _, p := range progress {
		if p.VirtualPath == "/q.txt" && p.Preparing {
			found = true
		}
	}
	if !found {
		t.Error("queued upload not visible in progress")
	}

	eng.UnregisterQueuedUpload(key)
	progress = eng.UploadProgress()
	for _, p := range progress {
		if p.VirtualPath == "/q.txt" {
			t.Error("queued upload should be gone after unregister")
		}
	}
}

// ── helper ──────────────────────────────────────────────────────────────────

func sha256sum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
