package metadata

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── Open error paths ────────────────────────────────────────────────────────

func TestOpen_MkdirAllError(t *testing.T) {
	// Use a path under a file (not a directory) so MkdirAll fails.
	tmpFile := filepath.Join(t.TempDir(), "blocking_file")
	os.WriteFile(tmpFile, []byte("x"), 0644)
	dbPath := filepath.Join(tmpFile, "subdir", "test.db")

	_, err := Open(dbPath)
	if err == nil {
		t.Fatal("expected error from MkdirAll")
	}
	if !strings.Contains(err.Error(), "creating db directory") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestOpen_Idempotent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	db1, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db1.Close()

	// Open same DB again — migrations should be idempotent.
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db2.Close()
}

func TestOpen_Conn(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Conn() == nil {
		t.Error("Conn() should not return nil")
	}
}

// ── IncrementCounter + LoadCounters ─────────────────────────────────────────

func TestIncrementCounter(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	if err := db.IncrementCounter("uploads", 5); err != nil {
		t.Fatal(err)
	}
	if err := db.IncrementCounter("uploads", 3); err != nil {
		t.Fatal(err)
	}
	if err := db.IncrementCounter("downloads", 1); err != nil {
		t.Fatal(err)
	}

	m, err := db.LoadCounters()
	if err != nil {
		t.Fatal(err)
	}
	if m["uploads"] != 8 {
		t.Errorf("uploads = %d, want 8", m["uploads"])
	}
	if m["downloads"] != 1 {
		t.Errorf("downloads = %d, want 1", m["downloads"])
	}
}

func TestLoadCounters_Empty(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	m, err := db.LoadCounters()
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
}

// ── DeductProviderFreeBytes + CreditProviderFreeBytes ───────────────────────

func TestDeductCreditProviderFreeBytes(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	total := int64(100000)
	free := int64(80000)
	db.UpsertProvider(&Provider{
		ID: "p1", Type: "drive", DisplayName: "Test",
		RcloneRemote: "gdrive", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})

	if err := db.DeductProviderFreeBytes("p1", 5000); err != nil {
		t.Fatal(err)
	}

	p, err := db.GetProviderByRemote("gdrive")
	if err != nil {
		t.Fatal(err)
	}
	if *p.QuotaFreeBytes != 75000 {
		t.Errorf("free = %d, want 75000", *p.QuotaFreeBytes)
	}

	if err := db.CreditProviderFreeBytes("p1", 2000); err != nil {
		t.Fatal(err)
	}

	p, err = db.GetProviderByRemote("gdrive")
	if err != nil {
		t.Fatal(err)
	}
	if *p.QuotaFreeBytes != 77000 {
		t.Errorf("free = %d, want 77000", *p.QuotaFreeBytes)
	}
}

func TestDeductProviderFreeBytes_FloorAtZero(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	total := int64(1000)
	free := int64(100)
	db.UpsertProvider(&Provider{
		ID: "p1", Type: "drive", DisplayName: "Test",
		RcloneRemote: "gdrive", QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})

	// Deduct more than available — should floor at 0
	if err := db.DeductProviderFreeBytes("p1", 500); err != nil {
		t.Fatal(err)
	}

	p, _ := db.GetProviderByRemote("gdrive")
	if *p.QuotaFreeBytes != 0 {
		t.Errorf("free = %d, want 0 (floored)", *p.QuotaFreeBytes)
	}
}

// ── Closed DB error paths ───────────────────────────────────────────────────

func TestIncrementCounter_ClosedDB(t *testing.T) {
	db := openTestDB(t)
	db.Close()
	err := db.IncrementCounter("x", 1)
	if err == nil {
		t.Fatal("expected error on closed DB")
	}
}

func TestLoadCounters_ClosedDB(t *testing.T) {
	db := openTestDB(t)
	db.Close()
	_, err := db.LoadCounters()
	if err == nil {
		t.Fatal("expected error on closed DB")
	}
}

func TestDeductProviderFreeBytes_ClosedDB(t *testing.T) {
	db := openTestDB(t)
	db.Close()
	err := db.DeductProviderFreeBytes("p1", 100)
	if err == nil {
		t.Fatal("expected error on closed DB")
	}
}

func TestCreditProviderFreeBytes_ClosedDB(t *testing.T) {
	db := openTestDB(t)
	db.Close()
	err := db.CreditProviderFreeBytes("p1", 100)
	if err == nil {
		t.Fatal("expected error on closed DB")
	}
}

// ── GetProviderChunkBytes ───────────────────────────────────────────────────

func TestGetProviderChunkBytes_EmptyDB(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	m, err := db.GetProviderChunkBytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
}

func TestGetProviderChunkBytes_ClosedConn(t *testing.T) {
	db := openTestDB(t)
	db.Close()
	_, err := db.GetProviderChunkBytes()
	if err == nil {
		t.Fatal("expected error on closed DB")
	}
}

// ── ListSubdirectories ─────────────────────────────────────────────────────

func TestListSubdirectories_ClosedConn(t *testing.T) {
	db := openTestDB(t)
	db.Close()
	_, err := db.ListSubdirectories("/")
	if err == nil {
		t.Fatal("expected error on closed DB")
	}
}

// ── GetAllProviders ─────────────────────────────────────────────────────────

func TestGetAllProviders_Empty(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	providers, err := db.GetAllProviders()
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 0 {
		t.Errorf("expected empty list, got %d", len(providers))
	}
}

func TestGetAllProviders_ClosedConn(t *testing.T) {
	db := openTestDB(t)
	db.Close()
	_, err := db.GetAllProviders()
	if err == nil {
		t.Fatal("expected error on closed DB")
	}
}

// ── GetFailedDeletions ──────────────────────────────────────────────────────

func TestGetFailedDeletions_HappyPath(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.InsertFailedDeletion("p1", "remote/path1", "network error")
	db.InsertFailedDeletion("p2", "remote/path2", "timeout")

	items, err := db.GetFailedDeletions(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
}

func TestGetFailedDeletions_ClosedConn(t *testing.T) {
	db := openTestDB(t)
	db.Close()
	_, err := db.GetFailedDeletions(10)
	if err == nil {
		t.Fatal("expected error on closed DB")
	}
}

// ── RecentActivity ──────────────────────────────────────────────────────────

func TestRecentActivity_HappyPath(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.InsertActivity("upload", "/docs/a.txt", "12 KB")
	db.InsertActivity("delete", "/docs/b.txt", "")

	entries, err := db.RecentActivity(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2, got %d", len(entries))
	}
	// Most recent first
	if entries[0].Action != "delete" {
		t.Errorf("expected delete first, got %s", entries[0].Action)
	}
}

func TestRecentActivity_DefaultLimit(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.InsertActivity("upload", "/a.txt", "")
	entries, err := db.RecentActivity(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1, got %d", len(entries))
	}
}

func TestRecentActivity_ClosedDB(t *testing.T) {
	db := openTestDB(t)
	db.Close()
	_, err := db.RecentActivity(10)
	if err == nil {
		t.Fatal("expected error on closed DB")
	}
}

// helper — opens a fresh test DB
func openTestDB(t *testing.T) *DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	return db
}
