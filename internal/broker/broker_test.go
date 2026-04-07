package broker

import (
	"testing"
	"time"

	"github.com/smit-p/pdrive/internal/metadata"
)

func ptr(v int64) *int64 { return &v }

func TestPFRDWeightedSelection(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()

	// Provider A: 10 GB free, Provider B: 90 GB free.
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "gdrive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(10e9),
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "dropbox", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(90e9),
	})

	b := NewBroker(db, PolicyPFRD, 0)

	counts := map[string]int{"a": 0, "b": 0}
	n := 10000
	for i := 0; i < n; i++ {
		id, err := b.AssignChunk(4 * 1024 * 1024) // 4 MB chunk
		if err != nil {
			t.Fatalf("AssignChunk failed: %v", err)
		}
		counts[id]++
	}

	// B (90 GB) should be picked ~9× more often than A (10 GB).
	ratioB := float64(counts["b"]) / float64(n)
	if ratioB < 0.80 || ratioB > 0.97 {
		t.Errorf("expected B to be picked ~90%% of the time, got %.1f%% (a=%d, b=%d)", ratioB*100, counts["a"], counts["b"])
	}
	t.Logf("pfrd distribution: A=%d (%.1f%%), B=%d (%.1f%%)", counts["a"], float64(counts["a"])/float64(n)*100, counts["b"], ratioB*100)
}

func TestMFSAlwaysPicksMostFree(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()

	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "gdrive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(10e9),
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "dropbox", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(90e9),
	})

	b := NewBroker(db, PolicyMFS, 0)

	for i := 0; i < 100; i++ {
		id, err := b.AssignChunk(4 * 1024 * 1024)
		if err != nil {
			t.Fatalf("AssignChunk failed: %v", err)
		}
		if id != "b" {
			t.Fatalf("expected MFS to always pick b, got %s", id)
		}
	}
}

func TestMinFreeSpaceFiltering(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()

	// A: 500 MB free, B: 100 MB free.
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "gdrive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(1e9), QuotaFreeBytes: ptr(500 * 1024 * 1024),
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "dropbox", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(1e9), QuotaFreeBytes: ptr(100 * 1024 * 1024),
	})

	// minFreeSpace = 256 MB. B (100 MB) should be excluded.
	b := NewBroker(db, PolicyPFRD, 256*1024*1024)

	for i := 0; i < 100; i++ {
		id, err := b.AssignChunk(4 * 1024 * 1024)
		if err != nil {
			t.Fatalf("AssignChunk failed: %v", err)
		}
		if id != "a" {
			t.Fatalf("expected only provider a (B should be filtered out), got %s", id)
		}
	}
}

func TestNoEligibleProviders(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()

	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "gdrive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(1e9), QuotaFreeBytes: ptr(10 * 1024 * 1024), // 10 MB
	})

	b := NewBroker(db, PolicyPFRD, 256*1024*1024) // minFreeSpace = 256 MB

	_, err := b.AssignChunk(4 * 1024 * 1024)
	if err != ErrNoSpace {
		t.Fatalf("expected ErrNoSpace, got %v", err)
	}
}

// ── Rate-limiting tests ─────────────────────────────────────────────────────

func TestRateLimitedProviderExcluded(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()

	future := time.Now().Unix() + 3600 // rate-limited for 1 hour
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "gdrive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(90e9),
		RateLimitedUntil: &future,
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "dropbox", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(10e9),
	})

	b := NewBroker(db, PolicyPFRD, 0)
	// All assignments should go to b since a is rate-limited.
	for i := 0; i < 100; i++ {
		id, err := b.AssignChunk(4 * 1024 * 1024)
		if err != nil {
			t.Fatalf("AssignChunk failed: %v", err)
		}
		if id != "b" {
			t.Fatalf("expected only b (a is rate-limited), got %s", id)
		}
	}
}

func TestExpiredRateLimitIncluded(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()

	past := time.Now().Unix() - 3600 // rate limit expired 1 hour ago
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "gdrive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(90e9),
		RateLimitedUntil: &past,
	})

	b := NewBroker(db, PolicyPFRD, 0)
	id, err := b.AssignChunk(4 * 1024 * 1024)
	if err != nil {
		t.Fatalf("AssignChunk: %v", err)
	}
	if id != "a" {
		t.Fatalf("expected a (rate limit expired), got %s", id)
	}
}

func TestAllProvidersRateLimited(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()

	future := time.Now().Unix() + 3600
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "gdrive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(90e9),
		RateLimitedUntil: &future,
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "dropbox", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(90e9),
		RateLimitedUntil: &future,
	})

	b := NewBroker(db, PolicyPFRD, 0)
	_, err := b.AssignChunk(4 * 1024 * 1024)
	if err != ErrNoSpace {
		t.Fatalf("expected ErrNoSpace when all providers rate-limited, got %v", err)
	}
}

// TestNewBroker_DefaultPolicy verifies that an empty policy defaults to PFRD.
func TestNewBroker_DefaultPolicy(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	b := NewBroker(db, "", 0)
	if b.policy != PolicyPFRD {
		t.Errorf("expected PolicyPFRD, got %q", b.policy)
	}
}

// ── EligibleFreeSpaces tests ────────────────────────────────────────────────

func TestEligibleFreeSpaces_Normal(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "drive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(50e9),
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "drive", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(30e9),
	})
	b := NewBroker(db, PolicyPFRD, 0)
	spaces, err := b.EligibleFreeSpaces()
	if err != nil {
		t.Fatal(err)
	}
	if len(spaces) != 2 {
		t.Fatalf("got %d spaces, want 2", len(spaces))
	}
}

func TestEligibleFreeSpaces_RateLimitedExcluded(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	future := time.Now().Unix() + 3600
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "drive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(50e9),
		RateLimitedUntil: &future,
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "drive", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(30e9),
	})
	b := NewBroker(db, PolicyPFRD, 0)
	spaces, err := b.EligibleFreeSpaces()
	if err != nil {
		t.Fatal(err)
	}
	if len(spaces) != 1 {
		t.Fatalf("got %d spaces, want 1 (rate-limited provider excluded)", len(spaces))
	}
	if spaces[0] != int64(30e9) {
		t.Errorf("got space %d, want %d", spaces[0], int64(30e9))
	}
}

func TestEligibleFreeSpaces_NilQuotaExcluded(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "drive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), // QuotaFreeBytes is nil
	})
	b := NewBroker(db, PolicyPFRD, 0)
	spaces, err := b.EligibleFreeSpaces()
	if err != nil {
		t.Fatal(err)
	}
	if len(spaces) != 0 {
		t.Fatalf("got %d spaces, want 0 (nil QuotaFreeBytes excluded)", len(spaces))
	}
}

func TestEligibleFreeSpaces_BelowThresholdExcluded(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "drive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(1e9), QuotaFreeBytes: ptr(100 * 1024 * 1024), // 100 MB
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "drive", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(1e9), QuotaFreeBytes: ptr(500 * 1024 * 1024), // 500 MB
	})
	b := NewBroker(db, PolicyPFRD, 256*1024*1024) // min 256 MB
	spaces, err := b.EligibleFreeSpaces()
	if err != nil {
		t.Fatal(err)
	}
	// Only provider b (500 MB) should pass; a (100 MB) <= 256 MB threshold
	if len(spaces) != 1 {
		t.Fatalf("got %d spaces, want 1", len(spaces))
	}
}

func TestEligibleFreeSpaces_NoProviders(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	b := NewBroker(db, PolicyPFRD, 0)
	spaces, err := b.EligibleFreeSpaces()
	if err != nil {
		t.Fatal(err)
	}
	if len(spaces) != 0 {
		t.Fatalf("got %d spaces, want 0", len(spaces))
	}
}

// ── TotalFreeSpace tests ────────────────────────────────────────────────────

func TestTotalFreeSpace_Normal(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "drive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(50e9),
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "drive", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(30e9),
	})
	b := NewBroker(db, PolicyPFRD, 0)
	total, err := b.TotalFreeSpace()
	if err != nil {
		t.Fatal(err)
	}
	want := int64(50e9) + int64(30e9)
	if total != want {
		t.Errorf("total = %d, want %d", total, want)
	}
}

func TestTotalFreeSpace_RateLimitedExcluded(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	future := time.Now().Unix() + 3600
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "drive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(50e9),
		RateLimitedUntil: &future,
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "drive", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(30e9),
	})
	b := NewBroker(db, PolicyPFRD, 0)
	total, err := b.TotalFreeSpace()
	if err != nil {
		t.Fatal(err)
	}
	// a is rate-limited, so only b's 30e9 is counted
	if total != int64(30e9) {
		t.Errorf("total = %d, want %d", total, int64(30e9))
	}
}

func TestTotalFreeSpace_NilQuotaSkipped(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "drive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), // QuotaFreeBytes nil
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "drive", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(30e9),
	})
	b := NewBroker(db, PolicyPFRD, 0)
	total, err := b.TotalFreeSpace()
	if err != nil {
		t.Fatal(err)
	}
	if total != int64(30e9) {
		t.Errorf("total = %d, want %d", total, int64(30e9))
	}
}

func TestTotalFreeSpace_NoProviders(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	b := NewBroker(db, PolicyPFRD, 0)
	total, err := b.TotalFreeSpace()
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
}

// ── pickPFRD edge case ──────────────────────────────────────────────────────

func TestPickPFRD_SingleCandidate(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "only", Type: "drive", DisplayName: "Only", RcloneRemote: "only:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(50e9),
	})
	b := NewBroker(db, PolicyPFRD, 0)
	for i := 0; i < 100; i++ {
		id, err := b.AssignChunk(1024)
		if err != nil {
			t.Fatal(err)
		}
		if id != "only" {
			t.Fatalf("expected 'only', got %s", id)
		}
	}
}

// TestPickPFRD_ZeroFreeSpace tests the totalFree <= 0 fallback in pickPFRD.
func TestPickPFRD_ZeroFreeSpace(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	zero := int64(0)
	db.UpsertProvider(&metadata.Provider{
		ID: "z", Type: "gdrive", DisplayName: "Z", RcloneRemote: "z:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: &zero,
	})
	b := NewBroker(db, PolicyPFRD, 0)
	// Call the public interface — eligible will return z (zero free space
	// passes since chunkSize=0 and minFreeSpace=0).
	id, err := b.AssignChunk(0)
	if err != nil {
		t.Fatal(err)
	}
	if id != "z" {
		t.Errorf("expected 'z', got %q", id)
	}
}

// TestEligible_MinFreeSpaceFilterOutAll verifies providers below minFreeSpace are skipped.
func TestEligible_MinFreeSpaceFilterOutAll(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "small", Type: "gdrive", DisplayName: "Small", RcloneRemote: "s:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(1e9),
	})
	b := NewBroker(db, PolicyPFRD, int64(50e9)) // minFreeSpace = 50 GB
	_, err := b.AssignChunk(1024)
	if err != ErrNoSpace {
		t.Fatalf("expected ErrNoSpace, got %v", err)
	}
}

// TestAssignChunk_PolicyMFS verifies MFS selects the provider with most free space.
func TestAssignChunk_PolicyMFS(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "gdrive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(10e9),
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "dropbox", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(90e9),
	})
	b := NewBroker(db, PolicyMFS, 0)
	id, err := b.AssignChunk(1024)
	if err != nil {
		t.Fatal(err)
	}
	if id != "b" {
		t.Errorf("MFS should pick 'b' (most free), got %q", id)
	}
}

// TestEligible_NilQuotaFreeBytes verifies providers without quota info are skipped.
func TestEligible_NilQuotaFreeBytes(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "noq", Type: "gdrive", DisplayName: "NoQuota", RcloneRemote: "n:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: nil,
	})
	b := NewBroker(db, PolicyPFRD, 0)
	_, err := b.AssignChunk(1024)
	if err != ErrNoSpace {
		t.Fatalf("expected ErrNoSpace for nil QuotaFreeBytes, got %v", err)
	}
}

// TestAssignChunk_DBError verifies that a closed DB triggers an error from eligible.
func TestAssignChunk_DBError(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	db.Close() // close immediately to trigger query error
	b := NewBroker(db, PolicyPFRD, 0)
	_, err := b.AssignChunk(1024)
	if err == nil {
		t.Fatal("expected error for closed DB")
	}
}

// ── EligibleFreeSpaces ──────────────────────────────────────────────────────

func TestEligibleFreeSpaces_Basic(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "gdrive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(50e9),
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "dropbox", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(30e9),
	})
	b := NewBroker(db, PolicyPFRD, 0)
	spaces, err := b.EligibleFreeSpaces()
	if err != nil {
		t.Fatal(err)
	}
	if len(spaces) != 2 {
		t.Fatalf("expected 2 spaces, got %d", len(spaces))
	}
}

func TestEligibleFreeSpaces_FiltersRateLimited(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	future := time.Now().Add(time.Hour).Unix()
	db.UpsertProvider(&metadata.Provider{
		ID: "rl", Type: "gdrive", DisplayName: "RL", RcloneRemote: "rl:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(50e9),
		RateLimitedUntil: &future,
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "ok", Type: "dropbox", DisplayName: "OK", RcloneRemote: "ok:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(30e9),
	})
	b := NewBroker(db, PolicyPFRD, 0)
	spaces, err := b.EligibleFreeSpaces()
	if err != nil {
		t.Fatal(err)
	}
	if len(spaces) != 1 {
		t.Fatalf("expected 1 space (rate-limited excluded), got %d", len(spaces))
	}
}

func TestEligibleFreeSpaces_FiltersNilQuota(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "nq", Type: "gdrive", DisplayName: "NQ", RcloneRemote: "nq:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: nil,
	})
	b := NewBroker(db, PolicyPFRD, 0)
	spaces, err := b.EligibleFreeSpaces()
	if err != nil {
		t.Fatal(err)
	}
	if len(spaces) != 0 {
		t.Errorf("expected 0 spaces for nil quota, got %d", len(spaces))
	}
}

func TestEligibleFreeSpaces_FiltersMinFreeSpace(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "small", Type: "gdrive", DisplayName: "Small", RcloneRemote: "s:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(1e9),
	})
	b := NewBroker(db, PolicyPFRD, int64(5e9))
	spaces, err := b.EligibleFreeSpaces()
	if err != nil {
		t.Fatal(err)
	}
	if len(spaces) != 0 {
		t.Errorf("expected 0 (below min free), got %d", len(spaces))
	}
}

func TestEligibleFreeSpaces_DBError(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	db.Close()
	b := NewBroker(db, PolicyPFRD, 0)
	_, err := b.EligibleFreeSpaces()
	if err == nil {
		t.Fatal("expected error for closed DB")
	}
}

// ── TotalFreeSpace ──────────────────────────────────────────────────────────

func TestTotalFreeSpace_Basic(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "a", Type: "gdrive", DisplayName: "A", RcloneRemote: "a:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(50e9),
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "b", Type: "dropbox", DisplayName: "B", RcloneRemote: "b:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(30e9),
	})
	b := NewBroker(db, PolicyPFRD, 0)
	total, err := b.TotalFreeSpace()
	if err != nil {
		t.Fatal(err)
	}
	expected := int64(80e9)
	if total != expected {
		t.Errorf("expected %d, got %d", expected, total)
	}
}

func TestTotalFreeSpace_ExcludesRateLimited(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	future := time.Now().Add(time.Hour).Unix()
	db.UpsertProvider(&metadata.Provider{
		ID: "rl", Type: "gdrive", DisplayName: "RL", RcloneRemote: "rl:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(50e9),
		RateLimitedUntil: &future,
	})
	db.UpsertProvider(&metadata.Provider{
		ID: "ok", Type: "dropbox", DisplayName: "OK", RcloneRemote: "ok:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: ptr(30e9),
	})
	b := NewBroker(db, PolicyPFRD, 0)
	total, err := b.TotalFreeSpace()
	if err != nil {
		t.Fatal(err)
	}
	if total != int64(30e9) {
		t.Errorf("expected 30GB (rate-limited excluded), got %d", total)
	}
}

func TestTotalFreeSpace_NilQuota(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	defer db.Close()
	db.UpsertProvider(&metadata.Provider{
		ID: "nq", Type: "gdrive", DisplayName: "NQ", RcloneRemote: "nq:",
		QuotaTotalBytes: ptr(100e9), QuotaFreeBytes: nil,
	})
	b := NewBroker(db, PolicyPFRD, 0)
	total, err := b.TotalFreeSpace()
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 {
		t.Errorf("expected 0 for nil quota, got %d", total)
	}
}

func TestTotalFreeSpace_DBError(t *testing.T) {
	db, _ := metadata.Open(":memory:")
	db.Close()
	b := NewBroker(db, PolicyPFRD, 0)
	_, err := b.TotalFreeSpace()
	if err == nil {
		t.Fatal("expected error for closed DB")
	}
}
