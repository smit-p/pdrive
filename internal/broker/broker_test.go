package broker

import (
	"testing"

	"github.com/smit-p/pdrive/internal/metadata"
)

// mockDB implements a minimal in-memory provider store for broker tests.
type mockDB struct {
	providers []metadata.Provider
}

func ptr(v int64) *int64 { return &v }

func (m *mockDB) GetAllProviders() ([]metadata.Provider, error) {
	return m.providers, nil
}

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
