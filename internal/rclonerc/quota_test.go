package rclonerc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewQuotaCache(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {})
	qc := NewQuotaCache(c)
	if qc == nil {
		t.Fatal("expected non-nil QuotaCache")
	}
	if qc.client != c {
		t.Error("expected client to match")
	}
	if qc.cache == nil {
		t.Error("expected non-nil cache map")
	}
}

func TestGetQuota_FetchesAndCaches(t *testing.T) {
	calls := 0
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		total := int64(1000)
		used := int64(400)
		free := int64(600)
		json.NewEncoder(w).Encode(map[string]*int64{
			"total": &total,
			"used":  &used,
			"free":  &free,
		})
	})
	qc := NewQuotaCache(c)

	info, err := qc.GetQuota("drive")
	if err != nil {
		t.Fatal(err)
	}
	if info.Total != 1000 || info.Used != 400 || info.Free != 600 {
		t.Errorf("unexpected quota: %+v", info)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}

	// Second call should use cache.
	info2, err := qc.GetQuota("drive")
	if err != nil {
		t.Fatal(err)
	}
	if info2 != info {
		t.Errorf("expected cached result, got %+v", info2)
	}
	if calls != 1 {
		t.Errorf("expected still 1 call (cached), got %d", calls)
	}
}

func TestGetQuota_NilFields(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		// Return JSON with no total/used/free fields.
		w.Write([]byte(`{}`))
	})
	qc := NewQuotaCache(c)

	info, err := qc.GetQuota("drive")
	if err != nil {
		t.Fatal(err)
	}
	if info.Total != 0 || info.Used != 0 || info.Free != 0 {
		t.Errorf("expected all zero for nil fields, got %+v", info)
	}
}

func TestGetQuota_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`server error`))
	})
	qc := NewQuotaCache(c)

	_, err := qc.GetQuota("drive")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetQuota_BadJSON(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`not-json`))
	})
	qc := NewQuotaCache(c)

	_, err := qc.GetQuota("drive")
	if err == nil {
		t.Fatal("expected error for bad JSON")
	}
}

func TestInvalidate(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		total := int64(100)
		json.NewEncoder(w).Encode(map[string]*int64{"total": &total})
	})
	qc := NewQuotaCache(c)

	// Populate cache.
	qc.GetQuota("drive")

	// Invalidate.
	qc.Invalidate("drive")

	// Should not find in cache.
	qc.mu.RLock()
	_, ok := qc.cache["drive"]
	qc.mu.RUnlock()
	if ok {
		t.Error("expected cache entry to be removed after Invalidate")
	}
}

func TestGetQuota_ExpiredCache(t *testing.T) {
	calls := 0
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		total := int64(int64(calls) * 100)
		json.NewEncoder(w).Encode(map[string]*int64{"total": &total})
	})
	qc := NewQuotaCache(c)

	// First fetch.
	qc.GetQuota("drive")
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	// Manually expire the cache entry.
	qc.mu.Lock()
	entry := qc.cache["drive"]
	entry.fetchedAt = time.Now().Add(-quotaCacheTTL - time.Second)
	qc.cache["drive"] = entry
	qc.mu.Unlock()

	// Should re-fetch.
	info, err := qc.GetQuota("drive")
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls after expiry, got %d", calls)
	}
	if info.Total != 200 {
		t.Errorf("expected updated total 200, got %d", info.Total)
	}
}

func TestGetQuota_VerifiesRemoteColon(t *testing.T) {
	var gotFS string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		if fs, ok := body["fs"]; ok {
			gotFS, _ = fs.(string)
		}
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := NewClient(strings.TrimPrefix(srv.URL, "http://"))
	qc := NewQuotaCache(c)

	qc.GetQuota("myremote")
	if gotFS != "myremote:" {
		t.Errorf("expected fs=myremote:, got %q", gotFS)
	}
}
