package daemon

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/metadata"
)

// ---------------------------------------------------------------------------
// enhanced fake RC server that supports file operations for recovery tests
// ---------------------------------------------------------------------------

// fakeRCFiles maps "remote:path" → file contents for GetFile / copyfile.
// fakeRCDirs maps "remote:dir" → list items for ListDir / operations/list.
type recoveryRCServer struct {
	remotes map[string]string           // name → type
	files   map[string][]byte           // "remote:path" → content
	dirs    map[string][]map[string]any // "remote:dir" → [{Name, IsDir, ...}, ...]
}

func newRecoveryRCServer(t *testing.T, cfg recoveryRCServer) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)

		switch r.URL.Path {
		case "/config/listremotes":
			names := make([]string, 0, len(cfg.remotes))
			for name := range cfg.remotes {
				names = append(names, name)
			}
			json.NewEncoder(w).Encode(map[string]any{"remotes": names})

		case "/config/get":
			name, _ := body["name"].(string)
			typ, ok := cfg.remotes[name]
			if !ok {
				http.Error(w, "not found", 404)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"type": typ})

		case "/operations/about":
			json.NewEncoder(w).Encode(map[string]any{"total": int64(100e9), "free": int64(50e9)})

		case "/operations/copyfile":
			// Simulate GetFile: copy "srcFs:srcRemote" to "dstFs/dstRemote"
			srcFs, _ := body["srcFs"].(string)
			srcRemote, _ := body["srcRemote"].(string)
			dstFs, _ := body["dstFs"].(string)
			dstRemote, _ := body["dstRemote"].(string)

			remote := strings.TrimSuffix(srcFs, ":")
			key := remote + ":" + srcRemote
			data, ok := cfg.files[key]
			if !ok {
				http.Error(w, "file not found: "+key, 404)
				return
			}
			// Write file to local filesystem (the "download")
			dstPath := filepath.Join(strings.TrimSuffix(dstFs, "/"), dstRemote)
			os.MkdirAll(filepath.Dir(dstPath), 0700)
			if err := os.WriteFile(dstPath, data, 0600); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			// Support async mode: return a jobid so waitForJob can poll.
			if _, isAsync := body["_async"]; isAsync {
				json.NewEncoder(w).Encode(map[string]any{"jobid": 1})
			} else {
				json.NewEncoder(w).Encode(map[string]any{})
			}

		case "/job/status":
			// All jobs complete immediately in tests.
			json.NewEncoder(w).Encode(map[string]any{
				"finished": true,
				"success":  true,
			})

		case "/operations/list":
			fs, _ := body["fs"].(string)
			remotePath, _ := body["remote"].(string)
			remote := strings.TrimSuffix(fs, ":")
			key := remote + ":" + remotePath
			items, ok := cfg.dirs[key]
			if !ok {
				// Empty directory
				json.NewEncoder(w).Encode(map[string]any{"list": []any{}})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"list": items})

		case "/core/version":
			json.NewEncoder(w).Encode(map[string]any{"version": "v1.0.0"})

		default:
			json.NewEncoder(w).Encode(map[string]any{})
		}
	}))
}

func newRecoveryDaemon(t *testing.T, srv *httptest.Server) (*Daemon, *metadata.DB) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	addr := strings.TrimPrefix(srv.URL, "http://")
	rm := NewRcloneManager("", "", addr)

	d := &Daemon{
		config: Config{
			ConfigDir: dir,
		},
		db:     db,
		rclone: rm,
	}
	return d, db
}

// makeTestBackupPayload creates a realistic backup payload (magic + timestamp + data).
func makeTestBackupPayload(t *testing.T, dbData []byte) []byte {
	t.Helper()
	hdr := make([]byte, 16)
	copy(hdr[:8], engine.BackupMagic[:])
	binary.BigEndian.PutUint64(hdr[8:16], uint64(1700000000000000000)) // fixed timestamp
	return append(hdr, dbData...)
}

// ---------------------------------------------------------------------------
// tryDownloadBackup tests
// ---------------------------------------------------------------------------

func TestTryDownloadBackup_Success(t *testing.T) {
	dbData := []byte("CREATE TABLE test;")
	payload := makeTestBackupPayload(t, dbData)

	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files: map[string][]byte{
			"gdrive:pdrive-meta/metadata.db": payload,
		},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)

	data, ts, ok := d.tryDownloadBackup("gdrive")
	if !ok {
		t.Fatal("tryDownloadBackup returned !ok")
	}
	if ts != 1700000000000000000 {
		t.Errorf("timestamp = %d, want 1700000000000000000", ts)
	}
	if string(data) != string(dbData) {
		t.Errorf("dbData = %q, want %q", string(data), string(dbData))
	}
}

func TestTryDownloadBackup_NoFile(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files:   map[string][]byte{},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)

	_, _, ok := d.tryDownloadBackup("gdrive")
	if ok {
		t.Error("tryDownloadBackup should return false for missing file")
	}
}

func TestTryDownloadBackup_CorruptPayload(t *testing.T) {
	// Bad magic header
	badPayload := []byte("not a valid backup payload that is at least 16 bytes")

	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files: map[string][]byte{
			"gdrive:pdrive-meta/metadata.db": badPayload,
		},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)

	_, _, ok := d.tryDownloadBackup("gdrive")
	if ok {
		t.Error("tryDownloadBackup should return false for corrupt payload")
	}
}

// ---------------------------------------------------------------------------
// tryRestoreDB tests
// ---------------------------------------------------------------------------

func TestTryRestoreDB_Backup(t *testing.T) {
	dbData := []byte("restored database content")
	payload := makeTestBackupPayload(t, dbData)

	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files: map[string][]byte{
			"gdrive:pdrive-meta/metadata.db": payload,
		},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)

	dbPath := filepath.Join(t.TempDir(), "restored.db")
	if !d.tryRestoreDB(dbPath) {
		t.Fatal("tryRestoreDB returned false")
	}

	got, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("reading restored db: %v", err)
	}
	if string(got) != string(dbData) {
		t.Errorf("restored data = %q, want %q", string(got), string(dbData))
	}
}

func TestTryRestoreDB_NoBackup(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		files:   map[string][]byte{},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)

	dbPath := filepath.Join(t.TempDir(), "restored.db")
	if d.tryRestoreDB(dbPath) {
		t.Error("tryRestoreDB should return false when no backup exists")
	}
}

func TestTryRestoreDB_PicksNewest(t *testing.T) {
	oldData := []byte("old database")
	newData := []byte("new database")

	oldPayload := make([]byte, 16+len(oldData))
	copy(oldPayload[:8], engine.BackupMagic[:])
	binary.BigEndian.PutUint64(oldPayload[8:16], 1000) // older
	copy(oldPayload[16:], oldData)

	newPayload := make([]byte, 16+len(newData))
	copy(newPayload[:8], engine.BackupMagic[:])
	binary.BigEndian.PutUint64(newPayload[8:16], 2000) // newer
	copy(newPayload[16:], newData)

	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"old-remote": "drive", "new-remote": "drive"},
		files: map[string][]byte{
			"old-remote:pdrive-meta/metadata.db": oldPayload,
			"new-remote:pdrive-meta/metadata.db": newPayload,
		},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)

	dbPath := filepath.Join(t.TempDir(), "restored.db")
	if !d.tryRestoreDB(dbPath) {
		t.Fatal("tryRestoreDB returned false")
	}

	got, _ := os.ReadFile(dbPath)
	if string(got) != string(newData) {
		t.Errorf("restored data = %q, want %q (the newer backup)", string(got), string(newData))
	}
}

func TestTryRestoreDB_NoRemotes(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{},
		files:   map[string][]byte{},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)

	dbPath := filepath.Join(t.TempDir(), "restored.db")
	if d.tryRestoreDB(dbPath) {
		t.Error("tryRestoreDB should return false with no remotes")
	}
}

// ---------------------------------------------------------------------------
// validateRestoredDB tests
// ---------------------------------------------------------------------------

func TestValidateRestoredDB_EmptyDB(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
	})
	defer srv.Close()

	d, _ := newRecoveryDaemon(t, srv)

	// Empty DB (no chunk_locations) should be valid
	if !d.validateRestoredDB() {
		t.Error("validateRestoredDB should return true for empty DB")
	}
}

func insertTestChunkLocation(t *testing.T, db *metadata.DB, providerID, providerType, remotePath string) {
	t.Helper()
	total, free := int64(100e9), int64(50e9)
	db.UpsertProvider(&metadata.Provider{
		ID: providerID, Type: providerType, DisplayName: providerID,
		RcloneRemote: providerID, QuotaTotalBytes: &total, QuotaFreeBytes: &free,
	})

	if _, err := db.Conn().Exec(`INSERT INTO files (id, virtual_path, size_bytes, sha256_full, upload_state, created_at, modified_at)
		VALUES ('f1', '/test.txt', 1024, 'abc', 'complete', 1700000000, 1700000000)`); err != nil {
		t.Fatalf("inserting file: %v", err)
	}
	if _, err := db.Conn().Exec(`INSERT INTO chunks (id, file_id, sequence, size_bytes, cloud_size, sha256)
		VALUES ('c1', 'f1', 0, 1024, 1040, 'abc')`); err != nil {
		t.Fatalf("inserting chunk: %v", err)
	}
	if _, err := db.Conn().Exec(`INSERT INTO chunk_locations (chunk_id, provider_id, remote_path)
		VALUES ('c1', ?, ?)`+``, providerID, remotePath); err != nil {
		t.Fatalf("inserting chunk_location: %v", err)
	}

	// Verify
	var count int
	db.Conn().QueryRow("SELECT COUNT(*) FROM chunk_locations").Scan(&count)
	if count == 0 {
		t.Fatal("chunk_locations INSERT did not stick")
	}
}

func TestValidateRestoredDB_ChunksExistOnCloud(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		dirs: map[string][]map[string]any{
			"gdrive:chunks/ab": {
				{"Name": "abcdef123456.chunk", "IsDir": false, "Size": 1024},
			},
		},
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)
	insertTestChunkLocation(t, db, "gdrive", "drive", "chunks/ab/abcdef123456.chunk")

	if !d.validateRestoredDB() {
		t.Error("validateRestoredDB should return true when chunks exist on cloud")
	}
}

func TestValidateRestoredDB_ChunksMissingOnCloud(t *testing.T) {
	srv := newRecoveryRCServer(t, recoveryRCServer{
		remotes: map[string]string{"gdrive": "drive"},
		dirs:    map[string][]map[string]any{}, // empty dirs — chunks don't exist
	})
	defer srv.Close()

	d, db := newRecoveryDaemon(t, srv)
	insertTestChunkLocation(t, db, "gdrive", "drive", "chunks/ab/missing.chunk")

	if d.validateRestoredDB() {
		t.Error("validateRestoredDB should return false when chunks are missing from cloud")
	}
}
