package vfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/smit-p/pdrive/internal/engine"
	"github.com/smit-p/pdrive/internal/junkfile"
	"github.com/smit-p/pdrive/internal/metadata"
)

// debounceDelay is how long we wait after the last write event before uploading.
// Finder writes large files in many small chunks; a 2-second quiet period
// reliably means the copy is done.
const debounceDelay = 2 * time.Second

// renameWindow is how long we hold a Remove event before treating it as a
// real deletion. If a Create of a file with the same size arrives within this
// window, we treat it as a rename (metadata-only, no re-upload).
const renameWindow = 500 * time.Millisecond

// recentRemoval holds info about a recently removed file for rename detection.
type recentRemoval struct {
	virtualPath string
	size        int64
	sha256Full  string
	timer       *time.Timer
}

// SyncDir watches a local directory and syncs changes to the cloud, Dropbox-style.
type SyncDir struct {
	root     string
	engine   *engine.Engine
	spoolDir string
	watcher  *fsnotify.Watcher

	pending map[string]*time.Timer
	mu      sync.Mutex

	// removals tracks recently removed files for rename detection.
	removals map[string]*recentRemoval // keyed by sha256+size

	// suppress watcher events caused by our own writes (downloads).
	// Value is the expiration time — events are suppressed until then.
	suppress map[string]time.Time
	supMu    sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

// NewSyncDir returns a new SyncDir that watches root and syncs to eng.
func NewSyncDir(root string, eng *engine.Engine, spoolDir string) *SyncDir {
	return &SyncDir{
		root:     root,
		engine:   eng,
		spoolDir: spoolDir,
		pending:  make(map[string]*time.Timer),
		removals: make(map[string]*recentRemoval),
		suppress: make(map[string]time.Time),
	}
}

// Start performs an initial sync, then watches for ongoing changes.
func (s *SyncDir) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	s.watcher = w

	if err := s.addWatchRecursive(s.root); err != nil {
		w.Close()
		return err
	}

	s.initialSync()

	go s.eventLoop()
	slog.Info("sync: watching local folder", "path", s.root)
	return nil
}

// Stop tears down the watcher and cancels pending uploads.
func (s *SyncDir) Stop() {
	if s.cancel == nil {
		return
	}
	s.cancel()
	if s.watcher != nil {
		s.watcher.Close()
	}
	s.mu.Lock()
	for _, t := range s.pending {
		t.Stop()
	}
	for _, r := range s.removals {
		r.timer.Stop()
	}
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// internal
// ---------------------------------------------------------------------------

func (s *SyncDir) addWatchRecursive(root string) error {
	return filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if shouldSkipDir(filepath.Base(p)) && p != root {
				return filepath.SkipDir
			}
			return s.watcher.Add(p)
		}
		return nil
	})
}

func (s *SyncDir) eventLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case ev, ok := <-s.watcher.Events:
			if !ok {
				return
			}
			s.handleEvent(ev)
		case err, ok := <-s.watcher.Errors:
			if !ok {
				return
			}
			slog.Error("sync: watcher error", "error", err)
		}
	}
}

func (s *SyncDir) handleEvent(ev fsnotify.Event) {
	absPath := ev.Name
	vp := s.virtualPath(absPath)
	if shouldSkipPath(vp) {
		return
	}

	// If we wrote this file ourselves (download), ignore Create/Write events.
	// Remove events are never suppressed — they always reflect user intent.
	if !ev.Has(fsnotify.Remove) && !ev.Has(fsnotify.Rename) {
		s.supMu.Lock()
		expiry, suppressed := s.suppress[absPath]
		if suppressed {
			if time.Now().After(expiry) {
				delete(s.suppress, absPath)
				suppressed = false
			}
		}
		s.supMu.Unlock()
		if suppressed {
			return
		}
	}

	switch {
	case ev.Has(fsnotify.Create):
		info, err := os.Stat(absPath)
		if err != nil {
			return
		}
		if info.IsDir() {
			if shouldSkipDir(filepath.Base(absPath)) {
				return
			}
			s.watcher.Add(absPath)
			s.engine.MkDir(vp + "/")
			slog.Info("sync: dir created", "path", vp)
			// Scan for files that landed before the watcher was added.
			s.scanDir(absPath)
			return
		}

		// Rename detection: check if a recent removal matches this file's
		// size AND content hash. Matching on size alone would silently
		// serve wrong content when two files happen to have the same size.
		localHash, hashErr := hashLocalFile(absPath)
		s.mu.Lock()
		matched := false
		if hashErr == nil {
			key := localHash + fmt.Sprintf(":%d", info.Size())
			if rem, ok := s.removals[key]; ok {
				rem.timer.Stop()
				delete(s.removals, key)
				s.mu.Unlock()
				// Perform metadata-only rename instead of delete + re-upload.
				if err := s.engine.RenameFile(rem.virtualPath, vp); err != nil {
					slog.Warn("sync: rename failed, falling back to upload",
						"old", rem.virtualPath, "new", vp, "error", err)
					s.debounce(absPath, vp)
				} else {
					slog.Info("sync: renamed (metadata-only)", "old", rem.virtualPath, "new", vp)
				}
				matched = true
			}
		}
		if !matched {
			s.mu.Unlock()
		}
		if !matched {
			s.debounce(absPath, vp)
		}

	case ev.Has(fsnotify.Write):
		s.debounce(absPath, vp)

	case ev.Has(fsnotify.Remove), ev.Has(fsnotify.Rename):
		s.mu.Lock()
		if t, ok := s.pending[absPath]; ok {
			t.Stop()
			delete(s.pending, absPath)
		}
		s.mu.Unlock()

		// For files, defer the delete to allow rename detection.
		if isDir, _ := s.engine.IsDir(vp + "/"); isDir {
			s.engine.DeleteDir(vp + "/")
			slog.Info("sync: dir removed", "path", vp)
		} else if existing, _ := s.engine.Stat(vp); existing != nil {
			key := existing.SHA256Full + fmt.Sprintf(":%d", existing.SizeBytes)
			s.mu.Lock()
			s.removals[key] = &recentRemoval{
				virtualPath: vp,
				size:        existing.SizeBytes,
				sha256Full:  existing.SHA256Full,
				timer: time.AfterFunc(renameWindow, func() {
					s.mu.Lock()
					delete(s.removals, key)
					s.mu.Unlock()
					s.engine.DeleteFile(vp)
					slog.Info("sync: file removed", "path", vp)
				}),
			}
			s.mu.Unlock()
		} else {
			slog.Info("sync: file removed (not in db)", "path", vp)
		}
	}
}

// scanDir walks a newly-created directory and debounces any files found.
// This handles the race where Finder copies files into a directory before
// fsnotify has started watching it.
func (s *SyncDir) scanDir(dir string) {
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == dir {
			return nil
		}
		if d.IsDir() {
			if shouldSkipDir(filepath.Base(path)) {
				return fs.SkipDir
			}
			s.watcher.Add(path)
			vp, _ := filepath.Rel(s.root, path)
			vp = "/" + vp
			s.engine.MkDir(vp + "/")
			return nil
		}
		vp, _ := filepath.Rel(s.root, path)
		vp = "/" + vp
		if shouldSkipPath(vp) {
			return nil
		}
		s.debounce(path, vp)
		return nil
	})
}

func (s *SyncDir) debounce(absPath, vp string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.pending[absPath]; ok {
		t.Stop()
	}
	s.pending[absPath] = time.AfterFunc(debounceDelay, func() {
		s.mu.Lock()
		delete(s.pending, absPath)
		s.mu.Unlock()
		s.upload(absPath, vp)
	})
}

func (s *SyncDir) upload(absPath, vp string) {
	info, err := os.Stat(absPath)
	if err != nil || info.IsDir() {
		return
	}
	size := info.Size()

	// Never upload stub files — they're placeholders for cloud-only content.
	if isStubFile(absPath) {
		return
	}

	// Register immediately so the UI shows "Preparing…" while we hash/spool.
	queueKey := s.engine.RegisterQueuedUpload(vp, size)

	// Skip if already uploaded with same content (hash-based dedup).
	if existing, _ := s.engine.Stat(vp); existing != nil && existing.SizeBytes == size {
		if localHash, err := hashLocalFile(absPath); err == nil && localHash == existing.SHA256Full {
			s.engine.UnregisterQueuedUpload(queueKey)
			return
		}
	}

	if size > engine.AsyncWriteThreshold {
		// Large file — copy to spool then upload in background.
		tmp, err := os.CreateTemp(s.spoolDir, "pdrive-sync-*")
		if err != nil {
			s.engine.UnregisterQueuedUpload(queueKey)
			slog.Error("sync: spool create failed", "path", vp, "error", err)
			return
		}
		src, err := os.Open(absPath)
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			s.engine.UnregisterQueuedUpload(queueKey)
			slog.Error("sync: open failed", "path", vp, "error", err)
			return
		}
		n, cpErr := io.Copy(tmp, src)
		src.Close()
		if cpErr != nil || n != size {
			tmp.Close()
			os.Remove(tmp.Name())
			s.engine.UnregisterQueuedUpload(queueKey)
			slog.Error("sync: spool copy failed", "path", vp, "error", cpErr)
			return
		}
		tmp.Seek(0, io.SeekStart)
		// WriteFileAsync adopts the queued entry — don't unregister here.
		if err := s.engine.WriteFileAsync(vp, tmp, tmp.Name(), size); err != nil {
			slog.Error("sync: async upload failed", "path", vp, "error", err)
			return
		}
		slog.Info("sync: upload started", "path", vp, "size", size)
	} else {
		// Small file — synchronous upload; remove queued entry when done.
		s.engine.UnregisterQueuedUpload(queueKey)
		data, err := os.ReadFile(absPath)
		if err != nil {
			slog.Error("sync: read failed", "path", vp, "error", err)
			return
		}
		if err := s.engine.WriteFile(vp, data); err != nil {
			slog.Error("sync: upload failed", "path", vp, "error", err)
			return
		}
		slog.Info("sync: uploaded", "path", vp, "size", size)
	}
}

// ---------------------------------------------------------------------------
// initial sync
// ---------------------------------------------------------------------------

func (s *SyncDir) initialSync() {
	slog.Info("sync: initial sync starting", "root", s.root)

	// 1. Upload local files that are not yet in the cloud (skip stubs).
	filepath.Walk(s.root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // best-effort
		}
		if info.IsDir() {
			if shouldSkipDir(filepath.Base(p)) && p != s.root {
				return filepath.SkipDir
			}
			return nil
		}
		if isStubFile(p) {
			return nil // don't upload stubs
		}
		vp := s.virtualPath(p)
		if shouldSkipPath(vp) {
			return nil
		}
		existing, _ := s.engine.Stat(vp)
		if existing == nil {
			slog.Info("sync: uploading local-only file", "path", vp)
			s.upload(p, vp)
		}
		return nil
	})

	// 2. Download cloud-only files to the local folder.
	s.downloadMissing("/")

	slog.Info("sync: initial sync complete")
}

func (s *SyncDir) downloadMissing(dirPath string) {
	files, dirs, err := s.engine.ListDir(dirPath)
	if err != nil {
		return
	}

	for _, d := range dirs {
		sub := dirPath + d + "/"
		localDir := filepath.Join(s.root, sub)
		os.MkdirAll(localDir, 0755)
		s.downloadMissing(sub)
	}

	for _, f := range files {
		if shouldSkipPath(f.VirtualPath) {
			continue
		}
		if f.UploadState != "complete" {
			continue
		}
		localPath := filepath.Join(s.root, f.VirtualPath)
		if _, err := os.Stat(localPath); err == nil {
			continue // already present locally (real file or stub)
		}
		// Create a stub (0-byte placeholder) instead of downloading.
		// The user can "pin" files to download them on demand.
		os.MkdirAll(filepath.Dir(localPath), 0755)
		s.suppressEvent(localPath)
		if err := createStubFile(localPath, f.SizeBytes); err != nil {
			slog.Error("sync: stub creation failed", "path", f.VirtualPath, "error", err)
			continue
		}
		slog.Info("sync: stub created", "path", f.VirtualPath, "size", f.SizeBytes)
	}
}

// PinFile downloads a cloud-only file to the local folder, replacing the stub.
// Uses streaming to avoid holding the entire file in memory.
func (s *SyncDir) PinFile(virtualPath string) error {
	localPath := filepath.Join(s.root, virtualPath)

	// Stream from cloud to a temp file.
	tmp, err := s.engine.ReadFileToTempFile(virtualPath)
	if err != nil {
		return fmt.Errorf("downloading %s: %w", virtualPath, err)
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()

	os.MkdirAll(filepath.Dir(localPath), 0755)
	s.suppressEvent(localPath)

	// Write to a temp file in the same directory, then atomically rename.
	// This prevents a partial file on crash from being treated as real data.
	dst, err := os.CreateTemp(filepath.Dir(localPath), ".pdrive-pin-*")
	if err != nil {
		return fmt.Errorf("creating temp file for %s: %w", virtualPath, err)
	}
	tmpName := dst.Name()
	n, copyErr := io.Copy(dst, tmp)
	if closeErr := dst.Close(); closeErr != nil && copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		os.Remove(tmpName)
		return fmt.Errorf("writing %s: %w", virtualPath, copyErr)
	}
	if err := os.Rename(tmpName, localPath); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("renaming temp to %s: %w", virtualPath, err)
	}

	clearStubMarker(localPath)
	slog.Info("sync: pinned (downloaded)", "path", virtualPath, "size", n)
	return nil
}

// UnpinFile removes local data and replaces it with a stub.
func (s *SyncDir) UnpinFile(virtualPath string) error {
	localPath := filepath.Join(s.root, virtualPath)

	// Verify the file exists in the cloud DB.
	f, err := s.engine.Stat(virtualPath)
	if err != nil || f == nil {
		return fmt.Errorf("file not found in cloud: %s", virtualPath)
	}
	if f.UploadState != "complete" {
		return fmt.Errorf("file still uploading: %s", virtualPath)
	}

	// Replace local file with a stub.
	s.suppressEvent(localPath)
	if err := createStubFile(localPath, f.SizeBytes); err != nil {
		return fmt.Errorf("creating stub for %s: %w", virtualPath, err)
	}

	slog.Info("sync: unpinned (evicted local data)", "path", virtualPath, "size", f.SizeBytes)
	return nil
}

// IsStub returns true if the local copy of virtualPath is a cloud-only stub.
func (s *SyncDir) IsStub(virtualPath string) bool {
	localPath := filepath.Join(s.root, virtualPath)
	return isStubFile(localPath)
}

// Root returns the local sync directory path.
func (s *SyncDir) Root() string {
	return s.root
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func (s *SyncDir) virtualPath(absPath string) string {
	rel, _ := filepath.Rel(s.root, absPath)
	return "/" + filepath.ToSlash(rel)
}

func (s *SyncDir) suppressEvent(absPath string) {
	s.supMu.Lock()
	s.suppress[absPath] = time.Now().Add(2 * time.Second)
	s.supMu.Unlock()
}

func shouldSkipPath(vp string) bool {
	base := filepath.Base(vp)
	return junkfile.IsOSJunk(base) ||
		base == ".pdrive" ||
		strings.HasPrefix(base, ".pdrive-")
}

func shouldSkipDir(name string) bool {
	switch name {
	case ".pdrive", ".Trash", ".Trashes", ".Spotlight-V100", ".fseventsd":
		return true
	}
	return false
}

// hashLocalFile computes the SHA256 hash of a local file.
func hashLocalFile(absPath string) (string, error) {
	f, err := os.Open(absPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ListFiles returns all complete files (used for reconciliation/testing).
func (s *SyncDir) ListFiles() []metadata.File {
	files, _, _ := s.engine.ListDir("/")
	return files
}
