package vfs

import (
	"context"
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
	"github.com/smit-p/pdrive/internal/metadata"
)

// debounceDelay is how long we wait after the last write event before uploading.
// Finder writes large files in many small chunks; a 2-second quiet period
// reliably means the copy is done.
const debounceDelay = 2 * time.Second

// SyncDir watches a local directory and syncs changes to the cloud, Dropbox-style.
type SyncDir struct {
	root     string
	engine   *engine.Engine
	spoolDir string
	watcher  *fsnotify.Watcher

	pending map[string]*time.Timer
	mu      sync.Mutex

	// suppress watcher events caused by our own writes (downloads).
	suppress map[string]struct{}
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
		suppress: make(map[string]struct{}),
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
	s.cancel()
	if s.watcher != nil {
		s.watcher.Close()
	}
	s.mu.Lock()
	for _, t := range s.pending {
		t.Stop()
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

	// If we wrote this file ourselves (download), ignore the event.
	s.supMu.Lock()
	_, suppressed := s.suppress[absPath]
	if suppressed {
		delete(s.suppress, absPath)
	}
	s.supMu.Unlock()
	if suppressed {
		return
	}

	switch {
	case ev.Has(fsnotify.Create):
		info, err := os.Stat(absPath)
		if err != nil {
			return
		}
		if info.IsDir() {
			s.watcher.Add(absPath)
			s.engine.MkDir(vp + "/")
			slog.Info("sync: dir created", "path", vp)
			// Scan for files that landed before the watcher was added.
			s.scanDir(absPath)
			return
		}
		s.debounce(absPath, vp)

	case ev.Has(fsnotify.Write):
		s.debounce(absPath, vp)

	case ev.Has(fsnotify.Remove), ev.Has(fsnotify.Rename):
		s.mu.Lock()
		if t, ok := s.pending[absPath]; ok {
			t.Stop()
			delete(s.pending, absPath)
		}
		s.mu.Unlock()

		if isDir, _ := s.engine.IsDir(vp + "/"); isDir {
			s.engine.DeleteDir(vp + "/")
			slog.Info("sync: dir removed", "path", vp)
		} else {
			s.engine.DeleteFile(vp)
			slog.Info("sync: file removed", "path", vp)
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

	// Skip if already uploaded with same size (cheap dedup).
	if existing, _ := s.engine.Stat(vp); existing != nil && existing.SizeBytes == size {
		return
	}

	if size > engine.AsyncWriteThreshold {
		// Large file — copy to spool then upload in background.
		tmp, err := os.CreateTemp(s.spoolDir, "pdrive-sync-*")
		if err != nil {
			slog.Error("sync: spool create failed", "path", vp, "error", err)
			return
		}
		src, err := os.Open(absPath)
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			slog.Error("sync: open failed", "path", vp, "error", err)
			return
		}
		n, cpErr := io.Copy(tmp, src)
		src.Close()
		if cpErr != nil || n != size {
			tmp.Close()
			os.Remove(tmp.Name())
			slog.Error("sync: spool copy failed", "path", vp, "error", cpErr)
			return
		}
		tmp.Seek(0, io.SeekStart)
		if err := s.engine.WriteFileAsync(vp, tmp, tmp.Name(), size); err != nil {
			slog.Error("sync: async upload failed", "path", vp, "error", err)
			return
		}
		slog.Info("sync: upload started", "path", vp, "size", size)
	} else {
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

	// 1. Upload local files that are not yet in the cloud.
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
			continue // already present locally
		}
		slog.Info("sync: downloading", "path", f.VirtualPath, "size", f.SizeBytes)
		data, err := s.engine.ReadFile(f.VirtualPath)
		if err != nil {
			slog.Error("sync: download failed", "path", f.VirtualPath, "error", err)
			continue
		}
		os.MkdirAll(filepath.Dir(localPath), 0755)
		s.suppressEvent(localPath)
		if err := os.WriteFile(localPath, data, 0644); err != nil {
			slog.Error("sync: write failed", "path", f.VirtualPath, "error", err)
		}
	}
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
	s.suppress[absPath] = struct{}{}
	s.supMu.Unlock()
}

func shouldSkipPath(vp string) bool {
	base := filepath.Base(vp)
	return base == ".DS_Store" ||
		strings.HasPrefix(base, "._") ||
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

// ListFiles returns all complete files (used for reconciliation/testing).
func (s *SyncDir) ListFiles() []metadata.File {
	files, _, _ := s.engine.ListDir("/")
	return files
}
