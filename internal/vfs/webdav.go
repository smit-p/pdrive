package vfs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"
	"time"

	"github.com/smit-p/pdrive/internal/engine"
	"golang.org/x/net/webdav"
)

// WebDAVFS implements webdav.FileSystem backed by the pdrive engine.
type WebDAVFS struct {
	engine   *engine.Engine
	spoolDir string // persistent directory for in-progress upload spool files
}

// NewWebDAVFS creates a new WebDAV filesystem.
// spoolDir is a persistent directory (e.g. ~/.pdrive/spool) used for upload
// spool files so they survive a daemon restart and can be resumed.
// Pass an empty string to fall back to os.TempDir().
func NewWebDAVFS(eng *engine.Engine, spoolDir string) *WebDAVFS {
	return &WebDAVFS{engine: eng, spoolDir: spoolDir}
}

func cleanPath(name string) string {
	p := path.Clean(name)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// Mkdir creates a virtual directory.
func (fs *WebDAVFS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	name = cleanPath(name)
	slog.Debug("webdav mkdir", "name", name)
	return fs.engine.MkDir(name)
}

// OpenFile opens a file for reading or writing.
func (fs *WebDAVFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	name = cleanPath(name)
	slog.Debug("webdav open", "name", name, "flag", flag)

	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0 {
		return &webDAVFile{
			fs:       fs,
			name:     name,
			writable: true,
		}, nil
	}

	// Check if it's a directory.
	if name == "/" {
		return &webDAVFile{fs: fs, name: name, isDir: true}, nil
	}

	isDir, _ := fs.engine.IsDir(name)
	if isDir {
		return &webDAVFile{fs: fs, name: name, isDir: true}, nil
	}

	// Check if it's a file.
	file, err := fs.engine.Stat(name)
	if err != nil {
		return nil, err
	}
	if file == nil {
		return nil, os.ErrNotExist
	}

	return &webDAVFile{
		fs:      fs,
		name:    name,
		size:    file.SizeBytes,
		modTime: time.Unix(file.ModifiedAt, 0),
	}, nil
}

// RemoveAll removes a file or directory (recursively).
func (fs *WebDAVFS) RemoveAll(ctx context.Context, name string) error {
	name = cleanPath(name)
	slog.Debug("webdav remove", "name", name)

	isDir, _ := fs.engine.IsDir(name)
	if isDir {
		return fs.engine.DeleteDir(name)
	}
	return fs.engine.DeleteFile(name)
}

// Rename renames/moves a file or directory.
func (fs *WebDAVFS) Rename(ctx context.Context, oldName, newName string) error {
	oldName = cleanPath(oldName)
	newName = cleanPath(newName)
	slog.Debug("webdav rename", "old", oldName, "new", newName)

	isDir, _ := fs.engine.IsDir(oldName)
	if isDir {
		return fs.engine.RenameDir(oldName, newName)
	}

	// Rename is a pure metadata operation — the chunks stay in place on cloud.
	return fs.engine.RenameFile(oldName, newName)
}

// Stat returns file info.
func (fs *WebDAVFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	name = cleanPath(name)

	if name == "/" {
		return &dirInfo{name: "/"}, nil
	}

	file, err := fs.engine.Stat(name)
	if err != nil {
		return nil, err
	}
	if file != nil {
		return &fileInfo{
			name:    path.Base(file.VirtualPath),
			size:    file.SizeBytes,
			modTime: time.Unix(file.ModifiedAt, 0),
		}, nil
	}

	isDir, err := fs.engine.IsDir(name)
	if err != nil {
		return nil, err
	}
	if isDir {
		return &dirInfo{name: path.Base(name)}, nil
	}

	return nil, os.ErrNotExist
}

// webDAVFile implements webdav.File.
type webDAVFile struct {
	fs       *WebDAVFS
	name     string
	isDir    bool
	writable bool
	size     int64
	modTime  time.Time

	// Write state — uses a temp file instead of in-memory buffer.
	tmpFile   *os.File
	tmpPath   string
	writeSize int64

	// Read state — streams from a temp file to avoid holding the entire
	// file in memory. Lazily initialised on first Read or Seek.
	readFile *os.File
}

func (f *webDAVFile) Close() error {
	if f.writable && f.tmpFile != nil {
		if err := f.tmpFile.Sync(); err != nil {
			f.cleanup()
			return err
		}
		if _, err := f.tmpFile.Seek(0, io.SeekStart); err != nil {
			f.cleanup()
			return err
		}

		// Large files: upload in background so the WebDAV PUT returns quickly.
		// This prevents Finder from timing out on multi-minute cloud uploads.
		if f.writeSize > engine.AsyncWriteThreshold {
			slog.Info("async upload started", "path", f.name, "size", f.writeSize)
			err := f.fs.engine.WriteFileAsync(f.name, f.tmpFile, f.tmpPath, f.writeSize)
			f.tmpFile = nil // engine owns the file now
			return err
		}

		// Small files: synchronous — fast enough for Finder.
		err := f.fs.engine.WriteFileStream(f.name, f.tmpFile, f.writeSize)
		f.cleanup()
		return err
	}
	if f.readFile != nil {
		name := f.readFile.Name()
		f.readFile.Close()
		os.Remove(name)
		f.readFile = nil
	}
	return nil
}

func (f *webDAVFile) cleanup() {
	if f.tmpFile != nil {
		f.tmpFile.Close()
		os.Remove(f.tmpPath)
		f.tmpFile = nil
	}
}

func (f *webDAVFile) Read(p []byte) (int, error) {
	if f.isDir {
		return 0, fmt.Errorf("cannot read a directory")
	}
	if err := f.ensureReadFile(); err != nil {
		return 0, err
	}
	return f.readFile.Read(p)
}

// ensureReadFile lazily downloads the file to a temp file on first access.
func (f *webDAVFile) ensureReadFile() error {
	if f.readFile != nil {
		return nil
	}
	tmp, err := f.fs.engine.ReadFileToTempFile(f.name)
	if err != nil {
		return err
	}
	f.readFile = tmp
	if fi, err := tmp.Stat(); err == nil {
		f.size = fi.Size()
	}
	return nil
}

func (f *webDAVFile) Write(p []byte) (int, error) {
	if !f.writable {
		return 0, fmt.Errorf("file not opened for writing")
	}
	// Lazily create temp file on first write.
	if f.tmpFile == nil {
		spoolDir := f.fs.spoolDir // may be empty — falls back to os.TempDir()
		tmp, err := os.CreateTemp(spoolDir, "pdrive-upload-*")
		if err != nil {
			return 0, fmt.Errorf("creating temp file: %w", err)
		}
		f.tmpFile = tmp
		f.tmpPath = tmp.Name()
	}
	n, err := f.tmpFile.Write(p)
	f.writeSize += int64(n)
	return n, err
}

func (f *webDAVFile) Seek(offset int64, whence int) (int64, error) {
	if f.isDir {
		return 0, fmt.Errorf("cannot seek a directory")
	}
	if err := f.ensureReadFile(); err != nil {
		return 0, err
	}
	return f.readFile.Seek(offset, whence)
}

func (f *webDAVFile) Readdir(count int) ([]os.FileInfo, error) {
	if !f.isDir {
		return nil, fmt.Errorf("not a directory")
	}

	files, dirs, err := f.fs.engine.ListDir(f.name)
	if err != nil {
		return nil, err
	}

	var infos []os.FileInfo
	for _, d := range dirs {
		infos = append(infos, &dirInfo{name: d})
	}
	for _, file := range files {
		infos = append(infos, &fileInfo{
			name:    path.Base(file.VirtualPath),
			size:    file.SizeBytes,
			modTime: time.Unix(file.ModifiedAt, 0),
		})
	}

	if count > 0 && len(infos) > count {
		infos = infos[:count]
	}

	return infos, nil
}

func (f *webDAVFile) Stat() (os.FileInfo, error) {
	if f.isDir {
		return &dirInfo{name: path.Base(f.name)}, nil
	}
	return &fileInfo{
		name:    path.Base(f.name),
		size:    f.size,
		modTime: f.modTime,
	}, nil
}

// fileInfo implements os.FileInfo for files.
type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (fi *fileInfo) Name() string       { return fi.name }
func (fi *fileInfo) Size() int64        { return fi.size }
func (fi *fileInfo) Mode() os.FileMode  { return 0644 }
func (fi *fileInfo) ModTime() time.Time { return fi.modTime }
func (fi *fileInfo) IsDir() bool        { return false }
func (fi *fileInfo) Sys() interface{}   { return nil }

// dirInfo implements os.FileInfo for directories.
type dirInfo struct {
	name string
}

func (di *dirInfo) Name() string       { return di.name }
func (di *dirInfo) Size() int64        { return 0 }
func (di *dirInfo) Mode() os.FileMode  { return os.ModeDir | 0755 }
func (di *dirInfo) ModTime() time.Time { return time.Now() }
func (di *dirInfo) IsDir() bool        { return true }
func (di *dirInfo) Sys() interface{}   { return nil }
