package fusefs

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/smit-p/pdrive/internal/engine"
)

// attrTimeout is the default TTL for kernel-cached attribute data.
// A short timeout is used because files can change on the cloud side.
var attrTimeout = 5 * time.Second

// Root is the top-level FUSE inode. All files and directories in the
// pdrive virtual filesystem appear as children of this node.
type Root struct {
	fs.Inode
	eng      *engine.Engine
	spoolDir string
}

var (
	_ fs.InodeEmbedder = (*Root)(nil)
	_ fs.NodeLookuper  = (*Root)(nil)
	_ fs.NodeReaddirer = (*Root)(nil)
	_ fs.NodeGetattrer = (*Root)(nil)
	_ fs.NodeMkdirer   = (*Root)(nil)
	_ fs.NodeCreater   = (*Root)(nil)
	_ fs.NodeUnlinker  = (*Root)(nil)
	_ fs.NodeRmdirer   = (*Root)(nil)
	_ fs.NodeRenamer   = (*Root)(nil)
	_ fs.NodeStatfser  = (*Root)(nil)
)

// NewRoot creates a new FUSE root node backed by the given engine.
func NewRoot(eng *engine.Engine, spoolDir string) *Root {
	return &Root{eng: eng, spoolDir: spoolDir}
}

// virtualPath returns the full pdrive virtual path for a child of this node.
func (r *Root) virtualPath(name string) string {
	p := r.Path(nil)
	if p == "" {
		return "/" + name
	}
	return "/" + p + "/" + name
}

// dirPath returns the pdrive virtual path for this node as a directory.
func (r *Root) dirPath() string {
	p := r.Path(nil)
	if p == "" {
		return "/"
	}
	return "/" + p
}

// Getattr fills in attributes for the root directory.
func (r *Root) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	now := uint64(time.Now().Unix())
	out.Mode = syscall.S_IFDIR | 0755
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Mtime = now
	out.Atime = now
	out.Ctime = now
	out.SetTimeout(attrTimeout)
	return 0
}

// Statfs returns filesystem statistics.
func (r *Root) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	// Report 1 TB virtual capacity so Finder/df show a reasonable size.
	const totalBlocks = 1 << 30 / 4096 * 1024 // ~1 TB in 4K blocks
	out.Blocks = totalBlocks
	out.Bfree = totalBlocks
	out.Bavail = totalBlocks
	out.Bsize = 4096
	out.Frsize = 4096
	out.NameLen = 255
	return 0
}

// Lookup finds a child node by name.
func (r *Root) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	vp := r.virtualPath(name)

	// Check if it's a file first.
	file, err := r.eng.Stat(vp)
	if err != nil {
		return nil, toErrno(err)
	}
	if file != nil {
		out.Mode = syscall.S_IFREG | 0644
		out.Size = uint64(file.SizeBytes)
		out.Mtime = uint64(file.ModifiedAt)
		out.Ctime = uint64(file.CreatedAt)
		out.Atime = uint64(file.ModifiedAt)
		out.SetEntryTimeout(attrTimeout)
		out.SetAttrTimeout(attrTimeout)
		child := r.NewInode(ctx, &Root{eng: r.eng, spoolDir: r.spoolDir}, fs.StableAttr{Mode: syscall.S_IFREG})
		return child, 0
	}

	// Check if it's a directory.
	isDir, err := r.eng.IsDir(vp)
	if err != nil {
		return nil, toErrno(err)
	}
	if isDir {
		out.Mode = syscall.S_IFDIR | 0755
		out.SetEntryTimeout(attrTimeout)
		out.SetAttrTimeout(attrTimeout)
		child := r.NewInode(ctx, &Root{eng: r.eng, spoolDir: r.spoolDir}, fs.StableAttr{Mode: syscall.S_IFDIR})
		return child, 0
	}

	return nil, syscall.ENOENT
}

// Readdir returns directory entries for Finder/ls.
func (r *Root) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	dp := r.dirPath()
	files, dirs, err := r.eng.ListDir(dp)
	if err != nil {
		return nil, toErrno(err)
	}

	entries := make([]fuse.DirEntry, 0, len(dirs)+len(files))
	for _, d := range dirs {
		entries = append(entries, fuse.DirEntry{
			Mode: syscall.S_IFDIR,
			Name: d,
		})
	}
	for _, f := range files {
		entries = append(entries, fuse.DirEntry{
			Mode: syscall.S_IFREG,
			Name: path.Base(f.VirtualPath),
		})
	}
	return fs.NewListDirStream(entries), 0
}

// Mkdir creates a new subdirectory.
func (r *Root) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	vp := r.virtualPath(name)
	if err := r.eng.MkDir(vp); err != nil {
		return nil, toErrno(err)
	}
	out.Mode = syscall.S_IFDIR | 0755
	out.SetEntryTimeout(attrTimeout)
	out.SetAttrTimeout(attrTimeout)
	child := r.NewInode(ctx, &Root{eng: r.eng, spoolDir: r.spoolDir}, fs.StableAttr{Mode: syscall.S_IFDIR})
	return child, 0
}

// Create creates a new file and returns a file handle for writing.
func (r *Root) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	vp := r.virtualPath(name)
	out.Mode = syscall.S_IFREG | 0644
	out.SetEntryTimeout(attrTimeout)
	out.SetAttrTimeout(attrTimeout)
	child := r.NewInode(ctx, &Root{eng: r.eng, spoolDir: r.spoolDir}, fs.StableAttr{Mode: syscall.S_IFREG})
	fh := &fuseFileHandle{
		eng:      r.eng,
		vpath:    vp,
		writable: true,
		spoolDir: r.spoolDir,
	}
	return child, fh, fuse.FOPEN_DIRECT_IO, 0
}

// Open opens an existing file for reading or writing.
func (r *Root) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	dp := r.dirPath()
	// If this is a directory, return nil handle.
	if r.IsDir() {
		return nil, 0, 0
	}

	vp := dp // For files, dirPath is the file's virtual path
	writable := flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_TRUNC) != 0
	fh := &fuseFileHandle{
		eng:      r.eng,
		vpath:    vp,
		writable: writable,
		spoolDir: r.spoolDir,
	}
	var fuseFlags uint32
	if writable {
		fuseFlags = fuse.FOPEN_DIRECT_IO
	}
	return fh, fuseFlags, 0
}

// Unlink removes a file.
func (r *Root) Unlink(ctx context.Context, name string) syscall.Errno {
	vp := r.virtualPath(name)
	if err := r.eng.DeleteFile(vp); err != nil {
		return toErrno(err)
	}
	return 0
}

// Rmdir removes an empty directory.
func (r *Root) Rmdir(ctx context.Context, name string) syscall.Errno {
	vp := r.virtualPath(name)
	if err := r.eng.DeleteDir(vp); err != nil {
		return toErrno(err)
	}
	return 0
}

// Rename moves a file or directory.
func (r *Root) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := r.virtualPath(name)

	// Resolve the new parent's virtual path.
	var newParentPath string
	if np, ok := newParent.(*Root); ok {
		newParentPath = np.dirPath()
	} else {
		// Fallback: get path from inode tree.
		p := newParent.EmbeddedInode().Path(nil)
		if p == "" {
			newParentPath = "/"
		} else {
			newParentPath = "/" + p
		}
	}
	newPath := newParentPath
	if !strings.HasSuffix(newPath, "/") {
		newPath += "/"
	}
	newPath += newName

	isDir, _ := r.eng.IsDir(oldPath)
	if isDir {
		if err := r.eng.RenameDir(oldPath, newPath); err != nil {
			return toErrno(err)
		}
	} else {
		if err := r.eng.RenameFile(oldPath, newPath); err != nil {
			return toErrno(err)
		}
	}
	return 0
}

// Setattr handles attribute changes (e.g., truncate).
func (r *Root) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	out.SetTimeout(attrTimeout)
	return 0
}

// --- File Handle ---

// fuseFileHandle implements read/write operations on an open file.
type fuseFileHandle struct {
	eng      *engine.Engine
	vpath    string
	writable bool
	spoolDir string

	mu        sync.Mutex
	readFile  *os.File // lazily opened for reads
	tmpFile   *os.File // staging file for writes
	tmpPath   string
	writeSize int64
}

var (
	_ fs.FileReader    = (*fuseFileHandle)(nil)
	_ fs.FileWriter    = (*fuseFileHandle)(nil)
	_ fs.FileFlusher   = (*fuseFileHandle)(nil)
	_ fs.FileReleaser  = (*fuseFileHandle)(nil)
	_ fs.FileGetattrer = (*fuseFileHandle)(nil)
)

// Read reads data from the file at the given offset.
func (fh *fuseFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if err := fh.ensureReadFile(); err != nil {
		return nil, toErrno(err)
	}

	n, err := fh.readFile.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, toErrno(err)
	}
	return fuse.ReadResultData(dest[:n]), 0
}

// ensureReadFile lazily downloads the file to a temp file.
func (fh *fuseFileHandle) ensureReadFile() error {
	if fh.readFile != nil {
		return nil
	}
	tmp, err := fh.eng.ReadFileToTempFile(fh.vpath)
	if err != nil {
		return err
	}
	fh.readFile = tmp
	return nil
}

// Write writes data to the staging temp file.
func (fh *fuseFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if !fh.writable {
		return 0, syscall.EBADF
	}

	// Lazily create temp file on first write.
	if fh.tmpFile == nil {
		dir := fh.spoolDir
		tmp, err := os.CreateTemp(dir, "pdrive-fuse-*")
		if err != nil {
			return 0, toErrno(err)
		}
		fh.tmpFile = tmp
		fh.tmpPath = tmp.Name()
	}

	n, err := fh.tmpFile.WriteAt(data, off)
	written := int64(n)
	newEnd := off + written
	if newEnd > fh.writeSize {
		fh.writeSize = newEnd
	}
	if err != nil {
		return uint32(n), toErrno(err)
	}
	return uint32(n), 0
}

// Getattr returns up-to-date attributes for this open file.
func (fh *fuseFileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.writable && fh.tmpFile != nil {
		out.Size = uint64(fh.writeSize)
		out.Mode = syscall.S_IFREG | 0644
		out.SetTimeout(attrTimeout)
		return 0
	}

	file, err := fh.eng.Stat(fh.vpath)
	if err != nil {
		return toErrno(err)
	}
	if file == nil {
		// File exists but may have been just created.
		out.Mode = syscall.S_IFREG | 0644
		out.SetTimeout(attrTimeout)
		return 0
	}
	out.Size = uint64(file.SizeBytes)
	out.Mtime = uint64(file.ModifiedAt)
	out.Ctime = uint64(file.CreatedAt)
	out.Atime = uint64(file.ModifiedAt)
	out.Mode = syscall.S_IFREG | 0644
	out.SetTimeout(attrTimeout)
	return 0
}

// Flush is called on close(2). Upload the file if we wrote to it.
func (fh *fuseFileHandle) Flush(ctx context.Context) syscall.Errno {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if !fh.writable || fh.tmpFile == nil {
		return 0
	}

	if err := fh.tmpFile.Sync(); err != nil {
		fh.cleanup()
		return toErrno(err)
	}
	if _, err := fh.tmpFile.Seek(0, io.SeekStart); err != nil {
		fh.cleanup()
		return toErrno(err)
	}

	// Large files: async upload so release returns quickly.
	if fh.writeSize > engine.AsyncWriteThreshold {
		slog.Info("fuse async upload started", "path", fh.vpath, "size", fh.writeSize)
		if err := fh.eng.WriteFileAsync(fh.vpath, fh.tmpFile, fh.tmpPath, fh.writeSize); err != nil {
			fh.cleanup()
			return toErrno(err)
		}
		fh.tmpFile = nil // engine owns the file now
		return 0
	}

	// Small files: synchronous upload.
	err := fh.eng.WriteFileStream(fh.vpath, fh.tmpFile, fh.writeSize)
	fh.cleanup()
	if err != nil {
		return toErrno(err)
	}
	return 0
}

// Release cleans up file handles.
func (fh *fuseFileHandle) Release(ctx context.Context) syscall.Errno {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	fh.cleanup()
	if fh.readFile != nil {
		name := fh.readFile.Name()
		fh.readFile.Close()
		os.Remove(name)
		fh.readFile = nil
	}
	return 0
}

func (fh *fuseFileHandle) cleanup() {
	if fh.tmpFile != nil {
		fh.tmpFile.Close()
		os.Remove(fh.tmpPath)
		fh.tmpFile = nil
	}
}
