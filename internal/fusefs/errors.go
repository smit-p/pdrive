// Package fusefs provides a FUSE filesystem backed by the pdrive engine.
//
// It maps kernel filesystem operations (lookup, getattr, read, write, etc.)
// to [engine.Engine] methods, using the same temp-file staging model as
// the WebDAV layer for writes. Files larger than [engine.AsyncWriteThreshold]
// are uploaded in the background so the FUSE release returns quickly.
package fusefs

import (
	"os"
	"strings"
	"syscall"
)

// toErrno maps common Go errors to FUSE-compatible syscall.Errno values.
func toErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if os.IsNotExist(err) {
		return syscall.ENOENT
	}
	if os.IsExist(err) {
		return syscall.EEXIST
	}
	if os.IsPermission(err) {
		return syscall.EACCES
	}

	msg := err.Error()
	switch {
	case strings.Contains(msg, "not empty"):
		return syscall.ENOTEMPTY
	case strings.Contains(msg, "is a directory"):
		return syscall.EISDIR
	case strings.Contains(msg, "not a directory"):
		return syscall.ENOTDIR
	case strings.Contains(msg, "no space"):
		return syscall.ENOSPC
	case strings.Contains(msg, "read-only"):
		return syscall.EROFS
	case strings.Contains(msg, "invalid argument"):
		return syscall.EINVAL
	case strings.Contains(msg, "name too long"):
		return syscall.ENAMETOOLONG
	}
	return syscall.EIO
}
