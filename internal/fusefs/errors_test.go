package fusefs

import (
	"fmt"
	"os"
	"syscall"
	"testing"
)

func TestToErrno_Nil(t *testing.T) {
	if got := toErrno(nil); got != 0 {
		t.Errorf("toErrno(nil) = %d, want 0", got)
	}
}

func TestToErrno_IsNotExist(t *testing.T) {
	if got := toErrno(os.ErrNotExist); got != syscall.ENOENT {
		t.Errorf("got %d, want ENOENT (%d)", got, syscall.ENOENT)
	}
	// Also test *os.PathError wrapping.
	pathErr := &os.PathError{Op: "open", Path: "/x", Err: os.ErrNotExist}
	if got := toErrno(pathErr); got != syscall.ENOENT {
		t.Errorf("PathError: got %d, want ENOENT", got)
	}
}

func TestToErrno_IsExist(t *testing.T) {
	if got := toErrno(os.ErrExist); got != syscall.EEXIST {
		t.Errorf("got %d, want EEXIST (%d)", got, syscall.EEXIST)
	}
}

func TestToErrno_IsPermission(t *testing.T) {
	if got := toErrno(os.ErrPermission); got != syscall.EACCES {
		t.Errorf("got %d, want EACCES (%d)", got, syscall.EACCES)
	}
}

func TestToErrno_StringMatches(t *testing.T) {
	tests := []struct {
		msg  string
		want syscall.Errno
	}{
		{"directory not empty", syscall.ENOTEMPTY},
		{"is a directory", syscall.EISDIR},
		{"not a directory", syscall.ENOTDIR},
		{"no space left", syscall.ENOSPC},
		{"read-only filesystem", syscall.EROFS},
		{"invalid argument", syscall.EINVAL},
		{"name too long", syscall.ENAMETOOLONG},
	}
	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			got := toErrno(fmt.Errorf("%s", tt.msg))
			if got != tt.want {
				t.Errorf("toErrno(%q) = %d, want %d", tt.msg, got, tt.want)
			}
		})
	}
}

func TestToErrno_UnknownFallsToEIO(t *testing.T) {
	got := toErrno(fmt.Errorf("something completely unexpected"))
	if got != syscall.EIO {
		t.Errorf("got %d, want EIO (%d)", got, syscall.EIO)
	}
}
