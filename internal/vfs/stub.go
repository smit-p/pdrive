package vfs

import (
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

// xattr key constants (xattrStub, xattrSize) are defined in
// stub_xattr_darwin.go and stub_xattr_linux.go to handle the
// platform-specific namespace requirements.

// createStubFile creates a 0-byte placeholder file with xattrs indicating
// it's a cloud-only stub. The Finder tag is set to gray.
func createStubFile(path string, realSize int64) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	f.Close()

	if err := unix.Setxattr(path, xattrStub, []byte("1"), 0); err != nil {
		return err
	}
	sizeStr := strconv.FormatInt(realSize, 10)
	if err := unix.Setxattr(path, xattrSize, []byte(sizeStr), 0); err != nil {
		return err
	}
	return nil
}

// isStubFile returns true if the file at path is a pdrive cloud-only stub.
func isStubFile(path string) bool {
	buf := make([]byte, 8)
	n, err := unix.Getxattr(path, xattrStub, buf)
	if err != nil || n == 0 {
		return false
	}
	return string(buf[:n]) == "1"
}

// clearStubMarker removes the stub xattrs.
func clearStubMarker(path string) {
	unix.Removexattr(path, xattrStub) //nolint:errcheck
	unix.Removexattr(path, xattrSize) //nolint:errcheck
}
