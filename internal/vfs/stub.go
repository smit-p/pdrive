package vfs

import (
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

// xattr keys used to mark stub (cloud-only) files on the local filesystem.
// These use the com.pdrive.* reverse-DNS namespace to avoid conflicts with
// other tools.  Values are plain UTF-8 strings stored via setxattr(2).
const (
	xattrStub = "com.pdrive.stub" // "1" when the file is a cloud-only placeholder
	xattrSize = "com.pdrive.size" // real file size in bytes as a decimal string
)

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
