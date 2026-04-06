//go:build linux

package vfs

// Linux requires the "user." namespace prefix for unprivileged xattr access.
const (
	xattrStub = "user.com.pdrive.stub" // "1" when the file is a cloud-only placeholder
	xattrSize = "user.com.pdrive.size" // real file size in bytes as a decimal string
)
