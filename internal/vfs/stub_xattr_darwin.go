//go:build darwin

package vfs

// macOS allows arbitrary xattr namespaces — use reverse-DNS directly.
const (
	xattrStub = "com.pdrive.stub" // "1" when the file is a cloud-only placeholder
	xattrSize = "com.pdrive.size" // real file size in bytes as a decimal string
)
