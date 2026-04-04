package vfs

import (
	"encoding/binary"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// xattr keys used to mark stub (cloud-only) files.
const (
	xattrStub = "com.pdrive.stub"                     // "1" if file is a cloud-only stub
	xattrSize = "com.pdrive.size"                     // real file size as decimal string
	xattrTags = "com.apple.metadata:_kMDItemUserTags" // Finder tags
)

// Finder tag color indices (used in the _kMDItemUserTags plist).
const (
	finderColorNone   = 0
	finderColorGray   = 1
	finderColorGreen  = 2
	finderColorPurple = 3
	finderColorBlue   = 4
	finderColorYellow = 5
	finderColorRed    = 6
	finderColorOrange = 7
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
	sizeStr := strings.TrimSpace(strings.Replace(
		strings.Replace(
			strings.Replace(
				formatInt(realSize), ",", "", -1),
			" ", "", -1),
		"\n", "", -1))
	if err := unix.Setxattr(path, xattrSize, []byte(sizeStr), 0); err != nil {
		return err
	}
	setFinderTag(path, "Cloud", finderColorGray)
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

// clearStubMarker removes the stub xattr and sets a green Finder tag.
func clearStubMarker(path string) {
	unix.Removexattr(path, xattrStub) //nolint:errcheck
	unix.Removexattr(path, xattrSize) //nolint:errcheck
	setFinderTag(path, "Local", finderColorGreen)
}

// setFinderTag sets a single Finder tag (colored dot) on the file.
// Uses a binary plist encoding of a single-element NSArray.
func setFinderTag(path, name string, color int) {
	plist := buildTagPlist(name, color)
	unix.Setxattr(path, xattrTags, plist, 0) //nolint:errcheck
}

// clearFinderTag removes the Finder tag xattr.
func clearFinderTag(path string) {
	unix.Removexattr(path, xattrTags) //nolint:errcheck
}

// formatInt converts an int64 to its decimal string representation without
// importing strconv (to keep the dependency graph small).
func formatInt(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// buildTagPlist constructs a minimal binary plist (bplist00) containing
// a single-element NSArray with one NSString: "Name\nColor".
//
// Binary plist00 layout for ["Green\n2"]:
//
//	Header:       "bplist00" (8 bytes)
//	Object 0:     Array (1 element, ref to obj 1)
//	Object 1:     ASCII string "Green\n2"
//	Offset table: offsets to object 0 and 1
//	Trailer:      32 bytes of metadata
func buildTagPlist(name string, color int) []byte {
	// The tag string is "Name\n<digit>" where \n is a literal newline.
	tagStr := name + "\n" + string(rune('0'+color))
	tagBytes := []byte(tagStr)

	// Objects
	// Array: 0xA1 (array, 1 element), 0x01 (ref to object 1)
	arrayObj := []byte{0xA1, 0x01}

	// ASCII string: 0x5X where X = length (for len < 15)
	var strObj []byte
	if len(tagBytes) < 15 {
		strObj = append([]byte{0x50 | byte(len(tagBytes))}, tagBytes...)
	} else {
		// For longer strings: 0x5F, then int object encoding the length.
		// Our tags are always short, so this shouldn't happen.
		strObj = append([]byte{0x50 | byte(len(tagBytes))}, tagBytes...)
	}

	// Build the file
	header := []byte("bplist00")

	arrayOffset := len(header)
	stringOffset := arrayOffset + len(arrayObj)
	offsetTableOffset := stringOffset + len(strObj)

	// Offset table: each entry is 1 byte (offsets are small).
	offsetTable := []byte{byte(arrayOffset), byte(stringOffset)}

	// Trailer: 32 bytes
	trailer := make([]byte, 32)
	trailer[6] = 1                                                        // offset int size = 1 byte
	trailer[7] = 1                                                        // object ref size = 1 byte
	binary.BigEndian.PutUint64(trailer[8:16], 2)                          // number of objects
	binary.BigEndian.PutUint64(trailer[16:24], 0)                         // root object = 0
	binary.BigEndian.PutUint64(trailer[24:32], uint64(offsetTableOffset)) // offset table start

	result := make([]byte, 0, len(header)+len(arrayObj)+len(strObj)+len(offsetTable)+len(trailer))
	result = append(result, header...)
	result = append(result, arrayObj...)
	result = append(result, strObj...)
	result = append(result, offsetTable...)
	result = append(result, trailer...)
	return result
}
