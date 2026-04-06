// Package junkfile provides a single source of truth for identifying OS-generated
// hidden files (macOS .DS_Store, AppleDouble resource forks, Windows Thumbs.db,
// etc.) that should never be stored in the cloud.
package junkfile

import "strings"

// IsOSJunk reports whether base is an OS-generated hidden filename that
// should never be uploaded or synced.
func IsOSJunk(base string) bool {
	return base == ".DS_Store" ||
		strings.HasPrefix(base, "._") ||
		base == "Thumbs.db" ||
		base == "desktop.ini"
}
