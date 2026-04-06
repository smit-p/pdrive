// Package config handles loading pdrive configuration from TOML files.
//
// Configuration is loaded from ~/.pdrive/config.toml by default.
// CLI flags always override values from the config file.
package config

import (
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"
)

// File represents the on-disk TOML configuration.
type File struct {
	SyncDir      string `toml:"sync_dir"`
	RcloneAddr   string `toml:"rclone_addr"`
	WebDAVAddr   string `toml:"webdav_addr"`
	RcloneBin    string `toml:"rclone_bin"`
	BrokerPolicy string `toml:"broker_policy"`
	MinFreeSpace int64  `toml:"min_free_space"`
	ChunkSize    int    `toml:"chunk_size"`
	RateLimit    int    `toml:"rate_limit"`
	Debug        bool   `toml:"debug"`
	Remotes      string `toml:"remotes"` // comma-separated

	// FUSE mount settings.
	MountBackend string `toml:"mount_backend"` // "webdav" or "fuse"
	MountPoint   string `toml:"mount_point"`
}

// Load reads a TOML config file from the given path.
// Returns a zero-value File (not an error) if the file does not exist.
func Load(configDir string) (File, error) {
	p := filepath.Join(configDir, "config.toml")
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return File{}, nil
		}
		return File{}, err
	}
	var f File
	if err := toml.Unmarshal(data, &f); err != nil {
		return File{}, err
	}
	return f, nil
}
