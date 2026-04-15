package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// daemonURL builds an API URL for the running daemon.
func daemonURL(addr, apiPath string, query url.Values) string {
	u := url.URL{
		Scheme:   "http",
		Host:     addr,
		Path:     apiPath,
		RawQuery: query.Encode(),
	}
	return u.String()
}

// daemonGet issues a GET to the running daemon and returns the response body.
func daemonGet(addr, apiPath string, query url.Values) ([]byte, error) {
	resp, err := http.Get(daemonURL(addr, apiPath, query))
	if err != nil {
		return nil, fmt.Errorf("cannot reach daemon at %s: %w", addr, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}
	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("endpoint not found (daemon may be outdated — restart the daemon)")
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("daemon returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// lsCache remembers the last listing so users can refer to items by number.
type lsCache struct {
	Dir      string   `json:"dir"`
	Items    []string `json:"items"`     // ordered list of names (dirs first, then files)
	DirCount int      `json:"dir_count"` // how many of Items[] are directories
	Parents  []string `json:"parents"`   // breadcrumb trail of parent dirs
}

func lsCachePath(configDir string) string {
	return filepath.Join(configDir, "ls-cache.json")
}

func readLsCache(configDir string) *lsCache {
	data, err := os.ReadFile(lsCachePath(configDir))
	if err != nil {
		return nil
	}
	var c lsCache
	if json.Unmarshal(data, &c) != nil {
		return nil
	}
	return &c
}

func writeLsCache(configDir, dir string, items []string, dirCount int, parents []string) {
	data, _ := json.Marshal(lsCache{Dir: dir, Items: items, DirCount: dirCount, Parents: parents})
	_ = os.WriteFile(lsCachePath(configDir), data, 0600)
}

// resolveResult holds the resolved path plus whether it's known to be a file.
type resolveResult struct {
	Path   string
	IsFile bool // true when we know the target is a file (not a directory)
}

// resolveLsArg handles input forms:
//  1. ".."                      → parent directory from cache
//  2. Absolute/relative path    → used as-is
//  3. Numeric index ("1", "2")  → looked up in the last ls-cache
//  4. Fuzzy substring           → matched case-insensitively against cache items
func resolveLsArg(arg, configDir string) resolveResult {
	// ".." → go to parent of the cached directory.
	if arg == ".." {
		if c := readLsCache(configDir); c != nil && c.Dir != "/" {
			return resolveResult{Path: path.Dir(c.Dir)}
		}
		return resolveResult{Path: "/"}
	}

	// Try numeric index first.
	if n, err := strconv.Atoi(arg); err == nil && n >= 1 {
		if c := readLsCache(configDir); c != nil && n <= len(c.Items) {
			isFile := n > c.DirCount // items past DirCount are files
			return resolveResult{
				Path:   path.Join(c.Dir, c.Items[n-1]),
				IsFile: isFile,
			}
		}
		fmt.Fprintf(os.Stderr, "No item #%d in last listing. Run `pdrive ls` first.\n", n)
		os.Exit(1)
	}

	// If it looks like a path (contains /), use as-is.
	if strings.Contains(arg, "/") {
		return resolveResult{Path: arg}
	}

	// Try fuzzy substring match against last listing.
	if c := readLsCache(configDir); c != nil {
		needle := strings.ToLower(arg)
		type match struct {
			name   string
			index  int
			isFile bool
		}
		var matches []match
		for i, item := range c.Items {
			if strings.Contains(strings.ToLower(item), needle) {
				matches = append(matches, match{item, i, i >= c.DirCount})
			}
		}
		if len(matches) == 1 {
			return resolveResult{
				Path:   path.Join(c.Dir, matches[0].name),
				IsFile: matches[0].isFile,
			}
		}
		if len(matches) > 1 {
			fmt.Fprintf(os.Stderr, "Ambiguous match for %q:\n", arg)
			for _, m := range matches {
				fmt.Fprintf(os.Stderr, "  %s\n", m.name)
			}
			os.Exit(1)
		}
	}

	// Fall through — treat as literal path.
	return resolveResult{Path: arg}
}

// --- formatting helpers ---

// fmtSize formats a byte count as a human-readable string (B / KB / MB / GB).
func fmtSize(b int64) string {
	switch {
	case b < 1024:
		return fmt.Sprintf("%d B", b)
	case b < 1024*1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	case b < 1024*1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
	default:
		return fmt.Sprintf("%.1f GB", float64(b)/(1024*1024*1024))
	}
}

// fmtAge formats a Unix timestamp as a relative-time string ("just now", "3h ago", etc.).
func fmtAge(unixSec int64) string {
	if unixSec == 0 {
		return "-"
	}
	d := time.Since(time.Unix(unixSec, 0))
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	case d < 30*24*time.Hour:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	default:
		return time.Unix(unixSec, 0).Format("Jan 02, 2006")
	}
}

// fmtDuration formats a duration in seconds as a human-readable string (e.g. "2d 5h 30m").
func fmtDuration(seconds float64) string {
	d := time.Duration(seconds * float64(time.Second))
	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	mins := int(d.Minutes()) % 60

	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh %dm", days, hours, mins)
	case hours > 0:
		return fmt.Sprintf("%dh %dm", hours, mins)
	case mins > 0:
		return fmt.Sprintf("%dm %ds", mins, int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
}

// stateLabel converts a file sync state into a user-friendly label.
func stateLabel(state string) string {
	switch state {
	case "local":
		return "local"
	case "stub":
		return "cloud"
	case "uploading":
		return "uploading…"
	default:
		return state
	}
}

// printCLIUsage prints the help text for all CLI subcommands.
func printCLIUsage() {
	fmt.Fprintf(os.Stderr, `pdrive — unified cloud storage

Usage:
  pdrive                          Start the daemon (default)

Navigation:
  pdrive browse                   Interactive file browser (TUI)
  pdrive ls [path|number]         List files and directories
  pdrive tree [path]              Show directory tree recursively
  pdrive find <pattern> [path]    Search for files by name

File operations:
  pdrive cat <path|number>        Print file contents to stdout
  pdrive get <path|number> [dest] Download file to local filesystem
  pdrive put <local-path> [dir]   Upload local file or directory
  pdrive pin <path|number> [...]  Download cloud-only files locally
  pdrive unpin <path|number> [...] Evict local copies (keep in cloud)
  pdrive mv <src> <dst>           Move or rename files/directories
  pdrive rm <path|number> [...]   Delete files/directories from cloud
  pdrive mkdir <path>             Create a directory

Info:
  pdrive info <path|number>       Show detailed file metadata and chunks
  pdrive du [path]                Show disk usage summary
  pdrive status                   Show storage summary and provider quotas
  pdrive remotes                  List rclone remotes and which are enabled
  pdrive remotes add <name>       Enable a remote for pdrive
  pdrive remotes remove <name>    Disable a remote from pdrive
  pdrive remotes reset            Use all remotes (clear selection)
  pdrive uploads                  Show in-flight upload progress
  pdrive health                   Check daemon health
  pdrive metrics                  Show telemetry counters

Management:
  pdrive stop                     Stop the daemon
  pdrive mount [--mountpoint=PATH] Switch to FUSE backend (default: ~/pdrive)
  pdrive unmount                  Unmount FUSE and stop the daemon
  pdrive help                     Show all daemon flags

Hints:
  Use numbers from ls output:    pdrive ls → pdrive cat 3
  Use ".." to go up:             pdrive ls ..
  Use fuzzy match:               pdrive cat vacation

Flags:
  --backend       Mount backend: webdav (default) or fuse
  --mountpoint    FUSE mount point path (default: ~/pdrive)
  --erasure       Reed-Solomon erasure coding (e.g. 3+1)
  --remotes       Override remote selection (comma-separated; prefer remotes add/remove)
  --webdav-addr   Daemon address (default 127.0.0.1:8765)
  --foreground    Run in foreground (for systemd/debugging)
  --debug         Enable debug logging
`)
}
