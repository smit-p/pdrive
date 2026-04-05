package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
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

// --- ls ---

type cliLsFile struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	ModifiedAt int64  `json:"modified_at"`
	LocalState string `json:"local_state"`
}

type cliLsResponse struct {
	Path  string      `json:"path"`
	Dirs  []string    `json:"dirs"`
	Files []cliLsFile `json:"files"`
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

func runLs(addr, configDir string, args []string) {
	var prevCache *lsCache
	var p string

	switch {
	case len(args) > 0:
		prevCache = readLsCache(configDir)
		res := resolveLsArg(args[0], configDir)
		p = res.Path
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
		// If the user selected a file, show its info instead of an empty listing.
		if res.IsFile {
			runInfo(addr, configDir, []string{p})
			return
		}
	default:
		// No args: re-list the current cached directory (or root if no cache).
		if c := readLsCache(configDir); c != nil {
			prevCache = c
			p = c.Dir
		} else {
			p = "/"
		}
	}

	query := url.Values{"path": {p}}
	body, err := daemonGet(addr, "/api/ls", query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	var resp cliLsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid response: %v\n", err)
		os.Exit(1)
	}

	// Show breadcrumb.
	if resp.Path != "/" {
		fmt.Printf("  %s\n\n", resp.Path)
	}

	// If the path is not root and returned nothing, distinguish empty dir from missing.
	if len(resp.Dirs) == 0 && len(resp.Files) == 0 {
		fmt.Println("(empty)")
		return
	}

	// Build ordered item list: dirs first, then files.
	sort.Strings(resp.Dirs)
	sort.Slice(resp.Files, func(i, j int) bool {
		return resp.Files[i].Name < resp.Files[j].Name
	})

	var items []string
	for _, d := range resp.Dirs {
		items = append(items, d)
	}
	for _, f := range resp.Files {
		items = append(items, f.Name)
	}

	// Build parents breadcrumb trail.
	var parents []string
	if prevCache != nil && prevCache.Parents != nil {
		parents = prevCache.Parents
	}
	// Keep parents consistent: if we navigated deeper, push the previous dir.
	// If we went up (..), trim the trail.
	if prevCache != nil {
		if strings.HasPrefix(resp.Path, prevCache.Dir+"/") {
			// Went deeper — add previous dir to parents
			parents = append(parents, prevCache.Dir)
		} else if len(parents) > 0 {
			// Went up or sideways — find the right level in parents
			for len(parents) > 0 && !strings.HasPrefix(resp.Path, parents[len(parents)-1]) {
				parents = parents[:len(parents)-1]
			}
		}
	}
	if resp.Path == "/" {
		parents = nil
	}

	// Save cache for future numeric/fuzzy access.
	writeLsCache(configDir, resp.Path, items, len(resp.Dirs), parents)

	// How wide is the index column?
	idxWidth := len(strconv.Itoa(len(items)))
	if idxWidth < 2 {
		idxWidth = 2
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)

	// Show ".." navigation hint when not at root.
	if resp.Path != "/" {
		fmt.Fprintf(w, "  %*s  ../\n", idxWidth, "..")
	}

	idx := 1

	for _, d := range resp.Dirs {
		fmt.Fprintf(w, "  %*d  %s/\n", idxWidth, idx, d)
		idx++
	}

	for _, f := range resp.Files {
		state := stateLabel(f.LocalState)
		fmt.Fprintf(w, "  %*d  %s\t%s\t%s\t%s\n", idxWidth, idx, f.Name, fmtSize(f.Size), fmtAge(f.ModifiedAt), state)
		idx++
	}
	w.Flush()
}

// --- status ---

// fmtProviderDetail returns a parenthesized identity/type label for a provider.
// Examples: "(alice@gmail.com, drive)", "(drive)", "".
func fmtProviderDetail(p cliStatusProvider) string {
	switch {
	case p.AccountIdentity != "" && p.Type != "":
		return "(" + p.AccountIdentity + ", " + p.Type + ")"
	case p.AccountIdentity != "":
		return "(" + p.AccountIdentity + ")"
	case p.Type != "":
		return "(" + p.Type + ")"
	default:
		return ""
	}
}

type cliStatusProvider struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	AccountIdentity   string `json:"account_identity"`
	QuotaTotalBytes   *int64 `json:"quota_total_bytes"`
	QuotaFreeBytes    *int64 `json:"quota_free_bytes"`
	QuotaUsedByPdrive *int64 `json:"quota_used_by_pdrive"`
}

type cliStatusResponse struct {
	TotalFiles int64               `json:"total_files"`
	TotalBytes int64               `json:"total_bytes"`
	Providers  []cliStatusProvider `json:"providers"`
}

func runStatus(addr string) {
	body, err := daemonGet(addr, "/api/status", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	var resp cliStatusResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Files:      %d\n", resp.TotalFiles)
	fmt.Printf("Total size: %s\n", fmtSize(resp.TotalBytes))
	if len(resp.Providers) > 0 {
		fmt.Println()
		fmt.Println("Providers:")
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		for _, p := range resp.Providers {
			free := "unknown"
			total := "unknown"
			used := "-"
			if p.QuotaFreeBytes != nil {
				free = fmtSize(*p.QuotaFreeBytes)
			}
			if p.QuotaTotalBytes != nil {
				total = fmtSize(*p.QuotaTotalBytes)
			}
			if p.QuotaUsedByPdrive != nil {
				used = fmtSize(*p.QuotaUsedByPdrive)
			}
			fmt.Fprintf(w, "  %s\t%s\t%s used by pdrive\t%s free / %s total\n", p.Name, fmtProviderDetail(p), used, free, total)
		}
		w.Flush()
	}
}

// --- remotes ---

// remotesConfigPath returns the path to the persistent remotes selection file.
func remotesConfigPath(configDir string) string {
	return filepath.Join(configDir, "remotes.json")
}

// loadRemotesConfig reads the enabled remotes from remotes.json.
// Returns nil (meaning "all") if the file doesn't exist.
func loadRemotesConfig(configDir string) ([]string, error) {
	data, err := os.ReadFile(remotesConfigPath(configDir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var remotes []string
	if err := json.Unmarshal(data, &remotes); err != nil {
		return nil, fmt.Errorf("invalid remotes.json: %w", err)
	}
	return remotes, nil
}

// saveRemotesConfig writes the enabled remotes to remotes.json.
func saveRemotesConfig(configDir string, remotes []string) error {
	if err := os.MkdirAll(configDir, 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(remotes, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(remotesConfigPath(configDir), data, 0600)
}

// listRcloneRemotes calls `rclone listremotes` directly (no daemon needed).
func listRcloneRemotes() ([]string, error) {
	out, err := exec.Command("rclone", "listremotes").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to run rclone listremotes: %w", err)
	}
	var remotes []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		name := strings.TrimSuffix(strings.TrimSpace(line), ":")
		if name != "" {
			remotes = append(remotes, name)
		}
	}
	return remotes, nil
}

func runRemotes(configDir string, args []string) {
	if len(args) == 0 {
		// pdrive remotes — list all remotes and show enabled ones.
		runRemotesList(configDir)
		return
	}

	switch args[0] {
	case "add":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: pdrive remotes add <name> [name...]\n")
			os.Exit(1)
		}
		runRemotesAdd(configDir, args[1:])
	case "remove":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: pdrive remotes remove <name> [name...]\n")
			os.Exit(1)
		}
		runRemotesRemove(configDir, args[1:])
	case "reset":
		runRemotesReset(configDir)
	default:
		fmt.Fprintf(os.Stderr, "Unknown remotes subcommand: %s\n", args[0])
		fmt.Fprintf(os.Stderr, "Usage: pdrive remotes [add|remove|reset]\n")
		os.Exit(1)
	}
}

func runRemotesList(configDir string) {
	allRemotes, err := listRcloneRemotes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if len(allRemotes) == 0 {
		fmt.Println("No rclone remotes configured. Run: rclone config")
		return
	}

	enabled, err := loadRemotesConfig(configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not read remotes config: %v\n", err)
	}

	// Build enabled set. nil/empty means all are enabled.
	allEnabled := len(enabled) == 0
	enabledSet := make(map[string]bool, len(enabled))
	for _, r := range enabled {
		enabledSet[r] = true
	}

	fmt.Println("Rclone remotes:")
	for _, name := range allRemotes {
		active := allEnabled || enabledSet[name]
		marker := "  "
		if active {
			marker = "* "
		}
		fmt.Printf("  %s%s\n", marker, name)
	}
	fmt.Println()
	if allEnabled {
		fmt.Println("* = enabled (all remotes are used — no selection configured)")
	} else {
		fmt.Println("* = enabled")
	}
	fmt.Println()
	fmt.Println("Manage remotes:")
	fmt.Println("  pdrive remotes add <name>       Enable a remote")
	fmt.Println("  pdrive remotes remove <name>    Disable a remote")
	fmt.Println("  pdrive remotes reset            Use all remotes (clear selection)")
}

func runRemotesAdd(configDir string, names []string) {
	// Validate names against actual rclone remotes.
	allRemotes, err := listRcloneRemotes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	available := make(map[string]bool, len(allRemotes))
	for _, r := range allRemotes {
		available[r] = true
	}
	for _, name := range names {
		if !available[name] {
			fmt.Fprintf(os.Stderr, "Error: %q is not a configured rclone remote\n", name)
			fmt.Fprintf(os.Stderr, "Available remotes: %s\n", strings.Join(allRemotes, ", "))
			os.Exit(1)
		}
	}

	// Load existing config.
	enabled, _ := loadRemotesConfig(configDir)

	// If no config yet, start from empty (not all) — user is explicitly choosing.
	enabledSet := make(map[string]bool, len(enabled))
	for _, r := range enabled {
		enabledSet[r] = true
	}

	for _, name := range names {
		if enabledSet[name] {
			fmt.Printf("%s is already enabled\n", name)
		} else {
			enabled = append(enabled, name)
			enabledSet[name] = true
			fmt.Printf("Enabled %s\n", name)
		}
	}

	if err := saveRemotesConfig(configDir, enabled); err != nil {
		fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\nRestart pdrive for changes to take effect.")
}

func runRemotesRemove(configDir string, names []string) {
	enabled, err := loadRemotesConfig(configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// If no config exists, we need to populate from all remotes first.
	if len(enabled) == 0 {
		allRemotes, err := listRcloneRemotes()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		enabled = allRemotes
	}

	// Validate that all names are in the enabled set.
	enabledSet := make(map[string]bool, len(enabled))
	for _, r := range enabled {
		enabledSet[r] = true
	}
	for _, name := range names {
		if !enabledSet[name] {
			fmt.Fprintf(os.Stderr, "Error: %q is not an enabled remote\n", name)
			fmt.Fprintf(os.Stderr, "Enabled remotes: %s\n", strings.Join(enabled, ", "))
			os.Exit(1)
		}
	}

	removeSet := make(map[string]bool, len(names))
	for _, name := range names {
		removeSet[name] = true
	}

	var remaining []string
	for _, r := range enabled {
		if !removeSet[r] {
			remaining = append(remaining, r)
		}
	}

	if len(remaining) == 0 {
		fmt.Fprintf(os.Stderr, "Error: cannot remove all remotes — at least one must be enabled\n")
		os.Exit(1)
	}

	for _, name := range names {
		fmt.Printf("Disabled %s\n", name)
	}

	if err := saveRemotesConfig(configDir, remaining); err != nil {
		fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\nRestart pdrive for changes to take effect.")
}

func runRemotesReset(configDir string) {
	p := remotesConfigPath(configDir)
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Remote selection cleared — pdrive will use all rclone remotes.")
	fmt.Println("\nRestart pdrive for changes to take effect.")
}

// --- uploads ---

type cliUploadProgress struct {
	VirtualPath    string `json:"VirtualPath"`
	TotalChunks    int    `json:"TotalChunks"`
	ChunksUploaded int    `json:"ChunksUploaded"`
	SizeBytes      int64  `json:"SizeBytes"`
	StartedAt      string `json:"StartedAt"`
	Failed         bool   `json:"Failed"`
}

func runUploads(addr string) {
	body, err := daemonGet(addr, "/api/uploads", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	var ups []cliUploadProgress
	if err := json.Unmarshal(body, &ups); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid response: %v\n", err)
		os.Exit(1)
	}

	if len(ups) == 0 {
		fmt.Println("No uploads in progress.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintf(w, "FILE\tPROGRESS\tSIZE\tSTATUS\n")
	for _, u := range ups {
		pct := 0
		if u.TotalChunks > 0 {
			pct = u.ChunksUploaded * 100 / u.TotalChunks
		}
		status := fmt.Sprintf("%d%%  %d/%d chunks", pct, u.ChunksUploaded, u.TotalChunks)
		state := "uploading"
		if u.Failed {
			state = "FAILED"
		} else if u.TotalChunks > 0 && u.ChunksUploaded >= u.TotalChunks {
			state = "finalizing"
		}
		name := path.Base(u.VirtualPath)
		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n", name, status, fmtSize(u.SizeBytes), state)
	}
	w.Flush()
}

// --- health ---

type cliHealthResponse struct {
	Status          string  `json:"status"`
	UptimeSeconds   float64 `json:"uptime_seconds"`
	InFlightUploads int     `json:"in_flight_uploads"`
	DBOK            bool    `json:"db_ok"`
}

func runHealth(addr string) {
	body, err := daemonGet(addr, "/api/health", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	var resp cliHealthResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid response: %v\n", err)
		os.Exit(1)
	}

	dbStatus := "ok"
	if !resp.DBOK {
		dbStatus = "ERROR"
	}

	fmt.Printf("Status:    %s\n", resp.Status)
	fmt.Printf("Uptime:    %s\n", fmtDuration(resp.UptimeSeconds))
	fmt.Printf("Uploads:   %d in-flight\n", resp.InFlightUploads)
	fmt.Printf("Database:  %s\n", dbStatus)

	if resp.Status != "ok" {
		os.Exit(1)
	}
}

// --- metrics ---

type cliMetricsResponse struct {
	FilesUploaded   int64 `json:"files_uploaded"`
	FilesDownloaded int64 `json:"files_downloaded"`
	FilesDeleted    int64 `json:"files_deleted"`
	ChunksUploaded  int64 `json:"chunks_uploaded"`
	BytesUploaded   int64 `json:"bytes_uploaded"`
	BytesDownloaded int64 `json:"bytes_downloaded"`
	DedupHits       int64 `json:"dedup_hits"`
}

func runMetrics(addr string) {
	body, err := daemonGet(addr, "/api/metrics", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	var resp cliMetricsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Files uploaded:    %d\n", resp.FilesUploaded)
	fmt.Printf("Files downloaded:  %d\n", resp.FilesDownloaded)
	fmt.Printf("Files deleted:     %d\n", resp.FilesDeleted)
	fmt.Printf("Chunks uploaded:   %d\n", resp.ChunksUploaded)
	fmt.Printf("Bytes uploaded:    %s\n", fmtSize(resp.BytesUploaded))
	fmt.Printf("Bytes downloaded:  %s\n", fmtSize(resp.BytesDownloaded))
	fmt.Printf("Dedup hits:        %d\n", resp.DedupHits)
}

// --- cat ---

func runCat(addr, configDir string, args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: pdrive cat <path|number>\n")
		os.Exit(1)
	}
	p := resolveLsArg(args[0], configDir).Path
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}

	resp, err := http.Get(daemonURL(addr, "/api/download", url.Values{"path": {p}}))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot reach daemon at %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		fmt.Fprintf(os.Stderr, "Error: file not found: %s\n", p)
		os.Exit(1)
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error: %s\n", strings.TrimSpace(string(body)))
		os.Exit(1)
	}

	if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
		fmt.Fprintf(os.Stderr, "Error: reading file: %v\n", err)
		os.Exit(1)
	}
}

// --- get ---

func runGet(addr, configDir string, args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: pdrive get <path|number> [destination]\n")
		os.Exit(1)
	}
	p := resolveLsArg(args[0], configDir).Path
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}

	// Determine destination path.
	dest := filepath.Base(p)
	if len(args) >= 2 {
		dest = args[1]
	}

	// Check if destination is a directory.
	if info, err := os.Stat(dest); err == nil && info.IsDir() {
		dest = filepath.Join(dest, filepath.Base(p))
	}

	resp, err := http.Get(daemonURL(addr, "/api/download", url.Values{"path": {p}}))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot reach daemon at %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		fmt.Fprintf(os.Stderr, "Error: file not found: %s\n", p)
		os.Exit(1)
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error: daemon returned %d: %s\n", resp.StatusCode, strings.TrimSpace(string(body)))
		os.Exit(1)
	}

	// Write to temp file then rename for atomicity.
	tmpDest := dest + ".pdrive-tmp"
	f, err := os.Create(tmpDest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: creating file: %v\n", err)
		os.Exit(1)
	}

	n, err := io.Copy(f, resp.Body)
	if err != nil {
		f.Close()
		os.Remove(tmpDest)
		fmt.Fprintf(os.Stderr, "Error: writing file: %v\n", err)
		os.Exit(1)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpDest)
		fmt.Fprintf(os.Stderr, "Error: closing file: %v\n", err)
		os.Exit(1)
	}
	if err := os.Rename(tmpDest, dest); err != nil {
		os.Remove(tmpDest)
		fmt.Fprintf(os.Stderr, "Error: renaming file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Downloaded %s → %s (%s)\n", p, dest, fmtSize(n))
}

// --- rm ---

func runRm(addr, configDir string, args []string) {
	for _, arg := range args {
		p := resolveLsArg(arg, configDir).Path
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
		apiURL := fmt.Sprintf("http://%s/api/delete?path=%s", addr, url.QueryEscape(p))
		resp, err := http.Post(apiURL, "", nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: cannot reach daemon at %s: %v\n", addr, err)
			os.Exit(1)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			fmt.Fprintf(os.Stderr, "Error: %s\n", strings.TrimSpace(string(body)))
			os.Exit(1)
		}
		fmt.Printf("Deleted: %s\n", p)
	}
}

// --- tree ---

type treeEntry struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

func runTree(addr, configDir string, args []string) {
	p := "/"
	if len(args) > 0 {
		p = resolveLsArg(args[0], configDir).Path
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
	}

	body, err := daemonGet(addr, "/api/tree", url.Values{"path": {p}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	var entries []treeEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid response: %v\n", err)
		os.Exit(1)
	}

	if len(entries) == 0 {
		fmt.Println("(empty)")
		return
	}

	// Build tree structure and display.
	root := p
	if root != "/" && !strings.HasSuffix(root, "/") {
		root += "/"
	}
	fmt.Println(p)
	printTree(entries, root)

	// Summary line.
	var totalSize int64
	dirs := make(map[string]bool)
	for _, e := range entries {
		totalSize += e.Size
		dir := path.Dir(e.Path)
		for dir != root && dir != "/" && dir != "." {
			dirs[dir] = true
			dir = path.Dir(dir)
		}
	}
	fmt.Printf("\n%d directories, %d files (%s)\n", len(dirs), len(entries), fmtSize(totalSize))
}

// printTree renders tree entries with box-drawing characters.
func printTree(entries []treeEntry, root string) {
	rootNode := &treeNode{children: make(map[string]*treeNode)}

	for _, e := range entries {
		rel := strings.TrimPrefix(e.Path, root)
		parts := strings.Split(rel, "/")
		cur := rootNode
		for i, part := range parts {
			if _, ok := cur.children[part]; !ok {
				cur.children[part] = &treeNode{name: part, children: make(map[string]*treeNode)}
			}
			cur = cur.children[part]
			if i == len(parts)-1 {
				cur.isFile = true
				cur.size = e.Size
			}
		}
	}

	renderNode(rootNode, "")
}

// treeNode represents a node in the tree display.
type treeNode struct {
	name     string
	size     int64
	children map[string]*treeNode
	isFile   bool
}

func renderNode(n *treeNode, prefix string) {
	// Collect and sort children: dirs first, then files.
	type child struct {
		name string
		node *treeNode
	}
	var dirs, files []child
	for name, ch := range n.children {
		if ch.isFile && len(ch.children) == 0 {
			files = append(files, child{name, ch})
		} else {
			dirs = append(dirs, child{name, ch})
		}
	}
	sort.Slice(dirs, func(i, j int) bool { return dirs[i].name < dirs[j].name })
	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })

	all := append(dirs, files...)
	for i, ch := range all {
		connector := "├── "
		childPrefix := "│   "
		if i == len(all)-1 {
			connector = "└── "
			childPrefix = "    "
		}
		if ch.node.isFile && len(ch.node.children) == 0 {
			fmt.Printf("%s%s%s  [%s]\n", prefix, connector, ch.name, fmtSize(ch.node.size))
		} else {
			fmt.Printf("%s%s%s/\n", prefix, connector, ch.name)
			renderNode(ch.node, prefix+childPrefix)
		}
	}
}

// --- find ---

type findEntry struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

func runFind(addr, configDir string, args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: pdrive find <pattern> [path]\n")
		os.Exit(1)
	}
	pattern := args[0]
	root := "/"
	if len(args) >= 2 {
		root = resolveLsArg(args[1], configDir).Path
		if !strings.HasPrefix(root, "/") {
			root = "/" + root
		}
	}

	body, err := daemonGet(addr, "/api/find", url.Values{"path": {root}, "pattern": {pattern}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	var entries []findEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid response: %v\n", err)
		os.Exit(1)
	}

	if len(entries) == 0 {
		fmt.Println("No matches found.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	for _, e := range entries {
		fmt.Fprintf(w, "  %s\t%s\n", e.Path, fmtSize(e.Size))
	}
	w.Flush()
	fmt.Printf("\n%d file(s) found\n", len(entries))
}

// --- mv ---

func runMv(addr, configDir string, args []string) {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: pdrive mv <src> <dst>\n")
		os.Exit(1)
	}
	src := resolveLsArg(args[0], configDir).Path
	if !strings.HasPrefix(src, "/") {
		src = "/" + src
	}
	dst := args[1]
	if !strings.HasPrefix(dst, "/") {
		dst = "/" + dst
	}

	apiURL := fmt.Sprintf("http://%s/api/mv?src=%s&dst=%s", addr, url.QueryEscape(src), url.QueryEscape(dst))
	resp, err := http.Post(apiURL, "", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot reach daemon at %s: %v\n", addr, err)
		os.Exit(1)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "Error: %s\n", strings.TrimSpace(string(body)))
		os.Exit(1)
	}
	fmt.Printf("Moved: %s → %s\n", src, dst)
}

// --- mkdir ---

func runMkdir(addr string, args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: pdrive mkdir <path>\n")
		os.Exit(1)
	}
	p := args[0]
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}

	apiURL := fmt.Sprintf("http://%s/api/mkdir?path=%s", addr, url.QueryEscape(p))
	resp, err := http.Post(apiURL, "", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot reach daemon at %s: %v\n", addr, err)
		os.Exit(1)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "Error: %s\n", strings.TrimSpace(string(body)))
		os.Exit(1)
	}
	fmt.Printf("Created: %s\n", p)
}

// --- info ---

type cliFileInfo struct {
	Path        string         `json:"path"`
	SizeBytes   int64          `json:"size_bytes"`
	CreatedAt   int64          `json:"created_at"`
	ModifiedAt  int64          `json:"modified_at"`
	SHA256      string         `json:"sha256"`
	UploadState string         `json:"upload_state"`
	Chunks      []cliChunkInfo `json:"chunks"`
}

type cliChunkInfo struct {
	Sequence      int      `json:"sequence"`
	SizeBytes     int      `json:"size_bytes"`
	EncryptedSize int      `json:"encrypted_size"`
	Providers     []string `json:"providers"`
}

func runInfo(addr, configDir string, args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: pdrive info <path|number>\n")
		os.Exit(1)
	}
	p := resolveLsArg(args[0], configDir).Path
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}

	body, err := daemonGet(addr, "/api/info", url.Values{"path": {p}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	var info cliFileInfo
	if err := json.Unmarshal(body, &info); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Path:     %s\n", info.Path)
	fmt.Printf("Size:     %s (%d bytes)\n", fmtSize(info.SizeBytes), info.SizeBytes)
	fmt.Printf("Created:  %s\n", time.Unix(info.CreatedAt, 0).Format("2006-01-02 15:04:05"))
	fmt.Printf("Modified: %s\n", time.Unix(info.ModifiedAt, 0).Format("2006-01-02 15:04:05"))
	fmt.Printf("SHA-256:  %s\n", info.SHA256)
	fmt.Printf("State:    %s\n", info.UploadState)

	if len(info.Chunks) > 0 {
		fmt.Printf("\nChunks: %d\n", len(info.Chunks))
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintf(w, "  #\tSize\tEncrypted\tProviders\n")
		for _, c := range info.Chunks {
			fmt.Fprintf(w, "  %d\t%s\t%s\t%s\n", c.Sequence, fmtSize(int64(c.SizeBytes)), fmtSize(int64(c.EncryptedSize)), strings.Join(c.Providers, ", "))
		}
		w.Flush()
	}
}

// --- du ---

type cliDuResponse struct {
	Path       string `json:"path"`
	FileCount  int64  `json:"file_count"`
	TotalBytes int64  `json:"total_bytes"`
}

func runDu(addr, configDir string, args []string) {
	p := "/"
	if len(args) > 0 {
		p = resolveLsArg(args[0], configDir).Path
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
	}

	body, err := daemonGet(addr, "/api/du", url.Values{"path": {p}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	var resp cliDuResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("%s in %d files: %s\n", fmtSize(resp.TotalBytes), resp.FileCount, resp.Path)
}

// --- formatting helpers ---

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
  pdrive help                     Show all daemon flags

Hints:
  Use numbers from ls output:    pdrive ls → pdrive cat 3
  Use ".." to go up:             pdrive ls ..
  Use fuzzy match:               pdrive cat vacation

Flags:
  --password      Encryption password (derives AES-256 key via Argon2id)
  --remotes       Override remote selection (comma-separated; prefer remotes add/remove)
  --webdav-addr   Daemon address (default 127.0.0.1:8765)
  --foreground    Run in foreground (for systemd/debugging)
  --debug         Enable debug logging
`)
}
