package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"text/tabwriter"
)

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
