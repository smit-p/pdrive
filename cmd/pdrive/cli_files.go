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
	"sort"
	"strings"
	"strconv"
	"text/tabwriter"
	"time"
)

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
