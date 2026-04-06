package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/smit-p/pdrive/internal/chunker"
	"github.com/smit-p/pdrive/internal/config"
	"github.com/smit-p/pdrive/internal/daemon"
	"github.com/smit-p/pdrive/internal/fusefs"
	"github.com/smit-p/pdrive/internal/rclonebin"
	"golang.org/x/term"
)

// Set by goreleaser ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: could not determine home directory: %v\n", err)
		os.Exit(1)
	}
	defaultConfigDir := filepath.Join(homeDir, ".pdrive")

	flag.Usage = func() { printCLIUsage() }

	configDir := flag.String("config-dir", defaultConfigDir, "Configuration directory")
	syncDir := flag.String("sync-dir", filepath.Join(homeDir, "pdrive"), "Local folder to sync (like Dropbox); empty disables sync")
	rcloneAddr := flag.String("rclone-addr", "127.0.0.1:5572", "rclone RC address")
	webdavAddr := flag.String("webdav-addr", "127.0.0.1:8765", "HTTP API/WebDAV address")
	rcloneBinFlag := flag.String("rclone-bin", "", "Absolute path to rclone binary (auto-detected if empty)")
	encKeyHex := flag.String("enc-key", "", "Encryption key (64-char hex string for AES-256); legacy, prefer --password")
	password := flag.String("password", "", "Encryption password (derives AES-256 key via Argon2id)")
	brokerPolicy := flag.String("broker-policy", "pfrd", "Chunk placement policy: pfrd (weighted random by free space) or mfs (most free space)")
	minFreeSpace := flag.Int64("min-free-space", 256*1024*1024, "Minimum free space (bytes) to keep on each provider (default 256 MB)")
	skipRestore := flag.Bool("skip-restore", false, "Skip restoring metadata DB from cloud on startup (use after a manual wipe)")
	chunkSize := flag.Int("chunk-size", 0, "Override chunk size in bytes (e.g. 67108864 for 64 MB); 0 uses dynamic sizing")
	rateLimit := flag.Int("rate-limit", 0, "API rate limit in requests per second (default 8)")
	remotesFlag := flag.String("remotes", "", "Comma-separated list of rclone remote names to use (default: all)")
	debug := flag.Bool("debug", false, "Enable debug logging")
	foreground := flag.Bool("foreground", false, "Run daemon in the foreground instead of backgrounding (useful with systemd or for debugging)")
	backend := flag.String("backend", "", "Mount backend: webdav (default) or fuse")
	mountPoint := flag.String("mountpoint", "", "FUSE mount point (e.g. /Volumes/pdrive); required when --backend=fuse")
	flag.Parse()

	// Load config file — CLI flags override config values.
	fileCfg, cfgErr := config.Load(*configDir)
	if cfgErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not load config file: %v\n", cfgErr)
	}
	applyConfigDefaults := func() {
		if fileCfg.SyncDir != "" && *syncDir == filepath.Join(homeDir, "pdrive") {
			*syncDir = fileCfg.SyncDir
		}
		if fileCfg.RcloneAddr != "" && *rcloneAddr == "127.0.0.1:5572" {
			*rcloneAddr = fileCfg.RcloneAddr
		}
		if fileCfg.WebDAVAddr != "" && *webdavAddr == "127.0.0.1:8765" {
			*webdavAddr = fileCfg.WebDAVAddr
		}
		if fileCfg.RcloneBin != "" && *rcloneBinFlag == "" {
			*rcloneBinFlag = fileCfg.RcloneBin
		}
		if fileCfg.BrokerPolicy != "" && *brokerPolicy == "pfrd" {
			*brokerPolicy = fileCfg.BrokerPolicy
		}
		if fileCfg.MinFreeSpace > 0 && *minFreeSpace == 256*1024*1024 {
			*minFreeSpace = fileCfg.MinFreeSpace
		}
		if fileCfg.ChunkSize > 0 && *chunkSize == 0 {
			*chunkSize = fileCfg.ChunkSize
		}
		if fileCfg.RateLimit > 0 && *rateLimit == 0 {
			*rateLimit = fileCfg.RateLimit
		}
		if fileCfg.Debug && !*debug {
			*debug = true
		}
		if fileCfg.Remotes != "" && *remotesFlag == "" {
			*remotesFlag = fileCfg.Remotes
		}
		if fileCfg.MountBackend != "" && *backend == "" {
			*backend = fileCfg.MountBackend
		}
		if fileCfg.MountPoint != "" && *mountPoint == "" {
			*mountPoint = fileCfg.MountPoint
		}
	}
	applyConfigDefaults()

	// Configure logging.
	logLevel := slog.LevelInfo
	if *debug {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))

	// Handle subcommands early, before any rclone lookup — these are all pure
	// HTTP calls to the running daemon and don't need rclone.
	if args := flag.Args(); len(args) > 0 {
		switch args[0] {
		case "pin", "unpin":
			if len(args) < 2 {
				fmt.Fprintf(os.Stderr, "Usage: pdrive %s <path|number>\n", args[0])
				os.Exit(1)
			}
			runPinUnpin(*webdavAddr, *configDir, args[0], args[1:])
			return
		case "ls":
			runLs(*webdavAddr, *configDir, args[1:])
			return
		case "browse":
			runBrowse(*webdavAddr, *configDir)
			return
		case "status":
			runStatus(*webdavAddr)
			return
		case "uploads":
			runUploads(*webdavAddr)
			return
		case "health":
			runHealth(*webdavAddr)
			return
		case "metrics":
			runMetrics(*webdavAddr)
			return
		case "cat":
			runCat(*webdavAddr, *configDir, args[1:])
			return
		case "get":
			runGet(*webdavAddr, *configDir, args[1:])
			return
		case "put":
			if len(args) < 2 {
				fmt.Fprintf(os.Stderr, "Usage: pdrive put <local-path> [remote-dir]\n")
				os.Exit(1)
			}
			runPut(*webdavAddr, args[1:])
			return
		case "rm":
			if len(args) < 2 {
				fmt.Fprintf(os.Stderr, "Usage: pdrive rm <path|number> [<path|number>...]\n")
				os.Exit(1)
			}
			runRm(*webdavAddr, *configDir, args[1:])
			return
		case "tree":
			runTree(*webdavAddr, *configDir, args[1:])
			return
		case "find":
			if len(args) < 2 {
				fmt.Fprintf(os.Stderr, "Usage: pdrive find <pattern> [path]\n")
				os.Exit(1)
			}
			runFind(*webdavAddr, *configDir, args[1:])
			return
		case "mv":
			if len(args) < 3 {
				fmt.Fprintf(os.Stderr, "Usage: pdrive mv <src> <dst>\n")
				os.Exit(1)
			}
			runMv(*webdavAddr, *configDir, args[1:])
			return
		case "mkdir":
			if len(args) < 2 {
				fmt.Fprintf(os.Stderr, "Usage: pdrive mkdir <path>\n")
				os.Exit(1)
			}
			runMkdir(*webdavAddr, args[1:])
			return
		case "info":
			if len(args) < 2 {
				fmt.Fprintf(os.Stderr, "Usage: pdrive info <path|number>\n")
				os.Exit(1)
			}
			runInfo(*webdavAddr, *configDir, args[1:])
			return
		case "du":
			runDu(*webdavAddr, *configDir, args[1:])
			return
		case "mount":
			runMount(*webdavAddr, *configDir, *mountPoint)
			return
		case "unmount":
			runUnmount(*webdavAddr, *configDir)
			return
		case "stop":
			stopDaemon(*configDir)
			return
		case "version":
			fmt.Printf("pdrive %s (commit %s, built %s)\n", version, commit, date)
			return
		case "remotes":
			runRemotes(*configDir, args[1:])
			return
		case "help":
			printCLIUsage()
			fmt.Fprintf(os.Stderr, "\nAll daemon flags:\n\n")
			flag.CommandLine.SetOutput(os.Stderr)
			flag.PrintDefaults()
			return
		default:
			fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n", args[0])
			fmt.Fprintln(os.Stderr)
			printCLIUsage()
			os.Exit(1)
		}
	}

	// Daemonize: unless --foreground is set, re-exec as a detached background
	// process then exit immediately. We pass --foreground to the child so it
	// skips this block and runs directly. Using a flag (not an env var) avoids
	// the risk of a leaked env var causing every invocation to skip daemonizing.
	if !*foreground {
		// Pre-flight: check FUSE availability before daemonizing so the user
		// sees the error right away instead of it being buried in daemon.log.
		if *backend == "fuse" {
			if err := fusefs.CheckFUSEAvailable(); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		}

		// Probe the API — if it responds the daemon is already running.
		hc := &http.Client{Timeout: 2 * time.Second}
		if resp, err := hc.Get("http://" + *webdavAddr + "/api/health"); err == nil {
			resp.Body.Close()
			// If user is requesting a specific backend, auto-restart.
			if *backend != "" {
				fmt.Println("Restarting pdrive daemon with new backend...")
				stopDaemon(*configDir)
				// Give the old daemon a moment to release ports.
				time.Sleep(500 * time.Millisecond)
			} else {
				fmt.Printf("pdrive daemon is already running at http://%s\n\n", *webdavAddr)
				printCLIUsage()
				return
			}
		}

		if err := os.MkdirAll(*configDir, 0700); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		logPath := filepath.Join(*configDir, "daemon.log")
		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: opening log file: %v\n", err)
			os.Exit(1)
		}
		exe, err := os.Executable()
		if err != nil {
			logFile.Close()
			fmt.Fprintf(os.Stderr, "Error: resolving executable path: %v\n", err)
			os.Exit(1)
		}
		// Re-exec self with --foreground so the child skips this daemonize block.
		childArgs := make([]string, len(os.Args[1:]))
		copy(childArgs, os.Args[1:])
		childArgs = append(childArgs, "--foreground")
		cmd := exec.Command(exe, childArgs...)
		cmd.Env = os.Environ()
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			logFile.Close()
			fmt.Fprintf(os.Stderr, "Error: failed to start daemon: %v\n", err)
			os.Exit(1)
		}
		logFile.Close()

		// Verify the daemon actually started successfully by polling /api/health.
		startOK := false
		for i := 0; i < 30; i++ {
			time.Sleep(500 * time.Millisecond)
			if resp, err := hc.Get("http://" + *webdavAddr + "/api/health"); err == nil {
				resp.Body.Close()
				startOK = true
				break
			}
			// Check if the child process already exited (startup failure).
			if cmd.ProcessState != nil {
				break
			}
		}

		if startOK {
			fmt.Printf("pdrive daemon started (PID %d)\n", cmd.Process.Pid)
			fmt.Printf("  Logs: %s\n", logPath)
			fmt.Printf("  Stop: pdrive stop\n")
		} else {
			// Daemon failed to start — show the tail of the log.
			fmt.Fprintf(os.Stderr, "Error: daemon failed to start. Recent log output:\n\n")
			showRecentLogErrors(logPath)
			os.Exit(1)
		}
		return
	}

	// Resolve encryption key.
	// Priority: --enc-key (raw hex) > --password (Argon2id) > existing salt file > existing key file > interactive prompt
	// When a password is given but no local salt exists, we defer key derivation
	// to the daemon (which will try to fetch the salt from cloud first).
	var encKey []byte
	var deferredPassword string // set when key derivation is deferred to daemon
	saltPath := filepath.Join(*configDir, "enc.salt")
	keyPath := filepath.Join(*configDir, "enc.key")

	switch {
	case *encKeyHex != "":
		// Legacy: raw 32-byte hex key.
		var err error
		encKey, err = hex.DecodeString(*encKeyHex)
		if err != nil || len(encKey) != 32 {
			fmt.Fprintf(os.Stderr, "Error: --enc-key must be a 64-character hex string (32 bytes)\n")
			os.Exit(1)
		}

	case *password != "":
		// Password given — derive immediately if local salt exists,
		// otherwise defer to daemon for cloud salt lookup.
		if salt, err := os.ReadFile(saltPath); err == nil && len(salt) == chunker.SaltSize {
			encKey = chunker.DeriveKey(*password, salt)
		} else {
			deferredPassword = *password
		}

	default:
		// Auto-detect: try salt file first (password-derived), then legacy key file.
		if salt, err := os.ReadFile(saltPath); err == nil && len(salt) == chunker.SaltSize {
			// Salt exists but no password on CLI — prompt interactively.
			fmt.Fprint(os.Stderr, "Enter pdrive password: ")
			pw, err := readPassword()
			if err != nil || pw == "" {
				fmt.Fprintf(os.Stderr, "\nError: password required (salt file exists at %s)\n", saltPath)
				os.Exit(1)
			}
			encKey = chunker.DeriveKey(pw, salt)
		} else if data, err := os.ReadFile(keyPath); err == nil && len(data) == 32 {
			// Legacy raw key file.
			encKey = data
		} else {
			// First run — prompt for a password.
			fmt.Fprintln(os.Stderr, "No encryption key found. Set up a password for pdrive.")
			fmt.Fprintln(os.Stderr, "This password encrypts all your data. Remember it — it cannot be recovered.")
			fmt.Fprintln(os.Stderr)
			fmt.Fprint(os.Stderr, "Enter new password: ")
			pw1, err := readPassword()
			if err != nil || len(pw1) < 8 {
				fmt.Fprintf(os.Stderr, "\nError: password must be at least 8 characters\n")
				os.Exit(1)
			}
			fmt.Fprint(os.Stderr, "\nConfirm password: ")
			pw2, _ := readPassword()
			if pw1 != pw2 {
				fmt.Fprintf(os.Stderr, "\nError: passwords do not match\n")
				os.Exit(1)
			}
			fmt.Fprintln(os.Stderr)
			// Defer to daemon — it will try cloud salt first, then generate fresh.
			deferredPassword = pw1
		}
	}

	// Find rclone binary — honor --rclone-bin if given.
	rcloneBin := *rcloneBinFlag
	if rcloneBin == "" {
		var err error
		rcloneBin, err = exec.LookPath("rclone")
		if err != nil {
			// Try bundled rclone next to our binary.
			exe, _ := os.Executable()
			bundled := filepath.Join(filepath.Dir(exe), "rclone")
			if _, err := os.Stat(bundled); err == nil {
				rcloneBin = bundled
			} else {
				// Auto-download rclone into ~/.pdrive/bin/rclone.
				rcloneBin, err = rclonebin.EnsureRclone(*configDir)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: rclone not found and auto-download failed: %v\n", err)
					fmt.Fprintf(os.Stderr, "Install rclone manually from https://rclone.org/install/\n")
					os.Exit(1)
				}
			}
		}
	}

	// Write PID file so we can detect a running instance and support `pdrive stop`.
	pidFile := filepath.Join(*configDir, "daemon.pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0600); err != nil {
		slog.Warn("could not write PID file", "error", err)
	} else {
		defer os.Remove(pidFile)
	}

	// Parse --remotes flag into a slice.
	var remotes []string
	if *remotesFlag != "" {
		for _, r := range strings.Split(*remotesFlag, ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				remotes = append(remotes, r)
			}
		}
	}

	cfg := daemon.Config{
		ConfigDir:    *configDir,
		RcloneBin:    rcloneBin,
		RcloneAddr:   *rcloneAddr,
		WebDAVAddr:   *webdavAddr,
		SyncDir:      *syncDir,
		EncKey:       encKey,
		Password:     deferredPassword,
		BrokerPolicy: *brokerPolicy,
		MinFreeSpace: *minFreeSpace,
		SkipRestore:  *skipRestore,
		ChunkSize:    *chunkSize,
		RatePerSec:   *rateLimit,
		Remotes:      remotes,
		MountBackend: *backend,
		MountPoint:   *mountPoint,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := daemon.New(cfg)
	if err := d.Start(ctx); err != nil {
		slog.Error("failed to start daemon", "error", err)
		os.Exit(1)
	}

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	slog.Info("received signal, shutting down", "signal", sig)
	cancel()
	d.Stop()
}

// readPidFile reads an integer PID from a file.
func readPidFile(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	var pid int
	if _, err := fmt.Sscan(strings.TrimSpace(string(data)), &pid); err != nil {
		return 0, err
	}
	return pid, nil
}

// stopDaemon sends SIGTERM to the running daemon process.
func stopDaemon(configDir string) {
	pidFile := filepath.Join(configDir, "daemon.pid")
	pid, err := readPidFile(pidFile)
	if err != nil {
		fmt.Println("pdrive daemon is not running.")
		return
	}
	proc, err := os.FindProcess(pid)
	if err != nil || proc.Signal(syscall.Signal(0)) != nil {
		fmt.Println("pdrive daemon is not running.")
		os.Remove(pidFile)
		return
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to stop daemon (PID %d): %v\n", pid, err)
		os.Exit(1)
	}
	fmt.Printf("pdrive daemon stopped (PID %d)\n", pid)
}

// runMount stops any running daemon and restarts with the FUSE backend.
func runMount(addr, configDir, mountPoint string) {
	// Pre-flight: check FUSE availability before doing anything.
	if err := fusefs.CheckFUSEAvailable(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Stop existing daemon if running.
	hc := &http.Client{Timeout: 2 * time.Second}
	if resp, err := hc.Get("http://" + addr + "/api/health"); err == nil {
		resp.Body.Close()
		fmt.Println("Stopping existing daemon...")
		stopDaemon(configDir)
		time.Sleep(500 * time.Millisecond)
	}

	// Re-exec ourselves with --backend=fuse.
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	args := []string{"--backend=fuse"}
	if mountPoint != "" {
		args = append(args, "--mountpoint="+mountPoint)
	}
	// Pass through any other flags from os.Args that aren't subcommands.
	for _, a := range os.Args[1:] {
		if a == "mount" || strings.HasPrefix(a, "--mountpoint") || strings.HasPrefix(a, "--backend") {
			continue
		}
		args = append(args, a)
	}
	cmd := exec.Command(exe, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
}

// runUnmount tells the daemon to stop (which unmounts FUSE).
func runUnmount(addr, configDir string) {
	// Unmounting is equivalent to stopping the daemon — the daemon's
	// Stop() method takes care of FUSE unmount.
	stopDaemon(configDir)
}

// showRecentLogErrors prints the last few ERROR/WARN lines from the daemon log.
func showRecentLogErrors(logPath string) {
	data, err := os.ReadFile(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  (could not read %s)\n", logPath)
		return
	}
	lines := strings.Split(string(data), "\n")
	// Show last 10 lines that contain useful info.
	start := len(lines) - 10
	if start < 0 {
		start = 0
	}
	for _, line := range lines[start:] {
		line = strings.TrimSpace(line)
		if line != "" {
			fmt.Fprintf(os.Stderr, "  %s\n", line)
		}
	}
}

// readPassword reads a password from stdin. Hides input when stdin is a terminal.
func readPassword() (string, error) {
	fd := int(os.Stdin.Fd())
	if term.IsTerminal(fd) {
		pw, err := term.ReadPassword(fd)
		return string(pw), err
	}
	// Non-interactive (pipe/redirect): read a line.
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		return strings.TrimSpace(scanner.Text()), nil
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", io.EOF
}

// runPinUnpin calls the running daemon's /api/pin or /api/unpin endpoint.
func runPinUnpin(addr, configDir, action string, paths []string) {
	for _, p := range paths {
		p = resolveLsArg(p, configDir).Path
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
		apiURL := fmt.Sprintf("http://%s/api/%s?path=%s", addr, action, url.QueryEscape(p))
		resp, err := http.Post(apiURL, "", nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: cannot reach daemon at %s: %v\n", addr, err)
			os.Exit(1)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			fmt.Fprintf(os.Stderr, "%s %s: %s\n", action, p, string(body))
			os.Exit(1)
		}
		switch action {
		case "pin":
			fmt.Printf("Downloaded: %s\n", p)
		case "unpin":
			fmt.Printf("Evicted local data: %s\n", p)
		}
	}
}
