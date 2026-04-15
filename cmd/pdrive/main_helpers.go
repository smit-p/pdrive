package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/smit-p/pdrive/internal/config"
	"github.com/smit-p/pdrive/internal/daemon"
	"github.com/smit-p/pdrive/internal/logutil"
	"github.com/smit-p/pdrive/internal/rclonebin"
)

// findRcloneBin locates the rclone binary from the given flag, PATH, a
// bundled binary next to the executable, or auto-download.
func findRcloneBin(rcloneBinFlag, configDir string) (string, error) {
	if rcloneBinFlag != "" {
		return rcloneBinFlag, nil
	}
	if bin, err := exec.LookPath("rclone"); err == nil {
		return bin, nil
	}
	exe, _ := os.Executable()
	bundled := filepath.Join(filepath.Dir(exe), "rclone")
	if _, err := os.Stat(bundled); err == nil {
		return bundled, nil
	}
	return rclonebin.EnsureRclone(configDir)
}

// parseRemotes splits a comma-separated remotes flag into a slice.
func parseRemotes(flag string) []string {
	if flag == "" {
		return nil
	}
	var remotes []string
	for _, r := range strings.Split(flag, ",") {
		r = strings.TrimSpace(r)
		if r != "" {
			remotes = append(remotes, r)
		}
	}
	return remotes
}

// mergeConfigFile applies config-file values where CLI flags were not
// explicitly set (i.e. still at their defaults).
func mergeConfigFile(fileCfg config.File, homeDir string, opts *cliOptions) {
	if fileCfg.SyncDir != "" && opts.SyncDir == filepath.Join(homeDir, "pdrive") {
		opts.SyncDir = fileCfg.SyncDir
	}
	if fileCfg.RcloneAddr != "" && opts.RcloneAddr == "127.0.0.1:5572" {
		opts.RcloneAddr = fileCfg.RcloneAddr
	}
	if fileCfg.WebDAVAddr != "" && opts.WebDAVAddr == "127.0.0.1:8765" {
		opts.WebDAVAddr = fileCfg.WebDAVAddr
	}
	if fileCfg.RcloneBin != "" && opts.RcloneBin == "" {
		opts.RcloneBin = fileCfg.RcloneBin
	}
	if fileCfg.BrokerPolicy != "" && opts.BrokerPolicy == "pfrd" {
		opts.BrokerPolicy = fileCfg.BrokerPolicy
	}
	if fileCfg.MinFreeSpace > 0 && opts.MinFreeSpace == 256*1024*1024 {
		opts.MinFreeSpace = fileCfg.MinFreeSpace
	}
	if fileCfg.ChunkSize > 0 && opts.ChunkSize == 0 {
		opts.ChunkSize = fileCfg.ChunkSize
	}
	if fileCfg.RateLimit > 0 && opts.RateLimit == 0 {
		opts.RateLimit = fileCfg.RateLimit
	}
	if fileCfg.Debug && !opts.Debug {
		opts.Debug = true
	}
	if fileCfg.Remotes != "" && opts.Remotes == "" {
		opts.Remotes = fileCfg.Remotes
	}
	if fileCfg.Erasure != "" && opts.Erasure == "" {
		opts.Erasure = fileCfg.Erasure
	}
	if fileCfg.MountBackend != "" && opts.Backend == "" {
		opts.Backend = fileCfg.MountBackend
	}
	if fileCfg.MountPoint != "" && opts.MountPoint == "" {
		opts.MountPoint = fileCfg.MountPoint
	}
}

// cliOptions holds the parsed CLI flag values so they can be passed to
// mergeConfigFile for config-file overrides.
type cliOptions struct {
	SyncDir      string
	RcloneAddr   string
	WebDAVAddr   string
	RcloneBin    string
	BrokerPolicy string
	MinFreeSpace int64
	ChunkSize    int
	RateLimit    int
	Debug        bool
	Remotes      string
	Erasure      string
	Backend      string
	MountPoint   string
}

// dispatchCmd routes a CLI subcommand to the appropriate handler.
// Returns (true, nil) when the command was handled, (true, err) for
// argument-validation errors, or (false, nil) when no subcommand was given.
func dispatchCmd(args []string, addr, configDir, mountPoint string) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}
	switch args[0] {
	case "pin", "unpin":
		if len(args) < 2 {
			return true, fmt.Errorf("usage: pdrive %s <path>", args[0])
		}
		runPinUnpin(addr, configDir, args[0], args[1:])
	case "ls":
		runLs(addr, configDir, args[1:])
	case "browse":
		runBrowse(addr, configDir)
	case "status":
		runStatus(addr)
	case "uploads":
		runUploads(addr)
	case "health":
		runHealth(addr)
	case "metrics":
		runMetrics(addr)
	case "cat":
		runCat(addr, configDir, args[1:])
	case "get":
		runGet(addr, configDir, args[1:])
	case "put":
		if len(args) < 2 {
			return true, fmt.Errorf("usage: pdrive put <local-path> [remote-dir]")
		}
		runPut(addr, args[1:])
	case "rm":
		if len(args) < 2 {
			return true, fmt.Errorf("usage: pdrive rm <path> [<path>...]")
		}
		runRm(addr, configDir, args[1:])
	case "tree":
		runTree(addr, configDir, args[1:])
	case "find":
		if len(args) < 2 {
			return true, fmt.Errorf("usage: pdrive find <pattern> [path]")
		}
		runFind(addr, configDir, args[1:])
	case "mv":
		if len(args) < 3 {
			return true, fmt.Errorf("usage: pdrive mv <src> <dst>")
		}
		runMv(addr, configDir, args[1:])
	case "mkdir":
		if len(args) < 2 {
			return true, fmt.Errorf("usage: pdrive mkdir <path>")
		}
		runMkdir(addr, args[1:])
	case "info":
		if len(args) < 2 {
			return true, fmt.Errorf("usage: pdrive info <path>")
		}
		runInfo(addr, configDir, args[1:])
	case "du":
		runDu(addr, configDir, args[1:])
	case "mount":
		runMount(addr, configDir, mountPoint)
	case "unmount":
		runUnmount(addr, configDir)
	case "stop":
		stopDaemon(configDir)
	case "version":
		fmt.Printf("pdrive %s (commit %s, built %s)\n", version, commit, date)
	case "remotes":
		runRemotes(configDir, addr, args[1:])
	case "help":
		printCLIUsage()
		fmt.Fprintf(os.Stderr, "\nAll daemon flags:\n\n")
		flag.CommandLine.PrintDefaults()
	default:
		return true, fmt.Errorf("unknown subcommand: %s", args[0])
	}
	return true, nil
}

// probeDaemonRunning probes the daemon's /api/health endpoint.
// Returns true if the daemon is already running.
func probeDaemonRunning(addr string) bool {
	hc := &http.Client{Timeout: 2 * time.Second}
	resp, err := hc.Get("http://" + addr + "/api/health")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}

// openDaemonLog creates the config directory and opens the daemon log file for
// appending.  Returns the file handle, log path, and any error.
func openDaemonLog(configDir string) (*os.File, string, error) {
	if err := os.MkdirAll(configDir, 0700); err != nil {
		return nil, "", fmt.Errorf("creating config dir: %w", err)
	}
	logPath := filepath.Join(configDir, "daemon.log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, logPath, fmt.Errorf("opening log file: %w", err)
	}
	return f, logPath, nil
}

// buildChildCmd creates the exec.Cmd for the daemon child process.
func buildChildCmd(exe string, origArgs []string, logFile *os.File) *exec.Cmd {
	childArgs := make([]string, len(origArgs))
	copy(childArgs, origArgs)
	childArgs = append(childArgs, "--foreground")
	cmd := exec.Command(exe, childArgs...)
	cmd.Env = os.Environ()
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd
}

// pollDaemonStart polls the daemon's health endpoint until it responds or
// maxAttempts is exhausted.  Returns true if the daemon is healthy.
func pollDaemonStart(addr string, cmd *exec.Cmd, maxAttempts int) bool {
	hc := &http.Client{Timeout: 2 * time.Second}
	for i := 0; i < maxAttempts; i++ {
		time.Sleep(500 * time.Millisecond)
		if resp, err := hc.Get("http://" + addr + "/api/health"); err == nil {
			resp.Body.Close()
			return true
		}
		if cmd.ProcessState != nil {
			return false
		}
	}
	return false
}

// buildDaemonConfig creates the daemon.Config from CLI options.
func buildDaemonConfig(configDir, rcloneBin, rcloneAddr, webdavAddr, syncDir string,
	brokerPolicy string,
	minFreeSpace int64, skipRestore bool,
	chunkSize, ratePerSec int, remotes []string,
	erasure, backend, mountPoint string) daemon.Config {
	return daemon.Config{
		ConfigDir:    configDir,
		RcloneBin:    rcloneBin,
		RcloneAddr:   rcloneAddr,
		WebDAVAddr:   webdavAddr,
		SyncDir:      syncDir,
		BrokerPolicy: brokerPolicy,
		MinFreeSpace: minFreeSpace,
		SkipRestore:  skipRestore,
		ChunkSize:    chunkSize,
		RatePerSec:   ratePerSec,
		Remotes:      remotes,
		Erasure:      erasure,
		MountBackend: backend,
		MountPoint:   mountPoint,
	}
}

// parsedConfig holds all configuration values after parsing CLI flags and
// merging them with the config file.
type parsedConfig struct {
	ConfigDir    string
	SyncDir      string
	RcloneAddr   string
	WebDAVAddr   string
	RcloneBin    string
	BrokerPolicy string
	MinFreeSpace int64
	SkipRestore  bool
	ChunkSize    int
	RateLimit    int
	Remotes      string
	Erasure      string
	Debug        bool
	Foreground   bool
	Backend      string
	MountPoint   string
	LogHandler   *logutil.RingHandler
	Args         []string // remaining positional arguments
}

// parseAndMergeConfig parses CLI flags from args using a dedicated FlagSet,
// loads the config file, merges defaults, and configures logging.
func parseAndMergeConfig(args []string) (*parsedConfig, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("could not determine home directory: %w", err)
	}
	defaultConfigDir := filepath.Join(homeDir, ".pdrive")

	fs := flag.NewFlagSet("pdrive", flag.ContinueOnError)
	cfg := &parsedConfig{}

	fs.StringVar(&cfg.ConfigDir, "config-dir", defaultConfigDir, "Configuration directory")
	fs.StringVar(&cfg.SyncDir, "sync-dir", filepath.Join(homeDir, "pdrive"), "Local folder to sync")
	fs.StringVar(&cfg.RcloneAddr, "rclone-addr", "127.0.0.1:5572", "rclone RC address")
	fs.StringVar(&cfg.WebDAVAddr, "webdav-addr", "127.0.0.1:8765", "HTTP API/WebDAV address")
	fs.StringVar(&cfg.RcloneBin, "rclone-bin", "", "Absolute path to rclone binary")
	fs.StringVar(&cfg.BrokerPolicy, "broker-policy", "pfrd", "Chunk placement policy")
	fs.Int64Var(&cfg.MinFreeSpace, "min-free-space", 256*1024*1024, "Minimum free space (bytes)")
	fs.BoolVar(&cfg.SkipRestore, "skip-restore", false, "Skip restoring metadata DB")
	fs.IntVar(&cfg.ChunkSize, "chunk-size", 0, "Override chunk size in bytes")
	fs.IntVar(&cfg.RateLimit, "rate-limit", 0, "API rate limit in requests per second")
	fs.StringVar(&cfg.Remotes, "remotes", "", "Comma-separated list of rclone remote names")
	fs.BoolVar(&cfg.Debug, "debug", false, "Enable debug logging")
	fs.BoolVar(&cfg.Foreground, "foreground", false, "Run daemon in foreground")
	fs.StringVar(&cfg.Erasure, "erasure", "", "Reed-Solomon erasure coding (e.g. 3+1 = 3 data + 1 parity shard)")
	fs.StringVar(&cfg.Backend, "backend", "", "Mount backend: webdav (default) or fuse")
	fs.StringVar(&cfg.MountPoint, "mountpoint", "", "FUSE mount point")

	fs.Usage = func() { printCLIUsage() }

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	cfg.Args = fs.Args()

	// Load config file — CLI flags override config values.
	fileCfg, _ := config.Load(cfg.ConfigDir)
	opts := cliOptions{
		SyncDir: cfg.SyncDir, RcloneAddr: cfg.RcloneAddr, WebDAVAddr: cfg.WebDAVAddr,
		RcloneBin: cfg.RcloneBin, BrokerPolicy: cfg.BrokerPolicy, MinFreeSpace: cfg.MinFreeSpace,
		ChunkSize: cfg.ChunkSize, RateLimit: cfg.RateLimit, Debug: cfg.Debug,
		Remotes: cfg.Remotes, Erasure: cfg.Erasure, Backend: cfg.Backend, MountPoint: cfg.MountPoint,
	}
	mergeConfigFile(fileCfg, homeDir, &opts)
	cfg.SyncDir = opts.SyncDir
	cfg.RcloneAddr = opts.RcloneAddr
	cfg.WebDAVAddr = opts.WebDAVAddr
	cfg.RcloneBin = opts.RcloneBin
	cfg.BrokerPolicy = opts.BrokerPolicy
	cfg.MinFreeSpace = opts.MinFreeSpace
	cfg.ChunkSize = opts.ChunkSize
	cfg.RateLimit = opts.RateLimit
	cfg.Debug = opts.Debug
	cfg.Remotes = opts.Remotes
	cfg.Erasure = opts.Erasure
	cfg.Backend = opts.Backend
	cfg.MountPoint = opts.MountPoint

	// Configure logging.
	logLevel := slog.LevelInfo
	if cfg.Debug {
		logLevel = slog.LevelDebug
	}
	textHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	ringHandler := logutil.NewRingHandler(textHandler, 1000)
	slog.SetDefault(slog.New(ringHandler))
	cfg.LogHandler = ringHandler

	return cfg, nil
}
