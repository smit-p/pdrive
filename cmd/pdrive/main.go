package main

import (
	"context"
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

	"github.com/smit-p/pdrive/internal/daemon"
	"github.com/smit-p/pdrive/internal/fusefs"
)

// Set by goreleaser ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	cfg, err := parseAndMergeConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Handle subcommands early, before any rclone lookup — these are all pure
	// HTTP calls to the running daemon and don't need rclone.
	if handled, err := dispatchCmd(cfg.Args, cfg.WebDAVAddr, cfg.ConfigDir, cfg.MountPoint); handled {
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Daemonize: unless --foreground is set, re-exec as a detached background
	// process then exit immediately. We pass --foreground to the child so it
	// skips this block and runs directly. Using a flag (not an env var) avoids
	// the risk of a leaked env var causing every invocation to skip daemonizing.
	if !cfg.Foreground {
		// Pre-flight: check FUSE availability before daemonizing so the user
		// sees the error right away instead of it being buried in daemon.log.
		if cfg.Backend == "fuse" {
			if err := fusefs.CheckFUSEAvailable(); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		}

		// Probe the API — if it responds the daemon is already running.
		if probeDaemonRunning(cfg.WebDAVAddr) {
			// If user is requesting a specific backend, auto-restart.
			if cfg.Backend != "" {
				fmt.Println("Restarting pdrive daemon with new backend...")
				stopDaemon(cfg.ConfigDir)
				// Give the old daemon a moment to release ports.
				time.Sleep(500 * time.Millisecond)
			} else {
				fmt.Printf("pdrive daemon is already running at http://%s\n\n", cfg.WebDAVAddr)
				printCLIUsage()
				return
			}
		}

		logFile, logPath, err := openDaemonLog(cfg.ConfigDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		exe, err := os.Executable()
		if err != nil {
			logFile.Close()
			fmt.Fprintf(os.Stderr, "Error: resolving executable path: %v\n", err)
			os.Exit(1)
		}
		// Re-exec self with --foreground so the child skips this daemonize block.
		cmd := buildChildCmd(exe, os.Args[1:], logFile)
		if err := cmd.Start(); err != nil {
			logFile.Close()
			fmt.Fprintf(os.Stderr, "Error: failed to start daemon: %v\n", err)
			os.Exit(1)
		}
		logFile.Close()

		// Verify the daemon actually started successfully by polling /api/health.
		startOK := pollDaemonStart(cfg.WebDAVAddr, cmd, 30)

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

	// Find rclone binary.
	rcloneBin, err := findRcloneBin(cfg.RcloneBin, cfg.ConfigDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: rclone not found: %v\n", err)
		os.Exit(1)
	}

	// Write PID file so we can detect a running instance and support `pdrive stop`.
	pidFile := filepath.Join(cfg.ConfigDir, "daemon.pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0600); err != nil {
		slog.Warn("could not write PID file", "error", err)
	} else {
		defer os.Remove(pidFile)
	}

	// Parse --remotes flag into a slice.
	remotes := parseRemotes(cfg.Remotes)

	dcfg := buildDaemonConfig(cfg.ConfigDir, rcloneBin, cfg.RcloneAddr, cfg.WebDAVAddr, cfg.SyncDir,
		cfg.BrokerPolicy,
		cfg.MinFreeSpace, cfg.SkipRestore,
		cfg.ChunkSize, cfg.RateLimit, remotes,
		cfg.Erasure, cfg.Backend, cfg.MountPoint)
	dcfg.LogHandler = cfg.LogHandler

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := daemon.New(dcfg)
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

// runPinUnpin calls the running daemon's /api/pin or /api/unpin endpoint.
func runPinUnpin(addr, configDir, action string, paths []string) {
	for _, p := range paths {
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
