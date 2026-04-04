package main

import (
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
	"text/template"

	"github.com/smit-p/pdrive/internal/daemon"
)

const launchAgentLabel = "com.smit.pdrive"

// launchAgentPlist is the launchd plist template for auto-restart on macOS.
var launchAgentPlist = template.Must(template.New("plist").Parse(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{{.Label}}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{{.BinPath}}</string>
		{{- range .Args}}
		<string>{{.}}</string>
		{{- end}}
	</array>
	<key>KeepAlive</key>
	<true/>
	<key>RunAtLoad</key>
	<true/>
	<key>StandardOutPath</key>
	<string>{{.LogPath}}</string>
	<key>StandardErrorPath</key>
	<string>{{.LogPath}}</string>
	<key>ThrottleInterval</key>
	<integer>10</integer>
</dict>
</plist>
`))

func main() {
	homeDir, _ := os.UserHomeDir()
	defaultConfigDir := filepath.Join(homeDir, ".pdrive")

	configDir := flag.String("config-dir", defaultConfigDir, "Configuration directory")
	syncDir := flag.String("sync-dir", filepath.Join(homeDir, "pdrive"), "Local folder to sync (like Dropbox); empty disables sync")
	rcloneAddr := flag.String("rclone-addr", "127.0.0.1:5572", "rclone RC address")
	webdavAddr := flag.String("webdav-addr", "127.0.0.1:8765", "HTTP API/WebDAV address")
	rcloneBinFlag := flag.String("rclone-bin", "", "Absolute path to rclone binary (auto-detected if empty)")
	encKeyHex := flag.String("enc-key", "", "Encryption key (64-char hex string for AES-256). If empty, uses a test key.")
	brokerPolicy := flag.String("broker-policy", "pfrd", "Chunk placement policy: pfrd (weighted random by free space) or mfs (most free space)")
	minFreeSpace := flag.Int64("min-free-space", 256*1024*1024, "Minimum free space (bytes) to keep on each provider (default 256 MB)")
	skipRestore := flag.Bool("skip-restore", false, "Skip restoring metadata DB from cloud on startup (use after a manual wipe)")
	debug := flag.Bool("debug", false, "Enable debug logging")
	install := flag.Bool("install", false, "Install pdrive as a launchd service (macOS) that auto-restarts on crash/reboot")
	uninstall := flag.Bool("uninstall", false, "Remove the launchd service installed by --install")
	flag.Parse()

	// Handle --uninstall before anything else.
	if *uninstall {
		if err := uninstallLaunchAgent(homeDir); err != nil {
			fmt.Fprintf(os.Stderr, "uninstall failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("pdrive launchd service removed.")
		return
	}

	// Configure logging.
	logLevel := slog.LevelInfo
	if *debug {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))

	// Resolve encryption key.
	var encKey []byte
	if *encKeyHex != "" {
		var err error
		encKey, err = hex.DecodeString(*encKeyHex)
		if err != nil || len(encKey) != 32 {
			fmt.Fprintf(os.Stderr, "Error: --enc-key must be a 64-character hex string (32 bytes)\n")
			os.Exit(1)
		}
	} else {
		// v0 hardcoded test key — replaced with PBKDF2 in v1.
		encKey = []byte("pdrive-test-key-0123456789abcdef")
		slog.Warn("using hardcoded test encryption key — do not use in production")
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
				fmt.Fprintf(os.Stderr, "Error: rclone not found. Install it: brew install rclone\n")
				os.Exit(1)
			}
		}
	}

	// --install: copy binary to ~/.pdrive/bin/ and register a launchd agent.
	if *install {
		if err := installLaunchAgent(homeDir, *configDir, *syncDir, *webdavAddr, *rcloneAddr, *encKeyHex, *brokerPolicy, *minFreeSpace, *debug, rcloneBin); err != nil {
			fmt.Fprintf(os.Stderr, "install failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("pdrive installed as launchd service. It will start now and restart automatically on crash or reboot.")
		fmt.Printf("  Binary: %s\n", filepath.Join(homeDir, ".pdrive", "bin", "pdrive"))
		fmt.Printf("  Plist:  %s\n", launchAgentPlistPath(homeDir))
		fmt.Printf("  Logs:   %s\n", filepath.Join(homeDir, ".pdrive", "daemon.log"))
		return
	}

	// Handle subcommands: pin / unpin
	if args := flag.Args(); len(args) > 0 {
		switch args[0] {
		case "pin", "unpin":
			if len(args) < 2 {
				fmt.Fprintf(os.Stderr, "Usage: pdrive %s <path>\n", args[0])
				os.Exit(1)
			}
			runPinUnpin(*webdavAddr, args[0], args[1:])
			return
		default:
			fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n", args[0])
			os.Exit(1)
		}
	}

	cfg := daemon.Config{
		ConfigDir:    *configDir,
		RcloneBin:    rcloneBin,
		RcloneAddr:   *rcloneAddr,
		WebDAVAddr:   *webdavAddr,
		SyncDir:      *syncDir,
		EncKey:       encKey,
		BrokerPolicy: *brokerPolicy,
		MinFreeSpace: *minFreeSpace,
		SkipRestore:  *skipRestore,
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

func launchAgentPlistPath(homeDir string) string {
	return filepath.Join(homeDir, "Library", "LaunchAgents", launchAgentLabel+".plist")
}

// installLaunchAgent copies the current binary to ~/.pdrive/bin/pdrive,
// writes a launchd plist, and loads the agent.
func installLaunchAgent(homeDir, configDir, syncDir, webdavAddr, rcloneAddr, encKeyHex, brokerPolicy string, minFreeSpace int64, debug bool, rcloneBin string) error {
	// Copy current binary to a persistent location.
	binDir := filepath.Join(homeDir, ".pdrive", "bin")
	if err := os.MkdirAll(binDir, 0700); err != nil {
		return fmt.Errorf("creating bin dir: %w", err)
	}
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolving executable path: %w", err)
	}
	destBin := filepath.Join(binDir, "pdrive")
	if err := copyFile(exePath, destBin, 0755); err != nil {
		return fmt.Errorf("copying binary: %w", err)
	}

	// Build the argument list for the plist (omit flags that have default values).
	args := []string{
		"--config-dir", configDir,
		"--sync-dir", syncDir,
		"--webdav-addr", webdavAddr,
		"--rclone-addr", rcloneAddr,
		// Always bake in the resolved rclone path so launchd doesn't need PATH.
		"--rclone-bin", rcloneBin,
	}
	if encKeyHex != "" {
		args = append(args, "--enc-key", encKeyHex)
	}
	if brokerPolicy != "" && brokerPolicy != "pfrd" {
		args = append(args, "--broker-policy", brokerPolicy)
	}
	if minFreeSpace != 256*1024*1024 {
		args = append(args, "--min-free-space", fmt.Sprintf("%d", minFreeSpace))
	}
	if debug {
		args = append(args, "--debug")
	}

	logPath := filepath.Join(homeDir, ".pdrive", "daemon.log")

	// Write plist file.
	plistPath := launchAgentPlistPath(homeDir)
	if err := os.MkdirAll(filepath.Dir(plistPath), 0755); err != nil {
		return fmt.Errorf("creating LaunchAgents dir: %w", err)
	}
	f, err := os.Create(plistPath)
	if err != nil {
		return fmt.Errorf("creating plist: %w", err)
	}
	defer f.Close()
	type plistData struct {
		Label   string
		BinPath string
		Args    []string
		LogPath string
	}
	if err := launchAgentPlist.Execute(f, plistData{
		Label:   launchAgentLabel,
		BinPath: destBin,
		Args:    args,
		LogPath: logPath,
	}); err != nil {
		return fmt.Errorf("rendering plist: %w", err)
	}
	f.Close()

	// Unload first in case it was previously loaded (ignore errors).
	exec.Command("launchctl", "unload", plistPath).Run() //nolint:errcheck

	// Load the agent.
	if out, err := exec.Command("launchctl", "load", plistPath).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl load: %w: %s", err, out)
	}
	return nil
}

// uninstallLaunchAgent unloads and removes the plist.
func uninstallLaunchAgent(homeDir string) error {
	plistPath := launchAgentPlistPath(homeDir)
	// Unload (ignore error if not loaded).
	exec.Command("launchctl", "unload", plistPath).Run() //nolint:errcheck
	if err := os.Remove(plistPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing plist: %w", err)
	}
	return nil
}

// copyFile copies src to dst with the given permissions.
func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	// Write to a temp file in the same dir then rename for atomicity.
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

// runPinUnpin calls the running daemon's /api/pin or /api/unpin endpoint.
func runPinUnpin(addr, action string, paths []string) {
	for _, p := range paths {
		// Normalize: allow relative paths from ~/pdrive or absolute virtual paths.
		if !filepath.IsAbs(p) || !strings.HasPrefix(p, "/") {
			// could be relative — just prefix with /
		}
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
