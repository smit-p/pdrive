package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/smit-p/pdrive/internal/daemon"
)

func main() {
	homeDir, _ := os.UserHomeDir()
	defaultConfigDir := filepath.Join(homeDir, ".pdrive")

	configDir := flag.String("config-dir", defaultConfigDir, "Configuration directory")
	rcloneAddr := flag.String("rclone-addr", "localhost:5572", "rclone RC address")
	webdavAddr := flag.String("webdav-addr", "localhost:8765", "WebDAV server address")
	encKeyHex := flag.String("enc-key", "", "Encryption key (64-char hex string for AES-256). If empty, uses a test key.")
	brokerPolicy := flag.String("broker-policy", "pfrd", "Chunk placement policy: pfrd (weighted random by free space) or mfs (most free space)")
	minFreeSpace := flag.Int64("min-free-space", 256*1024*1024, "Minimum free space (bytes) to keep on each provider (default 256 MB)")
	skipRestore := flag.Bool("skip-restore", false, "Skip restoring metadata DB from cloud on startup (use after a manual wipe)")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

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

	// Find rclone binary.
	rcloneBin, err := exec.LookPath("rclone")
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

	cfg := daemon.Config{
		ConfigDir:    *configDir,
		RcloneBin:    rcloneBin,
		RcloneAddr:   *rcloneAddr,
		WebDAVAddr:   *webdavAddr,
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
