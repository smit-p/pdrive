// Package rclonebin handles automatic downloading of the rclone binary
// when it is not found on the system.
package rclonebin

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const rcloneVersion = "current" // "current" resolves to latest stable

// downloadBaseURL is the base URL for rclone downloads. Package-level so tests
// can redirect to httptest.Server.
var downloadBaseURL = "https://downloads.rclone.org"

// EnsureRclone checks for rclone at the given path or downloads it into
// configDir/bin/rclone. Returns the path to a usable rclone binary.
func EnsureRclone(configDir string) (string, error) {
	binDir := filepath.Join(configDir, "bin")
	dest := filepath.Join(binDir, "rclone")
	if runtime.GOOS == "windows" {
		dest += ".exe"
	}

	// Already downloaded previously — just return it.
	if info, err := os.Stat(dest); err == nil && info.Mode().IsRegular() {
		slog.Debug("using cached rclone", "path", dest)
		return dest, nil
	}

	slog.Info("rclone not found — downloading automatically...")

	goos := runtime.GOOS
	arch := runtime.GOARCH
	switch arch {
	case "amd64", "arm64":
		// supported as-is
	default:
		return "", fmt.Errorf("unsupported architecture: %s", arch)
	}

	dlURL := fmt.Sprintf("%s/%s/rclone-%s-%s-%s.zip",
		downloadBaseURL, rcloneVersion, rcloneVersion, goos, arch)

	slog.Info("downloading rclone", "url", dlURL)

	resp, err := http.Get(dlURL) //nolint:gosec // trusted first-party URL
	if err != nil {
		return "", fmt.Errorf("downloading rclone: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("downloading rclone: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading rclone download: %w", err)
	}

	// Sanity check: a real rclone zip is at least a few MB.
	if len(body) < 1024*1024 {
		return "", fmt.Errorf("rclone download suspiciously small (%d bytes), aborting", len(body))
	}

	// Extract the rclone binary from the zip.
	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return "", fmt.Errorf("opening rclone zip: %w", err)
	}

	rcloneName := "rclone"
	if runtime.GOOS == "windows" {
		rcloneName = "rclone.exe"
	}

	var rcloneFile *zip.File
	for _, f := range zr.File {
		if strings.HasSuffix(f.Name, "/"+rcloneName) || f.Name == rcloneName {
			rcloneFile = f
			break
		}
	}
	if rcloneFile == nil {
		return "", fmt.Errorf("rclone binary not found in zip archive")
	}

	rc, err := rcloneFile.Open()
	if err != nil {
		return "", fmt.Errorf("extracting rclone: %w", err)
	}
	defer func() { _ = rc.Close() }()

	if err := os.MkdirAll(binDir, 0755); err != nil {
		return "", fmt.Errorf("creating bin directory: %w", err)
	}

	out, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return "", fmt.Errorf("writing rclone binary: %w", err)
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, rc); err != nil {
		_ = os.Remove(dest)
		return "", fmt.Errorf("writing rclone binary: %w", err)
	}

	slog.Info("rclone installed", "path", dest)
	return dest, nil
}
