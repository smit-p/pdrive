package fusefs

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/smit-p/pdrive/internal/engine"
)

// Server manages the FUSE mount lifecycle.
type Server struct {
	mountPoint string
	fuseServer *fuse.Server
}

// CheckFUSEAvailable verifies that the FUSE driver is installed and loaded.
// On macOS this means macFUSE; on Linux it checks for /dev/fuse.
// Returns nil if FUSE is ready, or an actionable error message.
func CheckFUSEAvailable() error {
	switch runtime.GOOS {
	case "darwin":
		// Check if macFUSE filesystem bundle exists.
		if _, err := os.Stat("/Library/Filesystems/macfuse.fs"); err != nil {
			return errors.New("macFUSE is not installed\n\n" +
				"  Install it with:  brew install --cask macfuse\n" +
				"  Then reboot and approve the system extension in\n" +
				"  System Settings → Privacy & Security")
		}
		// Check if the kernel extension is loaded.
		out, err := exec.Command("kextstat").Output()
		if err == nil && strings.Contains(string(out), "macfuse") {
			return nil
		}
		// Also check system extensions (newer macOS).
		out, err = exec.Command("systemextensionsctl", "list").Output()
		if err == nil && strings.Contains(string(out), "macfuse") {
			return nil
		}
		// macFUSE is installed but kext isn't loaded — try loading it.
		if tryLoadMacFUSEKext() {
			return nil // loaded successfully
		}
		return errors.New("macFUSE is installed but the kernel extension is not loaded\n\n" +
			"  Try loading it manually:\n" +
			"    sudo kextload /Library/Filesystems/macfuse.fs/Contents/Extensions/$(sw_vers -productVersion | cut -d. -f1)/macfuse.kext\n\n" +
			"  If that doesn't work, check System Settings → Privacy & Security\n" +
			"  for a blocked system extension and approve it, then reboot")

	case "linux":
		if _, err := os.Stat("/dev/fuse"); err != nil {
			return errors.New("FUSE device /dev/fuse not found.\n\n" +
				"  Install FUSE:  sudo apt install fuse3  (Debian/Ubuntu)\n" +
				"            or:  sudo dnf install fuse3  (Fedora)")
		}
		return nil

	default:
		return fmt.Errorf("FUSE is not supported on %s", runtime.GOOS)
	}
}

// tryLoadMacFUSEKext attempts to load the macFUSE kernel extension.
// Returns true if the kext was loaded successfully.
func tryLoadMacFUSEKext() bool {
	// Determine the major macOS version for the correct kext path.
	out, err := exec.Command("sw_vers", "-productVersion").Output()
	if err != nil {
		return false
	}
	ver := strings.TrimSpace(string(out))
	major, _, _ := strings.Cut(ver, ".")
	kextPath := fmt.Sprintf("/Library/Filesystems/macfuse.fs/Contents/Extensions/%s/macfuse.kext", major)
	if _, err := os.Stat(kextPath); err != nil {
		return false
	}
	// Try loading — requires sudo, so this will only work if the user has
	// passwordless sudo or if run from a privileged context. Silently fail
	// otherwise and fall through to the error message with the manual command.
	if err := exec.Command("sudo", "-n", "kextload", kextPath).Run(); err != nil {
		return false
	}
	// Verify it actually loaded.
	out, err = exec.Command("kextstat").Output()
	return err == nil && strings.Contains(string(out), "macfuse")
}

// Mount creates a FUSE filesystem backed by the pdrive engine and mounts it
// at the given mountpoint. The caller must call Unmount to cleanly release
// the mount when shutting down.
func Mount(mountPoint string, eng *engine.Engine, spoolDir string) (*Server, error) {
	if err := CheckFUSEAvailable(); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		if os.IsPermission(err) {
			return nil, fmt.Errorf("cannot create mount point %s (permission denied).\n\n"+
				"  Fix with:  sudo mkdir -p %s && sudo chown $USER %s\n"+
				"  Or use a path you own:  --mountpoint=~/pdrive",
				mountPoint, mountPoint, mountPoint)
		}
		return nil, fmt.Errorf("creating mount point: %w", err)
	}

	root := NewRoot(eng, spoolDir)

	timeout := attrTimeout
	opts := &fs.Options{
		EntryTimeout: &timeout,
		AttrTimeout:  &timeout,
		MountOptions: fuse.MountOptions{
			Name:          "pdrive",
			FsName:        "pdrive",
			AllowOther:    false,
			DisableXAttrs: true,
			MaxReadAhead:  128 * 1024,
			Debug:         false,
			Options:       []string{"volname=PDrive"},
		},
	}

	server, err := fs.Mount(mountPoint, root, opts)
	if err != nil {
		return nil, fmt.Errorf("mounting FUSE filesystem at %s: %w", mountPoint, err)
	}

	slog.Info("FUSE filesystem mounted", "mountpoint", mountPoint)
	return &Server{mountPoint: mountPoint, fuseServer: server}, nil
}

// Unmount cleanly unmounts the FUSE filesystem.
func (s *Server) Unmount() error {
	if s.fuseServer == nil {
		return nil
	}
	slog.Info("unmounting FUSE filesystem", "mountpoint", s.mountPoint)
	if err := s.fuseServer.Unmount(); err != nil {
		return fmt.Errorf("unmounting %s: %w", s.mountPoint, err)
	}
	// Give the kernel a moment to release the mount.
	time.Sleep(100 * time.Millisecond)
	return nil
}

// Wait blocks until the FUSE server exits (e.g., after unmount).
func (s *Server) Wait() {
	if s.fuseServer != nil {
		s.fuseServer.Wait()
	}
}

// MountPoint returns the path where the filesystem is mounted.
func (s *Server) MountPoint() string {
	return s.mountPoint
}
