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

const (
	qaWorkflowPinName   = "pdrive Pin to Local"
	qaWorkflowUnpinName = "pdrive Free Up Space"
)

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
		fmt.Printf("  Binary:        %s\n", filepath.Join(homeDir, ".pdrive", "bin", "pdrive"))
		fmt.Printf("  Plist:         %s\n", launchAgentPlistPath(homeDir))
		fmt.Printf("  Logs:          %s\n", filepath.Join(homeDir, ".pdrive", "daemon.log"))
		fmt.Printf("  Quick Actions: right-click a file in ~/pdrive → Services → 'Pin to Local' or 'Free Up Space'\n")
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

	// Install Finder Quick Actions for pin/unpin (non-fatal if it fails).
	if err := installQuickActions(homeDir, syncDir); err != nil {
		slog.Warn("could not install Finder Quick Actions", "error", err)
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
	uninstallQuickActions(homeDir) //nolint:errcheck
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

// installQuickActions writes two Automator Service workflow bundles to
// ~/Library/Services/ so they appear in Finder's right-click Services menu.
func installQuickActions(homeDir, syncDir string) error {
	servicesDir := filepath.Join(homeDir, "Library", "Services")
	if err := os.MkdirAll(servicesDir, 0755); err != nil {
		return fmt.Errorf("creating Services dir: %w", err)
	}

	pdriveBin := filepath.Join(homeDir, ".pdrive", "bin", "pdrive")

	pinScript := fmt.Sprintf(`PDRIVE=%q
SYNC=%q
while IFS= read -r f; do
    vpath="${f#$SYNC}"
    [ -n "$vpath" ] && "$PDRIVE" pin "$vpath"
done`, pdriveBin, syncDir)

	unpinScript := fmt.Sprintf(`PDRIVE=%q
SYNC=%q
while IFS= read -r f; do
    vpath="${f#$SYNC}"
    [ -n "$vpath" ] && "$PDRIVE" unpin "$vpath"
done`, pdriveBin, syncDir)

	workflows := []struct {
		name   string
		title  string
		script string
		uuid   string
	}{
		{qaWorkflowPinName, "pdrive: Pin to Local", pinScript, "A1B2C3D4-0001-0001-0001-000000000001"},
		{qaWorkflowUnpinName, "pdrive: Free Up Space", unpinScript, "A1B2C3D4-0001-0001-0001-000000000002"},
	}

	for _, wf := range workflows {
		contentsDir := filepath.Join(servicesDir, wf.name+".workflow", "Contents")
		if err := os.MkdirAll(contentsDir, 0755); err != nil {
			return fmt.Errorf("creating workflow dir: %w", err)
		}
		if err := writeWorkflowDoc(contentsDir, wf.script, wf.uuid); err != nil {
			return err
		}
		if err := writeWorkflowInfo(contentsDir, wf.title); err != nil {
			return err
		}
	}

	// Restart the Pasteboard Server so macOS reloads the Services menu.
	exec.Command("killall", "pbs").Run() //nolint:errcheck
	return nil
}

// uninstallQuickActions removes the workflow bundles from ~/Library/Services/.
func uninstallQuickActions(homeDir string) error {
	servicesDir := filepath.Join(homeDir, "Library", "Services")
	for _, name := range []string{qaWorkflowPinName, qaWorkflowUnpinName} {
		p := filepath.Join(servicesDir, name+".workflow")
		if err := os.RemoveAll(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	exec.Command("killall", "pbs").Run() //nolint:errcheck
	return nil
}

// writeWorkflowDoc writes the Automator document.wflow XML for a Run Shell Script service.
func writeWorkflowDoc(contentsDir, script, uuid string) error {
	// %q-formatted paths in the script may contain backslashes on Windows but
	// are always clean on macOS. We escape & and < for XML safety.
	xmlScript := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(script)

	content := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>AMApplicationBuild</key>
	<string>521.1</string>
	<key>AMApplicationVersion</key>
	<string>2.10</string>
	<key>AMDocumentVersion</key>
	<string>2</string>
	<key>actions</key>
	<array>
		<dict>
			<key>action</key>
			<dict>
				<key>AMAccepts</key>
				<dict>
					<key>Container</key>
					<string>List</string>
					<key>Optional</key>
					<true/>
					<key>Types</key>
					<array>
						<string>com.apple.cocoa.path</string>
					</array>
				</dict>
				<key>AMActionVersion</key>
				<string>2.0.3</string>
				<key>AMApplication</key>
				<array>
					<string>Finder</string>
				</array>
				<key>AMParameterProperties</key>
				<dict>
					<key>COMMAND_STRING</key>
					<dict/>
					<key>shell</key>
					<dict/>
					<key>source</key>
					<dict/>
				</dict>
				<key>AMProvides</key>
				<dict>
					<key>Container</key>
					<string>List</string>
					<key>Types</key>
					<array>
						<string>com.apple.cocoa.path</string>
					</array>
				</dict>
				<key>ActionBundlePath</key>
				<string>/System/Library/Automator/Run Shell Script.action</string>
				<key>ActionName</key>
				<string>Run Shell Script</string>
				<key>ActionParameters</key>
				<dict>
					<key>COMMAND_STRING</key>
					<string>%s</string>
					<key>CheckedForDefaults</key>
					<true/>
					<key>shell</key>
					<string>/bin/bash</string>
					<key>source</key>
					<string>pass_input</string>
				</dict>
				<key>BundleIdentifier</key>
				<string>com.apple.RunShellScript</string>
				<key>CFBundleVersion</key>
				<string>2.0.3</string>
				<key>CanShowSelectedItemsWhenRun</key>
				<false/>
				<key>CanShowWhenRun</key>
				<true/>
				<key>Category</key>
				<array>
					<string>AMCategoryUtilities</string>
				</array>
				<key>Class Name</key>
				<string>RunShellScriptAction</string>
				<key>InputUUID</key>
				<string>77D5CEF2-9A51-4B27-A0C0-000000000001</string>
				<key>Keywords</key>
				<array>
					<string>Shell</string>
					<string>Script</string>
					<string>Command</string>
					<string>Run</string>
					<string>Unix</string>
				</array>
				<key>OutputUUID</key>
				<string>77D5CEF2-9A51-4B27-A0C0-000000000002</string>
				<key>UUID</key>
				<string>%s</string>
				<key>UnlocalizedApplications</key>
				<array>
					<string>Automator</string>
				</array>
				<key>arguments</key>
				<dict>
					<key>0</key>
					<dict>
						<key>default value</key>
						<string>do shell script ""</string>
						<key>name</key>
						<string>COMMAND_STRING</string>
						<key>required</key>
						<string>0</string>
						<key>type</key>
						<string>0</string>
						<key>uuid</key>
						<string>77D5CEF2-9A51-4B27-A0C0-000000000003</string>
					</dict>
				</dict>
				<key>isViewVisible</key>
				<integer>1</integer>
				<key>location</key>
				<string>309.000000:253.000000</string>
			</dict>
			<key>isViewVisible</key>
			<integer>1</integer>
		</dict>
	</array>
	<key>connectors</key>
	<dict/>
	<key>workflowMetaData</key>
	<dict>
		<key>workflowTypeIdentifier</key>
		<string>com.apple.Automator.servicesMenu</string>
	</dict>
</dict>
</plist>`, xmlScript, uuid)

	return os.WriteFile(filepath.Join(contentsDir, "document.wflow"), []byte(content), 0644)
}

// writeWorkflowInfo writes the Info.plist that names the item in the Services menu.
func writeWorkflowInfo(contentsDir, menuTitle string) error {
	content := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>NSServices</key>
	<array>
		<dict>
			<key>NSMenuItem</key>
			<dict>
				<key>default</key>
				<string>%s</string>
			</dict>
			<key>NSMessage</key>
			<string>runWorkflowAsService</string>
			<key>NSSendFileTypes</key>
			<array>
				<string>public.item</string>
			</array>
		</dict>
	</array>
</dict>
</plist>`, menuTitle)

	return os.WriteFile(filepath.Join(contentsDir, "Info.plist"), []byte(content), 0644)
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
