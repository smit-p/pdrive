package daemon

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/smit-p/pdrive/internal/metadata"
)

// syncProviders discovers rclone remotes and registers/updates them as providers
// in the metadata database. This is the bridge between "rclone config" and pdrive's
// provider-aware broker. Called on every startup to keep the DB in sync.
func (d *Daemon) syncProviders() {
	remotes, err := d.rclone.Client().ListRemotes()
	if err != nil {
		slog.Warn("could not list rclone remotes for provider sync", "error", err)
		return
	}
	if len(remotes) == 0 {
		slog.Warn("no rclone remotes configured — uploads will fail until a remote is added")
		return
	}

	// If the user specified --remotes, use that. Otherwise load remotes.json.
	allowedRemotes := d.config.Remotes
	if len(allowedRemotes) == 0 {
		remotesFile := filepath.Join(d.config.ConfigDir, "remotes.json")
		if data, err := os.ReadFile(remotesFile); err == nil {
			var saved []string
			if err := json.Unmarshal(data, &saved); err == nil && len(saved) > 0 {
				allowedRemotes = saved
				slog.Info("loaded remote selection from remotes.json", "remotes", saved)
			}
		}
	}

	if len(allowedRemotes) > 0 {
		allowed := make(map[string]bool, len(allowedRemotes))
		for _, r := range allowedRemotes {
			allowed[r] = true
		}
		filtered := make([]string, 0, len(remotes))
		for _, r := range remotes {
			if allowed[r] {
				filtered = append(filtered, r)
			} else {
				slog.Debug("skipping remote (not in selection)", "remote", r)
			}
		}
		remotes = filtered
		if len(remotes) == 0 {
			slog.Warn("none of the selected remotes match configured rclone remotes")
			return
		}
	}

	now := time.Now().Unix()
	for _, remote := range remotes {
		remoteType, err := d.rclone.Client().GetRemoteType(remote)
		if err != nil {
			slog.Debug("could not get remote type", "remote", remote, "error", err)
			remoteType = "unknown"
		}

		// Check if this remote is already registered (possibly from a restored DB).
		existing, _ := d.db.GetProviderByRemote(remote)

		providerID := remote // use remote name as stable ID
		if existing != nil {
			providerID = existing.ID
		}

		// Fetch quota from cloud.
		var quotaTotal, quotaFree *int64
		var polledAt *int64
		aboutResult, err := d.rclone.Client().About(remote)
		if err != nil {
			slog.Debug("could not fetch quota", "remote", remote, "error", err)
			// Keep existing quota values if available.
			if existing != nil {
				quotaTotal = existing.QuotaTotalBytes
				quotaFree = existing.QuotaFreeBytes
				polledAt = existing.QuotaPolledAt
			}
		} else {
			quotaTotal = &aboutResult.Total
			quotaFree = &aboutResult.Free
			polledAt = &now
		}

		p := &metadata.Provider{
			ID:              providerID,
			Type:            remoteType,
			DisplayName:     remote,
			RcloneRemote:    remote,
			QuotaTotalBytes: quotaTotal,
			QuotaFreeBytes:  quotaFree,
			QuotaPolledAt:   polledAt,
		}
		// Preserve rate-limit state from existing record.
		if existing != nil {
			p.RateLimitedUntil = existing.RateLimitedUntil
		}

		// Fetch account identity (email/username). The About() call above
		// forces rclone to refresh any expired OAuth tokens, so the
		// config/get call inside FetchAccountIdentity gets a fresh token.
		if existing != nil && existing.AccountIdentity != "" {
			p.AccountIdentity = existing.AccountIdentity
		} else {
			identity, err := d.rclone.Client().FetchAccountIdentity(remote)
			if err != nil {
				slog.Debug("could not fetch account identity", "remote", remote, "error", err)
			}
			p.AccountIdentity = identity
		}

		if err := d.db.UpsertProvider(p); err != nil {
			slog.Warn("failed to register provider", "remote", remote, "error", err)
			continue
		}
		slog.Info("provider synced", "remote", remote, "type", remoteType)
	}
}

// checkMissingProviders compares the providers in the restored DB with the
// currently available rclone remotes. If any DB providers are missing (the user
// has not yet configured those remotes on this machine), it logs warnings so the
// user knows which accounts to add.
// Returns the list of missing provider remote names.
func (d *Daemon) checkMissingProviders() []string {
	dbProviders, err := d.db.GetAllProviders()
	if err != nil || len(dbProviders) == 0 {
		return nil
	}

	remotes, err := d.rclone.Client().ListRemotes()
	if err != nil {
		slog.Warn("could not list remotes for missing-provider check", "error", err)
		return nil
	}

	available := make(map[string]bool, len(remotes))
	for _, r := range remotes {
		available[r] = true
	}

	var missing []string
	for _, p := range dbProviders {
		if !available[p.RcloneRemote] {
			label := p.RcloneRemote
			if p.AccountIdentity != "" {
				label += " (" + p.AccountIdentity + ", " + p.Type + ")"
			} else if p.Type != "" {
				label += " (" + p.Type + ")"
			}
			missing = append(missing, label)
		}
	}

	if len(missing) > 0 {
		slog.Warn("restored metadata references cloud providers not configured on this machine",
			"missing", missing,
			"total_db_providers", len(dbProviders),
			"available_remotes", len(remotes),
		)
		slog.Warn(fmt.Sprintf("pdrive needs %d additional cloud account(s) to access all your files: %s",
			len(missing), strings.Join(missing, ", ")))
		slog.Warn("add the missing remotes with: rclone config create <name> <type>")
	}

	return missing
}

// purgeJunkFiles removes OS-generated junk files (.DS_Store, ._* resource
// forks, Thumbs.db, etc.) from the metadata DB. These files are harmless but
// waste storage quota and clutter the UI.
func (d *Daemon) purgeJunkFiles() {
	all, err := d.engine.DB().ListAllFiles("/")
	if err != nil {
		slog.Warn("junk purge: could not list files", "error", err)
		return
	}
	var purged int
	for _, f := range all {
		if isJunkFile(f.VirtualPath) {
			if err := d.engine.DeleteFile(f.VirtualPath); err != nil {
				slog.Warn("junk purge: delete failed", "path", f.VirtualPath, "error", err)
			} else {
				purged++
			}
		}
	}
	if purged > 0 {
		slog.Info("junk purge: removed OS-generated files", "count", purged)
	}
}
