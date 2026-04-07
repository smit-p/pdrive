package rclonerc

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// IsRateLimited checks if an error from the rclone RC API indicates that the
// cloud provider returned a rate-limit response (HTTP 429 or similar).
// Since rclone wraps cloud errors opaquely, we check the error message text.
func IsRateLimited(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "429") ||
		strings.Contains(msg, "ratelimit") ||
		strings.Contains(msg, "rate limit") ||
		strings.Contains(msg, "too many requests") ||
		strings.Contains(msg, "userratelimitexceeded") ||
		strings.Contains(msg, "ratelimitexceeded")
}

// IsQuotaExceeded checks if an error from rclone indicates the cloud provider
// rejected the operation because of insufficient storage quota.
func IsQuotaExceeded(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "quota") ||
		strings.Contains(msg, "storagelimitexceeded") ||
		strings.Contains(msg, "storage limit") ||
		strings.Contains(msg, "insufficient storage") ||
		strings.Contains(msg, "not enough space") ||
		strings.Contains(msg, "disk full") ||
		strings.Contains(msg, "no space left") ||
		strings.Contains(msg, "enospc") ||
		strings.Contains(msg, "413") ||
		strings.Contains(msg, "over quota") ||
		strings.Contains(msg, "account full")
}

// ensureColon normalizes a remote name to always end with a colon.
func ensureColon(remote string) string {
	return strings.TrimSuffix(remote, ":") + ":"
}

// PutFile uploads data to remote:remotePath using rclone RC operations/copyfile.
// The data is written to a temporary file first, then rclone copies it from disk
// to the cloud provider using its native backend (which supports resumable uploads
// for Google Drive, chunked uploads for Dropbox, etc.). The copy runs as an async
// rclone job to avoid HTTP timeout issues with large chunks.
func (c *Client) PutFile(remote, remotePath string, data io.Reader) error {
	tmpDir, err := os.MkdirTemp("", "pdrive-ul-")
	if err != nil {
		return fmt.Errorf("creating temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	base := filepath.Base(remotePath)
	tmpFile := filepath.Join(tmpDir, base)
	f, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	if _, err := io.Copy(f, data); err != nil {
		_ = f.Close()
		return fmt.Errorf("writing temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	// Launch as an async rclone job so the HTTP call returns immediately
	// and we are not subject to HTTP client timeouts for slow uploads.
	// Use _group so transfer stats are queryable in a single stats group
	// rather than being isolated per-job.
	result, err := c.call("operations/copyfile", map[string]interface{}{
		"srcFs":     tmpDir + "/",
		"srcRemote": base,
		"dstFs":     ensureColon(remote),
		"dstRemote": remotePath,
		"_async":    true,
		"_group":    "pdrive",
	})
	if err != nil {
		return fmt.Errorf("starting async upload: %w", err)
	}

	var job struct {
		JobID int `json:"jobid"`
	}
	if err := json.Unmarshal(result, &job); err != nil {
		return fmt.Errorf("parsing async job response: %w", err)
	}

	// Poll job/status until the copy finishes.
	return c.waitForJob(job.JobID)
}

// waitForJob polls rclone job/status until the async job completes or fails.
// Uses exponential backoff starting at 100ms, capped at 5s between polls.
// Gives up after maxJobPollIterations to prevent infinite loops on stuck jobs.
func (c *Client) waitForJob(jobID int) error {
	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second
	const maxIterations = 720 // ~1 hour at 5 s ceiling

	for i := 0; i < maxIterations; i++ {
		time.Sleep(backoff)

		result, err := c.call("job/status", map[string]interface{}{
			"jobid": jobID,
		})
		if err != nil {
			return fmt.Errorf("polling job %d: %w", jobID, err)
		}

		var status struct {
			Finished bool   `json:"finished"`
			Success  bool   `json:"success"`
			Error    string `json:"error"`
		}
		if err := json.Unmarshal(result, &status); err != nil {
			return fmt.Errorf("parsing job status: %w", err)
		}

		if status.Finished {
			if !status.Success {
				return fmt.Errorf("rclone job %d failed: %s", jobID, status.Error)
			}
			return nil
		}

		backoff = min(backoff*2, maxBackoff)
	}

	return fmt.Errorf("rclone job %d: timed out after %d poll iterations", jobID, maxIterations)
}

// tempFileReadCloser wraps an os.File and removes its parent temp directory
// when Close is called. This avoids reading the entire downloaded file into
// memory — the caller streams from the file handle and cleanup happens on Close.
type tempFileReadCloser struct {
	*os.File
	tmpDir string
}

func (t *tempFileReadCloser) Close() error {
	err := t.File.Close()
	_ = os.RemoveAll(t.tmpDir)
	return err
}

// GetFile downloads a file from remote:remotePath using rclone RC operations/copyfile
// to a local temp directory and returns a streaming reader. The caller must Close
// the returned ReadCloser to release the temp file.
func (c *Client) GetFile(remote, remotePath string) (io.ReadCloser, error) {
	tmpDir, err := os.MkdirTemp("", "pdrive-dl-")
	if err != nil {
		return nil, fmt.Errorf("creating temp dir: %w", err)
	}

	dstRemote := filepath.Base(remotePath)

	_, err = c.call("operations/copyfile", map[string]interface{}{
		"srcFs":     ensureColon(remote),
		"srcRemote": remotePath,
		"dstFs":     tmpDir + "/",
		"dstRemote": dstRemote,
	})
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("downloading file: %w", err)
	}

	f, err := os.Open(filepath.Join(tmpDir, dstRemote))
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("opening downloaded file: %w", err)
	}

	return &tempFileReadCloser{File: f, tmpDir: tmpDir}, nil
}

// DeleteFile deletes a file at remote:remotePath using rclone RC operations/deletefile.
func (c *Client) DeleteFile(remote, remotePath string) error {
	_, err := c.call("operations/deletefile", map[string]interface{}{
		"fs":     ensureColon(remote),
		"remote": remotePath,
	})
	if err != nil {
		return fmt.Errorf("deleting file: %w", err)
	}
	return nil
}

// Mkdir creates a directory on the remote via rclone RC operations/mkdir.
// It is a no-op if the directory already exists.
func (c *Client) Mkdir(remote, remotePath string) error {
	_, err := c.call("operations/mkdir", map[string]interface{}{
		"fs":     ensureColon(remote),
		"remote": remotePath,
	})
	if err != nil {
		return fmt.Errorf("mkdir %s: %w", remotePath, err)
	}
	return nil
}

// Cleanup empties the trash on the given remote (e.g. Google Drive).
func (c *Client) Cleanup(remote string) error {
	_, err := c.call("operations/cleanup", map[string]interface{}{
		"fs": ensureColon(remote),
	})
	if err != nil {
		return fmt.Errorf("cleanup: %w", err)
	}
	return nil
}

// ListItem represents a file/directory entry from rclone RC.
type ListItem struct {
	Path    string `json:"Path"`
	Name    string `json:"Name"`
	Size    int64  `json:"Size"`
	IsDir   bool   `json:"IsDir"`
	ModTime string `json:"ModTime"`
}

// ListDir lists the contents of remote:remotePath using rclone RC operations/list.
func (c *Client) ListDir(remote, remotePath string) ([]ListItem, error) {
	result, err := c.call("operations/list", map[string]interface{}{
		"fs":     ensureColon(remote),
		"remote": remotePath,
	})
	if err != nil {
		return nil, fmt.Errorf("listing directory: %w", err)
	}

	var resp struct {
		List []ListItem `json:"list"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("parsing list response: %w", err)
	}
	return resp.List, nil
}

// About returns raw quota information for a remote via operations/about.
func (c *Client) About(remote string) (QuotaInfo, error) {
	result, err := c.call("operations/about", map[string]interface{}{
		"fs": ensureColon(remote),
	})
	if err != nil {
		return QuotaInfo{}, fmt.Errorf("fetching quota for %s: %w", remote, err)
	}

	var resp struct {
		Total *int64 `json:"total"`
		Used  *int64 `json:"used"`
		Free  *int64 `json:"free"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return QuotaInfo{}, fmt.Errorf("parsing about response: %w", err)
	}

	info := QuotaInfo{}
	if resp.Total != nil {
		info.Total = *resp.Total
	}
	if resp.Used != nil {
		info.Used = *resp.Used
	}
	if resp.Free != nil {
		info.Free = *resp.Free
	}
	return info, nil
}

// GetRemoteConfig returns the full configuration map for the named remote
// via the config/get RC endpoint.
func (c *Client) GetRemoteConfig(remote string) (map[string]interface{}, error) {
	result, err := c.call("config/get", map[string]interface{}{"name": remote})
	if err != nil {
		return nil, fmt.Errorf("getting remote config for %s: %w", remote, err)
	}
	var cfg map[string]interface{}
	if err := json.Unmarshal(result, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config response: %w", err)
	}
	return cfg, nil
}

// GetRemoteType returns the backend type (e.g., "drive", "s3", "dropbox")
// for the named remote via the config/get RC endpoint.
func (c *Client) GetRemoteType(remote string) (string, error) {
	cfg, err := c.GetRemoteConfig(remote)
	if err != nil {
		return "", err
	}
	if t, ok := cfg["type"].(string); ok {
		return t, nil
	}
	return "unknown", nil
}

// TransferProgress holds the current aggregate transfer stats from rclone.
type TransferProgress struct {
	SpeedBytes   float64          // aggregate upload speed in bytes/sec
	Transferring map[string]int64 // name → bytes transferred so far
}

// TransferStats queries rclone core/stats and returns the current transfer
// speed and a map of in-flight transfer names to bytes transferred.
// Queries both the default and "pdrive" stat groups so that async job
// transfers (which use _group:"pdrive") are included.
// Non-fatal: returns a zero value on error so callers can degrade gracefully.
func (c *Client) TransferStats() TransferProgress {
	type statsResponse struct {
		Speed        float64 `json:"speed"`
		Transferring []struct {
			Name  string `json:"name"`
			Bytes int64  `json:"bytes"`
		} `json:"transferring"`
	}

	var speed float64
	m := make(map[string]int64)

	// Query default global group for aggregate speed.
	if result, err := c.call("core/stats", nil); err == nil {
		var stats statsResponse
		if json.Unmarshal(result, &stats) == nil {
			speed = stats.Speed
			for _, t := range stats.Transferring {
				m[t.Name] = t.Bytes
			}
		}
	}

	// Query the "pdrive" group where async upload jobs report their transfers.
	if result, err := c.call("core/stats", map[string]interface{}{"group": "pdrive"}); err == nil {
		var stats statsResponse
		if json.Unmarshal(result, &stats) == nil {
			if stats.Speed > speed {
				speed = stats.Speed
			}
			for _, t := range stats.Transferring {
				if existing, ok := m[t.Name]; !ok || t.Bytes > existing {
					m[t.Name] = t.Bytes
				}
			}
		}
	}

	return TransferProgress{
		SpeedBytes:   speed,
		Transferring: m,
	}
}

// ListRemotes returns all configured rclone remote names.
func (c *Client) ListRemotes() ([]string, error) {
	result, err := c.call("config/listremotes", map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("listing remotes: %w", err)
	}
	var resp struct {
		Remotes []string `json:"remotes"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("parsing listremotes response: %w", err)
	}
	return resp.Remotes, nil
}
