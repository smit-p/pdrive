package rclonerc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// ensureColon normalizes a remote name to always end with a colon.
func ensureColon(remote string) string {
	return strings.TrimSuffix(remote, ":") + ":"
}

// PutFile uploads data to remote:remotePath using rclone RC operations/uploadfile.
// remotePath should be the full path (e.g., "pdrive-chunks/abc123").
func (c *Client) PutFile(remote, remotePath string, data io.Reader) error {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	// operations/uploadfile expects:
	//   remote = parent directory to upload into
	//   file0 = file content with the filename as the base name
	dir := filepath.Dir(remotePath)
	base := filepath.Base(remotePath)

	part, err := writer.CreateFormFile("file0", base)
	if err != nil {
		return fmt.Errorf("creating form file: %w", err)
	}
	if _, err := io.Copy(part, data); err != nil {
		return fmt.Errorf("copying file data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("closing multipart writer: %w", err)
	}

	params := url.Values{}
	params.Set("fs", ensureColon(remote))
	params.Set("remote", dir)
	req, err := http.NewRequest("POST", c.baseURL+"/operations/uploadfile?"+params.Encode(), &body)
	if err != nil {
		return fmt.Errorf("creating upload request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("uploading file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// GetFile downloads a file from remote:remotePath using rclone RC operations/copyfile
// to a local temp directory, reads the content, and cleans up.
func (c *Client) GetFile(remote, remotePath string) ([]byte, error) {
	tmpDir, err := os.MkdirTemp("", "pdrive-dl-")
	if err != nil {
		return nil, fmt.Errorf("creating temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	dstRemote := filepath.Base(remotePath)

	_, err = c.call("operations/copyfile", map[string]interface{}{
		"srcFs":     ensureColon(remote),
		"srcRemote": remotePath,
		"dstFs":     tmpDir + "/",
		"dstRemote": dstRemote,
	})
	if err != nil {
		return nil, fmt.Errorf("downloading file: %w", err)
	}

	data, err := os.ReadFile(filepath.Join(tmpDir, dstRemote))
	if err != nil {
		return nil, fmt.Errorf("reading downloaded file: %w", err)
	}

	return data, nil
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
