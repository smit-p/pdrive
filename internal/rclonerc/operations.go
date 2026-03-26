package rclonerc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
)

// PutFile uploads data to remote:remotePath using rclone RC operations/uploadfile.
// The remote should be the rclone remote name (e.g., "gdrive").
func (c *Client) PutFile(remote, remotePath string, data io.Reader) error {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	// Add the fs (remote name with colon) and remote (path) fields.
	if err := writer.WriteField("fs", remote+":"); err != nil {
		return fmt.Errorf("writing fs field: %w", err)
	}
	if err := writer.WriteField("remote", remotePath); err != nil {
		return fmt.Errorf("writing remote field: %w", err)
	}

	// Add the file content.
	part, err := writer.CreateFormFile("file0", remotePath)
	if err != nil {
		return fmt.Errorf("creating form file: %w", err)
	}
	if _, err := io.Copy(part, data); err != nil {
		return fmt.Errorf("copying file data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("closing multipart writer: %w", err)
	}

	req, err := http.NewRequest("POST", c.baseURL+"/operations/uploadfile", &body)
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

// GetFile downloads a file from remote:remotePath using rclone RC operations/cat.
func (c *Client) GetFile(remote, remotePath string) ([]byte, error) {
	result, err := c.call("operations/cat", map[string]interface{}{
		"fs":     remote + ":",
		"remote": remotePath,
	})
	if err != nil {
		return nil, fmt.Errorf("downloading file: %w", err)
	}

	// operations/cat returns the raw content as a JSON string
	var content string
	if err := json.Unmarshal(result, &content); err != nil {
		// If it's not a JSON string, the raw bytes are the content
		return []byte(result), nil
	}
	return []byte(content), nil
}

// DeleteFile deletes a file at remote:remotePath using rclone RC operations/deletefile.
func (c *Client) DeleteFile(remote, remotePath string) error {
	_, err := c.call("operations/deletefile", map[string]interface{}{
		"fs":     remote + ":",
		"remote": remotePath,
	})
	if err != nil {
		return fmt.Errorf("deleting file: %w", err)
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
		"fs":     remote + ":",
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
