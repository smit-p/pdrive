// Package rclonerc provides a Go client for the rclone RC (remote control)
// HTTP API.  It is used by the pdrive engine to upload, download, and
// delete encrypted file chunks on any rclone-supported cloud backend
// (Google Drive, Dropbox, OneDrive, S3, etc.).
//
// Key capabilities:
//   - Async file upload via operations/copyfile + job polling
//   - Streaming file download to temp files (no full-file memory allocation)
//   - Quota fetching with a TTL cache ([QuotaCache])
//   - Account identity detection for display labels ([FetchAccountIdentity])
//   - Rate-limit detection via error message heuristics ([IsRateLimited])
package rclonerc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client communicates with rclone's RC (remote control) HTTP API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new rclone RC client.
func NewClient(addr string) *Client {
	return &Client{
		baseURL: "http://" + addr,
		httpClient: &http.Client{
			Timeout: 30 * time.Minute, // downloads/sync calls can be slow
		},
	}
}

// call makes a POST request to the rclone RC API with a JSON body.
func (c *Client) call(endpoint string, params map[string]interface{}) (json.RawMessage, error) {
	body, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshaling params: %w", err)
	}

	resp, err := c.httpClient.Post(c.baseURL+"/"+endpoint, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("calling %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response from %s: %w", endpoint, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rclone RC %s returned %d: %s", endpoint, resp.StatusCode, string(respBody))
	}

	return json.RawMessage(respBody), nil
}

// StreamGetFile issues an HTTP GET to rclone's --rc-serve endpoint,
// returning a streaming reader that delivers data as rclone downloads
// from the cloud provider. Unlike GetFile (which copies the entire file
// to a temp dir before returning), the reader yields bytes incrementally.
// The caller must Close the returned ReadCloser.
func (c *Client) StreamGetFile(remote, remotePath string) (io.ReadCloser, error) {
	// rclone --rc-serve uses the path format /[remote:]path
	url := c.baseURL + "/[" + ensureColon(remote) + "]" + remotePath
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("streaming GET %s: %w", remotePath, err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("streaming GET %s: HTTP %d: %s", remotePath, resp.StatusCode, string(body))
	}
	return resp.Body, nil
}

// Ping checks if the rclone RC server is reachable.
func (c *Client) Ping() error {
	_, err := c.call("core/version", nil)
	return err
}
