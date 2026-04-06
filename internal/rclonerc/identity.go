package rclonerc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// identityTimeout limits how long we wait for a provider's userinfo API.
const identityTimeout = 10 * time.Second

// FetchAccountIdentity tries to determine the user identity (email/username)
// for the given rclone remote. It checks config fields first, then for OAuth
// backends queries the provider's userinfo API using the stored access token.
//
// This is best-effort: a failure returns ("", nil) so callers can proceed
// without identity information.
func (c *Client) FetchAccountIdentity(remote string) (string, error) {
	cfg, err := c.GetRemoteConfig(remote)
	if err != nil {
		return "", err
	}

	// Non-OAuth backends often store a user/account field directly.
	for _, key := range []string{"user", "account", "login"} {
		if v, ok := cfg[key].(string); ok && v != "" {
			return v, nil
		}
	}

	// S3-like backends: use masked access key as differentiator.
	if v, ok := cfg["access_key_id"].(string); ok && v != "" {
		return maskKey(v), nil
	}

	// OAuth backends: extract access token and query provider's userinfo endpoint.
	remoteType, _ := cfg["type"].(string)
	token := extractAccessToken(cfg)
	if token == "" {
		return "", nil
	}
	return fetchOAuthIdentity(remoteType, token)
}

// extractAccessToken parses the OAuth access_token from the rclone config's
// "token" field, which is stored as a JSON string.
func extractAccessToken(cfg map[string]interface{}) string {
	tokenRaw, ok := cfg["token"]
	if !ok {
		return ""
	}

	// config/get returns the token as a JSON-encoded string.
	tokenStr, ok := tokenRaw.(string)
	if !ok {
		return ""
	}

	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if json.Unmarshal([]byte(tokenStr), &tok) != nil {
		return ""
	}
	return tok.AccessToken
}

type oauthProvider struct {
	url    string
	method string
	parser func([]byte) string
}

var oauthProviders = map[string]oauthProvider{
	"drive": {
		url:    "https://www.googleapis.com/drive/v3/about?fields=user(emailAddress)",
		method: "GET",
		parser: parseGoogleDriveAbout,
	},
	"dropbox": {
		url:    "https://api.dropboxapi.com/2/users/get_current_account",
		method: "POST",
		parser: parseDropboxUserinfo,
	},
	"onedrive": {
		url:    "https://graph.microsoft.com/v1.0/me",
		method: "GET",
		parser: parseMicrosoftUserinfo,
	},
	"box": {
		url:    "https://api.box.com/2.0/users/me",
		method: "GET",
		parser: parseBoxUserinfo,
	},
}

// fetchOAuthIdentity makes a single HTTP call to the provider's userinfo API
// using the given access token.
func fetchOAuthIdentity(remoteType, accessToken string) (string, error) {
	pc, ok := oauthProviders[remoteType]
	if !ok {
		return "", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), identityTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, pc.method, pc.url, nil)
	if err != nil {
		return "", fmt.Errorf("creating identity request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetching identity: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("identity API returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return "", fmt.Errorf("reading identity response: %w", err)
	}

	return pc.parser(body), nil
}

func parseGoogleDriveAbout(body []byte) string {
	var resp struct {
		User struct {
			EmailAddress string `json:"emailAddress"`
		} `json:"user"`
	}
	json.Unmarshal(body, &resp) //nolint:errcheck
	return resp.User.EmailAddress
}

func parseDropboxUserinfo(body []byte) string {
	var resp struct {
		Email string `json:"email"`
	}
	json.Unmarshal(body, &resp) //nolint:errcheck
	return resp.Email
}

func parseMicrosoftUserinfo(body []byte) string {
	var resp struct {
		Mail              string `json:"mail"`
		UserPrincipalName string `json:"userPrincipalName"`
	}
	json.Unmarshal(body, &resp) //nolint:errcheck
	if resp.Mail != "" {
		return resp.Mail
	}
	return resp.UserPrincipalName
}

func parseBoxUserinfo(body []byte) string {
	var resp struct {
		Login string `json:"login"`
	}
	json.Unmarshal(body, &resp) //nolint:errcheck
	return resp.Login
}

// maskKey returns a masked version of a secret key for display purposes.
func maskKey(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:4] + "..." + s[len(s)-4:]
}
