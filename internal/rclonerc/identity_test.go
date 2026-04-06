package rclonerc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --- extractAccessToken ---

func TestExtractAccessToken_Valid(t *testing.T) {
	cfg := map[string]interface{}{
		"type":  "drive",
		"token": `{"access_token":"ya29.test-token","token_type":"Bearer","refresh_token":"1//ref"}`,
	}
	got := extractAccessToken(cfg)
	if got != "ya29.test-token" {
		t.Errorf("extractAccessToken = %q, want %q", got, "ya29.test-token")
	}
}

func TestExtractAccessToken_NoToken(t *testing.T) {
	cfg := map[string]interface{}{"type": "drive"}
	if got := extractAccessToken(cfg); got != "" {
		t.Errorf("extractAccessToken = %q, want empty", got)
	}
}

func TestExtractAccessToken_InvalidJSON(t *testing.T) {
	cfg := map[string]interface{}{"token": "not-json"}
	if got := extractAccessToken(cfg); got != "" {
		t.Errorf("extractAccessToken = %q, want empty", got)
	}
}

func TestExtractAccessToken_NotString(t *testing.T) {
	cfg := map[string]interface{}{"token": 12345}
	if got := extractAccessToken(cfg); got != "" {
		t.Errorf("extractAccessToken = %q, want empty", got)
	}
}

// --- maskKey ---

func TestMaskKey(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"AKIAIOSFODNN7EXAMPLE", "AKIA...MPLE"},
		{"short", "short"},
		{"12345678", "12345678"},
		{"123456789", "1234...6789"},
	}
	for _, tt := range tests {
		if got := maskKey(tt.in); got != tt.want {
			t.Errorf("maskKey(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// --- parsers ---

func TestParseGoogleDriveAbout(t *testing.T) {
	body := `{"user":{"emailAddress":"alice@gmail.com","displayName":"Alice"}}`
	if got := parseGoogleDriveAbout([]byte(body)); got != "alice@gmail.com" {
		t.Errorf("got %q, want alice@gmail.com", got)
	}
}

func TestParseDropboxUserinfo(t *testing.T) {
	body := `{"email":"bob@dropbox.com","name":{"display_name":"Bob"}}`
	if got := parseDropboxUserinfo([]byte(body)); got != "bob@dropbox.com" {
		t.Errorf("got %q, want bob@dropbox.com", got)
	}
}

func TestParseMicrosoftUserinfo_Mail(t *testing.T) {
	body := `{"mail":"carol@outlook.com","displayName":"Carol","userPrincipalName":"carol@live.com"}`
	if got := parseMicrosoftUserinfo([]byte(body)); got != "carol@outlook.com" {
		t.Errorf("got %q, want carol@outlook.com", got)
	}
}

func TestParseMicrosoftUserinfo_UPN(t *testing.T) {
	body := `{"mail":"","userPrincipalName":"carol@live.com"}`
	if got := parseMicrosoftUserinfo([]byte(body)); got != "carol@live.com" {
		t.Errorf("got %q, want carol@live.com", got)
	}
}

func TestParseBoxUserinfo(t *testing.T) {
	body := `{"login":"dave@box.com","name":"Dave"}`
	if got := parseBoxUserinfo([]byte(body)); got != "dave@box.com" {
		t.Errorf("got %q, want dave@box.com", got)
	}
}

// --- fetchOAuthIdentity ---

func TestFetchOAuthIdentity_Google(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("unexpected auth header: %s", r.Header.Get("Authorization"))
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"user": map[string]string{"emailAddress": "alice@gmail.com"},
		})
	}))
	defer srv.Close()

	// Temporarily override the google provider URL for testing.
	orig := oauthProviders["drive"]
	oauthProviders["drive"] = oauthProvider{url: srv.URL, method: "GET", parser: parseGoogleDriveAbout}
	defer func() { oauthProviders["drive"] = orig }()

	identity, err := fetchOAuthIdentity("drive", "test-token")
	if err != nil {
		t.Fatal(err)
	}
	if identity != "alice@gmail.com" {
		t.Errorf("got %q, want alice@gmail.com", identity)
	}
}

func TestFetchOAuthIdentity_Dropbox(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		json.NewEncoder(w).Encode(map[string]string{"email": "bob@dropbox.com"})
	}))
	defer srv.Close()

	orig := oauthProviders["dropbox"]
	oauthProviders["dropbox"] = oauthProvider{url: srv.URL, method: "POST", parser: parseDropboxUserinfo}
	defer func() { oauthProviders["dropbox"] = orig }()

	identity, err := fetchOAuthIdentity("dropbox", "test-token")
	if err != nil {
		t.Fatal(err)
	}
	if identity != "bob@dropbox.com" {
		t.Errorf("got %q, want bob@dropbox.com", identity)
	}
}

func TestFetchOAuthIdentity_UnsupportedBackend(t *testing.T) {
	identity, err := fetchOAuthIdentity("mega", "token")
	if err != nil {
		t.Fatal(err)
	}
	if identity != "" {
		t.Errorf("expected empty identity for unsupported backend, got %q", identity)
	}
}

func TestFetchOAuthIdentity_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	orig := oauthProviders["drive"]
	oauthProviders["drive"] = oauthProvider{url: srv.URL, method: "GET", parser: parseGoogleDriveAbout}
	defer func() { oauthProviders["drive"] = orig }()

	_, err := fetchOAuthIdentity("drive", "expired-token")
	if err == nil {
		t.Fatal("expected error for 401 response")
	}
}

// --- FetchAccountIdentity (integration with rclone RC mock) ---

func TestFetchAccountIdentity_ConfigUser(t *testing.T) {
	// Non-OAuth backend with a "user" field in config.
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"type": "sftp",
			"user": "deploy@server.com",
			"host": "server.com",
		})
	})
	identity, err := c.FetchAccountIdentity("mysftp")
	if err != nil {
		t.Fatal(err)
	}
	if identity != "deploy@server.com" {
		t.Errorf("got %q, want deploy@server.com", identity)
	}
}

func TestFetchAccountIdentity_S3AccessKey(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"type":              "s3",
			"access_key_id":     "AKIAIOSFODNN7EXAMPLE",
			"secret_access_key": "xxx",
		})
	})
	identity, err := c.FetchAccountIdentity("aws")
	if err != nil {
		t.Fatal(err)
	}
	if identity != "AKIA...MPLE" {
		t.Errorf("got %q, want AKIA...MPLE", identity)
	}
}

func TestFetchAccountIdentity_OAuthFallback(t *testing.T) {
	// OAuth backend with a token. We override the provider URL to a local mock.
	userInfoSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"user": map[string]string{"emailAddress": "alice@gmail.com"},
		})
	}))
	defer userInfoSrv.Close()

	orig := oauthProviders["drive"]
	oauthProviders["drive"] = oauthProvider{url: userInfoSrv.URL, method: "GET", parser: parseGoogleDriveAbout}
	defer func() { oauthProviders["drive"] = orig }()

	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"type":  "drive",
			"token": `{"access_token":"ya29.test","token_type":"Bearer"}`,
		})
	})
	identity, err := c.FetchAccountIdentity("gdrive")
	if err != nil {
		t.Fatal(err)
	}
	if identity != "alice@gmail.com" {
		t.Errorf("got %q, want alice@gmail.com", identity)
	}
}

func TestFetchAccountIdentity_NoIdentity(t *testing.T) {
	// Backend with no identifying info at all.
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"type": "local",
		})
	})
	identity, err := c.FetchAccountIdentity("localfs")
	if err != nil {
		t.Fatal(err)
	}
	if identity != "" {
		t.Errorf("expected empty identity, got %q", identity)
	}
}

func TestFetchAccountIdentity_ConfigError(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`error`))
	})
	_, err := c.FetchAccountIdentity("bad")
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- GetRemoteConfig ---

func TestGetRemoteConfig_Success(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"type":  "drive",
			"scope": "drive",
		})
	})
	cfg, err := c.GetRemoteConfig("gdrive")
	if err != nil {
		t.Fatal(err)
	}
	if cfg["type"] != "drive" {
		t.Errorf("type = %v, want drive", cfg["type"])
	}
}

func TestGetRemoteConfig_Error(t *testing.T) {
	c := fakeRclone(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`not found`))
	})
	_, err := c.GetRemoteConfig("bad")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "getting remote config") {
		t.Errorf("unexpected error: %v", err)
	}
}
