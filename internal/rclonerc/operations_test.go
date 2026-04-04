package rclonerc

import (
	"fmt"
	"testing"
)

func TestIsRateLimited(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("connection refused"), false},
		{fmt.Errorf("rclone RC operations/uploadfile: HTTP 500: 429 Too Many Requests"), true},
		{fmt.Errorf("rateLimitExceeded"), true},
		{fmt.Errorf("upload failed: userRateLimitExceeded"), true},
		{fmt.Errorf("too many requests"), true},
		{fmt.Errorf("rate limit exceeded"), true},
		{fmt.Errorf("upload failed: HTTP 500"), false},
	}
	for _, tt := range tests {
		got := IsRateLimited(tt.err)
		if got != tt.want {
			t.Errorf("IsRateLimited(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}
