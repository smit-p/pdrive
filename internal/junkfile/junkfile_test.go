package junkfile

import "testing"

func TestIsOSJunk(t *testing.T) {
	tests := []struct {
		base string
		want bool
	}{
		{".DS_Store", true},
		{"._something", true},
		{"Thumbs.db", true},
		{"desktop.ini", true},
		{"readme.txt", false},
		{"photo.jpg", false},
		{".gitignore", false},
		{"._", true},
	}
	for _, tt := range tests {
		if got := IsOSJunk(tt.base); got != tt.want {
			t.Errorf("IsOSJunk(%q) = %v, want %v", tt.base, got, tt.want)
		}
	}
}
