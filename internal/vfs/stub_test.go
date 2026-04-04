package vfs

import (
	"bytes"
	"strconv"
	"testing"
)

func TestFormatInt(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{42, "42"},
		{-7, "-7"},
		{-1, "-1"},
		{1099511627776, "1099511627776"},
		{9223372036854775807, "9223372036854775807"},
	}
	for _, tc := range cases {
		got := formatInt(tc.in)
		if got != tc.want {
			t.Errorf("formatInt(%d) = %q, want %q", tc.in, got, tc.want)
		}
		if got != strconv.FormatInt(tc.in, 10) {
			t.Errorf("formatInt(%d) = %q, differs from strconv %q",
				tc.in, got, strconv.FormatInt(tc.in, 10))
		}
	}
}

func TestBuildTagPlist_ValidHeader(t *testing.T) {
	p := buildTagPlist("Green", finderColorGreen)
	if !bytes.HasPrefix(p, []byte("bplist00")) {
		t.Error("plist must start with bplist00 header")
	}
}

func TestBuildTagPlist_ContainsTag(t *testing.T) {
	p := buildTagPlist("Gray", finderColorGray)
	if !bytes.Contains(p, []byte("Gray")) {
		t.Error("plist must contain the tag name")
	}
}

func TestBuildTagPlist_DifferentColors(t *testing.T) {
	gray := buildTagPlist("Gray", finderColorGray)
	green := buildTagPlist("Green", finderColorGreen)
	if bytes.Equal(gray, green) {
		t.Error("different colors must produce different plists")
	}
}
