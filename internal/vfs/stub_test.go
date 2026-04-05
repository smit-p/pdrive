package vfs

import (
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
