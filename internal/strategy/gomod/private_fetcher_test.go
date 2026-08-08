package gomod_test

import (
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/strategy/gomod"
)

func TestVersionToGitRef(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{
			name:    "TaggedVersion",
			version: "v1.2.3",
			want:    "v1.2.3",
		},
		{
			name:    "PseudoVersion",
			version: "v0.0.0-20160603174536-ad42235f7e24",
			want:    "ad42235f7e24",
		},
		{
			name:    "PseudoVersionWithBase",
			version: "v1.2.4-0.20230101120000-abcdef123456",
			want:    "abcdef123456",
		},
		{
			name:    "PreReleaseVersion",
			version: "v1.0.0-rc1",
			want:    "v1.0.0-rc1",
		},
		{
			name:    "IncompatibleVersion",
			version: "v2.0.0+incompatible",
			want:    "v2.0.0+incompatible",
		},
		{
			name:    "BareRef",
			version: "HEAD",
			want:    "HEAD",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref := gomod.VersionToGitRef(tt.version)
			assert.Equal(t, tt.want, ref)
		})
	}
}

func TestCheckoutRefRejectsOptionShapedRevisions(t *testing.T) {
	tests := []struct {
		name    string
		version string
		wantRef string
		wantOK  bool
	}{
		{name: "Tag", version: "v1.2.3", wantRef: "v1.2.3", wantOK: true},
		{name: "PseudoVersion", version: "v0.0.0-20160603174536-ad42235f7e24",
			wantRef: "ad42235f7e24", wantOK: true},
		{name: "PathspecFromFile", version: "--pathspec-from-file=/etc/passwd"},
		{name: "ShortOption", version: "-f"},
		{name: "Empty", version: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ref, ok := gomod.CheckoutRef(test.version)
			assert.Equal(t, test.wantOK, ok)
			assert.Equal(t, test.wantRef, ref)
		})
	}
}
