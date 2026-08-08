package gitobject_test

import (
	"net/http"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/gitobject"
)

const upstream = "https://github.com/acme/private-repo"

// TestKeysMatchLegacyDerivation pins the key derivation this package took over
// from the git strategy, so moving it here cannot silently invalidate every
// object already in a deployed cache.
func TestKeysMatchLegacyDerivation(t *testing.T) {
	tests := []struct {
		name   string
		object gitobject.Object
		suffix string
	}{
		{"Snapshot", gitobject.Snapshot(upstream), ".snapshot"},
		{"MirrorSnapshot", gitobject.MirrorSnapshot(upstream), ".mirror-snapshot"},
		{"LFSSnapshot", gitobject.LFSSnapshot(upstream), ".lfs-snapshot"},
		{"Bundle", gitobject.Bundle(upstream, "abc123"), ".bundle.abc123"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, cache.NewKey(upstream+test.suffix), test.object.Key)
			assert.Equal(t, upstream, test.object.Upstream)
		})
	}
}

func TestRepositoryIsSelfAuthenticating(t *testing.T) {
	object := gitobject.Snapshot(upstream)

	relabelled := object
	relabelled.Upstream = "https://github.com/acme/public-repo"

	tests := []struct {
		name         string
		key          cache.Key
		headers      http.Header
		wantUpstream string
		wantOK       bool
	}{
		{"RoundTrips", object.Key, object.Headers(), upstream, true},
		{"SurvivesMergedHeaders", object.Key,
			object.Headers(http.Header{"Content-Type": {"application/zstd"}}), upstream, true},
		{"NoMetadata", object.Key, http.Header{}, "", false},
		// A relabelled object is no longer stored at the key its metadata
		// implies, which is what makes the claim unforgeable.
		{"RelabelledUpstream", object.Key, relabelled.Headers(), "", false},
		{"MissingKind", object.Key,
			http.Header{gitobject.UpstreamHeader: {upstream}}, "", false},
		{"MissingUpstream", object.Key,
			http.Header{gitobject.KindHeader: {"snapshot"}}, "", false},
		{"WrongKey", cache.NewKey("elsewhere"), object.Headers(), "", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotUpstream, ok := gitobject.Repository(test.key, test.headers)
			assert.Equal(t, test.wantOK, ok)
			assert.Equal(t, test.wantUpstream, gotUpstream)
		})
	}
}

func TestHeadersDoNotMutateTheBase(t *testing.T) {
	base := http.Header{"Content-Type": {"application/zstd"}}
	headers := gitobject.Snapshot(upstream).Headers(base)

	assert.Equal(t, "application/zstd", headers.Get("Content-Type"))
	assert.Equal(t, upstream, headers.Get(gitobject.UpstreamHeader))
	assert.Equal(t, http.Header{"Content-Type": {"application/zstd"}}, base)
}
