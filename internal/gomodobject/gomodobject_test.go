package gomodobject_test

import (
	"net/http"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/gomodobject"
)

const (
	modulePath = "cachew.invalid/org/private-repo"
	zipName    = modulePath + "/@v/v1.0.0.zip"
)

// TestKeysAreTheModuleProxyPath pins that keys stay the unsalted hash of the
// name the module proxy caches under, which is both what makes objects
// addressable through the object API and what makes their metadata verifiable.
func TestKeysAreTheModuleProxyPath(t *testing.T) {
	object := gomodobject.New(zipName, true)

	assert.Equal(t, cache.NewKey(zipName), object.Key)
	assert.Equal(t, zipName, object.Name)
	assert.True(t, object.Private)
}

func TestModuleIsSelfAuthenticating(t *testing.T) {
	private := gomodobject.New(zipName, true)
	public := gomodobject.New("cachew.invalid/other/public/@v/v1.0.0.zip", false)

	relabelled := private
	relabelled.Name = public.Name
	relabelled.Private = false

	escaped := gomodobject.New("cachew.invalid/org/!private/@v/v1.0.0.zip", true)
	unescapable := gomodobject.New("cachew.invalid/org/Private/@v/v1.0.0.zip", true)
	versionless := gomodobject.New("cachew.invalid/org/private-repo", true)

	tests := []struct {
		name        string
		key         cache.Key
		headers     http.Header
		wantModule  string
		wantPrivate bool
		wantOK      bool
	}{
		{"PrivateRoundTrips", private.Key, private.Headers(), modulePath, true, true},
		{"PublicRoundTrips", public.Key, public.Headers(), "cachew.invalid/other/public", false, true},
		{"SurvivesMergedHeaders", private.Key,
			private.Headers(http.Header{"Content-Type": {"application/zip"}}), modulePath, true, true},
		{"EscapedPathIsUnescaped", escaped.Key, escaped.Headers(), "cachew.invalid/org/Private", true, true},
		{"NoMetadata", private.Key, http.Header{}, "", false, false},
		{"MissingName", private.Key,
			http.Header{gomodobject.PrivateHeader: {"true"}}, "", false, false},
		{"MissingPrivate", private.Key,
			http.Header{gomodobject.NameHeader: {zipName}}, "", false, false},
		// A relabelled object is no longer stored at the key its metadata implies,
		// which is what makes the claim unforgeable.
		{"RelabelledName", private.Key, relabelled.Headers(), "", false, false},
		{"WrongKey", cache.NewKey("elsewhere"), private.Headers(), "", false, false},
		// Fails closed: an unreadable claim must not read as public.
		{"UnparseablePrivate", private.Key,
			http.Header{gomodobject.NameHeader: {zipName}, gomodobject.PrivateHeader: {"maybe"}},
			modulePath, true, true},
		{"UnescapableModulePath", unescapable.Key, unescapable.Headers(), "", true, false},
		{"NoVersionSeparator", versionless.Key, versionless.Headers(), "", true, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotModule, gotPrivate, ok := gomodobject.Module(test.key, test.headers)
			assert.Equal(t, test.wantOK, ok)
			assert.Equal(t, test.wantModule, gotModule)
			assert.Equal(t, test.wantPrivate, gotPrivate)
		})
	}
}

func TestHeadersDoNotMutateTheBase(t *testing.T) {
	base := http.Header{"Content-Type": {"application/zip"}}
	headers := gomodobject.New(zipName, true).Headers(base)

	assert.Equal(t, "application/zip", headers.Get("Content-Type"))
	assert.Equal(t, zipName, headers.Get(gomodobject.NameHeader))
	assert.Equal(t, "true", headers.Get(gomodobject.PrivateHeader))
	assert.Equal(t, http.Header{"Content-Type": {"application/zip"}}, base)
}
