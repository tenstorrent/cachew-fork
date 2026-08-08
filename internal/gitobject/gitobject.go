// Package gitobject derives the cache keys and object metadata for the
// artefacts the git strategy caches on behalf of an upstream repository.
//
// It lives outside internal/strategy/git because the generic object API in
// internal/strategy must recover the repository a cached object belongs to in
// order to authorize reads of it, and internal/strategy/git imports
// internal/strategy, so the dependency cannot run the other way.
package gitobject

import (
	"maps"
	"net/http"

	"github.com/tenstorrent/internal-cachew/internal/cache"
)

// Namespace is the cache namespace the git strategy's objects live in. It is
// the strategy's registered name, which is what the strategy registry
// namespaces a strategy's cache by.
const Namespace = "git"

const (
	// UpstreamHeader is object metadata carrying the upstream repository URL an
	// object was cached for.
	UpstreamHeader = "X-Cachew-Git-Upstream"
	// KindHeader is object metadata carrying the key suffix identifying which
	// artefact of that repository an object holds.
	KindHeader = "X-Cachew-Git-Object"
)

// MetadataHeaders are the object metadata headers this package owns. Callers
// that persist client-supplied metadata must strip them, so that a client
// cannot assert a repository of its own choosing.
var MetadataHeaders = []string{UpstreamHeader, KindHeader} //nolint:gochecknoglobals

// An Object is the cache key of one repository artefact paired with the
// metadata recording the repository it belongs to.
//
// Key and metadata are produced together so a caller cannot write a git object
// without also recording its repository, which is what makes authorizing reads
// of that object possible at all.
type Object struct {
	// Key is the object's cache key.
	Key cache.Key
	// Upstream is the upstream repository URL the object was cached for.
	Upstream string
	// Kind is the key suffix identifying which artefact the object holds.
	Kind string
}

// Snapshot is a repository's working-tree snapshot archive.
func Snapshot(upstreamURL string) Object { return newObject(upstreamURL, "snapshot") }

// MirrorSnapshot is the archive of a repository's bare mirror.
func MirrorSnapshot(upstreamURL string) Object { return newObject(upstreamURL, "mirror-snapshot") }

// LFSSnapshot is the archive of a repository's fetched Git LFS objects.
func LFSSnapshot(upstreamURL string) Object { return newObject(upstreamURL, "lfs-snapshot") }

// Bundle is the git bundle carrying the commits between baseCommit and a
// repository's HEAD.
func Bundle(upstreamURL, baseCommit string) Object {
	return newObject(upstreamURL, "bundle."+baseCommit)
}

func newObject(upstreamURL, kind string) Object {
	return Object{Key: cache.NewKey(upstreamURL + "." + kind), Upstream: upstreamURL, Kind: kind}
}

// Headers returns the object metadata recording this object's repository,
// merged over any headers the caller supplies.
func (o Object) Headers(base ...http.Header) http.Header {
	headers := http.Header{}
	for _, h := range base {
		maps.Copy(headers, h.Clone())
	}
	headers.Set(UpstreamHeader, o.Upstream)
	headers.Set(KindHeader, o.Kind)
	return headers
}

// Repository returns the upstream repository URL a cached object belongs to,
// and whether that could be established at all.
//
// The recorded claim is self-authenticating. Keys are the unsalted hash of
// upstream and kind, so recomputing the key from the metadata proves the
// metadata describes this particular object: an attacker cannot relabel a
// private repository's snapshot as belonging to a public repository to escape
// authorization, because the relabelled object would no longer be stored at the
// key its metadata implies. Objects whose metadata is absent or does not
// reproduce their key have no recoverable repository, and callers must fail
// closed on them rather than guess.
func Repository(key cache.Key, headers http.Header) (upstreamURL string, ok bool) {
	upstream := headers.Get(UpstreamHeader)
	kind := headers.Get(KindHeader)
	if upstream == "" || kind == "" {
		return "", false
	}
	if cache.NewKey(upstream+"."+kind) != key {
		return "", false
	}
	return upstream, true
}
