// Package gomodobject derives the cache keys and object metadata for the
// module files the gomod strategy caches on behalf of an upstream repository.
//
// It lives outside internal/strategy/gomod for the same reason
// internal/gitobject lives outside internal/strategy/git: the generic object API
// in internal/strategy must recover the repository a cached object belongs to in
// order to authorize reads of it, and internal/strategy/gomod imports
// internal/strategy, so the dependency cannot run the other way.
package gomodobject

import (
	"maps"
	"net/http"
	"strconv"
	"strings"

	"golang.org/x/mod/module"

	"github.com/tenstorrent/internal-cachew/internal/cache"
)

// Namespace is the cache namespace the gomod strategy's objects live in. It is
// the strategy's registered name, which is what the strategy registry
// namespaces a strategy's cache by.
const Namespace = "gomod"

const (
	// NameHeader is object metadata carrying the module proxy path an object was
	// cached for, e.g. "example.com/mod/@v/v1.0.0.zip".
	NameHeader = "X-Cachew-Gomod-Name"
	// PrivateHeader is object metadata recording whether that module was
	// resolved from a private repository, and so whether reads of the object
	// need authorizing.
	PrivateHeader = "X-Cachew-Gomod-Private"
)

// MetadataHeaders are the object metadata headers this package owns.
var MetadataHeaders = []string{NameHeader, PrivateHeader} //nolint:gochecknoglobals

// An Object is the cache key of one cached module file paired with the metadata
// recording the module it belongs to.
//
// Key and metadata are produced together so a caller cannot write a module file
// without also recording its module, which is what makes authorizing reads of
// that object possible at all.
type Object struct {
	// Key is the object's cache key.
	Key cache.Key
	// Name is the module proxy path the object was cached for.
	Name string
	// Private reports whether the module resolves to a private repository.
	Private bool
}

// New is the object holding the module proxy path name, cached from a private
// repository or not.
func New(name string, private bool) Object {
	return Object{Key: cache.NewKey(name), Name: name, Private: private}
}

// Headers returns the object metadata recording this object's module, merged
// over any headers the caller supplies.
func (o Object) Headers(base ...http.Header) http.Header {
	headers := http.Header{}
	for _, h := range base {
		maps.Copy(headers, h.Clone())
	}
	headers.Set(NameHeader, o.Name)
	headers.Set(PrivateHeader, strconv.FormatBool(o.Private))
	return headers
}

// Module returns the module path a cached object belongs to, whether that module
// resolves to a private repository, and whether any of that could be
// established at all.
//
// The recorded claim is self-authenticating in the same way internal/gitobject's
// is: keys are the unsalted hash of the module proxy path, so recomputing the
// key from the metadata proves the metadata describes this particular object. A
// client cannot relabel a private module's zip as some public module's to escape
// authorization, because the relabelled object would no longer be stored at the
// key its metadata implies — and it cannot flip Private on an object it did not
// write, because rewriting the metadata means rewriting the content too.
//
// Objects whose metadata is absent or does not reproduce their key have no
// recoverable module, and callers must fail closed on them rather than guess.
func Module(key cache.Key, headers http.Header) (modulePath string, private, ok bool) {
	name := headers.Get(NameHeader)
	rawPrivate := headers.Get(PrivateHeader)
	if name == "" || rawPrivate == "" {
		return "", false, false
	}
	if cache.NewKey(name) != key {
		return "", false, false
	}
	// Anything but an explicit "false" is treated as private: the alternative is
	// serving an unreadable claim's object to anyone.
	private, err := strconv.ParseBool(rawPrivate)
	if err != nil {
		private = true
	}
	// The module path is everything before the first "/@", which is the split
	// the module proxy itself makes when building these paths.
	escaped, _, found := strings.Cut(name, "/@")
	if !found || escaped == "" {
		return "", private, false
	}
	unescaped, err := module.UnescapePath(escaped)
	if err != nil {
		return "", private, false
	}
	return unescaped, private, true
}

// GitURL is the single derivation of the repository a module path names.
// Authorization and content resolution share it so that the repository
// authorized against is always the repository served.
func GitURL(modulePath string) string {
	return "https://" + modulePath
}
