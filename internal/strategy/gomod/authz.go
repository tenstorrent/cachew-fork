package gomod

import (
	"net/http"
	"path"
	"strings"

	"golang.org/x/mod/module"

	"github.com/tenstorrent/internal-cachew/internal/gomodobject"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/repoauthz"
)

// authorizeModuleRead gates reads of private modules on the requesting client's
// own credentials, the same decision the git strategy and the object API make
// for the same repositories.
//
// A private module is served from a mirror cachew cloned with its own service
// credentials, so without this the module proxy hands the full source of every
// private repository matching private-paths to any client that can reach the
// endpoint — the confused-deputy hole the git routes already close.
//
// The check has to sit in front of the goproxy handler rather than inside the
// private Fetcher: goproxy consults its Cacher before calling the fetcher at
// all, so a fetcher-level check would be skipped for every module already in
// the (potentially shared, potentially remote) cache.
// The escape hatch turns off this middleware only. Classification of private
// modules stays live, because it is also what marks cached objects private, and
// that marking is persisted and replicated: recording a private module as public
// would outlive the flag and follow the object into every cache tier.
func (s *Strategy) authorizeModuleRead(next http.Handler) http.Handler {
	if s.config.DisablePrivateModuleAuthz {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		modulePath, private := s.privateModulePath(r.URL.Path)
		if !private {
			next.ServeHTTP(w, r)
			return
		}

		ctx := r.Context()
		logger := logging.FromContext(ctx)
		upstream := gomodobject.GitURL(modulePath)
		outcome, err := s.authz.Authorize(ctx, upstream, r.Header.Get("Authorization"))
		if err != nil {
			logger.WarnContext(ctx, "Upstream authorization probe failed, denying private module read",
				"upstream", upstream, "error", err)
		}
		if !outcome.Allowed() {
			logger.InfoContext(ctx, "Denying private module read", "upstream", upstream, "outcome", string(outcome))
			repoauthz.Respond(w, outcome)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// privateModulePath reports the private module path a goproxy request path
// addresses, if any.
//
// The split mirrors goproxy's own: the module path is everything before the
// first "/@". Every candidate spelling of the path is tested and any match
// requires authorization, so a disagreement with goproxy's parsing can only
// ever deny a request goproxy would have served, never the reverse.
func (s *Strategy) privateModulePath(urlPath string) (string, bool) {
	if s.private == nil {
		return "", false
	}
	for _, candidate := range modulePathCandidates(urlPath) {
		if s.private.IsPrivate(candidate) {
			return candidate, true
		}
	}
	return "", false
}

// privateModuleName reports whether a module proxy path, as the goproxy cacher
// spells it, resolves to a private repository.
func (s *Strategy) privateModuleName(name string) bool {
	_, private := s.privateModulePath("/" + name)
	return private
}

func modulePathCandidates(urlPath string) []string {
	var candidates []string
	for _, p := range []string{urlPath, path.Clean(urlPath)} {
		escaped, _, ok := strings.Cut(strings.TrimPrefix(p, "/"), "/@")
		if !ok || escaped == "" {
			continue
		}
		candidates = append(candidates, escaped)
		if unescaped, err := module.UnescapePath(escaped); err == nil && unescaped != escaped {
			candidates = append(candidates, unescaped)
		}
	}
	return candidates
}
