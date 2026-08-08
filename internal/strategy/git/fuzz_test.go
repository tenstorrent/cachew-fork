package git //nolint:testpackage // white-box fuzzing of unexported request-target helpers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/logging"
)

// fuzzMirrorRoot is absolute and fixed so containment is decided lexically,
// without the fuzz target touching the filesystem.
const fuzzMirrorRoot = "/var/lib/cachew/git-mirrors"

// FuzzRepoTargetResolution asserts the invariant the traversal bug broke: for
// any request target cachew accepts, the repository identity the authorization
// check is made against and the repository whose mirror the bytes come from are
// provably the same string, and the mirror path never leaves the mirror root.
//
// The two derivations are independent — the identity is assembled by string
// concatenation in this package, the on-disk path by url.Parse plus
// filepath.Join in gitclone — so any input on which they disagree is an
// opportunity to have access checked against one repository and served from
// another.
//
// "Accepts" means both gates that run before dispatch: validateRepoTarget on the
// raw target, and validateCanonicalRepoPath on the repository path derived from
// it. The second is called here rather than restated, so the target keeps
// exercising the code that actually guards the serve paths; what remains to be
// proven is that every gated path routes through it and that the identity that
// survives it stays under the mirror root.
func FuzzRepoTargetResolution(f *testing.F) {
	seeds := []struct {
		host     string
		path     string
		rawQuery string
	}{
		{"github.com", "org/repo/info/refs", "service=git-upload-pack"},
		{"github.com", "org/repo.git/git-upload-pack", ""},
		{"github.com", "org/repo/snapshot.tar.zst", ""},
		{"github.com", "org/repo/snapshot.bundle", "service=git-receive-pack"},
		{"github.com", "org/repo/lfs-snapshot.tar.zst", ""},
		{"github.com", "org/repo.git/ensure-refs", ""},
		{"github.com", "org/repo.git.git/ensure-refs", ""},
		{"github.com", "org/repo.git.git/snapshot.tar.zst", ""},
		{"github.com", "org/repo/git-receive-pack", ""},
		{"github.com", "org/repo/info/lfs/objects/batch", ""},
		{"github.com", "org/../../private/repo/info/refs", ""},
		{"github.com", "org/%2e%2e/%2e%2e/private/repo/info/refs", ""},
		{"github.com", "org/..%2f..%2fprivate/repo/info/refs", ""},
		{"github.com", "org/repo#fragment/info/refs", ""},
		{"github.com", "org/repo?query=1/info/refs", ""},
		{"github.com/../..", "org/repo/info/refs", ""},
		{"github.com", "org/repo\x00/info/refs", ""},
		{"github.com", "org/repö/info/refs", ""},
		{"", "", ""},
		{"github.com", strings.Repeat("a/", 200) + "info/refs", ""},
	}
	for _, seed := range seeds {
		f.Add(seed.host, seed.path, seed.rawQuery)
	}

	f.Fuzz(func(t *testing.T, host, pathValue, rawQuery string) {
		if err := validateRepoTarget(host, pathValue); err != nil {
			return
		}

		r := &http.Request{
			Method: http.MethodGet,
			URL:    &url.URL{Path: "/git/" + host + "/" + pathValue, RawQuery: rawQuery},
			Header: http.Header{},
		}
		repoPath, gated := repoPathForRequest(r, pathValue)

		if servedFromMirror(pathValue) && !gated {
			t.Fatalf("mirror-serving path %q is not gated", pathValue)
		}
		if gated != mayUseServiceCredentials(r, pathValue) {
			t.Fatalf("gating and credential substitution disagree for %q", pathValue)
		}
		if !gated {
			return
		}
		if validateCanonicalRepoPath(host, repoPath) != nil {
			return // Denied before dispatch, so nothing is served.
		}

		// The identity every gated serve path authorizes against.
		identity := host + "/" + repoPath
		onDisk, err := gitclone.RepoPathFromURL("https://" + identity)
		if err != nil {
			return // Resolution fails closed, so nothing is served.
		}
		if onDisk != identity {
			t.Fatalf("identity divergence: authorization checks %q, mirror resolves to %q", identity, onDisk)
		}
		if joined := filepath.Join(fuzzMirrorRoot, onDisk); !strings.HasPrefix(joined, fuzzMirrorRoot+"/") {
			t.Fatalf("mirror path %q escapes %q", joined, fuzzMirrorRoot)
		}
	})
}

// FuzzUpstreamCredentialForwarding asserts that what the authorization check
// examines and what the upstream leg carries cannot diverge: cachew's own
// credentials may only ever replace the client's on a path that was gated, and
// only for the organisation that gating authorized. Anywhere else the client's
// Authorization header must survive byte-for-byte, absence included, because
// upstream is the only authority on those paths.
func FuzzUpstreamCredentialForwarding(f *testing.F) {
	seeds := []struct {
		host       string
		path       string
		rawQuery   string
		clientAuth string
		extraAuth  string
		token      string
	}{
		{"github.com", "org/repo/info/refs", "service=git-upload-pack", "Bearer client-token", "", "service-token"},
		{"github.com", "org/repo/git-receive-pack", "", "Bearer client-token", "", "service-token"},
		{"github.com", "org/repo/info/refs", "service=git-receive-pack", "Basic dXNlcjpwdw==", "", "service-token"},
		{"github.com", "org/repo/info/lfs/objects/batch", "", "Bearer client-token", "", "service-token"},
		{"github.com", "org/repo/snapshot.tar.zst", "", "", "", "service-token"},
		{"ghe.example.com", "org/repo/info/refs", "", "Bearer client-token", "", "service-token"},
		{"github.com", "org/repo/info/refs", "", "Bearer a", "Bearer b", "service-token"},
		{"github.com", "org/repo/info/refs", "", "Bearer \x00\x7f", "", "service-token"},
		{"github.com", "org/repo/info/refs", "", "Bearer x\r\nX-Injected: 1", "", "tok\r\nX-Injected: 1"},
		{"github.com", "org/repo/info/refs", "", strings.Repeat("A", 8192), "", strings.Repeat("t", 8192)},
		{"github.com", "org/repo/info/refs", "", "Bearer ü", "", ""},
	}
	for _, seed := range seeds {
		f.Add(seed.host, seed.path, seed.rawQuery, seed.clientAuth, seed.extraAuth, seed.token)
	}

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})

	f.Fuzz(func(t *testing.T, host, pathValue, rawQuery, clientAuth, extraAuth, token string) {
		if err := validateRepoTarget(host, pathValue); err != nil {
			return
		}

		r := httptest.NewRequest(http.MethodGet, "/git/"+url.PathEscape(host)+"/x", nil).WithContext(ctx)
		r.URL.RawQuery = rawQuery
		r.SetPathValue("host", host)
		r.SetPathValue("path", pathValue)
		clientValues := []string{}
		for _, value := range []string{clientAuth, extraAuth} {
			if value == "" {
				continue
			}
			r.Header.Add("Authorization", value)
			clientValues = append(clientValues, value)
		}

		repoPath, gated := repoPathForRequest(r, pathValue)
		var requestedOrgs []string
		s := &Strategy{}
		s.SetServiceToken(func(_ context.Context, org string) (string, error) {
			requestedOrgs = append(requestedOrgs, org)
			return token, nil
		})
		s.directUpstream(r)

		forwarded := r.Header.Values("Authorization")
		if len(requestedOrgs) > 0 {
			org, _, _ := strings.Cut(repoPath, "/")
			if !gated {
				t.Fatalf("service credentials resolved for ungated path %q", pathValue)
			}
			if len(requestedOrgs) != 1 || requestedOrgs[0] != org {
				t.Fatalf("credentials resolved for %v, but authorization gated %q", requestedOrgs, org)
			}
		}
		if slices.Equal(forwarded, clientValues) {
			return
		}
		switch {
		case !gated:
			t.Fatalf("client credentials %v rewritten to %v on ungated path %q", clientValues, forwarded, pathValue)

		case host != "github.com":
			t.Fatalf("client credentials %v rewritten to %v for non-GitHub host %q", clientValues, forwarded, host)

		case !slices.Equal(forwarded, []string{basicAuthHeader("x-access-token", token)}):
			t.Fatalf("upstream leg carries %v, not exactly cachew's own credentials", forwarded)
		}
	})
}

// servedFromMirror reports whether a path is answered from (or fetched into)
// the local mirror regardless of the request's query string, and so must always
// be gated.
func servedFromMirror(pathValue string) bool {
	for _, suffix := range []string{EnsureRefsPath, "/snapshot.tar.zst", "/snapshot.bundle", "/lfs-snapshot.tar.zst"} {
		if strings.HasSuffix(pathValue, suffix) {
			return true
		}
	}
	return false
}
