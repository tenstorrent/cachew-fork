package gomod_test

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/gomodobject"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/strategy/gomod"
)

const (
	privateModule = "cachew.invalid/org/private-repo"
	clientUser    = "client"
	clientToken   = "client-token"
)

// privateUpstream serves bare repositories under root via `git http-backend`,
// refusing anonymous requests, so cachew's authorization probes get the same
// answers a real private host would give.
type privateUpstream struct {
	root    string
	gitPath string
}

func newPrivateUpstream(t *testing.T, root string) *privateUpstream {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not found in PATH")
	}
	return &privateUpstream{root: root, gitPath: gitPath}
}

func (u *privateUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user, password, hasAuth := r.BasicAuth()
	if !hasAuth || user != clientUser || password != clientToken {
		w.Header().Set("WWW-Authenticate", `Basic realm="upstream"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	r2 := r.Clone(r.Context())
	r2.URL.Path = strings.Replace(r.URL.Path, ".git/", "/", 1)
	handler := &cgi.Handler{
		Path: u.gitPath,
		Args: []string{"http-backend"},
		Dir:  u.root,
		Env: []string{
			"GIT_PROJECT_ROOT=" + u.root,
			"GIT_HTTP_EXPORT_ALL=1",
			"REMOTE_USER=" + user,
			"PATH=" + os.Getenv("PATH"),
		},
	}
	handler.ServeHTTP(w, r2)
}

// rewriteToUpstream sends the https:// probes the authorizer builds from a
// module path to a local test upstream.
type rewriteToUpstream struct {
	target *url.URL
	inner  http.RoundTripper
}

func (t *rewriteToUpstream) RoundTrip(r *http.Request) (*http.Response, error) {
	clone := r.Clone(r.Context())
	// Mirrors are laid out by host under the mirror root, so keep the host as the
	// leading path segment when redirecting the probe at the local upstream.
	clone.URL.Path = "/" + r.URL.Host + r.URL.Path
	clone.URL.Scheme = t.target.Scheme
	clone.URL.Host = t.target.Host
	clone.Host = ""
	return t.inner.RoundTrip(clone) //nolint:wrapcheck // transport must return the round tripper's error unwrapped
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	assert.NoError(t, err, "git %v: %s", args, string(out))
}

// seedPrivateMirror creates the bare mirror the clone manager would have
// created for the module's repository, as if cachew had already cloned it with
// its own service credentials, and returns the mirror root.
func seedPrivateMirror(t *testing.T) string {
	t.Helper()
	mirrorRoot := t.TempDir()
	mirror := filepath.Join(mirrorRoot, filepath.FromSlash(privateModule))
	assert.NoError(t, os.MkdirAll(filepath.Dir(mirror), 0o700))

	work := t.TempDir()
	runGit(t, work, "init", "-q", "-b", "main", ".")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "test")
	assert.NoError(t, os.WriteFile(filepath.Join(work, "go.mod"),
		[]byte("module "+privateModule+"\n\ngo 1.24\n"), 0o600))
	assert.NoError(t, os.WriteFile(filepath.Join(work, "secret.go"),
		[]byte("package private\n\nconst Secret = \"private-source\"\n"), 0o600))
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-qm", "initial")
	runGit(t, work, "tag", "v1.0.0")
	runGit(t, work, "clone", "-q", "--bare", work, mirror)
	return mirrorRoot
}

func newGomodStrategy(t *testing.T, config gomod.Config) (*httptest.Server, cache.Cache) {
	t.Helper()
	mirrorRoot := seedPrivateMirror(t)
	upstream := httptest.NewServer(newPrivateUpstream(t, mirrorRoot))
	t.Cleanup(upstream.Close)

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 64, MaxTTL: time.Hour})
	assert.NoError(t, err)
	provider := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)

	mux := http.NewServeMux()
	strategy, err := gomod.New(ctx, config, memCache, mux, provider, nil)
	assert.NoError(t, err)
	logger := logging.FromContext(ctx)
	// Production requests reach strategies through logging middleware.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(logging.ContextWithLogger(r.Context(), logger)))
	})

	upstreamURL, err := url.Parse(upstream.URL)
	assert.NoError(t, err)
	strategy.SetAuthzHTTPClient(&http.Client{
		Transport: &rewriteToUpstream{target: upstreamURL, inner: http.DefaultTransport},
	})

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server, memCache
}

func get(t *testing.T, server *httptest.Server, target, authorization string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+target, nil)
	assert.NoError(t, err)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	return resp.StatusCode, body
}

// TestPrivateModuleReadsAreAuthorized covers the module proxy's half of the
// confused-deputy problem: private module content comes from a mirror cloned
// with cachew's own credentials, so serving it must depend on the client's.
func TestPrivateModuleReadsAreAuthorized(t *testing.T) {
	targets := []struct {
		name   string
		target string
	}{
		{name: "List", target: "/gomod/" + privateModule + "/@v/list"},
		{name: "Latest", target: "/gomod/" + privateModule + "/@latest"},
		{name: "Info", target: "/gomod/" + privateModule + "/@v/v1.0.0.info"},
		{name: "Mod", target: "/gomod/" + privateModule + "/@v/v1.0.0.mod"},
		{name: "Zip", target: "/gomod/" + privateModule + "/@v/v1.0.0.zip"},
	}

	t.Run("AnonymousIsDenied", func(t *testing.T) {
		server, _ := newGomodStrategy(t, gomod.Config{
			Proxy:        "https://proxy.golang.org",
			PrivatePaths: []string{"cachew.invalid/org/*"},
		})
		for _, tt := range targets {
			t.Run(tt.name, func(t *testing.T) {
				status, body := get(t, server, tt.target, "")
				assert.Equal(t, http.StatusUnauthorized, status)
				assert.False(t, bytes.Contains(body, []byte("private-source")))
			})
		}
	})

	t.Run("WrongCredentialsAreDenied", func(t *testing.T) {
		server, _ := newGomodStrategy(t, gomod.Config{
			Proxy:        "https://proxy.golang.org",
			PrivatePaths: []string{"cachew.invalid/org/*"},
		})
		for _, tt := range targets {
			t.Run(tt.name, func(t *testing.T) {
				status, body := get(t, server, tt.target, basic("attacker", "wrong"))
				assert.Equal(t, http.StatusForbidden, status)
				assert.False(t, bytes.Contains(body, []byte("private-source")))
			})
		}
	})

	t.Run("ClientCredentialsAreAccepted", func(t *testing.T) {
		server, _ := newGomodStrategy(t, gomod.Config{
			Proxy:        "https://proxy.golang.org",
			PrivatePaths: []string{"cachew.invalid/org/*"},
		})
		for _, tt := range targets {
			t.Run(tt.name, func(t *testing.T) {
				status, body := get(t, server, tt.target, basic(clientUser, clientToken))
				assert.Equal(t, http.StatusOK, status)
				assert.True(t, len(body) > 0)
			})
		}
	})

	// A cached module must not become readable without credentials: goproxy
	// answers from its Cacher before consulting the fetcher, which is why the
	// check cannot live in the fetcher.
	t.Run("CachedModuleStillRequiresCredentials", func(t *testing.T) {
		server, _ := newGomodStrategy(t, gomod.Config{
			Proxy:        "https://proxy.golang.org",
			PrivatePaths: []string{"cachew.invalid/org/*"},
		})
		target := "/gomod/" + privateModule + "/@v/v1.0.0.zip"
		status, body := get(t, server, target, basic(clientUser, clientToken))
		assert.Equal(t, http.StatusOK, status)
		assert.True(t, zipContainsPrivateSource(t, body))

		status, body = get(t, server, target, "")
		assert.Equal(t, http.StatusUnauthorized, status)
		assert.False(t, bytes.Contains(body, []byte("private-source")))
	})

	t.Run("PublicModulePathsAreNotGated", func(t *testing.T) {
		server, _ := newGomodStrategy(t, gomod.Config{
			Proxy:        "https://proxy.golang.org",
			PrivatePaths: []string{"cachew.invalid/org/*"},
		})
		// Not a private path, so it is never authorized against upstream; the
		// public fetcher answers (and fails, with no network here) instead of
		// the request being refused for want of credentials.
		status, _ := get(t, server, "/gomod/cachew.invalid/other/public/@v/list", "")
		assert.NotEqual(t, http.StatusUnauthorized, status)
		assert.NotEqual(t, http.StatusForbidden, status)
	})

	// The escape hatch stops the module proxy authorizing reads. It must not also
	// relabel the objects it caches as public: that metadata is persisted and
	// replicated, so it would outlive the flag and travel to every cache tier,
	// leaving the object API serving private sources anonymously.
	t.Run("EscapeHatchRestoresUnauthenticatedReadsWithoutRelabellingObjects", func(t *testing.T) {
		server, memCache := newGomodStrategy(t, gomod.Config{
			Proxy:                     "https://proxy.golang.org",
			PrivatePaths:              []string{"cachew.invalid/org/*"},
			DisablePrivateModuleAuthz: true,
		})
		const name = privateModule + "/@v/v1.0.0.zip"
		status, body := get(t, server, "/gomod/"+name, "")
		assert.Equal(t, http.StatusOK, status)
		assert.True(t, zipContainsPrivateSource(t, body))

		key := cache.NewKey(name)
		rc, headers, err := memCache.Open(context.Background(), key)
		assert.NoError(t, err)
		assert.NoError(t, rc.Close())
		modulePath, private, ok := gomodobject.Module(key, headers)
		assert.True(t, ok)
		assert.Equal(t, privateModule, modulePath)
		assert.True(t, private, "the escape hatch cached a private module as public")
	})
}

// TestPrivateModulePathTraversalDoesNotEscapeTheGate checks that no spelling of
// a private module's path reaches content without authorization.
func TestPrivateModulePathTraversalDoesNotEscapeTheGate(t *testing.T) {
	server, _ := newGomodStrategy(t, gomod.Config{
		Proxy:        "https://proxy.golang.org",
		PrivatePaths: []string{"cachew.invalid/org/*"},
	})

	targets := []struct {
		name   string
		target string
	}{
		{name: "DotSegment", target: "/gomod/cachew.invalid/org/./private-repo/@v/v1.0.0.zip"},
		{name: "ParentSegment", target: "/gomod/cachew.invalid/other/../org/private-repo/@v/v1.0.0.zip"},
		{name: "EncodedDotSegment", target: "/gomod/cachew.invalid/org/%2e/private-repo/@v/v1.0.0.zip"},
		{name: "EncodedParentSegment", target: "/gomod/cachew.invalid/x/%2e%2e/org/private-repo/@v/v1.0.0.zip"},
		{name: "DoubleSlash", target: "/gomod/cachew.invalid/org//private-repo/@v/v1.0.0.zip"},
		{name: "EncodedSlash", target: "/gomod/cachew.invalid/org%2fprivate-repo/@v/v1.0.0.zip"},
		{name: "TrailingDot", target: "/gomod/cachew.invalid/org/private-repo./@v/v1.0.0.zip"},
		{name: "MixedCase", target: "/gomod/cachew.invalid/org/Private-Repo/@v/v1.0.0.zip"},
	}
	for _, tt := range targets {
		t.Run(tt.name, func(t *testing.T) {
			status, body := get(t, server, tt.target, "")
			assert.False(t, bytes.Contains(body, []byte("private-source")),
				"served private content for %s (status %d)", tt.target, status)
		})
	}
}

func basic(user, password string) string {
	req := &http.Request{Header: http.Header{}}
	req.SetBasicAuth(user, password)
	return req.Header.Get("Authorization")
}

// zipContainsPrivateSource reports whether a module zip carries the marker only
// the private fixture's source contains, which is what distinguishes real module
// content from an empty or public-only response.
func zipContainsPrivateSource(t *testing.T, body []byte) bool {
	t.Helper()
	const want = "private-source"
	reader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	assert.NoError(t, err)
	for _, file := range reader.File {
		contents, err := file.Open()
		assert.NoError(t, err)
		data, err := io.ReadAll(contents)
		assert.NoError(t, err)
		assert.NoError(t, contents.Close())
		if bytes.Contains(data, []byte(want)) {
			return true
		}
	}
	return false
}
