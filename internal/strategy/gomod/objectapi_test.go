package gomod_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/gomodobject"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/repoauthz"
	"github.com/tenstorrent/internal-cachew/internal/strategy"
	"github.com/tenstorrent/internal-cachew/internal/strategy/gomod"
)

// probeCounter separates the two kinds of request the authorizer makes. The
// anonymous ref advertisement is the one a cached visibility verdict avoids;
// the credentialed one is the per-client decision, which is deliberately never
// cached and so must recur on every request.
type probeCounter struct {
	inner http.RoundTripper

	mu           sync.Mutex
	anonymous    int
	credentialed int
}

func (p *probeCounter) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Query().Get("service") == "git-upload-pack" {
		p.mu.Lock()
		if r.Header.Get("Authorization") == "" {
			p.anonymous++
		} else {
			p.credentialed++
		}
		p.mu.Unlock()
	}
	return p.inner.RoundTrip(r) //nolint:wrapcheck // transport must return the round tripper's error unwrapped
}

func (p *probeCounter) counts() (anonymous, credentialed int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.anonymous, p.credentialed
}

// mountGomodObjectAPI mounts the module proxy and the object API over one root
// cache, the way cachewd does, so that requests can address the module proxy's
// cached objects directly through the object API. The two Authorizers are
// supplied separately so a test can mount them the way cachewd does — one
// instance for both — or deliberately not, to show what that costs.
func mountGomodObjectAPI(
	t *testing.T,
	config gomod.Config,
	proxyAuthz, apiAuthz *repoauthz.Authorizer,
) (*httptest.Server, cache.Cache, *probeCounter) {
	t.Helper()
	mirrorRoot := seedPrivateMirror(t)
	upstream := httptest.NewServer(newPrivateUpstream(t, mirrorRoot))
	t.Cleanup(upstream.Close)

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	root, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 64, MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, root.Close()) })
	provider := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)

	mux := http.NewServeMux()
	_, err = gomod.New(ctx, config, root.Namespace(gomodobject.Namespace), mux, provider, proxyAuthz)
	assert.NoError(t, err)
	_, err = strategy.NewAPIV1(ctx, strategy.APIV1Config{}, root.Namespace("apiv1"), mux, apiAuthz, nil)
	assert.NoError(t, err)

	upstreamURL, err := url.Parse(upstream.URL)
	assert.NoError(t, err)
	probes := &probeCounter{inner: &rewriteToUpstream{target: upstreamURL, inner: http.DefaultTransport}}
	probeClient := &http.Client{Transport: probes}
	proxyAuthz.SetHTTPClient(probeClient)
	if apiAuthz != proxyAuthz {
		apiAuthz.SetHTTPClient(probeClient)
	}

	logger := logging.FromContext(ctx)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(logging.ContextWithLogger(r.Context(), logger)))
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server, root, probes
}

func newGomodObjectAPI(t *testing.T, config gomod.Config) (*httptest.Server, cache.Cache) {
	t.Helper()
	authz := repoauthz.NewAuthorizer(nil, 0)
	server, root, _ := mountGomodObjectAPI(t, config, authz, authz)
	return server, root
}

func objectPath(namespace, name string) string {
	key := cache.NewKey(name)
	return "/api/v1/object/" + namespace + "/" + key.String()
}

// TestPrivateModuleObjectReadsAreAuthorized covers the other way into the module
// proxy's cache: the object API namespaces by a name the caller supplies, so
// naming the module proxy's namespace addresses its objects without passing
// through its handlers. Keys are the unsalted hash of a guessable module path,
// so this is reachable by anyone who can name a private module and version.
func TestPrivateModuleObjectReadsAreAuthorized(t *testing.T) {
	config := gomod.Config{
		Proxy:        "https://proxy.golang.org",
		PrivatePaths: []string{"cachew.invalid/org/*"},
	}
	zipName := privateModule + "/@v/v1.0.0.zip"

	warm := func(t *testing.T) (*httptest.Server, cache.Cache) {
		t.Helper()
		server, root := newGomodObjectAPI(t, config)
		status, body := get(t, server, "/gomod/"+zipName, basic(clientUser, clientToken))
		assert.Equal(t, http.StatusOK, status)
		assert.True(t, zipContainsPrivateSource(t, body))
		return server, root
	}

	// Denials report absence rather than refusal: keys are the unsalted hash of a
	// guessable module path, so a distinguishable refusal would tell an anonymous
	// client which private module versions this deployment has cached.
	t.Run("AnonymousIsReportedAbsent", func(t *testing.T) {
		server, _ := warm(t)
		for _, name := range []string{
			zipName,
			privateModule + "/@v/v1.0.0.mod",
			privateModule + "/@v/v1.0.0.info",
		} {
			status, body := get(t, server, objectPath(gomodobject.Namespace, name), "")
			assert.Equal(t, http.StatusNotFound, status, "object %s", name)
			assert.False(t, bytes.Contains(body, []byte("private-source")))
			assert.False(t, bytes.Contains(body, []byte("secret.go")))
		}
	})

	t.Run("WrongCredentialsAreReportedAbsent", func(t *testing.T) {
		server, _ := warm(t)
		status, body := get(t, server, objectPath(gomodobject.Namespace, zipName), basic("attacker", "wrong"))
		assert.Equal(t, http.StatusNotFound, status)
		assert.False(t, bytes.Contains(body, []byte("private-source")))
	})

	t.Run("ClientCredentialsAreAccepted", func(t *testing.T) {
		server, _ := warm(t)
		status, body := get(t, server, objectPath(gomodobject.Namespace, zipName), basic(clientUser, clientToken))
		assert.Equal(t, http.StatusOK, status)
		assert.True(t, zipContainsPrivateSource(t, body))
	})

	// Namespaces are byte-exact in every backend's metadata but collate to one
	// directory on a case-insensitive filesystem, so a differently cased spelling
	// must not become an unauthorized view of the same objects.
	t.Run("CaseVariantNamespaceIsNotAWayIn", func(t *testing.T) {
		server, _ := warm(t)
		for _, namespace := range []string{"GOMOD", "GoMod"} {
			status, body := get(t, server, objectPath(namespace, zipName), "")
			assert.Equal(t, http.StatusNotFound, status, "namespace %s", namespace)
			assert.False(t, bytes.Contains(body, []byte("private-source")))
		}
	})

	t.Run("HeadIsGatedLikeGet", func(t *testing.T) {
		server, _ := warm(t)
		req, err := http.NewRequestWithContext(context.Background(), http.MethodHead,
			server.URL+objectPath(gomodobject.Namespace, zipName), nil)
		assert.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err)
		assert.NoError(t, resp.Body.Close())
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	// Objects predating this metadata, or written by anything that does not record
	// it, may well be private module source, so they are reported as absent
	// rather than served — the same way the git namespace fails closed.
	t.Run("UnattributableObjectsAreNotServed", func(t *testing.T) {
		server, root := newGomodObjectAPI(t, config)
		name := privateModule + "/@v/v2.0.0.zip"
		writeObject(t, root.Namespace(gomodobject.Namespace), cache.NewKey(name), nil, "private-source")

		status, body := get(t, server, objectPath(gomodobject.Namespace, name), "")
		assert.Equal(t, http.StatusNotFound, status)
		assert.False(t, bytes.Contains(body, []byte("private-source")))
	})

	t.Run("PublicModuleObjectsAreServedAnonymously", func(t *testing.T) {
		server, root := newGomodObjectAPI(t, config)
		name := "cachew.invalid/other/public/@v/v1.0.0.mod"
		object := gomodobject.New(name, false)
		writeObject(t, root.Namespace(gomodobject.Namespace), object.Key, object.Headers(), "module public")

		status, body := get(t, server, objectPath(gomodobject.Namespace, name), "")
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "module public", string(body))
	})

	// The claim is only trustworthy because it reproduces the key, so a private
	// object relabelled as some public module's must not be served.
	t.Run("RelabelledObjectsAreNotServed", func(t *testing.T) {
		server, root := newGomodObjectAPI(t, config)
		public := gomodobject.New("cachew.invalid/other/public/@v/v1.0.0.zip", false)
		writeObject(t, root.Namespace(gomodobject.Namespace), cache.NewKey(zipName), public.Headers(), "private-source")

		status, body := get(t, server, objectPath(gomodobject.Namespace, zipName), "")
		assert.Equal(t, http.StatusNotFound, status)
		assert.False(t, bytes.Contains(body, []byte("private-source")))
	})
}

func writeObject(t *testing.T, c cache.Cache, key cache.Key, headers http.Header, content string) {
	t.Helper()
	w, err := c.Create(context.Background(), key, headers, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte(content))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
}

// TestVisibilityVerdictIsSharedBetweenEntryPoints proves the module proxy and
// the object API reach their decisions about a private repository through one
// verdict cache. Both authorize the same repository by the same means, so
// whichever entry point probes upstream first, the other must reuse that answer
// instead of paying for — and storing — its own.
func TestVisibilityVerdictIsSharedBetweenEntryPoints(t *testing.T) {
	config := gomod.Config{
		Proxy:        "https://proxy.golang.org",
		PrivatePaths: []string{"cachew.invalid/org/*"},
	}
	zipName := privateModule + "/@v/v1.0.0.zip"
	modName := privateModule + "/@v/v1.0.0.mod"
	modContent := "module " + privateModule + "\n\ngo 1.24\n"

	t.Run("ModuleProxyProbesFirst", func(t *testing.T) {
		authz := repoauthz.NewAuthorizer(nil, 0)
		server, _, probes := mountGomodObjectAPI(t, config, authz, authz)

		status, body := get(t, server, "/gomod/"+zipName, basic(clientUser, clientToken))
		assert.Equal(t, http.StatusOK, status)
		assert.True(t, zipContainsPrivateSource(t, body))
		anonymous, credentialed := probes.counts()
		assert.Equal(t, 1, anonymous)
		assert.Equal(t, 1, credentialed)

		status, body = get(t, server, objectPath(gomodobject.Namespace, zipName), basic(clientUser, clientToken))
		assert.Equal(t, http.StatusOK, status)
		assert.True(t, zipContainsPrivateSource(t, body))
		anonymous, credentialed = probes.counts()
		assert.Equal(t, 1, anonymous, "the object API re-probed a repository the module proxy had already resolved")
		assert.Equal(t, 2, credentialed, "the per-client decision must not be cached")
	})

	t.Run("ObjectAPIProbesFirst", func(t *testing.T) {
		authz := repoauthz.NewAuthorizer(nil, 0)
		server, root, probes := mountGomodObjectAPI(t, config, authz, authz)
		// Seeded directly so the object API is the first path to authorize this
		// repository, which a request through the module proxy would otherwise be.
		object := gomodobject.New(modName, true)
		writeObject(t, root.Namespace(gomodobject.Namespace), object.Key, object.Headers(), modContent)

		status, body := get(t, server, objectPath(gomodobject.Namespace, modName), basic(clientUser, clientToken))
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, modContent, string(body))
		anonymous, credentialed := probes.counts()
		assert.Equal(t, 1, anonymous)
		assert.Equal(t, 1, credentialed)

		status, body = get(t, server, "/gomod/"+modName, basic(clientUser, clientToken))
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, modContent, string(body))
		anonymous, credentialed = probes.counts()
		assert.Equal(t, 1, anonymous, "the module proxy re-probed a repository the object API had already resolved")
		assert.Equal(t, 2, credentialed, "the per-client decision must not be cached")
	})

	// The counts above only mean something if separate Authorizers would produce
	// different ones, which is exactly what cachewd used to wire up.
	t.Run("SeparateAuthorizersProbeOncePerEntryPoint", func(t *testing.T) {
		server, _, probes := mountGomodObjectAPI(t, config,
			repoauthz.NewAuthorizer(nil, 0), repoauthz.NewAuthorizer(nil, 0))

		status, _ := get(t, server, "/gomod/"+zipName, basic(clientUser, clientToken))
		assert.Equal(t, http.StatusOK, status)
		status, _ = get(t, server, objectPath(gomodobject.Namespace, zipName), basic(clientUser, clientToken))
		assert.Equal(t, http.StatusOK, status)

		anonymous, _ := probes.counts()
		assert.Equal(t, 2, anonymous)
	})

	// A denial learned through one entry point must be reused as a denial by the
	// other, rather than the second path treating the repository as public.
	t.Run("DenialsAreSharedToo", func(t *testing.T) {
		authz := repoauthz.NewAuthorizer(nil, 0)
		server, _, probes := mountGomodObjectAPI(t, config, authz, authz)

		status, _ := get(t, server, "/gomod/"+zipName, "")
		assert.Equal(t, http.StatusUnauthorized, status)
		anonymous, credentialed := probes.counts()
		assert.Equal(t, 1, anonymous)
		assert.Equal(t, 0, credentialed)

		status, body := get(t, server, objectPath(gomodobject.Namespace, zipName), "")
		assert.Equal(t, http.StatusNotFound, status)
		assert.False(t, bytes.Contains(body, []byte("private-source")))
		anonymous, _ = probes.counts()
		assert.Equal(t, 1, anonymous)
	})
}
