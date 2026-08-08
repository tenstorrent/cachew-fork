package strategy_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/githubapp"
	"github.com/tenstorrent/internal-cachew/internal/gitobject"
	"github.com/tenstorrent/internal-cachew/internal/gomodobject"
	"github.com/tenstorrent/internal-cachew/internal/jobscheduler"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/repoauthz/authztest"
	"github.com/tenstorrent/internal-cachew/internal/strategy"
	"github.com/tenstorrent/internal-cachew/internal/strategy/git"
)

const (
	privateUpstream = "https://github.com/acme/private-repo"
	publicUpstream  = "https://github.com/acme/public-repo"
	goodCredentials = "Bearer good-token"
)

// forgedKey is an attacker-chosen key, i.e. one that is not the key its
// repository metadata implies.
var forgedKey = cache.NewKey("attacker-chosen") //nolint:gochecknoglobals

// upstreamRedirectTransport routes the authorization probes apiv1 makes against
// https://github.com/... at a local fake upstream, so no test touches the real
// internet.
type upstreamRedirectTransport struct {
	baseURL string
}

func (t upstreamRedirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	target, err := url.Parse(t.baseURL)
	if err != nil {
		return nil, err
	}
	routed := req.Clone(req.Context())
	routed.URL.Scheme = target.Scheme
	routed.URL.Host = target.Host
	routed.Host = target.Host
	return http.DefaultTransport.RoundTrip(routed)
}

// newFakeUpstream serves smart-HTTP ref advertisements for one public and one
// private repository, the private one only to goodCredentials.
func newFakeUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/acme/public-repo.git/info/refs", func(w http.ResponseWriter, _ *http.Request) {
		authztest.WriteAdvertisement(w)
	})
	mux.HandleFunc("/acme/private-repo.git/info/refs", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != goodCredentials {
			w.Header().Set("WWW-Authenticate", `Basic realm="upstream"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		authztest.WriteAdvertisement(w)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func seedObject(ctx context.Context, t *testing.T, c cache.Cache, key cache.Key, headers http.Header, body string) {
	t.Helper()
	cw, err := c.Create(ctx, key, headers, time.Hour)
	assert.NoError(t, err)
	_, err = io.WriteString(cw, body)
	assert.NoError(t, err)
	assert.NoError(t, cw.Close())
}

// newGitAuthzAPI mounts apiv1 exactly as the strategy registry does — on a
// namespaced view of the same root cache the git strategy writes through — and
// seeds that root with the objects the git strategy would have produced.
func newGitAuthzAPI(t *testing.T, config strategy.APIV1Config) (http.Handler, context.Context) {
	t.Helper()
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
	root := newSeededGitCache(ctx, t)

	mux := http.NewServeMux()
	api, err := strategy.NewAPIV1(ctx, config, root.Namespace("apiv1"), mux, nil, nil)
	assert.NoError(t, err)
	api.SetAuthzHTTPClient(&http.Client{Transport: upstreamRedirectTransport{baseURL: newFakeUpstream(t).URL}})
	return mux, ctx
}

func newSeededGitCache(ctx context.Context, t *testing.T) cache.Cache {
	t.Helper()
	root, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { root.Close() })

	gitCache := root.Namespace(gitobject.Namespace)
	for _, object := range []gitobject.Object{
		gitobject.Snapshot(privateUpstream),
		gitobject.MirrorSnapshot(privateUpstream),
		gitobject.LFSSnapshot(privateUpstream),
		gitobject.Bundle(privateUpstream, strings.Repeat("a", 40)),
		gitobject.Snapshot(publicUpstream),
	} {
		seedObject(ctx, t, gitCache, object.Key, object.Headers(), "contents of "+object.Upstream)
	}

	// An object written before repository metadata existed, and one at an
	// attacker-chosen key whose metadata claims the public repository.
	seedObject(ctx, t, gitCache, cache.NewKey("legacy.snapshot"), http.Header{}, "legacy contents")
	forged := gitobject.Snapshot(privateUpstream)
	forged.Upstream = publicUpstream
	seedObject(ctx, t, gitCache, forgedKey, forged.Headers(), "relabelled contents")

	// A different strategy's namespace, to pin that the git gate is scoped.
	seedObject(ctx, t, root.Namespace("hermit"), cache.NewKey("unrelated"), http.Header{}, "unrelated")

	return root
}

// TestGitNamespaceObjectAuthorization pins the bypass this closes: apiv1 hands
// out whichever namespace the caller names and cache namespacing replaces
// rather than nests, so /api/v1/object/git/<key> reached the git strategy's
// private-repository snapshots with no authorization check at all. Keys are an
// unsalted hash of the upstream URL, so they are computable offline.
func TestGitNamespaceObjectAuthorization(t *testing.T) {
	handler, ctx := newGitAuthzAPI(t, strategy.APIV1Config{})

	tests := []struct {
		name       string
		method     string
		namespace  string
		key        cache.Key
		pathSuffix string
		authHeader string
		wantStatus int
		wantBody   string
	}{
		// Denials report absence: keys are computable offline, so distinguishing
		// "you may not read this" from "not here" would tell an anonymous client
		// which private repositories this deployment mirrors.
		{name: "AnonymousPrivateSnapshotReportedAbsent", method: http.MethodGet, namespace: "git",
			key: gitobject.Snapshot(privateUpstream).Key, wantStatus: http.StatusNotFound},
		{name: "RejectedCredentialsReportedAbsent", method: http.MethodGet, namespace: "git",
			key: gitobject.Snapshot(privateUpstream).Key, authHeader: "Bearer wrong", wantStatus: http.StatusNotFound},
		{name: "AcceptedCredentialsServed", method: http.MethodGet, namespace: "git",
			key: gitobject.Snapshot(privateUpstream).Key, authHeader: goodCredentials,
			wantStatus: http.StatusOK, wantBody: "contents of " + privateUpstream},
		{name: "PublicRepositoryServedAnonymously", method: http.MethodGet, namespace: "git",
			key:        gitobject.Snapshot(publicUpstream).Key,
			wantStatus: http.StatusOK, wantBody: "contents of " + publicUpstream},
		{name: "AnonymousMirrorSnapshotReportedAbsent", method: http.MethodGet, namespace: "git",
			key: gitobject.MirrorSnapshot(privateUpstream).Key, wantStatus: http.StatusNotFound},
		{name: "AnonymousLFSSnapshotReportedAbsent", method: http.MethodGet, namespace: "git",
			key: gitobject.LFSSnapshot(privateUpstream).Key, wantStatus: http.StatusNotFound},
		{name: "AnonymousBundleReportedAbsent", method: http.MethodGet, namespace: "git",
			key: gitobject.Bundle(privateUpstream, strings.Repeat("a", 40)).Key, wantStatus: http.StatusNotFound},
		{name: "AnonymousHeadReportedAbsent", method: http.MethodHead, namespace: "git",
			key: gitobject.Snapshot(privateUpstream).Key, wantStatus: http.StatusNotFound},
		{name: "InvalidateRefused", method: http.MethodPost, namespace: "git", pathSuffix: "/invalidate",
			key: gitobject.Snapshot(publicUpstream).Key, authHeader: goodCredentials, wantStatus: http.StatusForbidden},
		{name: "UnattributableObjectNotFound", method: http.MethodGet, namespace: "git",
			key: cache.NewKey("legacy.snapshot"), authHeader: goodCredentials, wantStatus: http.StatusNotFound},
		// Relabelling cannot launder a git object: keys are derived from the
		// upstream URL, so a claim of a different repository no longer
		// reproduces the key the object is stored at.
		{name: "ForgedRepositoryMetadataNotFound", method: http.MethodGet, namespace: "git",
			key: forgedKey, authHeader: goodCredentials, wantStatus: http.StatusNotFound},
		{name: "MissingObjectNotFound", method: http.MethodGet, namespace: "git",
			key: cache.NewKey("never.written"), authHeader: goodCredentials, wantStatus: http.StatusNotFound},
		{name: "MutationRefused", method: http.MethodPost, namespace: "git",
			key: gitobject.Snapshot(publicUpstream).Key, authHeader: goodCredentials, wantStatus: http.StatusForbidden},
		{name: "DeleteRefused", method: http.MethodDelete, namespace: "git",
			key: gitobject.Snapshot(publicUpstream).Key, authHeader: goodCredentials, wantStatus: http.StatusForbidden},
		// Namespace names may contain uppercase, and a case-insensitive filesystem
		// collates them to one directory of objects while their metadata is keyed
		// byte-exactly. A differently cased spelling must therefore not reach the
		// git namespace's files with its controls skipped: reads find no metadata
		// and fail closed, and mutations of the objects every consumer of a
		// repository restores from stay refused.
		{name: "CaseVariantReadNotFound", method: http.MethodGet, namespace: "GIT",
			key: gitobject.Snapshot(privateUpstream).Key, authHeader: goodCredentials, wantStatus: http.StatusNotFound},
		{name: "CaseVariantMutationRefused", method: http.MethodPost, namespace: "GIT",
			key: gitobject.Snapshot(publicUpstream).Key, authHeader: goodCredentials, wantStatus: http.StatusForbidden},
		{name: "CaseVariantDeleteRefused", method: http.MethodDelete, namespace: "Git",
			key: gitobject.Snapshot(publicUpstream).Key, authHeader: goodCredentials, wantStatus: http.StatusForbidden},
		{name: "CaseVariantInvalidateRefused", method: http.MethodPost, namespace: "gIt", pathSuffix: "/invalidate",
			key: gitobject.Snapshot(publicUpstream).Key, authHeader: goodCredentials, wantStatus: http.StatusForbidden},
		{name: "OtherNamespacesUnaffected", method: http.MethodGet, namespace: "hermit",
			key: cache.NewKey("unrelated"), wantStatus: http.StatusOK, wantBody: "unrelated"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := "/api/v1/object/" + test.namespace + "/" + test.key.String() + test.pathSuffix
			req := httptest.NewRequest(test.method, path, nil)
			if test.authHeader != "" {
				req.Header.Set("Authorization", test.authHeader)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req.WithContext(ctx))

			assert.Equal(t, test.wantStatus, w.Code, "body: %q", w.Body.String())
			if test.wantBody != "" {
				assert.Equal(t, test.wantBody, w.Body.String())
			} else {
				assert.NotContains(t, w.Body.String(), "contents of")
			}
			// A challenge would single out the keys that resolve just as a 401 does.
			assert.Equal(t, "", w.Header().Get("WWW-Authenticate"))
		})
	}
}

func requestObject(
	ctx context.Context, t *testing.T, handler http.Handler, method string, key cache.Key, authHeader string,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "/api/v1/object/git/"+key.String(), nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req.WithContext(ctx))
	return w
}

// TestDeniedObjectReadsLookExactlyLikeAbsentOnes pins that the object API is not
// an oracle for what a deployment mirrors. Keys are an unsalted hash of the
// upstream URL, so a client can compute the key of a private repository it only
// guessed the name of; if a refused read answered differently from a read of a
// key that was never written, that guess would be confirmed by the difference.
func TestDeniedObjectReadsLookExactlyLikeAbsentOnes(t *testing.T) {
	handler, ctx := newGitAuthzAPI(t, strategy.APIV1Config{})

	absent := requestObject(ctx, t, handler, http.MethodGet, cache.NewKey("never.written"), "")
	tests := []struct {
		name       string
		key        cache.Key
		authHeader string
	}{
		{"AnonymousPrivateSnapshot", gitobject.Snapshot(privateUpstream).Key, ""},
		{"AnonymousMirrorSnapshot", gitobject.MirrorSnapshot(privateUpstream).Key, ""},
		{"AnonymousLFSSnapshot", gitobject.LFSSnapshot(privateUpstream).Key, ""},
		{"AnonymousBundle", gitobject.Bundle(privateUpstream, strings.Repeat("a", 40)).Key, ""},
		{"RejectedCredentials", gitobject.Snapshot(privateUpstream).Key, "Bearer wrong"},
		{"UnattributableObject", cache.NewKey("legacy.snapshot"), ""},
		{"ForgedRepositoryMetadata", forgedKey, goodCredentials},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			denied := requestObject(ctx, t, handler, http.MethodGet, test.key, test.authHeader)

			assert.Equal(t, absent.Code, denied.Code)
			assert.Equal(t, absent.Body.String(), denied.Body.String())
			assert.Equal(t, absent.Header(), denied.Header())
		})
	}
}

// TestGitNamespaceObjectAuthorizationDisabled covers the escape hatch for
// deployments that knowingly mirror only public repositories: with
// disable-git-object-authz set, reads that carry no credentials at all are
// served, while mutations of the namespace stay refused.
func TestGitNamespaceObjectAuthorizationDisabled(t *testing.T) {
	handler, ctx := newGitAuthzAPI(t, strategy.APIV1Config{DisableGitObjectAuthz: true})

	tests := []struct {
		name       string
		method     string
		key        cache.Key
		wantStatus int
		wantBody   string
	}{
		{name: "AnonymousPrivateSnapshotServed", method: http.MethodGet,
			key: gitobject.Snapshot(privateUpstream).Key, wantStatus: http.StatusOK, wantBody: "contents of " + privateUpstream},
		{name: "AnonymousMirrorSnapshotServed", method: http.MethodGet,
			key:        gitobject.MirrorSnapshot(privateUpstream).Key,
			wantStatus: http.StatusOK, wantBody: "contents of " + privateUpstream},
		{name: "AnonymousLFSSnapshotServed", method: http.MethodGet,
			key:        gitobject.LFSSnapshot(privateUpstream).Key,
			wantStatus: http.StatusOK, wantBody: "contents of " + privateUpstream},
		{name: "AnonymousBundleServed", method: http.MethodGet,
			key:        gitobject.Bundle(privateUpstream, strings.Repeat("a", 40)).Key,
			wantStatus: http.StatusOK, wantBody: "contents of " + privateUpstream},
		{name: "AnonymousHeadServed", method: http.MethodHead,
			key: gitobject.Snapshot(privateUpstream).Key, wantStatus: http.StatusOK},
		{name: "UnattributableObjectServed", method: http.MethodGet,
			key: cache.NewKey("legacy.snapshot"), wantStatus: http.StatusOK, wantBody: "legacy contents"},
		{name: "PublicRepositoryStillServed", method: http.MethodGet,
			key: gitobject.Snapshot(publicUpstream).Key, wantStatus: http.StatusOK, wantBody: "contents of " + publicUpstream},
		// The flag disables authorization of reads only: nothing outside the git
		// strategy legitimately writes these objects.
		{name: "MutationStillRefused", method: http.MethodPost,
			key: gitobject.Snapshot(publicUpstream).Key, wantStatus: http.StatusForbidden},
		{name: "DeleteStillRefused", method: http.MethodDelete,
			key: gitobject.Snapshot(publicUpstream).Key, wantStatus: http.StatusForbidden},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := requestObject(ctx, t, handler, test.method, test.key, "")
			assert.Equal(t, test.wantStatus, w.Code, "body: %q", w.Body.String())
			if test.wantBody != "" {
				assert.Equal(t, test.wantBody, w.Body.String())
			}
		})
	}
}

// TestGitObjectAuthzIndependentOfGitStrategyFlag pins that the two escape
// hatches stay two knobs. The git strategy's disable-upstream-authz opens its
// own /git/ routes only; the object API's gate is governed solely by
// disable-git-object-authz, so a deployment that opens one entry point does not
// silently open the other. Both strategies are built here the way config.Load
// builds them — one mux, one root cache — so a future change that plumbed one
// config into the other would fail this test.
func TestGitObjectAuthzIndependentOfGitStrategyFlag(t *testing.T) {
	for _, bin := range []string{"git", "git-lfs"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not found in PATH", bin)
		}
	}

	tests := []struct {
		name       string
		gitConfig  git.Config
		apiConfig  strategy.APIV1Config
		wantStatus int
		// wantGitRouteChallenged asserts the git strategy's own gate, which must
		// stay closed unless its own flag is set. It is only meaningful while
		// disable-upstream-authz is unset; once that is set the route is open by
		// design and the outcome depends on mirror state.
		wantGitRouteChallenged bool
	}{
		{name: "BothDefaultBothGatesEnforced",
			wantStatus: http.StatusNotFound, wantGitRouteChallenged: true},
		{name: "GitFlagAloneDoesNotOpenObjectGate",
			gitConfig: git.Config{DisableUpstreamAuthz: true}, wantStatus: http.StatusNotFound},
		{name: "ObjectFlagAloneDoesNotOpenGitRoute",
			apiConfig:  strategy.APIV1Config{DisableGitObjectAuthz: true},
			wantStatus: http.StatusOK, wantGitRouteChallenged: true},
		{name: "BothFlagsOpenObjectGate",
			gitConfig: git.Config{DisableUpstreamAuthz: true},
			apiConfig: strategy.APIV1Config{DisableGitObjectAuthz: true}, wantStatus: http.StatusOK},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
			root := newSeededGitCache(ctx, t)
			mux := http.NewServeMux()

			scheduler, err := jobscheduler.New(logging.ContextWithLogger(t.Context(), logging.FromContext(ctx)), jobscheduler.Config{})
			assert.NoError(t, err)
			t.Cleanup(scheduler.Wait)
			clones := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: t.TempDir(), FetchInterval: 24 * time.Hour}, nil)
			gitStrategy, err := git.New(ctx, test.gitConfig,
				func() (*jobscheduler.RootScheduler, error) { return scheduler, nil },
				root.Namespace(gitobject.Namespace), mux, clones,
				func() (*githubapp.TokenManager, error) { return nil, nil }, nil) //nolint:nilnil
			assert.NoError(t, err)

			api, err := strategy.NewAPIV1(ctx, test.apiConfig, root.Namespace("apiv1"), mux, nil, nil)
			assert.NoError(t, err)
			transport := upstreamRedirectTransport{baseURL: newFakeUpstream(t).URL}
			gitStrategy.SetHTTPTransport(transport)
			api.SetAuthzHTTPClient(&http.Client{Transport: transport})

			w := requestObject(ctx, t, mux, http.MethodGet, gitobject.Snapshot(privateUpstream).Key, "")
			assert.Equal(t, test.wantStatus, w.Code, "body: %q", w.Body.String())
			if test.wantStatus == http.StatusOK {
				assert.Equal(t, "contents of "+privateUpstream, w.Body.String())
			} else {
				assert.NotContains(t, w.Body.String(), "contents of")
			}

			if !test.wantGitRouteChallenged {
				return
			}
			// A host that cannot resolve, so a denial cannot be mistaken for a
			// failed fetch: the probes are redirected at the fake upstream, the
			// git subprocess is not.
			gitRequest := httptest.NewRequestWithContext(ctx, http.MethodGet,
				"/git/example.invalid/acme/private-repo/snapshot.tar.zst", nil)
			gitRecorder := httptest.NewRecorder()
			mux.ServeHTTP(gitRecorder, gitRequest)
			assert.Equal(t, http.StatusUnauthorized, gitRecorder.Code, "body: %q", gitRecorder.Body.String())
		})
	}
}

// TestPutObjectDoesNotPersistCredentials pins that a caller's Authorization
// header is not stored as object metadata, which ServeCacheHit would otherwise
// replay to whoever reads the object next, and that the metadata the git and
// gomod strategies authorize reads against cannot be set by a writer.
func TestPutObjectDoesNotPersistCredentials(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
	root, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { root.Close() })

	mux := http.NewServeMux()
	_, err = strategy.NewAPIV1(ctx, strategy.APIV1Config{}, root.Namespace("apiv1"), mux, nil, nil)
	assert.NoError(t, err)

	key := cache.NewKey("credential-leak")
	put := httptest.NewRequest(http.MethodPost, "/api/v1/object/default/"+key.String(), strings.NewReader("body"))
	put.Header.Set("Authorization", goodCredentials)
	put.Header.Set("Cookie", "session=secret")
	put.Header.Set(gitobject.UpstreamHeader, privateUpstream)
	put.Header.Set(gomodobject.NameHeader, "example.com/mod/@v/v1.0.0.zip")
	put.Header.Set(gomodobject.PrivateHeader, "false")
	putRecorder := httptest.NewRecorder()
	mux.ServeHTTP(putRecorder, put.WithContext(ctx))
	assert.Equal(t, http.StatusOK, putRecorder.Code, "body: %q", putRecorder.Body.String())

	get := httptest.NewRequest(http.MethodGet, "/api/v1/object/default/"+key.String(), nil)
	getRecorder := httptest.NewRecorder()
	mux.ServeHTTP(getRecorder, get.WithContext(ctx))
	assert.Equal(t, http.StatusOK, getRecorder.Code)
	assert.Equal(t, "", getRecorder.Header().Get("Authorization"))
	assert.Equal(t, "", getRecorder.Header().Get("Cookie"))
	assert.Equal(t, "", getRecorder.Header().Get(gitobject.UpstreamHeader))
	assert.Equal(t, "", getRecorder.Header().Get(gomodobject.NameHeader))
	assert.Equal(t, "", getRecorder.Header().Get(gomodobject.PrivateHeader))
}
