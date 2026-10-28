package git //nolint:testpackage // white-box testing required for unexported authorization helpers

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/jobscheduler"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/repoauthz"
	"github.com/tenstorrent/internal-cachew/internal/repoauthz/authztest"
)

// stubTransport stands in for an upstream git host. A 200 with no body override
// answers as one serving the repository, so probes see the advertisement a real
// host sends; contentType and body let a test serve some other 200 instead.
type stubTransport struct {
	status      int
	contentType string
	body        string
	err         error
	calls       int
}

func (s *stubTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	rec := httptest.NewRecorder()
	switch {
	case s.body != "" || s.contentType != "":
		rec.Header().Set("Content-Type", s.contentType)
		rec.WriteHeader(s.status)
		_, _ = rec.WriteString(s.body) //nolint:errcheck // a recorder cannot fail
	case s.status == http.StatusOK:
		authztest.WriteAdvertisement(rec)
	default:
		rec.WriteHeader(s.status)
	}
	resp := rec.Result()
	resp.Request = r
	return resp, nil
}

func TestRepoPathForRequest(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		target   string
		path     string
		wantRepo string
		wantGate bool
	}{
		{name: "InfoRefs", method: http.MethodGet, target: "/git/github.com/org/repo/info/refs?service=git-upload-pack",
			path: "org/repo/info/refs", wantRepo: "org/repo", wantGate: true},
		{name: "InfoRefsDotGit", method: http.MethodGet, target: "/git/github.com/org/repo.git/info/refs?service=git-upload-pack",
			path: "org/repo.git/info/refs", wantRepo: "org/repo", wantGate: true},
		{name: "UploadPack", method: http.MethodPost, target: "/git/github.com/org/repo/git-upload-pack",
			path: "org/repo/git-upload-pack", wantRepo: "org/repo", wantGate: true},
		{name: "EnsureRefs", method: http.MethodPost, target: "/git/github.com/org/repo/ensure-refs",
			path: "org/repo/ensure-refs", wantRepo: "org/repo", wantGate: true},
		{name: "EnsureRefsDotGit", method: http.MethodPost, target: "/git/github.com/org/repo.git/ensure-refs",
			path: "org/repo.git/ensure-refs", wantRepo: "org/repo", wantGate: true},
		{name: "Snapshot", method: http.MethodGet, target: "/git/github.com/org/repo/snapshot.tar.zst",
			path: "org/repo/snapshot.tar.zst", wantRepo: "org/repo", wantGate: true},
		{name: "Bundle", method: http.MethodGet, target: "/git/github.com/org/repo/snapshot.bundle",
			path: "org/repo/snapshot.bundle", wantRepo: "org/repo", wantGate: true},
		{name: "LFSSnapshot", method: http.MethodGet, target: "/git/github.com/org/repo/lfs-snapshot.tar.zst",
			path: "org/repo/lfs-snapshot.tar.zst", wantRepo: "org/repo", wantGate: true},
		{name: "ReceivePackAdvertisement", method: http.MethodGet, target: "/git/github.com/org/repo/info/refs?service=git-receive-pack",
			path: "org/repo/info/refs"},
		// A mirror-serving path stays gated regardless of the service query
		// parameter. handleRequest dispatches on the path suffix before it
		// looks at ?service=, so excluding these here would de-gate a request
		// that still gets served from the mirror.
		{name: "EnsureRefsWithReceivePackQuery", method: http.MethodPost, target: "/git/github.com/org/repo/ensure-refs?service=git-receive-pack",
			path: "org/repo/ensure-refs", wantRepo: "org/repo", wantGate: true},
		{name: "SnapshotWithReceivePackQuery", method: http.MethodGet, target: "/git/github.com/org/repo/snapshot.tar.zst?service=git-receive-pack",
			path: "org/repo/snapshot.tar.zst", wantRepo: "org/repo", wantGate: true},
		{name: "BundleWithReceivePackQuery", method: http.MethodGet, target: "/git/github.com/org/repo/snapshot.bundle?service=git-receive-pack",
			path: "org/repo/snapshot.bundle", wantRepo: "org/repo", wantGate: true},
		{name: "LFSSnapshotWithReceivePackQuery", method: http.MethodGet, target: "/git/github.com/org/repo/lfs-snapshot.tar.zst?service=git-receive-pack",
			path: "org/repo/lfs-snapshot.tar.zst", wantRepo: "org/repo", wantGate: true},
		{name: "ReceivePack", method: http.MethodPost, target: "/git/github.com/org/repo/git-receive-pack",
			path: "org/repo/git-receive-pack"},
		{name: "ReceivePackDotGit", method: http.MethodPost, target: "/git/github.com/org/repo.git/git-receive-pack",
			path: "org/repo.git/git-receive-pack"},
		{name: "LFSBatch", method: http.MethodPost, target: "/git/github.com/org/repo/info/lfs/objects/batch",
			path: "org/repo/info/lfs/objects/batch"},
		{name: "LFSObject", method: http.MethodPut, target: "/git/github.com/org/repo/info/lfs/objects/abc123",
			path: "org/repo/info/lfs/objects/abc123"},
		{name: "LFSLock", method: http.MethodPost, target: "/git/github.com/org/repo.git/info/lfs/locks",
			path: "org/repo.git/info/lfs/locks"},
		{name: "Unknown", method: http.MethodGet, target: "/git/github.com/org/repo/something/else",
			path: "org/repo/something/else"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.target, nil)
			repoPath, gated := repoPathForRequest(r, tt.path)
			assert.Equal(t, tt.wantGate, gated)
			assert.Equal(t, tt.wantRepo, repoPath)
			// Substituting cachew's credentials and gating the client must
			// never diverge.
			assert.Equal(t, tt.wantGate, mayUseServiceCredentials(r, tt.path))
		})
	}
}

func TestDirectUpstreamCredentials(t *testing.T) {
	const token = "ghs-service-token"
	clientAuth := basicAuthHeader("client", "client-secret")
	serviceAuth := basicAuthHeader("x-access-token", token)

	tests := []struct {
		name           string
		method         string
		host           string
		path           string
		query          string
		clientAuth     string
		noServiceToken bool
		wantAuth       string
		wantOrg        string
	}{
		{name: "UploadPackAdvertisementUsesServiceCredentials", method: http.MethodGet, host: "github.com",
			path: "org/repo/info/refs", query: "?service=git-upload-pack", wantAuth: serviceAuth, wantOrg: "org"},
		{name: "UploadPackUsesServiceCredentialsOverClients", method: http.MethodPost, host: "github.com",
			path: "org/repo.git/git-upload-pack", clientAuth: clientAuth, wantAuth: serviceAuth, wantOrg: "org"},
		{name: "ReceivePackAdvertisementKeepsClientCredentials", method: http.MethodGet, host: "github.com",
			path: "org/repo/info/refs", query: "?service=git-receive-pack", clientAuth: clientAuth, wantAuth: clientAuth},
		{name: "AnonymousReceivePackAdvertisementStaysAnonymous", method: http.MethodGet, host: "github.com",
			path: "org/repo/info/refs", query: "?service=git-receive-pack"},
		{name: "ReceivePackKeepsClientCredentials", method: http.MethodPost, host: "github.com",
			path: "org/repo/git-receive-pack", clientAuth: clientAuth, wantAuth: clientAuth},
		{name: "AnonymousReceivePackStaysAnonymous", method: http.MethodPost, host: "github.com",
			path: "org/repo.git/git-receive-pack"},
		{name: "LFSBatchKeepsClientCredentials", method: http.MethodPost, host: "github.com",
			path: "org/repo/info/lfs/objects/batch", clientAuth: clientAuth, wantAuth: clientAuth},
		{name: "AnonymousLFSBatchStaysAnonymous", method: http.MethodPost, host: "github.com",
			path: "org/repo/info/lfs/objects/batch"},
		{name: "AnonymousLFSObjectUploadStaysAnonymous", method: http.MethodPut, host: "github.com",
			path: "org/repo/info/lfs/objects/abc123"},
		{name: "NonGitHubHostKeepsClientCredentials", method: http.MethodGet, host: "gitlab.example.com",
			path: "org/repo/info/refs", query: "?service=git-upload-pack", clientAuth: clientAuth, wantAuth: clientAuth},
		{name: "WithoutGitHubAppKeepsClientCredentials", method: http.MethodGet, host: "github.com",
			path: "org/repo/info/refs", query: "?service=git-upload-pack", clientAuth: clientAuth,
			noServiceToken: true, wantAuth: clientAuth},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
			r := httptest.NewRequestWithContext(ctx, tt.method, "/git/"+tt.host+"/"+tt.path+tt.query, nil)
			r.SetPathValue("host", tt.host)
			r.SetPathValue("path", tt.path)
			if tt.clientAuth != "" {
				r.Header.Set("Authorization", tt.clientAuth)
			}

			gotOrg := ""
			s := &Strategy{}
			if !tt.noServiceToken {
				s.serviceToken = func(_ context.Context, org string) (string, error) {
					gotOrg = org
					return token, nil
				}
			}

			s.directUpstream(r)

			assert.Equal(t, tt.wantAuth, r.Header.Get("Authorization"))
			assert.Equal(t, tt.wantOrg, gotOrg)
			assert.Equal(t, "https://"+tt.host+"/"+tt.path+tt.query, r.URL.String())
			assert.Equal(t, tt.host, r.Host)
		})
	}
}

func basicAuthHeader(user, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
}

func TestProactiveUpstreamContactAllowed(t *testing.T) {
	tests := []struct {
		name                 string
		status               int
		contentType          string
		body                 string
		disableUpstreamAuthz bool
		want                 bool
	}{
		{name: "PublicRepo", status: http.StatusOK, want: true},
		{name: "AuthRequired", status: http.StatusUnauthorized},
		{name: "NotFound", status: http.StatusNotFound},
		{name: "LoginPage", status: http.StatusOK, contentType: "text/html", body: "<html>sign in</html>"},
		{name: "EscapeHatch", status: http.StatusUnauthorized, disableUpstreamAuthz: true, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
			manager, err := gitclone.NewManager(ctx, gitclone.Config{MirrorRoot: t.TempDir()}, nil)
			assert.NoError(t, err)
			repo, err := manager.GetOrCreate(ctx, "https://example.invalid/org/repo")
			assert.NoError(t, err)

			transport := &stubTransport{status: tt.status, contentType: tt.contentType, body: tt.body}
			s := &Strategy{
				config:       Config{DisableUpstreamAuthz: tt.disableUpstreamAuthz},
				cloneManager: manager,
				authz:        repoauthz.NewAuthorizer(&http.Client{Transport: transport}, time.Minute),
				metrics:      newGitMetrics(ctx, Config{DisableUpstreamAuthz: tt.disableUpstreamAuthz}, logging.FromContext(ctx)),
			}

			assert.Equal(t, tt.want, s.proactiveUpstreamContactAllowed(ctx, repo))
			assert.Equal(t, tt.want, s.backgroundFetchAllowed(ctx, repo))
		})
	}
}

// TestPeriodicSnapshotJobsSkipAuthRequiredRepos pins the isolation the gate
// exists for across every periodic job that would reach upstream: an
// auth-required repository must not be contacted with cachew's own credentials
// with no authorized client request driving it. The LFS job is the sharp case,
// because "git lfs fetch" contacts the host from inside snapshot generation
// rather than through a separable pre-fetch.
func TestPeriodicSnapshotJobsSkipAuthRequiredRepos(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	manager, err := gitclone.NewManager(ctx, gitclone.Config{MirrorRoot: t.TempDir()}, nil)
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, "https://example.invalid/org/repo")
	assert.NoError(t, err)

	transport := &stubTransport{status: http.StatusUnauthorized}
	scheduler := &capturingScheduler{}
	s := &Strategy{
		config:       Config{SnapshotInterval: time.Hour},
		cloneManager: manager,
		authz:        repoauthz.NewAuthorizer(&http.Client{Transport: transport}, time.Minute),
		metrics:      newGitMetrics(ctx, Config{SnapshotInterval: time.Hour}, logging.FromContext(ctx)),
		scheduler:    scheduler,
	}

	s.scheduleSnapshotJobs(repo)

	// The mirror was never cloned, so a job that runs its body fails on the
	// missing clone. The base job does exactly that: its gate suppresses only
	// the pre-fetch, and generating the snapshot itself never leaves the
	// mirror. The LFS job has nothing it can do without upstream, so the gate
	// has to skip the whole job, which is what its clean return records.
	tests := []struct {
		job     string
		wantErr bool
	}{
		{job: snapshotJobBase, wantErr: true},
		{job: snapshotJobLFS},
	}
	for _, tt := range tests {
		run, ok := scheduler.jobs[tt.job+"-periodic"]
		assert.True(t, ok, "no periodic job registered for %s", tt.job)
		err := run(ctx)
		if tt.wantErr {
			assert.Error(t, err, "%s no longer reaches its mirror", tt.job)
			continue
		}
		assert.NoError(t, err, "%s reached upstream for an auth-required repository", tt.job)
	}
	assert.Equal(t, 1, transport.calls, "each job probed upstream separately")
}

// TestDeniedRequestSchedulesNoUpstreamWork pins why submitFetch needs no
// background-fetch gate of its own: authorizeServe runs ahead of every dispatch
// in handleRequest, so a request that fails it never reaches the code that
// could schedule a clone or a fetch. Adding the gate there instead would stop
// authorized clients refreshing the private repositories they can read.
func TestDeniedRequestSchedulesNoUpstreamWork(t *testing.T) {
	tests := []struct {
		name   string
		method string
		target string
		path   string
	}{
		{name: "InfoRefs", method: http.MethodGet,
			target: "/git/example.invalid/org/repo/info/refs?service=git-upload-pack", path: "org/repo/info/refs"},
		{name: "UploadPack", method: http.MethodPost,
			target: "/git/example.invalid/org/repo/git-upload-pack", path: "org/repo/git-upload-pack"},
		{name: "EnsureRefs", method: http.MethodPost,
			target: "/git/example.invalid/org/repo/ensure-refs", path: "org/repo/ensure-refs"},
		{name: "Snapshot", method: http.MethodGet,
			target: "/git/example.invalid/org/repo/snapshot.tar.zst", path: "org/repo/snapshot.tar.zst"},
		{name: "Bundle", method: http.MethodGet,
			target: "/git/example.invalid/org/repo/snapshot.bundle", path: "org/repo/snapshot.bundle"},
		{name: "LFSSnapshot", method: http.MethodGet,
			target: "/git/example.invalid/org/repo/lfs-snapshot.tar.zst", path: "org/repo/lfs-snapshot.tar.zst"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
			manager, err := gitclone.NewManager(ctx, gitclone.Config{MirrorRoot: t.TempDir()}, nil)
			assert.NoError(t, err)

			scheduler := &capturingScheduler{}
			s := &Strategy{
				cloneManager: manager,
				authz: repoauthz.NewAuthorizer(
					&http.Client{Transport: &stubTransport{status: http.StatusUnauthorized}}, time.Minute),
				metrics:   newGitMetrics(ctx, Config{}, logging.FromContext(ctx)),
				scheduler: scheduler,
			}

			r := httptest.NewRequest(tt.method, tt.target, nil).WithContext(ctx)
			r.SetPathValue("host", "example.invalid")
			r.SetPathValue("path", tt.path)
			w := httptest.NewRecorder()
			s.handleRequest(w, r)

			assert.Equal(t, http.StatusUnauthorized, w.Code)
			assert.Equal(t, 0, len(scheduler.jobs), "a denied request scheduled background work")
		})
	}
}

type capturingScheduler struct {
	jobs map[string]func(ctx context.Context) error
}

func (c *capturingScheduler) WithQueuePrefix(string) jobscheduler.Scheduler { return c }

// TrySubmit records jobs so authorization tests can detect unintended background work.
func (c *capturingScheduler) TrySubmit(queue, id string, _ int, run func(ctx context.Context) error) bool {
	c.Submit(queue, id, run)
	return true
}

func (c *capturingScheduler) Submit(_, id string, run func(ctx context.Context) error) {
	c.SubmitPeriodicJob("", id, 0, run)
}

func (c *capturingScheduler) SubmitPeriodicJob(_, id string, _ time.Duration, run func(ctx context.Context) error) {
	if c.jobs == nil {
		c.jobs = map[string]func(ctx context.Context) error{}
	}
	c.jobs[id] = run
}

func (c *capturingScheduler) SubmitPeriodicJobAfter(queue, id string, _, interval time.Duration, run func(ctx context.Context) error) {
	c.SubmitPeriodicJob(queue, id, interval, run)
}

func (c *capturingScheduler) CancelPeriodicJob(_, id string) {
	delete(c.jobs, id)
}

func (c *capturingScheduler) QueueActive(string) bool { return false }

func TestValidateRepoTarget(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		path    string
		wantErr bool
	}{
		{name: "Simple", host: "github.com", path: "org/repo/info/refs"},
		{name: "DotGitSuffix", host: "github.com", path: "org/repo.git/info/refs"},
		{name: "DeepPath", host: "github.com", path: "org/repo.git/info/lfs/objects/batch"},
		{name: "DottedRepoName", host: "github.com", path: "org/repo.name/info/refs"},
		{name: "PortInHost", host: "git.example.com:8443", path: "org/repo/info/refs"},
		{name: "SingleSegmentPath", host: "github.com", path: "snapshot.tar.zst"},

		{name: "EmptyHost", host: "", path: "org/repo/info/refs", wantErr: true},
		{name: "SeparatorInHost", host: "attacker.invalid/..", path: "org/repo/info/refs", wantErr: true},
		{name: "BackslashInHost", host: `attacker.invalid\..`, path: "org/repo/info/refs", wantErr: true},
		{name: "HostIsTraversal", host: "..", path: "org/repo/info/refs", wantErr: true},
		{name: "HostIsDot", host: ".", path: "org/repo/info/refs", wantErr: true},
		{name: "TraversalInPath", host: "github.com", path: "org/../other/repo/info/refs", wantErr: true},
		{name: "LeadingTraversalInPath", host: "github.com", path: "../org/repo/info/refs", wantErr: true},
		{name: "DotSegmentInPath", host: "github.com", path: "org/./repo/info/refs", wantErr: true},
		{name: "DoubleSlashInPath", host: "github.com", path: "org//repo/info/refs", wantErr: true},
		{name: "LeadingSlashInPath", host: "github.com", path: "/org/repo/info/refs", wantErr: true},
		{name: "TrailingSlashInPath", host: "github.com", path: "org/repo/info/refs/", wantErr: true},
		{name: "EmptyPath", host: "github.com", path: "", wantErr: true},
		{name: "NullByteInPath", host: "github.com", path: "org/repo\x00/info/refs", wantErr: true},
		{name: "NullByteInHost", host: "github.com\x00", path: "org/repo/info/refs", wantErr: true},
		{name: "NewlineInPath", host: "github.com", path: "org/repo\n/info/refs", wantErr: true},
		{name: "BackslashInPath", host: "github.com", path: `org\..\repo/info/refs`, wantErr: true},
		{name: "EncodedTraversalInPath", host: "github.com", path: "org/%2e%2e/%2e%2e/private/repo/info/refs", wantErr: true},
		{name: "EncodedSeparatorInPath", host: "github.com", path: "org/..%2f..%2fprivate/repo/info/refs", wantErr: true},
		{name: "FragmentInPath", host: "github.com", path: "org/repo#fragment/info/refs", wantErr: true},
		{name: "QueryInPath", host: "github.com", path: "org/repo?query=1/info/refs", wantErr: true},
		{name: "BareDotGitSegment", host: "github.com", path: ".git/info/refs", wantErr: true},
		{name: "BareDotGitSegmentInPath", host: "github.com", path: "org/.git/info/refs", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRepoTarget(tt.host, tt.path)
			assert.Equal(t, tt.wantErr, err != nil, "got %v", err)
		})
	}
}

func TestValidateCanonicalRepoPath(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		repoPath string
		wantErr  bool
	}{
		{name: "Simple", host: "github.com", repoPath: "org/repo"},
		{name: "DottedRepoName", host: "github.com", repoPath: "org/repo.name"},
		{name: "NestedPath", host: "gitlab.example.com", repoPath: "group/subgroup/repo"},
		{name: "PortInHost", host: "git.example.com:8443", repoPath: "org/repo"},
		{name: "SingleSegment", host: "github.com", repoPath: "repo"},

		{name: "ResidualDotGit", host: "github.com", repoPath: "org/repo.git", wantErr: true},
		{name: "BareDotGit", host: "github.com", repoPath: ".git", wantErr: true},
		{name: "EmptyRepoPath", host: "github.com", repoPath: "", wantErr: true},
		{name: "TrailingSlash", host: "github.com", repoPath: "org/repo/", wantErr: true},
		{name: "Traversal", host: "github.com", repoPath: "org/../other", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCanonicalRepoPath(tt.host, tt.repoPath)
			assert.Equal(t, tt.wantErr, err != nil, "got %v", err)
		})
	}
}

func TestAuthorizeServeDeniesNonCanonicalRepoPath(t *testing.T) {
	tests := []struct {
		name                 string
		path                 string
		disableUpstreamAuthz bool
	}{
		{name: "EnsureRefs", path: "org/repo.git.git/ensure-refs"},
		{name: "Snapshot", path: "org/repo.git.git/snapshot.tar.zst"},
		{name: "InfoRefs", path: "org/repo.git.git/info/refs"},
		{name: "UpstreamAuthzDisabled", path: "org/repo.git.git/ensure-refs", disableUpstreamAuthz: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
			s := &Strategy{metrics: newGitMetrics(ctx, Config{DisableUpstreamAuthz: tt.disableUpstreamAuthz}, logging.FromContext(ctx)), config: Config{DisableUpstreamAuthz: tt.disableUpstreamAuthz}}

			r := httptest.NewRequest(http.MethodGet, "/git/github.com/"+tt.path, nil).WithContext(ctx)
			w := httptest.NewRecorder()

			assert.False(t, s.authorizeServe(w, r, "github.com", tt.path))
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}
