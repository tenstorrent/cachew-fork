//go:build integration

package git_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/githubapp"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/strategy/git"
)

// runGit runs a git command, failing the test on error.
func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Test",
		"GIT_AUTHOR_EMAIL=test@test.com",
		"GIT_COMMITTER_NAME=Test",
		"GIT_COMMITTER_EMAIL=test@test.com",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// gitRevParse returns the full SHA for the given ref in dir.
func gitRevParse(t *testing.T, dir, ref string) string {
	t.Helper()
	cmd := exec.Command("git", "-C", dir, "rev-parse", ref)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git rev-parse %s: %v\n%s", ref, err, out)
	}
	return strings.TrimSpace(string(out))
}

// pktLine encodes s as a git pkt-line.
func pktLine(s string) string {
	return fmt.Sprintf("%04x%s", len(s)+4, s)
}

// upstreamRedirectTransport rewrites all outbound requests to targetBaseURL,
// preserving the path and query, and counts each rewrite.
type upstreamRedirectTransport struct {
	targetBaseURL string
	inner         http.RoundTripper
	hits          *atomic.Int32
	// hostRoutes optionally redirects specific upstream hosts elsewhere,
	// preserving the distinction between hosts that targetBaseURL erases.
	hostRoutes map[string]string
}

func (t *upstreamRedirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.hits.Add(1)
	req = req.Clone(req.Context())
	target := t.targetBaseURL
	if routed, ok := t.hostRoutes[req.URL.Host]; ok {
		target = routed
	}
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(target, "http://")
	req.Host = req.URL.Host
	return t.inner.RoundTrip(req)
}

// testServerWithLogging creates an httptest.Server that injects a logger into the request context.
func testServerWithLogging(ctx context.Context, handler http.Handler) *httptest.Server {
	wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger := logging.FromContext(ctx).With("request", fmt.Sprintf("%s %s", r.Method, r.RequestURI))
		r = r.WithContext(logging.ContextWithLogger(r.Context(), logger))
		logger.Debug("Request received")
		handler.ServeHTTP(w, r)
	})
	return httptest.NewServer(wrapper)
}

// TestIntegrationGitCloneViaProxy tests cloning a repository through the git proxy.
// This test requires git to be installed and network access.
func TestIntegrationGitCloneViaProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	if os.Getenv("CI") != "" {
		t.Skip("skipping: requires real network access to github.com, unavailable from CI runners")
	}

	// Check if git is available
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	clonesDir := filepath.Join(tmpDir, "clones")
	workDir := filepath.Join(tmpDir, "work")

	err := os.MkdirAll(workDir, 0o750)
	assert.NoError(t, err)

	// Create the git strategy
	gc := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    clonesDir,
		FetchInterval: 15,
	}, nil)
	mux := http.NewServeMux()
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	_, err = git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, gc, func() (*githubapp.TokenManager, error) { return nil, nil }, nil) //nolint:nilnil
	assert.NoError(t, err)

	// Start a test server with logging middleware
	server := testServerWithLogging(ctx, mux)
	defer server.Close()

	// Clone a small public repository through the proxy
	// Using a small test repo to keep the test fast
	repoURL := fmt.Sprintf("%s/git/github.com/octocat/Hello-World", server.URL)

	// First clone - should forward to upstream and start background clone
	cmd := exec.Command("git", "clone", repoURL, filepath.Join(workDir, "repo1"))
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("git clone output: %s", output)
	}
	assert.NoError(t, err)

	// Verify the clone worked
	readmePath := filepath.Join(workDir, "repo1", "README")
	_, err = os.Stat(readmePath)
	assert.NoError(t, err)

	// Wait a bit for background clone to complete
	time.Sleep(2 * time.Second)

	// Second clone - should be served from local cache
	cmd = exec.Command("git", "clone", repoURL, filepath.Join(workDir, "repo2"))
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Logf("git clone output: %s", output)
	}
	assert.NoError(t, err)

	// Verify the second clone worked
	readmePath2 := filepath.Join(workDir, "repo2", "README")
	_, err = os.Stat(readmePath2)
	assert.NoError(t, err)

	// Verify the clone was created
	clonePath := filepath.Join(clonesDir, "github.com", "octocat", "Hello-World")
	info, err := os.Stat(clonePath)
	assert.NoError(t, err)
	assert.True(t, info.IsDir())

	// Verify it has a HEAD file (bare mirror clone)
	headFile := filepath.Join(clonePath, "HEAD")
	_, err = os.Stat(headFile)
	assert.NoError(t, err)
}

// TestIntegrationGitFetchViaProxy tests fetching updates through the proxy.
func TestIntegrationGitFetchViaProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	if os.Getenv("CI") != "" {
		t.Skip("skipping: requires real network access to github.com, unavailable from CI runners")
	}

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	clonesDir := filepath.Join(tmpDir, "clones")
	workDir := filepath.Join(tmpDir, "work")

	err := os.MkdirAll(workDir, 0o750)
	assert.NoError(t, err)

	gc := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    clonesDir,
		FetchInterval: 15,
	}, nil)

	mux := http.NewServeMux()
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	_, err = git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, gc, func() (*githubapp.TokenManager, error) { return nil, nil }, nil) //nolint:nilnil
	assert.NoError(t, err)

	server := testServerWithLogging(ctx, mux)
	defer server.Close()

	repoURL := fmt.Sprintf("%s/git/github.com/octocat/Hello-World", server.URL)

	// Clone first
	cmd := exec.Command("git", "clone", repoURL, filepath.Join(workDir, "repo"))
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("git clone output: %s", output)
	}
	assert.NoError(t, err)

	// Wait for background clone
	time.Sleep(2 * time.Second)

	// Fetch should work
	cmd = exec.Command("git", "-C", filepath.Join(workDir, "repo"), "fetch", "origin")
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Logf("git fetch output: %s", output)
	}
	assert.NoError(t, err)
}

// TestIntegrationPushForwardsToUpstream verifies that push operations are forwarded.
// This test uses a local git server to verify push forwarding.
func TestIntegrationPushForwardsToUpstream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	clonesDir := filepath.Join(tmpDir, "clones")
	upstreamDir := filepath.Join(tmpDir, "upstream")
	workDir := filepath.Join(tmpDir, "work")

	// Create a bare upstream repo
	err := os.MkdirAll(upstreamDir, 0o750)
	assert.NoError(t, err)

	cmd := exec.Command("git", "init", "--bare", filepath.Join(upstreamDir, "repo.git"))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("git init output: %s", output)
	}
	assert.NoError(t, err)

	// Track if we received a push request
	pushReceived := false

	// Create a mock upstream that serves git protocol
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Upstream received: %s %s", r.Method, r.URL.Path)

		if r.URL.Query().Get("service") == "git-receive-pack" || r.URL.Path == "/test/repo/git-receive-pack" {
			pushReceived = true
		}

		// For this test, just acknowledge we received the request
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	defer upstreamServer.Close()

	mux := http.NewServeMux()
	gc := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    clonesDir,
		FetchInterval: 15,
	}, nil)
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	_, err = git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, gc, func() (*githubapp.TokenManager, error) { return nil, nil }, nil) //nolint:nilnil
	assert.NoError(t, err)

	server := testServerWithLogging(ctx, mux)
	defer server.Close()

	// Create a local repo to push from
	err = os.MkdirAll(workDir, 0o750)
	assert.NoError(t, err)

	repoPath := filepath.Join(workDir, "repo")
	cmd = exec.Command("git", "init", repoPath)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Logf("git init output: %s", output)
	}
	assert.NoError(t, err)

	// Configure git
	cmd = exec.Command("git", "-C", repoPath, "config", "user.email", "test@test.com")
	_, _ = cmd.CombinedOutput()
	cmd = exec.Command("git", "-C", repoPath, "config", "user.name", "Test")
	_, _ = cmd.CombinedOutput()

	// Create a commit
	testFile := filepath.Join(repoPath, "test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0o644)
	assert.NoError(t, err)

	cmd = exec.Command("git", "-C", repoPath, "add", "test.txt")
	_, _ = cmd.CombinedOutput()

	cmd = exec.Command("git", "-C", repoPath, "commit", "-m", "test commit")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Logf("git commit output: %s", output)
	}
	assert.NoError(t, err)

	// Try to push through the proxy - this will fail but should forward to upstream
	// We're just verifying the forwarding logic, not actual push success
	proxyURL := fmt.Sprintf("%s/git/localhost/test/repo", server.URL)
	cmd = exec.Command("git", "-C", repoPath, "push", proxyURL, "HEAD:main")
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	_, _ = cmd.CombinedOutput()

	// Note: The push will likely fail because our mock upstream doesn't implement
	// the full git protocol, but the important thing is verifying the proxy
	// attempted to forward it (which we can verify through logs or the pushReceived flag
	// if we had wired up the server properly)
	t.Logf("Push forwarding test completed, pushReceived=%v", pushReceived)
}

// countingTransport wraps an http.RoundTripper to count outbound requests by URL path pattern.
type countingTransport struct {
	inner   http.RoundTripper
	counter *atomic.Int32
	pattern string
}

func (ct *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.Contains(req.URL.Path, ct.pattern) {
		ct.counter.Add(1)
	}
	return ct.inner.RoundTrip(req)
}

// TestIntegrationSpoolReusesDuringClone clones github.com/git/git through the proxy,
// waits 5 seconds (enough for the first clone to start but not finish), then clones
// again. The second clone should be served from the spool rather than making a new
// upstream request.
func TestIntegrationSpoolReusesDuringClone(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "debug"})
	tmpDir := t.TempDir()
	clonesDir := filepath.Join(tmpDir, "clones")
	workDir := filepath.Join(tmpDir, "work")
	err := os.MkdirAll(workDir, 0o750)
	assert.NoError(t, err)

	// Count actual outbound upstream requests via a transport wrapper.
	var upstreamUploadPackRequests atomic.Int32

	mux := http.NewServeMux()
	gc := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    clonesDir,
		FetchInterval: 15,
	}, nil)
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	strategy, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, gc, func() (*githubapp.TokenManager, error) { return nil, nil }, nil) //nolint:nilnil
	assert.NoError(t, err)

	strategy.SetHTTPTransport(&countingTransport{
		inner:   http.DefaultTransport,
		counter: &upstreamUploadPackRequests,
		pattern: "git-upload-pack",
	})

	server := testServerWithLogging(ctx, mux)
	defer server.Close()

	repoURL := fmt.Sprintf("%s/git/github.com/git/git", server.URL)

	// First clone – triggers upstream pass-through and background clone.
	t.Log("Starting first clone")
	cmd := exec.Command("git", "clone", "--depth=1", repoURL, filepath.Join(workDir, "repo1"))
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("first clone output: %s", output)
	}
	assert.NoError(t, err)

	// Record how many upstream upload-pack requests the first clone made.
	firstCloneCount := upstreamUploadPackRequests.Load()
	t.Logf("Upstream upload-pack requests after first clone: %d", firstCloneCount)
	assert.True(t, firstCloneCount > 0, "first clone should have made upstream requests")

	// Wait long enough for the background clone to have started but (likely) not
	// finished for a repo as large as git/git.
	t.Log("Waiting 5 seconds for background clone to be in progress")
	time.Sleep(5 * time.Second)

	// Second clone – should be served from the spool if the background clone is
	// still running, or from the local backend if it already finished.
	t.Log("Starting second clone")
	cmd = exec.Command("git", "clone", "--depth=1", repoURL, filepath.Join(workDir, "repo2"))
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Logf("second clone output: %s", output)
	}
	assert.NoError(t, err)

	// Verify both clones produced a working checkout.
	for _, name := range []string{"repo1", "repo2"} {
		gitDir := filepath.Join(workDir, name, ".git")
		_, statErr := os.Stat(gitDir)
		assert.NoError(t, statErr, "expected .git in %s", name)
	}

	// The second clone should not have generated any new upstream upload-pack
	// requests — it should have been served entirely from the spool or local backend.
	totalCount := upstreamUploadPackRequests.Load()
	t.Logf("Total upstream upload-pack requests: %d (first clone: %d)", totalCount, firstCloneCount)
	assert.Equal(t, firstCloneCount, totalCount, "second clone should not have made additional upstream upload-pack requests")
}

// TestIntegrationNotOurRefFallsBackToUpstream reproduces the force-push race
// condition that causes "not our ref" errors and verifies that cachew falls back
// to upstream rather than returning the git protocol error to the client.
//
// The race is:
//  1. Client receives /info/refs showing commit A as the tip of a branch.
//  2. A concurrent background fetch runs, incorporating a force-push that makes
//     commit A unreachable from all refs in the local mirror.
//  3. Client sends POST /git-upload-pack requesting commit A.
//  4. git upload-pack rejects the request: "not our ref <A>".
//
// After the fix, cachew detects the stderr error before any bytes reach the
// client and transparently forwards the request to upstream.
//
// Run with: go test -v -run TestIntegrationNotOurRefFallsBackToUpstream -tags integration -timeout 30s
func TestIntegrationNotOurRefFallsBackToUpstream(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "debug"})
	tmpDir := t.TempDir()

	upstreamDir := filepath.Join(tmpDir, "upstream.git")
	workDir := filepath.Join(tmpDir, "work")
	clonesDir := filepath.Join(tmpDir, "clones")

	// --- Build the upstream repo with an initial commit ---
	runGit(t, "", "init", "--bare", upstreamDir)
	runGit(t, "", "clone", upstreamDir, workDir)
	err := os.WriteFile(filepath.Join(workDir, "file.txt"), []byte("initial"), 0o644)
	assert.NoError(t, err)
	runGit(t, workDir, "add", ".")
	runGit(t, workDir, "commit", "-m", "initial commit")
	runGit(t, workDir, "push", "origin", "HEAD:main")

	orphanedSHA := gitRevParse(t, workDir, "HEAD")
	t.Logf("Orphaned SHA will be: %s", orphanedSHA)

	// --- Clone upstream as the cachew mirror ---
	// The path must match what cachew derives from the URL /git/local/repo:
	//   host="local", repoPath="repo" → clonesDir/local/repo
	mirrorPath := filepath.Join(clonesDir, "local", "repo")
	runGit(t, "", "clone", "--mirror", upstreamDir, mirrorPath)

	// --- Force-push a completely new history to upstream ---
	// An orphan branch has no ancestors in common with the current history,
	// so after fetching the mirror, orphanedSHA becomes unreachable from all refs.
	runGit(t, workDir, "checkout", "--orphan", "replacement")
	err = os.WriteFile(filepath.Join(workDir, "file2.txt"), []byte("replacement"), 0o644)
	assert.NoError(t, err)
	runGit(t, workDir, "add", ".")
	runGit(t, workDir, "commit", "-m", "replacement commit")
	runGit(t, workDir, "push", "--force", "origin", "replacement:main")

	// Fetch the mirror so it picks up the force-push.
	// After this, orphanedSHA is in the ODB but unreachable from any ref.
	runGit(t, mirrorPath, "fetch", "--prune")

	// Sanity-check: orphanedSHA is still in the ODB ...
	catFile := exec.Command("git", "-C", mirrorPath, "cat-file", "-e", orphanedSHA)
	assert.NoError(t, catFile.Run(), "orphaned SHA should still exist in the mirror ODB")

	// ... but is not reachable from any branch.
	branchContains := exec.Command("git", "-C", mirrorPath, "branch", "--contains", orphanedSHA)
	out, _ := branchContains.CombinedOutput()
	assert.Equal(t, "", strings.TrimSpace(string(out)), "orphaned SHA should not be reachable from any branch")

	// --- Mock upstream: records requests, returns a minimal git flush response ---
	var upstreamHits atomic.Int32
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		t.Logf("Mock upstream received: %s %s", r.Method, r.URL.Path)
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("0008NAK\n")) // minimal NAK response
	}))
	defer mockUpstream.Close()

	// --- Set up cachew pointing at clonesDir ---
	mux := http.NewServeMux()
	gc := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    clonesDir,
		FetchInterval: 24 * time.Hour, // prevent auto-fetch during the test
	}, nil)
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	strategy, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, gc,
		func() (*githubapp.TokenManager, error) { return nil, nil }, nil) //nolint:nilnil
	assert.NoError(t, err)
	// The warm-up goroutine uses context.WithoutCancel so it survives
	// t.Context() cancellation. Wait for it before TempDir cleanup.
	t.Cleanup(func() { waitForReady(t, strategy) })

	// Redirect all upstream proxy requests to the mock server.
	var redirectHits atomic.Int32
	strategy.SetHTTPTransport(&upstreamRedirectTransport{
		targetBaseURL: mockUpstream.URL,
		inner:         http.DefaultTransport,
		hits:          &redirectHits,
	})

	cachewServer := testServerWithLogging(ctx, mux)
	defer cachewServer.Close()

	// --- Send a raw upload-pack POST requesting the orphaned SHA ---
	// This mirrors what a git client does after receiving /info/refs that
	// advertised orphanedSHA (before the force-push fetch updated the mirror).
	body := pktLine("want "+orphanedSHA+"\n") + "0000" + pktLine("done\n")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		cachewServer.URL+"/git/local/repo/git-upload-pack",
		strings.NewReader(body))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")

	// A dedicated client keeps this request out of the strategy's redirect
	// transport, so it reaches cachew rather than the mock upstream directly.
	client := &http.Client{Transport: http.DefaultTransport}
	resp, err := client.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	// After the fix: the request must have been forwarded to upstream.
	// Before the fix: upstreamHits == 0 and this assertion fails.
	assert.True(t, upstreamHits.Load() > 0,
		"cachew should fall back to upstream when git upload-pack returns 'not our ref'")
}

const (
	authzUser     = "tester"
	authzPassword = "s3cret"

	// cachewServiceUser and cachewServiceToken stand in for the credentials a
	// GitHub App installation hands cachew. Upstream accepts them for private
	// repositories, including for writes, so a request that reaches upstream as
	// cachew is rejected by cachew's own logic or not at all.
	cachewServiceUser  = "x-access-token"
	cachewServiceToken = "cachew-installation-token"
)

// authzUpstream serves bare repositories under root via `git http-backend`,
// rejecting requests without valid basic auth for repository paths listed in
// private.
//
// Requests for "<repo>.git/..." are rewritten to "<repo>/..." so a single
// on-disk repository answers both the ".git" URLs cachew's authorization probes
// use and the plain paths a git client (and cachew's reverse proxy) use. Probes
// are counted separately from proxied client traffic on that same basis.
type authzUpstream struct {
	root    string
	gitPath string
	private []string
	probes  atomic.Int32
	denials atomic.Int32
	// asCachew and asClient count requests by the identity they arrived with,
	// which is what distinguishes a forwarded push from one cachew performed on
	// the client's behalf.
	asCachew atomic.Int32
	asClient atomic.Int32
}

func newAuthzUpstream(t *testing.T, root string, private ...string) *authzUpstream {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not found in PATH")
	}
	assert.NoError(t, os.MkdirAll(root, 0o750))
	return &authzUpstream{root: root, gitPath: gitPath, private: private}
}

func (u *authzUpstream) requiresAuth(path string) bool {
	for _, prefix := range u.private {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func (u *authzUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.Path, ".git/info/refs") {
		u.probes.Add(1)
	}

	user, password, hasAuth := r.BasicAuth()
	asCachew := hasAuth && user == cachewServiceUser && password == cachewServiceToken
	asClient := hasAuth && user == authzUser && password == authzPassword
	switch {
	case asCachew:
		u.asCachew.Add(1)
	case asClient:
		u.asClient.Add(1)
	}

	if u.requiresAuth(r.URL.Path) && !asCachew && !asClient {
		u.denials.Add(1)
		w.Header().Set("WWW-Authenticate", `Basic realm="upstream"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if strings.Contains(r.URL.Path, "/info/lfs/") {
		w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
		_, _ = w.Write([]byte(`{"transfer":"basic","objects":[]}`))
		return
	}

	r2 := r.Clone(r.Context())
	r2.URL.Path = strings.Replace(r.URL.Path, ".git/", "/", 1)
	// Go's cgi.Handler rejects chunked request bodies with a 400.
	if r2.ContentLength < 0 {
		body, err := io.ReadAll(r2.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusInternalServerError)
			return
		}
		r2.Body = io.NopCloser(strings.NewReader(string(body)))
		r2.ContentLength = int64(len(body))
		r2.TransferEncoding = nil
	}

	env := []string{
		"GIT_PROJECT_ROOT=" + u.root,
		"GIT_HTTP_EXPORT_ALL=1",
		"PATH=" + os.Getenv("PATH"),
	}
	// git http-backend refuses receive-pack unless it knows who is pushing, so
	// an authenticated identity is what makes a push possible at all here —
	// exactly as on a real host.
	if asCachew || asClient {
		env = append(env, "REMOTE_USER="+user)
	}

	handler := &cgi.Handler{
		Path: u.gitPath,
		Args: []string{"http-backend"},
		Dir:  u.root,
		Env:  env,
	}
	handler.ServeHTTP(w, r2)
}

// seedUpstreamRepo creates a bare repository at root/name with one commit,
// returning the work tree it was pushed from and the new HEAD SHA.
func seedUpstreamRepo(t *testing.T, root, name string) (workDir, sha string) {
	t.Helper()
	bare := filepath.Join(root, name)
	runGit(t, "", "init", "--bare", bare)
	runGit(t, "", "-C", bare, "symbolic-ref", "HEAD", "refs/heads/main")
	workDir = filepath.Join(t.TempDir(), strings.ReplaceAll(name, "/", "-"))
	runGit(t, "", "clone", bare, workDir)
	return workDir, commitToUpstream(t, workDir, "initial")
}

// commitToUpstream commits content in an upstream work tree, pushes it to main
// and returns the new SHA.
func commitToUpstream(t *testing.T, workDir, content string) string {
	t.Helper()
	assert.NoError(t, os.WriteFile(filepath.Join(workDir, "file.txt"), []byte(content), 0o644))
	runGit(t, workDir, "add", ".")
	runGit(t, workDir, "commit", "-m", content)
	runGit(t, workDir, "push", "origin", "HEAD:main")
	return gitRevParse(t, workDir, "HEAD")
}

// mirrorUpstream pre-seeds a warm cachew mirror for host/repoPath, matching the
// layout cachew derives from /git/{host}/{path...}.
func mirrorUpstream(t *testing.T, clonesDir, host, repoPath, upstreamDir string) string {
	t.Helper()
	mirrorPath := filepath.Join(clonesDir, host, filepath.FromSlash(repoPath))
	runGit(t, "", "clone", "--mirror", upstreamDir, mirrorPath)
	return mirrorPath
}

// startAuthzCachew boots a git strategy whose upstream requests are redirected
// to mockURL. The redirect is passed to git.New rather than installed
// afterwards because the startup warm-up (and its visibility probes) runs
// immediately, and those requests must be redirected too.
//
// A non-empty serviceToken stands in for GitHub App credentials, so upstream
// can tell a request cachew authenticated as itself from one it forwarded on
// the client's behalf. It is installed before the server starts serving.
func startAuthzCachew(ctx context.Context, t *testing.T, mockURL, clonesDir, serviceToken string) (*git.Strategy, string) {
	t.Helper()
	return startAuthzCachewRouted(ctx, t, mockURL, clonesDir, serviceToken, nil)
}

// startAuthzCachewRouted is startAuthzCachew with per-host routing overrides:
// an upstream request whose URL host matches a key in hostRoutes is sent to
// that base URL instead of mockURL. That distinction matters for tests where
// the *host* cachew probes must differ from the host whose mirror it resolves,
// which the blanket redirect would otherwise erase.
func startAuthzCachewRouted(
	ctx context.Context, t *testing.T, mockURL, clonesDir, serviceToken string, hostRoutes map[string]string,
) (*git.Strategy, string) {
	t.Helper()
	var redirects atomic.Int32
	redirect := &upstreamRedirectTransport{
		targetBaseURL: mockURL,
		inner:         http.DefaultTransport,
		hits:          &redirects,
		hostRoutes:    hostRoutes,
	}

	mux := http.NewServeMux()
	gc := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    clonesDir,
		FetchInterval: 24 * time.Hour,
	}, nil)
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	strategy, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, gc,
		func() (*githubapp.TokenManager, error) { return nil, nil }, nil, //nolint:nilnil
		git.WithHTTPTransport(redirect))
	assert.NoError(t, err)
	t.Cleanup(func() { waitForReady(t, strategy) })
	waitForReady(t, strategy)
	if serviceToken != "" {
		strategy.SetServiceToken(func(context.Context, string) (string, error) { return serviceToken, nil })
	}

	server := testServerWithLogging(ctx, mux)
	t.Cleanup(server.Close)
	return strategy, server.URL
}

// getRefs requests the upload-pack ref advertisement for repoPath through
// cachew, optionally with client credentials, and returns the response status
// and body.
func getRefs(ctx context.Context, t *testing.T, cachewURL, repoPath string, withCredentials bool) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		cachewURL+"/git/local/"+repoPath+"/info/refs?service=git-upload-pack", nil)
	assert.NoError(t, err)
	if withCredentials {
		req.SetBasicAuth(authzUser, authzPassword)
	}
	resp, err := (&http.Client{}).Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	return resp.StatusCode, string(body)
}

// ensureRefs asks cachew to make refs/heads/main match sha, which is the
// on-demand refresh path an authorized client uses.
func ensureRefs(
	ctx context.Context, t *testing.T, cachewURL, repoPath, sha string, withCredentials bool,
) (int, git.EnsureRefsResponse) {
	t.Helper()
	payload, err := json.Marshal(git.EnsureRefsRequest{Refs: map[string]string{"refs/heads/main": sha}})
	assert.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		cachewURL+"/git/local/"+repoPath+git.EnsureRefsPath, strings.NewReader(string(payload)))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if withCredentials {
		req.SetBasicAuth(authzUser, authzPassword)
	}
	resp, err := (&http.Client{}).Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	var decoded git.EnsureRefsResponse
	if resp.StatusCode == http.StatusOK {
		assert.NoError(t, json.NewDecoder(resp.Body).Decode(&decoded))
		return resp.StatusCode, decoded
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, decoded
}

func TestIntegrationAuthRequiredRepoChecksClientCredentials(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "debug"})
	tmpDir := t.TempDir()
	upstreamRoot := filepath.Join(tmpDir, "upstream")
	clonesDir := filepath.Join(tmpDir, "clones")

	upstream := newAuthzUpstream(t, upstreamRoot, "/private/")
	_, sha := seedUpstreamRepo(t, upstreamRoot, "private/repo")
	mirrorUpstream(t, clonesDir, "local", "private/repo", filepath.Join(upstreamRoot, "private/repo"))

	mock := testServerWithLogging(ctx, upstream)
	defer mock.Close()
	_, cachewURL := startAuthzCachew(ctx, t, mock.URL, clonesDir, "")

	t.Run("AnonymousClientDenied", func(t *testing.T) {
		status, body := getRefs(ctx, t, cachewURL, "private/repo", false)
		assert.Equal(t, http.StatusUnauthorized, status)
		assert.False(t, strings.Contains(body, sha), "mirrored refs must not leak to an unauthenticated client")
	})

	t.Run("AnonymousUploadPackDenied", func(t *testing.T) {
		body := pktLine("want "+sha+"\n") + "0000" + pktLine("done\n")
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			cachewURL+"/git/local/private/repo/git-upload-pack", strings.NewReader(body))
		assert.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
		resp, err := (&http.Client{}).Do(req)
		assert.NoError(t, err)
		defer resp.Body.Close()
		got, err := io.ReadAll(resp.Body)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		assert.False(t, strings.Contains(string(got), "PACK"), "no pack data may be served to an unauthenticated client")
	})

	t.Run("BadCredentialsDenied", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			cachewURL+"/git/local/private/repo/info/refs?service=git-upload-pack", nil)
		assert.NoError(t, err)
		req.SetBasicAuth(authzUser, "wrong-password")
		resp, err := (&http.Client{}).Do(req)
		assert.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
		assert.False(t, strings.Contains(string(body), sha), "mirrored refs must not leak to a rejected client")
	})

	t.Run("ValidCredentialsServed", func(t *testing.T) {
		status, body := getRefs(ctx, t, cachewURL, "private/repo", true)
		assert.Equal(t, http.StatusOK, status)
		assert.True(t, strings.Contains(body, sha), "authorized client should receive the mirrored refs")
	})

	t.Run("ValidCredentialsCanClone", func(t *testing.T) {
		repoURL := strings.Replace(cachewURL, "http://",
			fmt.Sprintf("http://%s:%s@", authzUser, authzPassword), 1) + "/git/local/private/repo"
		target := filepath.Join(t.TempDir(), "clone")
		cmd := exec.Command("git", "clone", repoURL, target)
		cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git clone: %v\n%s", err, out)
		}
		assert.Equal(t, sha, gitRevParse(t, target, "HEAD"))
	})
}

func TestIntegrationPublicRepoServedWithoutClientCredentials(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "debug"})
	tmpDir := t.TempDir()
	upstreamRoot := filepath.Join(tmpDir, "upstream")
	clonesDir := filepath.Join(tmpDir, "clones")

	upstream := newAuthzUpstream(t, upstreamRoot)
	_, sha := seedUpstreamRepo(t, upstreamRoot, "public/repo")
	mirrorUpstream(t, clonesDir, "local", "public/repo", filepath.Join(upstreamRoot, "public/repo"))

	mock := testServerWithLogging(ctx, upstream)
	defer mock.Close()
	_, cachewURL := startAuthzCachew(ctx, t, mock.URL, clonesDir, "")

	for range 3 {
		status, body := getRefs(ctx, t, cachewURL, "public/repo", false)
		assert.Equal(t, http.StatusOK, status)
		assert.True(t, strings.Contains(body, sha), "public repo should be served from the mirror without credentials")
	}

	assert.Equal(t, int32(1), upstream.probes.Load(),
		"the public verdict must be cached, so repeated requests add no upstream probes")
	assert.Equal(t, int32(0), upstream.denials.Load())
}

func TestIntegrationAuthRequiredRepoNotProactivelyRefreshed(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "debug"})
	tmpDir := t.TempDir()
	upstreamRoot := filepath.Join(tmpDir, "upstream")
	clonesDir := filepath.Join(tmpDir, "clones")

	upstream := newAuthzUpstream(t, upstreamRoot, "/private/")
	privateWork, privateSHA := seedUpstreamRepo(t, upstreamRoot, "private/repo")
	publicWork, publicSHA := seedUpstreamRepo(t, upstreamRoot, "public/repo")
	privateMirror := mirrorUpstream(t, clonesDir, "local", "private/repo", filepath.Join(upstreamRoot, "private/repo"))
	publicMirror := mirrorUpstream(t, clonesDir, "local", "public/repo", filepath.Join(upstreamRoot, "public/repo"))

	// Advance both upstreams so a proactive refresh is observable as a change
	// in the mirror's main ref.
	newPrivateSHA := commitToUpstream(t, privateWork, "private update")
	newPublicSHA := commitToUpstream(t, publicWork, "public update")
	assert.NotEqual(t, privateSHA, newPrivateSHA)
	assert.NotEqual(t, publicSHA, newPublicSHA)

	mock := testServerWithLogging(ctx, upstream)
	defer mock.Close()
	_, cachewURL := startAuthzCachew(ctx, t, mock.URL, clonesDir, "")

	assert.Equal(t, newPublicSHA, gitRevParse(t, publicMirror, "refs/heads/main"),
		"public repo should still be refreshed proactively at startup")
	assert.Equal(t, privateSHA, gitRevParse(t, privateMirror, "refs/heads/main"),
		"auth-required repo must not be refreshed proactively with cachew's own credentials")

	status, _ := getRefs(ctx, t, cachewURL, "private/repo", false)
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.Equal(t, privateSHA, gitRevParse(t, privateMirror, "refs/heads/main"),
		"a denied request must not trigger a refresh either")

	status, _ = ensureRefs(ctx, t, cachewURL, "private/repo", newPrivateSHA, false)
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.Equal(t, privateSHA, gitRevParse(t, privateMirror, "refs/heads/main"),
		"a denied on-demand refresh must not fetch")

	status, body := ensureRefs(ctx, t, cachewURL, "private/repo", newPrivateSHA, true)
	assert.Equal(t, http.StatusOK, status)
	assert.True(t, body.Fetched, "an authorized on-demand request should fetch")
	assert.Equal(t, newPrivateSHA, body.Refs["refs/heads/main"])
	assert.Equal(t, newPrivateSHA, gitRevParse(t, privateMirror, "refs/heads/main"),
		"an authorized on-demand request should refresh the auth-required mirror")
}

// pushThroughCachew pushes workDir's HEAD to refs/heads/branch through cachew,
// reporting the combined git output and whether the push succeeded.
func pushThroughCachew(t *testing.T, workDir, cachewURL, repoPath, branch string, withCredentials bool) (string, bool) {
	t.Helper()
	base := cachewURL
	if withCredentials {
		base = strings.Replace(cachewURL, "http://", fmt.Sprintf("http://%s:%s@", authzUser, authzPassword), 1)
	}
	cmd := exec.Command("git", "-C", workDir, "push", base+"/git/github.com/"+repoPath, "HEAD:refs/heads/"+branch)
	// A stray credential helper would silently supply the credentials this test
	// is asserting the absence of.
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "HOME="+t.TempDir())
	out, err := cmd.CombinedOutput()
	return string(out), err == nil
}

func upstreamHasRef(t *testing.T, bare, ref string) bool {
	t.Helper()
	return exec.Command("git", "-C", bare, "rev-parse", "--verify", ref).Run() == nil
}

func TestIntegrationPushIsNotAuthenticatedAsCachew(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "debug"})
	tmpDir := t.TempDir()
	upstreamRoot := filepath.Join(tmpDir, "upstream")
	clonesDir := filepath.Join(tmpDir, "clones")

	upstream := newAuthzUpstream(t, upstreamRoot, "/private/")
	work, _ := seedUpstreamRepo(t, upstreamRoot, "private/repo")
	bare := filepath.Join(upstreamRoot, "private/repo")

	assert.NoError(t, os.WriteFile(filepath.Join(work, "pushed.txt"), []byte("pushed"), 0o644))
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "push through cachew")
	pushedSHA := gitRevParse(t, work, "HEAD")

	mock := testServerWithLogging(ctx, upstream)
	defer mock.Close()
	_, cachewURL := startAuthzCachew(ctx, t, mock.URL, clonesDir, cachewServiceToken)

	t.Run("AnonymousPushRejected", func(t *testing.T) {
		out, ok := pushThroughCachew(t, work, cachewURL, "private/repo", "anonymous", false)
		assert.False(t, ok, "a push without client credentials must not be accepted:\n%s", out)
		assert.Equal(t, int32(0), upstream.asCachew.Load(),
			"cachew must not substitute its own credentials on a push")
		assert.True(t, upstream.denials.Load() > 0, "upstream itself should have rejected the push")
		assert.False(t, upstreamHasRef(t, bare, "refs/heads/anonymous"), "no ref may be created upstream")
	})

	t.Run("ClientCredentialsPushAccepted", func(t *testing.T) {
		out, ok := pushThroughCachew(t, work, cachewURL, "private/repo", "authorized", true)
		assert.True(t, ok, "a push with valid client credentials should succeed:\n%s", out)
		assert.True(t, upstream.asClient.Load() > 0, "the push should be attributed to the client")
		assert.Equal(t, int32(0), upstream.asCachew.Load())
		assert.Equal(t, pushedSHA, gitRevParse(t, bare, "refs/heads/authorized"))
	})
}

// lfsBatch posts an LFS upload batch request for repoPath through cachew.
func lfsBatch(ctx context.Context, t *testing.T, cachewURL, repoPath string, withCredentials bool) (int, string) {
	t.Helper()
	payload := `{"operation":"upload","transfers":["basic"],"objects":[{"oid":"abc123","size":1}]}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		cachewURL+"/git/github.com/"+repoPath+"/info/lfs/objects/batch", strings.NewReader(payload))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	if withCredentials {
		req.SetBasicAuth(authzUser, authzPassword)
	}
	resp, err := (&http.Client{}).Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	return resp.StatusCode, string(body)
}

func TestIntegrationLFSRequestsCarryClientCredentials(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "debug"})
	tmpDir := t.TempDir()
	upstreamRoot := filepath.Join(tmpDir, "upstream")
	clonesDir := filepath.Join(tmpDir, "clones")

	upstream := newAuthzUpstream(t, upstreamRoot, "/private/")
	mock := testServerWithLogging(ctx, upstream)
	defer mock.Close()
	_, cachewURL := startAuthzCachew(ctx, t, mock.URL, clonesDir, cachewServiceToken)

	t.Run("AnonymousBatchDenied", func(t *testing.T) {
		status, _ := lfsBatch(ctx, t, cachewURL, "private/repo", false)
		assert.Equal(t, http.StatusUnauthorized, status)
		assert.Equal(t, int32(0), upstream.asCachew.Load(),
			"cachew must not substitute its own credentials on LFS requests")
	})

	t.Run("ClientCredentialsBatchServed", func(t *testing.T) {
		status, body := lfsBatch(ctx, t, cachewURL, "private/repo", true)
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, body, "transfer")
		assert.True(t, upstream.asClient.Load() > 0, "the LFS request should be attributed to the client")
		assert.Equal(t, int32(0), upstream.asCachew.Load())
	})
}

// permissiveUpstream answers every ref advertisement with a well-formed but
// empty upload-pack response, and 200s everything else. It stands in for a host
// an attacker controls (or any host that simply answers 200), which is all that
// is needed to make cachew classify a repository as public.
type permissiveUpstream struct{ hits atomic.Int32 }

func (u *permissiveUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.hits.Add(1)
	if strings.Contains(r.URL.Path, "/info/refs") {
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		_, _ = io.WriteString(w, pktLine("# service=git-upload-pack\n")+"0000"+"0000")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func TestIntegrationHostTraversalCannotLaunderAuthorization(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "debug"})
	tmpDir := t.TempDir()
	upstreamRoot := filepath.Join(tmpDir, "upstream")
	clonesDir := filepath.Join(tmpDir, "clones")

	upstream := newAuthzUpstream(t, upstreamRoot, "/private/")
	_, sha := seedUpstreamRepo(t, upstreamRoot, "private/repo")
	mirrorUpstream(t, clonesDir, "local", "private/repo", filepath.Join(upstreamRoot, "private/repo"))

	mock := testServerWithLogging(ctx, upstream)
	defer mock.Close()
	attacker := &permissiveUpstream{}
	attackerServer := testServerWithLogging(ctx, attacker)
	defer attackerServer.Close()

	_, cachewURL := startAuthzCachewRouted(ctx, t, mock.URL, clonesDir, "",
		map[string]string{"attacker.invalid": attackerServer.URL})

	// Sanity check: the honest path to the same mirror is refused.
	status, body := getRefs(ctx, t, cachewURL, "private/repo", false)
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.False(t, strings.Contains(body, sha))

	// Each host below resolves, via filepath.Join, to the mirror of
	// local/private/repo while naming attacker.invalid as the authority to
	// probe. Every suffix is a mirror-serving path, so none may be answered.
	hosts := []string{
		"attacker.invalid%2f%2e%2e",
		"attacker.invalid%2f..",
		"attacker.invalid%2F%2E%2E",
	}
	suffixes := []string{
		"/info/refs?service=git-upload-pack",
		"/snapshot.tar.zst",
		"/snapshot.bundle",
		"/lfs-snapshot.tar.zst",
	}
	for _, host := range hosts {
		for _, suffix := range suffixes {
			t.Run(strings.NewReplacer("%", "", "/", "", ".", "", "?", "", "=", "").Replace(host+suffix), func(t *testing.T) {
				target := cachewURL + "/git/" + host + "/local/private/repo" + suffix
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
				assert.NoError(t, err)
				resp, err := (&http.Client{}).Do(req)
				assert.NoError(t, err)
				defer resp.Body.Close()
				got, err := io.ReadAll(resp.Body)
				assert.NoError(t, err)
				assert.False(t, strings.Contains(string(got), sha),
					"a request authorized against another host must not serve this mirror (status %d)", resp.StatusCode)
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
					"host traversal must be refused outright rather than resolved to a mirror")
			})
		}

		// ensure-refs reads the mirror directly rather than through
		// git http-backend, whose own path-alias check incidentally blocks the
		// smart-HTTP variants above.
		t.Run("EnsureRefs"+strings.NewReplacer("%", "", "/", "", ".", "").Replace(host), func(t *testing.T) {
			payload, err := json.Marshal(git.EnsureRefsRequest{Refs: map[string]string{"refs/heads/main": ""}})
			assert.NoError(t, err)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost,
				cachewURL+"/git/"+host+"/local/private/repo"+git.EnsureRefsPath, strings.NewReader(string(payload)))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp, err := (&http.Client{}).Do(req)
			assert.NoError(t, err)
			defer resp.Body.Close()
			got, err := io.ReadAll(resp.Body)
			assert.NoError(t, err)
			assert.False(t, strings.Contains(string(got), sha),
				"a request authorized against another host must not resolve refs from this mirror (status %d)", resp.StatusCode)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
				"host traversal must be refused outright rather than resolved to a mirror")
		})
	}
}

func TestIntegrationReceivePackQueryCannotBypassGating(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "debug"})
	tmpDir := t.TempDir()
	upstreamRoot := filepath.Join(tmpDir, "upstream")
	clonesDir := filepath.Join(tmpDir, "clones")

	upstream := newAuthzUpstream(t, upstreamRoot, "/private/")
	_, sha := seedUpstreamRepo(t, upstreamRoot, "private/repo")
	mirrorUpstream(t, clonesDir, "local", "private/repo", filepath.Join(upstreamRoot, "private/repo"))

	mock := testServerWithLogging(ctx, upstream)
	defer mock.Close()
	_, cachewURL := startAuthzCachew(ctx, t, mock.URL, clonesDir, "")

	t.Run("EnsureRefs", func(t *testing.T) {
		payload, err := json.Marshal(git.EnsureRefsRequest{Refs: map[string]string{"refs/heads/main": sha}})
		assert.NoError(t, err)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			cachewURL+"/git/local/private/repo"+git.EnsureRefsPath+"?service=git-receive-pack",
			strings.NewReader(string(payload)))
		assert.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := (&http.Client{}).Do(req)
		assert.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		assert.NoError(t, err)
		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"?service=git-receive-pack must not turn off authorization for a mirror-serving path")
		assert.False(t, strings.Contains(string(body), sha),
			"mirrored refs must not leak to an unauthenticated client")
	})

	for _, suffix := range []string{"/snapshot.tar.zst", "/snapshot.bundle", "/lfs-snapshot.tar.zst"} {
		t.Run(strings.TrimPrefix(suffix, "/"), func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet,
				cachewURL+"/git/local/private/repo"+suffix+"?service=git-receive-pack", nil)
			assert.NoError(t, err)
			resp, err := (&http.Client{}).Do(req)
			assert.NoError(t, err)
			defer resp.Body.Close()
			_, err = io.Copy(io.Discard, resp.Body)
			assert.NoError(t, err)
			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
				"?service=git-receive-pack must not turn off authorization for a mirror-serving path")
		})
	}
}
