package git_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/githubapp"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/strategy/git"
)

type infoRefsTransport struct {
	hits atomic.Int32
}

// RoundTrip records proxy requests and returns a response that identifies the upstream path.
func (t *infoRefsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.hits.Add(1)
	body := "proxied-info-refs"
	return &http.Response{
		StatusCode:    http.StatusOK,
		Status:        http.StatusText(http.StatusOK),
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}, nil
}

type infoRefsFixture struct {
	ctx          context.Context
	handler      http.Handler
	uploadPack   http.Handler
	transport    *infoRefsTransport
	logs         *bytes.Buffer
	upstreamPath string
	workPath     string
	mirrorPath   string
}

func newInfoRefsFixture(t *testing.T, checkInterval ...time.Duration) *infoRefsFixture {
	t.Helper()
	cloneConfig := gitclone.Config{}
	if len(checkInterval) > 0 {
		cloneConfig.RefCheckInterval = checkInterval[0]
	}
	return newInfoRefsFixtureWithConfig(t, git.Config{DisableUpstreamAuthz: true}, cloneConfig)
}

// newInfoRefsFixtureWithConfig builds the fixture with the given strategy and
// clone manager configuration; the clone manager's MirrorRoot is always the
// fixture's own.
func newInfoRefsFixtureWithConfig(t *testing.T, gitConfig git.Config, cloneConfig gitclone.Config) *infoRefsFixture {
	t.Helper()

	tmpDir := t.TempDir()
	upstreamPath := filepath.Join(tmpDir, "upstream.git")
	workPath := filepath.Join(tmpDir, "work")
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	mirrorPath := filepath.Join(mirrorRoot, "example.test", "org", "repo")

	runInfoRefsGit(t, "init", "--bare", upstreamPath)
	runInfoRefsGit(t, "clone", upstreamPath, workPath)
	runInfoRefsGit(t, "-C", workPath, "config", "user.email", "test@test.com")
	runInfoRefsGit(t, "-C", workPath, "config", "user.name", "Test")
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "file.txt"), []byte("initial"), 0o644))
	runInfoRefsGit(t, "-C", workPath, "add", "file.txt")
	runInfoRefsGit(t, "-C", workPath, "commit", "-m", "initial")
	runInfoRefsGit(t, "-C", workPath, "push", "origin", "HEAD:main")
	runInfoRefsGit(t, "-C", upstreamPath, "symbolic-ref", "HEAD", "refs/heads/main")
	runInfoRefsGit(t, "clone", "--mirror", upstreamPath, mirrorPath)

	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := logging.ContextWithLogger(t.Context(), logger)
	mux := newTestMux()
	cloneConfig.MirrorRoot = mirrorRoot
	manager := gitclone.NewManagerProvider(ctx, cloneConfig, nil)
	clones, err := manager()
	assert.NoError(t, err)
	_, err = clones.GetOrCreate(ctx, "https://example.test/org/repo")
	assert.NoError(t, err)
	runInfoRefsGit(t, "-C", mirrorPath, "config", "remote.origin.url", upstreamPath)
	strategy, err := git.New(ctx, gitConfig, newTestScheduler(ctx, t), nil, mux, manager,
		func() (*githubapp.TokenManager, error) { return nil, nil }, nil) //nolint:nilnil
	assert.NoError(t, err)
	waitForReady(t, strategy)

	transport := &infoRefsTransport{}
	strategy.SetHTTPTransport(transport)
	handler := mux.handlers["GET /git/{host}/{path...}"]
	assert.NotZero(t, handler)
	uploadPack := mux.handlers["POST /git/{host}/{path...}"]
	assert.NotZero(t, uploadPack)

	return &infoRefsFixture{
		ctx:          ctx,
		handler:      handler,
		uploadPack:   uploadPack,
		transport:    transport,
		logs:         logs,
		upstreamPath: upstreamPath,
		workPath:     workPath,
		mirrorPath:   mirrorPath,
	}
}

func runInfoRefsGit(t *testing.T, args ...string) string {
	t.Helper()
	output, err := exec.Command("git", args...).CombinedOutput()
	assert.NoError(t, err, string(output))
	return strings.TrimSpace(string(output))
}

func setInfoRefsLsRemote(t *testing.T, target string, fail bool) string {
	t.Helper()
	realGit, err := exec.LookPath("git")
	assert.NoError(t, err)
	wrapperDir := t.TempDir()
	wrapper := filepath.Join(wrapperDir, "git")
	calls := filepath.Join(wrapperDir, "ls-remote-calls")
	lsRemote := fmt.Sprintf("exec %q ls-remote %q", realGit, target)
	if fail {
		lsRemote = `echo "ls-remote failed" >&2
  exit 1`
	}
	script := fmt.Sprintf(`#!/bin/sh
if echo " $* " | grep -q ' ls-remote '; then
  printf 'check\n' >> %q
  %s
fi
exec %q "$@"
`, calls, lsRemote, realGit)
	assert.NoError(t, os.WriteFile(wrapper, []byte(script), 0o755))
	t.Setenv("PATH", wrapperDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return calls
}

func (f *infoRefsFixture) request(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(f.ctx, http.MethodGet,
		"/git/example.test/org/repo/info/refs?service=git-upload-pack", nil)
	req.SetPathValue("host", "example.test")
	req.SetPathValue("path", "org/repo/info/refs")
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, req)
	return w
}

func (f *infoRefsFixture) requestLsRefs(t *testing.T, refPrefixes ...string) *httptest.ResponseRecorder {
	t.Helper()
	var body strings.Builder
	body.WriteString(pkt("command=ls-refs\n") + "0001")
	for _, prefix := range refPrefixes {
		body.WriteString(pkt("ref-prefix " + prefix + "\n"))
	}
	body.WriteString("0000")
	req := httptest.NewRequestWithContext(f.ctx, http.MethodPost,
		"/git/example.test/org/repo/git-upload-pack", strings.NewReader(body.String()))
	req.Header.Set("Git-Protocol", "version=2")
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	req.SetPathValue("host", "example.test")
	req.SetPathValue("path", "org/repo/git-upload-pack")
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, req)
	return w
}

func (f *infoRefsFixture) commitUpstream(t *testing.T) string {
	t.Helper()
	assert.NoError(t, os.WriteFile(filepath.Join(f.workPath, "file.txt"), []byte("updated"), 0o644))
	runInfoRefsGit(t, "-C", f.workPath, "commit", "-am", "updated")
	runInfoRefsGit(t, "-C", f.workPath, "push", "origin", "HEAD:main")
	return runInfoRefsGit(t, "-C", f.workPath, "rev-parse", "HEAD")
}

func TestInfoRefsCheckErrorServesMirror(t *testing.T) {
	fixture := newInfoRefsFixture(t)
	setInfoRefsLsRemote(t, "", true)

	w := fixture.request(t)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-git-upload-pack-advertisement")
	assert.Equal(t, int32(0), fixture.transport.hits.Load())
	assert.Contains(t, fixture.logs.String(), "Ref staleness check failed, serving mirror")
}

func TestLsRefsCheckErrorServesMirror(t *testing.T) {
	fixture := newInfoRefsFixture(t)
	setInfoRefsLsRemote(t, "", true)

	w := fixture.requestLsRefs(t)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-git-upload-pack-result")
	assert.Equal(t, int32(0), fixture.transport.hits.Load())
	assert.Contains(t, fixture.logs.String(), "Ref staleness check failed, serving mirror")
}

func TestUploadPackCachedCommitSkipsRefFreshnessCheck(t *testing.T) {
	fixture := newInfoRefsFixture(t)
	want := runInfoRefsGit(t, "-C", fixture.mirrorPath, "rev-parse", "refs/heads/main")
	calls := setInfoRefsLsRemote(t, "", true)

	w := fixture.requestUploadPack(t, want)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-git-upload-pack-result")
	assert.Contains(t, w.Body.String(), "PACK")
	assert.Equal(t, int32(0), fixture.transport.hits.Load())
	_, err := os.Stat(calls)
	assert.True(t, os.IsNotExist(err))
}

func TestLsRefsStaleForwardsUpstream(t *testing.T) {
	fixture := newInfoRefsFixture(t, time.Nanosecond)
	setInfoRefsLsRemote(t, fixture.upstreamPath, false)
	fixture.commitUpstream(t)

	w := fixture.requestLsRefs(t)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "proxied-info-refs", w.Body.String())
	assert.Equal(t, int32(1), fixture.transport.hits.Load())
}

func TestInfoRefsStaleForwardsAndFetches(t *testing.T) {
	fixture := newInfoRefsFixture(t, time.Nanosecond)
	setInfoRefsLsRemote(t, fixture.upstreamPath, false)
	wantSHA := fixture.commitUpstream(t)

	w := fixture.request(t)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "proxied-info-refs", w.Body.String())
	assert.Equal(t, int32(1), fixture.transport.hits.Load())

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runInfoRefsGit(t, "-C", fixture.mirrorPath, "rev-parse", "refs/heads/main") == wantSHA {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("mirror did not fetch %s", wantSHA)
}

func TestInfoRefsV2StaleServesCapabilitiesAndFetches(t *testing.T) {
	fixture := newInfoRefsFixture(t, time.Nanosecond)
	setInfoRefsLsRemote(t, fixture.upstreamPath, false)
	wantSHA := fixture.commitUpstream(t)

	req := httptest.NewRequestWithContext(fixture.ctx, http.MethodGet,
		"/git/example.test/org/repo/info/refs?service=git-upload-pack", nil)
	req.Header.Set("Git-Protocol", "version=2")
	req.SetPathValue("host", "example.test")
	req.SetPathValue("path", "org/repo/info/refs")
	w := httptest.NewRecorder()
	fixture.handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, int32(0), fixture.transport.hits.Load(), "a v2 capability advertisement names no refs, so it must not be forwarded")
	assert.Contains(t, w.Body.String(), "version 2")
	waitForMirrorRef(t, fixture.mirrorPath, "refs/heads/main", wantSHA)
}

func TestLsRefsStalenessScopedToRefPrefixes(t *testing.T) {
	tests := []struct {
		name      string
		prefixes  []string
		forwarded bool
	}{
		{name: "MovedBranchForwards", prefixes: []string{"refs/heads/main"}, forwarded: true},
		{name: "NoPrefixesForwards", prefixes: nil, forwarded: true},
		{name: "UnmovedBranchServesMirror", prefixes: []string{"refs/heads/release"}, forwarded: false},
		{name: "TagsOnlyServesMirror", prefixes: []string{"refs/tags/"}, forwarded: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newInfoRefsFixture(t, time.Nanosecond)
			setInfoRefsLsRemote(t, fixture.upstreamPath, false)
			fixture.commitUpstream(t)

			w := fixture.requestLsRefs(t, tt.prefixes...)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, tt.forwarded, fixture.transport.hits.Load() == 1)
		})
	}
}

func waitForMirrorRef(t *testing.T, mirrorPath, ref, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runInfoRefsGit(t, "-C", mirrorPath, "rev-parse", ref) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("mirror did not fetch %s", want)
}

func TestInfoRefsFreshServesMirrorAndCachesCheck(t *testing.T) {
	fixture := newInfoRefsFixture(t)
	setInfoRefsLsRemote(t, fixture.upstreamPath, false)

	w := fixture.request(t)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, int32(0), fixture.transport.hits.Load())
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-git-upload-pack-advertisement")
	assert.NotEqual(t, "proxied-info-refs", w.Body.String())

	assert.NoError(t, os.Rename(fixture.upstreamPath, fixture.upstreamPath+".unavailable"))
	w = fixture.request(t)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, int32(0), fixture.transport.hits.Load())
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-git-upload-pack-advertisement")
}
