package git_test

import (
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel/attribute"
)

// TestUploadPackStaleCommitGraphRepairsAndServes covers the incident end to end.
// The mirror carries a commit-graph indexing a commit a prune deleted, so
// upload-pack dies reporting repository corruption on the remote side. cachew
// must repair the index, because no later fetch or repack rebuilds it.
func TestUploadPackStaleCommitGraphRepairsAndServes(t *testing.T) {
	reader := setupMetricsReader(t)
	fixture := newInfoRefsFixture(t)
	setInfoRefsLsRemote(t, fixture.upstreamPath, false)

	// An orphan branch, so deleting it leaves a genuinely unreachable commit.
	orphan := commitOrphanBranch(t, fixture.workPath, "feature")
	runInfoRefsGit(t, "-C", fixture.workPath, "push", "origin", "feature")
	runInfoRefsGit(t, "-C", fixture.mirrorPath, "config", "remote.origin.url", fixture.upstreamPath)
	runInfoRefsGit(t, "-C", fixture.mirrorPath, "config", "core.commitGraph", "true")
	// Left on, a fetch rewrites the graph itself and would mask whether the
	// serve-path repair is doing anything.
	runInfoRefsGit(t, "-C", fixture.mirrorPath, "config", "fetch.writeCommitGraph", "false")
	runInfoRefsGit(t, "-C", fixture.mirrorPath, "config", "uploadpack.allowAnySHA1InWant", "true")
	runInfoRefsGit(t, "-C", fixture.mirrorPath, "fetch", "--prune", "--prune-tags")
	runInfoRefsGit(t, "-C", fixture.mirrorPath, "commit-graph", "write", "--reachable")

	// Upstream deletes the branch; the mirror prunes the object it orphaned while
	// the graph written above keeps indexing it.
	runInfoRefsGit(t, "-C", fixture.workPath, "push", "origin", ":feature")
	runInfoRefsGit(t, "-C", fixture.mirrorPath, "fetch", "--prune", "--prune-tags")
	runInfoRefsGit(t, "-C", fixture.mirrorPath, "reflog", "expire", "--expire=now", "--all")
	runInfoRefsGit(t, "-C", fixture.mirrorPath, "-c", "gc.auto=0", "repack", "-a", "-d", "-q")
	runInfoRefsGit(t, "-C", fixture.mirrorPath, "-c", "gc.auto=0", "prune", "--expire=now")
	assert.False(t, graphVerifiesIn(t, fixture.mirrorPath),
		"setup must leave a stale commit-graph, otherwise this test proves nothing")

	// Ask for the pruned commit by OID with the capabilities a real client sends.
	// Without them upload-pack answers "not our ref" and never walks the graph,
	// which is what makes this the corruption path rather than a missing object.
	w := fixture.requestUploadPackWithCaps(t, orphan)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, graphVerifiesIn(t, fixture.mirrorPath),
		"the mirror's commit-graph must be repaired after a serve failure, or every later request pays the same fault")
	assert.Contains(t, fixture.logs.String(), "aborting due to possible repository corruption")

	repairs := counterPoints(collectMetrics(fixture.ctx, t, reader), operationsMetric, attribute.NewSet(
		attribute.String("operation", "commit-graph-repair"),
		attribute.String("status", "success"),
		attribute.String("trigger", "request"),
	))
	assert.Equal(t, 1, len(repairs))
	assert.Equal(t, int64(1), repairs[0].Value, "a retryable serve failure must rewrite the graph exactly once")
}

// requestUploadPackWithCaps sends a want carrying the capabilities a real client
// negotiates, which is what makes upload-pack run pack-objects over the graph.
func (f *infoRefsFixture) requestUploadPackWithCaps(t *testing.T, want string) *httptest.ResponseRecorder {
	t.Helper()
	body := pkt("want "+want+" multi_ack_detailed side-band-64k thin-pack ofs-delta\n") + "0000" + pkt("done\n")
	req := httptest.NewRequestWithContext(f.ctx, http.MethodPost,
		"/git/example.test/org/repo/git-upload-pack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	req.SetPathValue("host", "example.test")
	req.SetPathValue("path", "org/repo/git-upload-pack")
	w := httptest.NewRecorder()
	f.uploadPack.ServeHTTP(w, req)
	return w
}

// commitOrphanBranch creates a branch whose tip is not reachable from main and
// returns its commit OID.
func commitOrphanBranch(t *testing.T, workPath, branch string) string {
	t.Helper()
	runInfoRefsGit(t, "-C", workPath, "checkout", "--orphan", branch)
	runInfoRefsGit(t, "-C", workPath, "rm", "-rqf", ".")
	runInfoRefsGit(t, "-C", workPath, "commit", "-m", "orphan", "--allow-empty")
	oid := runInfoRefsGit(t, "-C", workPath, "rev-parse", "HEAD")
	runInfoRefsGit(t, "-C", workPath, "checkout", "main")
	return oid
}

func graphVerifiesIn(t *testing.T, repoPath string) bool {
	t.Helper()
	return exec.Command("git", "-C", repoPath, "commit-graph", "verify").Run() == nil //nolint:gosec // test-owned temp path
}
