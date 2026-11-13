package gitclone //nolint:testpackage // white-box testing required for unexported fields

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/logging"
)

// buildStaleCommitGraph leaves the mirror with a commit-graph that names an
// object a prune deleted, which is the state production hit: upstream deletes a
// ref, the mirror prunes the now-unreachable object, and the graph written
// before that prune keeps indexing it.
func buildStaleCommitGraph(t *testing.T, repo *Repository, upstreamPath string) string {
	t.Helper()

	gitOutput(t, repo.path, "commit-graph", "write", "--reachable")
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	packMirror(t, repo.path)
	gitOutput(t, repo.path, "-c", "gc.auto=0", "prune", "--expire=now")
	assert.False(t, objectExists(t, repo.path, orphaned), "the pruned commit should be gone from the object store")
	return orphaned
}

// commitGraphVerifies reports whether the commit-graph still resolves cleanly,
// which is the same check a serving read performs.
func commitGraphVerifies(t *testing.T, repoPath string) bool {
	t.Helper()
	return exec.Command("git", "-C", repoPath, "commit-graph", "verify").Run() == nil //nolint:gosec // test-owned temp path
}

func TestRewriteCommitGraphDropsPrunedCommits(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testRepoConfig())

	orphaned := buildStaleCommitGraph(t, repo, upstreamPath)
	assert.False(t, commitGraphVerifies(t, repo.path),
		"the commit-graph should be stale before the repair, otherwise this test proves nothing")

	assert.NoError(t, repo.RewriteCommitGraph(ctx))

	assert.True(t, commitGraphVerifies(t, repo.path),
		"rewriting the graph should leave it naming only commits the object store still has")
	assert.False(t, objectExists(t, repo.path, orphaned),
		"repairing the index must not resurrect the pruned object")
}

// TestRewriteCommitGraphReportsTimeout ensures the rewrite is bounded by the
// caller's context: it runs inline on the request path, so a mirror with a huge
// history must not hold the request open indefinitely.
func TestRewriteCommitGraphReportsTimeout(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testRepoConfig())
	buildStaleCommitGraph(t, repo, upstreamPath)

	expired, cancel := context.WithTimeout(ctx, time.Nanosecond)
	defer cancel()

	err := repo.RewriteCommitGraph(expired)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "commit-graph") || strings.Contains(err.Error(), "context"),
		"expected a commit-graph or context error, got %v", err)
}
