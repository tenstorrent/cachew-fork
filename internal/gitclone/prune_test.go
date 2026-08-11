package gitclone //nolint:testpackage // white-box testing required for unexported fields

import (
	"context"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/logging"
)

func testPruneConfig(grace time.Duration) Config {
	cfg := testRepoConfig()
	cfg.PruneTimeout = 5 * time.Minute
	cfg.PruneGrace = grace
	return cfg
}

// orphanUpstreamHistory force-pushes an unrelated history over the upstream's
// current branch and fetches it into the mirror, leaving the mirror's previous
// HEAD in the object store with nothing pointing at it. This is the shape that
// upstream branch deletion and force-push produce in production.
func orphanUpstreamHistory(t *testing.T, repo *Repository, upstreamPath string) string {
	t.Helper()
	workPath := filepath.Join(filepath.Dir(upstreamPath), "work")

	orphaned := gitOutput(t, workPath, "rev-parse", "HEAD")

	branchOut := gitOutput(t, workPath, "rev-parse", "--abbrev-ref", "HEAD")
	for _, args := range [][]string{
		{"git", "-C", workPath, "checkout", "--orphan", "replacement"},
		{"git", "-C", workPath, "commit", "-m", "replacement", "--allow-empty"},
		{"git", "-C", workPath, "push", "--force", upstreamPath, "replacement:" + branchOut},
	} {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput() //nolint:gosec // test-owned temp paths
		assert.NoError(t, err, string(out))
	}

	out, err := exec.Command("git", "-C", repo.path, "fetch", "--prune", "--prune-tags").CombinedOutput() //nolint:gosec // test-owned temp paths
	assert.NoError(t, err, string(out))

	assert.True(t, objectExists(t, repo.path, orphaned), "orphaned object should still be in the mirror before a prune")
	return orphaned
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).Output() //nolint:gosec // test-owned temp paths
	assert.NoError(t, err)
	return strings.TrimSpace(string(out))
}

func objectExists(t *testing.T, repoPath, sha string) bool {
	t.Helper()
	return exec.Command("git", "-C", repoPath, "cat-file", "-e", sha).Run() == nil //nolint:gosec // test-owned temp paths
}

func packMirror(t *testing.T, repoPath string) {
	t.Helper()
	out, err := exec.Command("git", "-C", repoPath, "repack", "-a", "-d").CombinedOutput() //nolint:gosec // test-owned temp paths
	assert.NoError(t, err, string(out))
}

func packFiles(t *testing.T, repoPath string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*"))
	assert.NoError(t, err)
	assert.True(t, len(matches) > 0, "mirror should have packs to age")
	return matches
}

func looseObjectFiles(t *testing.T, repoPath string) []string {
	t.Helper()
	loose, err := looseObjects(repoPath)
	assert.NoError(t, err)
	assert.True(t, len(loose) > 0, "prune should have quarantined at least one loose object")
	return slices.Collect(maps.Keys(loose))
}

func backdate(t *testing.T, paths []string, when time.Time) {
	t.Helper()
	for _, path := range paths {
		assert.NoError(t, os.Chtimes(path, when, when))
	}
}

func TestNewManagerDefaultsPruneTimeout(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})

	for _, configured := range []time.Duration{0, -time.Minute} {
		manager, err := NewManager(ctx, Config{MirrorRoot: t.TempDir(), PruneTimeout: configured}, nil)
		assert.NoError(t, err)
		assert.Equal(t, time.Hour, manager.config.PruneTimeout,
			"a non-positive prune-timeout would cancel every prune before it started")
	}
}

func TestPruneUnreachableDeletesOrphanedObject(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(0))
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	_, err := repo.PruneUnreachable(ctx)
	assert.NoError(t, err)

	assert.False(t, objectExists(t, repo.path, orphaned),
		"prune should delete an object no ref points at any more")
}

func TestRepackRetainsOrphanedObject(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(0))
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	assert.NoError(t, repo.Repack(ctx))

	assert.True(t, objectExists(t, repo.path, orphaned),
		"geometric repack carries unreachable objects forward, so only a prune reclaims them")
}

func TestPruneUnreachableKeepsObjectWithinGrace(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(24*time.Hour))
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	_, err := repo.PruneUnreachable(ctx)
	assert.NoError(t, err)

	assert.True(t, objectExists(t, repo.path, orphaned),
		"an object orphaned inside the grace window should survive the prune")
}

func TestPruneUnreachableKeepsObjectOrphanedFromAnOldPack(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(24*time.Hour))

	packMirror(t, repo.path)
	backdate(t, packFiles(t, repo.path), time.Now().Add(-90*24*time.Hour))
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	_, err := repo.PruneUnreachable(ctx)
	assert.NoError(t, err)

	assert.True(t, objectExists(t, repo.path, orphaned),
		"an object orphaned moments ago must survive its grace window even though the pack holding it predates it")
}

func TestPruneUnreachableDeletesOnceQuarantineElapses(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(24*time.Hour))

	packMirror(t, repo.path)
	backdate(t, packFiles(t, repo.path), time.Now().Add(-90*24*time.Hour))
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	_, err := repo.PruneUnreachable(ctx)
	assert.NoError(t, err)
	assert.True(t, objectExists(t, repo.path, orphaned), "the first prune should quarantine the object, not delete it")

	// Ageing the quarantined object past the window is how we observe that the
	// clock runs from the quarantine rather than restarting on every prune.
	backdate(t, looseObjectFiles(t, repo.path), time.Now().Add(-25*time.Hour))

	_, err = repo.PruneUnreachable(ctx)
	assert.NoError(t, err)

	assert.False(t, objectExists(t, repo.path, orphaned),
		"an object quarantined for longer than the grace window should be deleted")
}

func TestPruneUnreachableKeepsReachableObjects(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(0))
	orphanUpstreamHistory(t, repo, upstreamPath)

	refs, err := repo.GetLocalRefs(ctx)
	assert.NoError(t, err)
	assert.True(t, len(refs) > 0, "mirror should still have refs to keep alive")

	_, err = repo.PruneUnreachable(ctx)
	assert.NoError(t, err)

	for ref, sha := range refs {
		assert.True(t, objectExists(t, repo.path, sha), "prune deleted %s, which %s points at", sha, ref)
	}
}

func TestPruneUnreachableWaitsForFetchExclusion(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, _ := newClonedTestRepo(t, testPruneConfig(0))

	release := make(chan struct{})
	acquired := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- repo.WithFetchExclusion(ctx, func() error {
			close(acquired)
			<-release
			return nil
		})
	}()
	<-acquired

	// Cancelling while the holder still owns the semaphore is how we observe
	// that the prune waited rather than running alongside it.
	blockedCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err := repo.PruneUnreachable(blockedCtx)
	assert.Error(t, err, "prune should block while another operation holds the fetch exclusion")

	close(release)
	assert.NoError(t, <-holderDone)

	// Once released, the same prune succeeds, so the failure above was the
	// exclusion and not a broken repack invocation.
	_, err = repo.PruneUnreachable(ctx)
	assert.NoError(t, err)
}
