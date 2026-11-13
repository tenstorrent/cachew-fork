package git_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/githubapp"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/strategy/git"
)

// TestPruneJobRepairsStaleCommitGraph covers the ordering behind the incident:
// the prune deletes the objects an upstream ref deletion orphaned, while the
// commit-graph written before it keeps indexing them. The scheduled prune has to
// leave the graph describing only what survived, because nothing else in the
// fetch or repack path ever rebuilds it.
func TestPruneJobRepairsStaleCommitGraph(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	tmpDir := t.TempDir()
	upstreamPath, workPath := newUpstreamWithOrphanableBranch(t, tmpDir)

	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	mux := newTestMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:       mirrorRoot,
		PruneTimeout:     5 * time.Minute,
		PruneGrace:       0,
		FetchTimeout:     5 * time.Minute,
		CloneTimeout:     5 * time.Minute,
		RefCheckInterval: 10 * time.Second,
	}, nil)
	manager, err := cm()
	assert.NoError(t, err)

	repo, err := manager.GetOrCreate(ctx, "https://example.test/org/repo")
	assert.NoError(t, err)
	// GetOrCreate normalises the URL to a mirror path; seed that path as a bare
	// repo pointed at the local upstream so the test needs no network.
	assert.NoError(t, os.MkdirAll(repo.Path(), 0o755))
	gitAt(t, repo.Path(), "init", "--bare", "-q")
	assert.NoError(t, setRemoteURL(t, repo.Path(), upstreamPath))
	// Clone is what normally applies cachew's mirror config, so reproduce the
	// commit-graph settings it installs here.
	gitAt(t, repo.Path(), "config", "core.commitGraph", "true")
	gitAt(t, repo.Path(), "config", "gc.writeCommitGraph", "true")
	gitAt(t, repo.Path(), "config", "fetch.writeCommitGraph", "true")
	// Fetch needs an explicit mirror refspec; Clone normally writes one.
	gitAt(t, repo.Path(), "config", "remote.origin.fetch", "+refs/*:refs/*")
	assert.NoError(t, repo.Fetch(ctx))

	// Index the branch that is about to be deleted, then delete it upstream and
	// fetch with prune, which is how a mirror is left holding a commit-graph that
	// names objects it no longer has.
	gitAt(t, repo.Path(), "commit-graph", "write", "--reachable")
	featureOID := gitAt(t, workPath, "rev-parse", "feature")
	gitAt(t, workPath, "push", "origin", ":feature")
	assert.NoError(t, repo.Fetch(ctx))
	gitAt(t, repo.Path(), "reflog", "expire", "--expire=now", "--all")
	// The fetch above runs with fetch.writeCommitGraph, so it refreshes the graph
	// and the object still exists. Prune is what strands the graph, and the prune
	// job under test is a second one: the first is deliberately run without its
	// graph repair so the test starts from the state production was in.
	gitAt(t, repo.Path(), "-c", "gc.auto=0", "repack", "-a", "-d", "-q")
	gitAt(t, repo.Path(), "-c", "gc.auto=0", "prune", "--expire=now")
	assert.False(t, objectPresentAt(t, repo.Path(), featureOID),
		"the pruned object should be gone from the object store")
	assert.False(t, graphVerifiesAt(t, repo.Path()),
		"test setup must leave a stale commit-graph, otherwise it proves nothing")

	s, err := git.New(ctx, git.Config{
		PruneInterval: time.Hour,
	}, newTestScheduler(ctx, t), nil, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }, nil) //nolint:nilnil
	assert.NoError(t, err)
	waitForReady(t, s)

	assert.NoError(t, s.PruneJob(ctx, repo))

	assert.True(t, graphVerifiesAt(t, repo.Path()),
		"the prune job must not leave a commit-graph indexing the objects it deleted")
	assert.False(t, objectPresentAt(t, repo.Path(), featureOID),
		"repairing the index must not resurrect the pruned object")
}

// newUpstreamWithOrphanableBranch builds a bare upstream plus its work tree. The
// feature branch is an orphan so deleting it leaves a genuinely unreachable
// commit rather than one still reachable from the default branch.
func newUpstreamWithOrphanableBranch(t *testing.T, dir string) (upstreamPath, workPath string) {
	t.Helper()
	workPath = filepath.Join(dir, "work")
	upstreamPath = filepath.Join(dir, "upstream.git")
	assert.NoError(t, os.MkdirAll(workPath, 0o755))

	for _, args := range [][]string{
		{"git", "-C", workPath, "init"},
		{"git", "-C", workPath, "config", "user.email", "test@example.com"},
		{"git", "-C", workPath, "config", "user.name", "Test"},
	} {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput() //nolint:gosec // test-owned temp paths
		assert.NoError(t, err, string(out))
	}
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "f.txt"), []byte("x"), 0o600))
	for _, args := range [][]string{
		{"git", "-C", workPath, "add", "."},
		{"git", "-C", workPath, "commit", "-m", "init"},
		{"git", "clone", "--bare", workPath, upstreamPath},
		{"git", "-C", workPath, "remote", "add", "origin", upstreamPath},
		{"git", "-C", workPath, "push", "origin", "HEAD:main"},
		{"git", "-C", workPath, "checkout", "--orphan", "feature"},
	} {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput() //nolint:gosec // test-owned temp paths
		assert.NoError(t, err, string(out))
	}
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "g.txt"), []byte("feature"), 0o600))
	for _, args := range [][]string{
		{"git", "-C", workPath, "add", "."},
		{"git", "-C", workPath, "commit", "-m", "feature only"},
		{"git", "-C", workPath, "push", "origin", "HEAD:feature"},
	} {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput() //nolint:gosec // test-owned temp paths
		assert.NoError(t, err, string(out))
	}
	return upstreamPath, workPath
}

func gitAt(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput() //nolint:gosec // test-owned temp paths
	assert.NoError(t, err, string(out))
	return string(out)
}

// setRemoteURL points the mirror's origin at the test's local upstream, so the
// clone and fetches need no network.
func setRemoteURL(t *testing.T, repoPath, upstreamPath string) error {
	t.Helper()
	// #nosec G204 - both paths are test-owned temp paths
	out, err := exec.Command("git", "-C", repoPath, "config", "remote.origin.url", upstreamPath).CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "set remote.origin.url: %s", string(out))
	}
	return nil
}

func graphVerifiesAt(t *testing.T, repoPath string) bool {
	t.Helper()
	return exec.Command("git", "-C", repoPath, "commit-graph", "verify").Run() == nil //nolint:gosec // test-owned temp path
}

func objectPresentAt(t *testing.T, repoPath, sha string) bool {
	t.Helper()
	return exec.Command("git", "-C", repoPath, "cat-file", "-e", sha).Run() == nil //nolint:gosec // test-owned temp path
}
