package git_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/snapshot"
	"github.com/tenstorrent/internal-cachew/internal/strategy/git"
)

// TestMirrorSnapshotExcludesCommitGraph pins the fix for the incident where a
// mirror snapshot shipped a commit-graph naming commits a prune had deleted, so
// every client that seeded from it failed with "Could not read <oid>" until it
// ran git gc.
func TestMirrorSnapshotExcludesCommitGraph(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	// A bare mirror carrying the commit-graph a real mirror would have, plus the
	// object paths that must survive.
	mirrorDir := t.TempDir()
	for _, path := range []string{
		"objects/info/commit-graphs/graph-abc.graph",
		"objects/info/commit-graph",
		"objects/pack/pack-abc.pack",
		"objects/ab/cdef",
		"refs/heads/main",
	} {
		full := filepath.Join(mirrorDir, path)
		assert.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
		assert.NoError(t, os.WriteFile(full, []byte("x"), 0o600))
	}

	// The graph is derived state, so it must not be archived; the objects that
	// make up the mirror must be.
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()

	key := cache.Key{9, 9, 9}
	// Archiving into "." from the mirror root is how generateAndUploadMirrorSnapshot
	// calls this, and it is what makes the "./objects/..." patterns line up.
	err = snapshot.Create(ctx, mem, key, mirrorDir, time.Hour, git.SnapshotExcludePatterns("."), 0)
	assert.NoError(t, err)

	restoredDir := t.TempDir()
	assert.NoError(t, snapshot.Restore(ctx, mem, key, restoredDir, 0))

	for _, path := range []string{"objects/info/commit-graphs", "objects/info/commit-graph"} {
		_, statErr := os.Stat(filepath.Join(restoredDir, path))
		assert.IsError(t, statErr, os.ErrNotExist)
	}
	for _, path := range []string{"objects/pack/pack-abc.pack", "objects/ab/cdef", "refs/heads/main"} {
		_, statErr := os.Stat(filepath.Join(restoredDir, path))
		assert.NoError(t, statErr)
	}
}

// TestMirrorSnapshotIsUsableAfterRestore is the end-to-end guard: a restored
// mirror must still be a healthy repository, which is what the stale graph broke.
func TestMirrorSnapshotIsUsableAfterRestore(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	gitBin, err := exec.LookPath("git")
	assert.NoError(t, err)

	mirrorDir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		out, runErr := exec.Command(gitBin, append([]string{"-C", mirrorDir}, args...)...).CombinedOutput()
		assert.NoError(t, runErr, string(out))
	}
	run("init", "--bare", "-q")
	run("-c", "user.email=t@e.st", "-c", "user.name=t", "commit-tree",
		"4b825dc642cb6eb9a060e54bf8d69288fbee4904", "-m", "root")
	// A commit-graph written here indexes that commit.
	run("commit-graph", "write", "--reachable")

	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()

	key := cache.Key{7, 7, 7}
	assert.NoError(t, snapshot.Create(ctx, mem, key, mirrorDir, time.Hour, git.SnapshotExcludePatterns("."), 0))

	restoredDir := t.TempDir()
	assert.NoError(t, snapshot.Restore(ctx, mem, key, restoredDir, 0))

	// fsck resolves every ref and object, and is what reported the corruption
	// on a poisoned snapshot.
	out, err := exec.Command(gitBin, "-C", restoredDir, "fsck", "--connectivity-only").CombinedOutput()
	assert.NoError(t, err, string(out))
	assert.NotContains(t, string(out), "commit-graph")
}
