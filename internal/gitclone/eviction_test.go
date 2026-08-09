package gitclone //nolint:testpackage // white-box testing required for unexported fields

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/logging"
)

// newMirrorManager returns a Manager over an empty mirror root, plus a helper
// that materialises a repository the way DiscoverExisting recognises one: a
// directory containing a HEAD file.
func newMirrorManager(t *testing.T) (context.Context, *Manager, func(upstreamURL string) *Repository) {
	t.Helper()
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	manager, err := NewManager(ctx, Config{MirrorRoot: t.TempDir(), MirrorTTL: time.Hour}, nil)
	assert.NoError(t, err)

	create := func(upstreamURL string) *Repository {
		t.Helper()
		path, err := manager.clonePathForURL(upstreamURL)
		assert.NoError(t, err)
		assert.NoError(t, os.MkdirAll(path, 0o750))
		assert.NoError(t, os.WriteFile(filepath.Join(path, "HEAD"), []byte("ref: refs/heads/main\n"), 0o600))
		repo, err := manager.GetOrCreate(ctx, upstreamURL)
		assert.NoError(t, err)
		assert.Equal(t, StateReady, repo.State())
		return repo
	}
	return ctx, manager, create
}

func TestEvictRemovesIdleMirror(t *testing.T) {
	ctx, manager, create := newMirrorManager(t)
	repo := create("https://github.com/org/idle")

	repo.mu.Lock()
	repo.lastServed = time.Now().Add(-2 * time.Hour)
	repo.mu.Unlock()

	idle := manager.IdleMirrors(time.Hour)
	assert.Equal(t, 1, len(idle))

	evicted, err := manager.Evict(ctx, repo, time.Hour)
	assert.NoError(t, err)
	assert.True(t, evicted)

	_, statErr := os.Stat(repo.Path())
	assert.True(t, os.IsNotExist(statErr), "mirror directory should be gone")
	assert.Zero(t, manager.Get("https://github.com/org/idle"), "evicted repo should be forgotten")

	// No staging directory may survive. DiscoverExisting would clean one up on
	// the next boot, but leaking a full mirror copy until then defeats the point.
	entries, err := os.ReadDir(manager.config.MirrorRoot)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.False(t, strings.HasPrefix(entry.Name(), cloneTempPrefix),
			"eviction left a staging directory behind: %s", entry.Name())
	}
}

func TestEvictDeclinesWhileServing(t *testing.T) {
	ctx, manager, create := newMirrorManager(t)
	repo := create("https://github.com/org/busy")
	repo.mu.Lock()
	repo.lastServed = time.Now().Add(-2 * time.Hour)
	repo.mu.Unlock()

	assert.True(t, repo.BeginServe(), "serve should be admitted before eviction")

	evicted, err := manager.Evict(ctx, repo, time.Hour)
	assert.NoError(t, err)
	assert.False(t, evicted, "a mirror with a request in flight must not be evicted")
	_, statErr := os.Stat(repo.Path())
	assert.NoError(t, statErr, "mirror should still exist")

	// Once the request finishes the mirror becomes evictable again, and the
	// admission gate must have been reopened by the declined attempt.
	repo.EndServe()
	evicted, err = manager.Evict(ctx, repo, time.Hour)
	assert.NoError(t, err)
	assert.True(t, evicted, "mirror should be evictable once the request completes")
}

func TestBeginServeRejectedDuringEviction(t *testing.T) {
	_, _, create := newMirrorManager(t)
	repo := create("https://github.com/org/evicting")

	assert.True(t, repo.beginEvict(time.Now().Add(time.Hour)), "idle repo should be claimable")
	assert.False(t, repo.BeginServe(), "requests must be turned away while evicting")

	repo.abortEvict()
	assert.True(t, repo.BeginServe(), "admission should reopen when eviction is abandoned")
	repo.EndServe()
}

func TestIdleMirrorsIgnoresRecentlyServed(t *testing.T) {
	_, manager, create := newMirrorManager(t)
	fresh := create("https://github.com/org/fresh")
	stale := create("https://github.com/org/stale")

	stale.mu.Lock()
	stale.lastServed = time.Now().Add(-2 * time.Hour)
	stale.mu.Unlock()

	idle := manager.IdleMirrors(time.Hour)
	assert.Equal(t, 1, len(idle))
	assert.Equal(t, stale.UpstreamURL(), idle[0].UpstreamURL())

	// A zero TTL is the documented kill switch and must never select anything.
	assert.Zero(t, len(manager.IdleMirrors(0)))
	assert.NotZero(t, fresh.LastServed())
}

func TestMarkServedSurvivesReload(t *testing.T) {
	ctx, manager, create := newMirrorManager(t)
	repo := create("https://github.com/org/persisted")
	assert.NoError(t, manager.MarkServed(repo))

	// A new Manager over the same root stands in for a pod restart: without
	// the on-disk record every mirror would look freshly served and the idle
	// clock would restart on every deploy.
	reloaded, err := NewManager(ctx, Config{MirrorRoot: manager.config.MirrorRoot, MirrorTTL: time.Hour}, nil)
	assert.NoError(t, err)
	again, err := reloaded.GetOrCreate(ctx, "https://github.com/org/persisted")
	assert.NoError(t, err)
	assert.Equal(t, repo.LastServed().Unix(), again.LastServed().Unix())
}

func TestMarkServedThrottlesWrites(t *testing.T) {
	_, manager, _ := newMirrorManager(t)
	const url = "https://github.com/org/chatty"

	wrote, err := manager.lastServed.record(url, time.Now())
	assert.NoError(t, err)
	assert.True(t, wrote, "first observation should be recorded")

	wrote, err = manager.lastServed.record(url, time.Now())
	assert.NoError(t, err)
	assert.False(t, wrote, "a second observation within the granularity should not rewrite the file")

	wrote, err = manager.lastServed.record(url, time.Now().Add(2*lastServedGranularity))
	assert.NoError(t, err)
	assert.True(t, wrote, "an observation past the granularity should be recorded")
}

func TestLastServedStoreToleratesCorruptFile(t *testing.T) {
	root := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(root, lastServedFile), []byte("{not json"), 0o600))

	// Failing closed here would make every mirror look ancient and evict the
	// entire cache on the next scan, so a corrupt record must read as empty.
	store := loadLastServedStore(root)
	assert.Zero(t, store.get("https://github.com/org/repo"))

	wrote, err := store.record("https://github.com/org/repo", time.Now())
	assert.NoError(t, err)
	assert.True(t, wrote)
	assert.NotZero(t, loadLastServedStore(root).get("https://github.com/org/repo"))
}
