package gitclone //nolint:testpackage // white-box testing required for unexported fields

import (
	"context"
	"os"
	"os/exec"
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
		output, err := exec.Command("git", "init", "--bare", path).CombinedOutput()
		assert.NoError(t, err, string(output))
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

	repo.metaMu.Lock()
	repo.lastServed = time.Now().Add(-2 * time.Hour)
	repo.metaMu.Unlock()

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
	repo.metaMu.Lock()
	repo.lastServed = time.Now().Add(-2 * time.Hour)
	repo.metaMu.Unlock()

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

func TestEvictForQuotaReportsReclaimAlongsideError(t *testing.T) {
	ctx, manager, create := newMirrorManager(t)
	repo := create("https://github.com/org/reclaimed")
	assert.NoError(t, manager.MarkServed(repo))

	// Break the post-eviction bookkeeping flush by putting a directory where the
	// record file goes, so the rename onto it fails. A quota caller subtracts
	// what this returns from its running total, so the reclaim must still be
	// reported: the bytes are gone, and discarding that would evict more mirrors
	// to reach a target already met.
	record := filepath.Join(manager.config.MirrorRoot, lastServedFile)
	assert.NoError(t, os.Remove(record))
	assert.NoError(t, os.Mkdir(record, 0o750))

	result, err := manager.EvictForQuota(ctx, repo)
	assert.Error(t, err)
	assert.Equal(t, EvictResult{Unlinked: true, Reclaimed: true}, result,
		"a failure after the delete must not cast doubt on the reclaim")

	_, statErr := os.Stat(repo.Path())
	assert.True(t, os.IsNotExist(statErr), "mirror directory should be gone")
	assert.Zero(t, manager.Get("https://github.com/org/reclaimed"), "evicted repo should be forgotten")
}

func TestEvictForQuotaReportsUnreclaimedWhenDeleteFails(t *testing.T) {
	ctx, manager, create := newMirrorManager(t)
	repo := create("https://github.com/org/stuck")

	// The mirror is unlinked but its staged copy survives, so the bytes are
	// still on the volume with nothing serving them. A quota caller that
	// subtracted them would stop reclaiming short of its target.
	manager.removeAll = func(string) error { return os.ErrPermission }

	result, err := manager.EvictForQuota(ctx, repo)
	assert.Error(t, err)
	assert.Equal(t, EvictResult{Unlinked: true, Reclaimed: false}, result)
	assert.Zero(t, manager.Get("https://github.com/org/stuck"), "unlinked repo must not still be served")

	var staged []string
	entries, err := os.ReadDir(manager.config.MirrorRoot)
	assert.NoError(t, err)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), cloneTempPrefix) {
			staged = append(staged, entry.Name())
		}
	}
	assert.Equal(t, 1, len(staged), "the undeleted copy should still be on disk")
}

func TestReclaimLeftoversRetriesUndeletedStagedMirror(t *testing.T) {
	ctx, manager, create := newMirrorManager(t)
	repo := create("https://github.com/org/stuck")

	manager.removeAll = func(string) error { return os.ErrPermission }
	result, err := manager.EvictForQuota(ctx, repo)
	assert.Error(t, err)
	assert.False(t, result.Reclaimed)

	remaining, err := manager.ReclaimLeftovers()
	assert.Error(t, err)
	assert.Equal(t, 1, len(remaining), "bytes still on the volume must stay visible to a byte budget")
	_, statErr := os.Stat(remaining[0])
	assert.NoError(t, statErr, "the staged copy should still be there")

	manager.removeAll = os.RemoveAll
	remaining, err = manager.ReclaimLeftovers()
	assert.NoError(t, err)
	assert.Zero(t, len(remaining), "a successful retry stops the bytes being counted")
	assert.Zero(t, len(manager.leftovers), "a reclaimed leftover must not be retried forever")
}

func TestReclaimLeftoversForgetsVanishedStagedMirror(t *testing.T) {
	ctx, manager, create := newMirrorManager(t)
	repo := create("https://github.com/org/vanished")

	manager.removeAll = func(string) error { return os.ErrPermission }
	_, err := manager.EvictForQuota(ctx, repo)
	assert.Error(t, err)

	manager.removeAll = func(path string) error {
		_ = os.RemoveAll(path)
		return os.ErrNotExist
	}
	remaining, err := manager.ReclaimLeftovers()
	assert.NoError(t, err, "an already-gone directory is a reclaim, not a failure")
	assert.Zero(t, len(remaining))
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
	manager.lastServed.mu.Lock()
	manager.lastServed.times[repo.UpstreamURL()] = time.Now().Add(-2 * lastServedGranularity)
	manager.lastServed.mu.Unlock()
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

func TestUnservedMirrorIdleClockSurvivesRestart(t *testing.T) {
	ctx, manager, create := newMirrorManager(t)
	const url = "https://github.com/org/never-served"
	repo := create(url)

	// Deliberately no MarkServed: this mirror exists on disk and no client has
	// ever asked for it. The invented seed must still reach the record, or the
	// next process start invents a new one and the clock never advances.
	assert.NotZero(t, repo.LastServed())
	persisted := manager.lastServed.get(url)
	assert.NotZero(t, persisted, "an unserved mirror's seeded idle clock must be persisted")
	assert.Equal(t, repo.LastServed().Unix(), persisted.Unix())

	// Backdate it to stand in for the mirror having sat unserved for a full
	// idle period. Written directly because record() throttles a value this
	// close to the stored one and would drop it.
	backdated := time.Now().Add(-2 * time.Hour)
	manager.lastServed.mu.Lock()
	manager.lastServed.times[url] = backdated
	assert.NoError(t, manager.lastServed.flushLocked())
	manager.lastServed.mu.Unlock()

	// A new Manager over the same root stands in for a redeploy.
	reloaded, err := NewManager(ctx, Config{MirrorRoot: manager.config.MirrorRoot, MirrorTTL: time.Hour}, nil)
	assert.NoError(t, err)
	again, err := reloaded.GetOrCreate(ctx, url)
	assert.NoError(t, err)
	assert.Equal(t, StateReady, again.State())
	assert.Equal(t, backdated.Unix(), again.LastServed().Unix(), "a restart must not re-seed an unserved mirror")

	idle := reloaded.IdleMirrors(time.Hour)
	assert.Equal(t, 1, len(idle), "an unserved mirror past mirror-ttl should be evictable after a restart")
	assert.Equal(t, url, idle[0].UpstreamURL())
}

func TestSeedLastServedPersistsOnlyExistingMirrors(t *testing.T) {
	_, manager, _ := newMirrorManager(t)
	const onDiskURL = "https://github.com/org/seeded"
	const newURL = "https://github.com/org/not-on-disk"

	seeded, err := manager.seedLastServed(onDiskURL, true)
	assert.NoError(t, err)
	assert.NotZero(t, seeded)
	assert.Equal(t, seeded.Unix(), manager.lastServed.get(onDiskURL).Unix(), "seed must be persisted")

	// Backdate, then seed again: the existing record wins, so an unserved
	// mirror keeps ageing instead of being refreshed on every observation.
	backdated := time.Now().Add(-3 * time.Hour)
	manager.lastServed.mu.Lock()
	manager.lastServed.times[onDiskURL] = backdated
	manager.lastServed.mu.Unlock()
	existing, err := manager.seedLastServed(onDiskURL, true)
	assert.NoError(t, err)
	assert.Equal(t, backdated.Unix(), existing.Unix())

	// A repository with nothing on disk is about to be served, and MarkServed
	// records that, so seeding leaves nothing behind.
	unseeded, err := manager.seedLastServed(newURL, false)
	assert.NoError(t, err)
	assert.NotZero(t, unseeded)
	assert.Zero(t, manager.lastServed.get(newURL))
}

func TestDiscoverExistingPersistsSeedsForEveryMirror(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	root := t.TempDir()
	upstream := createBareRepo(t, t.TempDir())

	urls := []string{"https://github.com/org/first", "https://github.com/org/second"}
	for _, url := range urls {
		path := filepath.Join(root, "github.com", "org", filepath.Base(url))
		assert.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
		assert.NoError(t, exec.Command("git", "clone", "--bare", upstream, path).Run())
	}

	manager, err := NewManager(ctx, Config{MirrorRoot: root, MirrorTTL: time.Hour}, nil)
	assert.NoError(t, err)
	discovered, err := manager.DiscoverExisting(ctx)
	assert.NoError(t, err)
	assert.Equal(t, len(urls), len(discovered))

	for _, repo := range discovered {
		persisted := loadLastServedStore(root).get(repo.UpstreamURL())
		assert.NotZero(t, persisted, "discovered mirror %s has no persisted idle clock", repo.UpstreamURL())
		assert.Equal(t, repo.LastServed().Unix(), persisted.Unix())
	}

	// Backdate the record to stand in for the mirrors having sat unserved for a
	// full idle period, then discover again as a redeploy would.
	backdated := time.Now().Add(-3 * time.Hour)
	store := loadLastServedStore(root)
	store.mu.Lock()
	for _, url := range urls {
		store.times[url] = backdated
	}
	assert.NoError(t, store.flushLocked())
	store.mu.Unlock()

	reloaded, err := NewManager(ctx, Config{MirrorRoot: root, MirrorTTL: time.Hour}, nil)
	assert.NoError(t, err)
	rediscovered, err := reloaded.DiscoverExisting(ctx)
	assert.NoError(t, err)
	assert.Equal(t, len(urls), len(rediscovered))
	for _, repo := range rediscovered {
		assert.Equal(t, backdated.Unix(), repo.LastServed().Unix(),
			"a restart re-seeded %s instead of keeping its recorded time", repo.UpstreamURL())
	}
	assert.Equal(t, len(urls), len(reloaded.IdleMirrors(time.Hour)),
		"unserved mirrors past mirror-ttl should be evictable after a restart")
}

func TestDiscoverExistingPersistsSeedsWhenWalkFails(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	root := t.TempDir()
	upstream := createBareRepo(t, t.TempDir())

	const reachedURL = "https://aaa.example.com/org/mirror"
	reached := filepath.Join(root, "aaa.example.com", "org", "mirror")
	assert.NoError(t, os.MkdirAll(filepath.Dir(reached), 0o750))
	assert.NoError(t, exec.Command("git", "clone", "--bare", upstream, reached).Run())

	// Walked after the mirror above, and looks like a mirror without being a
	// git repository, so configuring it fails and aborts the walk once a mirror
	// has already been registered.
	broken := filepath.Join(root, "zzz.example.com", "org", "mirror")
	assert.NoError(t, os.MkdirAll(broken, 0o750))
	assert.NoError(t, os.WriteFile(filepath.Join(broken, "HEAD"), []byte("ref: refs/heads/main\n"), 0o600))

	manager, err := NewManager(ctx, Config{MirrorRoot: root, MirrorTTL: time.Hour}, nil)
	assert.NoError(t, err)
	_, err = manager.DiscoverExisting(ctx)
	assert.Error(t, err, "a mirror that cannot be configured should fail warm-up")

	assert.NotZero(t, manager.Get(reachedURL), "the mirror reached before the failure should be registered")
	assert.NotZero(t, loadLastServedStore(root).get(reachedURL),
		"a mirror registered before the walk failed must keep a persisted idle clock")
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

func TestSeedMissingRollsBackWhenPersistFails(t *testing.T) {
	store := loadLastServedStore(filepath.Join(t.TempDir(), "missing-dir"))
	const url = "https://github.com/org/repo"

	assert.Error(t, store.seedMissing([]string{url}, time.Now()))
	assert.Zero(t, store.get(url))

	wrote, err := store.record(url, time.Now())
	assert.Error(t, err)
	assert.False(t, wrote)
	assert.Zero(t, store.get(url), "an unwritten record must not throttle the next write")
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
