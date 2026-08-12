package gitclone //nolint:testpackage // white-box testing required for unexported fields

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

// requireGitAtLeast skips the test when the git on PATH is older than the given
// version. The container image and CI both ship a current git; a developer
// machine or a distribution with a long support window may not, and a test that
// exercises a flag their git has never heard of tells them nothing.
func requireGitAtLeast(t *testing.T, major, minor int) {
	t.Helper()
	out, err := exec.Command("git", "--version").Output()
	assert.NoError(t, err)

	fields := strings.Fields(string(out))
	assert.True(t, len(fields) >= 3, "unexpected git --version output: "+string(out)) //nolint:mnd // "git version X.Y.Z"
	parts := strings.Split(fields[2], ".")
	assert.True(t, len(parts) >= 2, "unexpected git version: "+fields[2]) //nolint:mnd // at least X.Y

	haveMajor, err := strconv.Atoi(parts[0])
	assert.NoError(t, err)
	haveMinor, err := strconv.Atoi(parts[1])
	assert.NoError(t, err)

	if haveMajor < major || (haveMajor == major && haveMinor < minor) {
		t.Skipf("git %s predates %d.%d", fields[2], major, minor)
	}
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

func looseObjectFilePaths(t *testing.T, repoPath string) []string {
	t.Helper()
	shards, err := filepath.Glob(filepath.Join(repoPath, "objects", "??"))
	assert.NoError(t, err)
	var paths []string
	for _, shard := range shards {
		entries, err := os.ReadDir(shard)
		assert.NoError(t, err)
		for _, entry := range entries {
			paths = append(paths, filepath.Join(shard, entry.Name()))
		}
	}
	return paths
}

func looseObjectCount(t *testing.T, repoPath string) int {
	t.Helper()
	return len(looseObjectFilePaths(t, repoPath))
}

func cruftPacks(t *testing.T, repoPath string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.mtimes"))
	assert.NoError(t, err)
	return matches
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

func TestPruneUnreachableReclaimsObjectOrphanedFromAnOldPack(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(24*time.Hour))

	packMirror(t, repo.path)
	backdate(t, packFiles(t, repo.path), time.Now().Add(-90*24*time.Hour))
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	_, err := repo.PruneUnreachable(ctx)
	assert.NoError(t, err)

	assert.False(t, objectExists(t, repo.path, orphaned),
		"grace is measured from the mtime of the pack the object came out of, which here predates the cutoff")
}

func TestPruneUnreachableDeletesOnceQuarantineElapses(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(24*time.Hour))

	// Deliberately not backdating the packs: the object has to start inside its
	// grace window for the second prune to be what reclaims it.
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	_, err := repo.PruneUnreachable(ctx)
	assert.NoError(t, err)
	assert.True(t, objectExists(t, repo.path, orphaned), "the first prune should quarantine the object, not delete it")
	assert.True(t, len(cruftPacks(t, repo.path)) > 0, "the quarantine should be a cruft pack")

	time.Sleep(1100 * time.Millisecond)
	repo.config.PruneGrace = time.Millisecond

	_, err = repo.PruneUnreachable(ctx)
	assert.NoError(t, err)

	assert.False(t, objectExists(t, repo.path, orphaned),
		"an object quarantined for longer than the grace window should be deleted")
}

func TestPruneUnreachableQuarantinesWithoutLooseningObjects(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(24*time.Hour))
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	_, err := repo.PruneUnreachable(ctx)
	assert.NoError(t, err)

	assert.True(t, objectExists(t, repo.path, orphaned), "the object is inside the grace window")
	assert.Equal(t, 0, looseObjectCount(t, repo.path),
		"a quarantined object belongs in a cruft pack, not in a loose file of its own")
	assert.True(t, len(cruftPacks(t, repo.path)) > 0, "retaining an unreachable object should write a cruft pack")
}

func TestPruneRepackArgs(t *testing.T) {
	cutoff := "2026-01-01T00:00:00Z"
	expiring := []string{"-d", "--cruft", "--cruft-expiration=" + cutoff, "--write-midx", "--write-bitmap-index"}

	for _, test := range []struct {
		name           string
		maxCruftSizeMB int
		expire         bool
		expected       []string
		reason         string
	}{
		{
			name: "ExpiresWithoutASizeLimit", maxCruftSizeMB: 0, expire: true,
			expected: expiring,
			reason:   "an unset size limit leaves git to write a single cruft pack",
		},
		{
			name: "TakesANegativeSizeAsNoLimit", maxCruftSizeMB: -1, expire: true,
			expected: expiring,
			reason:   "a negative size is not a size",
		},
		{
			name: "ExpiresWithASizeLimit", maxCruftSizeMB: 2, expire: true,
			expected: append(append([]string{}, expiring...), "--max-cruft-size=2097152"),
			reason:   "the limit reaches git in bytes",
		},
		{
			name: "CombinesInsteadOfExpiring", maxCruftSizeMB: 2, expire: false,
			expected: []string{
				"-d", "--cruft", "--combine-cruft-below-size=2097152",
				"--write-midx", "--write-bitmap-index",
			},
			reason: "git only leaves a mature cruft pack alone when no expiration is asked for",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, pruneRepackArgs(cutoff, test.maxCruftSizeMB, test.expire), test.reason)
		})
	}
}

func TestCruftExpireDue(t *testing.T) {
	for _, test := range []struct {
		name                string
		maxCruftSizeMB      int
		pruneExpireInterval time.Duration
		sinceLastExpire     time.Duration
		expected            bool
	}{
		{
			name: "DefaultsToExpiringEveryPrune", expected: true,
		},
		{
			name: "ExpiresWhenOnlyTheSizeIsSet", maxCruftSizeMB: 1, sinceLastExpire: time.Minute,
			expected: true,
		},
		{
			name: "ExpiresWhenOnlyTheIntervalIsSet", pruneExpireInterval: time.Hour, sinceLastExpire: time.Minute,
			expected: true,
		},
		{
			name: "CombinesWithinTheInterval", maxCruftSizeMB: 1, pruneExpireInterval: time.Hour,
			sinceLastExpire: time.Minute, expected: false,
		},
		{
			name: "ExpiresOnceTheIntervalElapses", maxCruftSizeMB: 1, pruneExpireInterval: time.Hour,
			sinceLastExpire: 2 * time.Hour, expected: true,
		},
		{
			name: "ExpiresBeforeItHasEverExpired", maxCruftSizeMB: 1, pruneExpireInterval: time.Hour,
			expected: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := &Repository{config: Config{
				MaxCruftSizeMB:      test.maxCruftSizeMB,
				PruneExpireInterval: test.pruneExpireInterval,
			}}
			if test.sinceLastExpire > 0 {
				repo.lastCruftExpireAt = time.Now().Add(-test.sinceLastExpire)
			}
			assert.Equal(t, test.expected, repo.cruftExpireDue())
		})
	}
}

func TestPruneUnreachableCombinesUntilTheExpireIntervalElapses(t *testing.T) {
	requireGitAtLeast(t, 2, 50) //nolint:mnd // git 2.50 added --combine-cruft-below-size
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	cfg := testPruneConfig(24 * time.Hour)
	cfg.MaxCruftSizeMB = 1
	cfg.PruneExpireInterval = time.Hour
	repo, upstreamPath := newClonedTestRepo(t, cfg)

	packMirror(t, repo.path)
	backdate(t, packFiles(t, repo.path), time.Now().Add(-90*24*time.Hour))
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	// The object is past its grace window from the outset, so only the cadence
	// decides whether it survives. Without this the first prune would expire it:
	// a repository that has never expired in this process is due.
	repo.lastCruftExpireAt = time.Now()

	_, err := repo.PruneUnreachable(ctx)
	assert.NoError(t, err)
	assert.True(t, objectExists(t, repo.path, orphaned),
		"a prune inside prune-expire-interval should combine cruft packs, not delete from them")
	assert.True(t, len(cruftPacks(t, repo.path)) > 0, "the combining prune should still quarantine the object")
	assert.Equal(t, 0, looseObjectCount(t, repo.path), "combining should not loosen anything either")

	repo.lastCruftExpireAt = time.Now().Add(-2 * time.Hour)

	_, err = repo.PruneUnreachable(ctx)
	assert.NoError(t, err)
	assert.False(t, objectExists(t, repo.path, orphaned),
		"the first prune past prune-expire-interval should delete what is past prune-grace")
}

func TestPruneUnreachableQuarantinesWithAMaxCruftSize(t *testing.T) {
	requireGitAtLeast(t, 2, 43) //nolint:mnd // git 2.43 added --max-cruft-size
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	cfg := testPruneConfig(24 * time.Hour)
	cfg.MaxCruftSizeMB = 1
	repo, upstreamPath := newClonedTestRepo(t, cfg)
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	_, err := repo.PruneUnreachable(ctx)
	assert.NoError(t, err)

	assert.True(t, objectExists(t, repo.path, orphaned), "the object is inside the grace window")
	assert.Equal(t, 0, looseObjectCount(t, repo.path),
		"a quarantined object belongs in a cruft pack, not in a loose file of its own")
	assert.True(t, len(cruftPacks(t, repo.path)) > 0, "retaining an unreachable object should write a cruft pack")
}

func TestPruneUnreachablePacksLooseRefs(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, _ := newClonedTestRepo(t, testPruneConfig(0))

	head := gitOutput(t, repo.path, "rev-parse", "HEAD")
	loosePath := filepath.Join(repo.path, "refs", "heads", "loose-ref")
	assert.NoError(t, os.MkdirAll(filepath.Dir(loosePath), 0o750))
	assert.NoError(t, os.WriteFile(loosePath, []byte(head+"\n"), 0o600))

	_, err := repo.PruneUnreachable(ctx)
	assert.NoError(t, err)

	_, statErr := os.Stat(loosePath)
	assert.True(t, os.IsNotExist(statErr), "prune should have folded the loose ref into packed-refs")

	packed, err := os.ReadFile(filepath.Join(repo.path, "packed-refs"))
	assert.NoError(t, err)
	assert.Contains(t, string(packed), "refs/heads/loose-ref")
}

func TestPruneUnreachableClearsLooseSetLeftByAnEarlierBuild(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(24*time.Hour))
	orphaned := orphanUpstreamHistory(t, repo, upstreamPath)

	// What the earlier build left behind: the unreachable object loose, stamped
	// with the moment it was quarantined, now well past the grace window.
	out, err := exec.Command("git", "-C", repo.path, "repack", "-A", "-d").CombinedOutput() //nolint:gosec // test-owned temp paths
	assert.NoError(t, err, string(out))
	assert.True(t, looseObjectCount(t, repo.path) > 0, "the earlier build's prune loosened the unreachable object")
	backdate(t, looseObjectFilePaths(t, repo.path), time.Now().Add(-25*time.Hour))

	_, err = repo.PruneUnreachable(ctx)
	assert.NoError(t, err)

	assert.False(t, objectExists(t, repo.path, orphaned),
		"an object left loose past its grace window by an earlier build should be deleted")
	assert.Equal(t, 0, looseObjectCount(t, repo.path), "the inherited loose set should be gone")
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

func TestFetchClearsRefLocksLeftByAKilledFetch(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(0))

	// The shape a killed fetch leaves: git is SIGKILLed on the deadline, so the
	// locks it held are still on disk with no process behind them, and every
	// later ref update fails against them until someone removes them.
	packedRefsLock := filepath.Join(repo.path, "packed-refs.lock")
	assert.NoError(t, os.WriteFile(packedRefsLock, nil, 0o600))
	fetchHeadLock := filepath.Join(repo.path, "FETCH_HEAD.lock")
	assert.NoError(t, os.WriteFile(fetchHeadLock, nil, 0o600))
	looseRefLock := filepath.Join(repo.path, "refs", "heads", "abandoned.lock")
	assert.NoError(t, os.MkdirAll(filepath.Dir(looseRefLock), 0o750))
	assert.NoError(t, os.WriteFile(looseRefLock, nil, 0o600))

	// Fail the next fetch immediately rather than waiting out a network timeout.
	assert.NoError(t, os.RemoveAll(upstreamPath))
	assert.Error(t, repo.FetchVerified(ctx), "a fetch from a deleted upstream should fail")

	for _, lock := range []string{packedRefsLock, fetchHeadLock, looseRefLock} {
		_, err := os.Stat(lock)
		assert.True(t, os.IsNotExist(err), "a failed fetch left the stale lock %s behind", lock)
	}
}

func TestFetchLeavesRefLocksAloneWhenMaintenanceIsEnabled(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(0))
	// Set after the clone so the test does not register the temp repo with the
	// host's git maintenance. Scheduled maintenance runs pack-refs and prefetch
	// out of process, so a lock may belong to a live run rather than a killed
	// one.
	repo.config.Maintenance = true

	packedRefsLock := filepath.Join(repo.path, "packed-refs.lock")
	assert.NoError(t, os.WriteFile(packedRefsLock, nil, 0o600))

	assert.NoError(t, os.RemoveAll(upstreamPath))
	assert.Error(t, repo.FetchVerified(ctx), "a fetch from a deleted upstream should fail")

	_, err := os.Stat(packedRefsLock)
	assert.NoError(t, err, "a failed fetch removed a lock scheduled maintenance could be holding")
}

func TestSuccessfulFetchKeepsRefsIntact(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	repo, upstreamPath := newClonedTestRepo(t, testPruneConfig(0))

	advanceBareUpstream(t, upstreamPath)
	assert.NoError(t, repo.FetchVerified(ctx))

	// Lock cleanup is on the failure path only, so a healthy fetch is unchanged.
	head := gitOutput(t, upstreamPath, "rev-parse", "HEAD")
	assert.True(t, objectExists(t, repo.path, head), "the fetch should have brought the new commit in")
}
