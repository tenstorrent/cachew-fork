package git

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/metrics"
)

func TestRepoLabelForUpstream(t *testing.T) {
	for _, tt := range []struct{ upstream, want string }{
		{"https://github.com/tenstorrent/internal-cachew", "github.com/tenstorrent/internal-cachew"},
		{"http://ghe.example.com/org/repo", "ghe.example.com/org/repo"},
		{"github.com/org/repo", "github.com/org/repo"},
	} {
		assert.Equal(t, tt.want, repoLabelForUpstream(tt.upstream), "upstream %s", tt.upstream)
	}
}

func TestStorageMetricSeriesNames(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})

	// metrics.New sets the global meter provider; restore it or later tests
	// bind to one this test closes.
	prevProvider := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })

	// Must precede newStorageMetrics: instruments bind at construction to
	// whatever provider is global then.
	client, err := metrics.New(ctx, metrics.Config{
		ServiceName:      "cachew-storage-test",
		Port:             0,
		EnablePrometheus: true,
	})
	assert.NoError(t, err)
	defer client.Close()

	sm := newStorageMetrics()
	sm.bytes.Record(ctx, 4096, metric.WithAttributes(attribute.String("component", storageComponentMirrors)))
	sm.bytes.Record(ctx, 8192, metric.WithAttributes(attribute.String("component", storageComponentCache)))
	sm.capacity.Record(ctx, 16384, metric.WithAttributes(attribute.String("component", storageComponentCache)))
	sm.repoBytes.Record(ctx, 4096, metric.WithAttributes(attribute.String("repository", "github.com/org/repo")))
	sm.seenRepos = map[string]struct{}{"github.com/org/repo": {}}

	scrape := func() string {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		w := httptest.NewRecorder()
		client.Handler().ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		return w.Body.String()
	}

	body := scrape()
	for _, want := range []string{
		`cachew_storage_bytes{component="git_mirrors"`,
		`cachew_storage_bytes{component="object_cache"`,
		`cachew_storage_capacity_bytes{component="object_cache"`,
		`cachew_storage_repo_bytes{`,
		`repository="github.com/org/repo"`,
	} {
		assert.Contains(t, body, want)
	}

	// Int64Gauge never expires a series, so a departed repo needs an explicit
	// 0 or it reports its last size forever.
	sm.zeroDepartedRepos(ctx, map[string]struct{}{})
	assert.Contains(t, scrape(),
		`cachew_storage_repo_bytes{otel_scope_name="cachew.storage",otel_scope_schema_url="",otel_scope_version="",repository="github.com/org/repo"} 0`)
}

func TestDirSizeBytes(t *testing.T) {
	root := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(root, "a"), make([]byte, 100), 0o600))
	assert.NoError(t, os.MkdirAll(filepath.Join(root, "sub"), 0o700))
	assert.NoError(t, os.WriteFile(filepath.Join(root, "sub", "b"), make([]byte, 50), 0o600))
	assert.NoError(t, os.Symlink(filepath.Join(root, "a"), filepath.Join(root, "link")))

	size, err := dirSizeBytes(root)
	assert.NoError(t, err)
	// Symlink excluded: not a regular file, so it contributes 0 despite
	// pointing at "a".
	assert.Equal(t, int64(150), size)
}

func TestDirSizeBytesMissingRoot(t *testing.T) {
	// WalkDir's callback swallows every error, including root-not-found, so
	// this is 0 bytes, not an error — sampleStorageUsage relies on that.
	size, err := dirSizeBytes(filepath.Join(t.TempDir(), "does-not-exist"))
	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)
}

// The budget must not give uncounted mirror bytes to the object cache, so a
// walk that skipped bytes still on disk must report a short count.
func TestMeasureDirSizeBytesReportsUnreadableSubtree(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores permission bits, so the subtree stays readable")
	}
	root := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(root, "counted"), make([]byte, 100), 0o600))
	locked := filepath.Join(root, "locked")
	assert.NoError(t, os.MkdirAll(locked, 0o700))
	assert.NoError(t, os.WriteFile(filepath.Join(locked, "hidden"), make([]byte, 50), 0o600))
	assert.NoError(t, os.Chmod(locked, 0o000))
	t.Cleanup(func() { assert.NoError(t, os.Chmod(locked, 0o700)) })

	total, exact, err := measureDirSizeBytes(root)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), total, "readable files should still be counted")
	assert.False(t, exact, "bytes left on disk but uncounted must be reported")
}

// Repacks and evictions delete files under a walk in progress, and those bytes
// really are gone, so treating a vanished entry as a gap would suppress
// enforcement on every pass.
func TestMeasureDirSizeBytesTreatsVanishedEntriesAsExact(t *testing.T) {
	root := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(root, "a"), make([]byte, 100), 0o600))

	total, exact, err := measureDirSizeBytes(root)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), total)
	assert.True(t, exact)

	total, exact, err = measureDirSizeBytes(filepath.Join(t.TempDir(), "does-not-exist"))
	assert.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.True(t, exact, "a missing root is a measurement of nothing, not a failed measurement")
}

// DiscoverExisting runs `git config` against each discovered path, so a
// plain directory with a HEAD file isn't enough — these must be real bare
// repos. Needs `git`, not `git-lfs`.
func newDiscoveredMirrorManager(ctx context.Context, t *testing.T, n int) *gitclone.Manager {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}
	mirrorRoot := t.TempDir()
	for i := range n {
		dir := filepath.Join(mirrorRoot, "github.com", "org", fmt.Sprintf("repo%d", i))
		assert.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o700))
		// dir is built from t.TempDir() and a fixed "repo%d" pattern, not
		// external input, and exec.Command passes it as a literal argv entry
		// rather than through a shell.
		cmd := exec.CommandContext(ctx, "git", "init", "--bare", dir)
		out, err := cmd.CombinedOutput()
		assert.NoError(t, err, "git init --bare: %s", out)
		assert.NoError(t, os.WriteFile(filepath.Join(dir, "data"), make([]byte, 1024), 0o600))
	}
	cmProvider := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	cm, err := cmProvider()
	assert.NoError(t, err)
	_, err = cm.DiscoverExisting(ctx)
	assert.NoError(t, err)
	assert.Equal(t, n, len(cm.All()))
	return cm
}

func TestSampleStorageUsageSkipsScanWhenAlreadyCancelled(t *testing.T) {
	_, baseCtx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(baseCtx, t, 3)

	ctx, cancel := context.WithCancel(baseCtx)
	cancel()

	s := &Strategy{
		ctx:          ctx,
		cloneManager: cm,
		cache:        cache.NoOpCache(),
		storage:      newStorageMetrics(),
	}

	// The per-repo ctx.Err() check exits before the first Record, so nothing
	// here should be recorded.
	s.sampleStorageUsage(ctx)
	assert.Equal(t, 0, len(s.storage.seenRepos))
}

// The sampler starts before warm-up, so an early pass sees a manager that has
// not finished discovering its mirrors, and a measurement of nothing counts as
// complete. Enforcing then would give the object cache the whole volume.
func TestSampleStorageUsageWaitsForReadiness(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(ctx, t, 2)
	c := newBudgetDiskCacheWithLimit(ctx, t, budgetCacheLimitMB)

	s := &Strategy{
		ctx:          ctx,
		cloneManager: cm,
		cache:        c,
		storage:      newStorageMetrics(),
		config:       Config{StorageLimitMB: budgetCacheLimitMB / 2},
	}

	configured := int64(budgetCacheLimitMB) * mib
	s.sampleStorageUsage(ctx)
	assert.Equal(t, configured, c.SizeLimit(), "the budget was applied before the mirrors were all discovered")

	s.ready.Store(true)
	s.sampleStorageUsage(ctx)
	assert.True(t, c.SizeLimit() < configured, "a ready strategy should enforce the shared budget")
}

func TestSampleStorageUsageRecordsRepoSizes(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(ctx, t, 2)

	s := &Strategy{
		ctx:          ctx,
		cloneManager: cm,
		cache:        cache.NoOpCache(),
		storage:      newStorageMetrics(),
	}

	s.sampleStorageUsage(ctx)
	assert.Equal(t, 2, len(s.storage.seenRepos))
	for _, repo := range cm.All() {
		name := repoLabelForUpstream(repo.UpstreamURL())
		_, tracked := s.storage.seenRepos[name]
		assert.True(t, tracked, "expected %s to be tracked after a scan", name)
	}
}
