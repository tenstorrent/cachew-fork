package git

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/hcl/v2"
	"go.opentelemetry.io/otel"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/jobscheduler"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/metrics"
)

const mib = 1024 * 1024

// The deployed configmap renders these keys verbatim, and cachewd refuses to
// start on an unknown key, so a typo in an hcl tag takes the service down. This
// pins the wire names.
func TestStorageBudgetConfigKeys(t *testing.T) {
	var cfg Config
	assert.NoError(t, hcl.Unmarshal([]byte(
		"storage-limit-mb = 81920\nstorage-min-cache-mb = 10240\nstorage-scan-interval = \"15m\"\n"), &cfg))
	assert.Equal(t, 81920, cfg.StorageLimitMB)
	assert.Equal(t, 10240, cfg.StorageMinCacheMB)
	assert.Equal(t, 15*time.Minute, cfg.StorageScanInterval)

	// Defaults must leave the budget off, so an upgrade that does not set it
	// behaves as before.
	var defaults Config
	assert.NoError(t, hcl.Unmarshal(nil, &defaults))
	assert.Equal(t, 0, defaults.StorageLimitMB)
	assert.Equal(t, 5120, defaults.StorageMinCacheMB)
}

// The budget arithmetic assumes non-negative inputs and an enforcement pass.
// Configurations that break either leave both components unbounded, which is
// worse than the budget being off, so they are rejected at startup.
func TestStorageBudgetConfigValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		config Config
		err    string
	}{
		{"NegativeBudget", Config{StorageLimitMB: -1, StorageScanInterval: time.Minute},
			"storage-limit-mb must not be negative"},
		{"NegativeFloor", Config{StorageMinCacheMB: -1}, "storage-min-cache-mb must not be negative"},
		{"BudgetWithoutMeasurement", Config{StorageLimitMB: 100}, "requires storage measurement"},
		{"BudgetWithMeasurement", Config{StorageLimitMB: 100, StorageMinCacheMB: 10,
			StorageScanInterval: time.Minute}, ""},
		{"BudgetDisabledNeedsNoMeasurement", Config{StorageMinCacheMB: 5120}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateAndDefaultConfig(test.config)
			if test.err == "" {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
			assert.Contains(t, err.Error(), test.err)
		})
	}
}

// budgetCacheLimitMB is the configured limit in every budget test, so an
// assertion that lands on it is asserting the cap, not the arithmetic.
const budgetCacheLimitMB = 100

// The dashboard queries these names literally, and the Prometheus exporter
// derives them from the instrument's unit as well as its name, so pin them.
func TestMirrorEvictionMetricSeriesNames(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})

	prevProvider := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })

	client, err := metrics.New(ctx, metrics.Config{
		ServiceName:      "cachew-mirror-evict-test",
		Port:             0,
		EnablePrometheus: true,
	})
	assert.NoError(t, err)
	defer client.Close()

	sm := newStorageMetrics()
	sm.recordMirrorEviction(ctx, mirrorEvictReasonIdle, 4096)
	sm.recordMirrorEviction(ctx, mirrorEvictReasonQuota, 8192)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	client.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()

	for _, want := range []string{
		`cachew_storage_mirror_evictions_total{`,
		`cachew_storage_mirror_evicted_bytes_total{`,
		`reason="idle"`,
		`reason="quota"`,
	} {
		assert.Contains(t, body, want, "expected series %q in scrape", want)
	}
}

func newBudgetDiskCache(ctx context.Context, t *testing.T) *cache.Disk {
	t.Helper()
	return newBudgetDiskCacheWithLimit(ctx, t, budgetCacheLimitMB)
}

func newBudgetDiskCacheWithLimit(ctx context.Context, t *testing.T, limitMB int) *cache.Disk {
	t.Helper()
	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:    t.TempDir(),
		LimitMB: limitMB,
		MaxTTL:  time.Hour,
		// Enforcement drives eviction explicitly. A ticker would add only timing
		// noise.
		EvictInterval: time.Hour,
	})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, c.Close()) })
	return c
}

// The object cache gets what the mirrors leave, which is what makes the two
// share one budget instead of two independent limits with an unbounded sum.
func TestEnforceStorageBudgetGivesCacheTheRemainder(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	disk := newBudgetDiskCache(ctx, t)

	s := &Strategy{
		ctx:     ctx,
		cache:   disk,
		storage: newStorageMetrics(),
		config:  Config{StorageLimitMB: 100, StorageMinCacheMB: 10},
	}

	s.enforceStorageBudget(ctx, map[string]int64{}, 30*mib)
	assert.Equal(t, int64(70*mib), disk.SizeLimit(), "cache should get budget minus mirrors")

	// Mirrors grow, and the cache's share shrinks to match.
	s.enforceStorageBudget(ctx, map[string]int64{}, 60*mib)
	assert.Equal(t, int64(40*mib), disk.SizeLimit())
}

// Mirrors take priority, but must not starve the cache. Below the floor every
// request is a cold miss, which defeats the point of the cache.
func TestEnforceStorageBudgetHoldsCacheFloor(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	disk := newBudgetDiskCache(ctx, t)

	s := &Strategy{
		ctx:   ctx,
		cache: disk,
		// Mirrors over the ceiling send enforcement into reclamation. With no
		// mirrors to reclaim it finds nothing, which is the path under test.
		cloneManager: newDiscoveredMirrorManager(ctx, t, 0),
		storage:      newStorageMetrics(),
		config:       Config{StorageLimitMB: 100, StorageMinCacheMB: 10},
	}

	// Mirrors alone exceed the budget, and the cache still keeps its floor.
	s.enforceStorageBudget(ctx, map[string]int64{}, 120*mib)
	assert.Equal(t, int64(10*mib), disk.SizeLimit())
}

// The last resort: with the cache at its floor and the mirrors still over
// budget, mirrors are deleted least recently served first until they fit.
func TestEnforceStorageBudgetReclaimsMirrors(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(ctx, t, 3)

	// Stamp distinct serve times so victim order is deterministic.
	all := cm.All()
	for _, repo := range all {
		assert.NoError(t, cm.MarkServed(repo))
		time.Sleep(5 * time.Millisecond)
	}
	ordered := cm.MirrorsByLastServed()
	oldest, newest := ordered[0], ordered[len(ordered)-1]

	sched, err := jobscheduler.New(logging.ContextWithLogger(t.Context(), logging.FromContext(ctx)),
		jobscheduler.Config{})
	assert.NoError(t, err)
	t.Cleanup(sched.Wait)

	s := &Strategy{
		ctx:          ctx,
		cache:        newBudgetDiskCache(ctx, t),
		cloneManager: cm,
		scheduler:    sched,
		storage:      newStorageMetrics(),
		config:       Config{StorageLimitMB: 100, StorageMinCacheMB: 10},
	}

	// Ceiling is 90 MiB. Three 40 MiB mirrors total 120 MiB, so exactly one
	// eviction brings them to 80 MiB and under.
	sizes := map[string]int64{}
	for _, repo := range all {
		sizes[repo.UpstreamURL()] = 40 * mib
	}
	s.enforceStorageBudget(ctx, sizes, 120*mib)

	assert.Equal(t, 2, len(cm.All()), "exactly one mirror should be reclaimed")
	assert.Zero(t, cm.Get(oldest.UpstreamURL()), "least recently served mirror should be the victim")
	assert.NotZero(t, cm.Get(newest.UpstreamURL()), "most recently served mirror should survive")
}

// A mirror being read must not be deleted under the client, however far over
// budget the mirrors are.
func TestReclaimMirrorsSkipsMirrorBeingServed(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(ctx, t, 1)

	repo := cm.All()[0]
	assert.True(t, repo.BeginServe(), "expected to take a serve reservation")
	defer repo.EndServe()

	sched, err := jobscheduler.New(logging.ContextWithLogger(t.Context(), logging.FromContext(ctx)),
		jobscheduler.Config{})
	assert.NoError(t, err)
	t.Cleanup(sched.Wait)

	s := &Strategy{
		ctx:          ctx,
		cache:        newBudgetDiskCache(ctx, t),
		cloneManager: cm,
		scheduler:    sched,
		storage:      newStorageMetrics(),
		config:       Config{StorageLimitMB: 100, StorageMinCacheMB: 10},
	}

	s.enforceStorageBudget(ctx, map[string]int64{repo.UpstreamURL(): 200 * mib}, 200*mib)

	assert.Equal(t, 1, len(cm.All()), "a mirror with a live serve reservation must not be reclaimed")
}

// A zero budget leaves each component on its own configured limit, so upgrading
// without setting storage-limit-mb changes nothing.
func TestEnforceStorageBudgetDisabled(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	disk := newBudgetDiskCache(ctx, t)

	s := &Strategy{
		ctx:     ctx,
		cache:   disk,
		storage: newStorageMetrics(),
		config:  Config{StorageLimitMB: 0, StorageMinCacheMB: 10},
	}

	s.enforceStorageBudget(ctx, map[string]int64{}, 90*mib)
	assert.Equal(t, int64(100*mib), disk.SizeLimit(), "a disabled budget must not touch the cache limit")
}

// A floor larger than the whole budget is a misconfiguration. Clamping keeps the
// budget enforceable instead of unsatisfiable.
func TestEnforceStorageBudgetClampsOversizedFloor(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	disk := newBudgetDiskCache(ctx, t)

	s := &Strategy{
		ctx:   ctx,
		cache: disk,
		// The clamp leaves the mirrors a ceiling of zero, so enforcement reaches
		// reclamation and needs a manager to ask.
		cloneManager: newDiscoveredMirrorManager(ctx, t, 0),
		storage:      newStorageMetrics(),
		config:       Config{StorageLimitMB: 50, StorageMinCacheMB: 200},
	}

	s.enforceStorageBudget(ctx, map[string]int64{}, 10*mib)
	assert.Equal(t, int64(50*mib), disk.SizeLimit(), "cache share is capped by the whole budget")
}

// A cache configured smaller than the floor can never fill it, so reserving the
// floor would hold back space only the mirrors can use, and delete mirrors that
// do fit the budget.
func TestEnforceStorageBudgetReservesOnlyWhatTheCacheCanHold(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(ctx, t, 1)

	sched, err := jobscheduler.New(logging.ContextWithLogger(t.Context(), logging.FromContext(ctx)),
		jobscheduler.Config{})
	assert.NoError(t, err)
	t.Cleanup(sched.Wait)

	disk := newBudgetDiskCacheWithLimit(ctx, t, 10)
	s := &Strategy{
		ctx:          ctx,
		cache:        disk,
		cloneManager: cm,
		scheduler:    sched,
		storage:      newStorageMetrics(),
		config:       Config{StorageLimitMB: 100, StorageMinCacheMB: 50},
	}

	// The mirror sits between the ceiling the floor would impose (50 MiB) and the
	// one the cache's own 10 MiB limit justifies (90 MiB).
	s.enforceStorageBudget(ctx, map[string]int64{cm.All()[0].UpstreamURL(): 85 * mib}, 85*mib)

	assert.Equal(t, int64(10*mib), disk.SizeLimit(), "the cache cannot be raised past its configured limit")
	assert.Equal(t, 1, len(cm.All()), "a mirror fitting the budget must not be reclaimed for space the cache cannot use")
}

// A zero floor means "the mirrors may take everything". The cache must really
// be squeezed to nothing, not keep its previous, larger limit while the mirrors
// are sized as if it had shrunk.
func TestEnforceStorageBudgetSqueezesCacheToNothingWithZeroFloor(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	disk := newBudgetDiskCache(ctx, t)

	s := &Strategy{
		ctx:          ctx,
		cache:        disk,
		cloneManager: newDiscoveredMirrorManager(ctx, t, 0),
		storage:      newStorageMetrics(),
		config:       Config{StorageLimitMB: 100, StorageMinCacheMB: 0},
	}

	s.enforceStorageBudget(ctx, map[string]int64{}, 100*mib)
	assert.True(t, disk.SizeLimit() < 100*mib, "cache must not keep its configured limit once the mirrors fill the budget")
}

// An S3-only deployment has no local limit to reassign, so the budget bounds
// only the mirrors, and all of it is theirs: the cache floor would reserve space
// nothing local can use.
func TestEnforceStorageBudgetWithoutAdjustableCache(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(ctx, t, 1)

	sched, err := jobscheduler.New(logging.ContextWithLogger(t.Context(), logging.FromContext(ctx)),
		jobscheduler.Config{})
	assert.NoError(t, err)
	t.Cleanup(sched.Wait)

	s := &Strategy{
		ctx:          ctx,
		cache:        cache.NoOpCache(),
		cloneManager: cm,
		scheduler:    sched,
		storage:      newStorageMetrics(),
		config:       Config{StorageLimitMB: 100, StorageMinCacheMB: 10},
	}

	// Between the mirror ceiling the floor would impose (90 MiB) and the budget
	// itself, so reserving the floor would reclaim a mirror that fits.
	s.enforceStorageBudget(ctx, map[string]int64{cm.All()[0].UpstreamURL(): 95 * mib}, 95*mib)

	assert.Equal(t, 1, len(cm.All()), "mirrors fitting the whole budget must not be reclaimed for an unusable cache floor")
}

// Reclamation walks mirrors least recently served first, so a repository in use
// is the last deleted.
func TestMirrorsByLastServedOrdersLeastRecentFirst(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(ctx, t, 3)

	all := cm.All()
	assert.Equal(t, 3, len(all))

	// Serve them in a known order. MarkServed stamps each with now.
	for _, repo := range all {
		assert.NoError(t, cm.MarkServed(repo))
		time.Sleep(5 * time.Millisecond)
	}

	ordered := cm.MirrorsByLastServed()
	assert.Equal(t, 3, len(ordered))
	for i := 1; i < len(ordered); i++ {
		assert.True(t, !ordered[i].LastServed().Before(ordered[i-1].LastServed()),
			"mirrors must be ordered least recently served first")
	}
}
