package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"

	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/metrics"
)

// The Prometheus exporter rewrites instrument names from their unit. A counter
// measured in "By" can gain a _bytes suffix, and any counter can gain _total, so
// the scraped name is not the OTel name with dots replaced. The Grafana
// dashboard queries these names literally, so pin them here.
func TestCacheMetricSeriesNames(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})

	// metrics.New sets the global meter provider. Restore it, or later tests
	// bind to one this test closes.
	prevProvider := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })

	// Must come before newCacheMetrics, because instruments bind to whatever
	// provider is global at construction.
	client, err := metrics.New(ctx, metrics.Config{
		ServiceName:      "cachew-cache-metrics-test",
		Port:             0,
		EnablePrometheus: true,
	})
	assert.NoError(t, err)
	defer client.Close()

	m := newCacheMetrics(diskBackendName)
	m.recordEviction(ctx, evictReasonExpired, "git", 4096)
	m.recordEviction(ctx, evictReasonSize, "git", 8192)
	m.recordEviction(ctx, evictReasonOrphaned, "git", 0)
	m.recordSweep(ctx, "success", 250*time.Millisecond)
	m.recordTTLExtension(ctx, "git")
	m.registerSizeLimit(func() int64 { return 70 * 1024 * 1024 })

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	client.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()

	for _, want := range []string{
		`cachew_cache_evictions_total{`,
		`cachew_cache_evicted_bytes_total{`,
		`cachew_cache_eviction_sweeps_total{`,
		`cachew_cache_eviction_sweep_duration_seconds_bucket{`,
		`cachew_cache_ttl_extensions_total{`,
		`cachew_cache_size_limit_bytes{`,
		`reason="expired"`,
		`reason="size"`,
		`reason="orphaned"`,
		`backend="disk"`,
		`namespace="git"`,
	} {
		assert.Contains(t, body, want, "expected series %q in scrape", want)
	}
}

func TestDiskPublishesSizeLimitWithoutAnyChange(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})

	prevProvider := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })

	client, err := metrics.New(ctx, metrics.Config{
		ServiceName:      "cachew-cache-size-limit-test",
		Port:             0,
		EnablePrometheus: true,
	})
	assert.NoError(t, err)
	defer client.Close()

	d, err := NewDisk(ctx, DiskConfig{
		Root:          t.TempDir(),
		LimitMB:       64,
		MaxTTL:        time.Hour,
		EvictInterval: time.Minute,
	})
	assert.NoError(t, err)
	defer d.Close()

	// Deliberately no SetSizeLimit call, and no eviction triggered.
	assert.Equal(t, "6.7108864e+07", sizeLimitValue(t, client))
}

func TestDiskSizeLimitFollowsBudget(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})

	prevProvider := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })

	client, err := metrics.New(ctx, metrics.Config{
		ServiceName:      "cachew-cache-size-limit-budget-test",
		Port:             0,
		EnablePrometheus: true,
	})
	assert.NoError(t, err)
	defer client.Close()

	d, err := NewDisk(ctx, DiskConfig{
		Root:          t.TempDir(),
		LimitMB:       64,
		MaxTTL:        time.Hour,
		EvictInterval: time.Minute,
	})
	assert.NoError(t, err)
	defer d.Close()

	d.SetSizeLimit(ctx, 32*1024*1024)
	assert.Equal(t, "3.3554432e+07", sizeLimitValue(t, client))

	// limit-mb is the ceiling: a budget cannot hand out more than configured.
	d.SetSizeLimit(ctx, 999*1024*1024)
	assert.Equal(t, "6.7108864e+07", sizeLimitValue(t, client))
}

func TestDiskCloseStopsPublishingSizeLimit(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})

	prevProvider := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })

	client, err := metrics.New(ctx, metrics.Config{
		ServiceName:      "cachew-cache-size-limit-close-test",
		Port:             0,
		EnablePrometheus: true,
	})
	assert.NoError(t, err)
	defer client.Close()

	root := t.TempDir()
	first, err := NewDisk(ctx, DiskConfig{
		Root:          root,
		LimitMB:       64,
		MaxTTL:        time.Hour,
		EvictInterval: time.Minute,
	})
	assert.NoError(t, err)
	assert.NoError(t, first.Close())

	assert.Equal(t, "", sizeLimitValue(t, client), "a closed cache kept publishing a limit nothing enforces")

	// Every disk cache labels the series backend="disk", so a registration the
	// closed one left behind would collide with this one on the same series.
	second, err := NewDisk(ctx, DiskConfig{
		Root:          root,
		LimitMB:       32,
		MaxTTL:        time.Hour,
		EvictInterval: time.Minute,
	})
	assert.NoError(t, err)
	defer second.Close()

	assert.Equal(t, "3.3554432e+07", sizeLimitValue(t, client))
}

func scrape(t *testing.T, client *metrics.Client) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	client.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	return w.Body.String()
}

// sizeLimitValue returns the scraped value of cachew_cache_size_limit_bytes, or
// "" when the series is absent. Matched by prefix rather than on a full line
// because the exporter appends otel_scope_* labels of its own.
func sizeLimitValue(t *testing.T, client *metrics.Client) string {
	t.Helper()
	for line := range strings.Lines(scrape(t, client)) {
		if !strings.HasPrefix(line, "cachew_cache_size_limit_bytes{") {
			continue
		}
		_, value, _ := strings.Cut(strings.TrimSpace(line), "} ")
		return value
	}
	return ""
}
