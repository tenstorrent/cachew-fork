package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	m.recordSizeLimit(ctx, 70*1024*1024)

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
