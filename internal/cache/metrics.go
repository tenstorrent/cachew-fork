package cache

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/tenstorrent/internal-cachew/internal/metrics"
)

// Eviction reasons. Constants rather than inline strings so label values are
// greppable.
const (
	// evictReasonExpired is an entry deleted because it passed its TTL.
	evictReasonExpired = "expired"
	// evictReasonSize is an entry deleted, least recently accessed first, to
	// get back under the size limit.
	evictReasonSize = "size"
	// evictReasonOrphaned is a metadata record whose file was already gone. It
	// frees no bytes, and is tracked separately because a non-zero rate means
	// something else is deleting the cache's files.
	evictReasonOrphaned = "orphaned"
)

// cacheMetrics instruments eviction and expiration for a cache backend.
//
// Held by pointer so every namespaced view of a backend shares one set of
// instruments.
type cacheMetrics struct {
	// backend labels every series, so a tiered cache's disk and S3 tiers stay
	// distinguishable. Fixed per backend rather than passed per call.
	backend       string
	meter         metric.Meter
	evictions     metric.Int64Counter
	evictedBytes  metric.Int64Counter
	sweeps        metric.Int64Counter
	sweepDuration metric.Float64Histogram
	ttlExtensions metric.Int64Counter
}

// newCacheMetrics constructs the eviction instruments for a backend.
//
// Caches are built before metrics.New installs the global meter provider, so
// these start as delegating stubs. Nothing here is recorded until the first
// sweep or read, which is long after startup, but it does mean a backend
// constructor must not record anything.
func newCacheMetrics(backend string) *cacheMetrics {
	meter := otel.Meter("cachew.cache")
	return &cacheMetrics{
		backend: backend,
		meter:   meter,
		evictions: metrics.NewMetric[metric.Int64Counter](meter, "cachew.cache.evictions_total", "{objects}",
			"Objects removed from a cache by reason (expired, size, orphaned), backend and namespace. "+
				"expired is TTL, size is least-recently-used eviction to fit the size limit, "+
				"orphaned is a metadata record whose file was already missing"),
		evictedBytes: metrics.NewMetric[metric.Int64Counter](meter, "cachew.cache.evicted_bytes_total", "By",
			"Bytes reclaimed from a cache by reason (expired, size), backend and namespace. "+
				"Orphaned records reclaim no bytes and so never appear here"),
		sweeps: metrics.NewMetric[metric.Int64Counter](meter, "cachew.cache.eviction_sweeps_total", "{sweeps}",
			"Eviction sweeps run by status (success, error) and backend. Sweeps with no evictions mean the "+
				"sweep ran and found nothing to reclaim, which differs from not running"),
		sweepDuration: metrics.NewHistogram(meter, "cachew.cache.eviction_sweep_duration_seconds", "s",
			"Wall-clock duration of an eviction sweep, which walks every metadata record and stats each file, "+
				"by backend", metrics.FastLatencyBuckets()),
		ttlExtensions: metrics.NewMetric[metric.Int64Counter](meter, "cachew.cache.ttl_extensions_total",
			"{extensions}",
			"Reads that slid an entry's expiry forward by its own TTL, by backend and namespace. "+
				"Reads that left expiry alone are not counted"),
	}
}

// registerSizeLimit publishes the limit eviction is enforcing, read from
// observe on every collection, and returns the registration the caller
// unregisters when the backend closes.
//
// Observable rather than recorded from [Disk.SetSizeLimit], because that only
// writes when the limit changes. A cache whose shared storage budget never
// binds sits at its configured limit-mb for the life of the process, so a
// write-on-change gauge would publish nothing at all and the series would be
// missing exactly when the cache is healthy.
func (m *cacheMetrics) registerSizeLimit(observe func() int64) metric.Registration {
	return metrics.RegisterObservableGauge(m.meter, "cachew.cache.size_limit_bytes", "By",
		"Size limit an eviction sweep is currently enforcing, by backend. Can be below the configured "+
			"limit-mb when a shared storage budget gave space to another component",
		observe, attribute.String("backend", m.backend))
}

func (m *cacheMetrics) recordEviction(ctx context.Context, reason string, namespace Namespace, bytes int64) {
	attrs := metric.WithAttributes(
		attribute.String("backend", m.backend),
		attribute.String("reason", reason),
		attribute.String("namespace", string(namespace)),
	)
	m.evictions.Add(ctx, 1, attrs)
	if bytes > 0 {
		m.evictedBytes.Add(ctx, bytes, attrs)
	}
}

func (m *cacheMetrics) recordSweep(ctx context.Context, status string, duration time.Duration) {
	m.sweeps.Add(ctx, 1, metric.WithAttributes(
		attribute.String("backend", m.backend),
		attribute.String("status", status),
	))
	m.sweepDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("backend", m.backend),
	))
}

func (m *cacheMetrics) recordTTLExtension(ctx context.Context, namespace Namespace) {
	m.ttlExtensions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("backend", m.backend),
		attribute.String("namespace", string(namespace)),
	))
}
