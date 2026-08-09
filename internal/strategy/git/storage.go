package git

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/metrics"
)

// Constants, not inline strings, so a dashboard's label filters stay greppable.
const (
	storageComponentMirrors = "git_mirrors"
	storageComponentCache   = "object_cache"
)

type storageMetrics struct {
	bytes     metric.Int64Gauge
	capacity  metric.Int64Gauge
	repoBytes metric.Int64Gauge

	// Int64Gauge never forgets an attribute set, so a repo missing from a scan
	// needs an explicit 0 here or it reports its last size forever.
	mu        sync.Mutex
	seenRepos map[string]struct{}
}

func newStorageMetrics() *storageMetrics {
	meter := otel.Meter("cachew.storage")
	return &storageMetrics{
		bytes: metrics.NewMetric[metric.Int64Gauge](meter, "cachew.storage.bytes", "By",
			"On-disk bytes held by a cachew component (git_mirrors, object_cache). Total cachew storage is sum() over this metric"),
		capacity: metrics.NewMetric[metric.Int64Gauge](meter, "cachew.storage.capacity_bytes", "By",
			"Configured storage limit for a cachew component, for components that have one"),
		repoBytes: metrics.NewMetric[metric.Int64Gauge](meter, "cachew.storage.repo_bytes", "By",
			"On-disk bytes of each mirrored repository's git directory"),
		seenRepos: map[string]struct{}{},
	}
}

// A goroutine here, not s.scheduler: that pool is small and shared with
// clone/fetch/repack/snapshot/eviction, and a scan sitting on it would delay
// those.
func (s *Strategy) scheduleStorageSampler() {
	interval := s.config.StorageScanInterval
	if interval <= 0 {
		return
	}
	logging.FromContext(s.ctx).InfoContext(s.ctx, "Storage usage sampling enabled", "scan_interval", interval)
	go s.runStorageSampler(interval)
}

func (s *Strategy) runStorageSampler(interval time.Duration) {
	// Guards against ctx already being cancelled by the time this goroutine
	// actually runs (New returning does not guarantee it hasn't).
	if s.ctx.Err() != nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	s.sampleStorageUsage(s.ctx)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.sampleStorageUsage(s.ctx)
		}
	}
}

// Walked per mirror because nothing tracks per-repo byte counts, and
// `git count-objects` is a subprocess per repo — no cheaper than the walk.
// The object cache already keeps a running total, so that's read instead.
//
// Sizes are apparent, not allocated blocks, and alternates are counted once
// per referencing mirror — both can diverge from du(1).
//
// Failures log and continue: a partial scan beats none.
func (s *Strategy) sampleStorageUsage(ctx context.Context) {
	logger := logging.FromContext(ctx)

	var mirrorsTotal int64
	current := make(map[string]struct{})
	for _, repo := range s.cloneManager.All() {
		// Per repo, not per file: dirSizeBytes takes no context, so one large
		// mirror's walk still runs to completion, but the rest of the scan
		// stops here.
		if ctx.Err() != nil {
			return
		}
		// dirSizeBytes swallows per-entry errors, so a mirror mid-clone or
		// mid-eviction yields a short count for one scan, not an error.
		size, err := dirSizeBytes(repo.Path())
		if err != nil {
			logger.WarnContext(ctx, "Failed to measure mirror size", "upstream", repo.UpstreamURL(), "error", err)
			continue
		}
		name := repoLabelForUpstream(repo.UpstreamURL())
		current[name] = struct{}{}
		mirrorsTotal += size
		s.storage.repoBytes.Record(ctx, size, metric.WithAttributes(attribute.String("repository", name)))
	}
	s.storage.zeroDepartedRepos(ctx, current)

	s.storage.bytes.Record(ctx, mirrorsTotal,
		metric.WithAttributes(attribute.String("component", storageComponentMirrors)))

	// S3 and the no-op cache return ErrStatsUnavailable rather than listing a
	// whole bucket — expected, not a failure, so object_cache is just absent.
	stats, err := s.cache.Stats(ctx)
	switch {
	case errors.Is(err, cache.ErrStatsUnavailable):
	case err != nil:
		logger.WarnContext(ctx, "Failed to read cache stats for storage metrics", "error", err)
	default:
		s.storage.bytes.Record(ctx, stats.Size,
			metric.WithAttributes(attribute.String("component", storageComponentCache)))
		if stats.Capacity > 0 {
			s.storage.capacity.Record(ctx, stats.Capacity,
				metric.WithAttributes(attribute.String("component", storageComponentCache)))
		}
	}
}

func (m *storageMetrics) zeroDepartedRepos(ctx context.Context, current map[string]struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name := range m.seenRepos {
		if _, still := current[name]; still {
			continue
		}
		m.repoBytes.Record(ctx, 0, metric.WithAttributes(attribute.String("repository", name)))
	}
	m.seenRepos = current
}

// Matches the "repository" label form used by the serve metrics (snapshot,
// bundle, incremental), so storage series join against those. authz and
// background-fetch metrics label with the raw URL instead and won't join.
func repoLabelForUpstream(upstreamURL string) string {
	return strings.TrimPrefix(strings.TrimPrefix(upstreamURL, "https://"), "http://")
}
