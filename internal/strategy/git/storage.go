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

// bytesPerMB matches the disk cache's reading of limit-mb, which is mebibytes
// despite the name.
const bytesPerMB = 1024 * 1024

// Mirror eviction reasons, constants for the same reason as the component
// labels above.
const (
	mirrorEvictReasonIdle  = "idle"
	mirrorEvictReasonQuota = "quota"
)

type storageMetrics struct {
	bytes           metric.Int64Gauge
	capacity        metric.Int64Gauge
	repoBytes       metric.Int64Gauge
	mirrorEvictions metric.Int64Counter
	mirrorEvictedB  metric.Int64Counter

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
		mirrorEvictions: metrics.NewMetric[metric.Int64Counter](meter, "cachew.storage.mirror_evictions_total",
			"{mirrors}",
			"Mirrored repositories deleted by reason (idle, quota). idle is mirror-ttl expiry, quota is reclamation "+
				"because the mirrors alone could not fit in storage-limit-mb"),
		mirrorEvictedB: metrics.NewMetric[metric.Int64Counter](meter, "cachew.storage.mirror_evicted_bytes_total",
			"By",
			"Bytes reclaimed by deleting mirrored repositories, by reason (idle, quota)"),
		seenRepos: map[string]struct{}{},
	}
}

func (m *storageMetrics) recordMirrorEviction(ctx context.Context, reason string, bytes int64) {
	attrs := metric.WithAttributes(attribute.String("reason", reason))
	m.mirrorEvictions.Add(ctx, 1, attrs)
	if bytes > 0 {
		m.mirrorEvictedB.Add(ctx, bytes, attrs)
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

	mirrorSizes, mirrorsTotal, complete := s.measureMirrors(ctx)
	if ctx.Err() != nil {
		return
	}

	s.storage.bytes.Record(ctx, mirrorsTotal,
		metric.WithAttributes(attribute.String("component", storageComponentMirrors)))

	// S3 and the no-op cache return ErrStatsUnavailable rather than listing a
	// whole bucket. That is expected, so object_cache is simply absent.
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

	// Only enforce on a complete measurement of a fully discovered mirror set.
	// A short count under-reports the mirrors, gives the object cache space that
	// is not there, and can delete mirrors that already fit.
	//
	// complete is not enough on its own, because the sampler starts before
	// warm-up: it only means every mirror registered so far was measured, which
	// an empty manager satisfies trivially. Enforcing then would give the cache
	// the whole volume, or reclaim a mirror as soon as it was discovered.
	// Readiness is the gate the idle evictor waits on. Until it opens, the cache
	// keeps its configured limit-mb, as it would with no shared budget.
	if !complete || !s.ready.Load() {
		logger.DebugContext(ctx, "Skipping storage budget enforcement",
			"measurement_complete", complete, "strategy_ready", s.ready.Load())
		return
	}
	s.enforceStorageBudget(ctx, mirrorSizes, mirrorsTotal)
}

// measureMirrors walks every mirror once, records the per-repo gauges and
// returns the sizes, so the shared budget reuses the measurement instead of
// walking again. The total also covers staged copies of evicted mirrors that
// could not be deleted, which occupy the volume but belong to no repository.
// complete is false if anything was measured only in part. The gauges still
// take the short count, but the budget must not be enforced against it.
func (s *Strategy) measureMirrors(ctx context.Context) (sizes map[string]int64, total int64, complete bool) {
	logger := logging.FromContext(ctx)
	sizes = make(map[string]int64)
	complete = true
	current := make(map[string]struct{})
	for _, repo := range s.cloneManager.All() {
		// Per repo, not per file: dirSizeBytes takes no context, so one large
		// mirror's walk still runs to completion, but the rest of the scan
		// stops here.
		if ctx.Err() != nil {
			return sizes, total, false
		}
		// A short count is fine for the gauge but not for enforcement: the
		// missing bytes are still on the volume, and giving them to the object
		// cache would put the pair over budget. So an inexact measurement is
		// reported as-is and only suppresses enforcement.
		size, exact, err := measureDirSizeBytes(repo.Path())
		if err != nil {
			logger.WarnContext(ctx, "Failed to measure mirror size", "upstream", repo.UpstreamURL(), "error", err)
			complete = false
			continue
		}
		if !exact {
			logger.WarnContext(ctx, "Mirror measurement incomplete, skipping budget enforcement this pass",
				"upstream", repo.UpstreamURL())
			complete = false
		}
		name := repoLabelForUpstream(repo.UpstreamURL())
		current[name] = struct{}{}
		sizes[repo.UpstreamURL()] = size
		total += size
		s.storage.repoBytes.Record(ctx, size, metric.WithAttributes(attribute.String("repository", name)))
	}
	s.storage.zeroDepartedRepos(ctx, current)

	// Staged copies of evicted mirrors whose deletion failed still occupy the
	// volume but belong to no repository, so retry the delete and count what
	// survives. Dropping them from the total would give those bytes to the
	// object cache and leave the volume over budget.
	leftovers, err := s.cloneManager.ReclaimLeftovers()
	if err != nil {
		logger.WarnContext(ctx, "Failed to delete leftover staged mirrors", "error", err)
	}
	for _, path := range leftovers {
		size, exact, err := measureDirSizeBytes(path)
		if err != nil || !exact {
			logger.WarnContext(ctx, "Leftover staged mirror measured only in part, skipping budget enforcement",
				"path", path, "error", err)
			complete = false
		}
		total += size
	}
	return sizes, total, complete
}

// enforceStorageBudget shares one on-disk budget between the git mirrors and
// the object cache. They sit on the same volume, and capping each separately
// leaves their sum unbounded.
//
// Mirrors take priority because they cost more to rebuild: re-cloning a large
// repository takes minutes of upstream transfer, while a missing cached object
// takes one regeneration. So mirrors are measured first and the object cache
// gets the remainder, floored at storage-min-cache-mb so it is not squeezed to
// nothing. Mirrors are reclaimed only when they cannot fit even with the cache
// at its floor.
func (s *Strategy) enforceStorageBudget(ctx context.Context, mirrorSizes map[string]int64, mirrorsTotal int64) {
	budget := int64(s.config.StorageLimitMB) * bytesPerMB
	if budget <= 0 {
		return
	}
	logger := logging.FromContext(ctx)

	// A floor larger than the whole budget is a misconfiguration. Clamping
	// instead of rejecting keeps the budget enforceable, and the mirrors then
	// get nothing.
	cacheFloor := min(int64(s.config.StorageMinCacheMB)*bytesPerMB, budget)

	// Never ask for a non-positive allowance. A cache that reads that as "no
	// change" keeps its previous, larger limit while reporting success, and the
	// mirrors would be sized as though it had shrunk. One byte is the smallest
	// unambiguous request, and is what a zero floor asks for.
	allowance := max(budget-mirrorsTotal, cacheFloor, 1)
	cacheLimit, adjustable := cache.SetLocalSizeLimit(ctx, s.cache, allowance)

	// Reserve only what the cache can use. Its configured limit-mb caps the
	// allowance, so a cache smaller than the floor would otherwise have space
	// held back that it can never fill, evicting mirrors that do fit.
	mirrorCeiling := budget - min(cacheFloor, cacheLimit)
	if !adjustable {
		// Nothing behind this cache has a local size limit, as in an S3-only
		// deployment where reclamation is a bucket lifecycle policy. There is no
		// cache share to reassign, so the budget bounds only the mirrors.
		// Reserving the floor would evict mirrors that fit, to make room for a
		// local cache that will never use it.
		logger.DebugContext(ctx, "Shared storage budget: cache has no adjustable size limit",
			"mirrors_bytes", mirrorsTotal)
		mirrorCeiling = budget
	}
	s.storage.capacity.Record(ctx, mirrorCeiling,
		metric.WithAttributes(attribute.String("component", storageComponentMirrors)))

	if mirrorsTotal <= mirrorCeiling {
		return
	}
	logger.InfoContext(ctx, "Mirrors over shared storage budget, reclaiming",
		"mirrors_bytes", mirrorsTotal, "mirror_ceiling_bytes", mirrorCeiling, "budget_bytes", budget)
	s.reclaimMirrorsToFit(ctx, mirrorSizes, mirrorsTotal, mirrorCeiling)
}

// reclaimMirrorsToFit deletes mirrors, least recently served first, until they
// fit under target.
//
// This is the last resort, reached only once the object cache is at its floor.
// It reuses the idle evictor's safety sequence: stop the repository's
// maintenance jobs, skip anything with a job running, and let the manager
// decline if a client is mid-clone.
func (s *Strategy) reclaimMirrorsToFit(
	ctx context.Context, mirrorSizes map[string]int64, mirrorsTotal, target int64,
) {
	logger := logging.FromContext(ctx)
	for _, repo := range s.cloneManager.MirrorsByLastServed() {
		if mirrorsTotal <= target {
			return
		}
		if ctx.Err() != nil {
			return
		}
		upstream := repo.UpstreamURL()
		if _, preparing := s.mirrorPreparations.Load(upstream); preparing {
			continue
		}
		size, measured := mirrorSizes[upstream]
		if !measured {
			// Appeared after the scan, so its size is unknown and evicting it
			// cannot be accounted for. Leave it for the next pass.
			continue
		}
		startupPending := s.cancelMaintenanceJobs(repo)
		if s.scheduler.QueueActive(upstream) {
			logger.InfoContext(ctx, "Skipping quota eviction, repository busy", "upstream", upstream)
			s.scheduleMaintenanceJobs(repo)
			if startupPending {
				s.scheduleStartupMaintenance(repo)
			}
			continue
		}
		// Both halves of the result stay meaningful alongside an error. Unlinked
		// says whether the repository is still ours to maintain. Reclaimed says
		// whether the bytes left the volume. An unlinked mirror whose staged
		// copy survived still occupies disk, so subtracting it would stop
		// reclaiming against a target that is not met. Move on instead.
		result, err := s.cloneManager.EvictForQuota(ctx, repo)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to evict mirror for storage budget", "upstream", upstream,
				"error", err, "unlinked", result.Unlinked, "reclaimed", result.Reclaimed)
		}
		if !result.Unlinked {
			s.scheduleMaintenanceJobs(repo)
			if startupPending {
				s.scheduleStartupMaintenance(repo)
			}
			continue
		}
		if !result.Reclaimed {
			continue
		}
		mirrorsTotal -= size
		s.storage.recordMirrorEviction(ctx, mirrorEvictReasonQuota, size)
		logger.InfoContext(ctx, "Evicted mirror for storage budget", "upstream", upstream,
			"bytes", size, "mirrors_bytes", mirrorsTotal, "target_bytes", target)
	}
	if mirrorsTotal > target {
		logger.WarnContext(ctx, "Mirrors still over storage budget after reclamation",
			"mirrors_bytes", mirrorsTotal, "target_bytes", target)
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
