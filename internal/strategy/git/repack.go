package git

import (
	"context"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/logging"
)

// repackJobID and pruneJobID are separate periodic jobs submitted on the
// repository's own queue, so they never overlap each other or a snapshot. They
// are deliberately not ordered relative to one another: prune deletes the
// unreachable objects outright, so a geometric repack that follows it has
// nothing to undo, and a prune that follows a repack still sees every
// unreachable object because a geometric roll-up carries them forward.
const (
	repackJobID = "repack-periodic"
	pruneJobID  = "prune-periodic"
	// startupMaintenanceJobID is a one-shot job rather than a periodic one, so
	// it is absent from maintenanceJobIDs: eviction has no periodic chain to
	// stop for it. It can still be sitting unstarted on the mirror's queue when
	// eviction runs, though, so cancelMaintenanceJobs tracks and cancels it
	// separately via pendingStartupMaintenance instead of folding it into that
	// list.
	startupMaintenanceJobID = "startup-maintenance"
)

// Triggers distinguishing which schedule drove an operation, on
// cachew_git_operations_total and the spans. "incremental" is recorded by the
// request path in git.go.
const (
	triggerBackground = "background"
	triggerStartup    = "startup"
)

func (s *Strategy) scheduleRepackJobs(repo *gitclone.Repository) {
	if _, loaded := s.repackJobsScheduled.LoadOrStore(repo.UpstreamURL(), true); loaded {
		return
	}
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), repackJobID, s.config.RepackInterval, func(ctx context.Context) error {
		return s.repack(ctx, repo, triggerBackground)
	})
}

func (s *Strategy) repack(ctx context.Context, repo *gitclone.Repository, trigger string) (returnErr error) {
	upstream := repo.UpstreamURL()
	ctx, span := tracer.Start(ctx, "git.repack",
		trace.WithAttributes(
			attribute.String("cachew.operation", "repack"),
			attribute.String("cachew.upstream", upstream),
			attribute.String("cachew.trigger", trigger),
		),
	)
	defer func() {
		if returnErr != nil {
			span.RecordError(returnErr)
			span.SetStatus(codes.Error, returnErr.Error())
		}
		span.End()
	}()

	// Pack count before and after gives us a direct view of how much
	// the geometric repack actually consolidated. A flat before/after
	// ratio over time means fragmentation is outpacing the schedule.
	if before, err := countPackFiles(repo.Path()); err == nil {
		s.metrics.recordRepackPackCount(ctx, upstream, "before", before)
		span.SetAttributes(attribute.Int("cachew.pack_count_before", before))
	}

	start := time.Now()
	err := repo.Repack(ctx)
	status := statusSuccess
	if err != nil {
		status = statusError
	}
	s.metrics.recordOperation(ctx, "repack", status, trigger, time.Since(start))

	if after, countErr := countPackFiles(repo.Path()); countErr == nil {
		s.metrics.recordRepackPackCount(ctx, upstream, "after", after)
		span.SetAttributes(attribute.Int("cachew.pack_count_after", after))
	}

	return errors.Wrap(err, "repack")
}

// schedulePruneJobs registers the periodic reclamation of objects that no ref
// points at any more. Kept separate from the repack job because it is far more
// expensive, holds the fetch exclusion, and so runs on its own much longer
// interval.
//
// Its first run is staggered rather than immediate: enabling pruning, or
// restarting a server whose mirrors have never been pruned, would otherwise
// queue a full-pack rewrite for every mirror at once, each holding that
// mirror's fetch exclusion.
func (s *Strategy) schedulePruneJobs(repo *gitclone.Repository) {
	upstream := repo.UpstreamURL()
	firstRun := firstPruneDelay(upstream, s.config.PruneInterval)
	s.scheduler.SubmitPeriodicJobAfter(upstream, pruneJobID, firstRun, s.config.PruneInterval, func(ctx context.Context) error {
		return s.prune(ctx, repo, triggerBackground)
	})
}

func (s *Strategy) prune(ctx context.Context, repo *gitclone.Repository, trigger string) (returnErr error) {
	upstream := repo.UpstreamURL()
	ctx, span := tracer.Start(ctx, "git.prune",
		trace.WithAttributes(
			attribute.String("cachew.operation", "prune"),
			attribute.String("cachew.upstream", upstream),
			attribute.String("cachew.trigger", trigger),
		),
	)
	defer func() {
		if returnErr != nil {
			span.RecordError(returnErr)
			span.SetStatus(codes.Error, returnErr.Error())
		}
		span.End()
	}()

	logger := logging.FromContext(ctx)
	logger.InfoContext(ctx, "Prune started", "upstream", upstream, "trigger", trigger)

	start := time.Now()
	reclaimed, err := repo.PruneUnreachable(ctx)
	status := statusSuccess
	if err != nil {
		status = statusError
	}
	s.metrics.recordOperation(ctx, "prune", status, trigger, time.Since(start))

	if err == nil {
		logger.InfoContext(ctx, "Prune completed", "upstream", upstream, "trigger", trigger, "reclaimed_bytes", reclaimed)
		s.metrics.recordPruneReclaimed(ctx, upstream, reclaimed)
		span.SetAttributes(attribute.Int64("cachew.prune_reclaimed_bytes", reclaimed))
	}

	return errors.Wrap(err, "prune")
}

// scheduleStartupMaintenance queues the one prune and repack that startup-maintenance
// asks for on a mirror just discovered at startup, so a change to either path can be
// observed on a real mirror set without waiting out firstPruneDelay's stagger.
//
// Queued on the mirror's own queue rather than run inline in warmExistingRepos, for
// two reasons: that loop gates /_readiness and PruneUnreachable is bounded only by
// prune-timeout (1h by default), and the queue is the one prune, repack and snapshot
// already share, so this cannot overlap the periodic jobs it duplicates. Those
// periodics still run on their own schedule — the staggered first prune and, on a
// volume with no last-run stamp, the immediate first repack — and a second pass
// finding nothing left to reclaim is the expected outcome, not a problem.
//
// Prune runs before repack because the fetch that precedes it in warmExistingRepos
// is what orphans the objects prune reclaims. Their order is not otherwise
// significant (see the note on the job IDs above), and a failing prune does not skip
// the repack: they touch the mirror independently.
//
// Marks the mirror pending in s.pendingStartupMaintenance until the job starts
// running, so cancelMaintenanceJobs can tell a still-queued pass apart from one
// that is already running or done, and cancel only the former before an
// eviction deletes the mirror out from under it.
func (s *Strategy) scheduleStartupMaintenance(repo *gitclone.Repository) {
	if !s.config.StartupMaintenance {
		return
	}
	prune, repack := s.config.PruneInterval > 0, s.config.RepackInterval > 0
	if !prune && !repack {
		return
	}
	upstream := repo.UpstreamURL()
	s.pendingStartupMaintenance.Store(upstream, struct{}{})
	s.scheduler.Submit(upstream, startupMaintenanceJobID, func(ctx context.Context) error {
		s.pendingStartupMaintenance.Delete(upstream)
		var pruneErr, repackErr error
		if prune {
			pruneErr = s.prune(ctx, repo, triggerStartup)
		}
		if repack {
			repackErr = s.repack(ctx, repo, triggerStartup)
		}
		return errors.WithStack(errors.Join(pruneErr, repackErr))
	})
}

// firstPruneDelay spreads mirrors' first prunes over the interval, keyed on the
// upstream URL so a mirror lands in the same slot across restarts instead of
// walking towards the front of the window each time the server comes up.
func firstPruneDelay(upstream string, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(upstream))
	// #nosec G115 -- interval > 0 here, so uint64(interval) <= math.MaxInt64 and
	// the modulo result is always < interval, which can never overflow back to int64.
	return time.Duration(h.Sum64() % uint64(interval))
}

// countPackFiles returns the number of .pack files in the mirror's
// objects/pack directory. Returns 0, nil if the directory is missing.
func countPackFiles(repoPath string) (int, error) {
	entries, err := os.ReadDir(filepath.Join(repoPath, "objects", "pack"))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, errors.Wrap(err, "read pack dir")
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".pack") {
			count++
		}
	}
	return count, nil
}
