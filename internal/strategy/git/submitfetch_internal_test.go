package git

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/jobscheduler"
	"github.com/tenstorrent/internal-cachew/internal/logging"
)

type recordingScheduler struct {
	mu   sync.Mutex
	jobs []func(ctx context.Context) error
}

func (r *recordingScheduler) WithQueuePrefix(string) jobscheduler.Scheduler { return r }

func (r *recordingScheduler) Submit(_, _ string, run func(ctx context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs = append(r.jobs, run)
}

func (r *recordingScheduler) TrySubmit(_, _ string, _ int, _ func(ctx context.Context) error) bool {
	return false
}

func (r *recordingScheduler) SubmitPeriodicJob(string, string, time.Duration, func(ctx context.Context) error) {
}

func (r *recordingScheduler) SubmitPeriodicJobAfter(string, string, time.Duration, time.Duration,
	func(ctx context.Context) error) {
}

func (r *recordingScheduler) CancelPeriodicJob(string, string) {}

func (r *recordingScheduler) QueueActive(string) bool { return false }

func (r *recordingScheduler) submitted() []func(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.jobs
}

func TestSubmitFetchKeepsOnePendingPerRepo(t *testing.T) {
	ctx := logging.ContextWithLogger(t.Context(), slog.Default())
	manager, err := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:       t.TempDir(),
		RefCheckInterval: time.Nanosecond,
	}, nil)()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, "https://example.test/org/repo")
	assert.NoError(t, err)
	sched := &recordingScheduler{}
	s := &Strategy{scheduler: sched, cloneManager: manager, metrics: newGitMetrics(ctx, Config{}, slog.Default())}

	for range 5 {
		s.submitFetch(repo)
	}
	assert.Equal(t, 1, len(sched.submitted()), "a burst of stale requests must queue one fetch")

	// The fetch itself fails (there is no mirror on disk); what matters is that
	// starting it frees the slot for one follow-up.
	_ = sched.submitted()[0](ctx) //nolint:errcheck
	for range 5 {
		s.submitFetch(repo)
	}
	assert.Equal(t, 2, len(sched.submitted()), "a request after the fetch started must queue exactly one follow-up")
}
