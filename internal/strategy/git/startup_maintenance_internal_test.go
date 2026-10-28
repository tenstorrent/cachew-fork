package git

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/hcl/v2"
	"go.opentelemetry.io/otel"

	"github.com/tenstorrent/internal-cachew/internal/jobscheduler"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/metrics"
)

func TestStartupMaintenanceConfigKey(t *testing.T) {
	var cfg Config
	assert.NoError(t, hcl.Unmarshal([]byte("startup-maintenance = true\n"), &cfg))
	assert.True(t, cfg.StartupMaintenance)

	var defaults Config
	assert.NoError(t, hcl.Unmarshal(nil, &defaults))
	assert.False(t, defaults.StartupMaintenance,
		"startup-maintenance gives up the per-mirror prune stagger, so an upgrade must not opt in for the operator")
}

func TestStartupMaintenanceQueuesOnePassPerMirrorQueue(t *testing.T) {
	sched := &queueCapturingScheduler{}
	s := &Strategy{
		config: Config{
			StartupMaintenance: true,
			PruneInterval:      24 * time.Hour,
			RepackInterval:     18 * time.Hour,
		},
		scheduler: sched,
	}

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(ctx, t, 3)
	repos := cm.All()
	assert.Equal(t, 3, len(repos))
	for _, repo := range repos {
		s.scheduleStartupMaintenance(repo)
	}

	assert.Equal(t, 3, len(sched.submitted))
	for _, repo := range repos {
		id, ok := sched.submitted[repo.UpstreamURL()]
		assert.True(t, ok, "mirror %s got no startup pass", repo.UpstreamURL())
		assert.Equal(t, startupMaintenanceJobID, id)
	}
}

func TestStartupMaintenanceRunsOnlyTheOperationsAlreadyEnabled(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(ctx, t, 1)
	repo := cm.All()[0]

	for name, tc := range map[string]struct {
		config Config
		queued bool
	}{
		"disabled": {
			config: Config{PruneInterval: 24 * time.Hour, RepackInterval: 18 * time.Hour},
			queued: false,
		},
		"enabled but nothing to run": {
			config: Config{StartupMaintenance: true},
			queued: false,
		},
		"repack only": {
			config: Config{StartupMaintenance: true, RepackInterval: 18 * time.Hour},
			queued: true,
		},
		"prune only": {
			config: Config{StartupMaintenance: true, PruneInterval: 24 * time.Hour},
			queued: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			sched := &queueCapturingScheduler{}
			s := &Strategy{config: tc.config, scheduler: sched}
			s.scheduleStartupMaintenance(repo)
			assert.Equal(t, tc.queued, len(sched.submitted) == 1)
		})
	}
}

func TestStartupMaintenancePassPrunesAndRepacksTheMirror(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})

	prevProvider := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })
	client, err := metrics.New(ctx, metrics.Config{
		ServiceName:      "cachew-startup-maintenance-test",
		Port:             0,
		EnablePrometheus: true,
	})
	assert.NoError(t, err)
	defer client.Close()

	cm := newDiscoveredMirrorManager(ctx, t, 1)
	repo := cm.All()[0]

	sched := &queueCapturingScheduler{}
	s := &Strategy{
		config: Config{
			StartupMaintenance: true,
			PruneInterval:      24 * time.Hour,
			RepackInterval:     18 * time.Hour,
		},
		scheduler: sched,
		metrics:   newGitMetrics(ctx, Config{}, logging.FromContext(ctx)),
	}
	s.scheduleStartupMaintenance(repo)
	assert.NoError(t, sched.runs[repo.UpstreamURL()](ctx))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	client.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()

	for _, want := range []string{
		`operation="prune"`,
		`operation="repack"`,
		`trigger="startup"`,
	} {
		assert.Contains(t, body, want,
			"the startup pass must be distinguishable from the periodic schedule on the dashboard")
	}
	assert.NotContains(t, body, `trigger="background"`,
		"nothing in this test ran on the periodic schedule")
}

// TestCancelMaintenanceJobsCancelsPendingStartupPass covers the gap flagged in
// review: startupMaintenanceJobID is deliberately absent from maintenanceJobIDs,
// so without separate tracking a startup pass still sitting on the queue (not
// yet started) would survive cancelMaintenanceJobs and could later run against
// a mirror an eviction deleted out from under it.
func TestCancelMaintenanceJobsCancelsPendingStartupPass(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(ctx, t, 1)
	repo := cm.All()[0]

	sched := &queueCapturingScheduler{}
	s := &Strategy{
		config: Config{
			StartupMaintenance: true,
			PruneInterval:      time.Hour,
			RepackInterval:     time.Hour,
		},
		scheduler: sched,
	}

	s.scheduleStartupMaintenance(repo)
	assert.Equal(t, 1, len(sched.submitted), "the pass should be queued")

	assert.True(t, s.cancelMaintenanceJobs(repo),
		"a still-queued startup pass must be reported so the caller can reschedule it")
	assert.Equal(t, 0, len(sched.submitted), "cancelling must remove the pending pass from the queue")

	assert.False(t, s.cancelMaintenanceJobs(repo),
		"nothing pending means nothing to reschedule if eviction backs off")
}

// TestReclaimMirrorsToFitReschedulesPendingStartupPassWhenAbandoned exercises
// the actual race: a startup pass is queued but has not started when quota
// eviction runs, the mirror's queue is otherwise busy so eviction backs off,
// and the pass must survive that round trip rather than being silently lost.
func TestReclaimMirrorsToFitReschedulesPendingStartupPassWhenAbandoned(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	cm := newDiscoveredMirrorManager(ctx, t, 1)
	repo := cm.All()[0]
	upstream := repo.UpstreamURL()

	sched, err := jobscheduler.New(logging.ContextWithLogger(t.Context(), logging.FromContext(ctx)),
		jobscheduler.Config{Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(sched.Wait)

	prevProvider := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })
	client, err := metrics.New(ctx, metrics.Config{
		ServiceName:      "cachew-startup-eviction-race-test",
		Port:             0,
		EnablePrometheus: true,
	})
	assert.NoError(t, err)
	defer client.Close()

	s := &Strategy{
		ctx:          ctx,
		cloneManager: cm,
		scheduler:    sched,
		storage:      newStorageMetrics(),
		metrics:      newGitMetrics(ctx, Config{}, logging.FromContext(ctx)),
		config: Config{
			StartupMaintenance: true,
			PruneInterval:      time.Hour,
			RepackInterval:     time.Hour,
		},
	}

	// Occupy the mirror's own queue so the startup pass submitted next sits
	// pending instead of running immediately.
	holdStarted, holdRelease := make(chan struct{}), make(chan struct{})
	sched.Submit(upstream, "test-hold-queue", func(context.Context) error {
		close(holdStarted)
		<-holdRelease
		return nil
	})
	<-holdStarted

	s.scheduleStartupMaintenance(repo)

	// The mirror is nominally over budget, but its queue is busy: eviction must
	// cancel the pending pass, decline to delete the mirror, and put the pass
	// back rather than leaving it cancelled.
	s.reclaimMirrorsToFit(ctx, map[string]int64{upstream: 1024}, 1024, 0)
	assert.Equal(t, 1, len(cm.All()), "a busy mirror must not be evicted")

	close(holdRelease)

	deadline := time.Now().Add(5 * time.Second)
	var body string
	for time.Now().Before(deadline) {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		w := httptest.NewRecorder()
		client.Handler().ServeHTTP(w, req)
		body = w.Body.String()
		if strings.Contains(body, `trigger="startup"`) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Contains(t, body, `trigger="startup"`,
		"the rescheduled startup pass must actually run once the queue frees up, not be lost")
}

// queueCapturingScheduler records the queue a job landed on, which
// capturingScheduler discards. The startup pass submits one job per mirror
// under a single job ID, so the queue is the only thing distinguishing them.
type queueCapturingScheduler struct {
	submitted map[string]string
	runs      map[string]func(ctx context.Context) error
}

func (q *queueCapturingScheduler) WithQueuePrefix(string) jobscheduler.Scheduler { return q }

// TrySubmit records jobs so maintenance tests can inspect background scheduling.
func (q *queueCapturingScheduler) TrySubmit(queue, id string, _ int, run func(ctx context.Context) error) bool {
	q.Submit(queue, id, run)
	return true
}

func (q *queueCapturingScheduler) Submit(queue, id string, run func(ctx context.Context) error) {
	if q.submitted == nil {
		q.submitted = map[string]string{}
		q.runs = map[string]func(ctx context.Context) error{}
	}
	q.submitted[queue] = id
	q.runs[queue] = run
}

func (q *queueCapturingScheduler) SubmitPeriodicJob(queue, id string, _ time.Duration, run func(ctx context.Context) error) {
	q.Submit(queue, id, run)
}

func (q *queueCapturingScheduler) SubmitPeriodicJobAfter(queue, id string, _, _ time.Duration, run func(ctx context.Context) error) {
	q.Submit(queue, id, run)
}

func (q *queueCapturingScheduler) CancelPeriodicJob(queue, _ string) {
	delete(q.submitted, queue)
	delete(q.runs, queue)
}

func (q *queueCapturingScheduler) QueueActive(string) bool { return false }
