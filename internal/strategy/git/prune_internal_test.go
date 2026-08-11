package git

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/hcl/v2"
	"go.opentelemetry.io/otel"

	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/metrics"
)

func TestPruneConfigKeys(t *testing.T) {
	var cfg Config
	assert.NoError(t, hcl.Unmarshal([]byte("prune-interval = \"24h\"\n"), &cfg))
	assert.Equal(t, 24*time.Hour, cfg.PruneInterval)

	// Off by default: pruning rewrites every pack and blocks fetches while it
	// runs, so an upgrade must not start doing it unasked.
	var defaults Config
	assert.NoError(t, hcl.Unmarshal(nil, &defaults))
	assert.Equal(t, time.Duration(0), defaults.PruneInterval)
}

func TestPruneJobIsCancelledWithTheMirror(t *testing.T) {
	assert.True(t, slices.Contains(maintenanceJobIDs, pruneJobID),
		"pruneJobID must be in maintenanceJobIDs so eviction stops it")
	assert.True(t, slices.Contains(maintenanceJobIDs, repackJobID),
		"repackJobID must be in maintenanceJobIDs so eviction stops it")
}

func TestPruneMetricSeriesName(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})

	prevProvider := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })

	client, err := metrics.New(ctx, metrics.Config{
		ServiceName:      "cachew-prune-metric-test",
		Port:             0,
		EnablePrometheus: true,
	})
	assert.NoError(t, err)
	defer client.Close()

	m := newGitMetrics(ctx, Config{}, logging.FromContext(ctx))
	m.recordPruneReclaimed(ctx, "github.com/org/repo", 4096)
	// A prune measuring flat or negative reclaims nothing, and recording it
	// would make the counter read as reclamation that did not happen.
	m.recordPruneReclaimed(ctx, "github.com/org/repo", -4096)
	m.recordOperation(ctx, "prune", "success", "background", time.Second)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	client.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()

	for _, want := range []string{
		`cachew_git_prune_reclaimed_bytes_total{`,
		`repository="github.com/org/repo"`,
		`operation="prune"`,
	} {
		assert.Contains(t, body, want, "expected series %q in scrape", want)
	}
	assert.Contains(t, body, "4096", "expected the positive reclaim to be recorded")
}

func TestGitRequestTypeCoversEveryLocallyServedPath(t *testing.T) {
	for path, want := range map[string]string{
		"/github.com/org/repo/info/refs":       "info-refs",
		"/github.com/org/repo/git-upload-pack": "upload-pack",
	} {
		assert.True(t, isGitRequest(path), "%s should dispatch to the local git handler", path)
		assert.Equal(t, want, gitRequestType(path))
	}
}

func TestFirstPruneDelayStaggersMirrors(t *testing.T) {
	interval := 24 * time.Hour
	first := firstPruneDelay("github.com/org/repo", interval)
	assert.Equal(t, first, firstPruneDelay("github.com/org/repo", interval),
		"the same mirror must land in the same slot across restarts")
	assert.True(t, first >= 0 && first < interval, "delay %s should fall inside the interval", first)

	slots := map[time.Duration]struct{}{}
	for _, upstream := range []string{
		"github.com/org/a", "github.com/org/b", "github.com/org/c",
		"github.com/org/d", "github.com/org/e",
	} {
		slots[firstPruneDelay(upstream, interval)] = struct{}{}
	}
	assert.Equal(t, 5, len(slots), "mirrors should not all queue their first prune at once")

	assert.Equal(t, time.Duration(0), firstPruneDelay("github.com/org/repo", 0))
}
