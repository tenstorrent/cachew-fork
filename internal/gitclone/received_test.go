package gitclone //nolint:testpackage // white-box testing required for the unexported pack helpers

import (
	"crypto/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/tenstorrent/internal-cachew/internal/logging"
)

func receivedGitBytes(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(t.Context(), &rm))
	want := attribute.NewSet(attribute.String("source", "git"), attribute.String("strategy", "git"))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != "cachew.upstream.received_bytes_total" || !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if dp.Attributes.Equals(&want) {
					return dp.Value
				}
			}
		}
	}
	return 0
}

func totalPackBytes(sizes map[string]int64) int64 {
	var total int64
	for _, size := range sizes {
		total += size
	}
	return total
}

// pushRandomCommit adds a commit carrying incompressible content upstream, so
// the next fetch has to transfer a pack of known minimum size.
func pushRandomCommit(t *testing.T, upstreamPath string) {
	t.Helper()
	work := filepath.Join(t.TempDir(), "push")
	run := func(args ...string) {
		out, err := exec.Command("git", args...).CombinedOutput() //nolint:gosec // test-owned temp paths
		assert.NoError(t, err, string(out))
	}
	run("clone", upstreamPath, work)
	assert.NoError(t, os.WriteFile(filepath.Join(work, "random.bin"), []byte(rand.Text()+rand.Text()), 0o600))
	run("-C", work, "add", ".")
	run("-C", work, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "random")
	run("-C", work, "push", "origin", "HEAD")
}

func TestCloneAndFetchCountReceivedPackBytes(t *testing.T) {
	prev := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})

	// file:// forces the pack protocol that https upstreams use; a plain path
	// would copy the upstream's loose objects instead.
	upstreamPath := createBareRepo(t, t.TempDir())
	repo := &Repository{
		state:       StateEmpty,
		config:      testRepoConfig(),
		path:        filepath.Join(t.TempDir(), "mirror"),
		upstreamURL: "file://" + upstreamPath,
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}
	assert.NoError(t, repo.Clone(ctx))
	cloned := totalPackBytes(packFileSizes(repo.path))
	assert.True(t, cloned > 0, "a clone must leave at least one pack")
	assert.Equal(t, cloned, receivedGitBytes(t, reader), "a clone counts every pack it wrote")

	pushRandomCommit(t, upstreamPath)
	before := packFileSizes(repo.path)
	assert.NoError(t, repo.Fetch(ctx))
	after := packFileSizes(repo.path)

	fetched := totalPackBytes(after) - totalPackBytes(before)
	assert.True(t, fetched > 0, "the fetch should have written a new pack")
	assert.Equal(t, cloned+fetched, receivedGitBytes(t, reader), "a fetch counts only the packs it added")

	assert.NoError(t, repo.Fetch(ctx))
	assert.Equal(t, cloned+fetched, receivedGitBytes(t, reader), "a no-op fetch receives nothing")
}
