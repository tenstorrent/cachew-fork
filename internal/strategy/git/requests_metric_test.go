package git_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel/attribute"

	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/strategy/git"
)

const requestsMetric = "cachew.git.requests_total"

func TestGitHTTPRequestsAreCountedByType(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/countedrepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	headSHA := setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	refsURL := fmt.Sprintf("%s/git/%s/%s/info/refs?service=git-upload-pack", cachewSrv.URL, host, repoPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, refsURL, nil)
	assert.NoError(t, err)
	refsResp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	_, err = io.ReadAll(refsResp.Body)
	assert.NoError(t, err)
	assert.NoError(t, refsResp.Body.Close())

	packResp, err := postUploadPack(ctx, cachewSrv.URL, repoPath, buildCompleteUploadPackBody(headSHA))
	assert.NoError(t, err)
	_, err = io.ReadAll(packResp.Body)
	assert.NoError(t, err)
	assert.NoError(t, packResp.Body.Close())

	rm := collectMetrics(ctx, t, reader)
	for _, requestType := range []string{"info-refs", "upload-pack"} {
		points := counterPoints(rm, requestsMetric, attribute.NewSet(attribute.String("type", requestType)))
		assert.Equal(t, 1, len(points), "expected one %s series in %s", requestType, requestsMetric)
		assert.Equal(t, int64(1), points[0].Value, "expected one %s request counted", requestType)
	}
}
