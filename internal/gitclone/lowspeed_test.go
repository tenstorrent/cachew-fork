package gitclone //nolint:testpackage // white-box testing required to build a mirror around a local HTTP upstream

import (
	"crypto/rand"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/logging"
)

// throttledWriter trickles a response out in small chunks, standing in for an
// upstream connection routed over a slow link.
type throttledWriter struct {
	http.ResponseWriter
}

func (w throttledWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		chunk := p[:min(len(p), 2048)]
		n, err := w.ResponseWriter.Write(chunk)
		written += n
		if err != nil {
			return written, err //nolint:wrapcheck
		}
		if f, ok := w.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
		p = p[len(chunk):]
		time.Sleep(50 * time.Millisecond)
	}
	return written, nil
}

// gitHTTPUpstream serves the bare repositories under root over smart HTTP. The
// first slowAttempts fetches are throttled; failAll answers every request with
// a server error instead. It returns the base URL and a counter of fetch
// attempts, one per info/refs request.
func gitHTTPUpstream(t *testing.T, root string, slowAttempts int32, failAll bool) (string, *atomic.Int32) {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	assert.NoError(t, err)
	backend := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env:  []string{"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "PATH=" + os.Getenv("PATH")},
	}
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/info/refs") {
			attempts.Add(1)
		}
		if failAll {
			http.Error(w, "upstream unavailable", http.StatusInternalServerError)
			return
		}
		if r.Method == http.MethodPost && attempts.Load() <= slowAttempts {
			backend.ServeHTTP(throttledWriter{w}, r)
			return
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server.URL, &attempts
}

func TestFetchRetriesSlowTransfer(t *testing.T) {
	tests := []struct {
		name         string
		slowAttempts int32
		failAll      bool
		wantErr      bool
		wantAttempts int32
	}{
		{name: "FastFetchRunsOnce", wantAttempts: 1},
		{name: "SlowAttemptIsRetried", slowAttempts: 1, wantAttempts: 2},
		{name: "OtherFailureIsNotRetried", failAll: true, wantErr: true, wantAttempts: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
			tmpDir := t.TempDir()
			upstreamPath := createBareRepo(t, tmpDir)

			cfg := testRepoConfig()
			cfg.FetchLowSpeedLimit = 100 * 1024
			cfg.FetchLowSpeedTime = time.Second
			repo := &Repository{
				state:       StateEmpty,
				config:      cfg,
				path:        filepath.Join(tmpDir, "mirror"),
				upstreamURL: "file://" + upstreamPath,
				fetchSem:    make(chan struct{}, 1),
			}
			repo.fetchSem <- struct{}{}
			assert.NoError(t, repo.Clone(ctx))

			// Large and incompressible, so a throttled transfer takes far longer
			// than the low-speed window and is aborted mid-pack.
			work := filepath.Join(tmpDir, "push")
			run := func(args ...string) {
				out, err := exec.Command("git", args...).CombinedOutput() //nolint:gosec // test-owned temp paths
				assert.NoError(t, err, string(out))
			}
			run("clone", upstreamPath, work)
			blob := make([]byte, 1<<20)
			_, _ = rand.Read(blob)
			assert.NoError(t, os.WriteFile(filepath.Join(work, "big.bin"), blob, 0o600))
			run("-C", work, "add", ".")
			run("-C", work, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "big")
			run("-C", work, "push", "origin", "HEAD")
			want := gitOutput(t, upstreamPath, "rev-parse", "HEAD")

			baseURL, attempts := gitHTTPUpstream(t, tmpDir, tt.slowAttempts, tt.failAll)
			gitOutput(t, repo.path, "config", "remote.origin.url", baseURL+"/upstream.git")

			err := repo.FetchWithTimeout(ctx, 30*time.Second)

			assert.Equal(t, tt.wantErr, err != nil, "fetch error: %v", err)
			assert.Equal(t, tt.wantAttempts, attempts.Load())
			if !tt.wantErr {
				assert.True(t, objectExists(t, repo.path, want), "the retried fetch must bring in the new commit")
			}
		})
	}
}

func TestNewManagerFetchLowSpeedConfig(t *testing.T) {
	tests := []struct {
		name      string
		limit     int
		window    time.Duration
		wantErr   bool
		wantLimit int
		wantTime  time.Duration
	}{
		{name: "UnsetUsesDefaults", wantLimit: 150 * 1024, wantTime: 60 * time.Second},
		{name: "ConfiguredIsKept", limit: 1024, window: 5 * time.Second, wantLimit: 1024, wantTime: 5 * time.Second},
		{name: "NegativeLimitIsRejected", limit: -1, wantErr: true},
		{name: "SubSecondWindowIsRejected", window: 500 * time.Millisecond, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
			manager, err := NewManager(ctx, Config{MirrorRoot: t.TempDir(), FetchLowSpeedLimit: tt.limit,
				FetchLowSpeedTime: tt.window}, nil)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantLimit, manager.config.FetchLowSpeedLimit)
			assert.Equal(t, tt.wantTime, manager.config.FetchLowSpeedTime)
		})
	}
}
