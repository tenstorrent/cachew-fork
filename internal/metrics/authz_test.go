package metrics_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/metrics"
	"github.com/tenstorrent/internal-cachew/internal/opa"
)

func allowAll(next http.Handler) http.Handler { return next }

// freePort reserves a port and releases it, so the metrics server can bind it
// without colliding with a parallel test.
func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert
	assert.NoError(t, listener.Close())
	return port
}

// TestMetricsListenerIsGatedByPolicy pins the leak this closes: the metrics
// listener was a structurally separate http.Server with no middleware, so it
// served on every interface regardless of the configured policy. Several series
// are labelled with the org/repo name of every repository cachew has checked or
// mirrored, so reaching the port enumerated private repository names without
// passing any repository-level authorization.
func TestMetricsListenerIsGatedByPolicy(t *testing.T) {
	tests := []struct {
		name       string
		policy     string
		wantStatus int
	}{
		{name: "DenyPolicyRejects", policy: "package cachew.authz\n\ndefault allow := false\n",
			wantStatus: http.StatusForbidden},
		{name: "AllowPolicyServes", policy: "package cachew.authz\n\ndefault allow := true\n",
			wantStatus: http.StatusOK},
		// The shipped default must not leak on a fresh deployment. Requests
		// here arrive from 127.0.0.1, which the default policy permits.
		{name: "DefaultPolicyAllowsLoopback", wantStatus: http.StatusOK},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx, _ := logging.Configure(t.Context(), logging.Config{})
			port := freePort(t)
			client, err := metrics.New(ctx, metrics.Config{
				ServiceName:      "cachew-authz-test",
				Port:             port,
				EnablePrometheus: true,
			})
			assert.NoError(t, err)
			t.Cleanup(func() { _ = client.Close() })

			authorizer, err := opa.New(ctx, opa.Config{Policy: test.policy})
			assert.NoError(t, err)
			assert.NoError(t, client.ServeMetrics(ctx, authorizer.Middleware))

			resp := getWhenReady(t, fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			assert.NoError(t, err)

			assert.Equal(t, test.wantStatus, resp.StatusCode)
			if test.wantStatus != http.StatusOK {
				assert.NotContains(t, string(body), "go_goroutines")
			}
		})
	}
}

// TestDefaultPolicyDeniesRemoteMetrics pins that the shipped default treats
// /metrics like /api and /admin rather than falling into the permissive branch.
func TestDefaultPolicyDeniesRemoteMetrics(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{})
	authorizer, err := opa.New(ctx, opa.Config{})
	assert.NoError(t, err)

	handler := authorizer.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "go_goroutines 1")
	}))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil).WithContext(ctx)
	req.RemoteAddr = "10.0.0.1:9999"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusForbidden, recorder.Code)
	assert.NotContains(t, recorder.Body.String(), "go_goroutines")
}

// TestServeMetricsRequiresAuthorization pins that the gate cannot be dropped by
// omission: a caller that passes no middleware gets an error instead of an
// ungated listener.
func TestServeMetricsRequiresAuthorization(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{})
	client, err := metrics.New(ctx, metrics.Config{
		ServiceName:      "cachew-authz-nil",
		Port:             freePort(t),
		EnablePrometheus: true,
	})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	err = client.ServeMetrics(ctx, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "requires an authorization middleware")
}

// getWhenReady polls until the metrics listener accepts connections, since
// ServeMetrics starts it in the background.
func getWhenReady(t *testing.T, url string) *http.Response {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		assert.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			return resp
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics listener never became ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
