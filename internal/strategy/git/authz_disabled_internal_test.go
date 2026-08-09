package git

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"

	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/metrics"
)

func TestNewGitMetricsRecordsAuthzDisabled(t *testing.T) {
	for _, tt := range []struct {
		name     string
		disabled bool
		want     string
	}{
		{"Enabled", false, `cachew_authz_disabled{check="upstream",otel_scope_name="cachew.git",otel_scope_schema_url="",otel_scope_version=""} 0`},
		{"Disabled", true, `cachew_authz_disabled{check="upstream",otel_scope_name="cachew.git",otel_scope_schema_url="",otel_scope_version=""} 1`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx, _ := logging.Configure(context.Background(), logging.Config{})

			prevProvider := otel.GetMeterProvider()
			t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })

			client, err := metrics.New(ctx, metrics.Config{
				ServiceName:      "cachew-authz-test",
				Port:             0,
				EnablePrometheus: true,
			})
			assert.NoError(t, err)
			defer client.Close()

			newGitMetrics(ctx, Config{DisableUpstreamAuthz: tt.disabled}, logging.FromContext(ctx))

			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			w := httptest.NewRecorder()
			client.Handler().ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Contains(t, w.Body.String(), tt.want)
		})
	}
}
