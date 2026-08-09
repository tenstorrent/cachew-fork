package strategy_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/metrics"
	"github.com/tenstorrent/internal-cachew/internal/strategy"
)

func TestNewAPIV1RecordsAuthzDisabled(t *testing.T) {
	for _, tt := range []struct {
		name     string
		disabled bool
		want     string
	}{
		{"Enabled", false, `cachew_authz_disabled{check="object_api",otel_scope_name="cachew.apiv1",otel_scope_schema_url="",otel_scope_version=""} 0`},
		{"Disabled", true, `cachew_authz_disabled{check="object_api",otel_scope_name="cachew.apiv1",otel_scope_schema_url="",otel_scope_version=""} 1`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})

			prevProvider := otel.GetMeterProvider()
			t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })

			client, err := metrics.New(ctx, metrics.Config{
				ServiceName:      "cachew-apiv1-authz-test",
				Port:             0,
				EnablePrometheus: true,
			})
			assert.NoError(t, err)
			defer client.Close()

			memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
			assert.NoError(t, err)
			defer memCache.Close()

			mux := http.NewServeMux()
			_, err = strategy.NewAPIV1(ctx, strategy.APIV1Config{DisableGitObjectAuthz: tt.disabled}, memCache, mux, nil, nil)
			assert.NoError(t, err)

			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			w := httptest.NewRecorder()
			client.Handler().ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Contains(t, w.Body.String(), tt.want)
		})
	}
}
