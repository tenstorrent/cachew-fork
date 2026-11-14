package metrics_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/tenstorrent/internal-cachew/internal/metrics"
)

func TestRegisterBuildInfo(t *testing.T) {
	prev := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		assert.NoError(t, provider.Shutdown(t.Context()))
	})

	metrics.RegisterBuildInfo("f590998", "f59099822c01fb415fb9c75f082011e7bfdb2a88")

	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(t.Context(), &rm))
	var got []metricdata.DataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok && m.Name == "cachew.build_info" {
				got = append(got, gauge.DataPoints...)
			}
		}
	}
	want := []metricdata.DataPoint[int64]{{
		Attributes: attribute.NewSet(
			attribute.String("version", "f590998"),
			attribute.String("git_commit", "f59099822c01fb415fb9c75f082011e7bfdb2a88"),
			attribute.String("go_version", runtime.Version()),
		),
		Value: 1,
	}}
	assert.Equal(t, want, got, assert.Exclude[time.Time]())
}
