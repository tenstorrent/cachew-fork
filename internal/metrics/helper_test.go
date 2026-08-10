package metrics_test

import (
	"os"
	"os/exec"
	"testing"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/tenstorrent/internal-cachew/internal/metrics"
)

const pristineProcessEnv = "CACHEW_TEST_PRISTINE_PROCESS"

func TestNewMetric(t *testing.T) {
	meter := otel.Meter("cachew.scheduler")
	queueDepth := metrics.NewMetric[metric.Int64Gauge](meter, "cachew.scheduler.queue_depth", "{jobs}",
		"Number of jobs waiting in the scheduler queue")
	queueDepth.Record(t.Context(), 10)
}

func TestNewConstGaugeSurvivesRegistrationBeforeSetMeterProvider(t *testing.T) {
	if os.Getenv(pristineProcessEnv) == "" {
		rerunInPristineProcess(t)
		return
	}

	prevProvider := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prevProvider) })

	metrics.NewConstGauge(otel.Meter("cachew.test"), "cachew.test.flag_enabled", "",
		"1 if the example flag is set, 0 otherwise", 1, attribute.String("check", "example"))

	reader := sdkmetric.NewManualReader(
		sdkmetric.WithTemporalitySelector(func(sdkmetric.InstrumentKind) metricdata.Temporality {
			return metricdata.DeltaTemporality
		}),
	)
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { assert.NoError(t, provider.Shutdown(t.Context())) })
	otel.SetMeterProvider(provider)

	for range 2 {
		var rm metricdata.ResourceMetrics
		assert.NoError(t, reader.Collect(t.Context(), &rm))
		assert.Equal(t, int64(1), gaugeValue(t, &rm, "cachew.test.flag_enabled"))
	}
}

func rerunInPristineProcess(t *testing.T) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	cmd.Env = append(os.Environ(), pristineProcessEnv+"=1")
	out, err := cmd.CombinedOutput()
	assert.NoError(t, err, "otel installs the global meter provider's delegate once per process, and sibling "+
		"tests here install one first, so the startup ordering under test only reproduces in a fresh process:\n%s", out)
}

func gaugeValue(t *testing.T, rm *metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			assert.True(t, ok, "%s is %T, want metricdata.Gauge[int64]", name, m.Data)
			assert.Equal(t, 1, len(gauge.DataPoints), "%s data points", name)
			return gauge.DataPoints[0].Value
		}
	}
	t.Fatalf("gauge %s not collected", name)
	return 0
}
