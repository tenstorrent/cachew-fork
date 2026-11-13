package transfer_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/tenstorrent/internal-cachew/internal/transfer"
)

const receivedMetric = "cachew.upstream.received_bytes_total"

func setupReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	prev := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
	return reader
}

// received returns the counter value for each source/strategy pair.
func received(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(t.Context(), &rm))
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != receivedMetric || !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				source, _ := dp.Attributes.Value(attribute.Key("source"))
				strategy, _ := dp.Attributes.Value(attribute.Key("strategy"))
				out[source.AsString()+"/"+strategy.AsString()] = dp.Value
			}
		}
	}
	return out
}

func TestClientCountsResponseBodyBytes(t *testing.T) {
	body := strings.Repeat("x", 1000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)

	tests := []struct {
		name    string
		consume func(t *testing.T, resp *http.Response)
		want    map[string]int64
	}{
		{
			name: "DrainedAndClosed",
			consume: func(t *testing.T, resp *http.Response) {
				t.Helper()
				_, err := io.Copy(io.Discard, resp.Body)
				assert.NoError(t, err)
				assert.NoError(t, resp.Body.Close())
			},
			want: map[string]int64{"http/test": 1000},
		},
		{
			name: "DrainedWithoutClose",
			consume: func(t *testing.T, resp *http.Response) {
				t.Helper()
				_, err := io.Copy(io.Discard, resp.Body)
				assert.NoError(t, err)
			},
			want: map[string]int64{"http/test": 1000},
		},
		{
			name: "ClosedPartwayCountsWhatWasRead",
			consume: func(t *testing.T, resp *http.Response) {
				t.Helper()
				_, err := io.ReadFull(resp.Body, make([]byte, 10))
				assert.NoError(t, err)
				assert.NoError(t, resp.Body.Close())
			},
			want: map[string]int64{"http/test": 10},
		},
		{
			name: "ClosedUnreadRecordsNothing",
			consume: func(t *testing.T, resp *http.Response) {
				t.Helper()
				assert.NoError(t, resp.Body.Close())
			},
			want: map[string]int64{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := setupReader(t)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
			assert.NoError(t, err)

			resp, err := transfer.NewClient("test").Do(req)
			assert.NoError(t, err)
			tt.consume(t, resp)
			_ = resp.Body.Close()

			assert.Equal(t, tt.want, received(t, reader))
		})
	}
}

type fixedResponse struct{ resp *http.Response }

func (f fixedResponse) RoundTrip(*http.Request) (*http.Response, error) { return f.resp, nil }

func TestTransportLeavesProtocolSwitchBodyUnwrapped(t *testing.T) {
	body := io.NopCloser(strings.NewReader(""))
	transport := transfer.NewTransport(fixedResponse{&http.Response{StatusCode: http.StatusSwitchingProtocols, Body: body}},
		"test")
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.test/", nil)

	resp, err := transport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, body, resp.Body, "an upgraded connection must reach the caller as the original body")
}
