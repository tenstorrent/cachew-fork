package git //nolint:testpackage // white-box testing required for the unexported upstream proxy

import (
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
)

type capturingTransport struct {
	req *http.Request
}

func (c *capturingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c.req = req
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("")),
		Request:    req,
	}, nil
}

type upstreamRequest struct {
	URL     string
	Host    string
	Headers http.Header
}

func TestUpstreamProxyForwardingHeaders(t *testing.T) {
	tests := []struct {
		name    string
		inbound http.Header
		want    http.Header
	}{
		{
			name: "AppendsClientToNewChain",
			want: http.Header{"X-Forwarded-For": {"10.0.0.5"}},
		},
		{
			name:    "AppendsClientToExistingChain",
			inbound: http.Header{"X-Forwarded-For": {"192.0.2.1"}},
			want:    http.Header{"X-Forwarded-For": {"192.0.2.1, 10.0.0.5"}},
		},
		{
			name: "DropsForwardedHostAndProto",
			inbound: http.Header{
				"X-Forwarded-Host":  {"cachew.internal"},
				"X-Forwarded-Proto": {"http"},
			},
			want: http.Header{"X-Forwarded-For": {"10.0.0.5"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &capturingTransport{}
			proxy := (&Strategy{}).newUpstreamProxy(transport)
			r := httptest.NewRequestWithContext(t.Context(), http.MethodGet,
				"/git/example.test/org/repo/info/refs?service=git-upload-pack", nil)
			r.RemoteAddr = "10.0.0.5:1234"
			r.SetPathValue("host", "example.test")
			r.SetPathValue("path", "org/repo/info/refs")
			maps.Copy(r.Header, tt.inbound)

			proxy.ServeHTTP(httptest.NewRecorder(), r)

			assert.NotZero(t, transport.req)
			// ReverseProxy blanks User-Agent so Go's default is not sent upstream.
			want := tt.want.Clone()
			want["User-Agent"] = []string{""}
			got := upstreamRequest{
				URL:     transport.req.URL.String(),
				Host:    transport.req.Host,
				Headers: transport.req.Header,
			}
			assert.Equal(t, upstreamRequest{
				URL:     "https://example.test/org/repo/info/refs?service=git-upload-pack",
				Host:    "example.test",
				Headers: want,
			}, got)
		})
	}
}
