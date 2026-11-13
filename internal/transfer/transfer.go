// Package transfer counts the bytes cachew receives from the upstream servers it
// caches, so they can be compared with the bytes it serves to clients.
package transfer

import (
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/tenstorrent/internal-cachew/internal/metrics"
)

// Values of the source label on the received-bytes counter.
const (
	// SourceHTTP is response bodies read from upstream over Go's HTTP client.
	SourceHTTP = "http"
	// SourceGit is pack data git clone and git fetch subprocesses wrote to a mirror.
	SourceGit = "git"
	// SourceLFS is objects git lfs fetch subprocesses downloaded.
	SourceLFS = "lfs"
)

// RecordReceived adds n bytes received from upstream by strategy through source.
func RecordReceived(ctx context.Context, source, strategy string, n int64) {
	if n <= 0 {
		return
	}
	// Resolved per call rather than once, so the counter follows the current
	// global meter provider. Calls are per response or per fetch, never per Read.
	counter := metrics.NewMetric[metric.Int64Counter](otel.Meter("cachew.upstream"),
		"cachew.upstream.received_bytes_total", "By",
		"Bytes received from upstream servers since process start, by source (http, git, lfs) and strategy")
	counter.Add(ctx, n, metric.WithAttributes(
		attribute.String("source", source),
		attribute.String("strategy", strategy),
	))
}

// NewTransport wraps base so every response body read through it counts as
// bytes received from upstream by strategy. A nil base means
// http.DefaultTransport.
func NewTransport(base http.RoundTripper, strategy string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &countingTransport{base: base, strategy: strategy}
}

// NewClient returns an HTTP client whose response bodies count as bytes
// received from upstream by strategy.
func NewClient(strategy string) *http.Client {
	return &http.Client{Transport: NewTransport(nil, strategy)}
}

type countingTransport struct {
	base     http.RoundTripper
	strategy string
}

func (t *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	// A protocol switch hands the body over as a raw connection that callers
	// type-assert to io.ReadWriteCloser, which a wrapper would hide.
	if err != nil || resp.Body == nil || resp.StatusCode == http.StatusSwitchingProtocols {
		return resp, err //nolint:wrapcheck
	}
	resp.Body = &countingBody{ReadCloser: resp.Body, ctx: req.Context(), strategy: t.strategy}
	return resp, nil
}

// countingBody records what was read once, at EOF or Close, whichever comes
// first: a caller may stop reading without draining, or drain without closing.
type countingBody struct {
	io.ReadCloser
	ctx      context.Context //nolint:containedctx
	strategy string
	n        atomic.Int64
	once     sync.Once
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.n.Add(int64(n))
	if err == io.EOF {
		b.record()
	}
	return n, err //nolint:wrapcheck
}

func (b *countingBody) Close() error {
	b.record()
	return b.ReadCloser.Close() //nolint:wrapcheck
}

func (b *countingBody) record() {
	b.once.Do(func() { RecordReceived(b.ctx, SourceHTTP, b.strategy, b.n.Load()) })
}
