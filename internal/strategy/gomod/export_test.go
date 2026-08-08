package gomod

import (
	"context"
	"io"
	"net/http"

	"github.com/tenstorrent/internal-cachew/internal/cache"
)

// SetAuthzHTTPClient points the strategy's upstream authorization probes at a
// test client.
func (s *Strategy) SetAuthzHTTPClient(client *http.Client) { s.authz.SetHTTPClient(client) }

// TestableGoproxyCacher exposes goproxyCacher.Put for external tests.
type TestableGoproxyCacher struct {
	inner goproxyCacher
}

// NewTestableGoproxyCacher creates a testable goproxy cacher for the given cache.
func NewTestableGoproxyCacher(c cache.Cache) *TestableGoproxyCacher {
	return &TestableGoproxyCacher{inner: goproxyCacher{cache: c}}
}

// Put delegates to goproxyCacher.Put.
func (t *TestableGoproxyCacher) Put(ctx context.Context, name string, content io.ReadSeeker) error {
	return t.inner.Put(ctx, name, content)
}

// CheckoutRef exposes checkoutRef for external tests.
func CheckoutRef(version string) (string, bool) { return checkoutRef(version) }
