package strategy

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

// TestWriteProxyErrorHonorsHTTPResponderStatus verifies writeProxyError uses
// the status code carried by an httputil.HTTPResponder (e.g. a wrapped cache
// open/create failure) instead of always collapsing to 502, while a plain,
// non-HTTPResponder error still falls back to 502 as before.
func TestWriteProxyErrorHonorsHTTPResponderStatus(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{
			name:       "HTTPResponderStatusPreserved",
			err:        httputil.Errorf(http.StatusInternalServerError, "failed to open cache: boom"),
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "PlainErrorFallsBackToBadGateway",
			err:        errors.New("boom"),
			wantStatus: http.StatusBadGateway,
		},
	}

	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/", nil)
			w := httptest.NewRecorder()
			writeProxyError(tt.err, w, r)
			assert.Equal(t, tt.wantStatus, w.Code)
		})
	}
}
