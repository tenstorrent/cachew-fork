package strategy_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/hcl/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

// Regression test for the upstream github.com/alecthomas/hcl/v2 bug where
// AllowExtra(ok bool) ignores ok and always allows extra fields. Registered
// strategy config blocks must reject unrecognized attributes instead of
// silently ignoring them.
func TestRegisterRejectsUnrecognizedAttribute(t *testing.T) {
	tests := []struct {
		name      string
		config    string
		expectErr string
	}{
		{
			name:   "ValidConfig",
			config: `host "https://example.com" {}`,
		},
		{
			name:      "UnrecognizedAttribute",
			config:    `host "https://example.com" { bogus-attr = "oops" }`,
			expectErr: "bogus-attr",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
			memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
			assert.NoError(t, err)
			defer memCache.Close()

			ast, err := hcl.Parse(strings.NewReader(tt.config))
			assert.NoError(t, err)
			block, ok := ast.Entries[0].(*hcl.Block)
			assert.True(t, ok)

			sr := strategy.NewRegistry()
			strategy.RegisterHost(sr)

			_, err = sr.Create(ctx, "host", block, memCache, http.NewServeMux(), nil)
			if tt.expectErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectErr)
		})
	}
}
