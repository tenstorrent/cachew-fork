package logging //nolint:testpackage

import (
	"context"
	"log/slog"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/hcl/v2"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      slog.Level
		wantError bool
	}{
		{name: "Debug", input: "debug", want: slog.LevelDebug},
		{name: "Info", input: "info", want: slog.LevelInfo},
		{name: "Warn", input: "warn", want: slog.LevelWarn},
		{name: "Error", input: "error", want: slog.LevelError},
		{name: "CaseInsensitive", input: "DEBUG", want: slog.LevelDebug},
		{name: "Empty", input: "", want: slog.LevelInfo},
		{name: "Invalid", input: "bogus", wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLevel(tt.input)
			if tt.wantError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestConfigLevelDefaultFromHCL proves that decoding Level as a plain string
// lets alecthomas/hcl v2.6.0 apply the `default:"info"` tag correctly, which
// it cannot do for a typed slog.Level field (no TextUnmarshaler branch in its
// default-decode path). Omitting the "log" block entirely, as
// hcl.HydratedImplicitBlocks(true) does for cachewd, hits this same path.
func TestConfigLevelDefaultFromHCL(t *testing.T) {
	var cfg Config
	assert.NoError(t, hcl.Unmarshal([]byte(``), &cfg))
	assert.Equal(t, "info", cfg.Level)

	logger, ctx, err := Configure(context.Background(), cfg)
	assert.NoError(t, err)
	assert.NotZero(t, logger)
	assert.NotZero(t, ctx)
	assert.Equal(t, slog.LevelInfo, GetLevel())
}

// TestConfigNonStringLevelDoesNotPanic proves that a non-string "level" value
// no longer reaches alecthomas/hcl's broken JSON-unmarshal fallback (which
// nil-pointer panics for a typed slog.Level field) — decoding into a plain
// string now just yields an ordinary decode error.
func TestConfigNonStringLevelDoesNotPanic(t *testing.T) {
	var cfg Config
	assert.Error(t, hcl.Unmarshal([]byte(`level = 4`), &cfg))
}

// TestConfigureInvalidLevel proves that a syntactically valid but unrecognised
// level string produces a clean error from Configure rather than a panic.
func TestConfigureInvalidLevel(t *testing.T) {
	var cfg Config
	assert.NoError(t, hcl.Unmarshal([]byte(`level = "bogus"`), &cfg))

	_, _, err := Configure(context.Background(), cfg)
	assert.Error(t, err)
}

// TestConfigureZeroValueLevel proves a bare Config{} (as used throughout the
// test suite, bypassing HCL decoding entirely) still yields LevelInfo, matching
// slog.Level's old zero-value behaviour now that empty means "default".
func TestConfigureZeroValueLevel(t *testing.T) {
	_, ctx, err := Configure(context.Background(), Config{})
	assert.NoError(t, err)
	assert.Equal(t, slog.LevelInfo, GetLevel())
	assert.NotZero(t, FromContext(ctx))
}

func TestConfigureValidLevels(t *testing.T) {
	tests := []struct {
		name  string
		level string
		want  slog.Level
	}{
		{name: "Debug", level: "debug", want: slog.LevelDebug},
		{name: "Info", level: "info", want: slog.LevelInfo},
		{name: "Warn", level: "warn", want: slog.LevelWarn},
		{name: "Error", level: "error", want: slog.LevelError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Configure(context.Background(), Config{Level: tt.level})
			assert.NoError(t, err)
			assert.Equal(t, tt.want, GetLevel())
		})
	}
}
