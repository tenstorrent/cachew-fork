// Package logging provides logging configuration and utility functions.
package logging

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/alecthomas/errors"
	"github.com/lmittmann/tint"
)

type Config struct {
	JSON bool `hcl:"json,optional" help:"Enable JSON logging."`
	// Decoded as a string rather than slog.Level: alecthomas/hcl v2.6.0 has no
	// TextUnmarshaler branch in its default-value decode path, so a typed
	// slog.Level field can never receive its "info" default (and non-string
	// HCL values panic in the library's JSON-unmarshal fallback). Parsed and
	// validated ourselves in parseLevel.
	Level   string            `hcl:"level,optional" help:"Set the logging level." default:"info"`
	Remap   map[string]string `hcl:"remap,optional" help:"Remap field names from old to new (e.g., msg=message, time=timestamp)."`
	Headers map[string]string `hcl:"headers,optional" help:"Propagate these inbound request headers to the given log attribute."`
}

// parseLevel parses s (e.g. "debug", "info", "warn+4") into an slog.Level,
// reusing slog.Level's own text format rather than hand-rolling one. An empty
// string, as constructed by a zero-value Config, means "use the default".
func parseLevel(s string) (slog.Level, error) {
	if s == "" {
		return slog.LevelInfo, nil
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return 0, errors.Wrapf(err, "invalid log level %q", s)
	}
	return level, nil
}

// Middleware returns an HTTP middleware that logs incoming requests and attaches
// any configured headers as log attributes.
func Middleware(next http.Handler, config Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// Propagate attributes tot the handlers.
		logger := FromContext(ctx).With("method", r.Method, "uri", r.RequestURI)
		start := time.Now()
		logger.Debug("Request received")
		var attrs []any
		for header, attr := range config.Headers {
			if h := r.Header.Get(header); h != "" {
				attrs = append(attrs, slog.String(attr, h))
			}
		}
		if len(attrs) > 0 {
			logger = logger.With(attrs...)
			r = r.WithContext(ContextWithLogger(ctx, logger))
		}
		next.ServeHTTP(w, r)
		logger.Debug("Request complete", "elapsed", time.Since(start))
	})
}

var levelVar = &slog.LevelVar{} //nolint:gochecknoglobals

type logKey struct{}

// SetLevel sets the global log level at runtime.
func SetLevel(level slog.Level) { levelVar.Set(level) }

// GetLevel returns the current global log level.
func GetLevel() slog.Level { return levelVar.Level() }

// Configure sets up logging with the given config and returns the logger and updated context.
func Configure(ctx context.Context, config Config) (*slog.Logger, context.Context, error) {
	level, err := parseLevel(config.Level)
	if err != nil {
		return nil, ctx, errors.Errorf("configure logging: %w", err)
	}
	levelVar.Set(level)

	var handler slog.Handler
	if config.JSON {
		options := &slog.HandlerOptions{Level: levelVar}
		if len(config.Remap) > 0 {
			options.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
				if len(groups) > 0 {
					return a
				}
				if newName, ok := config.Remap[a.Key]; ok {
					a.Key = newName
				}
				return a
			}
		}
		handler = &messageHandler{inner: slog.NewJSONHandler(os.Stdout, options)}
	} else {
		handler = tint.NewHandler(os.Stderr, &tint.Options{
			Level: levelVar,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey && len(groups) == 0 {
					return slog.Attr{}
				}
				return a
			},
		})
	}
	logger := slog.New(&sanitizeHandler{inner: handler})
	return logger, context.WithValue(ctx, logKey{}, logger), nil
}

func FromContext(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(logKey{}).(*slog.Logger)
	if !ok {
		panic("no logger in context")
	}
	return logger
}

// ContextWithLogger returns a new context with the given logger.
func ContextWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, logKey{}, logger)
}
