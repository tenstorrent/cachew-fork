package logging //nolint:testpackage

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"
	"github.com/lmittmann/tint"
)

// hostile is what a client can put into a logged value: a URL path decodes
// percent-escapes before a handler sees them, so "%1b%5b2K" arrives as an
// erase-line sequence and "%0a" as a newline.
const hostile = "org/repo\x1b[2K\x1b[1AWRN forged line\nsecond\x9bJ\x7f"

func TestControlCharactersCannotReachTheLogSink(t *testing.T) {
	tests := []struct {
		name    string
		handler func(io.Writer) slog.Handler
	}{
		{"Text", func(w io.Writer) slog.Handler {
			return tint.NewHandler(w, &tint.Options{Level: slog.LevelDebug})
		}},
		{"JSON", func(w io.Writer) slog.Handler {
			return &messageHandler{inner: slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug})}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(&sanitizeHandler{inner: test.handler(&buf)}).
				With("host", hostile).WithGroup("request")
			logger.Warn("Rejecting git request target "+hostile, "path", hostile,
				"error", errors.New(hostile), "nested", slog.GroupValue(slog.String("ref", hostile)))

			out := buf.String()
			// tint colours its own output, so the assertion has to name the sequences
			// only the client could have supplied rather than look for ESC at all.
			assert.False(t, strings.Contains(out, "\x1b[2K"), "erase-line sequence reached the sink: %q", out)
			assert.False(t, strings.Contains(out, "\x1b[1A"), "cursor-up sequence reached the sink: %q", out)
			assert.False(t, strings.Contains(out, "\x9b"), "C1 control sequence introducer reached the sink: %q", out)
			assert.False(t, strings.Contains(out, "\x7f"), "DEL reached the sink: %q", out)
			assert.Equal(t, 1, strings.Count(out, "\n"), "record spans more than one line: %q", out)

			// Escaping must preserve the text, so an operator still sees what was sent.
			// The needles omit the backslash because each handler quotes it differently.
			assert.True(t, strings.Count(out, "x{1b}[2K") >= 5, "escaped escape missing from %q", out)
			assert.True(t, strings.Count(out, "x{0a}second") >= 5, "escaped newline missing from %q", out)
		})
	}
}

func TestSanitizeStringLeavesPrintableTextAlone(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"Empty", "", ""},
		{"Printable", "github.com/org/repo", "github.com/org/repo"},
		{"UnicodeIsNotControl", "réf-ünïcode-☃", "réf-ünïcode-☃"},
		{"Escape", "a\x1bb", `a\x{1b}b`},
		{"Newline", "a\nb", `a\x{0a}b`},
		{"CarriageReturn", "a\rb", `a\x{0d}b`},
		{"Tab", "a\tb", `a\x{09}b`},
		{"Null", "a\x00b", `a\x{00}b`},
		{"Delete", "a\x7fb", `a\x{7f}b`},
		{"C1Introducer", "a\u009bb", `a\x{9b}b`},
		{"InvalidUTF8", "a\xffb", `a\x{ff}b`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, sanitizeString(test.in))
		})
	}
}

// TestSanitizeValuePreservesTypes pins that only values that needed escaping are
// rewritten, so that structured sinks keep receiving numbers as numbers.
func TestSanitizeValuePreservesTypes(t *testing.T) {
	tests := []struct {
		name string
		in   slog.Value
		want slog.Value
	}{
		{"Int", slog.IntValue(42), slog.IntValue(42)},
		{"Bool", slog.BoolValue(true), slog.BoolValue(true)},
		{"CleanAny", slog.AnyValue(errors.New("boom")), slog.AnyValue(errors.New("boom"))},
		{"HostileAny", slog.AnyValue(errors.New("a\nb")), slog.StringValue(`a\x{0a}b`)},
		{"Group",
			slog.GroupValue(slog.String("ref", "a\nb"), slog.Int("n", 1)),
			slog.GroupValue(slog.String("ref", `a\x{0a}b`), slog.Int("n", 1))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := sanitizeValue(test.in)
			assert.Equal(t, test.want.Kind(), got.Kind())
			assert.Equal(t, test.want.String(), got.String())
		})
	}
}
