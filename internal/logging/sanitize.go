package logging

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"unicode/utf8"

	"github.com/alecthomas/errors"
)

// sanitizeHandler escapes control characters in log records before they reach
// the handler that writes them.
//
// Requests put client-supplied strings into logs: percent-escapes in a URL path
// are decoded before a handler ever sees them, so a value logged from a path
// segment can contain any byte. The text handler deliberately writes ANSI
// escapes through verbatim so that colours survive quoting, which hands a client
// the sequences that erase and rewrite lines an operator has already been shown.
// Escaping in the handler covers every call site at once, including the strings
// that arrive inside errors, rather than relying on each one to remember.
type sanitizeHandler struct {
	inner slog.Handler
}

func (h *sanitizeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *sanitizeHandler) Handle(ctx context.Context, r slog.Record) error {
	clean := slog.NewRecord(r.Time, r.Level, sanitizeString(r.Message), r.PC)
	r.Attrs(func(a slog.Attr) bool {
		clean.AddAttrs(sanitizeAttr(a))
		return true
	})
	return errors.Wrap(h.inner.Handle(ctx, clean), "handle log record")
}

func (h *sanitizeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, 0, len(attrs))
	for _, attr := range attrs {
		clean = append(clean, sanitizeAttr(attr))
	}
	return &sanitizeHandler{inner: h.inner.WithAttrs(clean)}
}

func (h *sanitizeHandler) WithGroup(name string) slog.Handler {
	return &sanitizeHandler{inner: h.inner.WithGroup(sanitizeString(name))}
}

func sanitizeAttr(attr slog.Attr) slog.Attr {
	return slog.Attr{Key: sanitizeString(attr.Key), Value: sanitizeValue(attr.Value)}
}

func sanitizeValue(v slog.Value) slog.Value {
	switch v.Kind() {
	case slog.KindString:
		return slog.StringValue(sanitizeString(v.String()))

	case slog.KindGroup:
		group := v.Group()
		clean := make([]slog.Attr, 0, len(group))
		for _, attr := range group {
			clean = append(clean, sanitizeAttr(attr))
		}
		return slog.GroupValue(clean...)

	case slog.KindLogValuer:
		return sanitizeValue(v.Resolve())

	case slog.KindAny:
		// Errors quote the input that failed, so they need the same treatment as
		// strings. Byte slices are left alone because the handlers already encode
		// them, and fmt renders everything else because it recovers from panics in
		// Error and String methods. The value is only replaced when escaping changed
		// something, so well-behaved values format exactly as they always did.
		if _, ok := v.Any().([]byte); ok {
			return v
		}
		text := fmt.Sprintf("%v", v.Any())
		clean := sanitizeString(text)
		if clean == text {
			return v
		}
		return slog.StringValue(clean)

	// The remaining kinds have no text a client could have supplied: the handler
	// formats them from the value itself.
	case slog.KindBool, slog.KindDuration, slog.KindFloat64,
		slog.KindInt64, slog.KindTime, slog.KindUint64:
		return v

	default:
		return v
	}
}

// sanitizeString rewrites control characters and bytes that are not valid UTF-8
// as their escaped form, returning strings that contain neither unchanged.
//
// Invalid bytes are escaped as well because a lone 0x9b is the C1 control
// sequence introducer to a terminal reading latin-1, and because a sink that
// re-encodes the string would otherwise decide for itself what to do with it.
//
// The braces in the escaped form matter: the text handler turns the literal text
// "\x1b" back into a real escape byte after quoting, so escaping to that
// spelling would hand back exactly what this is removing.
func sanitizeString(s string) string {
	if utf8.ValidString(s) && !strings.ContainsFunc(s, isControl) {
		return s
	}

	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			fmt.Fprintf(&b, `\x{%02x}`, s[i])

		case isControl(r):
			fmt.Fprintf(&b, `\x{%02x}`, r)

		default:
			b.WriteString(s[i : i+size])
		}
		i += size
	}
	return b.String()
}

// isControl reports whether a rune is one a log sink may act on rather than
// print. C1 is included because U+009B introduces a control sequence just as
// ESC-[ does, and newlines because a record should stay one line whatever the
// handler's own quoting does.
func isControl(r rune) bool {
	return r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f)
}
