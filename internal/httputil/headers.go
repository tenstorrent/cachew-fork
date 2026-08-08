package httputil

import "net/http"

// TransportHeaders are headers added by the HTTP transport layer that should not be cached.
var TransportHeaders = []string{ //nolint:gochecknoglobals
	"Content-Length",
	"Date",
	"Accept-Encoding",
	"User-Agent",
	"Transfer-Encoding",
	"Time-To-Live",
	"If-Match",
	"If-None-Match",
	"Range",
	"If-Range",
	"Content-Range",
}

// CredentialHeaders carry caller credentials and must never be persisted as
// cached object metadata: stored metadata is replayed to whoever later reads the
// object, which would hand one caller's credentials to the next.
var CredentialHeaders = []string{ //nolint:gochecknoglobals
	"Authorization",
	"Proxy-Authorization",
	"Cookie",
	"Set-Cookie",
}

// HopByHopHeaders are hop-by-hop headers that should not be forwarded by proxies (RFC 7230).
var HopByHopHeaders = []string{ //nolint:gochecknoglobals
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailers",
	"Transfer-Encoding",
	"Upgrade",
	"Host",
}

// FilterHeaders returns a copy of headers with the specified header keys removed.
func FilterHeaders(headers http.Header, skip ...string) http.Header {
	skipSet := make(map[string]bool, len(skip))
	for _, s := range skip {
		skipSet[http.CanonicalHeaderKey(s)] = true
	}
	filtered := make(http.Header, len(headers))
	for key, values := range headers {
		if skipSet[http.CanonicalHeaderKey(key)] {
			continue
		}
		filtered[key] = values
	}
	return filtered
}
