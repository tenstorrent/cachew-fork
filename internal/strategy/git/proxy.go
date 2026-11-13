package git

import (
	"net/http"
	"net/http/httputil"
	"strings"

	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/transfer"
)

func (s *Strategy) forwardToUpstream(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	logger := logging.FromContext(r.Context())

	logger.DebugContext(r.Context(), "Forwarding to upstream", "method", r.Method, "host", host, "path", pathValue)

	s.proxy.ServeHTTP(w, r)
}

// newUpstreamProxy builds the reverse proxy that forwards requests to upstreams,
// counting the bytes it receives from them.
func (s *Strategy) newUpstreamProxy(transport http.RoundTripper) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite:   s.rewriteUpstream,
		Transport: transfer.NewTransport(transport, "git"),
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			logging.FromContext(r.Context()).ErrorContext(r.Context(), "Upstream request failed", "error", err)
			w.WriteHeader(http.StatusBadGateway)
		},
	}
}

// rewriteUpstream retargets an outbound proxy request at its upstream.
//
// Rewrite strips inbound forwarding headers, where the deprecated Director
// appended the client to them. Carrying X-Forwarded-For across keeps the chain
// upstreams see unchanged. SetXForwarded's Host and Proto headers are dropped
// because they would expose cachew's own address to every upstream.
func (s *Strategy) rewriteUpstream(pr *httputil.ProxyRequest) {
	pr.Out.Header["X-Forwarded-For"] = pr.In.Header["X-Forwarded-For"]
	pr.SetXForwarded()
	pr.Out.Header.Del("X-Forwarded-Host")
	pr.Out.Header.Del("X-Forwarded-Proto")
	s.directUpstream(pr.Out)
}

// directUpstream retargets a cachew request at the upstream it proxies for.
//
// cachew's GitHub App credentials are attached only on paths where the client
// has already been authorized against this repository; on pass-through paths
// the client's Authorization header is left exactly as it arrived, absent
// included, so upstream authorizes the client rather than cachew. See
// mayUseServiceCredentials.
func (s *Strategy) directUpstream(req *http.Request) {
	pathValue := req.PathValue("path")

	req.URL.Scheme = "https"
	req.URL.Host = req.PathValue("host")
	req.URL.Path = "/" + pathValue
	req.Host = req.URL.Host

	if s.serviceToken == nil || req.URL.Host != "github.com" || !mayUseServiceCredentials(req, pathValue) {
		return
	}
	org, _, _ := strings.Cut(strings.TrimPrefix(req.URL.Path, "/"), "/")
	if org == "" {
		return
	}
	ctx := req.Context()
	token, err := s.serviceToken(ctx, org)
	if err != nil || token == "" {
		return
	}
	req.SetBasicAuth("x-access-token", token)
	logging.FromContext(ctx).DebugContext(ctx, "Injecting GitHub App auth into upstream request", "org", org)
}
