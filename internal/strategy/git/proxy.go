package git

import (
	"net/http"
	"strings"

	"github.com/tenstorrent/internal-cachew/internal/logging"
)

func (s *Strategy) forwardToUpstream(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	logger := logging.FromContext(r.Context())

	logger.DebugContext(r.Context(), "Forwarding to upstream", "method", r.Method, "host", host, "path", pathValue)

	s.proxy.ServeHTTP(w, r)
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
