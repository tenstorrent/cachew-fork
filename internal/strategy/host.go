package strategy

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/strategy/handler"
)

func RegisterHost(r *Registry) {
	Register(r, "host", "A generic host-based proxying strategy.", NewHost)
}

// HostConfig represents the configuration for the Host strategy.
//
// In HCL it looks something like this:
//
//	host {
//		target = "https://github.com/"
//	}
//
// In this example, the strategy will be mounted under "/github.com".
type HostConfig struct {
	Target  string            `hcl:"target,label" help:"The target URL to proxy requests to."`
	Headers map[string]string `hcl:"headers,optional" help:"Headers to add to upstream requests."`
}

// The Host [Strategy] forwards all GET requests to the specified host, caching the response payloads.
type Host struct {
	target  *url.URL
	cache   cache.Cache
	client  *http.Client
	prefix  string
	headers map[string]string
}

var _ Strategy = (*Host)(nil)

func NewHost(_ context.Context, config HostConfig, cache cache.Cache, mux Mux) (*Host, error) {
	u, err := url.Parse(config.Target)
	if err != nil {
		return nil, errors.Errorf("invalid target URL: %w", err)
	}
	prefix := "/" + u.Host + strings.TrimSuffix(u.EscapedPath(), "/")
	h := &Host{
		target:  u,
		cache:   cache,
		client:  &http.Client{},
		prefix:  prefix,
		headers: config.Headers,
	}

	hdlr := handler.New(h.client, cache).
		CacheKey(func(r *http.Request) string {
			return h.buildTargetURL(r).String()
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			targetURL := h.buildTargetURL(r)
			req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL.String(), nil)
			if err != nil {
				return nil, errors.Wrap(err, "creating upstream request")
			}
			for k, v := range h.headers {
				req.Header.Set(k, v)
			}
			return req, nil
		})

	mux.Handle("GET "+prefix+"/", hdlr)
	return h, nil
}

func (d *Host) String() string { return "host:" + d.target.Host + d.target.Path }

// buildTargetURL constructs the target URL from the incoming request.
func (d *Host) buildTargetURL(r *http.Request) *url.URL {
	// Strip the prefix from the request path
	reqPath := r.URL.Path
	if len(reqPath) >= len(d.prefix) {
		reqPath = reqPath[len(d.prefix):]
	}
	if reqPath == "" {
		reqPath = "/"
	}

	targetURL := *d.target
	// d.target.Path is the configured target's own path prefix (e.g. "/a"), which must be
	// preserved and joined with reqPath, not overwritten by it.
	targetURL.Path = strings.TrimSuffix(d.target.Path, "/") + reqPath
	targetURL.RawPath = ""
	targetURL.RawQuery = r.URL.RawQuery
	return &targetURL
}
