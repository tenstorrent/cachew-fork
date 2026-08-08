package gomod

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"os/exec"

	"github.com/alecthomas/errors"
	"github.com/goproxy/goproxy"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/repoauthz"
	"github.com/tenstorrent/internal-cachew/internal/strategy"
)

// Register registers the module proxy, authorizing private module reads with
// authz. It is the same Authorizer the object API gets, so a private module
// reached through both entry points is probed and cached once rather than once
// per entry point.
func Register(r *strategy.Registry, cloneManager gitclone.ManagerProvider, authz *repoauthz.Authorizer) {
	strategy.Register(r, "gomod", "Caches Go module proxy requests.", func(ctx context.Context, config Config, cache cache.Cache, mux strategy.Mux) (*Strategy, error) {
		return New(ctx, config, cache, mux, cloneManager, authz)
	})
}

type Config struct {
	Proxy        string   `hcl:"proxy,optional" help:"Upstream Go module proxy URL (defaults to proxy.golang.org)" default:"https://proxy.golang.org"`
	PrivatePaths []string `hcl:"private-paths,optional" help:"Module path patterns for private repositories"`
	//nolint:lll
	DisablePrivateModuleAuthz bool `hcl:"disable-private-module-authz,optional" help:"Disable per-request client authorization of private module reads (/gomod/<private module>/@v/...). Independent of the git strategy's disable-upstream-authz. Unsafe: any client that can reach the module proxy can then download the full source of every private repository matching private-paths." default:"false"`
}

type Strategy struct {
	config       Config
	cache        cache.Cache
	logger       *slog.Logger
	proxy        *url.URL
	goproxy      *goproxy.Goproxy
	cloneManager *gitclone.Manager
	// private is nil unless private module support is configured, and authz is
	// consulted only for the module paths it claims.
	private *CompositeFetcher
	authz   *repoauthz.Authorizer
}

var _ strategy.Strategy = (*Strategy)(nil)

// New creates the module proxy strategy. A nil authz gives it an Authorizer of
// its own, which is only right for a standalone instance: cachewd shares one
// across every strategy that authorizes the same repositories.
func New(
	ctx context.Context,
	config Config,
	cache cache.Cache,
	mux strategy.Mux,
	cloneManagerProvider gitclone.ManagerProvider,
	authz *repoauthz.Authorizer,
) (*Strategy, error) {
	if authz == nil {
		authz = repoauthz.NewAuthorizer(nil, 0)
	}
	if len(config.PrivatePaths) > 0 {
		if _, err := exec.LookPath("git"); err != nil {
			return nil, errors.New("git is required for private module support but not found in PATH")
		}
	}

	parsedURL, err := url.Parse(config.Proxy)
	if err != nil {
		return nil, errors.Errorf("invalid proxy URL: %w", err)
	}

	cloneManager, err := cloneManagerProvider()
	if err != nil {
		return nil, errors.Errorf("failed to create clone manager: %w", err)
	}

	s := &Strategy{
		config:       config,
		cache:        cache,
		logger:       logging.FromContext(ctx),
		proxy:        parsedURL,
		cloneManager: cloneManager,
		authz:        authz,
	}

	publicFetcher := &goproxy.GoFetcher{
		Env: []string{
			"GOPROXY=" + config.Proxy,
			"GOSUMDB=off", // Disable checksum database validation in fetcher, to prevent unneccessary double validation
		},
	}

	var fetcher goproxy.Fetcher = publicFetcher

	if len(config.PrivatePaths) > 0 {
		s.cloneManager = cloneManager
		privateFetcher := newPrivateFetcher(s.logger, cloneManager)
		s.private = NewCompositeFetcher(publicFetcher, privateFetcher, config.PrivatePaths)
		fetcher = s.private

		s.logger.InfoContext(ctx, "Configured private module support", "private-paths", config.PrivatePaths)
		if config.DisablePrivateModuleAuthz {
			s.logger.WarnContext(ctx, "Authorization of private module reads is disabled, any client reaching the module proxy can download private repository source")
		}
	}

	s.goproxy = &goproxy.Goproxy{
		Logger:  s.logger,
		Fetcher: fetcher,
		Cacher: &goproxyCacher{
			cache:   cache,
			private: s.privateModuleName,
		},
		ProxiedSumDBs: []string{
			"sum.golang.org https://sum.golang.org",
		},
	}

	s.logger.InfoContext(ctx, "Initialized Go module proxy strategy", "proxy", s.proxy)

	mux.Handle("GET /gomod/{path...}", http.StripPrefix("/gomod", s.authorizeModuleRead(s.goproxy)))

	return s, nil
}

func (s *Strategy) String() string {
	return "gomod:" + s.proxy.Host
}
