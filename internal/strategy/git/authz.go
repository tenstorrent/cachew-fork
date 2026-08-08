package git

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/repoauthz"
)

// authorizeServe decides whether the request may be served, and writes the
// denial response itself when it may not.
//
// cachew mirrors repositories with its own service credentials, so anything in
// a warm mirror would otherwise be readable by any client that can reach the
// endpoint. For repositories that upstream refuses to serve anonymously, this
// re-validates the specific client against the specific repository on every
// request, using only the credentials the client itself supplied. Public
// repositories skip the check entirely so the common case pays nothing.
//
// The per-request decision is intentionally NOT cached: a cache would reopen
// the same confused-deputy hole for the lifetime of its entries.
func (s *Strategy) authorizeServe(w http.ResponseWriter, r *http.Request, host, pathValue string) bool {
	repoPath, gated := repoPathForRequest(r, pathValue)
	// Deliberately ahead of the DisableUpstreamAuthz escape hatch: that switch
	// turns off checking the client's credentials, not checking that the target
	// names the mirror it will be served from.
	if gated && s.denyNonCanonicalRepoPath(w, r, host, repoPath, pathValue) {
		return false
	}
	if !gated || s.config.DisableUpstreamAuthz {
		return true
	}

	upstreamURL := "https://" + host + "/" + repoPath
	repoName := host + "/" + repoPath
	ctx, span := tracer.Start(r.Context(), "git.authorize_serve",
		trace.WithAttributes(
			attribute.String("cachew.operation", "authorize_serve"),
			attribute.String("cachew.repository", repoName),
		),
	)
	defer span.End()
	logger := logging.FromContext(ctx)
	start := time.Now()

	outcome := repoauthz.Outcome("denied")
	defer func() {
		span.SetAttributes(attribute.String("cachew.authz_outcome", string(outcome)))
		s.metrics.recordAuthzCheck(ctx, repoName, string(outcome), time.Since(start))
	}()

	outcome, err := s.authz.Authorize(ctx, upstreamURL, r.Header.Get("Authorization"))
	if err != nil {
		logger.WarnContext(ctx, "Upstream authorization probe failed, denying", "upstream", upstreamURL, "error", err)
	}
	switch outcome {
	case repoauthz.OutcomeNoCredentials:
		logger.InfoContext(ctx, "Denying unauthenticated request for auth-required repository", "upstream", upstreamURL)

	case repoauthz.OutcomeRejected:
		logger.InfoContext(ctx, "Denying request, client credentials rejected by upstream", "upstream", upstreamURL)

	case repoauthz.OutcomePublic, repoauthz.OutcomeAllowed, repoauthz.OutcomeProbeError,
		repoauthz.OutcomeInternalError, repoauthz.OutcomeInvalidTarget:
	}
	if !outcome.Allowed() {
		repoauthz.Respond(w, outcome)
		return false
	}
	return true
}

// denyNonCanonicalRepoPath reports whether a gated request's repository path
// fails to name the mirror it would be served from, writing the denial response
// when it does. Reported as an invalid target, not an internal error: every
// input that reaches this failing has a residual ".git" (or similar) that
// validateRepoTarget's segment checks don't catch, which is a property of the
// request, not a runtime failure.
func (s *Strategy) denyNonCanonicalRepoPath(w http.ResponseWriter, r *http.Request, host, repoPath, pathValue string) bool {
	err := validateCanonicalRepoPath(host, repoPath)
	if err == nil {
		return false
	}
	ctx := r.Context()
	logging.FromContext(ctx).WarnContext(ctx, "Denying request, repository target does not name its own mirror",
		"host", host, "path", pathValue, "error", err)
	s.metrics.recordAuthzCheck(ctx, host+"/"+repoPath, string(repoauthz.OutcomeInvalidTarget), 0)
	repoauthz.Respond(w, repoauthz.OutcomeInvalidTarget)
	return true
}

// validateRepoTarget rejects request targets that could resolve to a different
// repository than the one authorization is checked against.
//
// The upstream URL is assembled by concatenating the client-controlled {host}
// and {path...} wildcards, but the mirror directory is derived from that URL
// with filepath.Join. The two disagree about "..": url.Parse keeps it in the
// path while Join collapses it into the host component. Go's ServeMux passes
// %2f through inside a single-segment wildcard, so {host} can carry both a
// separator and a traversal segment — enough to have authorization checked
// against an attacker's authority while the bytes are served from a private
// repository's mirror. Empty and "." segments are refused for the same reason:
// they alias one mirror under several distinct upstream URLs.
//
// Rejecting rather than normalising is deliberate. Normalisation would have to
// match filepath.Join's semantics exactly, forever, in every caller; refusing
// the input keeps the two derivations trivially equal instead.
func validateRepoTarget(host, pathValue string) error {
	if host == "" {
		return errors.New("empty host")
	}
	for _, segment := range append([]string{host}, strings.Split(pathValue, "/")...) {
		switch {
		case segment == "":
			return errors.Errorf("empty path segment in %q", host+"/"+pathValue)

		case segment == "." || segment == "..":
			return errors.Errorf("path traversal segment %q in %q", segment, host+"/"+pathValue)

		// Repositories are named "repo" or "repo.git"; a bare ".git" segment is
		// only a repository name to the ".git"-stripping in the repository path
		// derivation, which turns it into an empty or trailing-slash identity
		// that no longer names what the mirror path points at.
		case segment == ".git":
			return errors.Errorf("bare %q segment in %q", segment, host+"/"+pathValue)

		case strings.ContainsAny(segment, `/\`):
			return errors.Errorf("path separator in segment %q of %q", segment, host+"/"+pathValue)

		case strings.ContainsFunc(segment, func(r rune) bool { return r < 0x20 || r == 0x7f }):
			return errors.Errorf("control character in segment of %q", host+"/"+pathValue)
		}
	}
	return validateResolvesToSameRepo(host, pathValue)
}

// resolvesToSameRepo refuses targets on which the two independent derivations
// of "which repository is this?" disagree.
//
// Segment-level checks alone cannot see this. url.Parse decodes percent-escapes
// a second time (ServeMux already decoded once, so %252e reaches this code as
// %2e and only becomes ".." inside gitclone), and it splits ? and # off the
// path entirely — so a target that looks like one repository here can name a
// different one, or a different endpoint, by the time the mirror path and the
// authorization probe URL are built from it.
//
// Requiring the round trip to be byte-identical is stronger than enumerating
// the offending characters, and stays correct if either derivation changes.
// allowGitSuffix says whether the target may still carry the one ".git" that
// gitclone strips: raw client paths legitimately do, canonical repository paths
// must not.
func resolvesToSameRepo(host, target string, allowGitSuffix bool) error {
	identity := host + "/" + target
	resolved, err := gitclone.RepoPathFromURL("https://" + identity)
	if err != nil {
		return errors.Errorf("target %q does not resolve to a repository: %w", identity, err)
	}
	want := identity
	if allowGitSuffix {
		want = strings.TrimSuffix(identity, ".git")
	}
	// gitclone builds an OS path; the comparison is about the segments, not the
	// separator Windows would use.
	if filepath.ToSlash(resolved) != want {
		return errors.Errorf("target %q resolves to a different repository (%q)", identity, resolved)
	}
	return nil
}

// validateResolvesToSameRepo refuses a raw client path whose two derivations
// disagree. It runs before any suffix stripping, where a trailing ".git" is
// legitimate convention rather than divergence.
func validateResolvesToSameRepo(host, pathValue string) error {
	return resolvesToSameRepo(host, pathValue, true)
}

// validateCanonicalRepoPath refuses a gated request whose repository path is not
// already the canonical name of the mirror it will be served from.
//
// repoPathForRequest strips one endpoint suffix and one ".git" to name the
// repository; gitclone strips a ".git" of its own when it turns that name back
// into a mirror directory. A target that arrives with the suffix doubled —
// "repo.git.git/ensure-refs" — keeps a residual ".git" through the first strip
// and loses it in the second, so authorization is checked against
// "host/repo.git" while the bytes come from the mirror of "host/repo".
//
// Unlike validateResolvesToSameRepo this tolerates no ".git" at all: repoPath is
// meant to be canonical by this point, so any further stripping by gitclone is
// itself the divergence. Asserting the invariant is stronger than counting
// suffixes — stripping every ".git" instead would resolve "repo.git.git" but
// strand ".git.git" on an empty path, where filepath.Join normalises the
// trailing separator the identity string keeps, and the two derivations
// disagree again.
func validateCanonicalRepoPath(host, repoPath string) error {
	return resolvesToSameRepo(host, repoPath, false)
}

// repoPathForRequest returns the repository path a request serves from, and
// whether that request is subject to client authorization.
//
// Only paths that can be answered from (or cause a fetch into) the local
// mirror are gated. Requests that are always forwarded to upstream verbatim
// are not: they carry the client's own credentials rather than cachew's (see
// mayUseServiceCredentials), so upstream is left to enforce its own
// authorization on them, and deriving a repository path from an arbitrary
// forwarded path (LFS endpoints in particular) is not reliable enough to fail
// closed on.
// The order of the checks below MUST match handleRequest's dispatch order.
// handleRequest recognises the mirror-serving suffixes before it looks at
// ?service=, so testing the receive-pack exclusion first here would de-gate
// requests that still get answered from the mirror.
func repoPathForRequest(r *http.Request, pathValue string) (repoPath string, gated bool) {
	if base, ok := strings.CutSuffix(pathValue, EnsureRefsPath); ok {
		return strings.TrimSuffix(base, ".git"), true
	}
	for _, suffix := range []string{"/snapshot.tar.zst", "/snapshot.bundle", "/lfs-snapshot.tar.zst"} {
		if base, ok := strings.CutSuffix(pathValue, suffix); ok {
			return ExtractRepoPath(base), true
		}
	}
	if r.URL.Query().Get("service") == "git-receive-pack" || strings.HasSuffix(pathValue, "/git-receive-pack") {
		return "", false
	}
	if isGitRequest(pathValue) {
		return ExtractRepoPath(pathValue), true
	}
	return "", false
}

// mayUseServiceCredentials reports whether cachew's own service credentials may
// replace the client's on the upstream leg of this request.
//
// This is deliberately the same predicate as gating: substituting cachew's
// broad credentials is only defensible once the client's access to that
// specific repository has been established, which is exactly what
// authorizeServe does for gated paths. On pass-through paths — push
// (git-receive-pack), the LFS API, anything unrecognised — upstream is the only
// authority, and it can only act as one if it sees the client's own
// credentials, or their absence. Injecting cachew's there would let a client
// with no upstream access push, or upload LFS objects, as cachew. Coupling the
// two decisions means a new pass-through path cannot be handed cachew's
// credentials without also being gated.
func mayUseServiceCredentials(r *http.Request, pathValue string) bool {
	_, gated := repoPathForRequest(r, pathValue)
	return gated
}

// backgroundFetchAllowed reports whether a proactive, credentialed background
// fetch of this mirror may run now.
//
// Repositories that require authentication are excluded entirely: refreshing
// them in the background would use cachew's own broad credentials with no
// client to authorize, so they are only refreshed on demand, off the back of a
// request that already passed authorizeServe.
func (s *Strategy) backgroundFetchAllowed(ctx context.Context, repo *gitclone.Repository) bool {
	if !s.fetchBackoffElapsed(ctx, repo) {
		return false
	}
	return s.proactiveUpstreamContactAllowed(ctx, repo)
}

// proactiveUpstreamContactAllowed reports whether cachew may reach upstream for
// repo with its own credentials and no client request driving it.
//
// It is the authorization half of backgroundFetchAllowed, separated for the
// proactive upstream contacts that are not fetches of the mirror and so have no
// fetch backoff to observe.
func (s *Strategy) proactiveUpstreamContactAllowed(ctx context.Context, repo *gitclone.Repository) bool {
	if s.config.DisableUpstreamAuthz {
		return true
	}
	requiresAuth, err := s.authz.RequiresAuth(ctx, repo.UpstreamURL())
	if err != nil {
		logging.FromContext(ctx).WarnContext(ctx, "Skipping background fetch, visibility probe failed",
			"upstream", repo.UpstreamURL(), "error", err)
		s.metrics.recordBackgroundFetchSkip(ctx, repo.UpstreamURL(), "probe_error")
		return false
	}
	if requiresAuth {
		logging.FromContext(ctx).DebugContext(ctx, "Skipping proactive background fetch for auth-required repository",
			"upstream", repo.UpstreamURL())
		s.metrics.recordBackgroundFetchSkip(ctx, repo.UpstreamURL(), "requires_auth")
		return false
	}
	return true
}

// fetchBackoffElapsed reports whether the exponential backoff following
// consecutive fetch failures has expired, so upstream may be retried.
func (s *Strategy) fetchBackoffElapsed(ctx context.Context, repo *gitclone.Repository) bool {
	wait := repo.BackgroundFetchBackoff(s.cloneManager.Config().FetchInterval)
	if wait <= 0 {
		return true
	}
	logging.FromContext(ctx).InfoContext(ctx, "Skipping background fetch, backing off after failures",
		"upstream", repo.UpstreamURL(), "failures", repo.FetchFailures(), "retry_in", wait)
	s.metrics.recordBackgroundFetchSkip(ctx, repo.UpstreamURL(), "backoff")
	return false
}
