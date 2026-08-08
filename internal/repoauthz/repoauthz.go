// Package repoauthz authorizes a client against a specific upstream git
// repository, using only the credentials that client itself supplied.
//
// cachew mirrors repositories with its own service credentials, so anything in
// a warm mirror or a cached snapshot would otherwise be readable by any client
// that can reach the endpoint. Every path that can hand out repository content
// — the git strategy's own routes and the generic object API — shares the
// primitives here so that all of them reach the same decision by the same
// means, rather than each re-deriving it.
package repoauthz

import (
	"context"
	"io"
	"maps"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/errors"
	"golang.org/x/sync/singleflight"
)

const (
	// ProbeTimeout bounds a single upstream authorization probe. Probes are on
	// the request path, so a hung upstream must fail closed quickly rather than
	// pinning the client.
	ProbeTimeout = 10 * time.Second

	// DefaultRequiresAuthTTL is how long a per-repo "does upstream require
	// authentication?" verdict is trusted before being re-probed anonymously.
	//
	// This is a deliberate trade-off. The verdict only decides whether to run
	// the (uncached) per-request client authorization check, so a stale verdict
	// has two failure modes: a repo that just became private keeps being served
	// without a client check for up to one TTL, and a repo that just became
	// public keeps paying for client checks for up to one TTL. Fifteen minutes
	// bounds the former to roughly the window an upstream permission change
	// takes to propagate through most CI systems anyway, while costing at most
	// one anonymous ref advertisement per repo per fifteen minutes — orders of
	// magnitude cheaper than the per-request checks it gates.
	DefaultRequiresAuthTTL = 15 * time.Minute

	// verdictCacheLimit caps the visibility verdict cache. Entries are keyed by
	// client-reachable upstream URL, which an unauthenticated client chooses, so
	// the map must be a hard cap rather than a hint: pruning only expired entries
	// bounds nothing while a client keeps naming fresh repositories.
	verdictCacheLimit = 4096

	// verdictCacheTarget is how far below the limit an eviction pass trims, so a
	// cache held at capacity evicts in blocks instead of scanning per insertion.
	verdictCacheTarget = verdictCacheLimit * 7 / 8

	// advertisementMediaType is the content type a smart HTTP server must send
	// for an upload-pack ref advertisement.
	advertisementMediaType = "application/x-git-upload-pack-advertisement"

	// advertisementServiceHeader is what the pkt-line an advertisement opens
	// with carries, after its four hexadecimal length digits.
	advertisementServiceHeader = "# service=git-upload-pack"

	// advertisementPrefixLimit is how much of a 200 response has to be read to
	// recognise it. Advertisements for large repositories run to megabytes and
	// nothing past the opening pkt-line identifies the response.
	advertisementPrefixLimit = 4 + len(advertisementServiceHeader)

	// maxProbeRedirects caps a probe's redirect chain. Supplying CheckRedirect
	// replaces net/http's own limit, so it has to be reimposed here.
	maxProbeRedirects = 10
)

// An Outcome is the result of an authorization decision.
type Outcome string

const (
	// OutcomePublic means upstream serves the repository anonymously, so no
	// client check was needed.
	OutcomePublic Outcome = "public"
	// OutcomeAllowed means upstream accepted the client's own credentials for
	// this repository.
	OutcomeAllowed Outcome = "allowed"
	// OutcomeNoCredentials means the repository requires authentication and the
	// request carried none.
	OutcomeNoCredentials Outcome = "no_credentials"
	// OutcomeRejected means upstream refused the client's credentials for this
	// repository.
	OutcomeRejected Outcome = "rejected"
	// OutcomeProbeError means upstream could not be consulted, so the decision
	// failed closed.
	OutcomeProbeError Outcome = "probe_error"
	// OutcomeInternalError means the repository could not be resolved locally.
	OutcomeInternalError Outcome = "internal_error"
	// OutcomeInvalidTarget means the request target itself is malformed: it
	// does not name the same repository the mirror would resolve it to.
	OutcomeInvalidTarget Outcome = "invalid_target"
)

// Allowed reports whether the outcome permits serving repository content.
func (o Outcome) Allowed() bool { return o == OutcomePublic || o == OutcomeAllowed }

// Respond writes the denial response for an outcome that does not permit
// serving. It writes nothing for outcomes that do.
func Respond(w http.ResponseWriter, outcome Outcome) {
	switch outcome {
	case OutcomeNoCredentials:
		// Challenge rather than flatly refusing so a real git client retries
		// with credentials for the cachew endpoint instead of giving up.
		w.Header().Set("WWW-Authenticate", `Basic realm="cachew"`)
		http.Error(w, "authentication required", http.StatusUnauthorized)

	case OutcomeRejected:
		http.Error(w, "forbidden", http.StatusForbidden)

	case OutcomeProbeError:
		http.Error(w, "upstream authorization check failed", http.StatusServiceUnavailable)

	case OutcomeInternalError:
		http.Error(w, "internal server error", http.StatusInternalServerError)

	case OutcomeInvalidTarget:
		http.Error(w, "invalid repository target", http.StatusBadRequest)

	case OutcomePublic, OutcomeAllowed:
	}
}

// A ProbeResult is what a ref advertisement probe learned about the
// credentials it carried.
type ProbeResult struct {
	// Status is the status of the final response, carried for logging.
	Status int
	// Advertised reports whether that response was an upload-pack ref
	// advertisement rather than some other 200.
	Advertised bool
}

// GrantsRead reports whether the probe proves the credentials it carried may
// read the repository.
//
// The status alone does not prove it. An upstream that answers an unauthorized
// advertisement request with 200 and an HTML login page — which is what an SSO
// portal in front of a git server does, and what several git hosts do for a
// repository the caller may not see — would otherwise be read as a grant, and
// hand the client a mirror cachew populated with its own credentials. Only a
// well-formed advertisement shows upstream actually served the repository to
// these credentials.
//
// A refusal is deliberately not classified further: 404 is not distinguished
// from 403, because upstream hosts hide "no access" from "does not exist" on
// purpose and re-exposing the difference here would leak private repository
// names.
func (p ProbeResult) GrantsRead() bool { return p.Status == http.StatusOK && p.Advertised }

// ProbeRefs issues a smart-HTTP ref advertisement request against upstream and
// reports what came back. authorization, when non-empty, is sent verbatim as
// the Authorization header.
//
// A successful advertisement is the narrowest upstream operation that proves
// read access, and it is idempotent, so it is safe to issue on every request.
func ProbeRefs(ctx context.Context, client *http.Client, upstreamURL, authorization string) (ProbeResult, error) {
	ctx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()

	probeURL, err := advertisementURL(upstreamURL)
	if err != nil {
		return ProbeResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		return ProbeResult{}, errors.Wrap(err, "build authorization probe request")
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}

	// The caller's client is shared between probes and, for the git strategy,
	// with the proxy, so the redirect policy goes on a copy of it.
	probeClient := *client
	probeClient.CheckRedirect = stopAtForeignHost
	resp, err := probeClient.Do(req)
	if err != nil {
		return ProbeResult{}, errors.Wrap(err, "authorization probe request")
	}
	result := ProbeResult{Status: resp.StatusCode}
	if resp.StatusCode == http.StatusOK {
		result.Advertised = isAdvertisement(resp)
	}
	// Only the opening pkt-line is read, and advertisements for large
	// repositories run to megabytes, so close without draining the rest.
	_ = resp.Body.Close() //nolint:errcheck // nothing actionable on close of an undrained body
	return result, nil
}

// stopAtForeignHost keeps a probe on the host whose repository is being
// authorized.
//
// net/http strips Authorization when a redirect changes host, so following one
// silently turns a credentialed probe into an anonymous one: an advertisement
// from the redirect target would then say the repository is readable by anyone,
// not that it is readable by this client. Surfacing the 3xx instead denies,
// because a redirect is not an advertisement. Same-host redirects — case
// normalisation, renames, http to https — are followed as before.
func stopAtForeignHost(req *http.Request, via []*http.Request) error {
	if len(via) >= maxProbeRedirects {
		return errors.Errorf("stopped after %d redirects", maxProbeRedirects)
	}
	if req.URL.Host != via[len(via)-1].URL.Host {
		return http.ErrUseLastResponse
	}
	return nil
}

// isAdvertisement reports whether resp is a smart-HTTP upload-pack ref
// advertisement, by the two signals a git client itself checks: the media type,
// and the pkt-line the payload must open with.
func isAdvertisement(resp *http.Response) bool {
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != advertisementMediaType {
		return false
	}
	prefix, err := io.ReadAll(io.LimitReader(resp.Body, int64(advertisementPrefixLimit)))
	if err != nil || len(prefix) != advertisementPrefixLimit {
		return false
	}
	for _, c := range prefix[:4] {
		if !isHexDigit(c) {
			return false
		}
	}
	return string(prefix[4:]) == advertisementServiceHeader
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// advertisementURL derives the upload-pack ref advertisement URL for a
// repository URL.
//
// Building this by string concatenation let a "?" or "#" anywhere in the
// repository URL swallow the endpoint and the service parameter, pointing the
// probe at something else entirely — the repository's landing page, or the
// receive-pack advertisement — whose 200 says nothing about read access to the
// repository being authorized. A query, fragment or userinfo is refused rather
// than dropped: a caller that supplied one meant something this function cannot
// honour, and guessing is how the two derivations drift apart again.
func advertisementURL(upstreamURL string) (string, error) {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		return "", errors.Errorf("parse upstream URL %q: %w", upstreamURL, err)
	}
	switch {
	case parsed.Scheme != "https" && parsed.Scheme != "http":
		return "", errors.Errorf("upstream URL %q is not an HTTP URL", upstreamURL)

	case parsed.Host == "" || parsed.Path == "" || parsed.Path == "/":
		return "", errors.Errorf("upstream URL %q does not name a repository", upstreamURL)

	case parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.User != nil:
		return "", errors.Errorf("upstream URL %q is not a plain repository URL", upstreamURL)
	}

	probe := *parsed
	probe.Path = strings.TrimSuffix(parsed.Path, ".git") + ".git/info/refs"
	if parsed.RawPath != "" {
		probe.RawPath = strings.TrimSuffix(parsed.RawPath, ".git") + ".git/info/refs"
	}
	probe.RawQuery = "service=git-upload-pack"
	return probe.String(), nil
}

// An Authorizer decides whether a client may read a repository, caching only
// the anonymous per-repository visibility verdict.
//
// The per-request client decision is intentionally never cached: a cache would
// reopen the same confused-deputy hole for the lifetime of its entries.
type Authorizer struct {
	client atomic.Pointer[http.Client]
	ttl    time.Duration
	probes singleflight.Group

	mu       sync.Mutex
	verdicts map[string]verdict
}

type verdict struct {
	requiresAuth bool
	probedAt     time.Time
}

// NewAuthorizer returns an Authorizer probing upstream with client and trusting
// each repository's visibility verdict for ttl. A nil client uses
// http.DefaultClient; a zero ttl uses DefaultRequiresAuthTTL.
func NewAuthorizer(client *http.Client, ttl time.Duration) *Authorizer {
	if ttl == 0 {
		ttl = DefaultRequiresAuthTTL
	}
	a := &Authorizer{ttl: ttl, verdicts: map[string]verdict{}}
	a.SetHTTPClient(client)
	return a
}

// SetHTTPClient replaces the client used for upstream probes. A nil client
// restores http.DefaultClient.
func (a *Authorizer) SetHTTPClient(client *http.Client) {
	if client == nil {
		client = http.DefaultClient
	}
	a.client.Store(client)
}

// Authorize reports whether authorization, the verbatim Authorization header of
// the client's request, grants read access to upstreamURL.
//
// The returned error explains an OutcomeProbeError so callers can log it; the
// outcome alone is enough to decide.
func (a *Authorizer) Authorize(ctx context.Context, upstreamURL, authorization string) (Outcome, error) {
	requiresAuth, err := a.RequiresAuth(ctx, upstreamURL)
	if err != nil {
		return OutcomeProbeError, errors.WithStack(err)
	}
	if !requiresAuth {
		return OutcomePublic, nil
	}
	if authorization == "" {
		return OutcomeNoCredentials, nil
	}
	result, err := ProbeRefs(ctx, a.client.Load(), upstreamURL, authorization)
	if err != nil {
		return OutcomeProbeError, errors.Wrap(err, "probe client access")
	}
	if !result.GrantsRead() {
		return OutcomeRejected, nil
	}
	return OutcomeAllowed, nil
}

// RequiresAuth reports whether upstream refuses to serve the repository
// anonymously, reusing the cached verdict while it is fresh.
//
// The probe is anonymous, so a failure to reach upstream must not be read as
// "public": callers get an error and fail closed.
func (a *Authorizer) RequiresAuth(ctx context.Context, upstreamURL string) (bool, error) {
	if requiresAuth, fresh := a.cached(upstreamURL); fresh {
		return requiresAuth, nil
	}

	// Detach from the caller's context so one client disconnecting does not
	// cancel a probe that other in-flight requests are sharing.
	probeCtx := context.WithoutCancel(ctx)
	result, err, _ := a.probes.Do(upstreamURL, func() (any, error) {
		if requiresAuth, fresh := a.cached(upstreamURL); fresh {
			return requiresAuth, nil
		}
		result, err := ProbeRefs(probeCtx, a.client.Load(), upstreamURL, "")
		if err != nil {
			return nil, errors.WithStack(err)
		}
		requiresAuth := !result.GrantsRead()
		a.store(upstreamURL, requiresAuth)
		return requiresAuth, nil
	})
	if err != nil {
		return true, errors.Wrap(err, "probe upstream visibility")
	}
	requiresAuth, ok := result.(bool)
	if !ok {
		return true, errors.New("unexpected probe result type")
	}
	return requiresAuth, nil
}

func (a *Authorizer) cached(upstreamURL string) (requiresAuth, fresh bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.verdicts[upstreamURL]
	if !ok || time.Since(v.probedAt) >= a.ttl {
		return false, false
	}
	return v.requiresAuth, true
}

func (a *Authorizer) store(upstreamURL string, requiresAuth bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.verdicts) >= verdictCacheLimit {
		a.evictLocked()
	}
	a.verdicts[upstreamURL] = verdict{requiresAuth: requiresAuth, probedAt: time.Now()}
}

// evictLocked frees room in a full verdict cache: expired entries first, then
// the oldest live ones if that was not enough.
//
// Evicting a live verdict is safe by construction. The cache only ever answers
// "is a fresh verdict already known?", and a miss re-probes upstream — an
// evicted verdict costs one anonymous ref advertisement, never a weaker
// decision, because RequiresAuth fails closed when the probe cannot be made.
// Preferring expired entries keeps that cost off repositories in active use.
func (a *Authorizer) evictLocked() {
	for url, v := range a.verdicts {
		if time.Since(v.probedAt) >= a.ttl {
			delete(a.verdicts, url)
		}
	}
	if len(a.verdicts) <= verdictCacheTarget {
		return
	}
	urls := slices.Collect(maps.Keys(a.verdicts))
	slices.SortFunc(urls, func(x, y string) int {
		return a.verdicts[x].probedAt.Compare(a.verdicts[y].probedAt)
	})
	for _, url := range urls[:len(urls)-verdictCacheTarget] {
		delete(a.verdicts, url)
	}
}
