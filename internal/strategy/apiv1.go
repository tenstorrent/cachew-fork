package strategy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/gitobject"
	"github.com/tenstorrent/internal-cachew/internal/gomodobject"
	"github.com/tenstorrent/internal-cachew/internal/httputil"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/metrics"
	"github.com/tenstorrent/internal-cachew/internal/repoauthz"
)

// RegisterAPIV1 registers the object API, authorizing reads of reserved
// namespaces with authz. Passing the same Authorizer here as the strategies
// that own those namespaces get is what keeps one repository's visibility
// verdict from being probed and cached once per entry point.
func RegisterAPIV1(r *Registry, authz *repoauthz.Authorizer) {
	Register(r, "apiv1", "The stable API of the cache server.",
		func(ctx context.Context, config APIV1Config, cache cache.Cache, mux Mux) (*APIV1, error) {
			return NewAPIV1(ctx, config, cache, mux, authz, r)
		})
}

var _ Strategy = (*APIV1)(nil)

// APIV1Config configures the object API.
type APIV1Config struct {
	DisableGitObjectAuthz bool `hcl:"disable-git-object-authz,optional" help:"Disable per-request client authorization of reads of git strategy objects through this API (GET/HEAD /api/v1/object/git/<key>). Independent of the git strategy's disable-upstream-authz, which gates only its own /git/ routes. Unsafe: any client that can reach the object API can then read snapshots, bundles and mirror archives of any mirrored private repo." default:"false"`
}

// The APIV1 strategy represents v1 of the proxy API.
type APIV1 struct {
	config     APIV1Config
	cache      cache.Cache
	logger     *slog.Logger
	authz      *repoauthz.Authorizer
	strategies *Registry
}

// NewAPIV1 creates the object API. A nil authz gives it an Authorizer of its
// own, which is only right for a standalone instance: cachewd shares one across
// every strategy that authorizes the same repositories.
//
// strategies is the registry the API is served from, and is consulted only to
// tell a namespace some strategy owns from the general-purpose object storage
// this API also provides. A nil registry means no strategy owns anything.
func NewAPIV1(
	ctx context.Context, config APIV1Config, cache cache.Cache, mux Mux, authz *repoauthz.Authorizer, strategies *Registry,
) (*APIV1, error) {
	if authz == nil {
		authz = repoauthz.NewAuthorizer(nil, 0)
	}
	s := &APIV1{
		config:     config,
		logger:     logging.FromContext(ctx),
		cache:      cache,
		authz:      authz,
		strategies: strategies,
	}
	var objectAPIAuthzDisabled int64
	if config.DisableGitObjectAuthz {
		objectAPIAuthzDisabled = 1
		s.logger.WarnContext(ctx, "Authorization of git object reads is disabled, any client reaching the object API can read private repository snapshots")
	}
	metrics.NewConstGauge(otel.Meter("cachew.apiv1"), "cachew.authz.disabled", "",
		"1 if a client-authorization escape hatch is enabled for this check, 0 otherwise. check identifies which: upstream (git strategy's disable-upstream-authz), object_api (apiv1's disable-git-object-authz)",
		objectAPIAuthzDisabled, attribute.String("check", "object_api"))
	mux.Handle("GET /api/v1/object/{namespace}/{key}", http.HandlerFunc(s.getObject))
	mux.Handle("HEAD /api/v1/object/{namespace}/{key}", http.HandlerFunc(s.statObject))
	mux.Handle("POST /api/v1/object/{namespace}/{key}", http.HandlerFunc(s.putObject))
	mux.Handle("DELETE /api/v1/object/{namespace}/{key}", http.HandlerFunc(s.deleteObject))
	mux.Handle("POST /api/v1/object/{namespace}/{key}/invalidate", http.HandlerFunc(s.invalidateObject))
	mux.Handle("GET /api/v1/stats", http.HandlerFunc(s.getStats))
	mux.Handle("GET /api/v1/namespaces", http.HandlerFunc(s.getNamespaces))
	return s, nil
}

func (d *APIV1) String() string { return "default" }

func (d *APIV1) statObject(w http.ResponseWriter, r *http.Request) {
	namespace, err := cache.ParseNamespace(r.PathValue("namespace"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid namespace")
		return
	}
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	if !d.authorizeObject(w, r, namespace, key) {
		return
	}

	namespacedCache := d.cache.Namespace(namespace)
	headers, err := namespacedCache.Stat(r.Context(), key, httputil.ConditionalOptions(r)...)
	if httputil.ServeCacheStat(w, headers, err) {
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		http.Error(w, "Cache object not found", http.StatusNotFound)
		return
	}
	d.httpError(w, http.StatusInternalServerError, err, "Failed to open cache object", "key", key)
}

func (d *APIV1) getObject(w http.ResponseWriter, r *http.Request) {
	namespace, err := cache.ParseNamespace(r.PathValue("namespace"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid namespace")
		return
	}
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	if !d.authorizeObject(w, r, namespace, key) {
		return
	}

	namespacedCache := d.cache.Namespace(namespace)
	cr, headers, err := namespacedCache.Open(r.Context(), key, httputil.ConditionalOptions(r)...)
	if handled, _, serveErr := httputil.ServeCacheHit(w, headers, cr, err); handled {
		if serveErr != nil {
			d.logger.Error("Failed to serve cache object", "error", serveErr, "key", key)
		}
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		http.Error(w, "Cache object not found", http.StatusNotFound)
		return
	}
	d.httpError(w, http.StatusInternalServerError, err, "Failed to open cache object", "key", key)
}

func (d *APIV1) putObject(w http.ResponseWriter, r *http.Request) {
	namespace, err := cache.ParseNamespace(r.PathValue("namespace"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid namespace")
		return
	}
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	if !d.authorizeObject(w, r, namespace, key) {
		return
	}

	var ttl time.Duration
	ttlh := r.Header.Get("Time-To-Live")
	if ttlh != "" {
		ttl, err = time.ParseDuration(ttlh)
		if err != nil {
			d.httpError(w, http.StatusBadRequest, err, "Invalid Time-To-Live header format, must be in Go duration format eg. 1h")
			return
		}
	}

	// Client-supplied metadata is replayed to later readers, so credentials must
	// not be persisted and the metadata the git and gomod strategies authorize
	// against must not be forgeable from outside them.
	skip := slices.Concat(httputil.TransportHeaders, httputil.CredentialHeaders,
		gitobject.MetadataHeaders, gomodobject.MetadataHeaders)
	headers := httputil.FilterHeaders(r.Header, skip...)
	var opts []cache.Option
	if etag := headers.Get(cache.ETagKey); etag != "" {
		rawETag, err := cache.RawETagFromHeader(etag)
		if err != nil {
			d.httpError(w, http.StatusBadRequest, err, "Invalid ETag header")
			return
		}
		opts = append(opts, cache.WithETag(rawETag))
		headers.Del(cache.ETagKey)
	}

	namespacedCache := d.cache.Namespace(namespace)
	cw, err := namespacedCache.Create(r.Context(), key, headers, ttl, opts...)
	if err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to create cache writer", "key", key)
		return
	}

	if _, err := io.Copy(cw, r.Body); err != nil {
		d.httpError(w, http.StatusInternalServerError, errors.Join(err, cw.Abort(err)), "Failed to copy request body to cache writer")
		return
	}

	if err := cw.Close(); err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to close cache writer")
		return
	}
}

func (d *APIV1) deleteObject(w http.ResponseWriter, r *http.Request) {
	namespace, err := cache.ParseNamespace(r.PathValue("namespace"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid namespace")
		return
	}
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	if !d.authorizeObject(w, r, namespace, key) {
		return
	}

	namespacedCache := d.cache.Namespace(namespace)
	err = namespacedCache.Delete(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "Cache object not found", http.StatusNotFound)
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to delete cache object", "key", key)
		return
	}
}

func (d *APIV1) invalidateObject(w http.ResponseWriter, r *http.Request) {
	namespace, err := cache.ParseNamespace(r.PathValue("namespace"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid namespace")
		return
	}
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	if !d.authorizeObject(w, r, namespace, key) {
		return
	}

	namespacedCache := d.cache.Namespace(namespace)
	if err := namespacedCache.Invalidate(r.Context(), key); err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to invalidate cache object", "key", key)
		return
	}
}

func (d *APIV1) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := d.cache.Stats(r.Context())
	if err != nil {
		if errors.Is(err, cache.ErrStatsUnavailable) {
			d.httpError(w, http.StatusNotImplemented, err, "Stats not available for this cache backend")
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to get cache stats")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		d.logger.Error("Failed to encode stats response", "error", err)
	}
}

func (d *APIV1) getNamespaces(w http.ResponseWriter, r *http.Request) {
	namespaces, err := d.cache.ListNamespaces(r.Context())
	if err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to list namespaces")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(namespaces); err != nil {
		d.logger.Error("Failed to encode namespaces response", "error", err)
	}
}

// SetAuthzHTTPClient replaces the HTTP client used for the upstream
// authorization probes that gate reads of the git namespace.
func (d *APIV1) SetAuthzHTTPClient(client *http.Client) { d.authz.SetHTTPClient(client) }

// authorizeObject applies the access control the addressed namespace requires,
// writing the denial response itself when access is refused.
//
// This API namespaces by a name the caller supplies, and cache namespacing
// replaces rather than nests, so a caller naming a strategy's namespace here
// addresses that strategy's objects directly, bypassing the strategy's own
// handlers and whatever authorization they apply. For the git strategy those
// objects are snapshots, mirror archives and bundles that cachew produced with
// its own service credentials, keyed by an unsalted hash of the upstream URL —
// computable offline by anyone who can guess a repository name.
func (d *APIV1) authorizeObject(w http.ResponseWriter, r *http.Request, namespace cache.Namespace, key cache.Key) bool {
	// Namespaces are compared case-insensitively because a case-insensitive
	// filesystem collates "GIT" and "git" to one directory of objects while the
	// namespace's metadata is keyed byte-exactly, so a differently cased spelling
	// would otherwise reach a reserved namespace's files without its controls.
	switch {
	case strings.EqualFold(string(namespace), gitobject.Namespace):
		return d.authorizeGitObject(w, r, namespace, key)

	case strings.EqualFold(string(namespace), gomodobject.Namespace):
		return d.authorizeModuleObject(w, r, namespace, key)

	default:
		return d.authorizeStrategyObject(w, r, namespace, key)
	}
}

// uncheckedStrategyNamespaces are the strategy-owned namespaces this API serves
// without a check of its own, listed so that adding a strategy does not
// silently add an unauthorized way to read what it caches.
//
// A namespace a strategy owns holds objects cachew fetched on its own behalf,
// and reaching them through this API bypasses the strategy's handlers and
// whatever those enforce; a namespace no strategy owns is this API's own
// general-purpose object storage, which is what "cachew save" writes and stays
// open. The entries here are the ones that are unchecked today, not an
// assertion that none of them ever needs a check.
//
//nolint:gochecknoglobals // the list is the API's own configuration, not state
var uncheckedStrategyNamespaces = []string{"apiv1", "artifactory", "github-releases", "hermit", "host", "proxy"}

// authorizeStrategyObject refuses access to a namespace owned by a strategy
// this API has no authorization rule for, and allows everything else.
//
// The fallthrough denies rather than allows so that a strategy registered later
// is refused here until someone decides what authorizing its objects means. It
// reports absence rather than refusal, matching the other denials: a client that
// may not read an object is not told it exists.
func (d *APIV1) authorizeStrategyObject(w http.ResponseWriter, r *http.Request, namespace cache.Namespace, key cache.Key) bool {
	if slices.ContainsFunc(uncheckedStrategyNamespaces, func(ns string) bool {
		return strings.EqualFold(string(namespace), ns)
	}) {
		return true
	}
	// Registered strategy names are lower case; the namespace is folded to match
	// for the same reason the switch above compares case-insensitively.
	if d.strategies == nil || !d.strategies.Exists(strings.ToLower(string(namespace))) {
		return true
	}
	d.logger.WarnContext(r.Context(), "Refusing access to a strategy namespace with no authorization rule",
		"method", r.Method, "namespace", namespace, "key", key)
	http.Error(w, "Cache object not found", http.StatusNotFound)
	return false
}

func (d *APIV1) authorizeGitObject(w http.ResponseWriter, r *http.Request, namespace cache.Namespace, key cache.Key) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		// Nothing outside the git strategy legitimately writes these objects,
		// and a caller that could would control what every consumer of that
		// repository restores from — a broader compromise than reading one
		// repository. Reads are authorized rather than refused because fetching
		// objects over this API is what the tiered cache is designed to do.
		// Git objects are therefore strategy-local by design: a remote cachew
		// tier that had to be populated over this API would need an
		// authenticated write path of its own, not this one relaxed.
		d.logger.WarnContext(r.Context(), "Refusing mutation of reserved git namespace",
			"method", r.Method, "namespace", namespace, "key", key)
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}
	// Escape hatch for deployments that knowingly serve only public
	// repositories, mirroring what disable-upstream-authz does for the git
	// strategy's own routes. The two are separate options because they protect
	// separate entry points: a deployment may reasonably open one and not the
	// other, and neither should silently turn the other off.
	if d.config.DisableGitObjectAuthz {
		return true
	}
	return d.authorizeGitRead(w, r, namespace, key)
}

// authorizeModuleObject gates a read of a gomod-namespaced object the same way
// the module proxy gates its own routes: objects recorded as resolving to a
// private repository are served only to a client whose own credentials upstream
// accepts for that repository.
//
// Module files cached from a private repository are cloned with cachew's own
// service credentials and keyed by an unsalted hash of the module proxy path, so
// without this any client that can guess a private module path and version can
// read its full source here regardless of what the /gomod/ routes enforce.
//
// Mutations are not refused as they are for the git namespace: unlike git
// objects, module files are written through this API by design, by the remote
// tier of a tiered cache. A client that writes here can therefore serve content
// of its own choosing back to itself, but cannot make cachew disclose a private
// module it could not already read — the metadata cannot be rewritten without
// also replacing the content it describes.
func (d *APIV1) authorizeModuleObject(w http.ResponseWriter, r *http.Request, namespace cache.Namespace, key cache.Key) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return true
	}
	ctx := r.Context()
	headers, err := d.cache.Namespace(namespace).Stat(ctx, key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "Cache object not found", http.StatusNotFound)
			return false
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to stat cache object", "key", key)
		return false
	}
	modulePath, private, ok := gomodobject.Module(key, headers)
	if !ok {
		// Fails closed like the git namespace does: an object with no verifiable
		// module has nothing to authorize against, and objects predating this
		// metadata may well be private module source.
		d.logger.WarnContext(ctx, "Refusing read of module object with no verifiable module metadata", "key", key)
		http.Error(w, "Cache object not found", http.StatusNotFound)
		return false
	}
	if !private {
		return true
	}

	upstream := gomodobject.GitURL(modulePath)
	outcome, err := d.authz.Authorize(ctx, upstream, r.Header.Get("Authorization"))
	if err != nil {
		d.logger.WarnContext(ctx, "Upstream authorization probe failed, denying module object read",
			"upstream", upstream, "error", err)
	}
	if !outcome.Allowed() {
		d.logger.InfoContext(ctx, "Denying module object read", "upstream", upstream, "outcome", string(outcome))
		d.respondObjectDenial(w, outcome)
		return false
	}
	return true
}

// authorizeGitRead gates a read of a git-namespaced object on the same
// per-request client authorization the git strategy applies to its own routes:
// public repositories are served without a check, and anything upstream refuses
// to serve anonymously is served only to a client whose own credentials
// upstream accepts for that repository.
//
// Objects whose repository cannot be established are reported as absent rather
// than served, because there is nothing to authorize them against.
func (d *APIV1) authorizeGitRead(w http.ResponseWriter, r *http.Request, namespace cache.Namespace, key cache.Key) bool {
	ctx := r.Context()
	// The namespace is the one the request named rather than the canonical
	// spelling, so the object authorized is always the object the read serves.
	headers, err := d.cache.Namespace(namespace).Stat(ctx, key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "Cache object not found", http.StatusNotFound)
			return false
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to stat cache object", "key", key)
		return false
	}
	upstream, ok := gitobject.Repository(key, headers)
	if !ok {
		d.logger.WarnContext(ctx, "Refusing read of git object with no verifiable repository metadata", "key", key)
		http.Error(w, "Cache object not found", http.StatusNotFound)
		return false
	}

	outcome, err := d.authz.Authorize(ctx, upstream, r.Header.Get("Authorization"))
	if err != nil {
		d.logger.WarnContext(ctx, "Upstream authorization probe failed, denying git object read",
			"upstream", upstream, "error", err)
	}
	if !outcome.Allowed() {
		d.logger.InfoContext(ctx, "Denying git object read", "upstream", upstream, "outcome", string(outcome))
		d.respondObjectDenial(w, outcome)
		return false
	}
	return true
}

// respondObjectDenial refuses a read of an object the client may not have,
// reporting a refusal about the client as absence.
//
// Objects here are addressed by an unsalted hash of an upstream URL or module
// path, so any client can compute the key of a repository it merely guessed at.
// Answering "you may not read this" for a key that resolves, while a key that
// does not resolve gets "not found", tells that client its guess was right and
// that this deployment mirrors the private repository — the difference between
// "no access" and "does not exist" that upstream hosts hide deliberately and
// that repoauthz.ProbeResult folds away for the same reason. Unlike the git
// strategy's own routes, nothing here needs a challenge to retry: the clients
// of this API attach their credentials up front, and a 404 makes an
// unauthorized tier fall through to fetching from upstream itself rather than
// failing the request outright.
//
// Outcomes that are not a decision about the client keep their own status. They
// are an operational signal an operator needs to see, and reaching one already
// requires upstream to be unreachable.
func (d *APIV1) respondObjectDenial(w http.ResponseWriter, outcome repoauthz.Outcome) {
	switch outcome {
	case repoauthz.OutcomeNoCredentials, repoauthz.OutcomeRejected:
		http.Error(w, "Cache object not found", http.StatusNotFound)

	case repoauthz.OutcomeProbeError, repoauthz.OutcomeInternalError, repoauthz.OutcomeInvalidTarget,
		repoauthz.OutcomePublic, repoauthz.OutcomeAllowed:
		repoauthz.Respond(w, outcome)
	}
}

func (d *APIV1) httpError(w http.ResponseWriter, code int, err error, message string, args ...any) {
	args = append(args, "error", err)
	d.logger.Error(message, args...)
	http.Error(w, message, code)
}
