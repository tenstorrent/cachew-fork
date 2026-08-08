# Per-repository client authorization

Status: shipped (PR #15 on `tt-main`, retargeted from #3).

## Problem

cachew mirrors upstream repositories using **its own** credentials (a GitHub
App installation token, or system git credentials). Once a mirror is warm,
`internal/strategy/git` would serve it to any client that could reach the
HTTP endpoint — the only gate was the network/path-level OPA policy in
`internal/opa`, which inspects `remote_addr` and `path` and never looks at
the requesting client's identity or the specific repository's permissions.
That's a textbook confused deputy: cachew holds broad read access and lends
it to callers who have none.

This applies to every path that can hand out repository content, not just
the git protocol routes: the object API (`GET /api/v1/object/git/<key>` and
`.../gomod/<key>`) and the Go module proxy read from the same mirrors and
metadata.

## The core guarantee

**The identity an authorization decision is made against and the identity
content is actually served from must be provably the same string.** Two
independent code paths compute "which repository is this":

- The **authorization identity** — built by string concatenation of the
  client-controlled `{host}` and `{path...}` route wildcards
  (`repoPathForRequest` / `ExtractRepoPath` in `internal/strategy/git/`).
- The **on-disk mirror path** — derived independently via `url.Parse` +
  `filepath.Join` (`gitclone.RepoPathFromURL` in `internal/gitclone/`).

If these two derivations can ever disagree for the same client-supplied
input, authorization gets checked against one repository while bytes are
served from another. Three layers enforce that they can't, applied in order
on every request, before any dispatch:

1. **`validateRepoTarget`** (`internal/strategy/git/authz.go`) — rejects any
   host/path segment that's empty, `.`, `..`, a bare `.git`, contains a path
   separator, or a control character.
2. **`validateResolvesToSameRepo`** — requires the *raw* client target to
   round-trip through `gitclone.RepoPathFromURL` to a byte-identical string
   (trimming one trailing `.git`, since that's legitimate client
   convention). This is what catches what segment checks can't:
   `url.Parse` decodes percent-escapes a second time after `ServeMux` has
   already decoded once, and it splits `?`/`#` off the path entirely.
3. **`validateCanonicalRepoPath`** — a stricter sibling, run on the
   already-computed `repoPath` inside `authorizeServe`. It requires the
   same round trip to be byte-identical **with no `.git` trimming at all**,
   because at this stage `repoPath` is supposed to already be canonical —
   any further stripping by `RepoPathFromURL` is itself the bug signal.

All three **reject rather than normalize**. Normalizing would have to match
`filepath.Join`'s semantics exactly, forever, in every caller; refusing the
input keeps the two derivations trivially equal instead.

### Why layer 3 exists: a worked example

Layers 1–2 alone missed a real divergence, found by fuzzing `tt-main` after
this PR was merged: `repoPathForRequest` and `ExtractRepoPath` strip only
**one** trailing `.git` from the request path. For `host=0`,
`path=0.git.git/ensure-refs`:

1. `repoPathForRequest` cuts the `/ensure-refs` suffix → `0.git.git`, then
   strips one `.git` → `repoPath = "0.git"` (still has a residual `.git`).
2. The authorization identity becomes `"0/0.git"`.
3. `gitclone.RepoPathFromURL("https://0/0.git")` strips its *own* trailing
   `.git` when resolving the mirror path → `"0/0"`.
4. `"0/0.git" != "0/0"` — divergence. `validateResolvesToSameRepo` never
   saw this, because it ran on the raw pre-strip path (`0.git.git/ensure-refs`,
   which doesn't end in `.git` and round-trips fine at that stage).

`validateCanonicalRepoPath` catches this by re-checking the *post-strip*
`repoPath` with zero tolerance for further stripping, and denies with `400`
(`repoauthz.OutcomeInvalidTarget`) rather than serving. See
`internal/strategy/git/testdata/fuzz/FuzzRepoTargetResolution/6c6f6e12ef301d5b`
for the saved regression input, and `TestValidateCanonicalRepoPath` /
`TestAuthorizeServeDeniesNonCanonicalRepoPath` in `authz_test.go`.

This was the **fourth** identity-divergence bug fuzzing found in this
subsystem; the fuzz target's doc comment and the PR description list all
four.

## Request flow

For every request to `/git/{host}/{path...}` (`handleRequest` in
`internal/strategy/git/git.go`):

```
validateRepoTarget(host, pathValue)          -- reject malformed/traversal targets, 400
        |
repoPath, gated := repoPathForRequest(...)   -- which repo, and does this path serve from the mirror?
        |
validateCanonicalRepoPath(host, repoPath)    -- reject residual-.git-style divergence, 400
        |                                       (runs even if disable-upstream-authz is set —
        |                                        this is target sanity, not client authorization)
if !gated || DisableUpstreamAuthz: allow      -- pass-through paths, or the escape hatch
        |
authz.Authorize(upstreamURL, r.Header["Authorization"])
        |
   RequiresAuth(upstreamURL)?                 -- cached anonymous probe, see below
      no  -> public: allow (zero added cost)
      yes -> Authorization header present?
               no  -> 401 + WWW-Authenticate (challenge, not flat refusal)
               yes -> re-probe upstream WITH the client's own header
                        yes -> allowed
                        no  -> 403 (401/403/404 from upstream all collapse to this)
```

**The per-request client probe is permanently, deliberately uncached.** A
cache would reopen the exact confused-deputy hole this exists to close, for
the lifetime of its entries. This is a load-bearing design constraint — see
the comments on `authorizeServe` and `repoauthz.Authorizer`.

Gated paths: `info/refs` (upload-pack), `git-upload-pack`, `ensure-refs`,
`snapshot.tar.zst`, `snapshot.bundle`, `lfs-snapshot.tar.zst` — everything
that can be answered from, or cause a fetch into, the local mirror.
Pass-through paths (`git-receive-pack`, the receive-pack advertisement, the
LFS batch API, anything unrecognised) forward the client's own
`Authorization` header verbatim, absence included, and let upstream enforce
its own authorization — because upstream is the only authority that can
fail closed on them.

## Credentials on the upstream leg

Gating decides what cachew serves *from its mirror*. `mayUseServiceCredentials`
(literally `repoPathForRequest(...).gated`) decides whose identity cachew
uses on the *upstream* leg of the same request — one predicate for both, so a
pass-through path added later can't be handed cachew's broad credentials
without also being brought under the gate. On a gated path, cachew's own
credentials replace the client's (safe: `authorizeServe` already proved this
client has access to this repo). On a pass-through path, the client's header
survives byte-for-byte.

## Cached per-repo "requires auth" flag

Separate from the uncached per-client check: a **cached, anonymous** ref
probe determines whether upstream serves the repo at all without
credentials. A `200` carrying an actual upload-pack ref advertisement →
public (skip the per-request check entirely — the common case pays
nothing); anything else → treat as auth-required. It fails closed (a probe
error is never cached) and collapses concurrent probes for the same repo
via `singleflight`.

The advertisement is what's checked, not the status, because an SSO or
interstitial login page is a `200` too. For the same reason the probe
refuses to follow a redirect to another host: the response that decides
access has to come from the host cachew is deciding about. Redirects within
the host are followed, capped at ten as net/http would.

Used for two things: skipping the per-client check for public repos, and
**excluding auth-required repos from all proactive upstream contact**.
`proactiveUpstreamContactAllowed` is that gate;
`backgroundFetchAllowed` is it plus the fetch backoff, for the contacts
that are fetches of the mirror. Between them they cover startup warm-up,
the pre-snapshot fetch, and the periodic LFS snapshot job (whose `git lfs
fetch` reaches upstream from inside snapshot generation, so the whole job
is skipped rather than a pre-step) — every place cachew would otherwise
touch upstream with its own credentials with no client to authorize.

Auth-required repos therefore refresh only synchronously, off the back of
an already-authorized on-demand request. The background fetches
`serveReadyRepo` submits are not gated here and must not be: they are
reachable only downstream of a full `authorizeServe` pass, so a client that
has already proved its access to that repository is driving them.

TTL: 15 minutes, configured by the top-level `requires-auth-ttl` option and
held in `repoauthz.Authorizer`'s bounded verdict cache
(`verdictCacheLimit = 4096`, oldest-live-first eviction). It is top-level
rather than per-strategy because one `Authorizer` now serves all three entry
points, so a per-strategy TTL would have had no single owner.

## Three entry points share one `Authorizer`

The git routes (`/git/...`), the object API (`/api/v1/object/git/...`,
`.../gomod/...`) and the gomod module proxy share **one**
`repoauthz.Authorizer` instance (constructed in `cmd/cachewd`'s
`newRegistries` and passed to each strategy that authorizes repository
reads): one verdict cache, one TTL, one `singleflight` group, so the same
private repository reached through any of the three is probed upstream once
per TTL, not once per entry point.

Only the anonymous per-repository visibility verdict is ever cached. The
per-client decision is computed per request, per entry point, and never
cached.

### Known risk: the public→private flip window

The cached verdict is *anonymous visibility*, and nothing invalidates it
when upstream changes. A repository that is public when cachew probes it and
is then made private upstream keeps its `public` verdict — and so keeps
being served to unauthenticated clients from the already-warm mirror — for
up to the full `requires-auth-ttl` (15 minutes by default) after the flip.

This is accepted, and bounded by construction: the window is at most one
TTL, it cannot extend (the verdict is not refreshed by use, only re-probed
after it expires), and it applies only to content already mirrored before
the flip. Neither GitHub nor generic git upstreams offer a visibility-change
hook cachew could subscribe to, so closing it would mean re-probing per
request — which is the cost this cache exists to avoid. Deployments that
care more about the window than the probe traffic should lower
`requires-auth-ttl`; setting it very low approaches per-request probing.

## Existence is not observable

Denied object reads are byte-identical to reads of an object that was never
written — same `404`, same body, same headers. Object keys are an unsalted
hash, so a client can compute the key of a repository it merely guessed;
distinguishing "no access" from "does not exist" would confirm the guess.
Outcomes that aren't a decision about the client (`probe_error`,
`internal_error`, `invalid_target`) keep their own status, since those are
operational signals an operator needs to see, not something to mask.

## Configuration

Two independent escape hatches, both logging a warning at startup when set:

| Flag | Covers | Does **not** cover |
| --- | --- | --- |
| `disable-upstream-authz` (`strategy git`) | the git strategy's `/git/...` routes | the object API |
| `disable-git-object-authz` (`strategy apiv1`) | `GET`/`HEAD /api/v1/object/git/<key>` | the git routes; git-namespace *mutations*, which stay `403` regardless |

They're deliberately separate: folding them together would mean opening the
git routes silently opened the object API too — the kind of implicit
widening this subsystem exists to prevent.

The one tunable that isn't an escape hatch is top-level `requires-auth-ttl`
(default `15m`), which bounds how long an anonymous visibility verdict is
reused — see the flip window above.

## Observability

- `cachew.git.authz_checks_total{repository,outcome}` — outcomes: `public`,
  `allowed`, `no_credentials`, `rejected`, `probe_error`, `internal_error`,
  `invalid_target`.
- `cachew.git.authz_check_duration_seconds`
- `cachew.git.background_fetch_skips_total{repository,reason}` — reasons:
  `requires_auth`, `backoff`, `probe_error`.
- A `git.authorize_serve` span carrying `cachew.authz_outcome`, and
  structured logs at each denial.

No credential value ever reaches logs, span attributes, metric labels, or
error bodies. Log output additionally escapes control characters
(`internal/logging`'s `sanitizeHandler`) so a client can't inject terminal
escape sequences into an operator's console via a value that ends up logged.

## Key files

| File | Role |
| --- | --- |
| `internal/strategy/git/authz.go` | `authorizeServe`, `validateRepoTarget`, `validateCanonicalRepoPath`, `repoPathForRequest`, `mayUseServiceCredentials`, `backgroundFetchAllowed`, `proactiveUpstreamContactAllowed` |
| `internal/strategy/git/proxy.go` | `directUpstream` — credential substitution on the upstream leg |
| `internal/repoauthz/repoauthz.go` | Shared `Authorizer`, verdict cache, `Outcome` type, `Respond`, `ProbeRefs`, `ProbeResult` |
| `internal/gitobject`, `internal/gomodobject` | Object metadata used to derive the repository an object belongs to, for the object-API authorization path |
| `internal/strategy/apiv1.go` | Object-API read gating (`respondObjectDenial`) |
| `internal/strategy/git/fuzz_test.go` | `FuzzRepoTargetResolution`, `FuzzUpstreamCredentialForwarding` — the identity-invariant fuzz targets |
| `internal/repoauthz/*_test.go` | `FuzzProbeRefsForwarding`, verdict-cache and fail-closed tests |

## Testing and fuzzing

Unit, integration (`-tags=integration`), and three native Go fuzz targets
(`testing.F`, `go test -fuzz`, no third-party fuzzer). Seed corpora —
including every bug's saved failing input — replay automatically on every
plain `go test ./...`. Re-run the fuzzers directly:

```sh
go test -run '^$' -fuzz='^FuzzRepoTargetResolution$'         -fuzztime=3m ./internal/strategy/git/
go test -run '^$' -fuzz='^FuzzUpstreamCredentialForwarding$' -fuzztime=3m ./internal/strategy/git/
go test -run '^$' -fuzz='^FuzzProbeRefsForwarding$'          -fuzztime=3m ./internal/repoauthz/
```

See PR #15's description for the full test inventory, the "fix removed →
test fails" table proving the tests aren't vacuous, and the accepted-risk
writeup for the cache-population timing side channel (deliberately left
open — see the `OutcomeProbeError`/timing note in that PR if you're
considering "fixing" it).
