# Cachew

Cachew (pronounced "cashew") is a tiered, protocol-aware, caching HTTP proxy for software engineering infrastructure. It understands higher-level protocols (Git, Docker, Go modules, etc.) and makes smarter caching decisions than a naive HTTP proxy.

## Strategies

### Git

Caches Git repositories with three complementary techniques:

1. **Snapshots** — periodic `.tar.zst` archives that restore 4–5x faster than `git clone`.
2. **Pack caching** — passthrough caching of packs from `git-upload-pack` for incremental pulls.
3. **Incremental pull-through**: when the mirror lacks a wanted object, fetches only the missing delta from upstream and serves the pack locally instead of forwarding the whole request. On by default; set `incremental-pullthrough = false` to disable.

Redirect Git traffic through cachew:

```ini
[url "https://cachew.example.com/git/github.com/"]
  insteadOf = https://github.com/
```

Restore a repository from a snapshot (with automatic delta bundle to reach HEAD):

```sh
cachew git restore https://github.com/org/repo ./repo
```

```hcl
git {
  snapshot-interval         = "1h"
  repack-interval           = "1h"
  incremental-pullthrough   = true    # default; false disables incremental pull-through
  incremental-fetch-timeout = "2m"    # default
}
```

### GitHub Releases

Caches public and private GitHub release assets. Private orgs use a token or GitHub App for authentication.

**URL pattern:** `/github-releases/{owner}/{repo}/{tag}/{asset}`

```hcl
github-releases {
  token        = "${GITHUB_TOKEN}"
  private-orgs = ["myorg"]
}
```

### Go Modules

Go module proxy (`GOPROXY`-compatible). Private modules are fetched via git clone.

**URL pattern:** `/gomod/...`

```sh
export GOPROXY=http://cachew.example.com/gomod,direct
```

```hcl
gomod {
  proxy         = "https://proxy.golang.org"
  private-paths = ["github.com/myorg/*"]
}
```

### Hermit

Caches [Hermit](https://cashapp.github.io/hermit/) package downloads. GitHub release URLs are automatically routed through the `github-releases` strategy.

**URL pattern:** `/hermit/{host}/{path...}`

```hcl
hermit {}
```

### Artifactory

Caches artifacts from JFrog Artifactory with host-based or path-based routing.

```hcl
artifactory "example.jfrog.io" {
  target = "https://example.jfrog.io"
}
```

### Host

Generic reverse-proxy caching for arbitrary HTTP hosts, with optional custom headers.

```hcl
host "https://ghcr.io" {
  headers = {
    "Authorization": "Bearer QQ=="
  }
}

host "https://w3.org" {}
```

### HTTP Proxy

Caching proxy for clients that use absolute-form HTTP requests (e.g. Android `sdkmanager --proxy_host`).

```hcl
proxy {}
```

## Cache Backends

Multiple backends can be configured simultaneously — they are automatically combined into a tiered cache. Cache blocks
are ordered from lowest/nearest to highest/authoritative. Reads check each tier in order and backfill lower tiers on a
hit. Writes go to all tiers in parallel. Replica invalidations evict only non-authoritative tiers; the final cache block
is authoritative. Tiered caches use the metadata backend to track authoritative ETags and invalidate stale lower-tier
copies before falling through to the authoritative tier.

### Memory

In-memory LRU cache.

```hcl
memory {
  limit-mb = 1024   # default
  max-ttl  = "1h"   # default
}
```

### Disk

On-disk LRU cache with TTL-based eviction.

```hcl
disk {
  limit-mb = 250000
  max-ttl  = "8h"
}
```

### S3

S3-compatible object storage (AWS S3, MinIO, etc.).

```hcl
s3 {
  bucket   = "my-cache-bucket"
  endpoint = "s3.amazonaws.com"
  region   = "us-east-1"
}
```

## Authorization (OPA)

Cachew uses [Open Policy Agent](https://www.openpolicyagent.org/) for request authorization. The default policy allows all requests from localhost and restricts remote access to non-admin paths (`/api/*`, `/admin/*`).

Policies must be in `package cachew.authz` and define an `allow` rule. If `allow` is true the request proceeds; otherwise it is rejected with 403.

```hcl
opa {
  policy = <<EOF
    package cachew.authz
    default allow := false
    allow if input.headers["authorization"]
  EOF
}
```

Or reference an external file with optional data:

```hcl
opa {
  policy-file = "./policy.rego"
  data-file   = "./opa-data.json"
}
```

**Input fields:** `input.method`, `input.path` (string array), `input.headers`, `input.remote_addr` (includes port — use `startswith` to match by IP).

### Testing policies

The `test` field holds a Rego test module that is run against the policy when `cachewd` starts. Any rule prefixed with `test_` is executed; if a test fails, `cachewd` exits.

```hcl
opa {
  policy = <<EOF
    package cachew.authz
    default allow := false
    allow if input.method == "POST"
  EOF
  test = <<EOF
    package cachew.authz_test
    import data.cachew.authz

    test_post_allowed if authz.allow with input as {"method": "POST"}
    test_get_denied if not authz.allow with input as {"method": "GET"}
  EOF
}
```

### Per-repository client authorization

OPA above is network/path-level: it never looks at *which repository* a
request is for, or *whose* credentials it carries. A separate mechanism
re-validates each client against the specific repository they're asking
for, using only the credentials the client itself supplied — so a client
with no access to a private repo can't read it just because cachew's own
mirror is warm. Covers the git strategy's routes, the object API, and the
Go module proxy. See [`docs/git-authz.md`](docs/git-authz.md) for the full
design. Escape hatches: `disable-upstream-authz` (`strategy git`),
`disable-git-object-authz` (`strategy apiv1`).

## GitHub App Authentication

For private Git repositories and GitHub release assets, configure a GitHub App:

```hcl
github-app {
  app-id           = "12345"
  private-key-path = "./github-app.pem"
  installations    = { "myorg": "67890" }
}
```

Installations can also be discovered dynamically via the GitHub API.

## CLI

### Server (`cachewd`)

```sh
cachewd --config cachew.hcl
cachewd --schema  # print config schema
```

### Client (`cachew`)

```sh
# Object operations
cachew get <namespace> <key> [-o file]
cachew put <namespace> <key> [file] [--ttl 1h]
cachew stat <namespace> <key>
cachew delete <namespace> <key>
cachew namespaces

# Directory snapshots
cachew save <namespace> <directory> [paths...] (--key <key> | -H <glob>) [--ttl 1h] [--exclude pattern]
cachew restore <namespace> <directory> (--key <key> | -H <glob>)  # exit 0 hit, 2 miss, 1 error

# Git
cachew git restore <repo-url> <directory> [--no-bundle]
```

**Global flags:** `--url` (`CACHEW_URL`), `--authorization` (`CACHEW_AUTHORIZATION`), `--platform` (prefix keys with `os-arch`), `--daily`/`--hourly` (prefix keys with date).

## Observability

```hcl
log {
  level = "info"  # debug, info, warn, error
}

metrics {
  service-name     = "cachew"
  cardinality-limit = 2000  # default; caps distinct attribute sets per instrument per collection cycle
}
```

Instruments labeled by something with unbounded lifetime cardinality (e.g. `repository`) keep every attribute set
they have ever seen for the life of the process — the OTel Go SDK has no way to forget one. Past `cardinality-limit`,
new attribute sets are dropped from that instrument and aggregated into a synthetic `otel.metric.overflow="true"`
series instead of growing memory without bound. `0` or negative disables the limit.

Storage usage is measured periodically and exported as gauges:

| Series | Labels | Meaning |
| --- | --- | --- |
| `cachew_storage_bytes` | `component` (`git_mirrors`, `object_cache`) | On-disk bytes per component; total cachew usage is `sum(cachew_storage_bytes)` |
| `cachew_storage_capacity_bytes` | `component` | Configured limit, for components that have one |
| `cachew_storage_repo_bytes` | `repository` | On-disk bytes of each mirrored repository |

Mirror sizes require walking each mirror directory, so the scan is periodic rather than continuous. It runs on its own
goroutine rather than the shared job-scheduler worker pool, so it never delays a fetch or clone. Tune or disable it
with `git { storage-scan-interval = "15m" }`; `0` or negative disables storage measurement. Sizes are apparent file sizes rather
than allocated blocks, and the `object_cache` series is absent for backends that cannot report stats cheaply (S3).

`cachew_authz_disabled{check=...}` is 1 if a client-authorization escape hatch is enabled, 0 otherwise, set once at
startup. `check="upstream"` is the git strategy's `disable-upstream-authz`; `check="object_api"` is the object API's
`disable-git-object-authz`. Both are meant to page someone — see [Per-repository client authorization](docs/git-authz.md).

Admin endpoints: `/_liveness`, `/_readiness`, `PUT /admin/log/level`, `/admin/pprof/`.

## Full Configuration Example

```hcl
state = "./state"
bind  = "0.0.0.0:8080"
url   = "http://cachew.example.com:8080/"

log {
  level = "info"
}

opa {
  policy = <<EOF
    package cachew.authz
    default allow := false
    allow if startswith(input.remote_addr, "127.0.0.1:")
  EOF
}

metrics {}

github-app {
  app-id           = "12345"
  private-key-path = "./github-app.pem"
}

git-clone {}

git {
  snapshot-interval = "1h"
  repack-interval   = "1h"
}

github-releases {
  token        = "${GITHUB_TOKEN}"
  private-orgs = ["myorg"]
}

gomod {
  proxy         = "https://proxy.golang.org"
  private-paths = ["github.com/myorg/*"]
}

hermit {}

host "https://ghcr.io" {
  headers = {
    "Authorization": "Bearer ${GHCR_TOKEN}"
  }
}

disk {
  limit-mb = 250000
  max-ttl  = "8h"
}

proxy {}
```
