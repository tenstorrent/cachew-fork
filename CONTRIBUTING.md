# Contributing to Cachew

Thank you for your interest in contributing to Cachew! This guide will help you get started with local development, building, and testing.

## Local Development

**Run natively (fastest for development):**
```bash
just run                    # Build and run on localhost:8080
```

**Run in Docker:**
```bash
just docker run             # Build and run in container
just docker run debug       # Run with debug logging
```

## Building and Testing

```bash
just build              # Build for current platform
just build-all          # Build all platforms
just build GOOS=linux GOARCH=amd64 # Build for linux/amd64
just test               # Run tests
just lint               # Lint code
just fmt                # Format code
```

### Fuzzing

The authorization code has three Go native fuzz targets (`testing.F`; no third-party fuzzer):

| Target | Package | What it asserts |
| --- | --- | --- |
| `FuzzRepoTargetResolution` | `internal/strategy/git` | For any accepted `(host, path, query)`: mirror-serving paths are always gated, credential substitution never disagrees with gating, the repository identity authorization uses is byte-identical to the one the mirror resolves to (or resolution fails closed), and the mirror path never escapes the mirror root. |
| `FuzzUpstreamCredentialForwarding` | `internal/strategy/git` | For any target and any client `Authorization`: cachew's own credentials replace the client's only on a gated path, only for `github.com`, only as exactly `Basic base64("x-access-token:"+token)`, and only for the org gating authorized — anywhere else the client's header values survive byte-for-byte, absence included. |
| `FuzzProbeRefsForwarding` | `internal/repoauthz` | For any repo path and authorization value, against a local `httptest` upstream: either the probe fails without reaching upstream, or upstream sees the client's credential exactly once and unchanged *and* the request that produced the verdict really was `….git/info/refs?service=git-upload-pack` for that repository. |

**Fast sanity check.** The seed corpora — the `f.Add(...)` calls in the test files, plus anything saved under
`testdata/fuzz/` — run as ordinary subtests under plain `go test ./...` (and so under `just test` and CI) even without
`-fuzz`. That already exercises the known-important cases on every run; fuzzing is only for finding new ones.

**Real fuzzing.** `-fuzz` accepts one package at a time, so run each target separately. `-run '^$'` skips the package's
other tests so the whole budget goes to the target:

```bash
go test -run '^$' -fuzz='^FuzzRepoTargetResolution$'         -fuzztime=2m ./internal/strategy/git/
go test -run '^$' -fuzz='^FuzzUpstreamCredentialForwarding$' -fuzztime=2m ./internal/strategy/git/
go test -run '^$' -fuzz='^FuzzProbeRefsForwarding$'          -fuzztime=2m ./internal/repoauthz/
```

A couple of minutes each is enough to meaningfully exercise them — on a normal workstation that is millions of
executions for the cheaper targets, and it is the scale at which the bugs these targets were written for were found (one
of them surfaced after ~33 seconds). Anything shorter is a smoke test of the invocation, not of the property.

**If a run fails.** Go writes the failing input to `testdata/fuzz/<FuzzTestName>/<hash>` in the target's package. That
file is a new seed: from then on the case is replayed by plain `go test`, so **commit it** along with the fix, and it
becomes a permanent regression test. The corpus entry in
`internal/strategy/git/testdata/fuzz/FuzzRepoTargetResolution/` is exactly that — a bug found by a fuzz run, fixed, and
pinned. Re-run the target after fixing to confirm it stays clean well past the point of the original failure.

## Docker

```bash
just docker build           # Build single-arch Docker image for local use
just docker build-multi     # Build multi-arch image (amd64 + arm64)
just docker run             # Run in container
just docker run debug       # Run with debug logging
just docker clean           # Clean up Docker images
```

## Fork identity and syncing with upstream

This repo is a fork of upstream [`block/cachew`](https://github.com/block/cachew). Two branches matter here:

- **`main`** mirrors upstream unmodified, module path and all (`github.com/block/cachew`). Never add fork-specific commits or rename anything on it.
- **`tt-main`** is this fork's own trunk. Its module is deliberately renamed to `github.com/tenstorrent/internal-cachew`, decoupled from upstream's identity, so this fork can be depended on directly by other internal Go modules without a module-path/fetch-path mismatch.

To pull in upstream changes, merge `main` into `tt-main` with:

```bash
scripts/sync-upstream.sh
```

This merges `origin/main`, then rewrites any `github.com/block/cachew` import that survived the merge (only possible for brand-new files upstream added, since an ordinary 3-way merge already keeps this fork's renamed imports for every file upstream didn't touch), then runs `gofmt`/`go build`/`go vet`/`go test` to confirm the result is green before you push. If the merge itself conflicts — which happens when upstream edits something this fork has diverged on, not from the renamed import paths — resolve normally and re-run the script to finish the normalization pass.

## Using the Cache

The `cachew` CLI client interacts with the `cachewd` server:

```bash
# Upload to cache
cachew put my-key myfile.txt --ttl 24h

# Download from cache  
cachew get my-key -o myfile.txt

# Check if cached
cachew stat my-key

# Snapshot a directory
cachew snapshot deps-cache ./node_modules --ttl 7d

# Restore a directory
cachew restore deps-cache ./node_modules
```
