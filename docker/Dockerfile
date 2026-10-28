# Runtime image - binaries are pre-built on the host
FROM alpine:3.24

ARG TARGETARCH
ARG GIT_LFS_VERSION=3.8.1

SHELL ["/bin/sh", "-o", "pipefail", "-c"]

# git is pinned rather than left floating on whatever alpine:3.24/main
# resolves to at build time: cachew shells out to git directly, so a silent
# minor-version bump can change git's own defaults underneath it (this is how
# git 2.51's maintenance.auto went unnoticed until it raced cachew's own
# repacks). Bump this deliberately, checking release notes, rather than
# letting it drift. Currently the latest git in alpine:3.24/main for both
# x86_64 and aarch64.
RUN apk add --no-cache 'ca-certificates>=20261020-r0' curl git=2.54.0-r0 git-daemon tzdata zstd && \
    case "${TARGETARCH}" in \
      amd64) git_lfs_sha=e72cf723a4a660438862282e8aecf3885fc8123e88bfcad534cf6b941a354b75 ;; \
      arm64) git_lfs_sha=67895dd77b63329796bc5efdd47b57f218e1f7b98717ebaf936a8238dbcaec0f ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    curl -fsSLo /tmp/git-lfs.tar.gz \
      "https://github.com/git-lfs/git-lfs/releases/download/v${GIT_LFS_VERSION}/git-lfs-linux-${TARGETARCH}-v${GIT_LFS_VERSION}.tar.gz" && \
    echo "${git_lfs_sha}  /tmp/git-lfs.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/git-lfs.tar.gz -C /tmp "git-lfs-${GIT_LFS_VERSION}/git-lfs" && \
    mv "/tmp/git-lfs-${GIT_LFS_VERSION}/git-lfs" /usr/local/bin/git-lfs && \
    chmod 0755 /usr/local/bin/git-lfs && \
    addgroup -g 1000 cachew && \
    adduser -D -u 1000 -G cachew cachew

# Set working directory (config uses relative paths like ./state/cache)
WORKDIR /app

# Copy pre-built binary from host (built by Justfile)
COPY dist/cachewd-linux-${TARGETARCH} /usr/local/bin/cachewd
COPY dist/cachew-linux-${TARGETARCH} /usr/local/bin/cachew

# Copy default configuration file
COPY cachew.hcl /app/cachew.hcl

# Create state directory with proper permissions
RUN mkdir -p /app/state/cache && \
    chown -R cachew:cachew /app

# Switch to non-root user
USER cachew

# Bind to all interfaces in Docker (can be overridden with CACHEW_BIND env var)
ENV CACHEW_BIND=0.0.0.0:8080

ENTRYPOINT ["/usr/local/bin/cachewd"]
CMD ["--config", "/app/cachew.hcl"]
