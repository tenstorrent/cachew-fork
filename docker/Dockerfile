# Runtime image - binaries are pre-built on the host
FROM alpine:3.24

ARG TARGETARCH

SHELL ["/bin/sh", "-o", "pipefail", "-c"]

# git is pinned rather than left floating on whatever alpine:3.24/main
# resolves to at build time: cachew shells out to git directly, so a silent
# minor-version bump can change git's own defaults underneath it (this is how
# git 2.51's maintenance.auto went unnoticed until it raced cachew's own
# repacks). Bump this deliberately, checking release notes, rather than
# letting it drift. Currently the latest git in alpine:3.24/main for both
# x86_64 and aarch64.
RUN apk add --no-cache ca-certificates curl git=2.54.0-r0 git-daemon git-lfs tzdata zstd && \
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
