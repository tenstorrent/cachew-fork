package git

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/cgi" //nolint:gosec // CVE-2016-5386 only affects Go < 1.6.3
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/errors"

	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/httputil"
	"github.com/tenstorrent/internal-cachew/internal/logging"
)

// bufferedResponseWriter buffers the response up to a threshold before writing
// to the underlying writer. Once the threshold is exceeded, the buffer is flushed
// and subsequent writes go directly through. If the threshold is never exceeded,
// the caller must call commit() explicitly, or the response is discarded (allowing
// a clean fallback to a different handler).
type bufferedResponseWriter struct {
	w         http.ResponseWriter
	headers   http.Header
	buf       bytes.Buffer
	code      int
	committed bool
	threshold int
}

func newBufferedResponseWriter(w http.ResponseWriter, threshold int) *bufferedResponseWriter {
	return &bufferedResponseWriter{
		w:         w,
		headers:   make(http.Header),
		code:      http.StatusOK,
		threshold: threshold,
	}
}

func (b *bufferedResponseWriter) Header() http.Header {
	if b.committed {
		return b.w.Header()
	}
	return b.headers
}

func (b *bufferedResponseWriter) WriteHeader(code int) {
	if !b.committed {
		b.code = code
	}
}

func (b *bufferedResponseWriter) Write(p []byte) (int, error) {
	if b.committed {
		return b.w.Write(p) //nolint:wrapcheck
	}
	if b.buf.Len()+len(p) >= b.threshold {
		b.commit()
		return b.w.Write(p) //nolint:wrapcheck
	}
	n, _ := b.buf.Write(p) // bytes.Buffer.Write never returns an error
	return n, nil
}

// commit flushes the buffered response to the underlying writer.
func (b *bufferedResponseWriter) commit() {
	if b.committed {
		return
	}
	b.committed = true
	for k, vs := range b.headers {
		for _, v := range vs {
			b.w.Header().Add(k, v)
		}
	}
	b.w.WriteHeader(b.code)
	_, _ = io.Copy(b.w, &b.buf) //nolint:errcheck
}

// backendFallback says why a local serve did not produce a usable response, so
// the caller can decide what to repair before retrying. The zero value means
// the response was committed to the client and no retry is possible.
type backendFallback int

const (
	backendServed backendFallback = iota
	// backendNotOurRef means upload-pack rejected the wants because the mirror
	// lacks the object, typically a force-push orphaning an advertised commit.
	backendNotOurRef
	// backendStaleCommitGraph means the commit-graph indexes commits the object
	// store no longer has, so the graph has to be rewritten before a retry can
	// help.
	backendStaleCommitGraph
)

// staleCommitGraphRepairTimeout bounds every graph rewrite a serve failure
// triggers, inline or background. The rewrite is a local pass over existing
// packs, far cheaper than a clone, but the inline one holds a client.
const staleCommitGraphRepairTimeout = 2 * time.Minute

// serveFromBackend serves the request using git http-backend against the local
// mirror. It returns backendServed when the response reached the client, or a
// backendFallback reason when the request should be retried — "not our ref"
// indicates the local mirror is missing an object the client wants (typically
// due to a concurrent force-push fetch orphaning a previously-advertised
// commit), and a stale commit-graph indicates the mirror's commit index
// disagrees with the objects a prune left behind.
func (s *Strategy) serveFromBackend(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository) backendFallback {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	gitPath, err := exec.LookPath("git")
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "git not found in PATH")
		return backendServed
	}

	absRoot, err := filepath.Abs(s.cloneManager.Config().MirrorRoot)
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "failed to get absolute path")
		return backendServed
	}

	host := r.PathValue("host")
	pathValue := r.PathValue("path")

	var gitOperation string
	var repoPathWithSuffix string

	for _, op := range []string{"/info/refs", "/git-upload-pack", "/git-receive-pack"} {
		if idx := strings.Index(pathValue, op); idx != -1 {
			repoPathWithSuffix = pathValue[:idx]
			gitOperation = pathValue[idx:]
			break
		}
	}

	repoPath := strings.TrimSuffix(repoPathWithSuffix, ".git")
	backendPath := "/" + host + "/" + repoPath + gitOperation

	logger.DebugContext(r.Context(), "Serving with git http-backend", "original_path", r.URL.Path,
		"backend_path", backendPath, "clone_path", repo.Path())

	// No read lock needed: git http-backend (upload-pack) handles concurrent
	// access safely via git's own file-level locking. Holding a read lock here
	// blocks fetches for the entire duration of a clone stream (minutes for
	// large repos), which combined with RWMutex writer-priority semantics
	// causes all subsequent requests to hang.

	// Buffer up to 1 MB before committing to the client. "not our ref" errors
	// are always tiny (<1 KB), so if the response stays small we can detect and
	// suppress the error before any bytes reach the client, allowing a clean
	// fallback to upstream. Large successful pack responses exceed the threshold
	// and are streamed through normally.
	const fallbackThreshold = 1 << 20 // 1 MB
	bw := newBufferedResponseWriter(w, fallbackThreshold)

	var stderrBuf bytes.Buffer

	// Host Git configuration is unrelated to serving mirrors and can add command-backed upload-pack behavior.
	handler := &cgi.Handler{
		Path:   gitPath,
		Args:   []string{"http-backend"},
		Stderr: &stderrBuf,
		Env: []string{
			"GIT_PROJECT_ROOT=" + absRoot,
			"GIT_HTTP_EXPORT_ALL=1",
			"GIT_HTTP_MAX_REQUEST_BUFFER=100M",
			"GIT_CONFIG_NOSYSTEM=1",
			"GIT_CONFIG_GLOBAL=" + os.DevNull,
			"PATH=" + os.Getenv("PATH"),
		},
	}

	r2 := r.Clone(r.Context())
	r2.URL.Path = backendPath

	// Go's cgi.Handler rejects chunked request bodies with a 400.
	// Buffer the body so we can set ContentLength and clear TransferEncoding.
	if r2.ContentLength < 0 {
		body, err := io.ReadAll(r2.Body)
		if err != nil {
			httputil.ErrorResponse(w, r, http.StatusInternalServerError, "failed to read request body")
			return backendServed
		}
		r2.Body = io.NopCloser(bytes.NewReader(body))
		r2.ContentLength = int64(len(body))
		r2.TransferEncoding = nil
	}

	handler.ServeHTTP(bw, r2)

	if stderrBuf.Len() > 0 {
		stderr := stderrBuf.String()
		logger.ErrorContext(r.Context(), "git http-backend error", "stderr", stderr, "path", backendPath)
		if isStaleCommitGraphError(stderr) {
			// An uncommitted response is still retryable, so the caller repairs
			// inline and re-serves. Once committed the request is lost, but later
			// requests should not keep paying for the same broken index.
			if !bw.committed {
				return backendStaleCommitGraph
			}
			s.repairCommitGraphAsync(r.Context(), repo)
		}
		if !bw.committed && (strings.Contains(stderr, "not our ref") || strings.Contains(stderr, "unknown ref")) {
			return backendNotOurRef
		}
	}

	bw.commit()
	return backendServed
}

// repairCommitGraphAsync rewrites the mirror's commit-graph in the background.
//
// Detached from the request context on purpose: the client that tripped the
// fault is already being failed, and tying the repair to its connection would
// abandon the work as soon as it disconnected. The request's logger is carried
// across so the repair is still attributed to the mirror it belongs to, and
// RewriteCommitGraph takes the fetch exclusion itself, so it serialises against
// fetches and snapshots.
func (s *Strategy) repairCommitGraphAsync(ctx context.Context, repo *gitclone.Repository) {
	upstream := repo.UpstreamURL()
	if _, loaded := s.commitGraphRepairs.LoadOrStore(upstream, struct{}{}); loaded {
		return
	}
	go func() {
		defer s.commitGraphRepairs.Delete(upstream)
		repairCtx := context.WithoutCancel(ctx)
		if err := s.rewriteCommitGraph(repairCtx, repo); err != nil {
			logging.FromContext(repairCtx).WarnContext(repairCtx, "Commit-graph repair failed", "upstream", upstream,
				"error", err)
		}
	}()
}

// rewriteCommitGraph rewrites the mirror's commit-graph within
// staleCommitGraphRepairTimeout and records the repair. The timeout covers the
// wait for the fetch exclusion too, so a long fetch or prune cannot hold a
// client indefinitely.
func (s *Strategy) rewriteCommitGraph(ctx context.Context, repo *gitclone.Repository) error {
	repairCtx, cancel := context.WithTimeout(ctx, staleCommitGraphRepairTimeout)
	defer cancel()
	start := time.Now()
	err := repo.RewriteCommitGraph(repairCtx)
	status := statusSuccess
	if err != nil {
		status = statusError
	}
	s.metrics.recordOperation(ctx, "commit-graph-repair", status, triggerRequest, time.Since(start))
	return errors.Wrap(err, "rewrite commit-graph")
}

// isStaleCommitGraphError reports whether git's stderr shows a commit-graph
// naming a commit the object store no longer has.
//
// Two wordings matter, because the same broken index fails in two places.
// Reading the graph directly says "Could not read <oid>" and "unable to parse
// commit <oid>". Serving a want through upload-pack instead dies one level down,
// as "git-pack-objects died with error" plus "possible repository corruption on
// the remote side", because pack-objects cannot resolve a commit the graph
// claims. The upload-pack pair is matched together and only when it names
// corruption, so a genuine pack failure is still reported as one.
func isStaleCommitGraphError(stderr string) bool {
	if strings.Contains(stderr, "unable to parse commit") {
		return true
	}
	if strings.Contains(stderr, "Could not read") && strings.Contains(stderr, "commit-graph") {
		return true
	}
	return strings.Contains(stderr, "git-pack-objects died with error") &&
		strings.Contains(stderr, "possible repository corruption on the remote side")
}

// checkRefsStale checks whether the local mirror's refs are behind upstream.
// Returns true if a fetch is needed. The caller decides whether to fetch
// synchronously or fall back to upstream.
func (s *Strategy) checkRefsStale(ctx context.Context, repo *gitclone.Repository) (bool, error) {
	needsFetch, err := repo.EnsureRefsUpToDate(ctx)
	if err != nil {
		return false, errors.Wrap(err, "check refs up to date")
	}
	return needsFetch, nil
}
