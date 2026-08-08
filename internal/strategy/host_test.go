package strategy_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/strategy"
)

func TestHostCaching(t *testing.T) {
	callCount := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("response"))
	}))
	defer backend.Close()

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: backend.URL}, memCache, mux)
	assert.NoError(t, err)

	// Request path must include the host prefix from the target URL
	u, _ := url.Parse(backend.URL)
	reqPath := "/" + u.Host + "/test"

	req1 := httptest.NewRequestWithContext(ctx, http.MethodGet, reqPath, nil)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, "response", w1.Body.String())
	assert.Equal(t, 1, callCount)

	req2 := httptest.NewRequestWithContext(ctx, http.MethodGet, reqPath, nil)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, "response", w2.Body.String())
	assert.Equal(t, 1, callCount, "second request should be served from cache")
}

func TestHostNonOKStatus(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer backend.Close()

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: backend.URL}, memCache, mux)
	assert.NoError(t, err)

	// Request path must include the host prefix from the target URL
	u, _ := url.Parse(backend.URL)
	reqPath := "/" + u.Host + "/missing"

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, reqPath, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "not found", w.Body.String())

	key := cache.NewKey(backend.URL + "/missing")
	_, _, err = memCache.Open(context.Background(), key)
	assert.Error(t, err, "non-OK responses should not be cached")
}

func TestHostInvalidTargetURL(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: "://invalid"}, memCache, mux)
	assert.Error(t, err)
}

func TestHostHeaders(t *testing.T) {
	var receivedHeaders http.Header
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewHost(ctx, strategy.HostConfig{
		Target:  backend.URL,
		Headers: map[string]string{"Authorization": "Bearer QQ==", "X-Custom": "value"},
	}, memCache, mux)
	assert.NoError(t, err)

	u, _ := url.Parse(backend.URL)
	reqPath := "/" + u.Host + "/test"

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, reqPath, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "Bearer QQ==", receivedHeaders.Get("Authorization"))
	assert.Equal(t, "value", receivedHeaders.Get("X-Custom"))
}

// upstreamPathRecorder returns an httptest.Server that records the path of every
// request it receives, along with a function to read the recorded paths.
func upstreamPathRecorder(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var paths []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(backend.Close)
	return backend, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(paths)
	}
}

func TestHostBuildTargetURLJoinsTargetPathAndRequestPath(t *testing.T) {
	tests := []struct {
		name       string
		targetPath string
		reqSuffix  string
		wantPath   string
	}{
		{name: "NoTargetPathWithRequestPath", targetPath: "", reqSuffix: "/file.txt", wantPath: "/file.txt"},
		{name: "TargetPathWithRequestPath", targetPath: "/a", reqSuffix: "/file.txt", wantPath: "/a/file.txt"},
		{name: "TargetPathTrailingSlashWithRootRequest", targetPath: "/a/", reqSuffix: "/", wantPath: "/a/"},
		{name: "NoTargetPathWithRootRequest", targetPath: "", reqSuffix: "/", wantPath: "/"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend, recordedPaths := upstreamPathRecorder(t)

			_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
			memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
			assert.NoError(t, err)
			defer memCache.Close()

			mux := http.NewServeMux()
			_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: backend.URL + test.targetPath}, memCache, mux)
			assert.NoError(t, err)

			u, _ := url.Parse(backend.URL)
			reqPath := "/" + u.Host + strings.TrimSuffix(test.targetPath, "/") + test.reqSuffix

			req := httptest.NewRequestWithContext(ctx, http.MethodGet, reqPath, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, []string{test.wantPath}, recordedPaths())
		})
	}
}

// TestHostDistinctTargetPathsProduceDistinctUpstreamURLs is a regression test for the bug where
// buildTargetURL overwrote the configured target's path instead of joining it with the request
// path, causing two differently-configured host blocks to collide on the same upstream URL (and
// therefore the same cache key).
func TestHostDistinctTargetPathsProduceDistinctUpstreamURLs(t *testing.T) {
	backend, recordedPaths := upstreamPathRecorder(t)

	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	hostA, err := strategy.NewHost(ctx, strategy.HostConfig{Target: backend.URL + "/a"}, memCache, mux)
	assert.NoError(t, err)
	hostB, err := strategy.NewHost(ctx, strategy.HostConfig{Target: backend.URL + "/b"}, memCache, mux)
	assert.NoError(t, err)

	u, _ := url.Parse(backend.URL)
	reqPathA := "/" + u.Host + "/a/file.txt"
	reqPathB := "/" + u.Host + "/b/file.txt"

	reqA := httptest.NewRequestWithContext(ctx, http.MethodGet, reqPathA, nil)
	wA := httptest.NewRecorder()
	mux.ServeHTTP(wA, reqA)
	assert.Equal(t, http.StatusOK, wA.Code)

	reqB := httptest.NewRequestWithContext(ctx, http.MethodGet, reqPathB, nil)
	wB := httptest.NewRecorder()
	mux.ServeHTTP(wB, reqB)
	assert.Equal(t, http.StatusOK, wB.Code)

	paths := recordedPaths()
	assert.Equal(t, []string{"/a/file.txt", "/b/file.txt"}, paths)
	assert.NotEqual(t, paths[0], paths[1])
	assert.NotEqual(t, hostA.String(), hostB.String())
}

func TestHostString(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{Level: "error"})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	host, err := strategy.NewHost(ctx, strategy.HostConfig{Target: "https://example.com/prefix"}, memCache, mux)
	assert.NoError(t, err)

	assert.Equal(t, "host:example.com/prefix", host.String())
}
