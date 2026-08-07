package cache_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
)

func TestMemoryCache(t *testing.T) {
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
		c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: 3 * time.Second})
		assert.NoError(t, err)
		return c
	})
}

func TestMemoryCacheSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}

	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{
		LimitMB: 50,
		MaxTTL:  10 * time.Minute,
	})
	assert.NoError(t, err)
	defer c.Close()

	cachetest.Soak(t, c, cachetest.SoakConfig{
		Duration:         time.Minute,
		NumObjects:       500,
		MaxObjectSize:    512 * 1024,
		MinObjectSize:    1024,
		OverwritePercent: 30,
		Concurrency:      8,
		TTL:              5 * time.Minute,
	})
}

// memoryWriteEntry writes size bytes to key with the given ttl and closes the writer.
func memoryWriteEntry(ctx context.Context, t *testing.T, c cache.Cache, key cache.Key, size int, ttl time.Duration) {
	t.Helper()
	w, err := c.Create(ctx, key, nil, ttl)
	assert.NoError(t, err)
	_, err = w.Write(make([]byte, size))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
}

// memoryActualSize sums the real Content-Length of every key still present, i.e. the ground truth
// that Stats().Size should match.
func memoryActualSize(ctx context.Context, t *testing.T, c cache.Cache, keys []cache.Key) int64 {
	t.Helper()
	var total int64
	for _, key := range keys {
		headers, err := c.Stat(ctx, key)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		assert.NoError(t, err)
		length, err := strconv.ParseInt(headers.Get("Content-Length"), 10, 64)
		assert.NoError(t, err)
		total += length
	}
	return total
}

// TestMemoryCloseOverwriteEvictionDoesNotDoubleCountSize reproduces the accounting bug where
// evictOldest could select the very entry Close() is about to overwrite (it's still present in
// nsEntries at that point), and Close() would then subtract that entry's old size a second time.
func TestMemoryCloseOverwriteEvictionDoesNotDoubleCountSize(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1, MaxTTL: time.Hour})
	assert.NoError(t, err)

	key1 := cache.NewKey("key1")
	key2 := cache.NewKey("key2")

	memoryWriteEntry(ctx, t, c, key1, 600*1024, time.Hour)
	memoryWriteEntry(ctx, t, c, key2, 300*1024, 2*time.Hour)
	memoryWriteEntry(ctx, t, c, key1, 900*1024, time.Hour) // overwrite key1 with a larger value, forcing eviction

	stats, err := c.Stats(ctx)
	assert.NoError(t, err)
	assert.Equal(t, memoryActualSize(ctx, t, c, []cache.Key{key1, key2}), stats.Size)
}

// TestMemoryCurrentSizeNeverGoesNegative checks that a sequence of writes, overwrites, and
// evictions never drives the tracked size negative, and always ends up matching the real total.
func TestMemoryCurrentSizeNeverGoesNegative(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1, MaxTTL: time.Hour})
	assert.NoError(t, err)

	keys := []cache.Key{cache.NewKey("a"), cache.NewKey("b"), cache.NewKey("c")}
	sizes := []int{600 * 1024, 300 * 1024, 900 * 1024, 700 * 1024, 100 * 1024}

	for i, size := range sizes {
		key := keys[i%len(keys)]
		memoryWriteEntry(ctx, t, c, key, size, time.Duration(i+1)*time.Hour)

		stats, err := c.Stats(ctx)
		assert.NoError(t, err)
		assert.True(t, stats.Size >= 0, "currentSize went negative: %d", stats.Size)
		assert.Equal(t, memoryActualSize(ctx, t, c, keys), stats.Size)
	}
}
