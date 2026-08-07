package cache_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

func TestMemoryEvictionKeepsRecentlyRead(t *testing.T) {
	read := cache.NewKey("read-key")
	unread := cache.NewKey("unread-key")
	overflow := cache.NewKey("overflow-key")

	for _, test := range []struct {
		name string
		read func(ctx context.Context, t *testing.T, c cache.Cache, key cache.Key)
	}{
		{name: "Open", read: func(ctx context.Context, t *testing.T, c cache.Cache, key cache.Key) {
			t.Helper()
			r, _, err := c.Open(ctx, key)
			assert.NoError(t, err)
			assert.NoError(t, r.Close())
		}},
		{name: "Stat", read: func(ctx context.Context, t *testing.T, c cache.Cache, key cache.Key) {
			t.Helper()
			_, err := c.Stat(ctx, key)
			assert.NoError(t, err)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

			c, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1, MaxTTL: time.Hour})
			assert.NoError(t, err)
			defer c.Close()

			// Two ~500KB entries fit inside the 1MB limit, so nothing is evicted before the read below. The read
			// entry gets the earliest expiry so that an expiry-ordered eviction would pick it first.
			writeMemoryEntry(ctx, t, c, read, time.Hour)
			writeMemoryEntry(ctx, t, c, unread, 2*time.Hour)
			time.Sleep(time.Millisecond)

			test.read(ctx, t, c, read)
			time.Sleep(time.Millisecond)

			writeMemoryEntry(ctx, t, c, overflow, 3*time.Hour)

			_, _, err = c.Open(ctx, unread)
			assert.Error(t, err)

			r, _, err := c.Open(ctx, read)
			assert.NoError(t, err)
			assert.NoError(t, r.Close())

			r, _, err = c.Open(ctx, overflow)
			assert.NoError(t, err)
			assert.NoError(t, r.Close())
		})
	}
}

func TestMemoryEvictionAcrossNamespaces(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	c, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	first := c.Namespace("first")
	second := c.Namespace("second")
	key := cache.NewKey("shared-key")

	writeMemoryEntry(ctx, t, first, key, time.Hour)
	writeMemoryEntry(ctx, t, second, key, 2*time.Hour)
	time.Sleep(time.Millisecond)

	r, _, err := first.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	time.Sleep(time.Millisecond)

	writeMemoryEntry(ctx, t, first, cache.NewKey("overflow-key"), 3*time.Hour)

	_, _, err = second.Open(ctx, key)
	assert.Error(t, err)

	r, _, err = first.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
}

func writeMemoryEntry(ctx context.Context, t *testing.T, c cache.Cache, key cache.Key, ttl time.Duration) {
	t.Helper()
	w, err := c.Create(ctx, key, nil, ttl)
	assert.NoError(t, err)
	_, err = w.Write(make([]byte, 500*1024))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
}
