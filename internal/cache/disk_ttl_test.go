package cache_test

import (
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/logging"
)

// A read past halfway through an entry's TTL slides its expiry forward, so an
// object in demand stays cached instead of expiring on a fixed schedule.
func TestDiskReadExtendsTTL(t *testing.T) {
	dir := t.TempDir()
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          dir,
		LimitMB:       10,
		MaxTTL:        time.Hour,
		EvictInterval: time.Hour, // sweeps must not interfere; expiry is checked on read
	})
	assert.NoError(t, err)
	defer c.Close()

	key := cache.NewKey("extended")
	w, err := c.Create(ctx, key, nil, time.Second)
	assert.NoError(t, err)
	_, err = w.Write([]byte("payload"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Past half the TTL, so the read can slide expiry. Three more reads at the
	// same spacing carry the entry well past its original 1s expiry, which is
	// what proves the slide rather than a long initial TTL.
	for range 4 {
		time.Sleep(600 * time.Millisecond)
		r, _, err := c.Open(ctx, key)
		assert.NoError(t, err, "entry expired despite being read continuously")
		assert.NoError(t, r.Close())
	}

	// Left alone for longer than the TTL, it must still expire.
	time.Sleep(1500 * time.Millisecond)
	_, _, err = c.Open(ctx, key)
	assert.Error(t, err, "entry outlived its TTL after reads stopped")
}

// Expiry slides by the entry's own TTL, never by max-ttl. A caller that asks
// for a short-lived object must keep getting one, even when max-ttl is larger.
func TestDiskReadDoesNotPromoteShortTTLToMaxTTL(t *testing.T) {
	dir := t.TempDir()
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          dir,
		LimitMB:       10,
		MaxTTL:        time.Hour,
		EvictInterval: time.Hour,
	})
	assert.NoError(t, err)
	defer c.Close()

	key := cache.NewKey("short-lived")
	w, err := c.Create(ctx, key, nil, time.Second)
	assert.NoError(t, err)
	_, err = w.Write([]byte("payload"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// One read past halfway slides expiry by 1s, not by the 1h max-ttl.
	time.Sleep(600 * time.Millisecond)
	r, _, err := c.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	time.Sleep(1500 * time.Millisecond)
	_, _, err = c.Open(ctx, key)
	assert.Error(t, err, "short TTL was promoted to max-ttl by a read")
}

// Stat is a read, so it must keep an entry alive for LRU as Open does.
// Otherwise a client that only stats an object lets it be evicted while still
// using it.
func TestDiskEvictionKeepsRecentlyStatted(t *testing.T) {
	for _, read := range []struct {
		name string
		fn   func(t *testing.T, c cache.Cache, key cache.Key)
	}{
		{"Open", func(t *testing.T, c cache.Cache, key cache.Key) {
			t.Helper()
			r, _, err := c.Open(t.Context(), key)
			assert.NoError(t, err)
			assert.NoError(t, r.Close())
		}},
		{"Stat", func(t *testing.T, c cache.Cache, key cache.Key) {
			t.Helper()
			_, err := c.Stat(t.Context(), key)
			assert.NoError(t, err)
		}},
	} {
		t.Run(read.name, func(t *testing.T) {
			dir := t.TempDir()
			_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

			c, err := cache.NewDisk(ctx, cache.DiskConfig{
				Root:          dir,
				LimitMB:       1,
				MaxTTL:        time.Hour,
				EvictInterval: 50 * time.Millisecond,
			})
			assert.NoError(t, err)
			defer c.Close()

			touched := cache.NewKey("touched")
			untouched := cache.NewKey("untouched")
			overflow := cache.NewKey("overflow")

			// Spacing exceeds evict-interval, which is also the window over
			// which a read's LRU bump is throttled.
			writeDiskEntry(ctx, t, c, touched)
			time.Sleep(60 * time.Millisecond)
			writeDiskEntry(ctx, t, c, untouched)
			time.Sleep(60 * time.Millisecond)

			read.fn(t, c, touched)
			time.Sleep(10 * time.Millisecond)

			writeDiskEntry(ctx, t, c, overflow)
			time.Sleep(200 * time.Millisecond)

			_, _, err = c.Open(ctx, untouched)
			assert.Error(t, err, "least recently used entry survived eviction")

			r, _, err := c.Open(ctx, touched)
			assert.NoError(t, err, "recently read entry was evicted")
			assert.NoError(t, r.Close())
		})
	}
}

// Lowering the limit at runtime is how a shared storage budget gives space to
// another component, so it must reclaim now, not at the next tick.
func TestDiskSetSizeLimitEvictsImmediately(t *testing.T) {
	dir := t.TempDir()
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:    dir,
		LimitMB: 10,
		MaxTTL:  time.Hour,
		// Long enough that a pass cannot be the ticker's doing.
		EvictInterval: time.Hour,
	})
	assert.NoError(t, err)
	defer c.Close()

	oldest := cache.NewKey("oldest")
	newest := cache.NewKey("newest")
	writeDiskEntry(ctx, t, c, oldest)
	time.Sleep(10 * time.Millisecond)
	writeDiskEntry(ctx, t, c, newest)

	assert.Equal(t, int64(10*1024*1024), c.SizeLimit())

	// Two 500KB entries against a 512KB ceiling: the older one has to go.
	c.SetSizeLimit(ctx, 512*1024)
	assert.Equal(t, int64(512*1024), c.SizeLimit())

	// Polled through the metadata count, not by opening an entry, because a read
	// would re-rank the LRU order the assertions below depend on.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := c.Stats(ctx)
		assert.NoError(t, err)
		if stats.Objects < 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	_, _, err = c.Open(ctx, oldest)
	assert.Error(t, err, "lowering the size limit did not reclaim the oldest entry")

	r, _, err := c.Open(ctx, newest)
	assert.NoError(t, err, "lowering the size limit reclaimed more than it needed to")
	assert.NoError(t, r.Close())
}

// Sweeps also run on SetSizeLimit and on every writer close, not just on
// evict-interval, so a read whose LRU bump was throttled must still rank as
// recent. Otherwise the most recently used entry is the one evicted.
func TestDiskEvictionKeepsReadThrottledInsideInterval(t *testing.T) {
	dir := t.TempDir()
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:    dir,
		LimitMB: 10,
		MaxTTL:  time.Hour,
		// Long enough that every read below is inside the throttle window and no
		// sweep can be the ticker's doing.
		EvictInterval: time.Hour,
	})
	assert.NoError(t, err)
	defer c.Close()

	read := cache.NewKey("read")
	unread := cache.NewKey("unread")
	writeDiskEntry(ctx, t, c, read)
	time.Sleep(10 * time.Millisecond)
	writeDiskEntry(ctx, t, c, unread)

	r, _, err := c.Open(ctx, read)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	// Two 500KB entries against a 512KB ceiling: one must go, and it must be the
	// untouched one.
	c.SetSizeLimit(ctx, 512*1024)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := c.Stats(ctx)
		assert.NoError(t, err)
		if stats.Objects < 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	_, _, err = c.Open(ctx, unread)
	assert.Error(t, err, "the untouched entry should have been the one evicted")

	r, _, err = c.Open(ctx, read)
	assert.NoError(t, err, "a read inside the throttle window was ranked by its stale persisted stamp")
	assert.NoError(t, r.Close())
}

// The configured limit-mb caps what a shared budget can hand out, so a caller
// cannot grow the cache past what the operator allowed.
func TestDiskSetSizeLimitCappedByConfiguredLimit(t *testing.T) {
	dir := t.TempDir()
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          dir,
		LimitMB:       1,
		MaxTTL:        time.Hour,
		EvictInterval: time.Hour,
	})
	assert.NoError(t, err)
	defer c.Close()

	configured := int64(1024 * 1024)
	assert.Equal(t, configured, c.SizeLimit())

	c.SetSizeLimit(ctx, 100*1024*1024)
	assert.Equal(t, configured, c.SizeLimit())

	// A non-positive limit would evict everything and miss every request, so it
	// is ignored.
	c.SetSizeLimit(ctx, 0)
	assert.Equal(t, configured, c.SizeLimit())
	c.SetSizeLimit(ctx, -1)
	assert.Equal(t, configured, c.SizeLimit())
}
