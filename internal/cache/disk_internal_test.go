package cache

import (
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/logging"
)

// Reads bump the LRU stamp, but not on every hit. A hot key would otherwise
// open a bbolt write transaction per read, the cost the halfway expiry guard
// exists to avoid.
func TestDiskRecordReadThrottlesAccessWrites(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

	d, err := NewDisk(ctx, DiskConfig{
		Root:          t.TempDir(),
		LimitMB:       10,
		MaxTTL:        time.Hour,
		EvictInterval: time.Minute,
	})
	assert.NoError(t, err)
	defer d.Close()

	key := NewKey("hot")
	w, err := d.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("payload"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	written, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)

	// Well inside the window and nowhere near halfway through the TTL, so there
	// is nothing worth writing.
	assert.NoError(t, d.recordRead(ctx, key, written, written.lastAccessedAt.Add(time.Second)))
	unchanged, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)
	assert.Equal(t, written.lastAccessedAt, unchanged.lastAccessedAt, "a read inside the window rewrote the stamp")

	// Past the window the stamp must move, or eviction ranks a key that is still
	// in demand as idle.
	stale := written.lastAccessedAt.Add(2 * time.Minute)
	assert.NoError(t, d.recordRead(ctx, key, written, stale))
	bumped, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)
	assert.True(t, bumped.lastAccessedAt.After(written.lastAccessedAt), "a read past the window did not bump the stamp")
}

// A throttled read skips the write but must not be lost. Eviction can run at
// any moment, so the access must survive until it is persisted.
func TestDiskThrottledReadStillRanksAsRecent(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

	d, err := NewDisk(ctx, DiskConfig{
		Root:          t.TempDir(),
		LimitMB:       10,
		MaxTTL:        time.Hour,
		EvictInterval: time.Minute,
	})
	assert.NoError(t, err)
	defer d.Close()

	key := NewKey("hot")
	w, err := d.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("payload"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	written, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)
	assert.Equal(t, written.lastAccessedAt, d.lastAccess(d.namespace, key, written.lastAccessedAt))

	throttled := written.lastAccessedAt.Add(time.Second)
	assert.NoError(t, d.recordRead(ctx, key, written, throttled))
	assert.Equal(t, throttled, d.lastAccess(d.namespace, key, written.lastAccessedAt))

	// Once the stamp is persisted the in-memory entry adds nothing, and keeping
	// it would grow the map for the life of the process.
	assert.NoError(t, d.recordRead(ctx, key, written, written.lastAccessedAt.Add(2*time.Minute)))
	persisted, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)
	_, held := d.recentAccess.Load(evictEntryKey{d.namespace, key})
	assert.False(t, held, "the overlay entry outlived the write that made it redundant")
	assert.Equal(t, persisted.lastAccessedAt, d.lastAccess(d.namespace, key, persisted.lastAccessedAt))

	// A throttled read later than this one is not covered by the stamp just
	// written, so it must survive the persist.
	newer := persisted.lastAccessedAt.Add(time.Hour)
	d.recentAccess.Store(evictEntryKey{d.namespace, key}, newer)
	assert.NoError(t, d.recordRead(ctx, key, persisted, persisted.lastAccessedAt.Add(2*time.Minute)))
	assert.Equal(t, newer, d.lastAccess(d.namespace, key, persisted.lastAccessedAt))

	// Deleting the key must drop it too, for the same reason.
	assert.NoError(t, d.recordRead(ctx, key, persisted, persisted.lastAccessedAt.Add(time.Second)))
	_, held = d.recentAccess.Load(evictEntryKey{d.namespace, key})
	assert.True(t, held)
	assert.NoError(t, d.Delete(ctx, key))
	_, held = d.recentAccess.Load(evictEntryKey{d.namespace, key})
	assert.False(t, held, "a deleted key left its access time behind")
}

// Reads hold only a shared lock, so two readers of one key race to renew it.
// Whoever writes second must not put back an earlier expiry or access time.
func TestDiskRecordAccessOnlyMovesForward(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

	d, err := NewDisk(ctx, DiskConfig{
		Root:          t.TempDir(),
		LimitMB:       10,
		MaxTTL:        time.Hour,
		EvictInterval: time.Minute,
	})
	assert.NoError(t, err)
	defer d.Close()

	key := NewKey("contended")
	w, err := d.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("payload"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	written, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)

	later := written.lastAccessedAt.Add(10 * time.Minute)
	assert.NoError(t, d.db.recordAccess(d.namespace, key, written.expiresAt.Add(time.Hour), later))
	renewed, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)

	// The losing reader: it loaded the same metadata and computed its renewal
	// from the older stamps.
	assert.NoError(t, d.db.recordAccess(d.namespace, key, written.expiresAt, written.lastAccessedAt))
	after, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)
	assert.Equal(t, renewed, after, "a stale renewal regressed the entry")
}
