package cache

import (
	"net/http"
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

func TestDiskOverwriteDoesNotExtendExpiry(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

	d, err := NewDisk(ctx, DiskConfig{
		Root:          t.TempDir(),
		LimitMB:       10,
		MaxTTL:        time.Hour,
		EvictInterval: time.Minute,
	})
	assert.NoError(t, err)
	defer d.Close()

	key := NewKey("regenerated")
	write := func(payload string) diskEntryMeta {
		t.Helper()
		w, err := d.Create(ctx, key, nil, time.Hour)
		assert.NoError(t, err)
		_, err = w.Write([]byte(payload))
		assert.NoError(t, err)
		assert.NoError(t, w.Close())
		meta, err := d.db.getEntry(d.namespace, key)
		assert.NoError(t, err)
		return meta
	}

	first := write("v1")
	// A later write with the same TTL computes a later expiry, which must be
	// discarded in favour of the one already recorded.
	time.Sleep(10 * time.Millisecond)
	second := write("v2")
	assert.Equal(t, first.expiresAt, second.expiresAt, "an overwrite extended the entry's TTL")

	// The base TTL still tracks what the entry was written with, so a read can
	// still slide expiry forward by it.
	assert.Equal(t, time.Hour, second.baseTTL)

	// The access stamp is not an expiry and must still move, or eviction ranks a
	// freshly written entry as the coldest thing in the cache.
	assert.True(t, second.lastAccessedAt.After(first.lastAccessedAt))
}

func TestDiskMetaDBSetKeepsTheEarlierExpiry(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

	d, err := NewDisk(ctx, DiskConfig{
		Root:          t.TempDir(),
		LimitMB:       10,
		MaxTTL:        2 * time.Hour,
		EvictInterval: time.Minute,
	})
	assert.NoError(t, err)
	defer d.Close()

	key := NewKey("explicit")
	now := time.Now()
	early := now.Add(30 * time.Minute)

	assert.NoError(t, d.db.set(key, d.namespace, early, now, 30*time.Minute, http.Header{"A": {"1"}}))

	late := now.Add(90 * time.Minute)
	later := now.Add(time.Minute)
	assert.NoError(t, d.db.set(key, d.namespace, late, later, 90*time.Minute, http.Header{"A": {"2"}}))

	meta, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)
	assert.Equal(t, early.Unix(), meta.expiresAt.Unix(), "set accepted an expiry later than the one recorded")
	assert.Equal(t, later.Unix(), meta.lastAccessedAt.Unix(), "the access stamp should follow the write")
	assert.Equal(t, 90*time.Minute, meta.baseTTL, "the base TTL should follow the write")
	assert.Equal(t, "2", meta.headers.Get("A"), "headers should follow the write")

	// Symmetrically, an expiry EARLIER than the recorded one is accepted: the
	// rule is that a write never lengthens an entry's life, not that expiry is
	// frozen.
	earliest := now.Add(5 * time.Minute)
	assert.NoError(t, d.db.set(key, d.namespace, earliest, later, 5*time.Minute, nil))
	shortened, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)
	assert.Equal(t, earliest.Unix(), shortened.expiresAt.Unix(), "a write should be able to shorten expiry")
}

func TestDiskOverwriteOfExpiredEntryGetsFreshTTL(t *testing.T) {
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "debug"})

	d, err := NewDisk(ctx, DiskConfig{
		Root:          t.TempDir(),
		LimitMB:       10,
		MaxTTL:        time.Hour,
		EvictInterval: time.Minute,
	})
	assert.NoError(t, err)
	defer d.Close()

	key := NewKey("lapsed")
	w, err := d.Create(ctx, key, nil, time.Millisecond)
	assert.NoError(t, err)
	_, err = w.Write([]byte("v1"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	expired, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)
	time.Sleep(5 * time.Millisecond)
	assert.True(t, time.Now().After(expired.expiresAt), "entry should have lapsed")

	w, err = d.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("v2"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	fresh, err := d.db.getEntry(d.namespace, key)
	assert.NoError(t, err)
	assert.True(t, fresh.expiresAt.After(time.Now().Add(30*time.Minute)),
		"rewriting a lapsed entry must not inherit its dead expiry")
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
