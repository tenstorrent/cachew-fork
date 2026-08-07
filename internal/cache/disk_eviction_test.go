package cache_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"
	"go.etcd.io/bbolt"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

func TestDiskEvictionBySize(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	// Create cache with 1MB limit and fast eviction
	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          dir,
		LimitMB:       1,
		MaxTTL:        time.Hour,
		EvictInterval: 50 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer c.Close()

	// Create 3 entries of ~500KB each (total 1.5MB, exceeding 1MB limit)
	data := make([]byte, 500*1024)
	keys := []cache.Key{
		cache.NewKey("key1"),
		cache.NewKey("key2"),
		cache.NewKey("key3"),
	}

	for _, key := range keys {
		w, err := c.Create(ctx, key, nil, time.Hour)
		assert.NoError(t, err)
		_, err = w.Write(data)
		assert.NoError(t, err)
		assert.NoError(t, w.Close())
		time.Sleep(10 * time.Millisecond) // Ensure different access times
	}

	// Wait for eviction to run
	time.Sleep(200 * time.Millisecond)

	// key1 (oldest) should be evicted
	_, _, err = c.Open(ctx, keys[0])
	assert.Error(t, err)

	// key2 and key3 should still exist
	r2, _, err := c.Open(ctx, keys[1])
	assert.NoError(t, err)
	assert.NoError(t, r2.Close())

	r3, _, err := c.Open(ctx, keys[2])
	assert.NoError(t, err)
	assert.NoError(t, r3.Close())
}

func TestDiskEvictionKeepsRecentlyRead(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          dir,
		LimitMB:       1,
		MaxTTL:        time.Hour,
		EvictInterval: 50 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer c.Close()

	read := cache.NewKey("read-key")
	unread := cache.NewKey("unread-key")
	overflow := cache.NewKey("overflow-key")

	// Two ~500KB entries stay under the 1MB limit, so nothing is evicted before the read below.
	writeDiskEntry(ctx, t, c, read)
	time.Sleep(10 * time.Millisecond)
	writeDiskEntry(ctx, t, c, unread)
	time.Sleep(10 * time.Millisecond)

	r, _, err := c.Open(ctx, read)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	time.Sleep(10 * time.Millisecond)

	writeDiskEntry(ctx, t, c, overflow)
	time.Sleep(200 * time.Millisecond)

	_, _, err = c.Open(ctx, unread)
	assert.Error(t, err)

	r, _, err = c.Open(ctx, read)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	r, _, err = c.Open(ctx, overflow)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
}

func TestDiskEvictionMissingLastAccessed(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	config := cache.DiskConfig{
		Root:          dir,
		LimitMB:       1,
		MaxTTL:        time.Hour,
		EvictInterval: 50 * time.Millisecond,
	}
	c, err := cache.NewDisk(ctx, config)
	assert.NoError(t, err)

	tracked := cache.NewKey("tracked-key")
	untracked := cache.NewKey("untracked-key")

	writeDiskEntry(ctx, t, c, tracked)
	time.Sleep(10 * time.Millisecond)
	writeDiskEntry(ctx, t, c, untracked)
	assert.NoError(t, c.Close())

	// Reproduce a database written before last access tracking existed, where the newest entry has no record.
	db, err := bbolt.Open(filepath.Join(dir, "metadata.db"), 0600, &bbolt.Options{Timeout: 5 * time.Second})
	assert.NoError(t, err)
	assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("lastaccessed"))
		if bucket == nil {
			return errors.New("last accessed bucket is missing")
		}
		return errors.WithStack(bucket.Delete([]byte(untracked.String())))
	}))
	assert.NoError(t, db.Close())

	c, err = cache.NewDisk(ctx, config)
	assert.NoError(t, err)
	defer c.Close()

	writeDiskEntry(ctx, t, c, cache.NewKey("overflow-key"))
	time.Sleep(200 * time.Millisecond)

	_, _, err = c.Open(ctx, untracked)
	assert.Error(t, err)

	r, _, err := c.Open(ctx, tracked)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
}

func TestDiskEvictionAcrossNamespaces(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	// Create cache with 1MB limit
	baseCache, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          dir,
		LimitMB:       1,
		MaxTTL:        time.Hour,
		EvictInterval: 50 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer baseCache.Close()

	// Create namespace views
	gitCache := baseCache.Namespace("git")
	gomodCache := baseCache.Namespace("gomod")

	// Create entries in different namespaces
	data := make([]byte, 500*1024)

	// git namespace
	gitKey := cache.NewKey("git-key")
	w, err := gitCache.Create(ctx, gitKey, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
	time.Sleep(10 * time.Millisecond)

	// gomod namespace
	gomodKey := cache.NewKey("gomod-key")
	w, err = gomodCache.Create(ctx, gomodKey, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
	time.Sleep(10 * time.Millisecond)

	// Another git entry to exceed limit
	gitKey2 := cache.NewKey("git-key2")
	w, err = gitCache.Create(ctx, gitKey2, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Wait for eviction
	time.Sleep(200 * time.Millisecond)

	// First git entry (oldest) should be evicted
	_, _, err = gitCache.Open(ctx, gitKey)
	assert.Error(t, err)

	// gomod entry should still exist
	r, _, err := gomodCache.Open(ctx, gomodKey)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	// Newer git entry should still exist
	r, _, err = gitCache.Open(ctx, gitKey2)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
}

func writeDiskEntry(ctx context.Context, t *testing.T, c cache.Cache, key cache.Key) {
	t.Helper()
	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(make([]byte, 500*1024))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
}
