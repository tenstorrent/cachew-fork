package cache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io/fs"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"go.etcd.io/bbolt"
)

//nolint:gochecknoglobals
var (
	ttlBucketName          = []byte("ttl")
	headersBucketName      = []byte("headers")
	lastAccessedBucketName = []byte("lastaccessed")
	baseTTLBucketName      = []byte("basettl")
)

// diskEntryMeta is what the read paths need about an entry. Stat and Open both
// need all of it, so it is fetched in one transaction.
type diskEntryMeta struct {
	expiresAt time.Time
	headers   http.Header
	// baseTTL is the TTL the entry was written with, and how far a read slides
	// expiry forward. Zero means the entry predates base-TTL tracking, so its
	// expiry is left alone.
	baseTTL time.Duration
	// lastAccessedAt is what eviction ranks entries by. Reads compare it to now
	// to decide whether re-recording it is worth a write. Zero means no record,
	// which counts as stale and always writes.
	lastAccessedAt time.Time
}

// diskMetaDB manages expiration times and headers for cache entries using bbolt.
type diskMetaDB struct {
	db              *bbolt.DB
	namespacesCache sync.Map // map[Namespace]bool - concurrent-safe
}

// compositeKey creates a unique database key from namespace and cache key.
// Format: "namespace/hexkey" when namespace is set, or just "hexkey" when empty.
func compositeKey(namespace Namespace, key Key) []byte {
	if namespace == "" {
		return []byte(key.String())
	}
	return []byte(string(namespace) + "/" + key.String())
}

// newDiskMetaDB creates a new bbolt-backed metadata storage for the disk cache.
func newDiskMetaDB(dbPath string) (*diskMetaDB, error) {
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, errors.Errorf("failed to open bbolt database: %w", err)
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(ttlBucketName); err != nil {
			return errors.WithStack(err)
		}
		if _, err := tx.CreateBucketIfNotExists(headersBucketName); err != nil {
			return errors.WithStack(err)
		}
		if _, err := tx.CreateBucketIfNotExists(lastAccessedBucketName); err != nil {
			return errors.WithStack(err)
		}
		if _, err := tx.CreateBucketIfNotExists(baseTTLBucketName); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		return nil, errors.Join(errors.Errorf("failed to create buckets: %w", err), db.Close())
	}

	// Initialize in-memory namespace cache by scanning existing entries
	metaDB := &diskMetaDB{db: db}
	err = db.View(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		if ttlBucket == nil {
			return nil
		}
		return ttlBucket.ForEach(func(k, _ []byte) error {
			namespace, _, found := bytes.Cut(k, []byte("/"))
			if found && len(namespace) > 0 {
				metaDB.namespacesCache.Store(Namespace(namespace), true)
			}
			return nil
		})
	})
	if err != nil {
		return nil, errors.Join(errors.Errorf("failed to initialize namespace cache: %w", err), db.Close())
	}

	return metaDB, nil
}

// recordAccess renews an entry's TTL and bumps its last access time, in a single write transaction to keep cache hits
// cheap.
//
// Both fields move forward only. Read paths hold a shared lock, so two readers
// of one key can race to renew it. Without this guard the loser would overwrite
// a later renewal with an older expiry and access time.
func (s *diskMetaDB) recordAccess(namespace Namespace, key Key, expiresAt, accessedAt time.Time) error {
	ttlBytes, err := expiresAt.MarshalBinary()
	if err != nil {
		return errors.Errorf("failed to marshal TTL: %w", err)
	}

	accessedBytes, err := accessedAt.MarshalBinary()
	if err != nil {
		return errors.Errorf("failed to marshal last accessed time: %w", err)
	}

	dbKey := compositeKey(namespace, key)
	err = s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		if storedTimeIsBefore(ttlBucket.Get(dbKey), expiresAt) {
			if err := ttlBucket.Put(dbKey, ttlBytes); err != nil {
				return errors.WithStack(err)
			}
		}

		lastAccessedBucket := tx.Bucket(lastAccessedBucketName)
		if !storedTimeIsBefore(lastAccessedBucket.Get(dbKey), accessedAt) {
			return nil
		}
		return errors.WithStack(lastAccessedBucket.Put(dbKey, accessedBytes))
	})
	if err != nil {
		return errors.WithStack(err)
	}

	// Add namespace to in-memory cache
	if namespace != "" {
		s.namespacesCache.Store(namespace, true)
	}

	return nil
}

// storedTimeIsBefore reports whether the time recorded in raw is older than t,
// and so whether writing t moves the value forward.
//
// Absent or unparseable bytes count as older: there is nothing worth keeping.
func storedTimeIsBefore(raw []byte, t time.Time) bool {
	if len(raw) == 0 {
		return true
	}
	var stored time.Time
	if err := stored.UnmarshalBinary(raw); err != nil {
		return true
	}
	return stored.Before(t)
}

func (s *diskMetaDB) set(
	key Key, namespace Namespace, expiresAt, accessedAt time.Time, baseTTL time.Duration, headers http.Header,
) error {
	ttlBytes, err := expiresAt.MarshalBinary()
	if err != nil {
		return errors.Errorf("failed to marshal TTL: %w", err)
	}

	accessedBytes, err := accessedAt.MarshalBinary()
	if err != nil {
		return errors.Errorf("failed to marshal last accessed time: %w", err)
	}

	headersBytes, err := json.Marshal(headers)
	if err != nil {
		return errors.Errorf("failed to encode headers: %w", err)
	}

	// Varint because a Duration is signed and so round trips without going
	// through uint64. The value is only fetched by exact key, never compared or
	// scanned as bytes, so byte order does not matter.
	baseTTLBytes := binary.AppendVarint(nil, int64(baseTTL))

	dbKey := compositeKey(namespace, key)
	err = s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		if err := ttlBucket.Put(dbKey, ttlBytes); err != nil {
			return errors.WithStack(err)
		}

		lastAccessedBucket := tx.Bucket(lastAccessedBucketName)
		if err := lastAccessedBucket.Put(dbKey, accessedBytes); err != nil {
			return errors.WithStack(err)
		}

		if baseTTLBucket := tx.Bucket(baseTTLBucketName); baseTTLBucket != nil {
			if err := baseTTLBucket.Put(dbKey, baseTTLBytes); err != nil {
				return errors.WithStack(err)
			}
		}

		headersBucket := tx.Bucket(headersBucketName)
		return errors.WithStack(headersBucket.Put(dbKey, headersBytes))
	})
	if err != nil {
		return errors.WithStack(err)
	}

	// Add namespace to in-memory cache
	if namespace != "" {
		s.namespacesCache.Store(namespace, true)
	}

	return nil
}

func (s *diskMetaDB) getTTL(namespace Namespace, key Key) (time.Time, error) {
	var expiresAt time.Time
	dbKey := compositeKey(namespace, key)
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ttlBucketName)
		ttlBytes := bucket.Get(dbKey)
		if ttlBytes == nil {
			return fs.ErrNotExist
		}
		return errors.WithStack(expiresAt.UnmarshalBinary(ttlBytes))
	})
	return expiresAt, errors.WithStack(err)
}

// getEntry reads an entry's expiry, base TTL, headers and access time in one
// transaction, so a cache hit costs one View instead of several.
func (s *diskMetaDB) getEntry(namespace Namespace, key Key) (diskEntryMeta, error) {
	var meta diskEntryMeta
	dbKey := compositeKey(namespace, key)
	err := s.db.View(func(tx *bbolt.Tx) error {
		ttlBytes := tx.Bucket(ttlBucketName).Get(dbKey)
		if ttlBytes == nil {
			return fs.ErrNotExist
		}
		if err := meta.expiresAt.UnmarshalBinary(ttlBytes); err != nil {
			return errors.WithStack(err)
		}

		headersBytes := tx.Bucket(headersBucketName).Get(dbKey)
		if headersBytes == nil {
			return fs.ErrNotExist
		}
		if err := json.Unmarshal(headersBytes, &meta.headers); err != nil {
			return errors.WithStack(err)
		}

		// Absent for entries written before base-TTL tracking, and zero if the
		// record does not decode. Either way the caller leaves expiry alone.
		if bucket := tx.Bucket(baseTTLBucketName); bucket != nil {
			if stored, n := binary.Varint(bucket.Get(dbKey)); n > 0 {
				meta.baseTTL = time.Duration(stored)
			}
		}

		// Zero when absent or undecodable, which read paths treat as stale.
		if accessedBytes := tx.Bucket(lastAccessedBucketName).Get(dbKey); accessedBytes != nil {
			if err := meta.lastAccessedAt.UnmarshalBinary(accessedBytes); err != nil {
				meta.lastAccessedAt = time.Time{}
			}
		}
		return nil
	})
	return meta, errors.WithStack(err)
}

func (s *diskMetaDB) delete(namespace Namespace, key Key) error {
	dbKey := compositeKey(namespace, key)
	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		if err := ttlBucket.Delete(dbKey); err != nil {
			return errors.WithStack(err)
		}

		lastAccessedBucket := tx.Bucket(lastAccessedBucketName)
		if err := lastAccessedBucket.Delete(dbKey); err != nil {
			return errors.WithStack(err)
		}

		if baseTTLBucket := tx.Bucket(baseTTLBucketName); baseTTLBucket != nil {
			if err := baseTTLBucket.Delete(dbKey); err != nil {
				return errors.WithStack(err)
			}
		}

		headersBucket := tx.Bucket(headersBucketName)
		return errors.WithStack(headersBucket.Delete(dbKey))
	}))
}

func (s *diskMetaDB) deleteAll(entries []evictEntryKey) error {
	if len(entries) == 0 {
		return nil
	}
	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		lastAccessedBucket := tx.Bucket(lastAccessedBucketName)
		headersBucket := tx.Bucket(headersBucketName)
		baseTTLBucket := tx.Bucket(baseTTLBucketName)

		for _, entry := range entries {
			dbKey := compositeKey(entry.namespace, entry.key)
			if err := ttlBucket.Delete(dbKey); err != nil {
				return errors.Errorf("failed to delete TTL: %w", err)
			}
			if err := lastAccessedBucket.Delete(dbKey); err != nil {
				return errors.Errorf("failed to delete last accessed time: %w", err)
			}
			if err := headersBucket.Delete(dbKey); err != nil {
				return errors.Errorf("failed to delete headers: %w", err)
			}
			if baseTTLBucket != nil {
				if err := baseTTLBucket.Delete(dbKey); err != nil {
					return errors.Errorf("failed to delete base TTL: %w", err)
				}
			}
		}
		return nil
	}))
}

// walk calls fn for every entry in the database.
//
// Entries written before last access tracking existed have no recorded access time, and are reported as the zero time
// so that they sort ahead of everything with a real one. That makes them the first candidates for eviction, which at
// worst re-warms the cache.
func (s *diskMetaDB) walk(fn func(key Key, namespace Namespace, expiresAt, lastAccessedAt time.Time) error) error {
	return errors.WithStack(s.db.View(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		if ttlBucket == nil {
			return nil
		}
		lastAccessedBucket := tx.Bucket(lastAccessedBucketName)
		return ttlBucket.ForEach(func(k, v []byte) error {
			var namespace Namespace
			var key Key

			before, hexKey, found := bytes.Cut(k, []byte("/"))
			if found {
				namespace = Namespace(before)
			} else {
				hexKey = k
			}
			if !isHexKey(string(hexKey)) {
				return nil
			}
			if err := key.UnmarshalText(hexKey); err != nil {
				return nil //nolint:nilerr
			}

			var expiresAt time.Time
			if err := expiresAt.UnmarshalBinary(v); err != nil {
				return nil //nolint:nilerr
			}

			var lastAccessedAt time.Time
			if lastAccessedBucket != nil {
				var parsed time.Time
				if accessedBytes := lastAccessedBucket.Get(k); accessedBytes != nil && parsed.UnmarshalBinary(accessedBytes) == nil {
					lastAccessedAt = parsed
				}
			}

			return fn(key, namespace, expiresAt, lastAccessedAt)
		})
	}))
}

func (s *diskMetaDB) count() (int64, error) {
	var count int64
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ttlBucketName)
		if bucket == nil {
			return nil
		}
		count = int64(bucket.Stats().KeyN)
		return nil
	})
	return count, errors.WithStack(err)
}

func (s *diskMetaDB) close() error {
	if err := s.db.Close(); err != nil {
		return errors.Errorf("failed to close bbolt database: %w", err)
	}
	return nil
}

func (s *diskMetaDB) listNamespaces() ([]string, error) {
	var namespaces []string
	s.namespacesCache.Range(func(key, _ any) bool {
		if ns, ok := key.(Namespace); ok {
			namespaces = append(namespaces, string(ns))
		}
		return true
	})
	sort.Strings(namespaces)
	return namespaces, nil
}
