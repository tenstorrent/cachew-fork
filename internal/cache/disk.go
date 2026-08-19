package cache

import (
	"context"
	"hash/fnv"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel/metric"

	"github.com/tenstorrent/internal-cachew/internal/httputil"
	"github.com/tenstorrent/internal-cachew/internal/logging"
)

// diskBackendName is the config keyword for this backend and its metrics label.
const diskBackendName = "disk"

// RegisterDisk cache with the given registry.
func RegisterDisk(r *Registry) {
	Register(
		r,
		diskBackendName,
		"Caches objects on local disk, with a maximum size limit and LRU eviction",
		NewDisk,
	)
}

type DiskConfig struct {
	Root          string        `hcl:"root,optional" help:"Root directory for the disk storage." default:"${CACHEW_STATE}/cache"`
	LimitMB       int           `hcl:"limit-mb,optional" help:"Maximum size of the disk cache in megabytes (defaults to 10GB)." default:"10240"`
	MaxTTL        time.Duration `hcl:"max-ttl,optional" help:"Maximum time-to-live for entries in the disk cache (defaults to 1 hour)." default:"1h"`
	EvictInterval time.Duration `hcl:"evict-interval,optional" help:"Interval at which to check files for eviction (defaults to 1 minute)." default:"1m"`
}

type Disk struct {
	logger    *slog.Logger
	config    DiskConfig
	namespace Namespace
	db        *diskMetaDB
	size      *atomic.Int64
	// limit is the size ceiling eviction enforces, in bytes. Starts at
	// config.LimitMB. A pointer because Namespace views copy the struct but
	// must share one mutable limit.
	limit   *atomic.Int64
	metrics *cacheMetrics
	// sizeLimitMetric is the callback publishing limit. Held so [Disk.Close]
	// can unregister it: an observable callback outlives its registrar, so a
	// closed cache would keep reporting a limit nothing is enforcing, and a
	// cache reopened over the same root would publish a second value for the
	// same series.
	sizeLimitMetric metric.Registration
	// runEviction is buffered so a request made during a sweep is queued
	// instead of dropped; that sweep may already have read the old limit.
	runEviction  chan struct{}
	stop         context.CancelFunc
	evictionDone chan struct{}
	locks        *[diskLockStripes]sync.RWMutex
	// recentAccess holds access times that recordRead throttled instead of
	// persisting, so eviction still ranks by true access order. Sweeps run on
	// every writer close and size-limit change, not just on evict-interval, so
	// a key read inside the throttle window could otherwise be evicted while it
	// is the most recently used entry.
	//
	// Entries are dropped when the stamp is persisted, when the key is deleted,
	// and when a sweep expires or evicts it, so the map stays bounded by the
	// live keyspace. A pointer because Namespace views must share one map.
	recentAccess *sync.Map // map[evictEntryKey]time.Time
}

const diskLockStripes = 1024

var _ Cache = (*Disk)(nil)

// NewDisk creates a new disk-based cache instance.
//
// config.Root MUST be set.
//
// This [Cache] implementation stores cache entries under a directory. If total usage exceeds the limit, entries are
// evicted least recently accessed first. TTLs and last access times are tracked in a bbolt database rather than
// derived from filesystem timestamps, which are unreliable on noatime mounts. If an entry exceeds its TTL or the
// default, it is evicted. The implementation is safe for concurrent use within a single Go process.
func NewDisk(ctx context.Context, config DiskConfig) (*Disk, error) {
	logging.FromContext(ctx).InfoContext(ctx, "Constructing disk cache", "limit-mb", config.LimitMB, "evict-interval", config.EvictInterval, "root", config.Root, "max-ttl", config.MaxTTL)
	// Validate config
	if config.Root == "" {
		return nil, errors.New("root directory is required")
	}
	if config.LimitMB == 0 {
		config.LimitMB = 10240
	}
	if config.MaxTTL == 0 {
		config.MaxTTL = time.Hour
	}
	if config.EvictInterval == 0 {
		config.EvictInterval = time.Minute
	}
	var err error
	config.Root, err = filepath.Abs(config.Root)
	if err != nil {
		return nil, errors.Errorf("failed to get absolute path for cache root: %w", err)
	}

	if err := os.MkdirAll(config.Root, 0750); err != nil {
		return nil, errors.Errorf("failed to create cache root: %w", err)
	}

	// Open TTL storage
	db, err := newDiskMetaDB(filepath.Join(config.Root, metaDBFileName))
	if err != nil {
		return nil, errors.Errorf("failed to create TTL storage: %w", err)
	}

	logger := logging.FromContext(ctx)

	// Determine the initial size, dropping files the metadata database has no
	// record of on the way.
	size, err := reconcileOrphanedFiles(ctx, config.Root, db)
	if err != nil {
		return nil, errors.Join(err, db.close())
	}

	ctx, stop := context.WithCancel(ctx)

	disk := &Disk{
		logger:       logger,
		config:       config,
		db:           db,
		size:         &atomic.Int64{},
		limit:        &atomic.Int64{},
		metrics:      newCacheMetrics(diskBackendName),
		runEviction:  make(chan struct{}, 1),
		stop:         stop,
		evictionDone: make(chan struct{}),
		locks:        &[diskLockStripes]sync.RWMutex{},
		recentAccess: &sync.Map{},
	}
	disk.size.Store(size)
	disk.limit.Store(int64(config.LimitMB) * 1024 * 1024)
	// Registered after the limit is stored so the first collection cannot
	// observe a zero ceiling.
	disk.sizeLimitMetric = disk.metrics.registerSizeLimit(disk.limit.Load)

	go disk.evictionLoop(ctx)

	return disk, nil
}

const (
	// metaDBFileName is the bbolt database inside the cache root. It is not a
	// cache entry, so it is excluded from the size accounting and never
	// reclaimed.
	metaDBFileName = "metadata.db"
	// diskTempPrefix marks an entry still being written, before the rename that
	// publishes it at its key. Only a killed process leaves one behind.
	diskTempPrefix = ".tmp-"
	// hexKeyLength is the length of a [Key] in hex, which is the file name a
	// cache entry is stored under.
	hexKeyLength = 64
)

// reconcileOrphanedFiles deletes files under root that no metadata record
// points at, and returns the total size of the files that remain.
//
// Eviction is driven entirely by walking the metadata database, so a file with
// no record is invisible to it: never expired, never chosen as an LRU victim,
// yet counted in the size the limit is enforced against. It would occupy the
// cache for the process's whole lifetime and be re-counted on every restart.
//
// Two things produce them, both requiring the process to die mid-write, which
// is why this runs at startup and not on a timer. A crash between the rename
// and the metadata write in commitLocked leaves a complete entry with no
// record. A crash before the rename leaves the writer's temp file. Neither can
// happen while cachew is running: commitLocked holds the key's lock across both
// steps and removes the file if the metadata write fails.
//
// It reuses the size walk NewDisk already does, so it adds no second directory
// traversal, but it is not free: it first reads every metadata record through
// db.walk and holds a key set sized to the whole cache while the walk runs.
// Measured at 200k entries, that is ~230ms and ~19 MiB. The scan is also not
// separable from the size returned here, since a file with no record has to
// come off that total before eviction starts enforcing the limit against it.
// Both costs stay below one [Disk.evict] sweep, which walks the same records,
// stats each one and sorts a larger per-entry slice, every evict-interval
// rather than once per process.
//
// Anything that is neither a temp file this cache could have written nor shaped
// like a cache entry is left alone and logged. The cache owns this directory,
// but silently deleting a file this code does not recognise is a worse failure
// than accounting for it.
//
// Logs its cost unconditionally rather than as a metric: this runs inside
// [NewDisk], before metrics.New installs the meter provider, so anything
// recorded here would bind to a stub and never surface.
func reconcileOrphanedFiles(ctx context.Context, root string, db *diskMetaDB) (int64, error) {
	logger := logging.FromContext(ctx)
	start := time.Now()

	recorded := map[evictEntryKey]struct{}{}
	if err := db.walk(func(key Key, namespace Namespace, _, _ time.Time) error {
		recorded[evictEntryKey{namespace, key}] = struct{}{}
		return nil
	}); err != nil {
		return 0, errors.Errorf("failed to walk metadata for orphan reconciliation: %w", err)
	}

	var size, orphanedBytes int64
	var orphanedFiles, tempFiles, unrecognised int
	err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || path == filepath.Join(root, metaDBFileName) {
			return nil
		}

		// The prefix alone is not enough: [Disk.Create] only ever opens a temp
		// file inside the shard directory its entry will be published in, so a
		// ".tmp-" file anywhere else was written by something other than this
		// cache and falls through to the unrecognised path below.
		if strings.HasPrefix(info.Name(), diskTempPrefix) && inEntryShardDir(root, path) {
			if removeErr := os.Remove(path); removeErr != nil {
				return errors.Errorf("failed to remove abandoned temp file %s: %w", path, removeErr)
			}
			tempFiles++
			orphanedBytes += info.Size()
			return nil
		}

		entry, ok := entryKeyForPath(root, path)
		if !ok {
			unrecognised++
			logger.WarnContext(ctx, "Unrecognised file in cache root, leaving it alone",
				"path", path, "size", info.Size())
			size += info.Size()
			return nil
		}
		if _, recorded := recorded[entry]; recorded {
			size += info.Size()
			return nil
		}

		if removeErr := os.Remove(path); removeErr != nil {
			return errors.Errorf("failed to remove orphaned cache file %s: %w", path, removeErr)
		}
		orphanedFiles++
		orphanedBytes += info.Size()
		return nil
	})
	if err != nil {
		return 0, errors.Errorf("failed to walk cache root: %w", err)
	}

	logger.InfoContext(ctx, "Reconciled cache files against the metadata database",
		"recorded_entries", len(recorded), "orphaned_files", orphanedFiles, "abandoned_temp_files", tempFiles,
		"unrecognised_files", unrecognised, "bytes", orphanedBytes, "duration", time.Since(start))
	return size, nil
}

// entryKeyForPath recovers the namespace and key a cache file is stored under,
// and reports whether the path is shaped like a cache entry at all. It is the
// inverse of [Disk.keyToPath]: "<namespace>/<xx>/<hexkey>", or "<xx>/<hexkey>"
// for the empty namespace.
func entryKeyForPath(root, path string) (evictEntryKey, bool) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return evictEntryKey{}, false
	}
	parts := strings.Split(rel, string(filepath.Separator))

	var namespace Namespace
	switch len(parts) {
	case 2: //nolint:mnd // <xx>/<hexkey>
	case 3: //nolint:mnd // <namespace>/<xx>/<hexkey>
		namespace = Namespace(parts[0])
	default:
		return evictEntryKey{}, false
	}

	hexKey := parts[len(parts)-1]
	// The sharding directory must agree with the key it holds, otherwise this
	// is not a file the cache wrote.
	if !isHexKey(hexKey) || parts[len(parts)-2] != hexKey[:2] {
		return evictEntryKey{}, false
	}
	var key Key
	if err := key.UnmarshalText([]byte(hexKey)); err != nil {
		return evictEntryKey{}, false
	}
	return evictEntryKey{namespace, key}, true
}

// inEntryShardDir reports whether path sits directly in a directory that
// [Disk.keyToPath] would publish an entry into: "<xx>/" or "<namespace>/<xx>/"
// under root, where <xx> is the two hex digits a key shards on.
//
// It is the check that keeps temp-file reclamation to paths this cache actually
// writes temp files in, rather than every file whose name happens to start with
// the prefix. It deliberately says nothing about the file's own name, because a
// temp file is named by [os.CreateTemp] and not by its key.
func inEntryShardDir(root, path string) bool {
	rel, err := filepath.Rel(root, filepath.Dir(path))
	if err != nil {
		return false
	}
	parts := strings.Split(rel, string(filepath.Separator))
	switch len(parts) {
	case 1: // <xx>
	case 2: //nolint:mnd // <namespace>/<xx>
	default:
		return false
	}
	return isShardDir(parts[len(parts)-1])
}

// isHexKey reports whether name is the lowercase hex form [Key.String]
// produces. Checked here rather than left to [Key.UnmarshalText], which hashes
// text it cannot decode instead of rejecting it, so an unrecognised file would
// be turned into a plausible key and deleted as an orphan.
func isHexKey(name string) bool {
	if len(name) != hexKeyLength {
		return false
	}
	return isLowerHex(name)
}

// isShardDir reports whether name is a key's two-hex-digit sharding directory.
func isShardDir(name string) bool {
	if len(name) != 2 { //nolint:mnd // a shard is the first two hex digits of a key
		return false
	}
	return isLowerHex(name)
}

func isLowerHex(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (d *Disk) keyLock(namespace Namespace, key Key) *sync.RWMutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(namespace))
	_, _ = h.Write(key[:])
	return &d.locks[h.Sum32()&(diskLockStripes-1)]
}

func (d *Disk) String() string { return "disk:" + d.config.Root }

func (d *Disk) Close() error {
	d.stop()
	<-d.evictionDone
	var err error
	if d.sizeLimitMetric != nil {
		if unregErr := d.sizeLimitMetric.Unregister(); unregErr != nil {
			err = errors.Errorf("failed to unregister size limit metric: %w", unregErr)
		}
	}
	if d.db != nil {
		err = errors.Join(err, d.db.close())
	}
	return err
}

func (d *Disk) Size() int64 {
	return d.size.Load()
}

// SizeLimit returns the size ceiling eviction is currently enforcing, in bytes.
func (d *Disk) SizeLimit() int64 {
	return d.limit.Load()
}

// SetSizeLimit changes the size ceiling eviction enforces and runs a sweep, so
// a reduction takes effect immediately instead of at the next tick.
//
// The configured limit-mb is the maximum: a caller sharing a storage budget can
// give the cache less space than the operator allowed, never more. A
// non-positive limit is ignored, because a zero-sized cache misses everything.
//
// The limit is published by the observable gauge registered in [NewDisk], not
// from here, so the series reports the steady state as well as changes to it.
func (d *Disk) SetSizeLimit(_ context.Context, limitBytes int64) {
	configured := int64(d.config.LimitMB) * 1024 * 1024
	if limitBytes <= 0 {
		return
	}
	limitBytes = min(limitBytes, configured)
	if d.limit.Swap(limitBytes) == limitBytes {
		return
	}
	d.TriggerEviction()
}

// TriggerEviction asks the eviction loop to sweep. It never blocks: if a sweep
// is already queued, a second request would only repeat it.
func (d *Disk) TriggerEviction() {
	select {
	case d.runEviction <- struct{}{}:
	default:
	}
}

func (d *Disk) Stats(_ context.Context) (Stats, error) {
	count, err := d.db.count()
	if err != nil {
		return Stats{}, err
	}
	return Stats{
		Objects:  count,
		Size:     d.size.Load(),
		Capacity: d.limit.Load(),
	}, nil
}

func (d *Disk) Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration, opts ...Option) (Writer, error) {
	if ttl > d.config.MaxTTL || ttl == 0 {
		ttl = d.config.MaxTTL
	}

	now := time.Now()
	// Clone (to avoid concurrent map writes) and drop transport headers.
	clonedHeaders := httputil.FilterHeaders(headers, httputil.TransportHeaders...)
	if clonedHeaders.Get("Last-Modified") == "" {
		clonedHeaders.Set("Last-Modified", now.UTC().Format(http.TimeFormat))
	}
	if err := setCreateETag(clonedHeaders, opts...); err != nil {
		return nil, err
	}

	path := d.keyToPath(d.namespace, key)
	fullPath := filepath.Join(d.config.Root, path)

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, errors.Errorf("failed to create directory %s: %w", dir, err)
	}

	f, err := os.CreateTemp(dir, diskTempPrefix+"*")
	if err != nil {
		return nil, errors.Errorf("failed to create temp file: %w", err)
	}

	expiresAt := now.Add(ttl)

	ctx, cancel := context.WithCancelCause(ctx)

	return &diskWriter{
		disk:      d,
		file:      f,
		key:       key,
		namespace: d.namespace,
		path:      fullPath,
		tempPath:  f.Name(),
		expiresAt: expiresAt,
		baseTTL:   ttl,
		headers:   clonedHeaders,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func (d *Disk) Delete(_ context.Context, key Key) error {
	path := d.keyToPath(d.namespace, key)
	fullPath := filepath.Join(d.config.Root, path)

	// Check if file is expired
	expired := false
	expiresAt, err := d.db.getTTL(d.namespace, key)
	if err == nil && time.Now().After(expiresAt) {
		expired = true
	}

	lock := d.keyLock(d.namespace, key)
	lock.Lock()
	defer lock.Unlock()

	info, err := os.Stat(fullPath)
	if err != nil {
		return errors.Errorf("failed to stat file: %w", err)
	}
	if err := os.Remove(fullPath); err != nil {
		return errors.Errorf("failed to remove file: %w", err)
	}
	if err := d.db.delete(d.namespace, key); err != nil {
		return errors.Errorf("failed to delete TTL metadata: %w", err)
	}
	d.recentAccess.Delete(evictEntryKey{d.namespace, key})

	d.size.Add(-info.Size())

	if expired {
		return errors.Errorf("%s: %w", path, fs.ErrNotExist)
	}
	return nil
}

func (d *Disk) Invalidate(ctx context.Context, key Key) error {
	err := d.Delete(ctx, key)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return errors.WithStack(err)
}

// recordRead bumps an entry's last access time for LRU and slides its expiry,
// so entries that stay in demand stay warm.
//
// Expiry slides by the entry's own TTL, not max-ttl, so a short-lived object
// stays short-lived. Entries written before base-TTL tracking have no recorded
// TTL and expire on their original schedule.
//
// The slide is skipped while more than half the window remains, as in the S3
// backend, otherwise every read of a hot key writes to bbolt to move an expiry
// that is nowhere near firing.
//
// The persisted access stamp is throttled to one write per evict-interval for
// the same reason. Throttled reads go to recentAccess, which eviction reads, so
// the throttle costs only stamp durability across a restart, not LRU accuracy.
func (d *Disk) recordRead(ctx context.Context, key Key, meta diskEntryMeta, now time.Time) error {
	expiresAt := meta.expiresAt
	extended := false
	if meta.baseTTL > 0 && expiresAt.Sub(now) < meta.baseTTL/2 {
		expiresAt = now.Add(min(meta.baseTTL, d.config.MaxTTL))
		extended = true
	}
	if !extended && now.Sub(meta.lastAccessedAt) < d.config.EvictInterval {
		d.recentAccess.Store(evictEntryKey{d.namespace, key}, now)
		return nil
	}
	if err := d.db.recordAccess(d.namespace, key, expiresAt, now); err != nil {
		return errors.Errorf("failed to record access: %w", err)
	}
	// The persisted stamp covers this read, so an overlay value no later than it
	// is redundant. A concurrent throttled read may have stored a later time,
	// which the stamp does not cover, so drop only the value seen here and only
	// if it is still the one stored.
	entry := evictEntryKey{d.namespace, key}
	if prev, ok := d.recentAccess.Load(entry); ok {
		if at, isTime := prev.(time.Time); !isTime || !at.After(now) {
			d.recentAccess.CompareAndDelete(entry, prev)
		}
	}
	if extended {
		d.metrics.recordTTLExtension(ctx, d.namespace)
	}
	return nil
}

// statLocked reads and renews an entry's metadata under the key's read lock, so
// a concurrent commit or delete cannot land between the read and the renewal.
//
// The expired case returns instead of deleting, because Delete takes the same
// lock exclusively and the caller must release this one first.
func (d *Disk) statLocked(
	ctx context.Context, key Key, fullPath string, now time.Time,
) (info os.FileInfo, headers http.Header, expired bool, err error) {
	lock := d.keyLock(d.namespace, key)
	lock.RLock()
	defer lock.RUnlock()

	info, err = os.Stat(fullPath)
	if err != nil {
		return nil, nil, false, errors.Errorf("failed to stat file: %w", err)
	}
	meta, err := d.db.getEntry(d.namespace, key)
	if err != nil {
		return nil, nil, false, errors.Errorf("failed to get metadata: %w", err)
	}
	if now.After(meta.expiresAt) {
		return nil, nil, true, nil
	}

	// A stat is a read, so it renews the entry like an open does. Otherwise a
	// client that only stats an object lets it expire while still using it.
	if err := d.recordRead(ctx, key, meta, now); err != nil {
		return nil, nil, false, err
	}
	return info, meta.headers, false, nil
}

func (d *Disk) Stat(ctx context.Context, key Key, opts ...Option) (http.Header, error) {
	path := d.keyToPath(d.namespace, key)
	fullPath := filepath.Join(d.config.Root, path)

	info, headers, expired, err := d.statLocked(ctx, key, fullPath, time.Now())
	if err != nil {
		return nil, err
	}
	if expired {
		return nil, errors.Join(fs.ErrNotExist, d.Delete(ctx, key))
	}

	headers.Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	if h, err := conditionalShortCircuit(headers, opts); err != nil {
		return h, err
	}
	return headers, nil
}

func (d *Disk) openLocked(
	ctx context.Context, key Key, fullPath string, now time.Time,
) (f *os.File, headers http.Header, expired bool, err error) {
	lock := d.keyLock(d.namespace, key)
	lock.RLock()
	defer lock.RUnlock()

	f, err = os.Open(fullPath)
	if err != nil {
		return nil, nil, false, errors.Errorf("failed to open file: %w", err)
	}
	meta, err := d.db.getEntry(d.namespace, key)
	if err != nil {
		return nil, nil, false, errors.Join(err, f.Close())
	}
	if now.After(meta.expiresAt) {
		return f, nil, true, nil
	}
	if err := d.recordRead(ctx, key, meta, now); err != nil {
		return nil, nil, false, errors.Join(err, f.Close())
	}
	return f, meta.headers, false, nil
}

func (d *Disk) Open(ctx context.Context, key Key, opts ...Option) (io.ReadCloser, http.Header, error) {
	path := d.keyToPath(d.namespace, key)
	fullPath := filepath.Join(d.config.Root, path)

	f, headers, expired, err := d.openLocked(ctx, key, fullPath, time.Now())
	if err != nil {
		return nil, nil, err
	}
	if expired {
		return nil, nil, errors.Join(fs.ErrNotExist, f.Close(), d.Delete(ctx, key))
	}

	finfo, err := f.Stat()
	if err != nil {
		return nil, nil, errors.Join(errors.Errorf("failed to stat file for size: %w", err), f.Close())
	}
	headers.Set("Content-Length", strconv.FormatInt(finfo.Size(), 10))

	if h, condErr := conditionalShortCircuit(headers, opts); condErr != nil {
		return nil, h, errors.Join(condErr, f.Close())
	}

	start, length, partial, rangeErr := rangeShortCircuit(headers, finfo.Size(), opts)
	if rangeErr != nil {
		return nil, headers, errors.Join(rangeErr, f.Close())
	}
	if partial {
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			return nil, headers, errors.Join(errors.Errorf("failed to seek for range: %w", err), f.Close())
		}
		return newLimitedReadCloser(f, length), headers, nil
	}

	return f, headers, nil
}

func (d *Disk) keyToPath(namespace Namespace, key Key) string {
	hexKey := key.String()

	// Use first two hex digits as directory, full hex as filename
	if namespace != "" {
		return filepath.Join(string(namespace), hexKey[:2], hexKey)
	}
	return filepath.Join(hexKey[:2], hexKey)
}

func (d *Disk) evictionLoop(ctx context.Context) {
	defer close(d.evictionDone)

	ticker := time.NewTicker(d.config.EvictInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.sweep(ctx); err != nil {
				d.logger.ErrorContext(ctx, "eviction failed", "error", err)
			}
		case <-d.runEviction:
			if err := d.sweep(ctx); err != nil {
				d.logger.ErrorContext(ctx, "eviction failed", "error", err)
			}
		}
	}
}

// sweep runs one eviction pass and records the outcome, so "ran and reclaimed
// nothing" can be told apart from "never ran".
func (d *Disk) sweep(ctx context.Context) error {
	start := time.Now()
	err := d.evict(ctx)
	status := "success"
	if err != nil {
		status = "error"
	}
	d.metrics.recordSweep(ctx, status, time.Since(start))
	return errors.WithStack(err)
}

type evictFileInfo struct {
	namespace  Namespace
	key        Key
	path       string
	size       int64
	expiresAt  time.Time
	accessedAt time.Time
}

type evictEntryKey struct {
	namespace Namespace
	key       Key
}

func (d *Disk) evict(ctx context.Context) error {
	var remainingFiles []evictFileInfo
	var expiredEntries []evictEntryKey
	now := time.Now()

	err := d.db.walk(func(key Key, namespace Namespace, expiresAt, lastAccessedAt time.Time) error {
		path := d.keyToPath(namespace, key)
		fullPath := filepath.Join(d.config.Root, path)

		info, err := os.Stat(fullPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Metadata outliving its file. Reclaims no bytes, so it is
				// counted separately from a real expiry.
				expiredEntries = append(expiredEntries, evictEntryKey{namespace, key})
				d.metrics.recordEviction(ctx, evictReasonOrphaned, namespace, 0)
			}
			return nil
		}

		if now.After(expiresAt) {
			if err := os.Remove(fullPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return errors.Errorf("failed to delete expired file %s: %w", path, err)
			}
			expiredEntries = append(expiredEntries, evictEntryKey{namespace, key})
			d.size.Add(-info.Size())
			d.metrics.recordEviction(ctx, evictReasonExpired, namespace, info.Size())
		} else {
			remainingFiles = append(remainingFiles, evictFileInfo{
				namespace:  namespace,
				key:        key,
				path:       path,
				size:       info.Size(),
				expiresAt:  expiresAt,
				accessedAt: d.lastAccess(namespace, key, lastAccessedAt),
			})
		}
		return nil
	})
	if err != nil {
		return errors.Errorf("failed to walk TTL entries: %w", err)
	}

	if err := d.deleteExpiredEntries(expiredEntries); err != nil {
		return err
	}

	return d.evictBySize(ctx, remainingFiles)
}

func (d *Disk) deleteExpiredEntries(expiredEntries []evictEntryKey) error {
	if len(expiredEntries) == 0 {
		return nil
	}
	if err := d.db.deleteAll(expiredEntries); err != nil {
		return errors.Errorf("failed to delete expired metadata: %w", err)
	}
	// The entries are gone, so their throttled access times rank nothing.
	// Dropping them keeps recentAccess bounded by the live keyspace.
	for _, entry := range expiredEntries {
		d.recentAccess.Delete(entry)
	}
	return nil
}

// lastAccess is the time eviction ranks an entry by: the later of the persisted
// stamp and any throttled read recordRead kept in memory. Older in-memory values
// are ignored, since a persist and a throttled read can land in either order.
func (d *Disk) lastAccess(namespace Namespace, key Key, persisted time.Time) time.Time {
	recent, ok := d.recentAccess.Load(evictEntryKey{namespace, key})
	if !ok {
		return persisted
	}
	at, ok := recent.(time.Time)
	if !ok || !at.After(persisted) {
		return persisted
	}
	return at
}

func (d *Disk) evictBySize(ctx context.Context, remainingFiles []evictFileInfo) error {
	limitBytes := d.limit.Load()
	if d.size.Load() <= limitBytes {
		return nil
	}

	// Sort by access time (oldest first)
	sort.Slice(remainingFiles, func(i, j int) bool {
		return remainingFiles[i].accessedAt.Before(remainingFiles[j].accessedAt)
	})

	var sizeEvictedEntries []evictEntryKey
	for _, f := range remainingFiles {
		if d.size.Load() <= limitBytes {
			break
		}

		fullPath := filepath.Join(d.config.Root, f.path)
		if err := os.Remove(fullPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return errors.Errorf("failed to delete file during size eviction %s: %w", f.path, err)
		}
		sizeEvictedEntries = append(sizeEvictedEntries, evictEntryKey{f.namespace, f.key})
		d.size.Add(-f.size)
		d.metrics.recordEviction(ctx, evictReasonSize, f.namespace, f.size)
	}

	return d.deleteExpiredEntries(sizeEvictedEntries)
}

type diskWriter struct {
	disk      *Disk
	file      *os.File
	key       Key
	namespace Namespace
	path      string
	tempPath  string
	expiresAt time.Time
	baseTTL   time.Duration
	headers   http.Header
	size      int64
	ctx       context.Context
	cancel    context.CancelCauseFunc
	closed    bool
}

func (w *diskWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, errors.WithStack(err)
}

func (w *diskWriter) Abort(err error) error {
	w.cancel(err)
	return w.Close()
}

func (w *diskWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.file.Close(); err != nil {
		return errors.Errorf("failed to close file: %w", err)
	}

	// Check if context was cancelled
	if err := w.ctx.Err(); err != nil {
		// Clean up temp file and abort
		return errors.Join(errors.Wrap(err, "create operation cancelled"), os.Remove(w.tempPath))
	}

	// Ensure directory exists (eviction may have removed it)
	dir := filepath.Dir(w.path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return errors.Errorf("failed to create directory: %w", err)
	}

	if err := w.commitLocked(); err != nil {
		return err
	}

	w.disk.size.Add(w.size)
	w.disk.TriggerEviction()

	return nil
}

func (w *diskWriter) commitLocked() error {
	lock := w.disk.keyLock(w.namespace, w.key)
	lock.Lock()
	defer lock.Unlock()

	if info, err := os.Stat(w.path); err == nil {
		w.disk.size.Add(-info.Size())
	}
	if err := os.Rename(w.tempPath, w.path); err != nil {
		return errors.Errorf("failed to rename temp file: %w", err)
	}
	if err := w.disk.db.set(w.key, w.namespace, w.expiresAt, time.Now(), w.baseTTL, w.headers); err != nil {
		return errors.Join(errors.Errorf("failed to set metadata: %w", err), os.Remove(w.path))
	}
	return nil
}

// Namespace creates a namespaced view of the disk cache.
func (d *Disk) Namespace(namespace Namespace) Cache {
	// Create a shallow copy with the namespace set
	c := *d
	c.namespace = namespace
	return &c
}

// ListNamespaces returns all unique namespaces in the disk cache.
func (d *Disk) ListNamespaces(_ context.Context) ([]string, error) {
	return d.db.listNamespaces()
}
