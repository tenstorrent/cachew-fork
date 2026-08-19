package gitclone

import (
	"context"
	"io/fs"
	"maps"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alecthomas/errors"

	"github.com/tenstorrent/internal-cachew/internal/logging"
)

type State int

const (
	StateEmpty   State = iota // Not cloned yet
	StateCloning              // Clone in progress
	StateReady                // Ready to use
)

// cloneTempPrefix is the prefix for the temporary directory created during a
// clone before it is atomically renamed into place. Interrupted clones (e.g. a
// killed process) can leave these behind, so they are skipped and removed by
// DiscoverExisting on startup.
const cloneTempPrefix = ".clone-"

// bytesPerMB matches how the rest of cachew reads a megabyte config value,
// which is mebibytes despite the name.
const bytesPerMB = 1024 * 1024

const (
	// DefaultFetchInterval is the cadence used when Config.FetchInterval is
	// unset, and the base of the background fetch backoff.
	DefaultFetchInterval = 15 * time.Minute

	// MaxFetchBackoff caps the backoff between failed background fetches. An
	// hour is long enough that a permanently broken mirror (deleted upstream,
	// revoked credentials) costs almost nothing, while still recovering
	// reasonably promptly once upstream is healthy again.
	MaxFetchBackoff = time.Hour
)

func (s State) String() string {
	switch s {
	case StateEmpty:
		return "empty"
	case StateCloning:
		return "cloning"
	case StateReady:
		return "ready"
	default:
		return "unknown"
	}
}

type GitTuningConfig struct {
	PostBuffer    int           // http.postBuffer size in bytes
	LowSpeedLimit int           // http.lowSpeedLimit in bytes/sec
	LowSpeedTime  time.Duration // http.lowSpeedTime
}

func DefaultGitTuningConfig() GitTuningConfig {
	return GitTuningConfig{
		PostBuffer:    524288000, // 500MB buffer
		LowSpeedLimit: 1000,      // 1KB/s minimum speed
		LowSpeedTime:  60 * time.Second,
	}
}

type Config struct {
	MirrorRoot       string        `hcl:"mirror-root,optional" help:"Directory to store git clones." default:"${CACHEW_STATE}/git-mirrors"`
	FetchInterval    time.Duration `hcl:"fetch-interval,optional" help:"How often to fetch from upstream in minutes." default:"15m"`
	RefCheckInterval time.Duration `hcl:"ref-check-interval,optional" help:"How long to cache ref checks." default:"10s"`
	Maintenance      bool          `hcl:"maintenance,optional" help:"Enable git maintenance scheduling for mirror repos. Its tasks run outside this process and take the same locks the git commands here do, so enabling it also gives up the cleanup of locks left by a killed command." default:"false"`
	PackThreads      int           `hcl:"pack-threads,optional" help:"Threads for git pack operations (0 = all CPU cores)." default:"0"`
	CloneTimeout     time.Duration `hcl:"clone-timeout,optional" help:"Upper bound for 'git clone --mirror'. Generous by default since large repos can take 30+ minutes." default:"1h"`
	FetchTimeout     time.Duration `hcl:"fetch-timeout,optional" help:"Upper bound for 'git fetch' so a slow upstream cannot block the fetch path indefinitely." default:"5m"`
	LsRemoteTimeout  time.Duration `hcl:"ls-remote-timeout,optional" help:"Upper bound for 'git ls-remote' so a slow upstream cannot block the request path indefinitely." default:"1m"`
	RepackTimeout    time.Duration `hcl:"repack-timeout,optional" help:"Upper bound for 'git repack' so a slow repack on a large repository cannot block the scheduler queue indefinitely." default:"10m"`
	PruneTimeout     time.Duration `hcl:"prune-timeout,optional" help:"Upper bound for the pruning repack, which rewrites every pack and so takes much longer than the geometric repack that repack-timeout bounds." default:"1h"`
	PruneGrace       time.Duration `hcl:"prune-grace,optional" help:"How long an unreachable object is kept before the pruning repack deletes it, measured from when the object was last packed rather than from when it stopped being reachable, since that is the only date git records. The first prune to see it unreachable moves it into a cruft pack and a later prune deletes it once the window has elapsed, which keeps a force-push from dropping an object a client is still mid-transfer on: upload-pack serves the mirror without a lock. An object written before the window and orphaned inside it is reclaimed immediately. 0 deletes every unreachable object on the first prune that sees it." default:"24h"`
	MaxCruftSizeMB   int           `hcl:"max-cruft-size-mb,optional" help:"Largest cruft pack the pruning repack will write, in megabytes. Above this git splits the unreachable objects across several cruft packs, which bounds the size of any one pack and of the memory the write needs. On its own it does not bound the work a prune does: because every prune expires objects, git still reads and rewrites every existing cruft pack on every run, so by itself this is not an I/O bound on a large unreachable set. Setting prune-expire-interval as well makes most prunes skip the expiration, which is what lets git leave the cruft packs that have reached this size alone. Requires git 2.43 or newer, which added --max-cruft-size; older git fails the prune. 0 writes a single cruft pack." default:"0"`
	// Inert without MaxCruftSizeMB: the repack it selects saves work only by
	// leaving cruft packs that have reached a size alone, and that is the
	// setting which supplies the size. See cruftExpireDue.
	PruneExpireInterval time.Duration `hcl:"prune-expire-interval,optional" help:"How long between the prunes that actually delete expired unreachable objects. Takes effect only alongside max-cruft-size-mb: set on its own it changes nothing, because the cheaper repack it selects saves work by leaving cruft packs that have reached max-cruft-size-mb untouched, and there is no such size to leave alone. With both set, a prune that comes round before this interval has elapsed only combines the cruft packs below that size and deletes nothing, and the full rewrite that expires objects is paid for once per interval instead of once per prune-interval. The tradeoff is delete latency: an unreachable object can now outlive prune-grace by up to this interval before it is physically removed. Storage is still bounded meanwhile, since the combining prune keeps consolidating fresh cruft; the objects are just not deleted until the next expiring prune. Requires git 2.50 or newer, which added --combine-cruft-below-size; older git fails the combining prune. 0 expires on every prune." default:"0"`
	RepackThreads       int           `hcl:"repack-threads,optional" help:"Threads for git repack operations. Limits memory since windowMemory and deltaCacheSize are per-thread. 0 = pack-threads." default:"4"`
	MirrorTTL           time.Duration `hcl:"mirror-ttl,optional" help:"Delete a mirrored repository that no client has requested for this long. Its next request re-clones it, so keep this well above mirror-snapshot-interval or eviction turns a warm serve into a cold one. 0 disables idle eviction." default:"0"`
	MirrorEvictScan     time.Duration `hcl:"mirror-evict-scan,optional" help:"How often to scan mirrors for idle eviction. Ignored when mirror-ttl is 0." default:"1h"`
}

// CredentialProvider provides credentials for git operations.
type CredentialProvider interface {
	GetTokenForURL(ctx context.Context, url string) (string, error)
}

type CredentialProviderProvider func() (CredentialProvider, error)

type Repository struct {
	mu                 sync.RWMutex
	config             Config
	state              State
	path               string
	upstreamURL        string
	lastFetch          time.Time
	lastFetchAttempt   time.Time // set when a git fetch starts, including failures; gates NeedsFetch
	lastCruftExpireAt  time.Time // set when a prune expires objects rather than only combining cruft; gates cruftExpireDue
	lastRefCheck       time.Time
	refCheckValid      bool
	refCheckStale      bool
	refCheckErr        error
	refCheckErrAt      time.Time // set when a ref check fails; gates the failure backoff
	refCheckInFlight   chan struct{}
	fetchFailures      int
	fetchSem           chan struct{}
	credentialProvider CredentialProvider
	lsRemoteRuns       atomic.Int64
	fetchActive        atomic.Bool // true while an executeFetch call is running
	// serves counts client requests currently reading this mirror. The
	// upload-pack path deliberately takes no lock — a read lock held for the
	// length of a clone stream would block every fetch — so this counter is
	// the only thing that can tell an evictor the directory is still in use.
	serves int
	// evicting closes the admission gate while the repository is being
	// removed, so a request arriving mid-eviction re-clones rather than
	// reading a directory about to be renamed out from under it.
	evicting   bool
	lastServed time.Time
}

type Manager struct {
	config             Config
	gitTuningConfig    GitTuningConfig
	clones             map[string]*Repository
	clonesMu           sync.RWMutex
	credentialProvider CredentialProvider
	lastServed         *lastServedStore
	// removeAll deletes a staged mirror during eviction. Indirected so tests can
	// reach the unlinked-but-not-reclaimed path, which otherwise needs a
	// filesystem failure.
	removeAll func(string) error
	// leftovers holds staged mirrors whose deletion failed. They still take up
	// space but belong to no repository in All(), so ReclaimLeftovers retries
	// and accounts for them.
	leftovers   map[string]struct{}
	leftoversMu sync.Mutex
}

// ManagerProvider is a function that lazily creates a singleton Manager.
type ManagerProvider func() (*Manager, error)

func NewManagerProvider(ctx context.Context, config Config, credentialProviderProvider CredentialProviderProvider) ManagerProvider {
	return sync.OnceValues(func() (*Manager, error) {
		var credentialProvider CredentialProvider
		if credentialProviderProvider != nil {
			var err error
			credentialProvider, err = credentialProviderProvider()
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		return NewManager(ctx, config, credentialProvider)
	})
}

func NewManager(ctx context.Context, config Config, credentialProvider CredentialProvider) (*Manager, error) {
	if config.MirrorRoot == "" {
		return nil, errors.New("mirror-root is required")
	}

	if config.FetchInterval == 0 {
		config.FetchInterval = DefaultFetchInterval
	}

	if config.RefCheckInterval == 0 {
		config.RefCheckInterval = 10 * time.Second
	}

	if config.PackThreads <= 0 {
		config.PackThreads = runtime.GOMAXPROCS(0)
	}

	if config.CloneTimeout == 0 {
		config.CloneTimeout = 1 * time.Hour
	}

	if config.FetchTimeout == 0 {
		config.FetchTimeout = 5 * time.Minute
	}

	if config.LsRemoteTimeout == 0 {
		config.LsRemoteTimeout = 60 * time.Second
	}

	if config.RepackTimeout == 0 {
		config.RepackTimeout = 10 * time.Minute
	}

	// Also covers negatives: a non-positive timeout cancels the prune context
	// before the first command starts, so every prune would fail rather than
	// run unbounded.
	if config.PruneTimeout <= 0 {
		config.PruneTimeout = 1 * time.Hour
	}

	if config.MirrorEvictScan == 0 {
		config.MirrorEvictScan = time.Hour
	}

	if err := os.MkdirAll(config.MirrorRoot, 0o750); err != nil {
		return nil, errors.Wrap(err, "create root directory")
	}

	if config.Maintenance {
		if err := startMaintenance(ctx); err != nil {
			logging.FromContext(ctx).WarnContext(ctx, "Failed to start git maintenance scheduler", "error", err)
		}
	}

	logging.FromContext(ctx).InfoContext(ctx, "Git clone manager initialised", "mirror_root", config.MirrorRoot,
		"fetch_interval", config.FetchInterval, "ref_check_interval", config.RefCheckInterval)

	return &Manager{
		config:             config,
		gitTuningConfig:    DefaultGitTuningConfig(),
		clones:             make(map[string]*Repository),
		credentialProvider: credentialProvider,
		lastServed:         loadLastServedStore(config.MirrorRoot),
		removeAll:          os.RemoveAll,
		leftovers:          make(map[string]struct{}),
	}, nil
}

func (m *Manager) Config() Config {
	return m.config
}

func (m *Manager) GetOrCreate(ctx context.Context, upstreamURL string) (*Repository, error) {
	m.clonesMu.RLock()
	repo, exists := m.clones[upstreamURL]
	m.clonesMu.RUnlock()

	if exists {
		return repo, nil
	}

	m.clonesMu.Lock()
	defer m.clonesMu.Unlock()

	if repo, exists = m.clones[upstreamURL]; exists {
		return repo, nil
	}

	clonePath, err := m.clonePathForURL(upstreamURL)
	if err != nil {
		return nil, err
	}

	// Checked before the repository is built: whether the mirror is already on
	// disk decides both its initial state and whether its idle clock is
	// persisted.
	_, headErr := os.Stat(filepath.Join(clonePath, "HEAD"))
	onDisk := headErr == nil

	state := StateEmpty
	if onDisk {
		state = StateReady
	}

	// A failed write leaves the mirrors intact; the only consequence is that
	// the next restart re-seeds this one, so serving continues.
	lastServed, err := m.seedLastServed(upstreamURL, onDisk)
	if err != nil {
		logging.FromContext(ctx).WarnContext(ctx,
			"Failed to persist initial last-served time, idle eviction will restart the clock for this mirror",
			"upstream", upstreamURL, "error", err)
	}

	repo = &Repository{
		state:              state,
		config:             m.config,
		path:               clonePath,
		upstreamURL:        upstreamURL,
		fetchSem:           make(chan struct{}, 1),
		credentialProvider: m.credentialProvider,
		lastServed:         lastServed,
	}

	repo.fetchSem <- struct{}{}

	m.clones[upstreamURL] = repo
	return repo, nil
}

func (m *Manager) Get(upstreamURL string) *Repository {
	m.clonesMu.RLock()
	defer m.clonesMu.RUnlock()
	return m.clones[upstreamURL]
}

// All returns the currently known repositories.
func (m *Manager) All() []*Repository {
	m.clonesMu.RLock()
	defer m.clonesMu.RUnlock()
	repos := make([]*Repository, 0, len(m.clones))
	for _, repo := range m.clones {
		repos = append(repos, repo)
	}
	return repos
}

// seedLastServed returns the recorded last-served time for a repository, or
// the current time if there is none. Treating an unrecorded repository as
// freshly served is the safe default: mirrors that predate the record, or
// whose record was lost, then get a full idle period before they can be
// evicted rather than being deleted the first time the evictor runs.
//
// onDisk marks a mirror that already exists, whose invented time is persisted
// so the idle clock runs from first observation and survives a restart.
// Re-seeding it to now on every start would hand it a fresh idle period each
// time, and a pod redeployed more often than mirror-ttl would never evict it.
// Nothing is persisted for a repository with no mirror yet: it is about to be
// served, and MarkServed records that.
//
// The returned time is usable even when persisting it fails, so callers decide
// whether that failure is worth reporting.
func (m *Manager) seedLastServed(upstreamURL string, onDisk bool) (time.Time, error) {
	if recorded := m.lastServed.get(upstreamURL); !recorded.IsZero() {
		return recorded, nil
	}
	now := time.Now()
	if onDisk {
		if err := m.lastServed.seedMissing([]string{upstreamURL}, now); err != nil {
			return now, errors.Wrap(err, "persist initial last-served time")
		}
	}
	return now, nil
}

// MarkServed records that a client has just been served from this repository.
// The persistent record is only rewritten once it falls behind by
// lastServedGranularity, so this is cheap enough for the request path.
func (m *Manager) MarkServed(repo *Repository) error {
	now := time.Now()
	repo.mu.Lock()
	repo.lastServed = now
	repo.mu.Unlock()
	if _, err := m.lastServed.record(repo.upstreamURL, now); err != nil {
		return errors.Wrap(err, "record last served")
	}
	return nil
}

// IdleMirrors returns the ready repositories no client has requested for at
// least ttl. A non-positive ttl disables idle eviction and returns nothing.
func (m *Manager) IdleMirrors(ttl time.Duration) []*Repository {
	if ttl <= 0 {
		return nil
	}
	cutoff := time.Now().Add(-ttl)
	var idle []*Repository
	for _, repo := range m.All() {
		repo.mu.RLock()
		stale := repo.state == StateReady && repo.lastServed.Before(cutoff)
		repo.mu.RUnlock()
		if stale {
			idle = append(idle, repo)
		}
	}
	return idle
}

// BeginServe opens the admission gate for a request that is about to read the
// mirror, reporting false if the repository is being evicted and the caller
// should treat it as absent. Every true result must be paired with EndServe.
func (r *Repository) BeginServe() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.evicting {
		return false
	}
	r.serves++
	return true
}

// EndServe releases a reservation taken by BeginServe.
func (r *Repository) EndServe() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.serves > 0 {
		r.serves--
	}
}

// LastServed returns when a client was last served from this repository.
func (r *Repository) LastServed() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastServed
}

// beginEvict closes the admission gate if the repository is idle and ready.
// Setting the flag before checking serves is what makes the check sound: a
// request that passes BeginServe after this point cannot exist, so a zero
// count here means no reader can appear later.
func (r *Repository) beginEvict(cutoff time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != StateReady || r.evicting || !r.lastServed.Before(cutoff) {
		return false
	}
	r.evicting = true
	if r.serves > 0 {
		r.evicting = false
		return false
	}
	return true
}

func (r *Repository) abortEvict() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evicting = false
}

// Evict removes an idle mirror from disk and forgets it, so that the next
// request for it clones afresh. It declines, returning false, if the
// repository stopped being idle or is busy — a fetch, snapshot or repack in
// flight holds the fetch semaphore, and losing that race simply defers
// eviction to the next scan.
//
// The caller must first stop the repository's periodic maintenance jobs;
// otherwise they keep firing against a directory that no longer exists.
//
// A true return means the mirror was unlinked. Use EvictForQuota to find out
// whether the space was reclaimed.
func (m *Manager) Evict(ctx context.Context, repo *Repository, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, nil
	}
	result, err := m.evictAt(ctx, repo, time.Now().Add(-ttl))
	return result.Unlinked, err
}

// EvictForQuota removes a mirror that is over a storage budget rather than
// idle, so it requires no idleness: a cutoff of now admits every repository.
//
// In-flight protections are unchanged. A repository being read holds a serve
// reservation, and a fetch, snapshot or repack holds the fetch semaphore, so
// this declines instead of deleting a directory in use. As with Evict, the
// caller must stop the repository's periodic maintenance jobs first.
//
// The result is meaningful even with an error, see [EvictResult]. A caller
// enforcing a byte budget must count Reclaimed, not Unlinked.
func (m *Manager) EvictForQuota(ctx context.Context, repo *Repository) (EvictResult, error) {
	return m.evictAt(ctx, repo, time.Now())
}

// EvictResult reports how far an eviction got. The two steps fail
// independently and mean different things to a caller.
type EvictResult struct {
	// Unlinked is set once the mirror is renamed out of its serving path and
	// dropped from the manager. It no longer serves requests or appears in
	// All(), so its maintenance jobs must stay cancelled whatever else failed.
	Unlinked bool
	// Reclaimed is set once the staged copy is deleted. Unlinked without
	// Reclaimed means the bytes are still on the volume in a temp directory no
	// scan attributes to a mirror, so a byte budget must not count them freed.
	// The path is kept for [Manager.ReclaimLeftovers] to retry, and
	// DiscoverExisting sweeps up anything left at the next start.
	Reclaimed bool
}

func (m *Manager) rememberLeftover(path string) {
	m.leftoversMu.Lock()
	defer m.leftoversMu.Unlock()
	m.leftovers[path] = struct{}{}
}

// ReclaimLeftovers retries deleting staged mirrors whose removal failed, and
// returns the paths still on the volume.
//
// Those bytes belong to no repository in All(), so nothing else looks at them
// before a restart. A byte budget must keep counting them until a retry
// succeeds, or it gives the same space away twice.
func (m *Manager) ReclaimLeftovers() (remaining []string, err error) {
	m.leftoversMu.Lock()
	paths := slices.Sorted(maps.Keys(m.leftovers))
	m.leftoversMu.Unlock()

	var errs []error
	for _, path := range paths {
		// A vanished directory counts as reclaimed: something else, a restart's
		// DiscoverExisting or an operator, already freed the space.
		if rmErr := m.removeAll(path); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			remaining = append(remaining, path)
			errs = append(errs, errors.Wrapf(rmErr, "delete leftover staged mirror %s", path))
			continue
		}
		m.leftoversMu.Lock()
		delete(m.leftovers, path)
		m.leftoversMu.Unlock()
	}
	return remaining, errors.Join(errs...)
}

// MirrorsByLastServed returns the ready repositories, least recently served
// first, which is the order to reclaim them in.
func (m *Manager) MirrorsByLastServed() []*Repository {
	var ready []*Repository
	for _, repo := range m.All() {
		repo.mu.RLock()
		isReady := repo.state == StateReady
		repo.mu.RUnlock()
		if isReady {
			ready = append(ready, repo)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		return ready[i].LastServed().Before(ready[j].LastServed())
	})
	return ready
}

func (m *Manager) evictAt(ctx context.Context, repo *Repository, cutoff time.Time) (EvictResult, error) {
	if !repo.beginEvict(cutoff) {
		return EvictResult{}, nil
	}
	result := EvictResult{}
	err := repo.WithFetchExclusion(ctx, func() error {
		// Rename before deleting. A straggler git process keeps working
		// through its open descriptors, while any new path lookup fails at
		// once instead of reading a half-deleted repository. The temp prefix
		// makes DiscoverExisting clean up the leftovers if we die mid-delete.
		trash, err := os.MkdirTemp(m.config.MirrorRoot, cloneTempPrefix+"evicted-")
		if err != nil {
			return errors.Wrap(err, "create eviction temp dir")
		}
		staged := filepath.Join(trash, "mirror")
		if err := os.Rename(repo.path, staged); err != nil {
			// Nothing was moved in, so this is just the empty temp dir.
			_ = os.RemoveAll(trash)
			return errors.Wrapf(err, "stage mirror for eviction %s", repo.upstreamURL)
		}
		m.clonesMu.Lock()
		delete(m.clones, repo.upstreamURL)
		m.clonesMu.Unlock()
		repo.state = StateEmpty
		result.Unlinked = true
		if err := m.removeAll(trash); err != nil {
			// The staged copy still occupies trash, so the bytes are not freed.
			// Remember it: nothing else finds it before the next start, and a
			// budget that stops counting it hands the space to the cache.
			m.rememberLeftover(trash)
			return errors.Wrapf(err, "delete evicted mirror %s (left at %s)", repo.upstreamURL, trash)
		}
		result.Reclaimed = true
		return nil
	})
	repo.abortEvict()
	if err != nil {
		return result, err
	}
	if result.Unlinked {
		if err := m.lastServed.forget(repo.upstreamURL); err != nil {
			// Bookkeeping only, and it runs after the delete, so the space is
			// reclaimed either way.
			return result, errors.Wrap(err, "forget last served")
		}
	}
	return result, nil
}

func (m *Manager) DiscoverExisting(ctx context.Context) ([]*Repository, error) {
	var discovered []*Repository
	// One instant shared by every mirror seeded on this pass, so the times held
	// in memory and the times written below cannot disagree. Seeds are collected
	// during the walk and flushed after it: writing the record file inside the
	// walk would create and delete files in the directory being walked.
	seededAt := time.Now()
	var seeds []string
	walkErr := filepath.Walk(m.config.MirrorRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			return nil
		}

		if strings.HasPrefix(info.Name(), cloneTempPrefix) {
			logging.FromContext(ctx).InfoContext(ctx, "Removing leftover clone temp dir", "path", path)
			if rmErr := os.RemoveAll(path); rmErr != nil {
				return errors.Wrapf(rmErr, "remove leftover clone temp dir %s", path)
			}
			return fs.SkipDir
		}

		headPath := filepath.Join(path, "HEAD")
		if _, statErr := os.Stat(headPath); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return nil
			}
			return errors.Wrap(statErr, "stat HEAD file")
		}

		relPath, err := filepath.Rel(m.config.MirrorRoot, path)
		if err != nil {
			return errors.Wrap(err, "get relative path")
		}

		urlPath := filepath.ToSlash(relPath)

		host, repoPath, found := strings.Cut(urlPath, "/")
		if !found {
			return nil
		}
		upstreamURL := "https://" + host + "/" + repoPath

		// A mirror with no record has never been served. Its clock starts now
		// and is persisted after the walk, so restarts do not keep resetting it.
		lastServed := m.lastServed.get(upstreamURL)
		if lastServed.IsZero() {
			lastServed = seededAt
			seeds = append(seeds, upstreamURL)
		}

		repo := &Repository{
			state:              StateReady,
			config:             m.config,
			path:               path,
			upstreamURL:        upstreamURL,
			fetchSem:           make(chan struct{}, 1),
			credentialProvider: m.credentialProvider,
			lastServed:         lastServed,
		}
		repo.fetchSem <- struct{}{}

		if err := configureMirror(ctx, path, m.config.PackThreads); err != nil {
			return errors.Wrapf(err, "configure mirror for %s", upstreamURL)
		}

		if m.config.Maintenance {
			if err := registerMaintenance(ctx, path); err != nil {
				return errors.Wrapf(err, "register maintenance for %s", upstreamURL)
			}
		}

		m.clonesMu.Lock()
		m.clones[upstreamURL] = repo
		m.clonesMu.Unlock()

		discovered = append(discovered, repo)

		return fs.SkipDir
	})

	// Persisted even when the walk failed part-way: the mirrors it did reach are
	// already registered with an in-memory seed, and startup carries on after
	// logging the warm-up error, so leaving those seeds unwritten would let a
	// restart keep resetting their idle clocks.
	if len(seeds) > 0 {
		if err := m.lastServed.seedMissing(seeds, seededAt); err != nil {
			logging.FromContext(ctx).WarnContext(ctx,
				"Failed to persist initial last-served times, idle eviction will restart the clock for these mirrors",
				"upstreams", seeds, "error", err)
		}
	}

	if walkErr != nil {
		return nil, errors.Wrap(walkErr, "walk root directory")
	}

	return discovered, nil
}

// RepoPathFromURL extracts a normalised "host/path" from an upstream Git URL,
// stripping any ".git" suffix.
func RepoPathFromURL(upstreamURL string) (string, error) {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		return "", errors.Wrap(err, "parse upstream URL")
	}
	repoPath := filepath.Join(parsed.Host, strings.TrimSuffix(parsed.Path, ".git"))
	// Host and path both usually originate in a client-supplied request target,
	// and Join collapses "..", so a traversal in either can walk out of the root
	// every caller joins this onto. Refuse instead of silently resolving.
	if repoPath == ".." || strings.HasPrefix(repoPath, ".."+string(filepath.Separator)) || filepath.IsAbs(repoPath) {
		return "", errors.Errorf("upstream URL %q resolves outside the mirror root", upstreamURL)
	}
	return repoPath, nil
}

func (m *Manager) clonePathForURL(upstreamURL string) (string, error) {
	repoPath, err := RepoPathFromURL(upstreamURL)
	if err != nil {
		return "", err
	}
	return filepath.Join(m.config.MirrorRoot, repoPath), nil
}

func (r *Repository) State() State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *Repository) Path() string {
	return r.path
}

func (r *Repository) UpstreamURL() string {
	return r.upstreamURL
}

func (r *Repository) LastFetch() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastFetch
}

func (r *Repository) NeedsFetch(fetchInterval time.Duration) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	last := r.lastFetch
	if r.lastFetchAttempt.After(last) {
		last = r.lastFetchAttempt
	}
	return time.Since(last) >= fetchInterval
}

// FetchInFlight reports whether executeFetch is currently running.
//
// Why: lastFetchAttempt is stamped on entry, not completion, so NeedsFetch
// cannot tell an in-progress fetch from one that just finished. Check this
// before treating the cooldown as a reason to skip a coalescing wait. A
// caller that waits may still block up to the in-flight fetch's timeout.
//
// Not based on fetchSem: that lock is also held by non-fetch operations
// (e.g. WithFetchExclusion's snapshot tar).
func (r *Repository) FetchInFlight() bool {
	return r.fetchActive.Load()
}

// FetchFailures returns the number of consecutive failed fetches.
func (r *Repository) FetchFailures() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.fetchFailures
}

// BackgroundFetchBackoff returns how much longer a background fetch must be
// deferred after consecutive fetch failures, or zero if one may run now. The
// backoff starts at base and doubles per failure up to MaxFetchBackoff; a
// successful fetch clears it.
func (r *Repository) BackgroundFetchBackoff(base time.Duration) time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	backoff := FetchBackoff(base, r.fetchFailures)
	if backoff == 0 {
		return 0
	}
	remaining := backoff - time.Since(r.lastFetchAttempt)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// FetchBackoff returns the interval to wait after the given number of
// consecutive fetch failures, starting at base and doubling per failure,
// capped at MaxFetchBackoff. Zero failures means no backoff, so healthy
// mirrors keep their normal cadence.
func FetchBackoff(base time.Duration, failures int) time.Duration {
	if failures <= 0 {
		return 0
	}
	if base <= 0 {
		base = DefaultFetchInterval
	}
	backoff := base
	for range failures - 1 {
		if backoff >= MaxFetchBackoff {
			break
		}
		backoff *= 2
	}
	return min(backoff, MaxFetchBackoff)
}

func (r *Repository) WithReadLock(fn func() error) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return fn()
}

func WithReadLockReturn[T any](repo *Repository, fn func() (T, error)) (T, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	return fn()
}

// ResetToEmpty transitions the repository back to StateEmpty so that a
// subsequent call to TryStartCloning can re-attempt the clone. Use this
// when a restored snapshot turns out to be corrupt or empty and needs to
// be replaced with a fresh clone.
func (r *Repository) ResetToEmpty() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = StateEmpty
	r.lastFetch = time.Time{}
	r.lastFetchAttempt = time.Time{}
}

// TryStartCloning atomically transitions the repository from StateEmpty to
// StateCloning. Returns true if this goroutine won the transition and should
// proceed with the clone/restore; false if another goroutine already claimed it.
func (r *Repository) TryStartCloning() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != StateEmpty {
		return false
	}
	r.state = StateCloning
	return true
}

// WithFetchExclusion runs fn while holding the fetch semaphore, preventing
// concurrent git fetch operations. Use this for operations like tar that
// read the repository directory non-atomically and need a consistent view.
func (r *Repository) WithFetchExclusion(ctx context.Context, fn func() error) error {
	select {
	case <-r.fetchSem:
		defer func() { r.fetchSem <- struct{}{} }()
		return fn()
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "context cancelled waiting for fetch exclusion")
	}
}

// ConfigureMirror configures a git directory at repoPath as a mirror with
// the repository's pack threads and optional maintenance settings.
func (r *Repository) ConfigureMirror(ctx context.Context, repoPath string) error {
	if err := configureMirror(ctx, repoPath, r.config.PackThreads); err != nil {
		return errors.Wrap(err, "configure mirror")
	}
	if r.config.Maintenance {
		if err := registerMaintenance(ctx, repoPath); err != nil {
			return errors.Wrap(err, "register maintenance")
		}
	}
	return nil
}

// MarkReady transitions the repository to StateReady, indicating it is
// up-to-date and can serve requests from the local mirror.
func (r *Repository) MarkReady() {
	r.mu.Lock()
	r.state = StateReady
	r.lastFetch = time.Now()
	r.mu.Unlock()
}

func (r *Repository) Clone(ctx context.Context) error {
	r.mu.Lock()
	if r.state == StateReady {
		r.mu.Unlock()
		return nil
	}
	r.state = StateCloning
	r.mu.Unlock()

	err := r.executeClone(ctx)

	r.mu.Lock()
	if err != nil {
		r.state = StateEmpty
		r.mu.Unlock()
		return err
	}

	r.state = StateReady
	r.lastFetch = time.Now()
	r.mu.Unlock()
	return nil
}

// mirrorConfigSettings returns git config key-value pairs applied to mirror
// clones to optimise upload-pack serving performance.
func mirrorConfigSettings(packThreads int) [][2]string {
	return [][2]string{
		// Protocol
		{"protocol.version", "2"},
		{"uploadpack.allowFilter", "true"},
		{"uploadpack.allowAnySHA1InWant", "true"},
		// Bitmaps
		{"repack.writeBitmaps", "true"},
		{"pack.useBitmaps", "true"},
		{"pack.useBitmapBoundaryTraversal", "true"},
		// Commit graph
		{"core.commitGraph", "true"},
		{"gc.writeCommitGraph", "true"},
		{"fetch.writeCommitGraph", "true"},
		// Multi-pack-index
		{"core.multiPackIndex", "true"},
		// Keep fetched objects as packs
		{"transfer.unpackLimit", "1"},
		{"fetch.unpackLimit", "1"},
		// Disable auto GC. gc.auto stops git gc; maintenance.auto stops the
		// detached "git maintenance run --auto" that git 2.51+ fires after a
		// fetch, whose geometric-repack task gc.auto does not gate. Left on, it
		// repacks concurrently with our own repack and prune, and whichever
		// process loses the race dies on the other's half-written temp pack.
		// git maintenance register sets the same key for the same reason.
		{"gc.auto", "0"},
		{"maintenance.auto", "false"},
		// Pack performance
		{"pack.threads", strconv.Itoa(packThreads)},
		{"pack.deltaCacheSize", "512m"},
		{"pack.windowMemory", "1g"},
		// LFS fetches are I/O-bound; the git-lfs default of 8 is too low.
		// See git-lfs/git-lfs#6241 for an upstream change (unreleased) that
		// raises the default to 3×NCPU.
		{"lfs.concurrenttransfers", "100"},
	}
}

func registerMaintenance(ctx context.Context, repoPath string) error {
	// #nosec G204 - repoPath is controlled by us
	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "config", "maintenance.strategy", "incremental")
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "set maintenance.strategy: %s", string(output))
	}
	// #nosec G204 - repoPath is controlled by us
	cmd = exec.CommandContext(ctx, "git", "-C", repoPath, "maintenance", "register")
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "maintenance register: %s", string(output))
	}
	return nil
}

func startMaintenance(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "git", "maintenance", "start")
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "maintenance start: %s", string(output))
	}
	return nil
}

func configureMirror(ctx context.Context, repoPath string, packThreads int) error {
	for _, kv := range mirrorConfigSettings(packThreads) {
		// #nosec G204 - repoPath and config values are controlled by us
		cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "config", kv[0], kv[1])
		output, err := cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "configure %s: %s", kv[0], string(output))
		}
	}
	return nil
}

func (r *Repository) executeClone(ctx context.Context) error {
	parentDir := filepath.Dir(r.path)
	if err := os.MkdirAll(parentDir, 0o750); err != nil {
		return errors.Wrap(err, "create clone directory")
	}

	tmpDir, err := os.MkdirTemp(parentDir, cloneTempPrefix+"*")
	if err != nil {
		return errors.Wrap(err, "create temp clone directory")
	}
	defer os.RemoveAll(tmpDir) //nolint:errcheck // best-effort cleanup on failure

	// git clone --mirror creates a directory inside tmpDir; we point it at a
	// known subdirectory so we can rename it atomically afterwards.
	cloneDest := filepath.Join(tmpDir, "repo")

	cloneCtx, cancel := context.WithTimeout(ctx, r.config.CloneTimeout)
	defer cancel()

	config := DefaultGitTuningConfig()
	// lowSpeedLimit is intentionally omitted: during initial clone of large
	// repos the server-side pack computation can take minutes at near-zero
	// transfer rate, which would trip the speed check. The cloneTimeout
	// provides the overall safety net instead.
	// #nosec G204 - r.upstreamURL and cloneDest are controlled by us
	args := []string{
		"clone", "--mirror",
		"-c", "http.postBuffer=" + strconv.Itoa(config.PostBuffer),
		r.upstreamURL, cloneDest,
	}

	cmd, err := r.GitCommand(cloneCtx, args...)
	if err != nil {
		return errors.Wrap(err, "create git command")
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "git clone --mirror: %s", string(output))
	}

	if err := r.ConfigureMirror(ctx, cloneDest); err != nil {
		return errors.WithStack(err)
	}

	if err := os.Rename(cloneDest, r.path); err != nil {
		return errors.Wrap(err, "move clone into place")
	}
	return nil
}

func (r *Repository) Fetch(ctx context.Context) error {
	return r.FetchWithTimeout(ctx, r.config.FetchTimeout)
}

// FetchWithTimeout fetches from upstream with a configurable timeout. Use this
// for catch-up fetches after snapshot restore where the delta may be large and
// the default Config.FetchTimeout is too short.
func (r *Repository) FetchWithTimeout(ctx context.Context, timeout time.Duration) error {
	_, err := r.fetchInternal(ctx, timeout, true, true)
	return err
}

// FetchCoalescing fetches from upstream, coalescing with a concurrent semaphore
// holder rather than issuing a redundant network call. The attempted flag
// reports whether this call ran a git fetch itself (true, whether or not it
// succeeded) or piggy-backed on another holder's work (false), so a caller can
// tell a failed fetch from a coalesce wait that timed out without ever
// contacting upstream. Callers that need a guaranteed fetch should fall back to
// FetchVerified only when attempted is false.
func (r *Repository) FetchCoalescing(ctx context.Context) (attempted bool, err error) {
	return r.fetchInternal(ctx, r.config.FetchTimeout, true, true)
}

// FetchLenient fetches from upstream with the given timeout but without the
// low-speed transfer check. Use this for post-restore catch-up fetches where
// the delta may be very large and GitHub's server-side pack computation can
// stall at near-zero transfer rate for minutes — the same situation that
// executeClone handles by omitting lowSpeedLimit.
func (r *Repository) FetchLenient(ctx context.Context, timeout time.Duration) error {
	_, err := r.fetchInternal(ctx, timeout, false, true)
	return err
}

// FetchVerified fetches from upstream, waiting for the fetch semaphore if
// another operation holds it. Unlike Fetch, it never coalesces with a
// concurrent semaphore holder — the holder may not be fetching at all (e.g.
// WithFetchExclusion) or its fetch may have failed — so a nil return
// guarantees this call ran a successful git fetch.
func (r *Repository) FetchVerified(ctx context.Context) error {
	_, err := r.fetchInternal(ctx, r.config.FetchTimeout, true, false)
	return err
}

// FetchVerifiedIfAbsent waits for the fetch semaphore and runs a git fetch only
// when oids are still missing. If a concurrent fetch already brought the objects
// in while this call waited, or the wait itself was cut short, it returns
// attempted=false without hitting upstream.
func (r *Repository) FetchVerifiedIfAbsent(ctx context.Context, oids []string) (attempted bool, err error) {
	select {
	case <-r.fetchSem:
		defer func() { r.fetchSem <- struct{}{} }()
	case <-ctx.Done():
		return false, errors.Wrap(ctx.Err(), "context cancelled before acquiring fetch semaphore")
	}

	missing, err := r.MissingObjects(ctx, oids)
	if err != nil {
		return false, err
	}
	if len(missing) == 0 {
		return false, nil
	}

	fetchCtx, cancel := context.WithTimeout(ctx, r.config.FetchTimeout)
	defer cancel()
	return true, r.executeFetch(fetchCtx, true)
}

// fetchInternal returns attempted=true when this call ran the git fetch itself,
// whether or not the fetch succeeded, and attempted=false when it never reached
// upstream: a coalescing caller piggy-backed on another semaphore holder, or the
// context ended while waiting for the semaphore.
func (r *Repository) fetchInternal(ctx context.Context, timeout time.Duration, enforceSpeedLimit, coalesce bool) (attempted bool, err error) {
	select {
	case <-r.fetchSem:
		defer func() {
			r.fetchSem <- struct{}{}
		}()
	case <-ctx.Done():
		return false, errors.Wrap(ctx.Err(), "context cancelled before acquiring fetch semaphore")
	default:
		// The semaphore is held. Coalescing callers treat the holder's work as
		// their fetch; verified callers wait their turn and fetch themselves.
		if coalesce {
			select {
			case <-r.fetchSem:
				r.fetchSem <- struct{}{}
				return false, nil
			case <-ctx.Done():
				return false, errors.Wrap(ctx.Err(), "context cancelled while waiting for fetch")
			}
		}
		select {
		case <-r.fetchSem:
			defer func() {
				r.fetchSem <- struct{}{}
			}()
		case <-ctx.Done():
			return false, errors.Wrap(ctx.Err(), "context cancelled before acquiring fetch semaphore")
		}
	}

	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return true, r.executeFetch(fetchCtx, enforceSpeedLimit)
}

// executeFetch runs git fetch against upstream. The caller must hold fetchSem.
func (r *Repository) executeFetch(ctx context.Context, enforceSpeedLimit bool) error {
	// Marks the whole call, not just the subprocess. Kept separate from
	// fetchSem, which non-fetch operations also hold.
	r.fetchActive.Store(true)
	defer r.fetchActive.Store(false)

	// Record the attempt before the network call so a failure or timeout still
	// trips NeedsFetch's cooldown and serial requests do not each pay a full
	// IncrementalFetchTimeout / FetchTimeout during an upstream outage.
	r.mu.Lock()
	r.lastFetchAttempt = time.Now()
	r.mu.Unlock()

	config := DefaultGitTuningConfig()

	args := []string{
		"-C", r.path,
		"-c", "http.postBuffer=" + strconv.Itoa(config.PostBuffer),
	}
	if enforceSpeedLimit {
		args = append(args,
			"-c", "http.lowSpeedLimit="+strconv.Itoa(config.LowSpeedLimit),
			"-c", "http.lowSpeedTime="+strconv.Itoa(int(config.LowSpeedTime.Seconds())),
		)
	}
	args = append(args, "fetch", "--prune", "--prune-tags")

	cmd, err := r.GitCommand(ctx, args...)
	if err != nil {
		return errors.Wrap(err, "create git command")
	}
	// Start the process in its own process group so we can kill the entire
	// tree (git spawns child processes like git-remote-https that inherit
	// stdout/stderr pipes and prevent CombinedOutput from returning).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		// A fetch is the command whose deadline actually fires in production,
		// and it is killed mid ref update: without this, one slow upstream
		// leaves a packed-refs.lock or a loose ref lock that makes every later
		// fetch fail to update the refs it just downloaded. Safe here because
		// the caller holds fetchSem and prune holds it too, so no other writer
		// in this process can own these locks; the out-of-process case is
		// [Repository.clearStaleLocks]'s to refuse.
		r.clearStaleLocks(ctx, "fetch", r.refUpdateLockPaths())
		r.recordFetchOutcome(false)
		return errors.Wrapf(err, "git fetch: %s", string(output))
	}

	r.recordFetchOutcome(true)
	r.mu.Lock()
	// Drop any cached ref-staleness verdict so the next check reopens instead of
	// serving a stale "behind" result for the rest of the interval. This also
	// clears a cached failure, since upstream is now known reachable.
	r.invalidateRefCheckLocked()
	r.mu.Unlock()
	return nil
}

// recordFetchOutcome updates the fetch bookkeeping that drives background
// backoff. Consecutive failures accumulate so BackgroundFetchBackoff can
// lengthen the retry interval; success resets to the normal cadence.
func (r *Repository) recordFetchOutcome(success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastFetchAttempt = time.Now()
	if !success {
		r.fetchFailures++
		return
	}
	r.fetchFailures = 0
	r.lastFetch = time.Now()
}

// EnsureRefsUpToDate checks whether the local mirror's refs match upstream.
// If refs are stale it returns NeedsFetch=true so the caller can schedule a
// background fetch via the job scheduler, rather than fetching synchronously
// on the request path (which would acquire a write lock and block all serving).
//
// Concurrent callers share one check: the first runs the ls-remote while later
// callers wait on refCheckInFlight and reuse its outcome. Without this, a burst
// of requests landing in the same window would each issue their own ls-remote
// against upstream.
func (r *Repository) EnsureRefsUpToDate(ctx context.Context) (needsFetch bool, err error) {
	r.mu.Lock()
	if r.refCheckValid && time.Since(r.lastRefCheck) < r.config.RefCheckInterval {
		stale, checkErr := r.refCheckStale, r.refCheckErr
		r.mu.Unlock()
		return stale, checkErr
	}
	// A failed check is cached for a short backoff window so that during an
	// upstream outage serial requests do not each run their own ls-remote and wait
	// out LsRemoteTimeout. The window is a fraction of RefCheckInterval so
	// recovery is still noticed promptly, and it is keyed off the failure
	// timestamp rather than refCheckValid so a check in flight is never mistaken
	// for a completed one.
	if r.refCheckErr != nil && time.Since(r.refCheckErrAt) < r.refCheckFailureBackoff() {
		checkErr := r.refCheckErr
		r.mu.Unlock()
		return false, checkErr
	}
	if r.refCheckInFlight != nil {
		ch := r.refCheckInFlight
		r.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return false, errors.Wrap(ctx.Err(), "context cancelled waiting for in-flight ref check")
		}
		r.mu.Lock()
		stale, checkErr := r.refCheckStale, r.refCheckErr
		r.mu.Unlock()
		return stale, checkErr
	}
	// Record the check time to rate-limit re-entry after a successful check, but
	// do NOT set refCheckValid=true until the ls-remote succeeds. Otherwise
	// concurrent callers read a cached "refs up to date" result while the check
	// is still in flight and skip a fetch they needed.
	r.lastRefCheck = time.Now()
	r.refCheckInFlight = make(chan struct{})
	inFlight := r.refCheckInFlight
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		close(inFlight)
		r.refCheckInFlight = nil
		r.mu.Unlock()
	}()

	// Detach from the caller's context so a disconnecting client cannot fail every
	// waiter with a cancellation error. runRefCheck applies LsRemoteTimeout
	// internally.
	return r.runRefCheck(context.WithoutCancel(ctx))
}

// refCheckFailureBackoff is how long a failed ref check is cached before another
// caller may retry it. It is a fraction of RefCheckInterval: long enough to
// collapse a burst of serial requests during an upstream outage, short enough
// that recovery is picked up quickly.
func (r *Repository) refCheckFailureBackoff() time.Duration {
	return r.config.RefCheckInterval / 4
}

// invalidateRefCheckLocked drops any cached ref-check verdict, including a
// cached failure, so the next check reopens. The caller must hold r.mu.
func (r *Repository) invalidateRefCheckLocked() {
	r.refCheckValid = false
	r.refCheckErr = nil
	r.refCheckErrAt = time.Time{}
}

// runRefCheck performs the ls-remote comparison for the caller that started the
// check and records the outcome for callers waiting on it.
func (r *Repository) runRefCheck(ctx context.Context) (bool, error) {
	// Bound the whole check, not just the ls-remote: GetLocalRefs holds the
	// repository read lock across a for-each-ref subprocess, so a hang there
	// would otherwise wedge the refCheckInFlight singleflight slot forever.
	ctx, cancel := context.WithTimeout(ctx, r.config.LsRemoteTimeout)
	defer cancel()

	r.mu.RLock()
	lastFetchBefore := r.lastFetch
	r.mu.RUnlock()

	record := func(valid, stale bool, err error) {
		r.mu.Lock()
		// A fetch that landed while this check was in flight already invalidated
		// the cache; do not overwrite that with a pre-fetch verdict.
		if r.lastFetch.After(lastFetchBefore) {
			r.mu.Unlock()
			return
		}
		r.refCheckValid = valid
		r.refCheckStale = stale
		r.refCheckErr = err
		if err != nil {
			r.refCheckErrAt = time.Now()
		} else {
			r.refCheckErrAt = time.Time{}
		}
		r.mu.Unlock()
	}

	localRefs, err := r.GetLocalRefs(ctx)
	if err != nil {
		err = errors.Wrap(err, "get local refs")
		record(false, false, err)
		return false, err
	}

	upstreamRefs, err := r.GetUpstreamRefs(ctx)
	if err != nil {
		err = errors.Wrap(err, "get upstream refs")
		record(false, false, err)
		return false, err
	}

	for ref, upstreamSHA := range upstreamRefs {
		if strings.HasSuffix(ref, "^{}") {
			continue
		}
		if !strings.HasPrefix(ref, "refs/heads/") {
			continue
		}
		localSHA, exists := localRefs[ref]
		if !exists || localSHA != upstreamSHA {
			// Cache the stale verdict under RefCheckInterval so a behind
			// mirror does not re-issue ls-remote on every serial request;
			// a successful fetch clears the cache so the next check reopens.
			record(true, true, nil)
			return true, nil
		}
	}

	// The check succeeded and refs match. Cache the result so subsequent calls
	// within the interval skip the ls-remote round trip.
	record(true, false, nil)
	return false, nil
}

// EnsureRefs guarantees the local mirror satisfies the caller's freshness
// requirements. A ref entry is satisfied when the local mirror has the ref
// at the given SHA (or any SHA, if the value is empty). A commit entry is
// satisfied when the commit exists in the local object database, regardless
// of which ref points at it. If anything is unsatisfied, EnsureRefs
// synchronously fetches from upstream once and then re-checks.
//
// Resolved maps each requested ref to its current local SHA (empty if the
// ref is still missing after the fetch). MissingCommits lists requested
// commits that still are not present locally after the fetch.
//
// This bypasses RefCheckInterval because callers are explicitly asserting
// they require these refs/commits to be fresh right now.
func (r *Repository) EnsureRefs(
	ctx context.Context, refs map[string]string, commits []string,
) (resolved map[string]string, missingCommits []string, fetched bool, err error) {
	localRefs, err := r.GetLocalRefs(ctx)
	if err != nil {
		return nil, nil, false, errors.Wrap(err, "get local refs")
	}

	if refsSatisfied(localRefs, refs) {
		missing := r.missingCommitsLocked(ctx, commits)
		if len(missing) == 0 {
			return resolvedRefs(localRefs, refs), nil, false, nil
		}
	}

	if err := r.FetchWithTimeout(ctx, r.config.FetchTimeout); err != nil {
		return nil, nil, false, errors.Wrap(err, "fetch upstream")
	}

	// Invalidate the cached ref-check so the normal transparent path also
	// re-evaluates after our forced fetch.
	r.mu.Lock()
	r.invalidateRefCheckLocked()
	r.mu.Unlock()

	localRefs, err = r.GetLocalRefs(ctx)
	if err != nil {
		return nil, nil, true, errors.Wrap(err, "get local refs after fetch")
	}
	return resolvedRefs(localRefs, refs), r.missingCommitsLocked(ctx, commits), true, nil
}

// missingCommitsLocked returns the subset of commits that are absent from
// the local object database. It takes the repository's read lock to stay
// consistent with concurrent fetches.
func (r *Repository) missingCommitsLocked(ctx context.Context, commits []string) []string {
	if len(commits) == 0 {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var missing []string
	for _, sha := range commits {
		if sha == "" {
			continue
		}
		// Callers pass client-supplied strings here, so anything that is not an
		// object name is reported missing without forking git: that both keeps
		// arbitrary revisions (and revision syntax such as "HEAD^{/regex}" or
		// ":/text") out of the argument list and denies a caller a free existence
		// oracle over the whole revision namespace.
		if !objectNameRe.MatchString(sha) {
			missing = append(missing, sha)
			continue
		}
		// #nosec G204 - r.path is ours and sha is a validated object name
		cmd := exec.CommandContext(ctx, "git", "-C", r.path, "cat-file", "-e", "--end-of-options", sha)
		if err := cmd.Run(); err != nil {
			missing = append(missing, sha)
		}
	}
	return missing
}

// objectNameRe matches the object names git prints: 40 hex digits for SHA-1
// repositories, 64 for SHA-256 ones.
var objectNameRe = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)

// refsSatisfied reports whether every ref in expect is present in localRefs at
// the expected SHA (or at any SHA when the expected SHA is empty).
func refsSatisfied(localRefs, expect map[string]string) bool {
	for ref, wantSHA := range expect {
		localSHA, ok := localRefs[ref]
		if !ok {
			return false
		}
		if wantSHA != "" && localSHA != wantSHA {
			return false
		}
	}
	return true
}

func resolvedRefs(localRefs, expect map[string]string) map[string]string {
	if len(expect) == 0 {
		return nil
	}
	out := make(map[string]string, len(expect))
	for ref := range expect {
		out[ref] = localRefs[ref]
	}
	return out
}

func (r *Repository) GetLocalRefs(ctx context.Context) (map[string]string, error) {
	var output []byte
	var err error

	r.mu.RLock()
	// #nosec G204 - r.path is controlled by us
	cmd := exec.CommandContext(ctx, "git", "-C", r.path, "for-each-ref", "--format=%(objectname) %(refname)")
	output, err = cmd.CombinedOutput()
	r.mu.RUnlock()

	if err != nil {
		return nil, errors.Wrap(err, "git for-each-ref")
	}

	return ParseGitRefs(output), nil
}

func (r *Repository) GetUpstreamRefs(ctx context.Context) (map[string]string, error) {
	r.lsRemoteRuns.Add(1)
	// #nosec G204 - r.upstreamURL is controlled by us
	cmd, err := r.GitCommand(ctx, "ls-remote", r.upstreamURL)
	if err != nil {
		return nil, errors.Wrap(err, "create git command")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.Wrap(err, "git ls-remote")
	}

	return ParseGitRefs(output), nil
}

// Repack consolidates pack files using geometric repacking. Unlike a full
// repack (-a), geometric repacking only merges packs when there is significant
// fragmentation (many small packs), making it orders of magnitude faster on
// large repositories in steady state. The --write-midx and --write-bitmap-index
// flags maintain the multi-pack index and reachability bitmaps for efficient
// serving via git http-backend.
func (r *Repository) Repack(ctx context.Context) error {
	logger := logging.FromContext(ctx)
	logger.InfoContext(ctx, "Geometric repack started", "upstream", r.upstreamURL)

	repackCtx, cancel := context.WithTimeout(ctx, r.config.RepackTimeout)
	defer cancel()

	if err := r.runRepack(repackCtx, "-d", "--geometric=2", "--write-midx", "--write-bitmap-index"); err != nil {
		return err
	}

	logger.InfoContext(ctx, "Geometric repack completed", "upstream", r.upstreamURL)
	return nil
}

// PruneUnreachable reclaims objects that no ref points at any more. Those are
// what git fetch --prune leaves behind: it deletes the ref, which makes the
// objects unreachable, but never removes the objects themselves.
//
// A plain git prune would reclaim nothing. Mirrors set transfer.unpackLimit=1
// so fetched objects always land in a pack, and prune only deletes loose
// objects; dropping a packed one takes a repack that leaves it out. That is why
// this repacks rather than reusing the --geometric roll-up [Repository.Repack]
// uses, which combines packs wholesale and so carries unreachable objects
// forward indefinitely. The cost is that every pack is rewritten, making this
// far more expensive than the geometric repack.
//
// git dates an object by when it was written, never by when it stopped being
// reachable: a packed object inherits the mtime of the pack holding it. Handing
// --expire that date directly cannot express a grace period, because a commit
// written months ago and orphaned by a force-push a second later already reads
// as ancient. Somebody has to record the missing timestamp.
//
// A cruft pack is the closest git gets. It is an ordinary pack plus a .mtimes
// sidecar giving a per-object time, and once an object is in one that time is
// preserved by every later cruft repack, so the clock keeps running across
// prunes and --cruft-expiration deletes the object when the window elapses.
//
// The timestamp an object gets on the way in, though, is the mtime of the pack
// it came out of. So prune-grace means time since the object was last packed,
// not time since it stopped being reachable, and an object written long ago and
// orphaned a second later is reclaimed by the first prune that sees it. That is
// a deliberate limit, not an oversight, and it is what keeps this affordable:
// the only way to make git record the timestamp we actually want is to loosen
// every unreachable object so its mtime can be stamped, and a loose object is
// zlib-compressed on its own with no delta against its neighbours. Measured on a
// mirror whose whole history had just been orphaned, quarantining that way took
// the object store from 108 KiB to 2564 KiB where the cruft pack held the same
// objects in 144 KiB: a prune that grows the repository 23x instead of shrinking
// it. A mirror carrying a few hundred thousand unreachable objects, which is
// what upstream PR-branch churn produces, cannot pay that, and the objects the
// shortfall applies to are not that churn -- those were fetched recently and so
// sit in recent packs.
//
// What the window still buys is the thing that matters: upload-pack serves the
// mirror without a lock (mirrors set uploadpack.allowAnySHA1InWant, so
// unreachable objects are serveable), and an in-flight fetch measured in minutes
// must not have an object deleted underneath it because a force-push landed. A
// zero grace dates the cutoff at now and reclaims everything unreachable.
//
// git prune still runs. Nothing here loosens objects any more, but a mirror
// pruned by an earlier build is still carrying the loose set that build made,
// with the quarantine stamps it wrote, and only git prune will clear it.
//
// Packing refs is the last thing git gc does that still applies here. Nothing
// else in cachew ever does it, so loose refs accumulate one file per ref until
// some unrelated fetch happens to rewrite packed-refs. Reflogs, the other thing
// gc expires, do not exist in these mirrors: they are bare and never set
// core.logAllRefUpdates, so no reflog can pin an unreachable object.
//
// Not every call expires. A repack that applies --cruft-expiration has to read
// and rewrite every existing cruft pack to do it, so its cost tracks the whole
// unreachable set rather than the churn since the last prune, and on a mirror
// carrying hundreds of thousands of unreachable objects that is the dominant
// cost of the job. Where max-cruft-size-mb and prune-expire-interval are both
// set, most calls therefore run a repack that only combines the cruft packs
// below that size and deletes nothing, and the expiring repack runs once per
// prune-expire-interval. Deletion is late by up to that interval as a result,
// which is the tradeoff both settings document. Neither set means every call
// expires, which is what a deployment that has not opted in keeps getting.
//
// Holds the fetch exclusion throughout, because a fetch writes objects before
// it writes the refs that make them reachable, and a prune overlapping that
// window would see those objects as garbage. A prune therefore blocks fetches
// and snapshots on the repository until it finishes, which is why
// prune-interval is meant to be much longer than fetch-interval.
//
// Returns the bytes the prune returned to the volume. Both measurements are
// taken under the exclusion, so a concurrent fetch cannot grow the mirror
// between them and hide the reclamation. It is a walk of the mirror directory
// because the object count git reports says nothing about the bytes those
// objects occupied. Best-effort: a failed walk costs the number, not the prune.
//
// It is a net delta of the whole directory, so it can legitimately come back
// NEGATIVE: the same repack writes a multi-pack-index and its bitmap, and on a
// mirror that has never had them the pair is tens of kilobytes of new files
// that can exceed what the prune actually reclaimed. Observed on a real mirror
// set as 16 of 55 first prunes reporting a small negative. It is a first-prune
// artifact — later prunes rewrite those files rather than create them — and the
// caller drops non-positive values rather than adding them to the reclaimed
// counter, so a negative here understates reclamation but never corrupts it.
func (r *Repository) PruneUnreachable(ctx context.Context) (int64, error) {
	var reclaimed int64
	err := r.WithFetchExclusion(ctx, func() error {
		before, sizeErr := dirSizeBytes(r.path)
		if sizeErr != nil {
			before = 0
		}

		// One budget for every command: the repack dominates, and the rest have
		// nothing to do if it did not finish.
		pruneCtx, cancel := context.WithTimeout(ctx, r.config.PruneTimeout)
		defer cancel()

		// Measured after the wait for the exclusion, so the window is grace
		// from when the prune runs rather than from when it was queued.
		cutoff := time.Now().Add(-max(r.config.PruneGrace, 0)).Format(time.RFC3339)

		// The loose prune and packRefs below stay on every tick either way:
		// they cost the same whichever repack ran, and holding the deletion of
		// loose objects back to the expiring cadence would buy nothing.
		expire := r.cruftExpireDue()
		if err := r.runRepack(pruneCtx, pruneRepackArgs(cutoff, r.config.MaxCruftSizeMB, expire)...); err != nil {
			return err
		}
		if expire {
			r.mu.Lock()
			r.lastCruftExpireAt = time.Now()
			r.mu.Unlock()
		}

		if err := r.runGit(pruneCtx, "prune", "--expire="+cutoff); err != nil {
			return err
		}
		if err := r.packRefs(pruneCtx); err != nil {
			return err
		}

		if before > 0 {
			if after, afterErr := dirSizeBytes(r.path); afterErr == nil {
				reclaimed = before - after
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return reclaimed, nil
}

// cruftExpireDue reports whether this prune is the one that expires unreachable
// objects, rather than only rolling the small cruft packs together.
//
// True unless the deployment has opted into the split by setting both knobs.
// prune-expire-interval alone cannot buy anything: the combining repack saves
// work only by leaving cruft packs that have reached a size alone, and
// max-cruft-size-mb is what sets that size. A repository that has never expired
// in this process is due, so a restart cannot indefinitely postpone deletion.
func (r *Repository) cruftExpireDue() bool {
	if r.config.MaxCruftSizeMB <= 0 || r.config.PruneExpireInterval <= 0 {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return time.Since(r.lastCruftExpireAt) >= r.config.PruneExpireInterval
}

// pruneRepackArgs builds the repack that moves unreachable objects into a cruft
// pack, and, when expire is set, deletes the ones that sat there past the grace
// window.
//
// --cruft already implies -a for the reachable objects, and git rejects it
// alongside -A outright. Either way the unreachable objects reachable only from
// the packs being rewritten end up in a cruft pack rather than loose.
//
// The two branches exist because git will not do both jobs at once.
// --combine-cruft-below-size is what lets a repack leave a mature cruft pack on
// disk instead of reading and rewriting it, and git takes that path only when
// no expiration was asked for (builtin/repack.c consults it under
// cruft_expiration being unset). An expiring repack therefore rewrites every
// existing cruft pack however large the unreachable set has grown, which is the
// cost prune-expire-interval exists to pay less often.
//
// When expiring, maxCruftSizeMB caps the size of each cruft pack the run writes
// and nothing else: git implements it as --max-pack-size for the cruft
// pack-objects. When combining, it is instead the threshold at or above which
// an existing cruft pack is left alone, and the caller only takes this branch
// when it is positive, since combining below zero bytes would rewrite nothing
// and combining below every size would rewrite everything.
func pruneRepackArgs(cutoff string, maxCruftSizeMB int, expire bool) []string {
	if !expire {
		return []string{
			"-d", "--cruft",
			"--combine-cruft-below-size=" + strconv.Itoa(maxCruftSizeMB*bytesPerMB),
			"--write-midx", "--write-bitmap-index",
		}
	}
	args := []string{"-d", "--cruft", "--cruft-expiration=" + cutoff, "--write-midx", "--write-bitmap-index"}
	if maxCruftSizeMB > 0 {
		args = append(args, "--max-cruft-size="+strconv.Itoa(maxCruftSizeMB*bytesPerMB))
	}
	return args
}

// clearStaleLocks removes the lock files a git command would have removed
// itself had it not been killed, and reports which ones it took.
//
// Every git subprocess in this package is started in its own process group and
// cancelled with SIGKILL, so a deadline or a cancelled context takes the
// process out before it can run the atexit handler that releases its locks.
// Git never breaks a stale lock on its own: it retries for a bounded time
// (core.packedRefsTimeout is 1s, core.filesRefLockTimeout 100ms) and then
// fails. One killed command would therefore make every later command that
// needs the same lock fail forever, which for a mirror means fetches that can
// no longer update refs, or repacks that can no longer rebuild the multi-pack
// index.
//
// Safe only because the caller passes the locks its own command takes, and
// only runs while it holds the exclusion that keeps another writer off them:
// fetchSem serialises fetches, and prune holds it across the whole sequence.
// It is not a sweep of the repository.
//
// That reasoning covers this process only, so config.Maintenance disables the
// cleanup outright: it registers the mirror with the scheduled git maintenance
// on the host, whose pack-refs, prefetch and incremental-repack tasks take
// exactly these locks from another process and at a time no exclusion here can
// constrain. A lock held by a live process is indistinguishable from one left
// by a dead one, and unlinking a live one corrupts the run that owns it, which
// is worse than the stalled ref updates this repairs.
func (r *Repository) clearStaleLocks(ctx context.Context, command string, lockPaths []string) {
	if r.config.Maintenance {
		logging.FromContext(ctx).WarnContext(ctx,
			"Left any stale git locks in place because scheduled maintenance may hold them",
			"command", command, "upstream", r.upstreamURL)
		return
	}
	for _, lockPath := range lockPaths {
		if err := os.Remove(lockPath); err != nil {
			continue
		}
		logging.FromContext(ctx).WarnContext(ctx, "Removed a stale git lock left by a failed command",
			"command", command, "lock", lockPath, "upstream", r.upstreamURL)
	}
}

// refUpdateLockPaths lists the locks a command that writes refs can be killed
// holding: the packed-refs lock, the FETCH_HEAD lock, and one per loose ref
// that was mid-update. Existence is not checked, because [clearStaleLocks]
// ignores the ones that are not there.
func (r *Repository) refUpdateLockPaths() []string {
	paths := []string{
		filepath.Join(r.path, "packed-refs.lock"),
		filepath.Join(r.path, "FETCH_HEAD.lock"),
	}
	// Loose ref locks are named after the ref, so they have to be found rather
	// than listed. A failed walk costs the cleanup, not the caller's error.
	refsDir := filepath.Join(r.path, "refs")
	_ = filepath.WalkDir(refsDir, func(path string, d fs.DirEntry, err error) error { //nolint:errcheck // best-effort: an unreadable refs dir just means no loose locks to clear
		if err != nil {
			return nil //nolint:nilerr // best-effort: skip unreadable entries
		}
		if !d.IsDir() && strings.HasSuffix(path, ".lock") {
			paths = append(paths, path)
		}
		return nil
	})
	return paths
}

// runGit invokes git in the mirror, in its own process group so a cancelled
// context kills any child it spawned rather than leaving it holding a lock.
func (r *Repository) runGit(ctx context.Context, args ...string) error {
	// #nosec G204 - r.path is controlled by us, as are the caller's arguments
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", r.path}, args...)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "git %s: %s", strings.Join(args, " "), string(output))
	}
	return nil
}

// dirSizeBytes sums the apparent size of every regular file under root.
// Entries that vanish under the walk are skipped: a prune deletes files while
// it runs, and those bytes really are gone.
func dirSizeBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // best-effort: skip unreadable entries
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return nil //nolint:nilerr // entry vanished or is unreadable
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, errors.Wrapf(err, "walk %s", root)
	}
	return total, nil
}

// runRepack invokes git repack with the given arguments, cleaning up the
// multi-pack-index lock if the process is killed.
func (r *Repository) runRepack(ctx context.Context, repackArgs ...string) error {
	threads := r.config.RepackThreads
	if threads <= 0 {
		threads = r.config.PackThreads
	}

	// Override pack settings for repack to bound memory usage. The repo-level
	// config uses high values (512m deltaCacheSize, 1g windowMemory) tuned for
	// serving performance with many threads. Repack is a background task that
	// can afford to be slower in exchange for bounded memory.
	args := append([]string{
		"-C", r.path,
		"-c", "pack.threads=" + strconv.Itoa(threads),
		"-c", "pack.windowMemory=256m",
		"-c", "pack.deltaCacheSize=128m",
		"repack",
	}, repackArgs...)

	// #nosec G204 - r.path is controlled by us, as are the caller's flags
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		// A stale multi-pack-index.lock blocks all future repacks, causing pack
		// fragmentation that eventually degrades snapshot serving performance.
		// Only this lock: Repack runs without the fetch exclusion, so a fetch
		// may be updating refs alongside it and its ref locks are live.
		r.clearStaleLocks(ctx, "repack", []string{
			filepath.Join(r.path, "objects", "pack", "multi-pack-index.lock"),
		})
		return errors.Wrapf(err, "git repack: %s", string(output))
	}
	return nil
}

// packRefs folds loose refs into packed-refs, cleaning up the locks it holds if
// the process is killed.
//
// A killed pack-refs cannot remove the packed-refs.lock it holds, and a stale
// lock makes every later ref packing fail, so loose refs would accumulate with
// nothing able to fold them away again. --prune also locks each loose ref it
// deletes, so those are cleared too. Callers run under the fetch exclusion,
// which is what makes taking them safe.
func (r *Repository) packRefs(ctx context.Context) error {
	if err := r.runGit(ctx, "pack-refs", "--all", "--prune"); err != nil {
		r.clearStaleLocks(ctx, "pack-refs", r.refUpdateLockPaths())
		return err
	}
	return nil
}

func (r *Repository) HasCommit(ctx context.Context, ref string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// ref may originate from a request, so --end-of-options keeps it in revision
	// position no matter what it starts with.
	// #nosec G204 - r.path is ours and ref cannot be read as an option
	cmd := exec.CommandContext(ctx, "git", "-C", r.path, "cat-file", "-e", "--end-of-options", ref)
	err := cmd.Run()
	return err == nil
}
