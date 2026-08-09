package gitclone

import (
	"context"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
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
	Maintenance      bool          `hcl:"maintenance,optional" help:"Enable git maintenance scheduling for mirror repos." default:"false"`
	PackThreads      int           `hcl:"pack-threads,optional" help:"Threads for git pack operations (0 = all CPU cores)." default:"0"`
	CloneTimeout     time.Duration `hcl:"clone-timeout,optional" help:"Upper bound for 'git clone --mirror'. Generous by default since large repos can take 30+ minutes." default:"1h"`
	FetchTimeout     time.Duration `hcl:"fetch-timeout,optional" help:"Upper bound for 'git fetch' so a slow upstream cannot block the fetch path indefinitely." default:"5m"`
	LsRemoteTimeout  time.Duration `hcl:"ls-remote-timeout,optional" help:"Upper bound for 'git ls-remote' so a slow upstream cannot block the request path indefinitely." default:"1m"`
	RepackTimeout    time.Duration `hcl:"repack-timeout,optional" help:"Upper bound for 'git repack' so a slow repack on a large repository cannot block the scheduler queue indefinitely." default:"10m"`
	RepackThreads    int           `hcl:"repack-threads,optional" help:"Threads for git repack operations. Limits memory since windowMemory and deltaCacheSize are per-thread. 0 = pack-threads." default:"4"`
	MirrorTTL        time.Duration `hcl:"mirror-ttl,optional" help:"Delete a mirrored repository that no client has requested for this long. Its next request re-clones it, so keep this well above mirror-snapshot-interval or eviction turns a warm serve into a cold one. 0 disables idle eviction." default:"0"`
	MirrorEvictScan  time.Duration `hcl:"mirror-evict-scan,optional" help:"How often to scan mirrors for idle eviction. Ignored when mirror-ttl is 0." default:"1h"`
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
	}, nil
}

func (m *Manager) Config() Config {
	return m.config
}

func (m *Manager) GetOrCreate(_ context.Context, upstreamURL string) (*Repository, error) {
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

	repo = &Repository{
		state:              StateEmpty,
		config:             m.config,
		path:               clonePath,
		upstreamURL:        upstreamURL,
		fetchSem:           make(chan struct{}, 1),
		credentialProvider: m.credentialProvider,
		lastServed:         m.seedLastServed(upstreamURL),
	}

	headFile := filepath.Join(clonePath, "HEAD")
	if _, err := os.Stat(headFile); err == nil {
		repo.state = StateReady
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
func (m *Manager) seedLastServed(upstreamURL string) time.Time {
	if recorded := m.lastServed.get(upstreamURL); !recorded.IsZero() {
		return recorded
	}
	return time.Now()
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
func (m *Manager) Evict(ctx context.Context, repo *Repository, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, nil
	}
	if !repo.beginEvict(time.Now().Add(-ttl)) {
		return false, nil
	}
	evicted := false
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
			_ = os.RemoveAll(trash)
			return errors.Wrapf(err, "stage mirror for eviction %s", repo.upstreamURL)
		}
		m.clonesMu.Lock()
		delete(m.clones, repo.upstreamURL)
		m.clonesMu.Unlock()
		repo.state = StateEmpty
		evicted = true
		if err := os.RemoveAll(trash); err != nil {
			return errors.Wrapf(err, "delete evicted mirror %s", repo.upstreamURL)
		}
		return nil
	})
	repo.abortEvict()
	if err != nil {
		return evicted, err
	}
	if evicted {
		if err := m.lastServed.forget(repo.upstreamURL); err != nil {
			return true, errors.Wrap(err, "forget last served")
		}
	}
	return evicted, nil
}

func (m *Manager) DiscoverExisting(ctx context.Context) ([]*Repository, error) {
	var discovered []*Repository
	err := filepath.Walk(m.config.MirrorRoot, func(path string, info os.FileInfo, err error) error {
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

		repo := &Repository{
			state:              StateReady,
			config:             m.config,
			path:               path,
			upstreamURL:        upstreamURL,
			fetchSem:           make(chan struct{}, 1),
			credentialProvider: m.credentialProvider,
			lastServed:         m.seedLastServed(upstreamURL),
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

	if err != nil {
		return nil, errors.Wrap(err, "walk root directory")
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
		// Disable auto GC
		{"gc.auto", "0"},
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

	threads := r.config.RepackThreads
	if threads <= 0 {
		threads = r.config.PackThreads
	}

	// Override pack settings for repack to bound memory usage. The repo-level
	// config uses high values (512m deltaCacheSize, 1g windowMemory) tuned for
	// serving performance with many threads. Repack is a background task that
	// can afford to be slower in exchange for bounded memory.
	// #nosec G204 - r.path is controlled by us
	cmd := exec.CommandContext(repackCtx, "git", "-C", r.path,
		"-c", "pack.threads="+strconv.Itoa(threads),
		"-c", "pack.windowMemory=256m",
		"-c", "pack.deltaCacheSize=128m",
		"repack", "-d", "--geometric=2", "--write-midx", "--write-bitmap-index")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		// When git repack is killed (e.g. by our timeout), it cannot clean up
		// the multi-pack-index.lock it holds. A stale lock blocks all future
		// repacks, causing pack fragmentation that eventually degrades snapshot
		// serving performance. Remove it so the next attempt can proceed.
		lockPath := filepath.Join(r.path, "objects", "pack", "multi-pack-index.lock")
		if rmErr := os.Remove(lockPath); rmErr == nil {
			logger.WarnContext(ctx, "Removed stale multi-pack-index.lock after repack failure", "upstream", r.upstreamURL)
		}
		return errors.Wrapf(err, "git repack: %s", string(output))
	}

	logger.InfoContext(ctx, "Geometric repack completed", "upstream", r.upstreamURL)
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
