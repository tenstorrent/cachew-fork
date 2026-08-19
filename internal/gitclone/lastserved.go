package gitclone

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alecthomas/errors"
)

// lastServedFile is the name of the record inside the mirror root. It sits
// beside the mirrors rather than in the shared metadata store because the
// mirror root is a single-writer ReadWriteOnce volume: these timestamps
// describe that volume's contents and are meaningless apart from it. Keeping
// them together also means wiping the mirror root wipes the bookkeeping.
const lastServedFile = ".last-served.json"

// lastServedGranularity is the coarsest useful resolution for an idle timer
// measured in days. Recording every request would put a disk write on the
// serve path for no benefit, so a repository's timestamp is only rewritten
// once it is this stale — the same trade-off Linux makes with relatime.
const lastServedGranularity = time.Hour

// lastServedStore records when each mirror was last asked for by a client.
// Writes are write-through so an unclean shutdown cannot lose more than
// lastServedGranularity of freshness for any repository.
type lastServedStore struct {
	path  string
	mu    sync.Mutex
	times map[string]time.Time
}

// loadLastServedStore reads the record under mirrorRoot, treating a missing or
// unreadable file as empty. A corrupt file must not be fatal: the mirrors
// themselves are intact and the worst case is that every repository looks
// freshly served, which delays eviction rather than destroying data.
func loadLastServedStore(mirrorRoot string) *lastServedStore {
	s := &lastServedStore{
		path:  filepath.Join(mirrorRoot, lastServedFile),
		times: map[string]time.Time{},
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return s
	}
	var raw map[string]int64
	if err := json.Unmarshal(data, &raw); err != nil {
		return s
	}
	for url, unix := range raw {
		s.times[url] = time.Unix(unix, 0)
	}
	return s
}

func (s *lastServedStore) get(upstreamURL string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.times[upstreamURL]
}

// record stores now for upstreamURL, skipping the write when the stored value
// is already within lastServedGranularity. It reports whether it wrote.
func (s *lastServedStore) record(upstreamURL string, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, existed := s.times[upstreamURL]
	if existed && now.Sub(previous) < lastServedGranularity {
		return false, nil
	}
	s.times[upstreamURL] = now
	if err := s.flushLocked(); err != nil {
		// The in-memory time is only there to throttle writes, so keeping one
		// that never reached disk would suppress the next write too.
		if existed {
			s.times[upstreamURL] = previous
		} else {
			delete(s.times, upstreamURL)
		}
		return false, err
	}
	return true, nil
}

// seedMissing records now for each URL that has no record yet, fixing those
// mirrors' idle clocks at the moment cachew first observed them. URLs that
// already have a record are left alone, so a real serve time is never
// overwritten and an unserved mirror keeps ageing across restarts.
//
// Persisting the invented time is what makes the clock survive a restart. A
// mirror discovered on disk with no record would otherwise be re-seeded to now
// on every process start, so a deployment cadence shorter than mirror-ttl would
// keep resetting it and it could never be evicted.
//
// Takes a batch and flushes at most once: startup discovery seeds every mirror
// on the volume at the same instant, and a flush rewrites the whole file.
func (s *lastServedStore) seedMissing(upstreamURLs []string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var added []string
	for _, url := range upstreamURLs {
		if _, ok := s.times[url]; ok {
			continue
		}
		s.times[url] = now
		added = append(added, url)
	}
	if len(added) == 0 {
		return nil
	}
	if err := s.flushLocked(); err != nil {
		// Keeping unpersisted seeds in memory would make a later seeding attempt
		// skip them, and record() would throttle against a time that was never
		// written, so a transient failure would suppress both until restart.
		for _, url := range added {
			delete(s.times, url)
		}
		return err
	}
	return nil
}

// forget drops upstreamURL, so an evicted repository does not linger in the
// record and re-appear as ancient if it is cloned again later.
func (s *lastServedStore) forget(upstreamURL string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.times[upstreamURL]; !ok {
		return nil
	}
	delete(s.times, upstreamURL)
	return s.flushLocked()
}

func (s *lastServedStore) flushLocked() error {
	raw := make(map[string]int64, len(s.times))
	for url, t := range s.times {
		raw[url] = t.Unix()
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return errors.Wrap(err, "marshal last-served record")
	}
	// Write-and-rename: a torn file here would reset every repository's idle
	// clock, and rename is atomic within the mirror root's filesystem.
	tmp, err := os.CreateTemp(filepath.Dir(s.path), lastServedFile+".*")
	if err != nil {
		return errors.Wrap(err, "create last-served temp file")
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return errors.Wrap(err, "write last-served record")
	}
	if err := tmp.Close(); err != nil {
		return errors.Wrap(err, "close last-served record")
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return errors.Wrap(err, "replace last-served record")
	}
	return nil
}
