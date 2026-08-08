package repoauthz

import (
	"strconv"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func repoURL(i int) string { return "https://example.invalid/org/repo" + strconv.Itoa(i) }

func TestVerdictCacheEnforcesItsLimit(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		stores  int
		wantMax int
	}{
		{name: "LiveEntriesAreStillCapped", ttl: time.Hour, stores: verdictCacheLimit * 3, wantMax: verdictCacheLimit},
		{name: "ExpiredEntriesArePrunedFirst", ttl: time.Nanosecond, stores: verdictCacheLimit * 3, wantMax: verdictCacheLimit},
		{name: "BelowTheLimitNothingIsEvicted", ttl: time.Hour, stores: verdictCacheTarget, wantMax: verdictCacheTarget},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := NewAuthorizer(nil, test.ttl)
			for i := range test.stores {
				a.store(repoURL(i), true)
				assert.True(t, len(a.verdicts) <= verdictCacheLimit,
					"cache grew past its limit at insertion %d: %d entries", i, len(a.verdicts))
			}
			assert.True(t, len(a.verdicts) <= test.wantMax, "want at most %d entries, got %d", test.wantMax, len(a.verdicts))
		})
	}
}

// TestVerdictCacheEvictsOldestFirst pins which entries an overflowing cache
// gives up. Probe times are seeded explicitly so "oldest" is unambiguous: a
// tight insertion loop can read the same clock value many times over.
func TestVerdictCacheEvictsOldestFirst(t *testing.T) {
	a := NewAuthorizer(nil, time.Hour)
	now := time.Now()
	for i := range verdictCacheLimit {
		a.verdicts[repoURL(i)] = verdict{
			requiresAuth: true,
			probedAt:     now.Add(-time.Duration(verdictCacheLimit-i) * time.Millisecond),
		}
	}

	a.store("https://example.invalid/org/newcomer", true)

	assert.Equal(t, verdictCacheTarget+1, len(a.verdicts))
	evicted := verdictCacheLimit - verdictCacheTarget
	for _, i := range []int{0, evicted - 1} {
		_, fresh := a.cached(repoURL(i))
		assert.False(t, fresh, "verdict %d should have been evicted as one of the oldest", i)
	}
	for _, i := range []int{evicted, verdictCacheLimit - 1} {
		_, fresh := a.cached(repoURL(i))
		assert.True(t, fresh, "verdict %d should have survived eviction", i)
	}
	_, fresh := a.cached("https://example.invalid/org/newcomer")
	assert.True(t, fresh, "the verdict that triggered eviction was evicted by its own insertion")
}
