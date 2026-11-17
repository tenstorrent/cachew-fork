package git

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/alecthomas/errors"
	"golang.org/x/sync/semaphore"
)

// packThreadBudget caps the pack.threads summed across every pack-objects
// process serving a clone at once. Each pack-objects can hold pack.windowMemory
// per thread, so bounding threads process-wide bounds that memory, and requests
// beyond the budget queue instead of pushing the pod past its memory limit.
type packThreadBudget struct {
	sem        *semaphore.Weighted
	size       int64
	maxPerPack int64
	inUse      atomic.Int64
	waiting    atomic.Int64
}

// newPackThreadBudget returns nil when size is not positive, which disables the
// budget.
func newPackThreadBudget(size, maxPerPack int) *packThreadBudget {
	if size <= 0 {
		return nil
	}
	return &packThreadBudget{
		sem:        semaphore.NewWeighted(int64(size)),
		size:       int64(size),
		maxPerPack: int64(max(1, maxPerPack)),
	}
}

// acquire returns as many threads as a single pack may use, shrunk to what is
// free, with a func that returns them to the budget. When nothing is free it
// waits, cancellably, for one thread rather than failing, then takes whatever
// else has freed up by then. Grants are reserved without blocking so that
// concurrent callers cannot both size a request against the same free count
// and leave threads idle while one of them queues.
func (b *packThreadBudget) acquire(ctx context.Context) (int64, func(), error) {
	limit := min(b.maxPerPack, b.size)
	k := b.tryAcquireUpTo(limit)
	if k == 0 {
		b.waiting.Add(1)
		err := b.sem.Acquire(ctx, 1)
		b.waiting.Add(-1)
		if err != nil {
			return 0, nil, errors.Wrap(err, "acquire pack threads")
		}
		k = 1 + b.tryAcquireUpTo(limit-1)
	}
	b.inUse.Add(k)
	return k, func() {
		b.inUse.Add(-k)
		b.sem.Release(k)
	}, nil
}

// tryAcquireUpTo reserves the largest count of at most n threads that is free
// without waiting, returning 0 when none is. TryAcquire refuses while requests
// are queued, so a caller arriving later cannot overtake them.
func (b *packThreadBudget) tryAcquireUpTo(n int64) int64 {
	for k := n; k > 0; k-- {
		if b.sem.TryAcquire(k) {
			return k
		}
	}
	return 0
}

// acquirePackThreads acquires threads from the strategy's budget and records how
// long the request waited. It returns 0 threads and a no-op release when the
// budget is disabled.
func (s *Strategy) acquirePackThreads(ctx context.Context) (int64, func(), error) {
	if s.packThreads == nil {
		return 0, func() {}, nil
	}
	start := time.Now()
	k, release, err := s.packThreads.acquire(ctx)
	s.metrics.recordPackThreadWait(ctx, err == nil, time.Since(start))
	return k, release, err
}
