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

// packThreadsFor picks how many threads a new pack-objects gets: as many as a
// single pack may use, shrunk to what is left of the budget, but never below 1
// so a request waits for a thread rather than failing.
func packThreadsFor(size, maxPerPack, inUse int64) int64 {
	return max(1, min(maxPerPack, size, size-inUse))
}

// acquire blocks until threads are available or ctx is done, and returns the
// thread count granted with a func that returns them to the budget.
func (b *packThreadBudget) acquire(ctx context.Context) (int64, func(), error) {
	k := packThreadsFor(b.size, b.maxPerPack, b.inUse.Load())
	b.waiting.Add(1)
	err := b.sem.Acquire(ctx, k)
	b.waiting.Add(-1)
	if err != nil {
		return 0, nil, errors.Wrap(err, "acquire pack threads")
	}
	b.inUse.Add(k)
	return k, func() {
		b.inUse.Add(-k)
		b.sem.Release(k)
	}, nil
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
