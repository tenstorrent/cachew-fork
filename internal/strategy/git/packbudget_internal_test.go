package git

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func TestPackThreadBudgetGrant(t *testing.T) {
	tests := []struct {
		name       string
		size       int
		maxPerPack int
		held       int64
		expected   int64
	}{
		{name: "IdleGetsFullPerPack", size: 16, maxPerPack: 8, held: 0, expected: 8},
		{name: "PartlyUsedGetsRemainder", size: 16, maxPerPack: 8, held: 11, expected: 5},
		{name: "PerPackAboveBudgetCapsAtBudget", size: 4, maxPerPack: 8, held: 0, expected: 4},
		{name: "PerPackFloorsAtOne", size: 4, maxPerPack: 0, held: 0, expected: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newPackThreadBudget(tt.size, tt.maxPerPack)
			if tt.held > 0 {
				assert.True(t, b.sem.TryAcquire(tt.held))
			}
			k, release, err := b.acquire(t.Context())
			assert.NoError(t, err)
			defer release()
			assert.Equal(t, tt.expected, k)
		})
	}
}

func TestNewPackThreadBudgetDisabled(t *testing.T) {
	assert.Zero(t, newPackThreadBudget(0, 8))
	assert.Zero(t, newPackThreadBudget(-1, 8))
}

func TestPackThreadBudgetConcurrentGrantsUseWholeBudget(t *testing.T) {
	b := newPackThreadBudget(10, 8)
	start := make(chan struct{})
	grants := make([]int64, 2)
	var wg sync.WaitGroup
	for i := range grants {
		wg.Go(func() {
			<-start
			k, _, err := b.acquire(t.Context())
			assert.NoError(t, err)
			grants[i] = k
		})
	}
	close(start)
	wg.Wait()

	slices.Sort(grants)
	assert.Equal(t, []int64{2, 8}, grants)
	assert.Equal(t, int64(10), b.inUse.Load())
}

func TestPackThreadBudgetQueuesWhenExhausted(t *testing.T) {
	b := newPackThreadBudget(10, 8)

	first, releaseFirst, err := b.acquire(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, int64(8), first)

	second, releaseSecond, err := b.acquire(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, int64(2), second)

	granted := make(chan int64, 1)
	go func() {
		k, release, err := b.acquire(t.Context())
		assert.NoError(t, err)
		release()
		granted <- k
	}()
	waitForWaiters(t, b, 1)

	releaseSecond()
	assert.Equal(t, int64(2), <-granted)
	releaseFirst()
	assert.Equal(t, int64(0), b.inUse.Load())
}

func TestPackThreadBudgetAcquireHonoursCancellation(t *testing.T) {
	b := newPackThreadBudget(4, 4)
	_, release, err := b.acquire(t.Context())
	assert.NoError(t, err)
	defer release()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, _, err = b.acquire(ctx)
	assert.IsError(t, err, context.DeadlineExceeded)
	assert.Equal(t, int64(0), b.waiting.Load())
	assert.Equal(t, int64(4), b.inUse.Load())
}

func waitForWaiters(t *testing.T, b *packThreadBudget, n int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for b.waiting.Load() != n {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d queued acquisitions, got %d", n, b.waiting.Load())
		}
		time.Sleep(time.Millisecond)
	}
}
