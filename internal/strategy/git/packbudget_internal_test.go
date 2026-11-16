package git

import (
	"context"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func TestPackThreadsFor(t *testing.T) {
	tests := []struct {
		name       string
		size       int64
		maxPerPack int64
		inUse      int64
		expected   int64
	}{
		{name: "IdleGetsFullPerPack", size: 16, maxPerPack: 8, inUse: 0, expected: 8},
		{name: "PartlyUsedGetsRemainder", size: 16, maxPerPack: 8, inUse: 11, expected: 5},
		{name: "ExhaustedFloorsAtOne", size: 16, maxPerPack: 8, inUse: 16, expected: 1},
		{name: "PerPackAboveBudgetCapsAtBudget", size: 4, maxPerPack: 8, inUse: 0, expected: 4},
		{name: "OverSubscribedFloorsAtOne", size: 4, maxPerPack: 8, inUse: 6, expected: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, packThreadsFor(tt.size, tt.maxPerPack, tt.inUse))
		})
	}
}

func TestNewPackThreadBudgetDisabled(t *testing.T) {
	assert.Zero(t, newPackThreadBudget(0, 8))
	assert.Zero(t, newPackThreadBudget(-1, 8))
}

func TestPackThreadBudgetQueuesWhenExhausted(t *testing.T) {
	b := newPackThreadBudget(10, 8)

	first, releaseFirst, err := b.acquire(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, int64(8), first)

	second, releaseSecond, err := b.acquire(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, int64(2), second)
	assert.Equal(t, int64(10), b.inUse.Load())

	granted := make(chan int64)
	go func() {
		k, release, err := b.acquire(t.Context())
		assert.NoError(t, err)
		release()
		granted <- k
	}()
	select {
	case k := <-granted:
		t.Fatalf("acquire of %d threads must wait while the budget is exhausted", k)
	case <-time.After(50 * time.Millisecond):
	}

	releaseSecond()
	assert.Equal(t, int64(1), <-granted)
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
