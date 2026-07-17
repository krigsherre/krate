package krate

import (
	"sync"
	"sync/atomic"
	"time"
)

type bucket struct {
	mu         sync.Mutex
	local      *LocalBucket
	window     *Window
	key        string
	consumed   atomic.Uint64
	lastAccess atomic.Int64

	redisExhausted atomic.Bool
	preBorrowing   atomic.Bool
}

func newBucket(key string, limit uint64, windowType WindowType, windowSize time.Duration) *bucket {
	b := &bucket{
		local:  NewLocalBucket(key, 0),
		window: NewWindow(windowType, windowSize, limit),
		key:    key,
	}
	b.consumed.Store(0)
	b.lastAccess.Store(time.Now().UnixMilli())
	return b
}

func (b *bucket) updateAccess(nowUnixMs int64) {
	b.lastAccess.Store(nowUnixMs)
}

func (b *bucket) getLastAccess() int64 {
	return b.lastAccess.Load()
}

func (b *bucket) tryConsume(n uint64) bool {
	return b.local.TryConsume(n)
}

func (b *bucket) refill(n uint64) {
	b.local.Refill(n)
}

func (b *bucket) needsReset(nowUnixMs int64) bool {
	return b.window.NeedsReset(nowUnixMs)
}

func (b *bucket) remaining() uint64 {
	return b.local.Remaining()
}

func (b *bucket) drain() uint64 {
	return b.local.Drain()
}

func (b *bucket) MarkRedisExhausted() { b.redisExhausted.Store(true) }

func (b *bucket) ClearRedisExhausted() { b.redisExhausted.Store(false) }

func (b *bucket) IsRedisExhausted() bool { return b.redisExhausted.Load() }

func (b *bucket) TryMarkPreBorrowing() bool {
	return b.preBorrowing.CompareAndSwap(false, true)
}

func (b *bucket) ClearPreBorrowing() { b.preBorrowing.Store(false) }

func (b *bucket) isPreBorrowing() bool { return b.preBorrowing.Load() }

func (b *bucket) belowPreBorrowThreshold(minBorrow uint64, threshold float64) bool {
	trigger := uint64(float64(minBorrow) * threshold)
	if trigger == 0 {
		trigger = 1
	}
	return b.remaining() < trigger
}

func (b *bucket) recordConsumption(n uint64) {
	b.consumed.Add(n)
}

func (b *bucket) getConsumed() uint64 {
	return b.consumed.Load()
}

func (b *bucket) resetConsumed() {
	b.consumed.Store(0)
}
