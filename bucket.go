package krate

import (
	"sync/atomic"
	"time"
)

type bucket struct {
	local  *LocalBucket
	window *Window
	key    string

	redisExhausted atomic.Bool
	preBorrowing   atomic.Bool
}

func newBucket(key string, limit uint64, windowType WindowType, windowSize time.Duration) *bucket {
	return &bucket{
		local:  NewLocalBucket(key, 0),
		window: NewWindow(windowType, windowSize, limit),
		key:    key,
	}
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
