package krate

import (
	"sync/atomic"
	_ "unsafe"
)

const cacheLineSize = 64

type LocalBucket struct {
	tokens atomic.Int64
	_pad   [cacheLineSize - 8]byte
}

func NewLocalBucket(_ string, initial uint64) *LocalBucket {
	b := &LocalBucket{}
	b.tokens.Store(int64(initial))
	return b
}

func (b *LocalBucket) TryConsume(n uint64) bool {
	in := int64(n)
	for {
		current := b.tokens.Load()
		if current < in {
			return false
		}
		if b.tokens.CompareAndSwap(current, current-in) {
			return true
		}
	}
}

func (b *LocalBucket) Refill(n uint64) {
	b.tokens.Add(int64(n))
}

func (b *LocalBucket) Remaining() uint64 {
	v := b.tokens.Load()
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func (b *LocalBucket) Drain() uint64 {
	old := b.tokens.Swap(0)
	if old < 0 {
		return 0
	}
	return uint64(old)
}
