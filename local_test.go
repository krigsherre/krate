package krate

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLocalBucketConsume(t *testing.T) {
	b := NewLocalBucket("test", 100)

	if ok := b.TryConsume(50); !ok {
		t.Fatal("TryConsume(50) failed with 100 tokens")
	}
	if got := b.Remaining(); got != 50 {
		t.Errorf("Remaining() = %d, want 50", got)
	}
}

func TestLocalBucketInsufficientTokens(t *testing.T) {
	b := NewLocalBucket("test", 10)

	if ok := b.TryConsume(20); ok {
		t.Fatal("TryConsume(20) succeeded with only 10 tokens")
	}
	if got := b.Remaining(); got != 10 {
		t.Errorf("Remaining() = %d, want 10 (unchanged)", got)
	}
}

func TestLocalBucketConcurrentTryConsume(t *testing.T) {
	b := NewLocalBucket("test", 10000)

	var consumed atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if b.TryConsume(1) {
					consumed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := consumed.Load(); got != 10000 {
		t.Errorf("total consumed = %d, want 10000", got)
	}
	if got := b.Remaining(); got != 0 {
		t.Errorf("Remaining() = %d, want 0", got)
	}
}

func TestLocalBucketRefill(t *testing.T) {
	b := NewLocalBucket("test", 100)

	b.TryConsume(100)
	if got := b.Remaining(); got != 0 {
		t.Fatalf("after consuming all: Remaining() = %d, want 0", got)
	}

	b.Refill(50)
	if got := b.Remaining(); got != 50 {
		t.Errorf("after Refill(50): Remaining() = %d, want 50", got)
	}
}

func TestLocalBucketDrain(t *testing.T) {
	b := NewLocalBucket("test", 500)

	drained := b.Drain()
	if drained != 500 {
		t.Errorf("Drain() = %d, want 500", drained)
	}
	if got := b.Remaining(); got != 0 {
		t.Errorf("after Drain: Remaining() = %d, want 0", got)
	}
}

func TestLocalBucketDrainEmpty(t *testing.T) {
	b := NewLocalBucket("test", 0)

	drained := b.Drain()
	if drained != 0 {
		t.Errorf("Drain() on empty = %d, want 0", drained)
	}
}



func TestLocalBucketNoUnderflow(t *testing.T) {
	b := NewLocalBucket("test", 10)

	if ok := b.TryConsume(15); ok {
		t.Fatal("TryConsume(15) succeeded with only 10 tokens")
	}
	if got := b.Remaining(); got != 10 {
		t.Errorf("Remaining() = %d, want 10 (must not underflow)", got)
	}
}

func BenchmarkLocalBucketTryConsume(b *testing.B) {
	bucket := NewLocalBucket("bench", uint64(b.N)+1000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket.TryConsume(1)
	}
}

func TestBucketDelegation(t *testing.T) {
	bkt := newBucket("k", 100, Fixed, 60*time.Second)

	if got := bkt.remaining(); got != 0 {
		t.Errorf("new bucket remaining = %d, want 0", got)
	}

	bkt.refill(60)
	if got := bkt.remaining(); got != 60 {
		t.Errorf("after refill(60): remaining = %d, want 60", got)
	}

	if ok := bkt.tryConsume(30); !ok {
		t.Fatal("tryConsume(30) failed")
	}
	if got := bkt.remaining(); got != 30 {
		t.Errorf("after consume(30): remaining = %d, want 30", got)
	}

	drained := bkt.drain()
	if drained != 30 {
		t.Errorf("drain = %d, want 30", drained)
	}
	if got := bkt.remaining(); got != 0 {
		t.Errorf("after drain: remaining = %d, want 0", got)
	}
}
