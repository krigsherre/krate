package krate

import (
	"context"
	"time"
)

type EvictionPolicy interface {
	ShouldEvict(b *bucket, nowMs int64) bool
}

type IdleEvictionPolicy struct {
	idleTimeout time.Duration
}

func NewIdleEvictionPolicy(timeout time.Duration) *IdleEvictionPolicy {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return &IdleEvictionPolicy{idleTimeout: timeout}
}

func (p *IdleEvictionPolicy) ShouldEvict(b *bucket, nowMs int64) bool {
	idleFor := time.Duration(nowMs-b.getLastAccess()) * time.Millisecond
	return idleFor > p.idleTimeout
}

type Janitor struct {
	interval time.Duration
	evictFn  func(nowMs int64)
}

func NewJanitor(interval time.Duration, evictFn func(nowMs int64)) *Janitor {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &Janitor{
		interval: interval,
		evictFn:  evictFn,
	}
}

func (j *Janitor) Start(ctx context.Context, clock Clock) {
	ticker := clock.NewTicker(j.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				j.evictFn(clock.Now().UnixMilli())
			}
		}
	}()
}
