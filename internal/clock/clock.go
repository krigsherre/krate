package clock

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	NewTicker(d time.Duration) *time.Ticker
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func NewRealClock() Clock { return realClock{} }

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) Since(t time.Time) time.Duration        { return time.Since(t) }
func (realClock) NewTicker(d time.Duration) *time.Ticker { return time.NewTicker(d) }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type waiter struct {
	deadline time.Time
	ch       chan time.Time
}

type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []waiter
}

func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

func (fc *FakeClock) Now() time.Time {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.now
}

func (fc *FakeClock) Since(t time.Time) time.Duration {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.now.Sub(t)
}

func (fc *FakeClock) SetNow(t time.Time) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.now = t
}

func (fc *FakeClock) Advance(d time.Duration) {
	fc.mu.Lock()
	fc.now = fc.now.Add(d)
	now := fc.now

	var remaining []waiter
	for _, w := range fc.waiters {
		if !now.Before(w.deadline) {
			go func(ch chan time.Time, t time.Time) {
				ch <- t
			}(w.ch, now)
		} else {
			remaining = append(remaining, w)
		}
	}
	fc.waiters = remaining
	fc.mu.Unlock()
}

func (fc *FakeClock) After(d time.Duration) <-chan time.Time {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	ch := make(chan time.Time, 1)
	deadline := fc.now.Add(d)

	if !fc.now.Before(deadline) {
		ch <- fc.now
		return ch
	}

	fc.waiters = append(fc.waiters, waiter{deadline: deadline, ch: ch})
	return ch
}

func (fc *FakeClock) NewTicker(d time.Duration) *time.Ticker {
	return time.NewTicker(d)
}

func (fc *FakeClock) Waiters() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.waiters)
}
