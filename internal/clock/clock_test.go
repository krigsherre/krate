package clock

import (
	"sync"
	"testing"
	"time"
)

func TestRealClockNow(t *testing.T) {
	c := NewRealClock()
	before := time.Now()
	got := c.Now()
	after := time.Now()

	if got.Before(before) || got.After(after) {
		t.Errorf("RealClock.Now() = %v, want between %v and %v", got, before, after)
	}
}

func TestFakeClockSetNow(t *testing.T) {
	fc := NewFakeClock(time.Unix(0, 0))

	ts := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	fc.SetNow(ts)

	if got := fc.Now(); !got.Equal(ts) {
		t.Errorf("after SetNow(%v), Now() = %v", ts, got)
	}
}

func TestFakeClockAdvance(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	fc.Advance(5 * time.Second)

	want := start.Add(5 * time.Second)
	if got := fc.Now(); !got.Equal(want) {
		t.Errorf("after Advance(5s), Now() = %v, want %v", got, want)
	}

	fc.Advance(3 * time.Second)
	want = start.Add(8 * time.Second)
	if got := fc.Now(); !got.Equal(want) {
		t.Errorf("after second Advance(3s), Now() = %v, want %v", got, want)
	}
}

func TestFakeClockSince(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	past := start.Add(-10 * time.Second)
	if got := fc.Since(past); got != 10*time.Second {
		t.Errorf("Since() = %v, want 10s", got)
	}
}

func TestFakeClockAfterFires(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	ch := fc.After(3 * time.Second)

	select {
	case <-ch:
		t.Fatal("After channel fired before deadline")
	case <-time.After(50 * time.Millisecond):
	}

	fc.Advance(5 * time.Second)

	select {
	case got := <-ch:
		want := start.Add(5 * time.Second)
		if !got.Equal(want) {
			t.Errorf("After received %v, want %v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("After channel did not fire after Advance past deadline")
	}
}

func TestFakeClockAfterZeroDuration(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	ch := fc.After(0)

	select {
	case got := <-ch:
		if !got.Equal(start) {
			t.Errorf("After(0) received %v, want %v", got, start)
		}
	case <-time.After(time.Second):
		t.Fatal("After(0) did not fire immediately")
	}
}

func TestFakeClockWaiters(t *testing.T) {
	fc := NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	if got := fc.Waiters(); got != 0 {
		t.Fatalf("initial Waiters() = %d, want 0", got)
	}

	fc.After(1 * time.Second)
	fc.After(2 * time.Second)
	fc.After(3 * time.Second)

	if got := fc.Waiters(); got != 3 {
		t.Fatalf("Waiters() after 3x After = %d, want 3", got)
	}

	fc.Advance(1500 * time.Millisecond)
	if got := fc.Waiters(); got != 2 {
		t.Fatalf("Waiters() after Advance(1.5s) = %d, want 2", got)
	}

	fc.Advance(2 * time.Second)
	if got := fc.Waiters(); got != 0 {
		t.Fatalf("Waiters() after second Advance = %d, want 0", got)
	}
}

func TestFakeClockConcurrentSafety(t *testing.T) {
	fc := NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = fc.Now()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				fc.Advance(time.Millisecond)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = fc.Waiters()
			}
		}()
	}
	wg.Wait()
}
