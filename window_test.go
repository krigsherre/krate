package krate

import (
	"testing"
	"time"
)

func TestWindowNeedsReset_Fixed(t *testing.T) {
	w := NewWindow(Fixed, 60*time.Second, 100)
	w.UpdateWindowStart(0)

	tests := []struct {
		nowUnixMs int64
		want      bool
	}{
		{59_000, false},
		{60_000, true},
		{60_001, true},
		{120_000, true},
		{0, false},
		{59_999, false},
	}

	for _, tc := range tests {
		got := w.NeedsReset(tc.nowUnixMs)
		if got != tc.want {
			t.Errorf("NeedsReset(%d) = %v, want %v", tc.nowUnixMs, got, tc.want)
		}
	}
}

func TestWindowUpdateWindowStart(t *testing.T) {
	w := NewWindow(Fixed, 60*time.Second, 100)
	w.UpdateWindowStart(100_000)

	if w.NeedsReset(159_000) {
		t.Error("NeedsReset(159_000) = true, want false")
	}

	if !w.NeedsReset(160_000) {
		t.Error("NeedsReset(160_000) = false, want true")
	}

	w.UpdateWindowStart(200_000)

	if w.NeedsReset(259_000) {
		t.Error("after UpdateWindowStart(200_000), NeedsReset(259_000) = true, want false")
	}
	if !w.NeedsReset(260_000) {
		t.Error("after UpdateWindowStart(200_000), NeedsReset(260_000) = false, want true")
	}
}

func TestWindowEffectiveLimit_Fixed(t *testing.T) {
	w := NewWindow(Fixed, 60*time.Second, 100)

	got := w.EffectiveLimit(0, 0, 0, 30)
	if got != 70 {
		t.Errorf("EffectiveLimit(fixed, currCount=30) = %d, want 70", got)
	}

	got = w.EffectiveLimit(0, 0, 0, 100)
	if got != 0 {
		t.Errorf("EffectiveLimit(fixed, currCount=100) = %d, want 0", got)
	}

	got = w.EffectiveLimit(0, 0, 0, 0)
	if got != 100 {
		t.Errorf("EffectiveLimit(fixed, currCount=0) = %d, want 100", got)
	}
}

func TestWindowEffectiveLimit_Sliding(t *testing.T) {
	w := NewWindow(Sliding, 60*time.Second, 100)

	got := w.EffectiveLimit(30_000, 60_000, 80, 20)
	if got != 40 {
		t.Errorf("EffectiveLimit(sliding, half, prev=80, curr=20) = %d, want 40", got)
	}

	got = w.EffectiveLimit(0, 60_000, 0, 50)
	if got != 50 {
		t.Errorf("EffectiveLimit(sliding, start, prev=0, curr=50) = %d, want 50", got)
	}

	got = w.EffectiveLimit(50_000, 60_000, 100, 0)
	if got != 17 {
		t.Errorf("EffectiveLimit(sliding, 50/60, prev=100, curr=0) = %d, want 17", got)
	}
}

func TestWindowEffectiveLimit_SlidingOverflow(t *testing.T) {
	w := NewWindow(Sliding, 60*time.Second, 100)

	got := w.EffectiveLimit(0, 60_000, 1000, 500)
	if got != 0 {
		t.Errorf("EffectiveLimit(overflow) = %d, want 0", got)
	}

	got = w.EffectiveLimit(59_000, 60_000, 200, 200)
	if got != 0 {
		t.Errorf("EffectiveLimit(overflow near end) = %d, want 0", got)
	}

	got = w.EffectiveLimit(0, 60_000, 0, 100)
	if got != 0 {
		t.Errorf("EffectiveLimit(effective==limit) = %d, want 0", got)
	}
}

func TestWindowAccessors(t *testing.T) {
	w := NewWindow(Sliding, 30*time.Second, 500)

	if got := w.Limit(); got != 500 {
		t.Errorf("Limit() = %d, want 500", got)
	}
	if got := w.WindowSize(); got != 30*time.Second {
		t.Errorf("WindowSize() = %v, want 30s", got)
	}

	w.UpdateWindowStart(42_000)
	if got := w.WindowStartMs(); got != 42_000 {
		t.Errorf("WindowStartMs() = %d, want 42000", got)
	}
}
