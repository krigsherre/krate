package krate

import (
	"sync/atomic"
	"time"
)

type Window struct {
	windowType  WindowType
	limit       uint64
	windowMs    int64
	windowSize  time.Duration
	windowStart atomic.Int64
}

func NewWindow(windowType WindowType, windowSize time.Duration, limit uint64) *Window {
	w := &Window{
		windowType: windowType,
		windowSize: windowSize,
		windowMs:   windowSize.Milliseconds(),
		limit:      limit,
	}
	return w
}

func (w *Window) NeedsReset(nowUnixMs int64) bool {
	return nowUnixMs >= w.windowStart.Load()+w.windowMs
}

func (w *Window) UpdateWindowStart(startMs int64) {
	w.windowStart.Store(startMs)
}

func (w *Window) WindowStartMs() int64 {
	return w.windowStart.Load()
}

func (w *Window) Limit() uint64 {
	return w.limit
}

func (w *Window) WindowSize() time.Duration {
	return w.windowSize
}

func (w *Window) EffectiveLimit(elapsedMs, windowMs int64, prevCount, currCount uint64) uint64 {
	switch w.windowType {
	case Sliding:
		if windowMs <= 0 {
			return 0
		}
		slidingPrev := uint64((int64(prevCount) * elapsedMs) / windowMs)
		effective := slidingPrev + currCount
		if effective >= w.limit {
			return 0
		}
		return w.limit - effective
	default:
		if currCount >= w.limit {
			return 0
		}
		return w.limit - currCount
	}
}
