package borrow

import (
	"math"
	"sync"
	"time"
)

type AdaptiveSizer struct {
	mu        sync.Mutex
	emaRate   float64
	alpha     float64
	minBorrow uint64
	maxBorrow uint64
	refillSec float64
	enabled   bool
	hasData   bool
}

func NewAdaptiveSizer(alpha float64, minBorrow, maxBorrow uint64, refillInterval time.Duration, enabled bool) *AdaptiveSizer {
	return &AdaptiveSizer{
		alpha:     alpha,
		minBorrow: minBorrow,
		maxBorrow: maxBorrow,
		refillSec: refillInterval.Seconds(),
		enabled:   enabled,
	}
}

func (s *AdaptiveSizer) Record(consumed uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emaRate = s.alpha*float64(consumed) + (1-s.alpha)*s.emaRate
	s.hasData = true
}

func (s *AdaptiveSizer) ComputeBorrowSize() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.enabled || !s.hasData {
		return s.maxBorrow
	}

	target := s.emaRate * s.refillSec * 2.0
	result := uint64(math.Round(target))

	if result < s.minBorrow {
		return s.minBorrow
	}
	if result > s.maxBorrow {
		return s.maxBorrow
	}
	return result
}

func (s *AdaptiveSizer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emaRate = 0
	s.hasData = false
}

func (s *AdaptiveSizer) CurrentRate() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.emaRate
}
