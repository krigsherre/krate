package borrow

import (
	"testing"
	"time"
)

func TestAdaptiveSizerDefault(t *testing.T) {
	s := NewAdaptiveSizer(0.3, 100, 1000, time.Second, false)

	for i := 0; i < 10; i++ {
		s.Record(50)
	}
	if got := s.ComputeBorrowSize(); got != 1000 {
		t.Errorf("disabled sizer: ComputeBorrowSize() = %d, want 1000", got)
	}
}

func TestAdaptiveSizerEMA(t *testing.T) {
	s := NewAdaptiveSizer(0.3, 10, 10000, time.Second, true)
	for i := 0; i < 5; i++ {
		s.Record(100)
	}

	rate := s.CurrentRate()
	if rate < 80 || rate > 85 {
		t.Errorf("EMA after 5×100 with α=0.3: rate = %.2f, want ~83", rate)
	}
}

func TestAdaptiveSizerClamp(t *testing.T) {
	s := NewAdaptiveSizer(0.3, 100, 500, time.Second, true)
	for i := 0; i < 20; i++ {
		s.Record(10000)
	}
	if got := s.ComputeBorrowSize(); got != 500 {
		t.Errorf("high rate: ComputeBorrowSize() = %d, want 500", got)
	}
	s.Reset()
	s.Record(1)
	if got := s.ComputeBorrowSize(); got != 100 {
		t.Errorf("low rate: ComputeBorrowSize() = %d, want 100", got)
	}
}

func TestAdaptiveSizerZeroConsumption(t *testing.T) {
	s := NewAdaptiveSizer(0.3, 100, 1000, time.Second, true)
	s.Record(0)

	if got := s.ComputeBorrowSize(); got != 100 {
		t.Errorf("zero consumption: ComputeBorrowSize() = %d, want 100", got)
	}
}

func TestAdaptiveSizerReset(t *testing.T) {
	s := NewAdaptiveSizer(0.3, 100, 1000, time.Second, true)

	for i := 0; i < 10; i++ {
		s.Record(100)
	}

	s.Reset()

	if got := s.ComputeBorrowSize(); got != 1000 {
		t.Errorf("after reset: ComputeBorrowSize() = %d, want 1000 (maxBorrow)", got)
	}
	if got := s.CurrentRate(); got != 0 {
		t.Errorf("after reset: CurrentRate() = %.2f, want 0", got)
	}
}

func TestAdaptiveSizerBorrowSizeFormula(t *testing.T) {
	s := NewAdaptiveSizer(1.0, 10, 10000, time.Second, true)
	s.Record(50)
	if got := s.ComputeBorrowSize(); got != 100 {
		t.Errorf("emaRate=50, refill=1s: ComputeBorrowSize() = %d, want 100", got)
	}
	s2 := NewAdaptiveSizer(1.0, 10, 10000, 2*time.Second, true)
	s2.Record(50)
	if got := s2.ComputeBorrowSize(); got != 200 {
		t.Errorf("emaRate=50, refill=2s: ComputeBorrowSize() = %d, want 200", got)
	}
}
