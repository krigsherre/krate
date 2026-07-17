package borrow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krigsherre/krate/internal/clock"
)

type mockPool struct {
	mu       sync.Mutex
	borrowFn func(ctx context.Context, key, instanceID string, requested uint64, leaseTTLms int64) (uint64, error)
	borrowN  int32
}

func (m *mockPool) Borrow(ctx context.Context, key, instanceID string, requested uint64, leaseTTLms int64) (uint64, error) {
	atomic.AddInt32(&m.borrowN, 1)
	if m.borrowFn != nil {
		return m.borrowFn(ctx, key, instanceID, requested, leaseTTLms)
	}
	return 0, nil
}

func TestManagerAcquireSuccess(t *testing.T) {
	mock := &mockPool{
		borrowFn: func(_ context.Context, _, _ string, _ uint64, _ int64) (uint64, error) {
			return 500, nil
		},
	}

	sizer := NewAdaptiveSizer(0.3, 100, 1000, time.Second, false)
	mgr := NewBorrowManager(mock, "inst-1", BorrowManagerOpts{
		Sizer:    sizer,
		LeaseTTL: 30 * time.Second,
	})

	granted, err := mgr.Acquire(context.Background(), "key1", 100, nil)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if granted != 500 {
		t.Errorf("granted = %d, want 500", granted)
	}
	if got := mgr.Borrowed("key1"); got != 500 {
		t.Errorf("Borrowed(key1) = %d, want 500", got)
	}
}

func TestManagerAcquireExhausted(t *testing.T) {
	mock := &mockPool{
		borrowFn: func(_ context.Context, _, _ string, _ uint64, _ int64) (uint64, error) {
			return 0, nil
		},
	}

	sizer := NewAdaptiveSizer(0.3, 100, 1000, time.Second, false)
	mgr := NewBorrowManager(mock, "inst-1", BorrowManagerOpts{
		Sizer:    sizer,
		LeaseTTL: 30 * time.Second,
	})

	granted, err := mgr.Acquire(context.Background(), "key1", 100, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if granted != 0 {
		t.Errorf("granted = %d, want 0 (exhausted)", granted)
	}
}

func TestManagerSingleflight(t *testing.T) {
	mock := &mockPool{
		borrowFn: func(_ context.Context, _, _ string, _ uint64, _ int64) (uint64, error) {
			time.Sleep(50 * time.Millisecond)
			return 100, nil
		},
	}

	sizer := NewAdaptiveSizer(0.3, 10, 1000, time.Second, false)
	mgr := NewBorrowManager(mock, "inst-1", BorrowManagerOpts{
		Sizer:    sizer,
		LeaseTTL: 30 * time.Second,
	})

	var ready, go2 sync.WaitGroup
	ready.Add(100)
	go2.Add(1)

	for i := 0; i < 100; i++ {
		go func() {
			ready.Done()
			go2.Wait()
			_, _ = mgr.Acquire(context.Background(), "key1", 10, nil)
		}()
	}

	ready.Wait()
	go2.Done()
	time.Sleep(200 * time.Millisecond)

	if got := atomic.LoadInt32(&mock.borrowN); got != 1 {
		t.Errorf("pool.Borrow called %d times, want 1 (singleflight)", got)
	}
}



func TestManagerRecordConsumption(t *testing.T) {
	mock := &mockPool{
		borrowFn: func(_ context.Context, _, _ string, _ uint64, _ int64) (uint64, error) {
			return 100, nil
		},
	}

	sizer := NewAdaptiveSizer(1.0, 10, 1000, time.Second, true)
	mgr := NewBorrowManager(mock, "inst-1", BorrowManagerOpts{
		Sizer:    sizer,
		LeaseTTL: 30 * time.Second,
	})

	mgr.RecordConsumption("key1", 50)

	if got := sizer.CurrentRate(); got != 50 {
		t.Errorf("CurrentRate() = %.2f, want 50", got)
	}
}

func TestManagerLeaseEviction(t *testing.T) {
	mock := &mockPool{
		borrowFn: func(_ context.Context, _, _ string, requested uint64, _ int64) (uint64, error) {
			return 500, nil
		},
	}

	fc := clock.NewFakeClock(time.Now())
	sizer := NewAdaptiveSizer(0.3, 100, 1000, time.Second, false)
	mgr := NewBorrowManager(mock, "inst-1", BorrowManagerOpts{
		Sizer:    sizer,
		LeaseTTL: 10 * time.Second,
		Clock:    fc,
	})

	_, err := mgr.Acquire(context.Background(), "key1", 500, nil)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if got := mgr.Borrowed("key1"); got != 500 {
		t.Errorf("Borrowed(key1) = %d, want 500", got)
	}

	fc.Advance(11 * time.Second)

	if got := mgr.Borrowed("key1"); got != 0 {
		t.Errorf("after lease expiration, Borrowed(key1) = %d, want 0", got)
	}

	all := mgr.AllBorrowed(0)
	if len(all) != 0 {
		t.Errorf("after lease expiration, AllBorrowed = %v, want empty", all)
	}
}

func TestManagerTopN(t *testing.T) {
	mock := &mockPool{
		borrowFn: func(_ context.Context, key, _ string, requested uint64, _ int64) (uint64, error) {
			if key == "key-low" {
				return 100, nil
			} else if key == "key-mid" {
				return 200, nil
			}
			return 300, nil
		},
	}

	sizer := NewAdaptiveSizer(0.3, 100, 1000, time.Second, false)
	mgr := NewBorrowManager(mock, "inst-1", BorrowManagerOpts{
		Sizer:    sizer,
		LeaseTTL: 10 * time.Second,
	})

	mgr.Acquire(context.Background(), "key-low", 100, nil)
	mgr.Acquire(context.Background(), "key-high", 300, nil)
	mgr.Acquire(context.Background(), "key-mid", 200, nil)

	top := mgr.AllBorrowed(2)
	if len(top) != 2 {
		t.Fatalf("AllBorrowed(2) returned %d keys, want 2", len(top))
	}

	if _, exists := top["key-high"]; !exists {
		t.Errorf("missing key-high in top 2")
	}
	if _, exists := top["key-mid"]; !exists {
		t.Errorf("missing key-mid in top 2")
	}
	if _, exists := top["key-low"]; exists {
		t.Errorf("unexpected key-low in top 2")
	}
}
