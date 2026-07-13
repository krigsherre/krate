package borrow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockPool struct {
	mu       sync.Mutex
	borrowFn func(ctx context.Context, key, instanceID string, requested uint64, leaseTTLms int64) (uint64, error)
	returnFn func(ctx context.Context, key, instanceID string, tokens uint64) error
	borrowN  int32
	returnN  int32
}

func (m *mockPool) Borrow(ctx context.Context, key, instanceID string, requested uint64, leaseTTLms int64) (uint64, error) {
	atomic.AddInt32(&m.borrowN, 1)
	if m.borrowFn != nil {
		return m.borrowFn(ctx, key, instanceID, requested, leaseTTLms)
	}
	return 0, nil
}

func (m *mockPool) Return(ctx context.Context, key, instanceID string, tokens uint64) error {
	atomic.AddInt32(&m.returnN, 1)
	if m.returnFn != nil {
		return m.returnFn(ctx, key, instanceID, tokens)
	}
	return nil
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

	granted, err := mgr.Acquire(context.Background(), "key1", 100)
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

	granted, err := mgr.Acquire(context.Background(), "key1", 100)
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
			_, _ = mgr.Acquire(context.Background(), "key1", 10)
		}()
	}

	ready.Wait()
	go2.Done()
	time.Sleep(200 * time.Millisecond)

	if got := atomic.LoadInt32(&mock.borrowN); got != 1 {
		t.Errorf("pool.Borrow called %d times, want 1 (singleflight)", got)
	}
}

func TestManagerReturnAll(t *testing.T) {
	var mu sync.Mutex
	returned := make(map[string][]uint64)

	mock := &mockPool{
		borrowFn: func(_ context.Context, _, _ string, requested uint64, _ int64) (uint64, error) {
			return requested, nil
		},
		returnFn: func(_ context.Context, key, _ string, tokens uint64) error {
			mu.Lock()
			returned[key] = append(returned[key], tokens)
			mu.Unlock()
			return nil
		},
	}

	sizer := NewAdaptiveSizer(0.3, 100, 100, time.Second, false)
	mgr := NewBorrowManager(mock, "inst-1", BorrowManagerOpts{
		Sizer:    sizer,
		LeaseTTL: 30 * time.Second,
	})

	for _, key := range []string{"a", "b", "c"} {
		if _, err := mgr.Acquire(context.Background(), key, 100); err != nil {
			t.Fatalf("Acquire(%s): %v", key, err)
		}
	}

	if err := mgr.ReturnAll(context.Background()); err != nil {
		t.Fatalf("ReturnAll: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, key := range []string{"a", "b", "c"} {
		var total uint64
		for _, v := range returned[key] {
			total += v
		}
		if total != 100 {
			t.Errorf("returned[%s] total = %d, want 100", key, total)
		}
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
