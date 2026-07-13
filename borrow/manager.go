package borrow

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/krigsherre/krate/internal/clock"
)

type PoolClient interface {
	Borrow(ctx context.Context, key, instanceID string, requested uint64, leaseTTLms int64) (uint64, error)
	Return(ctx context.Context, key, instanceID string, tokens uint64) error
}

type BorrowManagerOpts struct {
	Sizer    *AdaptiveSizer
	LeaseTTL time.Duration
	Clock    clock.Clock
	Logger   *slog.Logger
	OnBorrow func(key string, granted uint64)
	OnReturn func(key string, tokens uint64)
}

type keyState struct {
	borrowed   uint64
	lastBorrow time.Time
}

type BorrowManager struct {
	pool       PoolClient
	sizer      *AdaptiveSizer
	instanceID string
	leaseTTL   time.Duration
	leaseTTLms int64

	mu         sync.Mutex
	activeKeys map[string]*keyState
	sf         singleflight.Group

	logger   *slog.Logger
	clock    clock.Clock
	onBorrow func(key string, granted uint64)
	onReturn func(key string, tokens uint64)
}

func NewBorrowManager(pool PoolClient, instanceID string, opts BorrowManagerOpts) *BorrowManager {
	m := &BorrowManager{
		pool:       pool,
		sizer:      opts.Sizer,
		instanceID: instanceID,
		leaseTTL:   opts.LeaseTTL,
		leaseTTLms: opts.LeaseTTL.Milliseconds(),
		activeKeys: make(map[string]*keyState),
		logger:     opts.Logger,
		clock:      opts.Clock,
		onBorrow:   opts.OnBorrow,
		onReturn:   opts.OnReturn,
	}
	if m.logger == nil {
		m.logger = slog.Default()
	}
	if m.clock == nil {
		m.clock = clock.NewRealClock()
	}
	return m
}

func (m *BorrowManager) Acquire(ctx context.Context, key string, need uint64) (uint64, error) {
	v, err, _ := m.sf.Do(key, func() (interface{}, error) {
		borrowSize := m.sizer.ComputeBorrowSize()
		if need > borrowSize {
			borrowSize = need
		}

		granted, err := m.pool.Borrow(ctx, key, m.instanceID, borrowSize, m.leaseTTLms)
		if err != nil {
			return uint64(0), err
		}
		return granted, nil
	})

	if err != nil {
		return 0, err
	}

	granted := v.(uint64)
	if granted == 0 {
		return 0, nil
	}

	m.mu.Lock()
	ks, ok := m.activeKeys[key]
	if !ok {
		ks = &keyState{}
		m.activeKeys[key] = ks
	}
	ks.borrowed += granted
	ks.lastBorrow = m.clock.Now()
	m.mu.Unlock()

	m.sizer.Record(granted)

	if m.onBorrow != nil {
		m.onBorrow(key, granted)
	}

	return granted, nil
}

func (m *BorrowManager) Return(ctx context.Context, key string, tokens uint64) error {
	err := m.pool.Return(ctx, key, m.instanceID, tokens)
	if err != nil {
		return err
	}

	m.mu.Lock()
	if ks, ok := m.activeKeys[key]; ok {
		if tokens > ks.borrowed {
			ks.borrowed = 0
		} else {
			ks.borrowed -= tokens
		}
	}
	m.mu.Unlock()

	if m.onReturn != nil {
		m.onReturn(key, tokens)
	}

	return nil
}

func (m *BorrowManager) ReturnAll(ctx context.Context) error {
	m.mu.Lock()
	keys := make(map[string]uint64, len(m.activeKeys))
	for k, ks := range m.activeKeys {
		keys[k] = ks.borrowed
	}
	m.mu.Unlock()

	var firstErr error
	for key, tokens := range keys {
		if tokens == 0 {
			continue
		}
		if err := m.Return(ctx, key, tokens); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *BorrowManager) RecordConsumption(key string, consumed uint64) {
	m.sizer.Record(consumed)
}

func (m *BorrowManager) Borrowed(key string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ks, ok := m.activeKeys[key]; ok {
		return ks.borrowed
	}
	return 0
}

func (m *BorrowManager) AllBorrowed() map[string]uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]uint64, len(m.activeKeys))
	for k, ks := range m.activeKeys {
		if ks.borrowed > 0 {
			out[k] = ks.borrowed
		}
	}
	return out
}
