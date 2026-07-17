package borrow

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"golang.org/x/sys/cpu"

	"github.com/krigsherre/krate/internal/clock"
)

type PoolClient interface {
	Borrow(ctx context.Context, key, instanceID string, requested uint64, leaseTTLms int64) (uint64, error)
}

type BorrowManagerOpts struct {
	Sizer    *AdaptiveSizer
	LeaseTTL time.Duration
	Clock    clock.Clock
	Logger   *slog.Logger
	OnBorrow func(key string, granted uint64)
}

type keyState struct {
	borrowed   uint64
	lastBorrow time.Time
}

const managerShardCount = 64

type managerShard struct {
	mu         sync.Mutex
	activeKeys map[string]*keyState
	_pad       cpu.CacheLinePad
}

type BorrowManager struct {
	pool       PoolClient
	sizer      *AdaptiveSizer
	instanceID string
	leaseTTL   time.Duration
	leaseTTLms int64

	shards   [managerShardCount]managerShard
	sfShards [managerShardCount]singleflight.Group

	logger   *slog.Logger
	clock    clock.Clock
	onBorrow func(key string, granted uint64)
}

func NewBorrowManager(pool PoolClient, instanceID string, opts BorrowManagerOpts) *BorrowManager {
	m := &BorrowManager{
		pool:       pool,
		sizer:      opts.Sizer,
		instanceID: instanceID,
		leaseTTL:   opts.LeaseTTL,
		leaseTTLms: opts.LeaseTTL.Milliseconds(),
		logger:     opts.Logger,
		clock:      opts.Clock,
		onBorrow:   opts.OnBorrow,
	}
	for i := 0; i < managerShardCount; i++ {
		m.shards[i].activeKeys = make(map[string]*keyState)
	}
	if m.logger == nil {
		m.logger = slog.Default()
	}
	if m.clock == nil {
		m.clock = clock.NewRealClock()
	}
	return m
}

func (m *BorrowManager) shardIndex(key string) int {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h) & (managerShardCount - 1)
}

func (m *BorrowManager) Acquire(ctx context.Context, key string, need uint64, onBorrow func(uint64)) (uint64, error) {
	shardIdx := m.shardIndex(key)
	v, err, _ := m.sfShards[shardIdx].Do(key, func() (interface{}, error) {
		borrowSize := m.sizer.ComputeBorrowSize()
		if need > borrowSize {
			borrowSize = need
		}

		granted, err := m.pool.Borrow(ctx, key, m.instanceID, borrowSize, m.leaseTTLms)
		if err != nil {
			return uint64(0), err
		}
		if granted > 0 {
			if onBorrow != nil {
				onBorrow(granted)
			}

			shard := &m.shards[shardIdx]
			shard.mu.Lock()
			ks, ok := shard.activeKeys[key]
			if !ok {
				ks = &keyState{}
				shard.activeKeys[key] = ks
			} else if ks.borrowed > 0 && m.clock.Now().Sub(ks.lastBorrow) > m.leaseTTL {
				ks.borrowed = 0
			}
			ks.borrowed += granted
			ks.lastBorrow = m.clock.Now()
			shard.mu.Unlock()
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

	m.sizer.Record(granted)

	if m.onBorrow != nil {
		m.onBorrow(key, granted)
	}

	return granted, nil
}

func (m *BorrowManager) RecordConsumption(key string, consumed uint64) {
	m.sizer.Record(consumed)
}

func (m *BorrowManager) Borrowed(key string) uint64 {
	shardIdx := m.shardIndex(key)
	shard := &m.shards[shardIdx]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if ks, ok := shard.activeKeys[key]; ok {
		if ks.borrowed > 0 && m.clock.Now().Sub(ks.lastBorrow) > m.leaseTTL {
			delete(shard.activeKeys, key)
			return 0
		}
		return ks.borrowed
	}
	return 0
}

func (m *BorrowManager) AllBorrowed(maxKeys int) map[string]uint64 {
	now := m.clock.Now()
	var active []keyVal

	for i := 0; i < managerShardCount; i++ {
		shard := &m.shards[i]
		shard.mu.Lock()
		for k, ks := range shard.activeKeys {
			if ks.borrowed > 0 && now.Sub(ks.lastBorrow) > m.leaseTTL {
				delete(shard.activeKeys, k)
				continue
			}
			if ks.borrowed > 0 {
				active = append(active, keyVal{key: k, val: ks.borrowed})
			}
		}
		shard.mu.Unlock()
	}

	if maxKeys > 0 && len(active) > maxKeys {
		sort.Slice(active, func(i, j int) bool {
			return active[i].val > active[j].val
		})
		active = active[:maxKeys]
	}

	out := make(map[string]uint64, len(active))
	for _, kv := range active {
		out[kv.key] = kv.val
	}
	return out
}

func (m *BorrowManager) Reset(key string) {
	shardIdx := m.shardIndex(key)
	shard := &m.shards[shardIdx]
	shard.mu.Lock()
	if ks, ok := shard.activeKeys[key]; ok {
		ks.borrowed = 0
	}
	shard.mu.Unlock()
}

func (m *BorrowManager) EvictKeys(keys []string) {
	var shardToKeys [managerShardCount][]string
	for _, k := range keys {
		idx := m.shardIndex(k)
		shardToKeys[idx] = append(shardToKeys[idx], k)
	}

	for idx, shKeys := range shardToKeys {
		if len(shKeys) == 0 {
			continue
		}
		shard := &m.shards[idx]
		shard.mu.Lock()
		for _, k := range shKeys {
			delete(shard.activeKeys, k)
		}
		shard.mu.Unlock()
	}
}

type keyVal struct {
	key string
	val uint64
}
