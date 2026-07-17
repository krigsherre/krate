package krate

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/cpu"

	"github.com/krigsherre/krate/internal/borrow"
	"github.com/krigsherre/krate/internal/cluster"
	"github.com/krigsherre/krate/internal/clock"
	"github.com/krigsherre/krate/internal/metrics"
	"github.com/krigsherre/krate/internal/peer"
	kratev1 "github.com/krigsherre/krate/internal/peer/peerpb"
	"github.com/krigsherre/krate/routing"
	"github.com/krigsherre/krate/internal/sketch"
	"github.com/redis/go-redis/v9"
)

const shardCount = 64

type bucketShard struct {
	mu      sync.RWMutex
	buckets map[string]*bucket
	_pad    cpu.CacheLinePad
}

type peerState struct {
	mu       sync.Mutex
	borrowed map[string]uint64
	consumed map[string]uint64
}

func newPeerState() *peerState {
	return &peerState{
		borrowed: make(map[string]uint64),
		consumed: make(map[string]uint64),
	}
}

type limiter struct {
	opts             options
	logger           *slog.Logger
	metrics          *metrics.Collector
	clock            Clock
	shards           [shardCount]bucketShard
	pool             *borrow.RedisPool
	borrowMgr        *borrow.BorrowManager
	acquirer         *peer.Acquirer
	mesh             *peer.Mesh
	server           *peer.TokenServer
	gossiper         *sketch.Gossiper
	heartbeat        *cluster.Heartbeat
	membership       *cluster.Membership
	router           routing.Router
	peerStates       sync.Map

	closed           atomic.Bool
	cancel           context.CancelFunc
	wg               sync.WaitGroup
}

func newLimiter(rdb redis.UniversalClient, opts options) (*limiter, error) {
	logger := opts.logger

	var clk Clock = clock.NewRealClock()
	if opts.clock != nil {
		clk = opts.clock
	}

	var mc *metrics.Collector
	if opts.metrics != nil {
		mc = metrics.NewCollector(opts.metrics)
	}

	pool := borrow.NewRedisPool(rdb)

	sizer := borrow.NewAdaptiveSizer(
		opts.emaAlpha,
		opts.minBorrow,
		opts.maxBorrow,
		opts.window,
		opts.adaptiveBorrow,
	)

	bm := borrow.NewBorrowManager(pool, opts.instanceID, borrow.BorrowManagerOpts{
		Sizer:    sizer,
		LeaseTTL: opts.leaseTTL,
		Logger:   logger,
	})

	gossiper := sketch.NewGossiper()
	membership := cluster.NewMembership(rdb, opts.heartbeatTimeout, logger)

	server := peer.NewTokenServer(opts.instanceID, opts.peerListen, logger)

	ctx, cancel := context.WithCancel(context.Background())

	l := &limiter{
		opts:             opts,
		logger:           logger,
		metrics:          mc,
		clock:            clk,
		pool:             pool,
		borrowMgr:        bm,
		gossiper:         gossiper,
		server:           server,
		membership:       membership,
		router:           opts.router,
		cancel:           cancel,
	}

	if l.router != nil {
		l.router.Init(l.gossiper, logger)
	}

	for i := range l.shards {
		l.shards[i].buckets = make(map[string]*bucket)
	}

	server.SetTokenAccessor(func(key string, requested uint64) (uint64, error) {
		b := l.getOrCreateBucket(key)
		remaining := b.remaining()
		reserved := uint64(float64(opts.limit) * opts.reservedMinimum)
		if remaining <= reserved {
			return 0, nil
		}
		return remaining - reserved, nil
	})

	server.SetTokenTransferer(func(key string, amount uint64) (uint64, error) {
		b := l.getOrCreateBucket(key)
		remaining := b.remaining()
		transfer := amount
		if transfer > remaining {
			transfer = remaining
		}
		if transfer == 0 {
			return 0, nil
		}
		if b.tryConsume(transfer) {
			return transfer, nil
		}
		return 0, nil
	})

	server.SetGossipHandler(func(originID string, consumed map[string]uint64, borrowed map[string]uint64) error {
		l.gossiper.UpdatePeer(originID, consumed, borrowed)
		return nil
	})

	if err := server.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start token server: %w", err)
	}

	actualAddr := server.Addr()
	gossipAddr := opts.gossipAddr
	if gossipAddr == "" {
		gossipAddr = actualAddr
	}

	info := cluster.MemberInfo{
		ID:         opts.instanceID,
		GossipAddr: gossipAddr,
		GRPCAddr:   actualAddr,
	}

	hb := cluster.NewHeartbeat(membership, info, opts.heartbeatInterval, logger)
	hb.Start(ctx)
	l.heartbeat = hb

	mesh := peer.NewMesh(opts.instanceID, membership, opts.gossipInterval, opts.gzipCompression, logger)
	mesh.SetPeerRemovedCallback(func(peerID string) {
		l.gossiper.RemovePeer(peerID)
		if l.router != nil {
			if pr, ok := l.router.(routing.PeerAwareRouter); ok {
				pr.RemovePeer(peerID)
			}
		}
	})
	mesh.Start(ctx)
	l.mesh = mesh

	l.wg.Add(1)
	go l.gossipLoop(ctx)

	if opts.evictionInterval > 0 && opts.evictionPolicy != nil {
		jan := NewJanitor(opts.evictionInterval, func(nowMs int64) {
			l.evictInactiveKeys(opts.evictionPolicy, nowMs)
		})
		jan.Start(ctx, l.clock)
	}

	pc := peer.NewPeerClient(opts.probeTimeout, logger)
	probeMode := peer.ProbeSequential
	if opts.probeMode == Parallel {
		probeMode = peer.ProbeParallel
	}
	acq := peer.NewAcquirer(mesh, gossiper, pc, opts.instanceID, opts.probeK, probeMode, logger)
	acq.SetExtraTokensCallback(func(key string, tokens uint64) {
		b := l.getOrCreateBucket(key)
		b.refill(tokens)
		if l.metrics != nil {
			l.metrics.RecordTokenReceived(tokens)
		}
	})
	if l.metrics != nil {
		acq.SetProbeResultCallback(func(key, peerID, result string) {
			l.metrics.RecordPeerProbe(key, result)
			if result == "stale" {
				l.metrics.RecordPeerStale(key, peerID)
			}
		})
	}
	l.acquirer = acq

	l.logger.Info("limiter started",
		"id", opts.instanceID,
		"addr", actualAddr,
		"limit", opts.limit,
		"window", opts.window,
	)

	return l, nil
}

func (l *limiter) gossipLoop(ctx context.Context) {
	defer l.wg.Done()

	interval := l.opts.gossipInterval
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}

	ticker := l.clock.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.gossipOnce(ctx)
		}
	}
}

func (l *limiter) gossipOnce(ctx context.Context) {
	currentBorrowed := l.borrowMgr.AllBorrowed(l.opts.maxGossipKeys)
	keys := make([]string, 0, len(currentBorrowed))
	for k := range currentBorrowed {
		keys = append(keys, k)
	}
	currentConsumed := l.getBucketsConsumed(keys)

	peers := l.mesh.GetPeers()
	for _, p := range peers {
		if !p.Healthy || p.Client == nil {
			continue
		}

		peerID := p.ID
		client := p.Client
		addr := p.GRPCAddr

		psVal, _ := l.peerStates.LoadOrStore(peerID, newPeerState())
		ps := psVal.(*peerState)

		ps.mu.Lock()
		deltaBorrowed := make(map[string]uint64)
		for k, v := range currentBorrowed {
			if ps.borrowed[k] != v {
				deltaBorrowed[k] = v
			}
		}
		for k := range ps.borrowed {
			if _, exists := currentBorrowed[k]; !exists {
				deltaBorrowed[k] = 0
			}
		}

		deltaConsumed := make(map[string]uint64)
		for k, v := range currentConsumed {
			if ps.consumed[k] != v {
				deltaConsumed[k] = v
			}
		}
		for k := range ps.consumed {
			if _, exists := currentConsumed[k]; !exists {
				deltaConsumed[k] = 0
			}
		}
		ps.mu.Unlock()

		if len(deltaBorrowed) == 0 && len(currentBorrowed) == 0 &&
			len(deltaConsumed) == 0 && len(currentConsumed) == 0 {
			continue
		}

		req := &kratev1.GossipRequest{
			OriginId: l.opts.instanceID,
			Consumed: deltaConsumed,
			Borrowed: deltaBorrowed,
		}

		go func(pid string, db, dc map[string]uint64) {
			gCtx, cancel := context.WithTimeout(ctx, l.opts.gossipInterval/2)
			defer cancel()

			_, err := client.Gossip(gCtx, req)
			if err != nil {
				l.logger.Debug("failed to gossip to peer", "addr", addr, "error", err)
				return
			}

			psVal, ok := l.peerStates.Load(pid)
			if !ok {
				return
			}
			ps := psVal.(*peerState)

			ps.mu.Lock()
			defer ps.mu.Unlock()

			for k, v := range db {
				if v == 0 {
					delete(ps.borrowed, k)
				} else {
					ps.borrowed[k] = v
				}
			}

			for k, v := range dc {
				if v == 0 {
					delete(ps.consumed, k)
				} else {
					ps.consumed[k] = v
				}
			}
		}(peerID, deltaBorrowed, deltaConsumed)
	}
}

func (l *limiter) Allow(ctx context.Context, key string) (bool, error) {
	return l.AllowN(ctx, key, 1)
}

func (l *limiter) AllowN(ctx context.Context, key string, n uint64) (bool, error) {
	if l.closed.Load() {
		return false, ErrClosed
	}

	start := l.clock.Now()
	b := l.getOrCreateBucket(key)
	b.updateAccess(start.UnixMilli())

	if b.tryConsume(n) {
		rem := b.remaining()
		b.recordConsumption(n)
		l.borrowMgr.RecordConsumption(key, n)
		l.metrics.RecordLocalHit(key)
		l.metrics.RecordAllowed(key)
		l.metrics.SetLocalTokens(key, rem)
		l.metrics.ObserveLocalTokensRemaining(rem)
		l.metrics.ObserveRequestDuration("local", l.clock.Since(start))

		if l.opts.preBorrowEnabled && b.belowPreBorrowThreshold(l.opts.minBorrow, l.opts.preBorrowThreshold) {
			l.maybePreBorrow(ctx, key, b)
		}

		return true, nil
	}

	nowMs := l.clock.Now().UnixMilli()
	if b.needsReset(nowMs) {
		if err := l.resetWindow(ctx, key, nowMs); err != nil {
			l.logger.Warn("window reset failed", "key", key, "error", err)
		}
		if b.tryConsume(n) {
			rem := b.remaining()
			b.recordConsumption(n)
			l.borrowMgr.RecordConsumption(key, n)
			l.metrics.RecordAllowed(key)
			l.metrics.SetLocalTokens(key, rem)
			l.metrics.ObserveLocalTokensRemaining(rem)
			return true, nil
		}
	}

	redisExhausted := b.IsRedisExhausted()
	if redisExhausted {
		l.metrics.RecordRedisSkip(key)
	}
	peerChecked := false

	for {
		rc := &routing.RouteContext{
			Key:            key,
			Need:           n,
			RedisExhausted: redisExhausted,
			HasPeers:       l.acquirer != nil && !peerChecked,
		}

		decision, err := l.router.Decide(ctx, rc)
		if err != nil {
			return false, err
		}

		if decision == routing.DecisionRedis {
			redisStart := l.clock.Now()
			granted, err := l.borrowMgr.Acquire(ctx, key, n, func(g uint64) {
				b.ClearRedisExhausted()
				b.refill(g)
			})
			l.metrics.ObserveRequestDuration("redis", l.clock.Since(redisStart))

			if err != nil {
				return false, err
			}
			if granted > 0 {
				l.metrics.RecordRedisBorrow(key, true)
				l.metrics.SetBorrowedTokens(key, l.borrowMgr.Borrowed(key))
				if b.tryConsume(n) {
					rem := b.remaining()
					b.recordConsumption(n)
					l.borrowMgr.RecordConsumption(key, n)
					l.metrics.RecordAllowed(key)
					l.metrics.SetLocalTokens(key, rem)
					l.metrics.ObserveLocalTokensRemaining(rem)
					return true, nil
				}
			} else {
				b.MarkRedisExhausted()
				l.metrics.RecordRedisBorrow(key, false)
				redisExhausted = true
			}
		} else if decision == routing.DecisionPeer {
			peerChecked = true
			peerStart := l.clock.Now()
			peerTokens, err := l.acquireFromPeers(ctx, key, n, func(g uint64) {
				b.refill(g)
				l.metrics.RecordTokenReceived(g)
			})
			l.metrics.ObserveRequestDuration("peer", l.clock.Since(peerStart))

			if err == nil && peerTokens > 0 {
				if b.tryConsume(n) {
					rem := b.remaining()
					b.recordConsumption(n)
					l.borrowMgr.RecordConsumption(key, n)
					l.metrics.RecordAllowed(key)
					l.metrics.SetLocalTokens(key, rem)
					l.metrics.ObserveLocalTokensRemaining(rem)
					return true, nil
				}
			}
		} else {
			break
		}
	}

	l.metrics.RecordRejected(key)
	l.metrics.ObserveLocalTokensRemaining(b.remaining())
	return false, nil
}

func (l *limiter) maybePreBorrow(ctx context.Context, key string, b *bucket) {
	if !b.TryMarkPreBorrowing() {
		return
	}

	l.metrics.RecordPreBorrow(key)

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		defer b.ClearPreBorrowing()
		bgCtx := context.Background()
		granted, err := l.borrowMgr.Acquire(bgCtx, key, 0, func(g uint64) {
			b.ClearRedisExhausted()
			b.refill(g)
		})
		if err != nil {
			l.logger.Debug("async pre-borrow failed", "key", key, "error", err)
			return
		}
		if granted > 0 {
			l.metrics.RecordRedisBorrow(key, true)
			l.logger.Debug("async pre-borrow succeeded", "key", key, "granted", granted)
		}
	}()
}

func shardIndex(key string) int {
	const (
		offset32 uint32 = 2166136261
		prime32  uint32 = 16777619
	)
	h := offset32
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return int(h) & (shardCount - 1)
}

func (l *limiter) getOrCreateBucket(key string) *bucket {
	s := &l.shards[shardIndex(key)]

	s.mu.RLock()
	b, ok := s.buckets[key]
	s.mu.RUnlock()
	if ok {
		return b
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok = s.buckets[key]; ok {
		return b
	}
	b = newBucket(key, l.opts.limit, l.opts.windowType, l.opts.window)
	s.buckets[key] = b
	return b
}

func (l *limiter) resetWindow(ctx context.Context, key string, nowMs int64) error {
	b := l.getOrCreateBucket(key)
	b.mu.Lock()
	defer b.mu.Unlock()

	windowMs := l.opts.window.Milliseconds()
	newWindowStart := (nowMs / windowMs) * windowMs

	if b.window.WindowStartMs() >= newWindowStart {
		return nil
	}

	if err := l.pool.ResetWindow(ctx, key, l.opts.limit, newWindowStart); err != nil {
		return fmt.Errorf("reset window: %w", err)
	}

	b.drain()
	b.ClearRedisExhausted()
	b.resetConsumed()

	b.window.UpdateWindowStart(newWindowStart)
	l.metrics.RecordWindowReset(key)
	l.borrowMgr.Reset(key)
	return nil
}

func (l *limiter) acquireFromPeers(ctx context.Context, key string, need uint64, onAcquire func(uint64)) (uint64, error) {
	peerTokens, err := l.acquirer.Acquire(ctx, key, need, onAcquire)
	if err != nil {
		l.logger.Warn("peer acquisition error", "key", key, "error", err)
		return 0, err
	}
	if peerTokens > 0 {
		l.logger.Debug("peer tokens acquired", "key", key, "tokens", peerTokens)
	}
	return peerTokens, nil
}

func (l *limiter) Close() error {
	if l.closed.Swap(true) {
		return ErrClosed
	}

	l.cancel()
	l.wg.Wait()

	ctx := context.Background()



	if l.heartbeat != nil {
		l.heartbeat.Stop()
	}
	if l.membership != nil {
		if err := l.membership.Deregister(ctx, l.opts.instanceID); err != nil {
			l.logger.Warn("deregister failed", "error", err)
		}
	}
	if l.mesh != nil {
		l.mesh.Stop()
	}
	if l.server != nil {
		l.server.Stop()
	}

	l.logger.Info("limiter closed", "id", l.opts.instanceID)
	return nil
}

func (l *limiter) getBucketConsumed(key string) uint64 {
	s := &l.shards[shardIndex(key)]
	s.mu.RLock()
	defer s.mu.RUnlock()
	if b, ok := s.buckets[key]; ok {
		return b.getConsumed()
	}
	return 0
}

func (l *limiter) getBucketsConsumed(keys []string) map[string]uint64 {
	var shardToKeys [shardCount][]string
	for _, k := range keys {
		idx := shardIndex(k)
		shardToKeys[idx] = append(shardToKeys[idx], k)
	}

	out := make(map[string]uint64, len(keys))

	for idx, shKeys := range shardToKeys {
		if len(shKeys) == 0 {
			continue
		}
		s := &l.shards[idx]
		s.mu.RLock()
		for _, k := range shKeys {
			if b, ok := s.buckets[k]; ok {
				if val := b.getConsumed(); val > 0 {
					out[k] = val
				}
			}
		}
		s.mu.RUnlock()
	}

	return out
}

func (l *limiter) evictInactiveKeys(policy EvictionPolicy, nowMs int64) {
	var allEvicted []string

	for i := range l.shards {
		shard := &l.shards[i]

		shard.mu.RLock()
		var keysToEvict []string
		for key, b := range shard.buckets {
			if policy.ShouldEvict(b, nowMs) && !b.isPreBorrowing() {
				keysToEvict = append(keysToEvict, key)
			}
		}
		shard.mu.RUnlock()

		if len(keysToEvict) == 0 {
			continue
		}

		shard.mu.Lock()
		for _, key := range keysToEvict {
			if b, ok := shard.buckets[key]; ok && policy.ShouldEvict(b, nowMs) && !b.isPreBorrowing() {
				delete(shard.buckets, key)
				allEvicted = append(allEvicted, key)
			}
		}
		shard.mu.Unlock()
	}

	if len(allEvicted) == 0 {
		return
	}

	l.borrowMgr.EvictKeys(allEvicted)
	l.gossiper.EvictKeys(allEvicted)
	if l.router != nil {
		if er, ok := l.router.(routing.EvictionAwareRouter); ok {
			er.EvictKeys(allEvicted)
		}
	}
	l.logger.Debug("evicted inactive rate limit keys in batch", "count", len(allEvicted))
}
