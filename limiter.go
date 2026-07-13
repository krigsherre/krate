package krate

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krigsherre/krate/borrow"
	"github.com/krigsherre/krate/cluster"
	"github.com/krigsherre/krate/internal/clock"
	"github.com/krigsherre/krate/metrics"
	"github.com/krigsherre/krate/peer"
	kratev1 "github.com/krigsherre/krate/peer/peerpb"
	"github.com/krigsherre/krate/sketch"
	"github.com/redis/go-redis/v9"
)

const shardCount = 64

type bucketShard struct {
	mu      sync.RWMutex
	buckets map[string]*bucket
	_pad    [24]byte
}

type limiter struct {
	opts       options
	logger     *slog.Logger
	metrics    *metrics.Collector
	clock      Clock
	shards     [shardCount]bucketShard
	pool       *borrow.RedisPool
	borrowMgr  *borrow.BorrowManager
	acquirer   *peer.Acquirer
	mesh       *peer.Mesh
	server     *peer.TokenServer
	gossiper   *sketch.Gossiper
	heartbeat  *cluster.Heartbeat
	membership *cluster.Membership
	localCMS   *sketch.CountMinSketch
	closed     atomic.Bool
	cancel     context.CancelFunc
	wg         sync.WaitGroup
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

	localCMS := sketch.NewCMS(opts.cmsWidth, opts.cmsDepth, opts.cmsSeed)
	gossiper := sketch.NewGossiper()
	membership := cluster.NewMembership(rdb, opts.heartbeatTimeout, logger)

	server := peer.NewTokenServer(opts.instanceID, opts.peerListen, logger)

	ctx, cancel := context.WithCancel(context.Background())

	l := &limiter{
		opts:       opts,
		logger:     logger,
		metrics:    mc,
		clock:      clk,
		pool:       pool,
		borrowMgr:  bm,
		gossiper:   gossiper,
		localCMS:   localCMS,
		server:     server,
		membership: membership,
		cancel:     cancel,
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

	server.SetGossipHandler(func(originID string, cmsState []byte, borrowed map[string]uint64) error {
		var peerCms *sketch.CountMinSketch
		var err error
		if len(cmsState) > 0 {
			peerCms, err = sketch.Unmarshal(cmsState)
			if err != nil {
				return err
			}
		}
		l.gossiper.UpdatePeer(originID, peerCms, borrowed)
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

	mesh := peer.NewMesh(opts.instanceID, membership, opts.gossipInterval, logger)
	mesh.Start(ctx)
	l.mesh = mesh

	l.wg.Add(1)
	go l.gossipLoop(ctx)

	pc := peer.NewPeerClient(opts.probeTimeout, logger)
	probeMode := peer.ProbeSequential
	if opts.probeMode == Parallel {
		probeMode = peer.ProbeParallel
	}
	acq := peer.NewAcquirer(mesh, gossiper, pc, opts.instanceID, opts.probeK, probeMode, logger)
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
	cmsState := l.localCMS.Marshal()
	borrowed := l.borrowMgr.AllBorrowed()

	req := &kratev1.GossipRequest{
		OriginId: l.opts.instanceID,
		CmsState: cmsState,
		Borrowed: borrowed,
	}

	peers := l.mesh.GetPeers()
	for _, p := range peers {
		if !p.Healthy || p.Client == nil {
			continue
		}

		go func(client kratev1.KratePeerServiceClient, addr string) {
			gCtx, cancel := context.WithTimeout(ctx, l.opts.gossipInterval/2)
			defer cancel()

			_, err := client.Gossip(gCtx, req)
			if err != nil {
				l.logger.Debug("failed to gossip to peer", "addr", addr, "error", err)
			}
		}(p.Client, p.GRPCAddr)
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

	if b.tryConsume(n) {
		rem := b.remaining()
		l.localCMS.Add(key, n)
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
			l.localCMS.Add(key, n)
			l.borrowMgr.RecordConsumption(key, n)
			l.metrics.RecordAllowed(key)
			l.metrics.SetLocalTokens(key, rem)
			l.metrics.ObserveLocalTokensRemaining(rem)
			return true, nil
		}
	}

	if b.IsRedisExhausted() {
		l.metrics.RecordRedisSkip(key)
	} else {
		redisStart := l.clock.Now()
		granted, err := l.borrowMgr.Acquire(ctx, key, n)
		l.metrics.ObserveRequestDuration("redis", l.clock.Since(redisStart))

		if err != nil {
			return false, err
		}
		if granted > 0 {
			b.ClearRedisExhausted()
			b.refill(granted)
			l.metrics.RecordRedisBorrow(key, true)
			l.metrics.SetBorrowedTokens(key, l.borrowMgr.Borrowed(key))
			if b.tryConsume(n) {
				rem := b.remaining()
				l.localCMS.Add(key, n)
				l.borrowMgr.RecordConsumption(key, n)
				l.metrics.RecordAllowed(key)
				l.metrics.SetLocalTokens(key, rem)
				l.metrics.ObserveLocalTokensRemaining(rem)
				return true, nil
			}
		} else {
			b.MarkRedisExhausted()
			l.metrics.RecordRedisBorrow(key, false)
		}
	}

	if l.acquirer != nil {
		peerStart := l.clock.Now()
		peerTokens, err := l.acquireFromPeers(ctx, key, n)
		l.metrics.ObserveRequestDuration("peer", l.clock.Since(peerStart))

		if err == nil && peerTokens > 0 {
			b.refill(peerTokens)
			l.metrics.RecordTokenReceived(peerTokens)
			if b.tryConsume(n) {
				rem := b.remaining()
				l.localCMS.Add(key, n)
				l.borrowMgr.RecordConsumption(key, n)
				l.metrics.RecordAllowed(key)
				l.metrics.SetLocalTokens(key, rem)
				l.metrics.ObserveLocalTokensRemaining(rem)
				return true, nil
			}
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
		granted, err := l.borrowMgr.Acquire(bgCtx, key, 0)
		if err != nil {
			l.logger.Debug("async pre-borrow failed", "key", key, "error", err)
			return
		}
		if granted > 0 {
			b.ClearRedisExhausted()
			b.refill(granted)
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
	windowMs := l.opts.window.Milliseconds()
	newWindowStart := (nowMs / windowMs) * windowMs

	if err := l.pool.ResetWindow(ctx, key, l.opts.limit, newWindowStart); err != nil {
		return fmt.Errorf("reset window: %w", err)
	}

	b := l.getOrCreateBucket(key)
	if drained := b.drain(); drained > 0 {
		if err := l.borrowMgr.Return(ctx, key, drained); err != nil {
			l.logger.Warn("return tokens after reset failed", "key", key, "error", err)
		}
	}

	b.ClearRedisExhausted()

	b.window.UpdateWindowStart(newWindowStart)
	l.metrics.RecordWindowReset(key)
	return nil
}

func (l *limiter) acquireFromPeers(ctx context.Context, key string, need uint64) (uint64, error) {
	peerTokens, err := l.acquirer.Acquire(ctx, key, need)
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

	if err := l.borrowMgr.ReturnAll(ctx); err != nil {
		l.logger.Warn("return tokens failed", "error", err)
	}

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
