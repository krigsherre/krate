package peer

import (
	"context"
	"log/slog"

	"golang.org/x/sync/singleflight"

	kratev1 "github.com/krigsherre/krate/peer/peerpb"
	"github.com/krigsherre/krate/sketch"
)

const (
	ProbeParallel   = 0
	ProbeSequential = 1
)

type peerLookup interface {
	GetPeerClient(id string) (kratev1.KratePeerServiceClient, bool)
}

type peerRanker interface {
	TopK(k int, key string) []sketch.PeerEntry
}

type tokenRequester interface {
	RequestTokens(ctx context.Context, client kratev1.KratePeerServiceClient, originID, limitKey string, need uint64) (uint64, error)
}

const acquirerSfShardCount = 64

type Acquirer struct {
	mesh     peerLookup
	gossiper peerRanker
	client   tokenRequester
	originID string
	probeK   int
	mode     int
	logger   *slog.Logger

	sfShards      [acquirerSfShardCount]singleflight.Group
	onProbeResult func(key, peerID, result string)
	onExtraTokens func(key string, tokens uint64)
}

func NewAcquirer(mesh peerLookup, gossiper peerRanker, client tokenRequester, originID string, probeK int, mode int, logger *slog.Logger) *Acquirer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Acquirer{
		mesh:     mesh,
		gossiper: gossiper,
		client:   client,
		originID: originID,
		probeK:   probeK,
		mode:     mode,
		logger:   logger,
	}
}

func (a *Acquirer) SetProbeResultCallback(fn func(key, peerID, result string)) {
	a.onProbeResult = fn
}

func (a *Acquirer) SetExtraTokensCallback(fn func(key string, tokens uint64)) {
	a.onExtraTokens = fn
}

func (a *Acquirer) shardIndex(key string) int {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h) & (acquirerSfShardCount - 1)
}

func (a *Acquirer) Acquire(ctx context.Context, key string, need uint64, onAcquire func(uint64)) (uint64, error) {
	shardIdx := a.shardIndex(key)
	v, err, _ := a.sfShards[shardIdx].Do(key, func() (interface{}, error) {
		granted, err := a.acquire(ctx, key, need)
		if err == nil && granted > 0 && onAcquire != nil {
			onAcquire(granted)
		}
		return granted, err
	})
	if err != nil {
		return 0, err
	}
	return v.(uint64), nil
}

func (a *Acquirer) acquire(ctx context.Context, key string, need uint64) (uint64, error) {
	candidates := a.gossiper.TopK(a.probeK, key)
	if len(candidates) == 0 {
		return 0, nil
	}

	switch a.mode {
	case ProbeParallel:
		return a.probeParallel(ctx, candidates, key, need)
	default:
		return a.probeSequential(ctx, candidates, key, need)
	}
}

func (a *Acquirer) probeParallel(ctx context.Context, candidates []sketch.PeerEntry, key string, need uint64) (uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type probeRes struct {
		got uint64
		err error
	}
	resChan := make(chan probeRes, len(candidates))

	for _, c := range candidates {
		c := c
		go func() {
			got, err := a.probe(ctx, c.ID, key, need, c.Surplus)
			resChan <- probeRes{got: got, err: err}
		}()
	}

	var (
		granted uint64
		active  = len(candidates)
	)

	for active > 0 {
		select {
		case res := <-resChan:
			active--
			if res.err == nil && res.got > 0 {
				granted = res.got
				cancel()
				go func(rem int) {
					for i := 0; i < rem; i++ {
						r := <-resChan
						if r.err == nil && r.got > 0 {
							if a.onExtraTokens != nil {
								a.onExtraTokens(key, r.got)
							}
						}
					}
				}(active)

				return granted, nil
			}
		case <-ctx.Done():
			go func(rem int) {
				for i := 0; i < rem; i++ {
					r := <-resChan
					if r.err == nil && r.got > 0 {
						if a.onExtraTokens != nil {
							a.onExtraTokens(key, r.got)
						}
					}
				}
			}(active)
			return 0, ctx.Err()
		}
	}

	return 0, nil
}

func (a *Acquirer) probeSequential(ctx context.Context, candidates []sketch.PeerEntry, key string, need uint64) (uint64, error) {
	for _, c := range candidates {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		got, err := a.probe(ctx, c.ID, key, need, c.Surplus)
		if err != nil {
			a.logger.Debug("peer probe error", "peer", c.ID, "error", err)
			continue
		}
		if got > 0 {
			return got, nil
		}
	}
	return 0, nil
}

func (a *Acquirer) probe(ctx context.Context, peerID, key string, need, estimatedSurplus uint64) (uint64, error) {
	peerClient, ok := a.mesh.GetPeerClient(peerID)
	if !ok {
		if estimatedSurplus > 0 {
			a.emitProbeResult(key, peerID, "stale")
		}
		return 0, nil
	}

	granted, err := a.client.RequestTokens(ctx, peerClient, a.originID, key, need)
	if err != nil {
		a.emitProbeResult(key, peerID, "error")
		return 0, err
	}

	if granted > 0 {
		a.emitProbeResult(key, peerID, "granted")
		return granted, nil
	}

	result := "exhausted"
	if estimatedSurplus > 0 {
		result = "stale"
	}
	a.emitProbeResult(key, peerID, result)
	return 0, nil
}

func (a *Acquirer) emitProbeResult(key, peerID, result string) {
	if a.onProbeResult != nil {
		a.onProbeResult(key, peerID, result)
	}
}

func (a *Acquirer) ProbeK() int {
	return a.probeK
}
