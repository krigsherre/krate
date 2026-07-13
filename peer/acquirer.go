package peer

import (
	"context"
	"log/slog"
	"sync"

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

type Acquirer struct {
	mesh     peerLookup
	gossiper peerRanker
	client   tokenRequester
	originID string
	probeK   int
	mode     int
	logger   *slog.Logger

	sf            singleflight.Group
	onProbeResult func(key, peerID, result string)
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

func (a *Acquirer) Acquire(ctx context.Context, key string, need uint64) (uint64, error) {
	v, err, _ := a.sf.Do(key, func() (interface{}, error) {
		return a.acquire(ctx, key, need)
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

	var (
		mu      sync.Mutex
		granted uint64
		found   bool
		wg      sync.WaitGroup
	)

	for _, c := range candidates {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := a.probe(ctx, c.ID, key, need, c.Surplus)
			if err != nil {
				return
			}
			if got > 0 {
				mu.Lock()
				if !found {
					granted = got
					found = true
					cancel()
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if !found {
		return 0, nil
	}
	return granted, nil
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
