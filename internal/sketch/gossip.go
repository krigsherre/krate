package sketch

import (
	"sort"
	"sync"
	"time"

	"golang.org/x/sys/cpu"
)

type PeerEntry struct {
	ID          string
	Surplus     uint64
	Consumed    uint64
	LastUpdated time.Time
}

const gossipShardCount = 64

type gossipShard struct {
	mu           sync.RWMutex
	peerConsumed map[string]map[string]uint64
	peerBorrowed map[string]map[string]uint64
	peerUpdated  map[string]map[string]time.Time
	lastConsumed map[string]map[string]uint64
	velocityEMA  map[string]map[string]float64
	lastUpdated  map[string]map[string]time.Time
	_pad         cpu.CacheLinePad
}

type Gossiper struct {
	mu     sync.RWMutex
	alpha  float64
	shards [gossipShardCount]gossipShard

	peersMu sync.RWMutex
	peers   map[string]struct{}
}

func NewGossiper() *Gossiper {
	g := &Gossiper{
		alpha: 0.5,
		peers: make(map[string]struct{}),
	}
	for i := 0; i < gossipShardCount; i++ {
		g.shards[i].peerConsumed = make(map[string]map[string]uint64)
		g.shards[i].peerBorrowed = make(map[string]map[string]uint64)
		g.shards[i].peerUpdated = make(map[string]map[string]time.Time)
		g.shards[i].lastConsumed = make(map[string]map[string]uint64)
		g.shards[i].velocityEMA = make(map[string]map[string]float64)
		g.shards[i].lastUpdated = make(map[string]map[string]time.Time)
	}
	return g
}

func (g *Gossiper) SetAlpha(alpha float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if alpha > 0 && alpha <= 1 {
		g.alpha = alpha
	}
}

func (g *Gossiper) shardIndex(key string) int {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h) & (gossipShardCount - 1)
}

func (g *Gossiper) UpdatePeer(id string, consumed map[string]uint64, borrowed map[string]uint64) {
	g.peersMu.Lock()
	g.peers[id] = struct{}{}
	g.peersMu.Unlock()

	type shardKeys struct {
		consumed map[string]uint64
		borrowed map[string]uint64
	}
	updates := make(map[int]*shardKeys)

	for k, v := range consumed {
		idx := g.shardIndex(k)
		su, ok := updates[idx]
		if !ok {
			su = &shardKeys{consumed: make(map[string]uint64), borrowed: make(map[string]uint64)}
			updates[idx] = su
		}
		su.consumed[k] = v
	}

	for k, v := range borrowed {
		idx := g.shardIndex(k)
		su, ok := updates[idx]
		if !ok {
			su = &shardKeys{consumed: make(map[string]uint64), borrowed: make(map[string]uint64)}
			updates[idx] = su
		}
		su.borrowed[k] = v
	}

	now := time.Now()
	g.mu.RLock()
	alpha := g.alpha
	g.mu.RUnlock()
	for idx, su := range updates {
		shard := &g.shards[idx]
		shard.mu.Lock()
		g.updateShardPeerLocked(shard, id, su.consumed, su.borrowed, alpha, now)
		shard.mu.Unlock()
	}
}

func (g *Gossiper) updateShardPeerLocked(shard *gossipShard, id string, consumed map[string]uint64, borrowed map[string]uint64, alpha float64, now time.Time) {
	cpCon, ok := shard.peerConsumed[id]
	if !ok {
		cpCon = make(map[string]uint64)
		shard.peerConsumed[id] = cpCon
	}

	if _, ok := shard.lastConsumed[id]; !ok {
		shard.lastConsumed[id] = make(map[string]uint64)
		shard.velocityEMA[id] = make(map[string]float64)
		shard.lastUpdated[id] = make(map[string]time.Time)
	}

	for k, v := range consumed {
		if v == 0 {
			delete(cpCon, k)
			delete(shard.lastConsumed[id], k)
			delete(shard.velocityEMA[id], k)
			delete(shard.lastUpdated[id], k)
		} else {
			cpCon[k] = v
			lastTime, timeOk := shard.lastUpdated[id][k]
			lastCon, consOk := shard.lastConsumed[id][k]
			if timeOk && consOk {
				dt := now.Sub(lastTime).Seconds()
				if dt > 0 {
					var diff float64
					if v >= lastCon {
						diff = float64(v - lastCon)
					} else {
						diff = float64(v)
					}
					currentVelocity := diff / dt
					prevEMA := shard.velocityEMA[id][k]
					newEMA := (alpha * currentVelocity) + ((1 - alpha) * prevEMA)
					shard.velocityEMA[id][k] = newEMA
				}
				shard.lastUpdated[id][k] = now
			} else {
				shard.lastUpdated[id][k] = now
			}
			shard.lastConsumed[id][k] = v
		}
	}

	cpBor, ok := shard.peerBorrowed[id]
	if !ok {
		cpBor = make(map[string]uint64)
		shard.peerBorrowed[id] = cpBor
	}
	for k, v := range borrowed {
		if v == 0 {
			delete(cpBor, k)
		} else {
			cpBor[k] = v
		}
	}

	cpUpd, ok := shard.peerUpdated[id]
	if !ok {
		cpUpd = make(map[string]time.Time)
		shard.peerUpdated[id] = cpUpd
	}
	for k := range consumed {
		cpUpd[k] = now
	}
	for k := range borrowed {
		cpUpd[k] = now
	}
}

func (g *Gossiper) RemovePeer(id string) {
	g.peersMu.Lock()
	delete(g.peers, id)
	g.peersMu.Unlock()

	for i := 0; i < gossipShardCount; i++ {
		shard := &g.shards[i]
		shard.mu.Lock()
		delete(shard.peerConsumed, id)
		delete(shard.peerBorrowed, id)
		delete(shard.peerUpdated, id)
		delete(shard.lastConsumed, id)
		delete(shard.velocityEMA, id)
		delete(shard.lastUpdated, id)
		shard.mu.Unlock()
	}
}

func (g *Gossiper) TopK(k int, key string) []PeerEntry {
	idx := g.shardIndex(key)
	shard := &g.shards[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	now := time.Now()
	if k <= 15 && k > 0 {
		var buf [16]PeerEntry
		top := buf[:0]
		for id, bm := range shard.peerBorrowed {
			borrowed := bm[key]
			if borrowed == 0 {
				continue
			}
			var consumed uint64
			if cm, ok := shard.peerConsumed[id]; ok {
				consumed = cm[key]
			}
			var rawSurplus uint64
			if borrowed > consumed {
				rawSurplus = borrowed - consumed
			}
			if rawSurplus == 0 {
				continue
			}

			var lastTime time.Time
			if um, ok := shard.peerUpdated[id]; ok {
				lastTime = um[key]
			}

			velocity := shard.velocityEMA[id][key]
			dt := now.Sub(lastTime).Seconds()
			if dt < 0 {
				dt = 0
			}

			expectedDecay := velocity * dt
			var predictedSurplus float64
			if expectedDecay > 0 {
				if float64(rawSurplus) > expectedDecay {
					predictedSurplus = float64(rawSurplus) - expectedDecay
				} else {
					predictedSurplus = 0
				}
			} else {
				predictedSurplus = float64(rawSurplus)
			}

			if predictedSurplus > 0 {
				entry := PeerEntry{
					ID:          id,
					Surplus:     uint64(predictedSurplus),
					Consumed:    consumed,
					LastUpdated: lastTime,
				}
				insertIdx := -1
				for i := 0; i < len(top); i++ {
					if entry.Surplus > top[i].Surplus {
						insertIdx = i
						break
					}
				}

				if insertIdx >= 0 {
					top = append(top, PeerEntry{})
					copy(top[insertIdx+1:], top[insertIdx:])
					top[insertIdx] = entry
				} else if len(top) < k {
					top = append(top, entry)
				}

				if len(top) > k {
					top = top[:k]
				}
			}
		}
		res := make([]PeerEntry, len(top))
		copy(res, top)
		return res
	}

	entries := make([]PeerEntry, 0, len(shard.peerBorrowed))
	for id, bm := range shard.peerBorrowed {
		borrowed := bm[key]
		if borrowed == 0 {
			continue
		}
		var consumed uint64
		if cm, ok := shard.peerConsumed[id]; ok {
			consumed = cm[key]
		}
		var rawSurplus uint64
		if borrowed > consumed {
			rawSurplus = borrowed - consumed
		}
		if rawSurplus == 0 {
			continue
		}

		var lastTime time.Time
		if um, ok := shard.peerUpdated[id]; ok {
			lastTime = um[key]
		}

		velocity := shard.velocityEMA[id][key]
		dt := now.Sub(lastTime).Seconds()
		if dt < 0 {
			dt = 0
		}

		expectedDecay := velocity * dt
		var predictedSurplus float64
		if expectedDecay > 0 {
			if float64(rawSurplus) > expectedDecay {
				predictedSurplus = float64(rawSurplus) - expectedDecay
			} else {
				predictedSurplus = 0
			}
		} else {
			predictedSurplus = float64(rawSurplus)
		}

		if predictedSurplus > 0 {
			entries = append(entries, PeerEntry{
				ID:          id,
				Surplus:     uint64(predictedSurplus),
				Consumed:    consumed,
				LastUpdated: lastTime,
			})
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Surplus > entries[j].Surplus
	})

	if len(entries) > k {
		entries = entries[:k]
	}
	return entries
}

func (g *Gossiper) PeerIDs() []string {
	g.peersMu.RLock()
	defer g.peersMu.RUnlock()
	ids := make([]string, 0, len(g.peers))
	for id := range g.peers {
		ids = append(ids, id)
	}
	return ids
}

func (g *Gossiper) EvictKeys(keys []string) {
	var shardToKeys [gossipShardCount][]string
	for _, k := range keys {
		idx := g.shardIndex(k)
		shardToKeys[idx] = append(shardToKeys[idx], k)
	}

	for idx, shKeys := range shardToKeys {
		if len(shKeys) == 0 {
			continue
		}
		shard := &g.shards[idx]
		shard.mu.Lock()
		for _, key := range shKeys {
			for id := range shard.peerConsumed {
				delete(shard.peerConsumed[id], key)
			}
			for id := range shard.peerBorrowed {
				delete(shard.peerBorrowed[id], key)
			}
			for id := range shard.peerUpdated {
				delete(shard.peerUpdated[id], key)
			}
			for id := range shard.lastConsumed {
				delete(shard.lastConsumed[id], key)
			}
			for id := range shard.velocityEMA {
				delete(shard.velocityEMA[id], key)
			}
			for id := range shard.lastUpdated {
				delete(shard.lastUpdated[id], key)
			}
		}
		shard.mu.Unlock()
	}
}
