package sketch

import (
	"sort"
	"sync"
	"time"
)

type PeerEntry struct {
	ID          string
	Surplus     uint64
	Consumed    uint64
	LastUpdated time.Time
}

type Gossiper struct {
	mu           sync.RWMutex
	peerConsumed map[string]map[string]uint64
	peerBorrowed map[string]map[string]uint64
	peerUpdated  map[string]map[string]time.Time
	lastConsumed map[string]map[string]uint64
	velocityEMA  map[string]map[string]float64
	lastUpdated  map[string]map[string]time.Time
	alpha        float64
}

func NewGossiper() *Gossiper {
	return &Gossiper{
		peerConsumed: make(map[string]map[string]uint64),
		peerBorrowed: make(map[string]map[string]uint64),
		peerUpdated:  make(map[string]map[string]time.Time),
		lastConsumed: make(map[string]map[string]uint64),
		velocityEMA:  make(map[string]map[string]float64),
		lastUpdated:  make(map[string]map[string]time.Time),
		alpha:        0.5,
	}
}

func (g *Gossiper) SetAlpha(alpha float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if alpha > 0 && alpha <= 1 {
		g.alpha = alpha
	}
}

func (g *Gossiper) UpdatePeer(id string, consumed map[string]uint64, borrowed map[string]uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	cpCon, ok := g.peerConsumed[id]
	if !ok {
		cpCon = make(map[string]uint64)
		g.peerConsumed[id] = cpCon
	}

	if _, ok := g.lastConsumed[id]; !ok {
		g.lastConsumed[id] = make(map[string]uint64)
		g.velocityEMA[id] = make(map[string]float64)
		g.lastUpdated[id] = make(map[string]time.Time)
	}

	now := time.Now()

	for k, v := range consumed {
		if v == 0 {
			delete(cpCon, k)
			delete(g.lastConsumed[id], k)
			delete(g.velocityEMA[id], k)
			delete(g.lastUpdated[id], k)
		} else {
			cpCon[k] = v
			lastTime, timeOk := g.lastUpdated[id][k]
			lastCon, consOk := g.lastConsumed[id][k]
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
					prevEMA := g.velocityEMA[id][k]
					newEMA := (g.alpha * currentVelocity) + ((1 - g.alpha) * prevEMA)
					g.velocityEMA[id][k] = newEMA
				}
				g.lastUpdated[id][k] = now
			} else {
				g.lastUpdated[id][k] = now
			}
			g.lastConsumed[id][k] = v
		}
	}

	cpBor, ok := g.peerBorrowed[id]
	if !ok {
		cpBor = make(map[string]uint64)
		g.peerBorrowed[id] = cpBor
	}
	for k, v := range borrowed {
		if v == 0 {
			delete(cpBor, k)
		} else {
			cpBor[k] = v
		}
	}

	cpUpd, ok := g.peerUpdated[id]
	if !ok {
		cpUpd = make(map[string]time.Time)
		g.peerUpdated[id] = cpUpd
	}
	for k := range consumed {
		cpUpd[k] = now
	}
	for k := range borrowed {
		cpUpd[k] = now
	}
}

func (g *Gossiper) RemovePeer(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.peerConsumed, id)
	delete(g.peerBorrowed, id)
	delete(g.peerUpdated, id)
	delete(g.lastConsumed, id)
	delete(g.velocityEMA, id)
	delete(g.lastUpdated, id)
}

func (g *Gossiper) TopK(k int, key string) []PeerEntry {
	g.mu.RLock()
	defer g.mu.RUnlock()

	now := time.Now()
	entries := make([]PeerEntry, 0, len(g.peerBorrowed))
	for id, bm := range g.peerBorrowed {
		borrowed := bm[key]
		if borrowed == 0 {
			continue
		}
		var consumed uint64
		if cm, ok := g.peerConsumed[id]; ok {
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
		if um, ok := g.peerUpdated[id]; ok {
			lastTime = um[key]
		}

		velocity := g.velocityEMA[id][key]
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
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := make([]string, 0, len(g.peerBorrowed))
	for id := range g.peerBorrowed {
		ids = append(ids, id)
	}
	return ids
}
