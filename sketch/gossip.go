package sketch

import (
	"sort"
	"sync"
)

type PeerEntry struct {
	ID      string
	Surplus uint64
}

type Gossiper struct {
	mu           sync.RWMutex
	peerConsumed map[string]map[string]uint64
	peerBorrowed map[string]map[string]uint64
}

func NewGossiper() *Gossiper {
	return &Gossiper{
		peerConsumed: make(map[string]map[string]uint64),
		peerBorrowed: make(map[string]map[string]uint64),
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
	for k, v := range consumed {
		if v == 0 {
			delete(cpCon, k)
		} else {
			cpCon[k] = v
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
}

func (g *Gossiper) RemovePeer(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.peerConsumed, id)
	delete(g.peerBorrowed, id)
}

func (g *Gossiper) TopK(k int, key string) []PeerEntry {
	g.mu.RLock()
	defer g.mu.RUnlock()

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
		var surplus uint64
		if borrowed > consumed {
			surplus = borrowed - consumed
		}
		if surplus > 0 {
			entries = append(entries, PeerEntry{ID: id, Surplus: surplus})
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
