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
	peerCMS      map[string]*CountMinSketch
	peerBorrowed map[string]map[string]uint64
}

func NewGossiper() *Gossiper {
	return &Gossiper{
		peerCMS:      make(map[string]*CountMinSketch),
		peerBorrowed: make(map[string]map[string]uint64),
	}
}

func (g *Gossiper) UpdatePeer(id string, cms *CountMinSketch, borrowed map[string]uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.peerCMS[id] = cms
	cp := make(map[string]uint64, len(borrowed))
	for k, v := range borrowed {
		cp[k] = v
	}
	g.peerBorrowed[id] = cp
}

func (g *Gossiper) RemovePeer(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.peerCMS, id)
	delete(g.peerBorrowed, id)
}

func (g *Gossiper) TopK(k int, key string) []PeerEntry {
	g.mu.RLock()
	defer g.mu.RUnlock()

	entries := make([]PeerEntry, 0, len(g.peerCMS))
	for id, cms := range g.peerCMS {
		borrowed := uint64(0)
		if bm, ok := g.peerBorrowed[id]; ok {
			borrowed = bm[key]
		}
		consumed := cms.Query(key)
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
	ids := make([]string, 0, len(g.peerCMS))
	for id := range g.peerCMS {
		ids = append(ids, id)
	}
	return ids
}
