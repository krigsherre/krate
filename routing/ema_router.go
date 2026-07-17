package routing

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/krigsherre/krate/internal/sketch"
	"golang.org/x/sys/cpu"
)

const routerShardCount = 64

type routerShard struct {
	mu           sync.Mutex
	lastConsumed map[string]map[string]uint64
	velocityEMA  map[string]map[string]float64
	lastUpdated  map[string]map[string]time.Time
	_pad         cpu.CacheLinePad
}

type EMAPredictiveRouter struct {
	gossiper *sketch.Gossiper
	logger   *slog.Logger
	alpha    float64
	shards   [routerShardCount]routerShard
}

func NewEMAPredictiveRouter(alpha float64) *EMAPredictiveRouter {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.5
	}
	m := &EMAPredictiveRouter{
		alpha: alpha,
	}
	for i := 0; i < routerShardCount; i++ {
		m.shards[i].lastConsumed = make(map[string]map[string]uint64)
		m.shards[i].velocityEMA = make(map[string]map[string]float64)
		m.shards[i].lastUpdated = make(map[string]map[string]time.Time)
	}
	return m
}

func (m *EMAPredictiveRouter) Init(gossiper *sketch.Gossiper, logger *slog.Logger) {
	m.gossiper = gossiper
	m.logger = logger
}

func (m *EMAPredictiveRouter) shardIndex(key string) int {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h) & (routerShardCount - 1)
}

func (m *EMAPredictiveRouter) updateModelsLocked(shard *routerShard, key string, peers []sketch.PeerEntry) {
	for _, p := range peers {
		if _, ok := shard.lastConsumed[p.ID]; !ok {
			shard.lastConsumed[p.ID] = make(map[string]uint64)
			shard.velocityEMA[p.ID] = make(map[string]float64)
			shard.lastUpdated[p.ID] = make(map[string]time.Time)
		}

		lastTime, timeOk := shard.lastUpdated[p.ID][key]
		lastConsumed, consOk := shard.lastConsumed[p.ID][key]
		if timeOk && consOk && p.LastUpdated.After(lastTime) {
			dt := p.LastUpdated.Sub(lastTime).Seconds()
			if dt > 0 {
				var diff float64
				if p.Consumed >= lastConsumed {
					diff = float64(p.Consumed - lastConsumed)
				} else {
					diff = float64(p.Consumed)
				}

				currentVelocity := diff / dt
				prevEMA := shard.velocityEMA[p.ID][key]
				newEMA := (m.alpha * currentVelocity) + ((1 - m.alpha) * prevEMA)
				shard.velocityEMA[p.ID][key] = newEMA
			}

			shard.lastUpdated[p.ID][key] = p.LastUpdated
		} else if !timeOk {
			shard.lastUpdated[p.ID][key] = p.LastUpdated
		}
		shard.lastConsumed[p.ID][key] = p.Consumed
	}
}

func (m *EMAPredictiveRouter) Decide(ctx context.Context, rc *RouteContext) (Decision, error) {
	if !rc.HasPeers || m.gossiper == nil {
		if rc.RedisExhausted {
			if m.logger != nil {
				m.logger.Debug("EMA Router: deny routing (no peers and Redis exhausted)", "key", rc.Key)
			}
			return DecisionDeny, nil
		}
		if m.logger != nil {
			m.logger.Debug("EMA Router: route to Redis (no peers or gossiper)", "key", rc.Key)
		}
		return DecisionRedis, nil
	}
	k := 2
	if len(m.gossiper.PeerIDs())+1 > 5 {
		k = 3
	}
	topPeers := m.gossiper.TopK(k, rc.Key)
	if len(topPeers) == 0 {
		if rc.RedisExhausted {
			if m.logger != nil {
				m.logger.Debug("EMA Router: deny routing (no top peers and Redis exhausted)", "key", rc.Key)
			}
			return DecisionDeny, nil
		}
		if m.logger != nil {
			m.logger.Debug("EMA Router: route to Redis (no top peers)", "key", rc.Key)
		}
		return DecisionRedis, nil
	}

	idx := m.shardIndex(rc.Key)
	shard := &m.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	m.updateModelsLocked(shard, rc.Key, topPeers)

	now := time.Now()
	for _, p := range topPeers {
		velocity := shard.velocityEMA[p.ID][rc.Key]
		lastTime := shard.lastUpdated[p.ID][rc.Key]

		dt := now.Sub(lastTime).Seconds()
		if dt < 0 {
			dt = 0
		}
		expectedDecay := velocity * dt
		var predictedSurplus float64
		if expectedDecay > 0 {
			if float64(p.Surplus) > expectedDecay {
				predictedSurplus = float64(p.Surplus) - expectedDecay
			} else {
				predictedSurplus = 0
			}
		} else {
			predictedSurplus = float64(p.Surplus)
		}
		if predictedSurplus >= float64(rc.Need) {
			if m.logger != nil {
				m.logger.Debug("EMA Router: route to Peer", "key", rc.Key, "peer", p.ID, "predictedSurplus", predictedSurplus, "need", rc.Need)
			}
			return DecisionPeer, nil
		}
	}
	if rc.RedisExhausted {
		if m.logger != nil {
			m.logger.Debug("EMA Router: deny routing (peers lack surplus and Redis exhausted)", "key", rc.Key)
		}
		return DecisionDeny, nil
	}
	if m.logger != nil {
		m.logger.Debug("EMA Router: route to Redis (peers lack surplus)", "key", rc.Key)
	}
	return DecisionRedis, nil
}

func (m *EMAPredictiveRouter) EvictKeys(keys []string) {
	var shardToKeys [routerShardCount][]string
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
		for _, key := range shKeys {
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

func (m *EMAPredictiveRouter) RemovePeer(peerID string) {
	for i := 0; i < routerShardCount; i++ {
		shard := &m.shards[i]
		shard.mu.Lock()
		delete(shard.lastConsumed, peerID)
		delete(shard.velocityEMA, peerID)
		delete(shard.lastUpdated, peerID)
		shard.mu.Unlock()
	}
}
