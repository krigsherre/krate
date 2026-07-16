package routing

import (
	"context"
	"sync"
	"time"

	"github.com/krigsherre/krate/sketch"
)

// MLPredictiveRouter uses an Exponential Moving Average (EMA) to predict
// peer token decay between gossip heartbeats.
type MLPredictiveRouter struct {
	mu       sync.RWMutex
	gossiper *sketch.Gossiper

	// Track the last known surplus per peer per key to calculate velocity
	lastSurplus map[string]map[string]uint64
	// Track the calculated consumption velocity (EMA) per peer per key
	velocityEMA map[string]map[string]float64
	// Track the time of the last update per peer per key
	lastUpdated map[string]map[string]time.Time

	// Alpha parameter for EMA (0 < alpha <= 1)
	alpha float64
}

// NewMLPredictiveRouter creates a new ML-based predictive router.
func NewMLPredictiveRouter(alpha float64) *MLPredictiveRouter {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.5 // default fallback
	}
	return &MLPredictiveRouter{
		lastSurplus: make(map[string]map[string]uint64),
		velocityEMA: make(map[string]map[string]float64),
		lastUpdated: make(map[string]map[string]time.Time),
		alpha:       alpha,
	}
}

// Init injects the gossiper into the router so it can read peer state.
func (m *MLPredictiveRouter) Init(gossiper *sketch.Gossiper) {
	m.gossiper = gossiper
}

// updateModels updates the EMA based on the latest gossip state.
// In a real production system, this would be hooked directly to gossip receive events
// or run asynchronously. For now, we sample it when deciding, with a lightweight lock.
func (m *MLPredictiveRouter) updateModels(key string, peers []sketch.PeerEntry) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range peers {
		// Initialize maps if missing
		if _, ok := m.lastSurplus[p.ID]; !ok {
			m.lastSurplus[p.ID] = make(map[string]uint64)
			m.velocityEMA[p.ID] = make(map[string]float64)
			m.lastUpdated[p.ID] = make(map[string]time.Time)
		}

		lastTime, timeOk := m.lastUpdated[p.ID][key]
		lastSurplus, surpOk := m.lastSurplus[p.ID][key]

		// If we have previous data and the surplus changed, it implies a new gossip heartbeat arrived.
		// If surplus hasn't changed, we assume we are just querying stale state between heartbeats.
		if timeOk && surpOk && now.After(lastTime) && lastSurplus != p.Surplus {
			dt := now.Sub(lastTime).Seconds()
			if dt > 0 {
				var diff float64
				if lastSurplus > p.Surplus {
					diff = float64(lastSurplus - p.Surplus)
				} else {
					diff = -float64(p.Surplus - lastSurplus)
				}

				currentVelocity := diff / dt

				// Apply EMA
				prevEMA := m.velocityEMA[p.ID][key]
				newEMA := (m.alpha * currentVelocity) + ((1 - m.alpha) * prevEMA)
				m.velocityEMA[p.ID][key] = newEMA
			}

			// Only update the timestamp if we observed a new heartbeat
			m.lastUpdated[p.ID][key] = now
		} else if !timeOk {
			// First time seeing this peer
			m.lastUpdated[p.ID][key] = now
		}

		// Update state
		m.lastSurplus[p.ID][key] = p.Surplus
	}
}

// Decide routes the request based on ML predictions.
func (m *MLPredictiveRouter) Decide(ctx context.Context, rc *RouteContext) (Decision, error) {
	if !rc.HasPeers || m.gossiper == nil {
		if rc.RedisExhausted {
			return DecisionDeny, nil
		}
		return DecisionRedis, nil
	}

	// Get top peers for this key from the gossip state
	// In Krate, TopK returns peers with surplus > 0 based on last heartbeat.
	topPeers := m.gossiper.TopK(3, rc.Key)
	if len(topPeers) == 0 {
		if rc.RedisExhausted {
			return DecisionDeny, nil
		}
		return DecisionRedis, nil
	}

	// Update our models based on the current snapshot
	m.updateModels(rc.Key, topPeers)

	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now()

	// Evaluate predictions for each top peer
	for _, p := range topPeers {
		velocity := m.velocityEMA[p.ID][rc.Key]
		lastTime := m.lastUpdated[p.ID][rc.Key]

		dt := now.Sub(lastTime).Seconds()
		if dt < 0 {
			dt = 0
		}

		// Predict current surplus: LastSurplus - (Velocity * TimeSinceLastGossip)
		expectedDecay := velocity * dt
		var predictedSurplus float64
		if expectedDecay > 0 {
			predictedSurplus = float64(p.Surplus) - expectedDecay
		} else {
			predictedSurplus = float64(p.Surplus)
		}

		// If the predicted surplus can cover our need, we route to this peer!
		if predictedSurplus >= float64(rc.Need) {
			return DecisionPeer, nil
		}
	}

	// If all top peers are predicted to be exhausted based on velocity,
	// we avoid the stale gRPC probe and fallback directly to Redis.
	if rc.RedisExhausted {
		return DecisionDeny, nil
	}
	return DecisionRedis, nil
}
