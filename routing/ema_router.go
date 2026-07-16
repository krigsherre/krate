package routing

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/krigsherre/krate/sketch"
)

type EMAPredictiveRouter struct {
	mu           sync.Mutex
	gossiper     *sketch.Gossiper
	logger       *slog.Logger
	lastConsumed map[string]map[string]uint64
	velocityEMA  map[string]map[string]float64
	lastUpdated  map[string]map[string]time.Time
	alpha        float64
}

func NewEMAPredictiveRouter(alpha float64) *EMAPredictiveRouter {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.5
	}
	return &EMAPredictiveRouter{
		lastConsumed: make(map[string]map[string]uint64),
		velocityEMA:  make(map[string]map[string]float64),
		lastUpdated:  make(map[string]map[string]time.Time),
		alpha:        alpha,
	}
}

func (m *EMAPredictiveRouter) Init(gossiper *sketch.Gossiper, logger *slog.Logger) {
	m.gossiper = gossiper
	m.logger = logger
}

func (m *EMAPredictiveRouter) updateModelsLocked(key string, peers []sketch.PeerEntry) {
	for _, p := range peers {
		if _, ok := m.lastConsumed[p.ID]; !ok {
			m.lastConsumed[p.ID] = make(map[string]uint64)
			m.velocityEMA[p.ID] = make(map[string]float64)
			m.lastUpdated[p.ID] = make(map[string]time.Time)
		}

		lastTime, timeOk := m.lastUpdated[p.ID][key]
		lastConsumed, consOk := m.lastConsumed[p.ID][key]
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
				prevEMA := m.velocityEMA[p.ID][key]
				newEMA := (m.alpha * currentVelocity) + ((1 - m.alpha) * prevEMA)
				m.velocityEMA[p.ID][key] = newEMA
			}

			m.lastUpdated[p.ID][key] = p.LastUpdated
		} else if !timeOk {
			m.lastUpdated[p.ID][key] = p.LastUpdated
		}
		m.lastConsumed[p.ID][key] = p.Consumed
	}
}

func (m *EMAPredictiveRouter) Decide(ctx context.Context, rc *RouteContext) (Decision, error) {
	if !rc.HasPeers || m.gossiper == nil {
		if rc.RedisExhausted {
			if m.logger != nil {
				m.logger.Warn("EMA Router: deny routing (no peers and Redis exhausted)", "key", rc.Key)
			}
			return DecisionDeny, nil
		}
		if m.logger != nil {
			m.logger.Debug("EMA Router: route to Redis (no peers or gossiper)", "key", rc.Key)
		}
		return DecisionRedis, nil
	}
	topPeers := m.gossiper.TopK(3, rc.Key)
	if len(topPeers) == 0 {
		if rc.RedisExhausted {
			if m.logger != nil {
				m.logger.Warn("EMA Router: deny routing (no top peers and Redis exhausted)", "key", rc.Key)
			}
			return DecisionDeny, nil
		}
		if m.logger != nil {
			m.logger.Debug("EMA Router: route to Redis (no top peers)", "key", rc.Key)
		}
		return DecisionRedis, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.updateModelsLocked(rc.Key, topPeers)

	now := time.Now()
	for _, p := range topPeers {
		velocity := m.velocityEMA[p.ID][rc.Key]
		lastTime := m.lastUpdated[p.ID][rc.Key]

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
			m.logger.Warn("EMA Router: deny routing (peers lack surplus and Redis exhausted)", "key", rc.Key)
		}
		return DecisionDeny, nil
	}
	if m.logger != nil {
		m.logger.Debug("EMA Router: route to Redis (peers lack surplus)", "key", rc.Key)
	}
	return DecisionRedis, nil
}
