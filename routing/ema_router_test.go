package routing

import (
	"context"
	"testing"
	"time"

	"github.com/krigsherre/krate/internal/sketch"
)

func TestEMARouterComparison(t *testing.T) {
	gossiper := sketch.NewGossiper()
	peerID := "peer-1"
	key := "api_limit"
	defaultRouter := NewDefaultRouter()
	emaRouter := NewEMAPredictiveRouter(0.5)
	emaRouter.Init(gossiper, nil)
	gossiper.UpdatePeer(peerID, map[string]uint64{key: 1000}, map[string]uint64{key: 3000})

	rc := &RouteContext{Key: key, Need: 50, HasPeers: true}
	emaRouter.Decide(context.Background(), rc)

	time.Sleep(100 * time.Millisecond)
	gossiper.UpdatePeer(peerID, map[string]uint64{key: 2500}, map[string]uint64{key: 3000})
	emaRouter.Decide(context.Background(), rc)
	time.Sleep(100 * time.Millisecond)
	defDecision, _ := defaultRouter.Decide(context.Background(), rc)
	if defDecision == DecisionPeer {
		t.Logf("Decision: DecisionPeer")
		t.Logf("Reasoning: It blindly routes to the peer because the static state says there is 500 surplus.")
		t.Logf("Outcome:   Stale Probe! The peer is actually exhausted. This wastes a gRPC call and ruins latency.")
	}
	emaDecision, _ := emaRouter.Decide(context.Background(), rc)
	if emaDecision == DecisionRedis {
		t.Logf("Decision: DecisionRedis")
		t.Logf("Reasoning: It calculated the velocity, predicted the remaining 500 tokens were burned, and bypassed the peer.")
		t.Logf("Outcome:   Saved a network hop! Bypasses the exhausted peer and goes straight to Redis.")
	}
	if defDecision != DecisionPeer {
		t.Errorf("Expected DefaultRouter to fall for the stale state and return DecisionPeer")
	}
	if emaDecision != DecisionRedis {
		t.Errorf("Expected EMARouter to predict exhaustion and return DecisionRedis, got %v", emaDecision)
	}
}

func TestEMARouterIdlePeer(t *testing.T) {
	gossiper := sketch.NewGossiper()
	peerID := "peer-1"
	key := "api_limit"
	emaRouter := NewEMAPredictiveRouter(0.5)
	emaRouter.Init(gossiper, nil)
	gossiper.UpdatePeer(peerID, map[string]uint64{key: 1000}, map[string]uint64{key: 3000})
	rc := &RouteContext{Key: key, Need: 50, HasPeers: true}
	emaRouter.Decide(context.Background(), rc)
	time.Sleep(100 * time.Millisecond)
	gossiper.UpdatePeer(peerID, map[string]uint64{key: 1000}, map[string]uint64{key: 3000})
	emaRouter.Decide(context.Background(), rc)
	time.Sleep(100 * time.Millisecond)
	gossiper.UpdatePeer(peerID, map[string]uint64{key: 1000}, map[string]uint64{key: 3000})
	time.Sleep(100 * time.Millisecond)

	decision, _ := emaRouter.Decide(context.Background(), rc)
	if decision != DecisionPeer {
		t.Errorf("Expected EMARouter to route to idle peer with surplus, but got %v", decision)
	}
}
