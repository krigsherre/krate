package routing

import (
	"context"
	"testing"
	"time"

	"github.com/krigsherre/krate/sketch"
)

func TestRouterComparison(t *testing.T) {
	// Initialize Gossiper and set up our dummy peer
	gossiper := sketch.NewGossiper()
	peerID := "peer-1"
	key := "api_limit"

	// Create both routers to compare
	defaultRouter := NewDefaultRouter()
	mlRouter := NewMLPredictiveRouter(0.5) // Alpha of 0.5 for learning rate
	mlRouter.Init(gossiper)

	// --- STEP 1: Initial State ---
	// Peer-1 reports they have 3000 borrowed tokens and 1000 consumed.
	// This means their last known Surplus = 2000.
	gossiper.UpdatePeer(peerID, map[string]uint64{key: 1000}, map[string]uint64{key: 3000})

	rc := &RouteContext{Key: key, Need: 50, HasPeers: true}
	// Let the ML router observe the first state
	mlRouter.Decide(context.Background(), rc)

	// --- STEP 2: Simulate High Traffic Spike (Velocity) ---
	// 100ms later, a gossip heartbeat comes in. The peer's consumed tokens jumped from 1000 to 2500!
	// Surplus is now only 500. This is a massive decay velocity (1500 tokens burned in 100ms).
	time.Sleep(100 * time.Millisecond)
	gossiper.UpdatePeer(peerID, map[string]uint64{key: 2500}, map[string]uint64{key: 3000})

	// Let the ML router observe this rapid decay and calculate its EMA velocity
	mlRouter.Decide(context.Background(), rc)

	// --- STEP 3: Stale State Window ---
	// Fast forward 100ms without a new gossip heartbeat (the network is busy).
	// Based on the velocity (15,000 tokens/sec), the remaining 500 surplus should be completely gone by now.
	// However, the static "Gossiper" state hasn't updated yet. It still claims the peer has 500 surplus.
	time.Sleep(100 * time.Millisecond)

	t.Logf("--- ROUTER COMPARISON SIMULATION ---")

	// 1. Ask the Default Router
	defDecision, _ := defaultRouter.Decide(context.Background(), rc)
	if defDecision == DecisionPeer {
		t.Logf("❌ [Default Router] Decision: DecisionPeer")
		t.Logf("    Reasoning: It blindly routes to the peer because the static state says there is 500 surplus.")
		t.Logf("    Outcome:   Stale Probe! The peer is actually exhausted. This wastes a gRPC call and ruins latency.")
	}

	// 2. Ask the ML Predictive Router
	mlDecision, _ := mlRouter.Decide(context.Background(), rc)
	if mlDecision == DecisionRedis {
		t.Logf("✅ [ML Router]      Decision: DecisionRedis")
		t.Logf("    Reasoning: It calculated the velocity, predicted the remaining 500 tokens were burned, and bypassed the peer.")
		t.Logf("    Outcome:   Saved a network hop! Bypasses the exhausted peer and goes straight to Redis.")
	}

	// Assertions for the test suite
	if defDecision != DecisionPeer {
		t.Errorf("Expected DefaultRouter to fall for the stale state and return DecisionPeer")
	}
	if mlDecision != DecisionRedis {
		t.Errorf("Expected MLRouter to predict exhaustion and return DecisionRedis, got %v", mlDecision)
	}
}
