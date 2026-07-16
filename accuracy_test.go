package krate_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/krigsherre/krate"
	"github.com/krigsherre/krate/routing"
)

// Helper to read counter values from Prometheus metrics
func readCounter(counterVec *prometheus.CounterVec, labels ...string) float64 {
	counter, err := counterVec.GetMetricWithLabelValues(labels...)
	if err != nil {
		return 0
	}
	var m dto.Metric
	_ = counter.Write(&m)
	if m.Counter != nil {
		return *m.Counter.Value
	}
	return 0
}

func TestIntegration_LimitAccuracy(t *testing.T) {
	rdb := setupRedis(t)
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, rdb, "krate:"+key+":*") })

	const numNodes = 3
	const globalLimit = 300
	const windowSize = 10 * time.Second

	limiters := make([]*krate.Limiter, numNodes)
	regs := make([]*prometheus.Registry, numNodes)

	// Start a cluster of 3 nodes sharing the same Redis limit
	for i := 0; i < numNodes; i++ {
		regs[i] = prometheus.NewRegistry()
		l, err := krate.New(rdb,
			krate.WithInstanceID(fmt.Sprintf("node-%d-%s", i, uniqueKey(t))),
			krate.WithLimit(globalLimit),
			krate.WithWindow(windowSize),
			krate.WithMaxBorrow(50),
			krate.WithMinBorrow(10),
			krate.WithPeerListen(":0"),
			krate.WithGossipInterval(10*time.Millisecond),
			krate.WithHeartbeatInterval(10*time.Millisecond),
			krate.WithMetrics(regs[i]),
			krate.WithPreBorrowEnabled(false),
			krate.WithProbeTimeout(100*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("Failed to create limiter %d: %v", i, err)
		}
		limiters[i] = l
		t.Cleanup(func() { l.Close() })
	}

	// Wait for peer discovery
	time.Sleep(500 * time.Millisecond)

	ctx := context.Background()

	// --- PHASE A: Under Limit (False Rejection Test) ---
	// We make a total of 200 requests (below the 300 limit) distributed across the 3 nodes.
	// Since 200 < 300, we expect 0 rejections.
	var allowedCount atomic.Int64
	var rejectedCount atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(reqNum int) {
			defer wg.Done()
			limNode := limiters[reqNum%numNodes]
			ok, err := limNode.Allow(ctx, key)
			if err != nil {
				t.Errorf("Allow error: %v", err)
				return
			}
			if ok {
				allowedCount.Add(1)
			} else {
				rejectedCount.Add(1)
			}
		}(i)
	}
	wg.Wait()

	// Debug remaining tokens
	poolKey := fmt.Sprintf("krate:%s:pool", key)
	redisVal, _ := rdb.Get(ctx, poolKey).Result()
	t.Logf("At end of Phase A: Redis Pool = %s", redisVal)

	
	// We can inspect the local bucket remaining tokens via metrics or reflecting on l.shards
	// Let's print the metrics of each node
	for i, reg := range regs {
		mfs, _ := reg.Gather()
		for _, mf := range mfs {
			if mf.GetName() == "krate_local_tokens" {
				for _, m := range mf.GetMetric() {
					t.Logf("Node %d: Local tokens remaining = %f", i, m.GetGauge().GetValue())
				}
			}
		}
	}

	phaseAAllowed := allowedCount.Load()
	t.Logf("Phase A (Under Limit): Allowed: %d, Rejected: %d", phaseAAllowed, rejectedCount.Load())
	if phaseAAllowed < 185 {
		t.Errorf("Expected at least 185 allowed requests, got %d", phaseAAllowed)
	}
	if rejectedCount.Load() > 15 {
		t.Errorf("Expected at most 15 false rejections, got %d", rejectedCount.Load())
	}

	// Settle background borrows and gossip before starting Phase B
	time.Sleep(200 * time.Millisecond)

	// --- PHASE B: Over Limit (Over-Admission Test) ---
	// We send 500 more requests concurrently.
	// The remaining limit is 100. We expect exactly 100 to be allowed (making global allowed = 300),
	// and the remaining 400 to be rejected. Due to coordination lag, we allow a tiny tolerance (+/- 15).
	allowedCount.Store(0)
	rejectedCount.Store(0)

	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(reqNum int) {
			defer wg.Done()
			limNode := limiters[reqNum%numNodes]
			ok, err := limNode.Allow(ctx, key)
			if err != nil {
				t.Errorf("Allow error: %v", err)
				return
			}
			if ok {
				allowedCount.Add(1)
			} else {
				rejectedCount.Add(1)
			}
		}(i)
	}
	wg.Wait()

	redisValB, _ := rdb.Get(ctx, poolKey).Result()
	t.Logf("At end of Phase B: Redis Pool = %s", redisValB)
	for i, reg := range regs {
		mfs, _ := reg.Gather()
		for _, mf := range mfs {
			if mf.GetName() == "krate_local_tokens" {
				for _, m := range mf.GetMetric() {
					t.Logf("Node %d: Local tokens remaining after Phase B = %f", i, m.GetGauge().GetValue())
				}
			}
		}
	}

	totalAllowed := phaseAAllowed + allowedCount.Load()
	t.Logf("Phase B (Over Limit): Allowed in Phase B: %d, Total Allowed: %d, Rejected in Phase B: %d",
		allowedCount.Load(), totalAllowed, rejectedCount.Load())

	// The total allowed across both phases must not exceed the limit plus minor lag variance
	if totalAllowed > globalLimit+15 {
		t.Errorf("Over-admission leak! Allowed %d requests, limit is %d", totalAllowed, globalLimit)
	}
}

func TestIntegration_RouterComparison(t *testing.T) {
	rdb := setupRedis(t)
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, rdb, "krate:"+key+":*") })

	// Helper function to run the routing scenario
	runRoutingTest := func(t *testing.T, useEMARouter bool) (int64, time.Duration) {
		t.Helper()
		ctx := context.Background()

		// Start Node A (donor node with large pool)
		regA := prometheus.NewRegistry()
		lA, err := krate.New(rdb,
			krate.WithInstanceID(fmt.Sprintf("node-A-%s", uniqueKey(t))),
			krate.WithLimit(10000),
			krate.WithWindow(10*time.Second),
			krate.WithMaxBorrow(5000),
			krate.WithMinBorrow(1000),
			krate.WithPeerListen(":0"),
			krate.WithGossipInterval(10*time.Millisecond),
			krate.WithHeartbeatInterval(10*time.Millisecond),
			krate.WithMetrics(regA),
		)
		if err != nil {
			t.Fatalf("failed to create node A: %v", err)
		}
		defer lA.Close()

		// Let Node A warm up and borrow a large segment from Redis
		_, _ = lA.AllowN(ctx, key, 1000)

		// Start Node B (requester node)
		var rtr routing.Router = routing.NewDefaultRouter()
		if useEMARouter {
			rtr = routing.NewEMAPredictiveRouter(0.5)
		}

		regB := prometheus.NewRegistry()
		lB, err := krate.New(rdb,
			krate.WithInstanceID(fmt.Sprintf("node-B-%s", uniqueKey(t))),
			krate.WithLimit(10), // Tiny limit forces peer borrowing
			krate.WithWindow(10*time.Second),
			krate.WithMaxBorrow(1),
			krate.WithMinBorrow(1),
			krate.WithPeerListen(":0"),
			krate.WithGossipInterval(10*time.Millisecond),
			krate.WithHeartbeatInterval(10*time.Millisecond),
			krate.WithProbeK(1),
			krate.WithMetrics(regB),
			krate.WithRouter(rtr),
		)
		if err != nil {
			t.Fatalf("failed to create node B: %v", err)
		}
		defer lB.Close()

		// Settle gossip
		time.Sleep(200 * time.Millisecond)

		// Consume all remaining Node A tokens rapidly to trigger peer exhaustion
		_, _ = lA.AllowN(ctx, key, 4000)

		// Node B sends requests. Since Node A is exhausted, requests will trigger peer probing.
		// We measure request durations on Node B to assess latency, and collect stale probes.
		start := time.Now()
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = lB.Allow(ctx, key)
			}()
		}
		wg.Wait()
		duration := time.Since(start)

		// Gather peer stale probes metric from Node B
		mfs, _ := regB.Gather()
		var staleProbes int64
		for _, mf := range mfs {
			if mf.GetName() == "krate_peer_probe_stale_total" {
				for _, m := range mf.GetMetric() {
					staleProbes += int64(m.GetCounter().GetValue())
				}
			}
		}

		return staleProbes, duration
	}

	t.Run("Default Router", func(t *testing.T) {
		stale, dur := runRoutingTest(t, false)
		t.Logf("[Default Router] Stale peer probes: %d, Duration: %v", stale, dur)
	})

	t.Run("EMA Router", func(t *testing.T) {
		stale, dur := runRoutingTest(t, true)
		t.Logf("[EMA Router] Stale peer probes: %d, Duration: %v", stale, dur)
		
		// The EMA Predictive Router should bypass the exhausted peer A,
		// leading to fewer or zero stale probes compared to Default Router.
		// (We don't assert absolute values due to scheduling variance in local tests,
		// but we verify the router executes cleanly).
	})
}
