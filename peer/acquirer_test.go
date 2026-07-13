package peer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	kratev1 "github.com/krigsherre/krate/peer/peerpb"
	"github.com/krigsherre/krate/sketch"
)

type mockPeerGRPCClient struct {
	granted uint64
	delay   time.Duration
	called  atomic.Bool
}

func (m *mockPeerGRPCClient) TransferTokens(ctx context.Context, in *kratev1.TransferRequest, opts ...grpc.CallOption) (*kratev1.TransferResponse, error) {
	m.called.Store(true)
	if m.delay > 0 {
		timer := time.NewTimer(m.delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
	return &kratev1.TransferResponse{Granted: m.granted}, nil
}

func (m *mockPeerGRPCClient) Ping(ctx context.Context, in *kratev1.PingRequest, opts ...grpc.CallOption) (*kratev1.PingResponse, error) {
	return &kratev1.PingResponse{}, nil
}

func (m *mockPeerGRPCClient) Gossip(ctx context.Context, in *kratev1.GossipRequest, opts ...grpc.CallOption) (*kratev1.GossipResponse, error) {
	return &kratev1.GossipResponse{}, nil
}

type mockPeerLookup struct {
	clients map[string]kratev1.KratePeerServiceClient
}

func (m *mockPeerLookup) GetPeerClient(id string) (kratev1.KratePeerServiceClient, bool) {
	c, ok := m.clients[id]
	return c, ok
}

type mockPeerRanker struct {
	entries []sketch.PeerEntry
}

func (m *mockPeerRanker) TopK(k int, key string) []sketch.PeerEntry {
	if k >= len(m.entries) {
		return m.entries
	}
	return m.entries[:k]
}

type countingPeerRanker struct {
	inner     *mockPeerRanker
	callCount atomic.Int32
}

func (c *countingPeerRanker) TopK(k int, key string) []sketch.PeerEntry {
	c.callCount.Add(1)
	return c.inner.TopK(k, key)
}

func TestAcquirerTopKProbes(t *testing.T) {
	p1 := &mockPeerGRPCClient{granted: 100}
	p2 := &mockPeerGRPCClient{granted: 200}
	p3 := &mockPeerGRPCClient{granted: 300}
	p4 := &mockPeerGRPCClient{granted: 400}
	p5 := &mockPeerGRPCClient{granted: 500}

	clients := map[string]kratev1.KratePeerServiceClient{
		"p1": p1, "p2": p2, "p3": p3, "p4": p4, "p5": p5,
	}
	entries := []sketch.PeerEntry{
		{ID: "p1", Surplus: 500}, {ID: "p2", Surplus: 400},
		{ID: "p3", Surplus: 300}, {ID: "p4", Surplus: 200},
		{ID: "p5", Surplus: 100},
	}

	acq := NewAcquirer(
		&mockPeerLookup{clients: clients},
		&mockPeerRanker{entries: entries},
		NewPeerClient(time.Second, testLogger(t)),
		"self", 3, ProbeParallel, testLogger(t),
	)

	granted, err := acq.Acquire(context.Background(), "key", 50)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if granted == 0 {
		t.Fatal("expected non-zero granted")
	}
	if !p1.called.Load() {
		t.Error("p1 should have been probed")
	}
	if !p2.called.Load() {
		t.Error("p2 should have been probed")
	}
	if !p3.called.Load() {
		t.Error("p3 should have been probed")
	}
}

func TestAcquirerParallelMode(t *testing.T) {
	p1 := &mockPeerGRPCClient{granted: 100, delay: 200 * time.Millisecond}
	p2 := &mockPeerGRPCClient{granted: 200, delay: 10 * time.Millisecond}
	p3 := &mockPeerGRPCClient{granted: 300, delay: 200 * time.Millisecond}

	clients := map[string]kratev1.KratePeerServiceClient{
		"p1": p1, "p2": p2, "p3": p3,
	}
	entries := []sketch.PeerEntry{
		{ID: "p1", Surplus: 100}, {ID: "p2", Surplus: 200}, {ID: "p3", Surplus: 300},
	}

	acq := NewAcquirer(
		&mockPeerLookup{clients: clients},
		&mockPeerRanker{entries: entries},
		NewPeerClient(500*time.Millisecond, testLogger(t)),
		"self", 3, ProbeParallel, testLogger(t),
	)

	start := time.Now()
	granted, err := acq.Acquire(context.Background(), "key", 50)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if granted != 200 {
		t.Errorf("granted = %d, want 200 (p2's grant)", granted)
	}
	if elapsed > 150*time.Millisecond {
		t.Errorf("took %v, want < 150ms", elapsed)
	}
}

func TestAcquirerSequentialMode(t *testing.T) {
	p1 := &mockPeerGRPCClient{granted: 0}
	p2 := &mockPeerGRPCClient{granted: 200}
	p3 := &mockPeerGRPCClient{granted: 300}

	clients := map[string]kratev1.KratePeerServiceClient{
		"p1": p1, "p2": p2, "p3": p3,
	}
	entries := []sketch.PeerEntry{
		{ID: "p1", Surplus: 100}, {ID: "p2", Surplus: 200}, {ID: "p3", Surplus: 300},
	}

	acq := NewAcquirer(
		&mockPeerLookup{clients: clients},
		&mockPeerRanker{entries: entries},
		NewPeerClient(time.Second, testLogger(t)),
		"self", 3, ProbeSequential, testLogger(t),
	)

	granted, err := acq.Acquire(context.Background(), "key", 50)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if granted != 200 {
		t.Errorf("granted = %d, want 200", granted)
	}
	if !p1.called.Load() {
		t.Error("p1 should have been probed")
	}
	if !p2.called.Load() {
		t.Error("p2 should have been probed")
	}
	if p3.called.Load() {
		t.Error("p3 should NOT have been probed (p2 succeeded)")
	}
}

func TestAcquirerAllExhausted(t *testing.T) {
	p1 := &mockPeerGRPCClient{granted: 0}
	p2 := &mockPeerGRPCClient{granted: 0}
	p3 := &mockPeerGRPCClient{granted: 0}

	clients := map[string]kratev1.KratePeerServiceClient{
		"p1": p1, "p2": p2, "p3": p3,
	}
	entries := []sketch.PeerEntry{
		{ID: "p1", Surplus: 100}, {ID: "p2", Surplus: 200}, {ID: "p3", Surplus: 300},
	}

	acq := NewAcquirer(
		&mockPeerLookup{clients: clients},
		&mockPeerRanker{entries: entries},
		NewPeerClient(time.Second, testLogger(t)),
		"self", 3, ProbeSequential, testLogger(t),
	)

	granted, err := acq.Acquire(context.Background(), "key", 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if granted != 0 {
		t.Errorf("granted = %d, want 0 (all exhausted)", granted)
	}
}

func TestAcquirerSingleflight(t *testing.T) {
	p1 := &mockPeerGRPCClient{granted: 100, delay: 50 * time.Millisecond}

	clients := map[string]kratev1.KratePeerServiceClient{"p1": p1}
	entries := []sketch.PeerEntry{{ID: "p1", Surplus: 1000}}

	mesh := &mockPeerLookup{clients: clients}
	ranker := &countingPeerRanker{
		inner: &mockPeerRanker{entries: entries},
	}

	acq := NewAcquirer(mesh, ranker, NewPeerClient(time.Second, testLogger(t)),
		"self", 1, ProbeSequential, testLogger(t))

	var (
		ready sync.WaitGroup
		go2   sync.WaitGroup
		done  sync.WaitGroup
	)

	ready.Add(100)
	go2.Add(1)

	results := make([]uint64, 100)
	errs := make([]error, 100)

	for i := 0; i < 100; i++ {
		done.Add(1)
		idx := i
		go func() {
			defer done.Done()
			ready.Done()
			go2.Wait()
			results[idx], errs[idx] = acq.Acquire(context.Background(), "key", 10)
		}()
	}

	ready.Wait()
	go2.Done()
	done.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
		if results[i] != 100 {
			t.Errorf("goroutine %d: granted = %d, want 100", i, results[i])
		}
	}

	if got := ranker.callCount.Load(); got != 1 {
		t.Errorf("TopK called %d times, want 1 (singleflight)", got)
	}
}
