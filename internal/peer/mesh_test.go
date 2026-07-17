package peer

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/krigsherre/krate/internal/cluster"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

type mockDiscoverer struct {
	members []cluster.MemberInfo
}

func (m *mockDiscoverer) Discover(ctx context.Context) ([]cluster.MemberInfo, error) {
	return m.members, nil
}

func TestMeshDiscovery(t *testing.T) {
	disc := &mockDiscoverer{
		members: []cluster.MemberInfo{
			{ID: "p1", GossipAddr: "127.0.0.1:7101", GRPCAddr: "127.0.0.1:7201"},
			{ID: "p2", GossipAddr: "127.0.0.1:7102", GRPCAddr: "127.0.0.1:7202"},
			{ID: "p3", GossipAddr: "127.0.0.1:7103", GRPCAddr: "127.0.0.1:7203"},
		},
	}

	m := NewMesh("self", disc, time.Hour, false, testLogger(t))
	defer m.Stop()
	m.refresh(context.Background())

	for _, id := range []string{"p1", "p2", "p3"} {
		p, ok := m.GetPeer(id)
		if !ok {
			t.Errorf("GetPeer(%s) not found", id)
			continue
		}
		if p.ID != id {
			t.Errorf("peer ID = %q, want %q", p.ID, id)
		}
		if !p.Healthy {
			t.Errorf("peer %s: Healthy = false", id)
		}
	}
}

func TestMeshGetPeer(t *testing.T) {
	disc := &mockDiscoverer{
		members: []cluster.MemberInfo{
			{ID: "p1", GossipAddr: "10.0.0.1:7100", GRPCAddr: "10.0.0.1:7200"},
		},
	}

	m := NewMesh("self", disc, time.Hour, false, testLogger(t))
	defer m.Stop()
	m.refresh(context.Background())

	p, ok := m.GetPeer("p1")
	if !ok {
		t.Fatal("GetPeer(p1) not found")
	}
	if p.GRPCAddr != "10.0.0.1:7200" {
		t.Errorf("GRPCAddr = %q, want %q", p.GRPCAddr, "10.0.0.1:7200")
	}
	if p.GossipAddr != "10.0.0.1:7100" {
		t.Errorf("GossipAddr = %q, want %q", p.GossipAddr, "10.0.0.1:7100")
	}
}

func TestMeshRemoval(t *testing.T) {
	disc := &mockDiscoverer{
		members: []cluster.MemberInfo{
			{ID: "p1", GossipAddr: "10.0.0.1:7100", GRPCAddr: "10.0.0.1:7200"},
			{ID: "p2", GossipAddr: "10.0.0.2:7100", GRPCAddr: "10.0.0.2:7200"},
		},
	}

	m := NewMesh("self", disc, time.Hour, false, testLogger(t))
	defer m.Stop()
	m.refresh(context.Background())

	if _, ok := m.GetPeer("p2"); !ok {
		t.Fatal("p2 should exist after first refresh")
	}

	disc.members = []cluster.MemberInfo{
		{ID: "p1", GossipAddr: "10.0.0.1:7100", GRPCAddr: "10.0.0.1:7200"},
	}

	m.refresh(context.Background())

	if _, ok := m.GetPeer("p2"); ok {
		t.Error("p2 should be removed after second refresh")
	}
	if _, ok := m.GetPeer("p1"); !ok {
		t.Error("p1 should still exist")
	}
}

func TestMeshGossipAddrs(t *testing.T) {
	disc := &mockDiscoverer{
		members: []cluster.MemberInfo{
			{ID: "p1", GossipAddr: "10.0.0.1:7100", GRPCAddr: "10.0.0.1:7200"},
			{ID: "p2", GossipAddr: "10.0.0.2:7100", GRPCAddr: "10.0.0.2:7200"},
		},
	}

	m := NewMesh("self", disc, time.Hour, false, testLogger(t))
	defer m.Stop()
	m.refresh(context.Background())

	addrs := m.GossipAddrs()
	if len(addrs) != 2 {
		t.Fatalf("GossipAddrs() = %d, want 2", len(addrs))
	}

	addrSet := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		addrSet[a] = true
	}
	if !addrSet["10.0.0.1:7100"] || !addrSet["10.0.0.2:7100"] {
		t.Errorf("GossipAddrs() = %v, want both addresses", addrs)
	}
}
