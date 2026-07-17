package peer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/krigsherre/krate/internal/cluster"
	kratev1 "github.com/krigsherre/krate/internal/peer/peerpb"
)

type MemberDiscoverer interface {
	Discover(ctx context.Context) ([]cluster.MemberInfo, error)
}

type Peer struct {
	ID         string
	GRPCAddr   string
	GossipAddr string
	Client     kratev1.KratePeerServiceClient
	Conn       *grpc.ClientConn
	LastSeen   time.Time
	Healthy    bool
}

type Mesh struct {
	instanceID string
	membership MemberDiscoverer

	mu            sync.RWMutex
	peers         map[string]*Peer
	conns         map[string]*grpc.ClientConn
	onPeerRemoved func(id string)

	logger      *slog.Logger
	interval    time.Duration
	gzipEnabled bool
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

func NewMesh(instanceID string, membership MemberDiscoverer, interval time.Duration, gzipEnabled bool, logger *slog.Logger) *Mesh {
	if logger == nil {
		logger = slog.Default()
	}
	return &Mesh{
		instanceID:  instanceID,
		membership:  membership,
		peers:       make(map[string]*Peer),
		conns:       make(map[string]*grpc.ClientConn),
		logger:      logger,
		interval:    interval,
		gzipEnabled: gzipEnabled,
	}
}

func (m *Mesh) SetPeerRemovedCallback(fn func(string)) {
	m.mu.Lock()
	m.onPeerRemoved = fn
	m.mu.Unlock()
}

func (m *Mesh) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	m.wg.Add(1)
	go m.loop(ctx)
}

func (m *Mesh) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()
	for id, conn := range m.conns {
		conn.Close()
		delete(m.conns, id)
	}
}

func (m *Mesh) loop(ctx context.Context) {
	defer m.wg.Done()
	m.refresh(ctx)

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.refresh(ctx)
		}
	}
}

func (m *Mesh) refresh(ctx context.Context) {
	members, err := m.membership.Discover(ctx)
	if err != nil {
		m.logger.Warn("mesh discovery failed", "error", err)
		return
	}

	current := make(map[string]cluster.MemberInfo, len(members))
	for _, member := range members {
		if member.ID == m.instanceID {
			continue
		}
		current[member.ID] = member
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for id := range m.peers {
		if _, ok := current[id]; !ok {
			if conn, exists := m.conns[id]; exists {
				conn.Close()
				delete(m.conns, id)
			}
			delete(m.peers, id)
			m.logger.Debug("peer removed", "id", id)
			if m.onPeerRemoved != nil {
				m.onPeerRemoved(id)
			}
		}
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if m.gzipEnabled {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
	}

	for id, member := range current {
		if p, exists := m.peers[id]; exists {
			p.LastSeen = time.Now()
			if p.Healthy {
				continue
			}
			// Attempt to redial unhealthy peer
			m.logger.Debug("retrying connection to unhealthy peer", "id", id, "addr", p.GRPCAddr)
			conn, err := grpc.NewClient(p.GRPCAddr, dialOpts...)
			if err == nil {
				p.Client = kratev1.NewKratePeerServiceClient(conn)
				p.Conn = conn
				p.Healthy = true
				m.conns[id] = conn
				m.logger.Debug("peer reconnected", "id", id, "addr", p.GRPCAddr)
			}
			continue
		}

		grpcAddr := member.GRPCAddr
		if grpcAddr == "" {
			grpcAddr = member.GossipAddr
		}

		conn, err := grpc.NewClient(grpcAddr, dialOpts...)

		p := &Peer{
			ID:         id,
			GRPCAddr:   grpcAddr,
			GossipAddr: member.GossipAddr,
			LastSeen:   time.Now(),
		}

		if err != nil {
			m.logger.Warn("failed to dial peer", "id", id, "addr", grpcAddr, "error", err)
			p.Healthy = false
		} else {
			p.Client = kratev1.NewKratePeerServiceClient(conn)
			p.Conn = conn
			p.Healthy = true
			m.conns[id] = conn
			m.logger.Debug("peer connected", "id", id, "addr", grpcAddr)
		}

		m.peers[id] = p
	}
}

func (m *Mesh) GetPeer(id string) (Peer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[id]
	if !ok {
		return Peer{}, false
	}
	return *p, true
}

func (m *Mesh) GetPeers() []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Peer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Healthy {
			result = append(result, *p)
		}
	}
	return result
}

func (m *Mesh) GetPeerClient(id string) (kratev1.KratePeerServiceClient, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[id]
	if !ok || !p.Healthy || p.Client == nil {
		return nil, false
	}
	return p.Client, true
}

func (m *Mesh) GossipAddrs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	addrs := make([]string, 0, len(m.peers))
	for _, p := range m.peers {
		if p.GossipAddr != "" {
			addrs = append(addrs, p.GossipAddr)
		}
	}
	return addrs
}

func (m *Mesh) PeerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, p := range m.peers {
		if p.Healthy {
			count++
		}
	}
	return count
}
