package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type MemberInfo struct {
	ID           string            `json:"id"`
	GossipAddr   string            `json:"gossip_addr"`
	GRPCAddr     string            `json:"grpc_addr"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	RegisteredAt int64             `json:"registered_at"`
}

type Membership struct {
	client redis.UniversalClient
	ttl    time.Duration
	prefix string
	logger *slog.Logger
}

func NewMembership(client redis.UniversalClient, ttl time.Duration, logger *slog.Logger) *Membership {
	if logger == nil {
		logger = slog.Default()
	}
	return &Membership{
		client: client,
		ttl:    ttl,
		prefix: "krate:cluster:",
		logger: logger,
	}
}

func (m *Membership) Register(ctx context.Context, info MemberInfo) error {
	if info.RegisteredAt == 0 {
		info.RegisteredAt = time.Now().UnixMilli()
	}

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal member info: %w", err)
	}

	hashKey := m.prefix + "members"
	if err := m.client.HSet(ctx, hashKey, info.ID, data).Err(); err != nil {
		return fmt.Errorf("register member %s: %w", info.ID, err)
	}

	m.logger.Debug("member registered", "id", info.ID, "addr", info.GossipAddr)
	return nil
}

func (m *Membership) Deregister(ctx context.Context, id string) error {
	hashKey := m.prefix + "members"
	err := m.client.HDel(ctx, hashKey, id).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("deregister member %s: %w", id, err)
	}
	m.logger.Debug("member deregistered", "id", id)
	return nil
}

func (m *Membership) Discover(ctx context.Context) ([]MemberInfo, error) {
	hashKey := m.prefix + "members"
	vals, err := m.client.HGetAll(ctx, hashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall members: %w", err)
	}

	if len(vals) == 0 {
		return []MemberInfo{}, nil
	}

	now := time.Now().UnixMilli()
	ttlMs := m.ttl.Milliseconds()

	members := make([]MemberInfo, 0, len(vals))
	var toDelete []string

	for id, s := range vals {
		var info MemberInfo
		if err := json.Unmarshal([]byte(s), &info); err != nil {
			m.logger.Warn("failed to unmarshal member", "id", id, "error", err)
			continue
		}

		if now-info.RegisteredAt > ttlMs {
			toDelete = append(toDelete, id)
			continue
		}

		members = append(members, info)
	}

	if len(toDelete) > 0 {
		// Clean up expired members asynchronously
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := m.client.HDel(bgCtx, hashKey, toDelete...).Err(); err != nil {
				m.logger.Warn("failed to delete expired members", "error", err)
			}
		}()
	}

	return members, nil
}

func (m *Membership) Refresh(ctx context.Context, info MemberInfo) error {
	// Update RegisteredAt timestamp to keep heartbeat alive
	info.RegisteredAt = time.Now().UnixMilli()
	return m.Register(ctx, info)
}
