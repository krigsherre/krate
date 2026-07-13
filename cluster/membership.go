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

	key := m.prefix + info.ID
	if err := m.client.Set(ctx, key, data, m.ttl).Err(); err != nil {
		return fmt.Errorf("register member %s: %w", info.ID, err)
	}

	m.logger.Debug("member registered", "id", info.ID, "addr", info.GossipAddr)
	return nil
}

func (m *Membership) Deregister(ctx context.Context, id string) error {
	key := m.prefix + id
	err := m.client.Del(ctx, key).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("deregister member %s: %w", id, err)
	}
	m.logger.Debug("member deregistered", "id", id)
	return nil
}

func (m *Membership) Discover(ctx context.Context) ([]MemberInfo, error) {
	pattern := m.prefix + "*"
	var allKeys []string

	var cursor uint64
	for {
		keys, nextCursor, err := m.client.Scan(ctx, cursor, pattern, 256).Result()
		if err != nil {
			return nil, fmt.Errorf("scan members: %w", err)
		}
		allKeys = append(allKeys, keys...)
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	if len(allKeys) == 0 {
		return []MemberInfo{}, nil
	}

	vals, err := m.client.MGet(ctx, allKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("mget members: %w", err)
	}

	members := make([]MemberInfo, 0, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			m.logger.Warn("unexpected MGET value type", "key", allKeys[i])
			continue
		}
		var info MemberInfo
		if err := json.Unmarshal([]byte(s), &info); err != nil {
			m.logger.Warn("failed to unmarshal member", "key", allKeys[i], "error", err)
			continue
		}
		members = append(members, info)
	}

	return members, nil
}

func (m *Membership) Refresh(ctx context.Context, info MemberInfo) error {
	return m.Register(ctx, info)
}
