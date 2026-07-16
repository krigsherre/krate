package routing

import (
	"context"
	"log/slog"

	"github.com/krigsherre/krate/sketch"
)

type Decision int

const (
	DecisionRedis Decision = iota
	DecisionPeer
	DecisionDeny
)

type RouteContext struct {
	Key            string
	Need           uint64
	RedisExhausted bool
	HasPeers       bool
}

type Router interface {
	Decide(ctx context.Context, rc *RouteContext) (Decision, error)
	Init(gossiper *sketch.Gossiper, logger *slog.Logger)
}

type DefaultRouter struct{}

func NewDefaultRouter() *DefaultRouter {
	return &DefaultRouter{}
}

func (r *DefaultRouter) Init(gossiper *sketch.Gossiper, logger *slog.Logger) {}

func (r *DefaultRouter) Decide(ctx context.Context, rc *RouteContext) (Decision, error) {
	if rc.HasPeers {
		return DecisionPeer, nil
	}
	if rc.RedisExhausted {
		return DecisionDeny, nil
	}
	return DecisionRedis, nil
}
