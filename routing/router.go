package routing

import (
	"context"

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
	Init(gossiper *sketch.Gossiper)
}

type DefaultRouter struct{}

func NewDefaultRouter() *DefaultRouter {
	return &DefaultRouter{}
}

func (r *DefaultRouter) Init(gossiper *sketch.Gossiper) {}

func (r *DefaultRouter) Decide(ctx context.Context, rc *RouteContext) (Decision, error) {
	if rc.HasPeers {
		return DecisionPeer, nil
	}
	if rc.RedisExhausted {
		return DecisionDeny, nil
	}
	return DecisionRedis, nil
}
