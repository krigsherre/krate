package routing

import "context"

type Decision int

const (
	DecisionRedis Decision = iota
	DecisionPeer
	DecisionNone
)

func (d Decision) String() string {
	switch d {
	case DecisionRedis:
		return "Redis"
	case DecisionPeer:
		return "Peer"
	default:
		return "None"
	}
}

type RouteContext struct {
	Key            string
	Need           uint64
	RedisExhausted bool
	HasPeers       bool
}

type Router interface {
	Decide(ctx context.Context, rc *RouteContext) (Decision, error)
}

type DefaultRouter struct{}

func NewDefaultRouter() *DefaultRouter {
	return &DefaultRouter{}
}

func (r *DefaultRouter) Decide(ctx context.Context, rc *RouteContext) (Decision, error) {
	if !rc.RedisExhausted {
		return DecisionRedis, nil
	}
	if rc.HasPeers {
		return DecisionPeer, nil
	}
	return DecisionNone, nil
}
