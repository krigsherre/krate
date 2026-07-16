package routing

import (
	"context"
	"testing"
)

func TestDefaultRouter(t *testing.T) {
	router := NewDefaultRouter()

	tests := []struct {
		name     string
		ctx      RouteContext
		expected Decision
	}{
		{
			name: "redis not exhausted",
			ctx: RouteContext{
				Key:            "test-key",
				Need:           1,
				RedisExhausted: false,
				HasPeers:       false,
			},
			expected: DecisionRedis,
		},
		{
			name: "redis exhausted with peers",
			ctx: RouteContext{
				Key:            "test-key",
				Need:           1,
				RedisExhausted: true,
				HasPeers:       true,
			},
			expected: DecisionPeer,
		},
		{
			name: "redis exhausted no peers",
			ctx: RouteContext{
				Key:            "test-key",
				Need:           1,
				RedisExhausted: true,
				HasPeers:       false,
			},
			expected: DecisionDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := router.Decide(context.Background(), &tt.ctx)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("Decide() = %v, want %v", got, tt.expected)
			}
		})
	}
}
