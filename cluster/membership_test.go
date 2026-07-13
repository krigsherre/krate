package cluster

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func setupRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	addr := os.Getenv("KRATE_TEST_REDIS")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func uniqueID(t *testing.T) string {
	t.Helper()
	return "t" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func newTestMembership(t *testing.T, client redis.UniversalClient, ttl time.Duration) *Membership {
	t.Helper()
	prefix := "krate:cluster:" + uniqueID(t) + ":"
	t.Cleanup(func() {
		ctx := context.Background()
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
			if err != nil {
				return
			}
			if len(keys) > 0 {
				client.Del(ctx, keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	})
	return &Membership{
		client: client,
		ttl:    ttl,
		prefix: prefix,
		logger: testLogger(t),
	}
}

func idsOf(members []MemberInfo) map[string]bool {
	ids := make(map[string]bool, len(members))
	for _, m := range members {
		ids[m.ID] = true
	}
	return ids
}

func TestRegisterAndDiscover(t *testing.T) {
	client := setupRedis(t)
	mem := newTestMembership(t, client, 30*time.Second)
	ctx := context.Background()

	infos := []MemberInfo{
		{ID: "node-1", GossipAddr: "10.0.0.1:7100", GRPCAddr: "10.0.0.1:7200"},
		{ID: "node-2", GossipAddr: "10.0.0.2:7100", GRPCAddr: "10.0.0.2:7200"},
		{ID: "node-3", GossipAddr: "10.0.0.3:7100", GRPCAddr: "10.0.0.3:7200"},
	}

	for _, info := range infos {
		if err := mem.Register(ctx, info); err != nil {
			t.Fatalf("Register(%s): %v", info.ID, err)
		}
	}

	discovered, err := mem.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(discovered) != 3 {
		t.Fatalf("Discover returned %d members, want 3", len(discovered))
	}

	found := idsOf(discovered)
	for _, info := range infos {
		if !found[info.ID] {
			t.Errorf("member %s not found in discover results", info.ID)
		}
	}
}

func TestDeregister(t *testing.T) {
	client := setupRedis(t)
	mem := newTestMembership(t, client, 30*time.Second)
	ctx := context.Background()

	if err := mem.Register(ctx, MemberInfo{ID: "node-1", GossipAddr: "10.0.0.1:7100"}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := mem.Deregister(ctx, "node-1"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	discovered, err := mem.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(discovered) != 0 {
		t.Errorf("Discover returned %d members after deregister, want 0", len(discovered))
	}
}

func TestDiscoverEmpty(t *testing.T) {
	client := setupRedis(t)
	mem := newTestMembership(t, client, 30*time.Second)
	ctx := context.Background()

	discovered, err := mem.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if discovered == nil {
		t.Fatal("Discover returned nil, want empty slice")
	}
	if len(discovered) != 0 {
		t.Errorf("Discover returned %d members, want 0", len(discovered))
	}
}

func TestRegisterOverwrite(t *testing.T) {
	client := setupRedis(t)
	mem := newTestMembership(t, client, 30*time.Second)
	ctx := context.Background()

	info1 := MemberInfo{ID: "node-1", GossipAddr: "10.0.0.1:7100"}
	info2 := MemberInfo{ID: "node-1", GossipAddr: "10.0.0.99:7100"}

	if err := mem.Register(ctx, info1); err != nil {
		t.Fatalf("Register(1): %v", err)
	}
	if err := mem.Register(ctx, info2); err != nil {
		t.Fatalf("Register(2): %v", err)
	}

	discovered, err := mem.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(discovered) != 1 {
		t.Fatalf("Discover returned %d members, want 1", len(discovered))
	}
	if discovered[0].GossipAddr != "10.0.0.99:7100" {
		t.Errorf("GossipAddr = %q, want %q", discovered[0].GossipAddr, "10.0.0.99:7100")
	}
}

func TestTTLExpiry(t *testing.T) {
	client := setupRedis(t)
	mem := newTestMembership(t, client, 200*time.Millisecond)
	ctx := context.Background()

	if err := mem.Register(ctx, MemberInfo{ID: "node-1", GossipAddr: "10.0.0.1:7100"}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	time.Sleep(350 * time.Millisecond)

	discovered, err := mem.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(discovered) != 0 {
		t.Errorf("Discover returned %d members after TTL expiry, want 0", len(discovered))
	}
}

func TestConcurrentRegister(t *testing.T) {
	client := setupRedis(t)
	mem := newTestMembership(t, client, 30*time.Second)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			info := MemberInfo{
				ID:         "node-" + strconv.Itoa(n),
				GossipAddr: "10.0.0." + strconv.Itoa(n) + ":7100",
			}
			if err := mem.Register(ctx, info); err != nil {
				t.Errorf("Register(node-%d): %v", n, err)
			}
		}(i)
	}
	wg.Wait()

	discovered, err := mem.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(discovered) != 20 {
		t.Errorf("Discover returned %d members, want 20", len(discovered))
	}
}
