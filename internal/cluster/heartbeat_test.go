package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestHeartbeatStartStop(t *testing.T) {
	client := setupRedis(t)
	mem := newTestMembership(t, client, 300*time.Millisecond)

	info := MemberInfo{ID: "hb-node-1", GossipAddr: "127.0.0.1:7100"}
	hb := NewHeartbeat(mem, info, 100*time.Millisecond, testLogger(t))

	hb.Start(context.Background())
	time.Sleep(50 * time.Millisecond)

	discovered, err := mem.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(discovered) != 1 {
		t.Fatalf("after Start: Discover returned %d members, want 1", len(discovered))
	}
	if discovered[0].ID != "hb-node-1" {
		t.Errorf("member ID = %q, want %q", discovered[0].ID, "hb-node-1")
	}

	hb.Stop()

	time.Sleep(400 * time.Millisecond)

	discovered, err = mem.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover after stop: %v", err)
	}
	if len(discovered) != 0 {
		t.Errorf("after Stop + TTL: Discover returned %d members, want 0", len(discovered))
	}
}

func TestHeartbeatRefreshes(t *testing.T) {
	client := setupRedis(t)
	mem := newTestMembership(t, client, 200*time.Millisecond)

	info := MemberInfo{ID: "hb-refresh-1", GossipAddr: "127.0.0.1:7100"}
	hb := NewHeartbeat(mem, info, 50*time.Millisecond, testLogger(t))

	hb.Start(context.Background())
	defer hb.Stop()

	time.Sleep(500 * time.Millisecond)

	discovered, err := mem.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(discovered) != 1 {
		t.Errorf("after 500ms with 200ms TTL: Discover returned %d members, want 1", len(discovered))
	}
}

func TestHeartbeatOnFailCallback(t *testing.T) {
	badClient := redis.NewClient(&redis.Options{
		Addr:         "localhost:1",
		DialTimeout:  10 * time.Millisecond,
		ReadTimeout:  10 * time.Millisecond,
		WriteTimeout: 10 * time.Millisecond,
	})
	defer badClient.Close()

	mem := NewMembership(badClient, 1*time.Second, testLogger(t))
	info := MemberInfo{ID: "fail-node", GossipAddr: "127.0.0.1:7100"}

	var failCount atomic.Int32
	hb := NewHeartbeat(mem, info, 50*time.Millisecond, testLogger(t))
	hb.OnFail = func(err error) {
		failCount.Add(1)
	}

	hb.Start(context.Background())
	time.Sleep(300 * time.Millisecond)

	hb.Stop()

	if got := failCount.Load(); got == 0 {
		t.Error("OnFail was never called")
	} else {
		t.Logf("OnFail called %d times", got)
	}
}
