package krate_test

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/krigsherre/krate"
)

func setupRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	addr := os.Getenv("KRATE_TEST_REDIS")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func cleanupKeys(t *testing.T, rdb redis.UniversalClient, patterns ...string) {
	t.Helper()
	ctx := context.Background()
	for _, p := range patterns {
		var cursor uint64
		for {
			keys, next, err := rdb.Scan(ctx, cursor, p, 100).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				rdb.Del(ctx, keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
}

func uniqueKey(t *testing.T) string {
	t.Helper()
	return "t" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func TestIntegration_AllowReject(t *testing.T) {
	rdb := setupRedis(t)
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, rdb, "krate:"+key+":*") })

	l, err := krate.New(rdb,
		krate.WithInstanceID(uniqueKey(t)),
		krate.WithLimit(100),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(100),
		krate.WithMinBorrow(10),
		krate.WithProbeK(0),
		krate.WithPeerListen(":0"),
		krate.WithGossipInterval(time.Hour),
		krate.WithHeartbeatInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	ctx := context.Background()

	allowed := 0
	for i := 0; i < 100; i++ {
		ok, err := l.Allow(ctx, key)
		if err != nil {
			t.Fatalf("Allow(%d): %v", i, err)
		}
		if ok {
			allowed++
		}
	}

	if allowed != 100 {
		t.Errorf("first 100: allowed = %d, want 100", allowed)
	}

	rejected := 0
	for i := 0; i < 20; i++ {
		ok, err := l.Allow(ctx, key)
		if err != nil {
			t.Fatalf("Allow(%d): %v", i, err)
		}
		if !ok {
			rejected++
		}
	}

	if rejected == 0 {
		t.Error("expected some rejections after exhausting pool")
	}
}

func TestIntegration_ConcurrentAccess(t *testing.T) {
	rdb := setupRedis(t)
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, rdb, "krate:"+key+":*") })

	l, err := krate.New(rdb,
		krate.WithInstanceID(uniqueKey(t)),
		krate.WithLimit(5000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(5000),
		krate.WithMinBorrow(100),
		krate.WithProbeK(0),
		krate.WithPeerListen(":0"),
		krate.WithGossipInterval(time.Hour),
		krate.WithHeartbeatInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	ctx := context.Background()
	var totalAllowed atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				ok, err := l.Allow(ctx, key)
				if err != nil {
					return
				}
				if ok {
					totalAllowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	allowed := totalAllowed.Load()
	if allowed == 0 {
		t.Error("no requests allowed")
	}
	if allowed > 5000 {
		t.Errorf("allowed = %d, exceeds limit of 5000", allowed)
	}
	t.Logf("allowed = %d / 1000", allowed)
}

func TestIntegration_GracefulShutdown(t *testing.T) {
	rdb := setupRedis(t)
	key := uniqueKey(t)
	instID := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, rdb, "krate:"+key+":*") })

	l, err := krate.New(rdb,
		krate.WithInstanceID(instID),
		krate.WithLimit(500),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(500),
		krate.WithMinBorrow(50),
		krate.WithProbeK(0),
		krate.WithPeerListen(":0"),
		krate.WithGossipInterval(time.Hour),
		krate.WithHeartbeatInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	l.Allow(ctx, key)
	l.Allow(ctx, key)
	l.Allow(ctx, key)

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	clusterKey := "krate:cluster:" + instID
	exists, err := rdb.Exists(ctx, clusterKey).Result()
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists != 0 {
		t.Error("cluster key still exists after Close")
	}
}

func TestIntegration_MetricsPopulated(t *testing.T) {
	rdb := setupRedis(t)
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, rdb, "krate:"+key+":*") })

	l, err := krate.New(rdb,
		krate.WithInstanceID(uniqueKey(t)),
		krate.WithLimit(1000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(1000),
		krate.WithMinBorrow(100),
		krate.WithProbeK(0),
		krate.WithPeerListen(":0"),
		krate.WithGossipInterval(time.Hour),
		krate.WithHeartbeatInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		l.Allow(ctx, key)
	}
}

func TestIntegration_WindowReset(t *testing.T) {
	rdb := setupRedis(t)
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, rdb, "krate:"+key+":*") })

	l, err := krate.New(rdb,
		krate.WithInstanceID(uniqueKey(t)),
		krate.WithLimit(10),
		krate.WithWindow(1*time.Second),
		krate.WithMaxBorrow(10),
		krate.WithMinBorrow(5),
		krate.WithProbeK(0),
		krate.WithPeerListen(":0"),
		krate.WithGossipInterval(time.Hour),
		krate.WithHeartbeatInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		l.Allow(ctx, key)
	}

	ok, _ := l.Allow(ctx, key)
	if ok {
		t.Error("expected rejection after exhausting pool")
	}

	time.Sleep(1200 * time.Millisecond)

	ok, err = l.Allow(ctx, key)
	if err != nil {
		t.Fatalf("Allow after reset: %v", err)
	}
	if !ok {
		t.Error("expected allow after window reset")
	}
}
