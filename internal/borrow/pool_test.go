package borrow

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

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

func uniqueKey(t *testing.T) string {
	t.Helper()
	return "test-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func cleanupKeys(t *testing.T, client redis.UniversalClient, pattern string) {
	t.Helper()
	ctx := context.Background()
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, 100).Result()
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
}

func TestBorrowGranted(t *testing.T) {
	client := setupRedis(t)
	rp := NewRedisPool(client)
	ctx := context.Background()
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, client, "krate:"+key+":*") })

	client.Set(ctx, PoolKey(key), 1000, 0)

	granted, err := rp.Borrow(ctx, key, "inst-1", 100, 30000)
	if err != nil {
		t.Fatalf("Borrow: %v", err)
	}
	if granted != 100 {
		t.Errorf("granted = %d, want 100", granted)
	}

	remaining, _ := client.Get(ctx, PoolKey(key)).Int64()
	if remaining != 900 {
		t.Errorf("pool remaining = %d, want 900", remaining)
	}

	borrowed, _ := client.Get(ctx, BorrowedKey(key, "inst-1")).Int64()
	if borrowed != 100 {
		t.Errorf("borrowed = %d, want 100", borrowed)
	}
}

func TestBorrowExhausted(t *testing.T) {
	client := setupRedis(t)
	rp := NewRedisPool(client)
	ctx := context.Background()
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, client, "krate:"+key+":*") })

	client.Set(ctx, PoolKey(key), 0, 0)

	granted, err := rp.Borrow(ctx, key, "inst-1", 100, 30000)
	if err != nil {
		t.Fatalf("Borrow: %v", err)
	}
	if granted != 0 {
		t.Errorf("granted = %d, want 0 (exhausted)", granted)
	}
}

func TestBorrowPartial(t *testing.T) {
	client := setupRedis(t)
	rp := NewRedisPool(client)
	ctx := context.Background()
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, client, "krate:"+key+":*") })

	client.Set(ctx, PoolKey(key), 50, 0)

	granted, err := rp.Borrow(ctx, key, "inst-1", 1000, 30000)
	if err != nil {
		t.Fatalf("Borrow: %v", err)
	}
	if granted != 50 {
		t.Errorf("granted = %d, want 50", granted)
	}

	remaining, _ := client.Get(ctx, PoolKey(key)).Int64()
	if remaining != 0 {
		t.Errorf("pool remaining = %d, want 0", remaining)
	}
}



func TestWindowReset(t *testing.T) {
	client := setupRedis(t)
	rp := NewRedisPool(client)
	ctx := context.Background()
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, client, "krate:"+key+":*") })

	client.Set(ctx, PoolKey(key), 100, 0)
	client.Set(ctx, WindowStartKey(key), 1000000, 0)

	err := rp.ResetWindow(ctx, key, 5000, 2000000)
	if err != nil {
		t.Fatalf("ResetWindow: %v", err)
	}

	pool, _ := client.Get(ctx, PoolKey(key)).Int64()
	if pool != 5000 {
		t.Errorf("pool = %d, want 5000", pool)
	}

	ws, _ := client.Get(ctx, WindowStartKey(key)).Int64()
	if ws != 2000000 {
		t.Errorf("window_start = %d, want 2000000", ws)
	}
}

func TestConcurrentBorrows(t *testing.T) {
	client := setupRedis(t)
	rp := NewRedisPool(client)
	ctx := context.Background()
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, client, "krate:"+key+":*") })

	client.Set(ctx, PoolKey(key), 1000, 0)

	var wg sync.WaitGroup
	var totalGranted atomic.Int64

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := "inst-" + strconv.Itoa(n)
			granted, err := rp.Borrow(ctx, key, id, 100, 30000)
			if err != nil {
				return
			}
			totalGranted.Add(int64(granted))
		}(i)
	}
	wg.Wait()

	if got := totalGranted.Load(); got > 1000 {
		t.Errorf("total granted = %d, exceeds pool of 1000", got)
	}
}

func TestGetState(t *testing.T) {
	client := setupRedis(t)
	rp := NewRedisPool(client)
	ctx := context.Background()
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, client, "krate:"+key+":*") })

	client.Set(ctx, PoolKey(key), 500, 0)
	client.Set(ctx, WindowStartKey(key), 1000000, 0)
	client.HSet(ctx, ConfigKey(key), map[string]interface{}{
		"limit":     "1000",
		"window_ms": "60000",
	})

	state, err := rp.GetState(ctx, key)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}

	if state.Remaining != 500 {
		t.Errorf("Remaining = %d, want 500", state.Remaining)
	}
	if state.WindowStart != 1000000 {
		t.Errorf("WindowStart = %d, want 1000000", state.WindowStart)
	}
	if state.Limit != 1000 {
		t.Errorf("Limit = %d, want 1000", state.Limit)
	}
	if state.WindowMs != 60000 {
		t.Errorf("WindowMs = %d, want 60000", state.WindowMs)
	}
}

func TestLeaseTTL(t *testing.T) {
	client := setupRedis(t)
	rp := NewRedisPool(client)
	ctx := context.Background()
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, client, "krate:"+key+":*") })

	client.Set(ctx, PoolKey(key), 1000, 0)

	granted, err := rp.Borrow(ctx, key, "inst-1", 100, 100)
	if err != nil {
		t.Fatalf("Borrow: %v", err)
	}
	if granted != 100 {
		t.Fatalf("granted = %d, want 100", granted)
	}

	time.Sleep(200 * time.Millisecond)

	exists, err := client.Exists(ctx, BorrowedKey(key, "inst-1")).Result()
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists != 0 {
		t.Error("borrowed key did not expire after TTL")
	}
}
