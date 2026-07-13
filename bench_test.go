package krate_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/krigsherre/krate"
	"github.com/krigsherre/krate/sketch"
)

func redisClient(tb testing.TB) *redis.Client {
	tb.Helper()
	addr := os.Getenv("KRATE_TEST_REDIS")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		tb.Skipf("redis not available at %s: %v", addr, err)
	}
	return rdb
}

func benchLimiter(tb testing.TB, rdb *redis.Client, key string, extra ...krate.Option) (*krate.Limiter, string) {
	tb.Helper()
	instID := fmt.Sprintf("bench-%s-%d", tb.Name(), time.Now().UnixNano())
	base := []krate.Option{
		krate.WithInstanceID(instID),
		krate.WithPeerListen(":0"),
		krate.WithGossipInterval(time.Hour),
		krate.WithHeartbeatInterval(time.Hour),
		krate.WithProbeK(0),
	}
	l, err := krate.New(rdb, append(base, extra...)...)
	if err != nil {
		tb.Fatalf("krate.New: %v", err)
	}
	cleanupKey := key
	tb.Cleanup(func() {
		l.Close()
		ctx := context.Background()
		var cursor uint64
		for {
			keys, next, _ := rdb.Scan(ctx, cursor, "krate:"+cleanupKey+"*", 200).Result()
			if len(keys) > 0 {
				rdb.Del(ctx, keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	})
	return l, instID
}

func BenchmarkCMS_Query(b *testing.B) {
	cms := sketch.NewCMS(256, 4, 42)
	for i := 0; i < 1000; i++ {
		cms.Add(fmt.Sprintf("key-%d", i), uint64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cms.Query(fmt.Sprintf("key-%d", i%1000))
	}
}

func BenchmarkCMS_QueryParallel(b *testing.B) {
	cms := sketch.NewCMS(256, 4, 42)
	for i := 0; i < 1000; i++ {
		cms.Add(fmt.Sprintf("key-%d", i), uint64(i))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cms.Query(fmt.Sprintf("key-%d", i%1000))
			i++
		}
	})
}

func BenchmarkCMS_Merge(b *testing.B) {
	cms1 := sketch.NewCMS(256, 4, 42)
	cms2 := sketch.NewCMS(256, 4, 42)
	for i := 0; i < 100; i++ {
		cms1.Add(fmt.Sprintf("key-%d", i), uint64(i*10))
		cms2.Add(fmt.Sprintf("key-%d", i), uint64(i*5))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := cms1.Clone()
		c.Merge(cms2, sketch.MergeMax)
	}
}

func BenchmarkCMS_Compress(b *testing.B) {
	cms := sketch.NewCMS(256, 4, 42)
	for i := 0; i < 100; i++ {
		cms.Add(fmt.Sprintf("key-%d", i), uint64(i*10))
	}
	data := cms.Marshal()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sketch.Compress(data)
	}
}

func BenchmarkCMS_MarshalUnmarshal(b *testing.B) {
	cms := sketch.NewCMS(256, 4, 42)
	for i := 0; i < 100; i++ {
		cms.Add(fmt.Sprintf("key-%d", i), uint64(i*10))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := cms.Marshal()
		sketch.Unmarshal(data)
	}
}

func BenchmarkAllow_LocalHit(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-local-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(100_000_000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(50_000_000),
		krate.WithMinBorrow(10_000_000),
	)

	ctx := context.Background()
	l.AllowN(ctx, key, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Allow(ctx, key)
	}
}

func BenchmarkAllow_LocalHit_Parallel(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-local-par-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(1_000_000_000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(500_000_000),
		krate.WithMinBorrow(100_000_000),
	)

	ctx := context.Background()
	l.AllowN(ctx, key, 1)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow(ctx, key)
		}
	})
}

func BenchmarkAllow_PreBorrow_Enabled(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-preborrow-on-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(100_000_000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(50_000_000),
		krate.WithMinBorrow(10_000_000),
		krate.WithPreBorrowEnabled(true),
		krate.WithPreBorrowThreshold(0.20),
	)

	ctx := context.Background()
	l.AllowN(ctx, key, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Allow(ctx, key)
	}
}

func BenchmarkAllow_PreBorrow_Disabled(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-preborrow-off-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(100_000_000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(50_000_000),
		krate.WithMinBorrow(10_000_000),
		krate.WithPreBorrowEnabled(false),
	)

	ctx := context.Background()
	l.AllowN(ctx, key, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Allow(ctx, key)
	}
}

func BenchmarkAllow_RedisBorrow(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-redis-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(1_000_000_000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(1),
		krate.WithMinBorrow(1),
		krate.WithAdaptiveBorrow(false),
		krate.WithPreBorrowEnabled(false),
	)

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Allow(ctx, key)
	}
}

func BenchmarkAllow_RedisBorrow_Parallel(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-redis-par-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(1_000_000_000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(1),
		krate.WithMinBorrow(1),
		krate.WithAdaptiveBorrow(false),
		krate.WithPreBorrowEnabled(false),
	)

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow(ctx, key)
		}
	})
}

func BenchmarkAllow_RedisZeroBypass(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-rzb-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(0),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(100),
		krate.WithMinBorrow(10),
		krate.WithPreBorrowEnabled(false),
	)

	ctx := context.Background()
	l.Allow(ctx, key)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Allow(ctx, key)
	}
	b.ReportMetric(0, "redis_calls/op")
}

func BenchmarkAllow_PeerAcquire(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-peer-%d", time.Now().UnixNano())

	donor, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(1_000_000_000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(500_000_000),
		krate.WithMinBorrow(100_000_000),
		krate.WithPreBorrowEnabled(false),
	)
	donor.AllowN(context.Background(), key, 1)

	requester, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(1),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(1),
		krate.WithMinBorrow(1),
		krate.WithAdaptiveBorrow(false),
		krate.WithPreBorrowEnabled(false),
		krate.WithProbeK(1),
		krate.WithGossipInterval(10*time.Millisecond),
	)
	time.Sleep(150 * time.Millisecond)

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		requester.Allow(ctx, key)
	}
}

func BenchmarkAllow_Rejected(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-rejected-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(0),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(100),
		krate.WithMinBorrow(10),
		krate.WithPreBorrowEnabled(false),
	)

	ctx := context.Background()
	l.Allow(ctx, key)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Allow(ctx, key)
	}
}

func BenchmarkAllow_Rejected_Parallel(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-rej-par-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(0),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(100),
		krate.WithMinBorrow(10),
		krate.WithPreBorrowEnabled(false),
	)

	ctx := context.Background()
	l.Allow(ctx, key)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow(ctx, key)
		}
	})
}

func BenchmarkAllow_WindowReset(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-reset-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, key,
		krate.WithLimit(1_000_000),
		krate.WithWindow(1*time.Millisecond),
		krate.WithMaxBorrow(500_000),
		krate.WithMinBorrow(100_000),
		krate.WithPreBorrowEnabled(false),
	)

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Allow(ctx, key)
	}
}

func BenchmarkAllow_HotKey_10K_Keys(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	const numKeys = 10_000
	prefix := fmt.Sprintf("bench-hk-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, prefix,
		krate.WithLimit(1_000_000_000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(100_000_000),
		krate.WithMinBorrow(50_000_000),
		krate.WithPreBorrowEnabled(false),
	)

	ctx := context.Background()
	for i := 0; i < numKeys; i++ {
		l.AllowN(ctx, fmt.Sprintf("%s:key:%d", prefix, i), 1)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Allow(ctx, fmt.Sprintf("%s:key:%d", prefix, i%numKeys))
	}
}

func BenchmarkAllow_HotKey_10K_Keys_Parallel(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	const numKeys = 10_000
	prefix := fmt.Sprintf("bench-hkpar-%d", time.Now().UnixNano())
	l, _ := benchLimiter(b, rdb, prefix,
		krate.WithLimit(1_000_000_000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(100_000_000),
		krate.WithMinBorrow(50_000_000),
		krate.WithPreBorrowEnabled(false),
	)

	ctx := context.Background()
	for i := 0; i < numKeys; i++ {
		l.AllowN(ctx, fmt.Sprintf("%s:key:%d", prefix, i), 1)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			l.Allow(ctx, fmt.Sprintf("%s:key:%d", prefix, i%numKeys))
			i++
		}
	})
}

func BenchmarkRedisOnly_Allow(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-rdonly-%d", time.Now().UnixNano())
	defer func() {
		rdb.Del(context.Background(), key)
	}()

	script := redis.NewScript(`
		local c = tonumber(redis.call('GET', KEYS[1]) or '0')
		local l = tonumber(ARGV[1])
		if c >= l then return 0 end
		local n = redis.call('INCR', KEYS[1])
		if n == 1 then redis.call('EXPIRE', KEYS[1], 60) end
		if n > l then redis.call('DECR', KEYS[1]); return 0 end
		return 1
	`)

	ctx := context.Background()
	limit := int64(1_000_000_000)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		script.Run(ctx, rdb, []string{key}, limit).Int64()
	}
}

func BenchmarkRedisOnly_Allow_Parallel(b *testing.B) {
	rdb := redisClient(b)
	defer rdb.Close()

	key := fmt.Sprintf("bench-rdonly-par-%d", time.Now().UnixNano())
	defer func() {
		rdb.Del(context.Background(), key)
	}()

	script := redis.NewScript(`
		local c = tonumber(redis.call('GET', KEYS[1]) or '0')
		local l = tonumber(ARGV[1])
		if c >= l then return 0 end
		local n = redis.call('INCR', KEYS[1])
		if n == 1 then redis.call('EXPIRE', KEYS[1], 60) end
		if n > l then redis.call('DECR', KEYS[1]); return 0 end
		return 1
	`)

	ctx := context.Background()
	limit := int64(1_000_000_000)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			script.Run(ctx, rdb, []string{key}, limit).Int64()
		}
	})
}
