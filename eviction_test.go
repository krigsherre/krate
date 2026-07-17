package krate_test

import (
	"context"
	"testing"
	"time"

	"github.com/krigsherre/krate"
	"github.com/krigsherre/krate/internal/clock"
)

func TestIntegration_EvictionJanitor(t *testing.T) {
	rdb := setupRedis(t)
	key := uniqueKey(t)
	t.Cleanup(func() { cleanupKeys(t, rdb, "krate:"+key+":*") })

	fakeClock := clock.NewFakeClock(time.Now())

	l, err := krate.New(rdb,
		krate.WithInstanceID(uniqueKey(t)),
		krate.WithLimit(1000),
		krate.WithWindow(60*time.Second),
		krate.WithMaxBorrow(50),
		krate.WithMinBorrow(10),
		krate.WithProbeK(0),
		krate.WithPeerListen(":0"),
		krate.WithGossipInterval(time.Hour),
		krate.WithHeartbeatInterval(time.Hour),
		krate.WithClock(fakeClock),
		krate.WithEvictionInterval(10*time.Millisecond),
		krate.WithIdleTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	ctx := context.Background()

	ok, err := l.Allow(ctx, key)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !ok {
		t.Fatal("expected request to be allowed")
	}

	fakeClock.Advance(20 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)

	ok, err = l.Allow(ctx, key)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !ok {
		t.Fatal("expected request to be allowed")
	}

	fakeClock.Advance(60 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)

	ok, err = l.Allow(ctx, key)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !ok {
		t.Fatal("expected request to be allowed after eviction (and recreation)")
	}
}
