package peer

import (
	"context"
	"testing"
	"time"

	kratev1 "github.com/krigsherre/krate/internal/peer/peerpb"
)

func TestClientRequestTokens(t *testing.T) {
	ts := NewTokenServer("server", "", testLogger(t))
	ts.SetTokenAccessor(func(key string, requested uint64) (uint64, error) {
		return requested, nil
	})
	ts.SetTokenTransferer(func(key string, amount uint64) (uint64, error) {
		return amount, nil
	})

	addr, cleanup := startTestServer(t, ts)
	defer cleanup()

	conn := dialTest(t, addr)
	grpcClient := kratev1.NewKratePeerServiceClient(conn)

	pc := NewPeerClient(100*time.Millisecond, testLogger(t))

	granted, err := pc.RequestTokens(context.Background(), grpcClient, "other", "key", 100)
	if err != nil {
		t.Fatalf("RequestTokens: %v", err)
	}
	if granted != 100 {
		t.Errorf("granted = %d, want 100", granted)
	}
}

func TestClientTimeout(t *testing.T) {
	ts := NewTokenServer("server", "", testLogger(t))
	ts.SetTokenAccessor(func(key string, requested uint64) (uint64, error) {
		time.Sleep(2 * time.Second)
		return requested, nil
	})
	ts.SetTokenTransferer(func(key string, amount uint64) (uint64, error) {
		return amount, nil
	})

	addr, cleanup := startTestServer(t, ts)
	defer cleanup()

	conn := dialTest(t, addr)
	grpcClient := kratev1.NewKratePeerServiceClient(conn)

	pc := NewPeerClient(50*time.Millisecond, testLogger(t))

	_, err := pc.RequestTokens(context.Background(), grpcClient, "other", "key", 100)
	if err == nil {
		t.Error("expected timeout error, got nil")
	}
}
