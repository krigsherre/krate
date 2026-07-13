package peer

import (
	"context"
	"net"
	"testing"

	kratev1 "github.com/krigsherre/krate/peer/peerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startTestServer(t *testing.T, ts *TokenServer) (addr string, cleanup func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	kratev1.RegisterKratePeerServiceServer(grpcSrv, ts)
	go grpcSrv.Serve(lis)
	return lis.Addr().String(), func() { grpcSrv.Stop() }
}

func dialTest(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestServerTransferTokens(t *testing.T) {
	ts := NewTokenServer("server-1", "", testLogger(t))
	ts.SetTokenAccessor(func(key string, requested uint64) (uint64, error) {
		return 500, nil
	})
	ts.SetTokenTransferer(func(key string, amount uint64) (uint64, error) {
		return amount, nil
	})

	addr, cleanup := startTestServer(t, ts)
	defer cleanup()

	conn := dialTest(t, addr)
	client := kratev1.NewKratePeerServiceClient(conn)

	resp, err := client.TransferTokens(context.Background(), &kratev1.TransferRequest{
		LimitKey:  "api:login",
		Requested: 100,
		OriginId:  "other-instance",
	})
	if err != nil {
		t.Fatalf("TransferTokens: %v", err)
	}
	if resp.Granted != 100 {
		t.Errorf("granted = %d, want 100", resp.Granted)
	}
}

func TestServerCycleDetection(t *testing.T) {
	ts := NewTokenServer("server-1", "", testLogger(t))
	ts.SetTokenAccessor(func(key string, requested uint64) (uint64, error) {
		return 500, nil
	})
	ts.SetTokenTransferer(func(key string, amount uint64) (uint64, error) {
		return amount, nil
	})

	addr, cleanup := startTestServer(t, ts)
	defer cleanup()

	conn := dialTest(t, addr)
	client := kratev1.NewKratePeerServiceClient(conn)

	_, err := client.TransferTokens(context.Background(), &kratev1.TransferRequest{
		LimitKey:  "api:login",
		Requested: 100,
		OriginId:  "server-1",
	})
	if err == nil {
		t.Fatal("expected error for cycle detection, got nil")
	}
}

func TestServerInsufficientSurplus(t *testing.T) {
	ts := NewTokenServer("server-1", "", testLogger(t))
	ts.SetTokenAccessor(func(key string, requested uint64) (uint64, error) {
		return 0, nil
	})
	ts.SetTokenTransferer(func(key string, amount uint64) (uint64, error) {
		return amount, nil
	})

	addr, cleanup := startTestServer(t, ts)
	defer cleanup()

	conn := dialTest(t, addr)
	client := kratev1.NewKratePeerServiceClient(conn)

	resp, err := client.TransferTokens(context.Background(), &kratev1.TransferRequest{
		LimitKey:  "api:login",
		Requested: 100,
		OriginId:  "other-instance",
	})
	if err != nil {
		t.Fatalf("TransferTokens: %v", err)
	}
	if resp.Granted != 0 {
		t.Errorf("granted = %d, want 0", resp.Granted)
	}
}

func TestServerPing(t *testing.T) {
	ts := NewTokenServer("server-1", "", testLogger(t))

	addr, cleanup := startTestServer(t, ts)
	defer cleanup()

	conn := dialTest(t, addr)
	client := kratev1.NewKratePeerServiceClient(conn)

	resp, err := client.Ping(context.Background(), &kratev1.PingRequest{})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if resp == nil {
		t.Fatal("Ping returned nil")
	}
}
