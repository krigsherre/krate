package kratev1_test

import (
	"testing"

	kratev1 "github.com/krigsherre/krate/peer/peerpb"
	"google.golang.org/protobuf/proto"
)

func TestTransferRequestRoundtrip(t *testing.T) {
	req := &kratev1.TransferRequest{
		LimitKey:  "api:login",
		Requested: 500,
		OriginId:  "node-1",
	}

	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got := &kratev1.TransferRequest{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.LimitKey != req.LimitKey {
		t.Errorf("LimitKey = %q, want %q", got.LimitKey, req.LimitKey)
	}
	if got.Requested != req.Requested {
		t.Errorf("Requested = %d, want %d", got.Requested, req.Requested)
	}
	if got.OriginId != req.OriginId {
		t.Errorf("OriginId = %q, want %q", got.OriginId, req.OriginId)
	}
}

func TestPingRoundtrip(t *testing.T) {
	data, err := proto.Marshal(&kratev1.PingRequest{})
	if err != nil {
		t.Fatalf("Marshal PingRequest: %v", err)
	}
	got := &kratev1.PingRequest{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("Unmarshal PingRequest: %v", err)
	}
}

func TestTransferRequestSize(t *testing.T) {
	req := &kratev1.TransferRequest{
		LimitKey:  "key",
		Requested: 42,
		OriginId:  "n1",
	}
	size := proto.Size(req)
	if size == 0 || size > 100 {
		t.Errorf("Size() = %d, want small positive value", size)
	}
}
