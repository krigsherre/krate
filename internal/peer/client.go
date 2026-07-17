package peer

import (
	"context"
	"log/slog"
	"time"

	kratev1 "github.com/krigsherre/krate/internal/peer/peerpb"
)

type PeerClient struct {
	timeout time.Duration
	logger  *slog.Logger
}

func NewPeerClient(timeout time.Duration, logger *slog.Logger) *PeerClient {
	if logger == nil {
		logger = slog.Default()
	}
	return &PeerClient{timeout: timeout, logger: logger}
}

func (pc *PeerClient) RequestTokens(ctx context.Context, client kratev1.KratePeerServiceClient, originID, limitKey string, need uint64) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, pc.timeout)
	defer cancel()

	resp, err := client.TransferTokens(ctx, &kratev1.TransferRequest{
		LimitKey:  limitKey,
		Requested: need,
		OriginId:  originID,
	})
	if err != nil {
		return 0, err
	}
	return resp.Granted, nil
}
