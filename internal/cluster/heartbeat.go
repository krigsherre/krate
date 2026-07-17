package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Heartbeat struct {
	membership *Membership
	info       MemberInfo
	interval   time.Duration
	logger     *slog.Logger
	OnFail     func(err error)
	stopOnce   sync.Once
	stopCh     chan struct{}
	doneCh     chan struct{}
}

func NewHeartbeat(m *Membership, info MemberInfo, interval time.Duration, logger *slog.Logger) *Heartbeat {
	if logger == nil {
		logger = slog.Default()
	}
	return &Heartbeat{
		membership: m,
		info:       info,
		interval:   interval,
		logger:     logger,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

func (h *Heartbeat) Start(ctx context.Context) {
	if err := h.membership.Register(ctx, h.info); err != nil {
		h.logger.Warn("heartbeat initial register failed", "id", h.info.ID, "error", err)
		if h.OnFail != nil {
			h.OnFail(err)
		}
	}

	go func() {
		defer close(h.doneCh)

		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := h.membership.Refresh(ctx, h.info); err != nil {
					h.logger.Warn("heartbeat refresh failed", "id", h.info.ID, "error", err)
					if h.OnFail != nil {
						h.OnFail(err)
					}
				}
			case <-h.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (h *Heartbeat) Stop() {
	h.stopOnce.Do(func() {
		close(h.stopCh)
	})
	<-h.doneCh
}
