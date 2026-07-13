package krate

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	NewTicker(d time.Duration) *time.Ticker
	After(d time.Duration) <-chan time.Time
}

type Limiter struct {
	inner *limiter
}

func New(rdb redis.UniversalClient, opts ...Option) (*Limiter, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	if o.logger == nil {
		o.logger = slog.Default()
	}

	if o.instanceID == "" {
		hostname, _ := os.Hostname()
		o.instanceID = fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano())
	}

	l, err := newLimiter(rdb, o)
	if err != nil {
		return nil, err
	}
	return &Limiter{inner: l}, nil
}

func (l *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	return l.inner.Allow(ctx, key)
}

func (l *Limiter) AllowN(ctx context.Context, key string, n uint64) (bool, error) {
	return l.inner.AllowN(ctx, key, n)
}

func (l *Limiter) Close() error {
	return l.inner.Close()
}
