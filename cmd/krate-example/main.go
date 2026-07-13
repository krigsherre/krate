package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/krigsherre/krate"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	rdb := redis.NewClient(&redis.Options{
		Addr: envOrDefault("REDIS_ADDR", "localhost:6379"),
	})

	limiter, err := krate.New(rdb,
		krate.WithInstanceID(envOrDefault("INSTANCE_ID", "instance-1")),
		krate.WithLimit(1000),
		krate.WithWindow(60*time.Second),
		krate.WithWindowType(krate.Fixed),
		krate.WithMinBorrow(50),
		krate.WithMaxBorrow(200),
		krate.WithAdaptiveBorrow(true),
		krate.WithPeerListen(envOrDefault("PEER_ADDR", ":7100")),
		krate.WithLogger(logger),
		krate.WithMetrics(nil),
	)
	if err != nil {
		logger.Error("failed to create limiter", "error", err)
		os.Exit(1)
	}
	defer limiter.Close()

	rateLimit := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-API-Key")
			if key == "" {
				key = r.RemoteAddr
			}

			allowed, err := limiter.Allow(r.Context(), key)
			if err != nil {
				logger.Error("rate limit error", "error", err)
				next.ServeHTTP(w, r)
				return
			}

			if !allowed {
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte("rate limit exceeded"))
				return
			}

			next.ServeHTTP(w, r)
		})
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello, world!"))
	})
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    ":8080",
		Handler: rateLimit(mux),
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	logger.Info("starting server", "addr", ":8080")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("server error", "error", err)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
