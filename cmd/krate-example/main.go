package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"

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

	metricsHandler := fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler())

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		path := string(ctx.Path())
		if path == "/metrics" {
			metricsHandler(ctx)
			return
		}
		if path != "/" {
			ctx.Error("Not Found", fasthttp.StatusNotFound)
			return
		}

		key := string(ctx.Request.Header.Peek("X-API-Key"))
		if key == "" {
			key = ctx.RemoteIP().String()
		}

		allowed, err := limiter.Allow(context.Background(), key)
		if err != nil {
			logger.Error("rate limit error", "error", err)
			ctx.Write([]byte("Hello, world!"))
			return
		}

		if !allowed {
			ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
			ctx.Write([]byte("rate limit exceeded"))
			return
		}

		ctx.Write([]byte("Hello, world!"))
	}

	srv := &fasthttp.Server{
		Handler: requestHandler,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		logger.Info("shutting down server...")
		if err := srv.Shutdown(); err != nil {
			logger.Error("shutdown error", "error", err)
		}
	}()

	logger.Info("starting fasthttp server", "addr", ":8080")
	if err := srv.ListenAndServe(":8080"); err != nil {
		logger.Error("server error", "error", err)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
