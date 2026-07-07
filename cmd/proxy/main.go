package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/hibiken/asynq"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/config"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/relay"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/server"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	// Initialize logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("Starting ActivityPub Relay Proxy...")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	slog.Info("Configuration loaded successfully", "domain", cfg.Domain, "port", cfg.Port)

	// Parse Private Key
	privKey, err := relay.ParsePrivateKey(cfg.PrivateKeyPem)
	if err != nil {
		slog.Error("Failed to parse private key", "error", err)
		os.Exit(1)
	}

	// Generate Public Key PEM for Actor Endpoint
	pubKeyPem, err := relay.GetPublicKeyPem(privKey)
	if err != nil {
		slog.Error("Failed to generate public key PEM", "error", err)
		os.Exit(1)
	}

	// Setup Redis client for general caching & state management
	redisOpt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		slog.Error("Failed to parse Redis URL for go-redis", "error", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(redisOpt)
	defer rdb.Close()

	// Parse Redis URL for Asynq (asynq requires RedisConnOpt)
	asynqRedisOpt, err := asynq.ParseRedisURI(cfg.RedisURL)
	if err != nil {
		slog.Error("Failed to parse Redis URL for Asynq", "error", err)
		os.Exit(1)
	}

	// Context for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Setup Asynq client for enqueuing tasks
	asynqClient := asynq.NewClient(asynqRedisOpt)
	defer asynqClient.Close()

	// 1. Setup & Start Asynq (Worker) Server
	srv := asynq.NewServer(
		asynqRedisOpt,
		asynq.Config{
			Concurrency: 10,
			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},
		},
	)

	// Create worker handlers and register them with ServeMux
	w := worker.New(cfg, rdb, asynqClient, privKey)
	mux := asynq.NewServeMux()
	w.RegisterHandlers(mux)

	// Start Asynq worker in a goroutine
	go func() {
		slog.Info("Starting Asynq worker server...")
		if err := srv.Run(mux); err != nil {
			slog.Error("Asynq server error", "error", err)
		}
	}()

	// 2. Setup & Start HTTP Server
	srvHandler := server.New(cfg, rdb, asynqClient, privKey, pubKeyPem)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Mount ActivityPub routes
	r.Mount("/", srvHandler.Routes())

	httpAddr := fmt.Sprintf(":%d", cfg.Port)
	httpSrv := &http.Server{
		Addr:    httpAddr,
		Handler: r,
	}

	// Start HTTP server in a goroutine
	go func() {
		slog.Info("Starting HTTP server", "addr", httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server error", "error", err)
		}
	}()

	// Wait for termination signal
	<-ctx.Done()
	slog.Info("Shutting down gracefully...")

	// Shutdown HTTP Server
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	} else {
		slog.Info("HTTP server stopped.")
	}

	// Shutdown Asynq Server
	srv.Shutdown()
	slog.Info("Asynq server stopped.")

	slog.Info("Shutdown complete.")
}
