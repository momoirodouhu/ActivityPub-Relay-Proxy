package server

import (
	"context"
	"crypto/rsa"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hibiken/asynq"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/config"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/relay"
	"github.com/redis/go-redis/v9"
)

type QueueClient interface {
	EnqueueContext(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error)
	Close() error
}

type Verifier interface {
	Verify(r *http.Request) error
}

// Server handles the HTTP API endpoints.
type Server struct {
	cfg         *config.Config
	rdb         redis.Cmdable
	asynqClient QueueClient
	privateKey  *rsa.PrivateKey
	pubKeyPem   string
	verifier    Verifier
}

// New creates a new Server instance.
func New(cfg *config.Config, rdb redis.Cmdable, asynqClient QueueClient, privateKey *rsa.PrivateKey, pubKeyPem string) *Server {
	return &Server{
		cfg:         cfg,
		rdb:         rdb,
		asynqClient: asynqClient,
		privateKey:  privateKey,
		pubKeyPem:   pubKeyPem,
		verifier:    relay.NewSignatureVerifier(rdb),
	}
}

// Routes returns the configured chi.Router for the API server.
func (s *Server) Routes() chi.Router {
	r := chi.NewRouter()

	// Landing Page (Plain text description)
	r.Get("/", s.handleIndex)

	// NodeInfo endpoints
	r.Get("/.well-known/nodeinfo", s.handleWellKnownNodeInfo)
	r.Get("/nodeinfo/2.0", s.handleNodeInfo)

	// WebFinger Actor discovery
	r.Get("/.well-known/webfinger", s.handleWebFinger)

	// Actor Profile
	r.Get("/users/{username}", s.handleActor)

	// Inbox
	r.Post("/users/{username}/inbox", s.handleInbox)

	// Outbox (mandatory placeholder)
	r.Get("/users/{username}/outbox", s.handleOutbox)

	return r
}
