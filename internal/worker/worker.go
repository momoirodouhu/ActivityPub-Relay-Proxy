package worker

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/config"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/relay"
	"github.com/redis/go-redis/v9"
)

type QueueClient interface {
	EnqueueContext(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error)
	Close() error
}

// Worker handles the processing of background tasks using Asynq.
type Worker struct {
	cfg         *config.Config
	rdb         redis.Cmdable
	asynqClient QueueClient
	privateKey  *rsa.PrivateKey
	httpClient  *http.Client
}

// New creates a new Worker instance.
func New(cfg *config.Config, rdb redis.Cmdable, asynqClient QueueClient, privateKey *rsa.PrivateKey) *Worker {
	return &Worker{
		cfg:         cfg,
		rdb:         rdb,
		asynqClient: asynqClient,
		privateKey:  privateKey,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// RegisterHandlers registers the ServeMux task handlers.
func (w *Worker) RegisterHandlers(mux *asynq.ServeMux) {
	mux.HandleFunc("relay:deliver", w.HandleDeliver)
	mux.HandleFunc("relay:follow", w.HandleFollow)
	mux.HandleFunc("relay:unfollow", w.HandleUnfollow)
}

// DeliverPayload defines the payload for task relay:deliver
type DeliverPayload struct {
	InboxURL string `json:"inbox_url"`
	Activity []byte `json:"activity"`
}

// HandleDeliver performs delivery of an ActivityPub activity to an inbox.
func (w *Worker) HandleDeliver(ctx context.Context, t *asynq.Task) error {
	var p DeliverPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("failed to unmarshal deliver payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", p.InboxURL, strings.NewReader(string(p.Activity)))
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}

	req.Header.Set("Content-Type", "application/activity+json")
	req.Header.Set("User-Agent", "ActivityPub-Relay-Proxy")
	req.Header.Set("Host", req.URL.Host)

	keyID := fmt.Sprintf("https://%s/users/%s#main-key", w.cfg.Domain, w.cfg.ActorUsername)
	if err := relay.SignRequest(req, p.Activity, w.privateKey, keyID); err != nil {
		return fmt.Errorf("failed to sign request: %w", err)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute HTTP POST to %s: %w", p.InboxURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delivery to %s failed with status %d: %s", p.InboxURL, resp.StatusCode, string(bodyBytes))
	}

	slog.Info("Successfully delivered activity", "inbox", p.InboxURL, "status", resp.StatusCode)
	return nil
}

// HandleFollow processes follower subscriptions asynchronously.
func (w *Worker) HandleFollow(ctx context.Context, t *asynq.Task) error {
	var followActivity map[string]any
	if err := json.Unmarshal(t.Payload(), &followActivity); err != nil {
		return fmt.Errorf("failed to unmarshal follow activity: %w", err)
	}

	actorURL, _ := followActivity["actor"].(string)
	if actorURL == "" {
		return errors.New("missing actor in Follow activity")
	}

	// 1. Fetch follower's Actor profile to obtain their inbox/sharedInbox
	inboxURL, err := w.fetchActorInbox(ctx, actorURL)
	if err != nil {
		return fmt.Errorf("failed to fetch actor inbox for %s: %w", actorURL, err)
	}

	// 2. Save subscription details to Redis
	err = w.rdb.SAdd(ctx, "relay:subscribers", inboxURL).Err()
	if err != nil {
		return fmt.Errorf("failed to save subscriber to Redis: %w", err)
	}
	err = w.rdb.Set(ctx, "relay:actor_to_inbox:"+actorURL, inboxURL, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to map actor to inbox in Redis: %w", err)
	}

	// 3. Construct Accept activity
	actorID := fmt.Sprintf("https://%s/users/%s", w.cfg.Domain, w.cfg.ActorUsername)
	acceptID := fmt.Sprintf("https://%s/activities/%s", w.cfg.Domain, uuid.NewString())
	acceptActivity := map[string]any{
		"@context": "https://www.w3.org/ns/activitystreams",
		"id":       acceptID,
		"type":     "Accept",
		"actor":    actorID,
		"object":   followActivity,
	}

	acceptBytes, err := json.Marshal(acceptActivity)
	if err != nil {
		return fmt.Errorf("failed to marshal Accept activity: %w", err)
	}

	// 4. Enqueue Delivery task to return the Accept activity
	deliverPayload, err := json.Marshal(DeliverPayload{
		InboxURL: inboxURL,
		Activity: acceptBytes,
	})
	if err != nil {
		return err
	}

	_, err = w.asynqClient.EnqueueContext(ctx, asynq.NewTask("relay:deliver", deliverPayload, asynq.MaxRetry(3)))
	if err != nil {
		return fmt.Errorf("failed to enqueue Accept deliver task: %w", err)
	}

	slog.Info("Handled Follow and enqueued Accept", "actor", actorURL, "inbox", inboxURL)
	return nil
}

// HandleUnfollow processes unfollow requests asynchronously.
func (w *Worker) HandleUnfollow(ctx context.Context, t *asynq.Task) error {
	var undoActivity map[string]any
	if err := json.Unmarshal(t.Payload(), &undoActivity); err != nil {
		return fmt.Errorf("failed to unmarshal undo activity: %w", err)
	}

	actorURL, _ := undoActivity["actor"].(string)
	if actorURL == "" {
		return errors.New("missing actor in Undo activity")
	}

	// Look up mapped inbox URL
	inboxKey := "relay:actor_to_inbox:" + actorURL
	inboxURL, err := w.rdb.Get(ctx, inboxKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			slog.Warn("Unfollow actor not found in Redis, skipping", "actor", actorURL)
			return nil
		}
		return fmt.Errorf("redis read failed: %w", err)
	}

	// Remove from set and clean key mapping
	err = w.rdb.SRem(ctx, "relay:subscribers", inboxURL).Err()
	if err != nil {
		return fmt.Errorf("failed to remove subscriber: %w", err)
	}
	err = w.rdb.Del(ctx, inboxKey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete actor mapping: %w", err)
	}

	slog.Info("Handled Unfollow and removed subscriber", "actor", actorURL, "inbox", inboxURL)
	return nil
}

func (w *Worker) fetchActorInbox(ctx context.Context, actorURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", actorURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/activity+json, application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"")
	req.Header.Set("User-Agent", "ActivityPub-Relay-Proxy")
	req.Header.Set("Host", req.URL.Host)

	keyID := fmt.Sprintf("https://%s/users/%s#main-key", w.cfg.Domain, w.cfg.ActorUsername)
	if err := relay.SignRequest(req, []byte{}, w.privateKey, keyID); err != nil {
		return "", fmt.Errorf("failed to sign fetch request: %w", err)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to fetch actor profile, status %d", resp.StatusCode)
	}

	var profile map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&profile); err != nil {
		return "", err
	}

	// Check endpoints.sharedInbox first for instance-level delivery efficiency
	if endpoints, ok := profile["endpoints"].(map[string]any); ok {
		if sharedInbox, ok := endpoints["sharedInbox"].(string); ok && sharedInbox != "" {
			return sharedInbox, nil
		}
	}

	// Fallback to Actor direct inbox
	if inbox, ok := profile["inbox"].(string); ok && inbox != "" {
		return inbox, nil
	}

	return "", errors.New("no inbox or sharedInbox found in actor profile")
}
