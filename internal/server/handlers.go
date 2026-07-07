package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/relay"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/worker"
)

// handleWebFinger handles actor discovery via WebFinger.
func (s *Server) handleWebFinger(w http.ResponseWriter, r *http.Request) {
	resource := r.URL.Query().Get("resource")
	if resource == "" {
		http.Error(w, "missing resource parameter", http.StatusBadRequest)
		return
	}

	expected := fmt.Sprintf("acct:%s@%s", s.cfg.ActorUsername, s.cfg.Domain)
	if resource != expected {
		http.Error(w, "resource not found", http.StatusNotFound)
		return
	}

	wf := relay.NewWebFinger(s.cfg.Domain, s.cfg.ActorUsername)
	w.Header().Set("Content-Type", "application/jrd+json")
	_ = json.NewEncoder(w).Encode(wf)
}

// handleActor serves the relay actor profile JSON.
func (s *Server) handleActor(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")
	if username != s.cfg.ActorUsername {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	actor := relay.NewActor(s.cfg.Domain, s.cfg.ActorUsername, s.pubKeyPem)
	w.Header().Set("Content-Type", "application/activity+json")
	_ = json.NewEncoder(w).Encode(actor)
}

// handleOutbox serves an empty OrderedCollection.
func (s *Server) handleOutbox(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")
	if username != s.cfg.ActorUsername {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	outbox := map[string]any{
		"@context":     "https://www.w3.org/ns/activitystreams",
		"id":           fmt.Sprintf("https://%s/users/%s/outbox", s.cfg.Domain, s.cfg.ActorUsername),
		"type":         "OrderedCollection",
		"totalItems":   0,
		"orderedItems": []any{},
	}

	w.Header().Set("Content-Type", "application/activity+json")
	_ = json.NewEncoder(w).Encode(outbox)
}

// handleInbox processes incoming activities sent to the relay.
func (s *Server) handleInbox(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")
	if username != s.cfg.ActorUsername {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	// 1. Verify HTTP Signature and body digest
	if err := s.verifier.Verify(r); err != nil {
		slog.Warn("Signature verification failed", "error", err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// 2. Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("Failed to read inbox request body", "error", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var activity map[string]any
	if err := json.Unmarshal(body, &activity); err != nil {
		slog.Error("Failed to unmarshal activity JSON", "error", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// 3. Deduplication check using the activity ID
	activityID, _ := activity["id"].(string)
	if activityID == "" {
		slog.Warn("Received activity without ID")
		http.Error(w, "missing activity ID", http.StatusBadRequest)
		return
	}

	dedupKey := "dedup:activity:" + activityID
	ok, err := s.rdb.SetNX(r.Context(), dedupKey, "1", s.cfg.DeduplicationTTL).Result()
	if err != nil {
		slog.Error("Redis error during deduplication check", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if !ok {
		// Silent drop since it was already processed
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// 4. Route activity based on its Type
	activityType, _ := activity["type"].(string)
	slog.Info("Processing activity", "id", activityID, "type", activityType)

	switch activityType {
	case "Follow":
		if err := s.handleFollow(r.Context(), activity); err != nil {
			slog.Error("Failed to handle Follow", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	case "Undo":
		if err := s.handleUndo(r.Context(), activity); err != nil {
			slog.Error("Failed to handle Undo", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	case "Create":
		if err := s.handleCreate(r.Context(), activity, body); err != nil {
			slog.Error("Failed to handle Create", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	case "Announce":
		if err := s.handleAnnounce(r.Context(), activity, body); err != nil {
			slog.Error("Failed to handle Announce", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	case "Delete", "Update":
		// Route based on sender domain
		actorURI, _ := activity["actor"].(string)
		var err error
		if s.isOwnInstance(actorURI) {
			err = s.enqueueForwardToExternal(r.Context(), body)
		} else {
			err = s.enqueueForwardToOwn(r.Context(), body)
		}
		if err != nil {
			slog.Error("Failed to enqueue forward task", "type", activityType, "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	default:
		slog.Debug("Ignored activity type", "type", activityType)
	}

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleFollow(ctx context.Context, activity map[string]any) error {
	payload, err := json.Marshal(activity)
	if err != nil {
		return err
	}
	task := asynq.NewTask("relay:follow", payload, asynq.MaxRetry(3))
	_, err = s.asynqClient.EnqueueContext(ctx, task)
	return err
}

func (s *Server) handleUndo(ctx context.Context, activity map[string]any) error {
	object, ok := activity["object"].(map[string]any)
	if ok {
		objType, _ := object["type"].(string)
		if objType == "Follow" {
			payload, err := json.Marshal(activity)
			if err != nil {
				return err
			}
			task := asynq.NewTask("relay:unfollow", payload, asynq.MaxRetry(3))
			_, err = s.asynqClient.EnqueueContext(ctx, task)
			return err
		}
	}

	// For non-Follow Undos, forward the action as-is
	rawBytes, err := json.Marshal(activity)
	if err != nil {
		return err
	}

	actorURI, _ := activity["actor"].(string)
	if s.isOwnInstance(actorURI) {
		return s.enqueueForwardToExternal(ctx, rawBytes)
	}
	return s.enqueueForwardToOwn(ctx, rawBytes)
}

func (s *Server) handleCreate(ctx context.Context, activity map[string]any, rawBody []byte) error {
	actorURI, _ := activity["actor"].(string)
	if s.isOwnInstance(actorURI) {
		// Own instance post: bypass filters, do not wrap in Announce, and forward to external subscribers only
		return s.enqueueForwardToExternal(ctx, rawBody)
	}

	// External post: apply filters, wrap in Announce, and forward to own instance only
	if !s.matchesFilter(activity) {
		slog.Info("Activity dropped: does not match filters", "id", activity["id"])
		return nil
	}

	announceBytes, err := s.wrapInAnnounce(activity)
	if err != nil {
		return fmt.Errorf("failed to wrap in Announce: %w", err)
	}

	return s.enqueueForwardToOwn(ctx, announceBytes)
}

func (s *Server) handleAnnounce(ctx context.Context, activity map[string]any, rawBody []byte) error {
	actorURI, _ := activity["actor"].(string)
	if s.isOwnInstance(actorURI) {
		// Own instance announce: bypass filters and forward to external subscribers only
		return s.enqueueForwardToExternal(ctx, rawBody)
	}

	// External announce: apply filters and forward to own instance only
	_, ok := activity["object"].(map[string]any)
	if !ok {
		// If no filters are defined, we can pass it through. Otherwise, we drop it.
		if len(s.cfg.FilterKeywords) == 0 && len(s.cfg.FilterHashtags) == 0 {
			return s.enqueueForwardToOwn(ctx, rawBody)
		}
		slog.Info("Announce activity dropped: object is not a nested map (cannot filter)", "id", activity["id"])
		return nil
	}

	if !s.matchesFilter(activity) {
		slog.Info("Announce activity dropped: nested object does not match filters", "id", activity["id"])
		return nil
	}

	return s.enqueueForwardToOwn(ctx, rawBody)
}

func (s *Server) matchesFilter(activity map[string]any) bool {
	// If no filters are defined, pass everything
	if len(s.cfg.FilterKeywords) == 0 && len(s.cfg.FilterHashtags) == 0 {
		return true
	}

	obj, ok := activity["object"].(map[string]any)
	if !ok {
		return false
	}

	// 1. Check content and summary (Content Warning)
	content, _ := obj["content"].(string)
	summary, _ := obj["summary"].(string)
	textToCheck := strings.ToLower(content + " " + summary)

	for _, kw := range s.cfg.FilterKeywords {
		if strings.Contains(textToCheck, strings.ToLower(kw)) {
			return true
		}
	}

	// 2. Check hashtags in tags structure
	if tags, ok := obj["tag"].([]any); ok {
		for _, t := range tags {
			if tagMap, ok := t.(map[string]any); ok {
				tType, _ := tagMap["type"].(string)
				tName, _ := tagMap["name"].(string)
				if strings.ToLower(tType) == "hashtag" || tType == "" {
					tagName := strings.TrimPrefix(strings.ToLower(tName), "#")
					for _, ht := range s.cfg.FilterHashtags {
						if tagName == strings.TrimPrefix(strings.ToLower(ht), "#") {
							return true
						}
					}
				}
			}
		}
	}

	return false
}

func (s *Server) wrapInAnnounce(activity map[string]any) ([]byte, error) {
	objectID, _ := activity["object"].(string)
	if objectID == "" {
		if objMap, ok := activity["object"].(map[string]any); ok {
			objectID, _ = objMap["id"].(string)
		}
	}

	if objectID == "" {
		return nil, errors.New("cannot extract object ID from activity")
	}

	actorID := fmt.Sprintf("https://%s/users/%s", s.cfg.Domain, s.cfg.ActorUsername)
	announceID := fmt.Sprintf("https://%s/activities/%s", s.cfg.Domain, uuid.NewString())

	announce := map[string]any{
		"@context":  "https://www.w3.org/ns/activitystreams",
		"id":        announceID,
		"type":      "Announce",
		"actor":     actorID,
		"published": time.Now().UTC().Format(time.RFC3339),
		"object":    objectID,
		"to":        []string{"https://www.w3.org/ns/activitystreams#Public"},
		"cc":        []string{actorID + "/followers"},
	}

	return json.Marshal(announce)
}

// isOwnInstance checks if the given actor URI belongs to the own Mastodon instance.
func (s *Server) isOwnInstance(actorURI string) bool {
	actorURL, err := url.Parse(actorURI)
	if err != nil {
		return false
	}
	destURL, err := url.Parse(s.cfg.DestinationURL)
	if err != nil {
		return false
	}
	return strings.ToLower(actorURL.Hostname()) == strings.ToLower(destURL.Hostname())
}

// enqueueForwardToOwn routes the activity specifically to the own Mastodon instance.
func (s *Server) enqueueForwardToOwn(ctx context.Context, activityBytes []byte) error {
	destInbox := strings.TrimSuffix(s.cfg.DestinationURL, "/") + "/inbox"
	return s.enqueueToInboxes(ctx, []string{destInbox}, activityBytes)
}

// enqueueForwardToExternal routes the activity to all external subscribers, excluding the own Mastodon instance.
func (s *Server) enqueueForwardToExternal(ctx context.Context, activityBytes []byte) error {
	inboxes, err := s.rdb.SMembers(ctx, "relay:subscribers").Result()
	if err != nil {
		return fmt.Errorf("failed to retrieve subscribers from Redis: %w", err)
	}

	var externalInboxes []string
	destInbox := strings.TrimSuffix(s.cfg.DestinationURL, "/") + "/inbox"
	for _, inbox := range inboxes {
		if strings.ToLower(inbox) != strings.ToLower(destInbox) {
			externalInboxes = append(externalInboxes, inbox)
		}
	}

	if len(externalInboxes) == 0 {
		return nil
	}

	return s.enqueueToInboxes(ctx, externalInboxes, activityBytes)
}

// enqueueToInboxes enqueues a delivery task for each of the target inboxes.
func (s *Server) enqueueToInboxes(ctx context.Context, inboxes []string, activityBytes []byte) error {
	for _, inbox := range inboxes {
		payload, err := json.Marshal(worker.DeliverPayload{
			InboxURL: inbox,
			Activity: activityBytes,
		})
		if err != nil {
			return err
		}

		task := asynq.NewTask("relay:deliver", payload, asynq.MaxRetry(5))
		if _, err := s.asynqClient.EnqueueContext(ctx, task); err != nil {
			slog.Error("Failed to enqueue delivery task", "inbox", inbox, "error", err)
		}
	}
	return nil
}
