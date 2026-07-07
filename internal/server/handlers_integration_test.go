package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/hibiken/asynq"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/config"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/testutil"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/worker"
)

// Mock Verifier
type mockVerifier struct {
	err error
}

func (m *mockVerifier) Verify(r *http.Request) error {
	return m.err
}

func TestHandleInbox_Integration(t *testing.T) {
	cfg := &config.Config{
		Domain:         "relay.example.com",
		ActorUsername:  "relay",
		DestinationURL: "https://my-mastodon.example.com",
		FilterKeywords: []string{"golang", "rust"},
	}

	tests := []struct {
		name                 string
		activity             map[string]any
		subscribers          []string
		expectedDeliveries   []string // Expected inbox URLs to deliver to
		expectedWrapAnnounce bool     // Whether the activity should be wrapped in Announce
		expectDrop           bool     // Whether the activity should be dropped
	}{
		{
			name: "Own Mastodon post (No filter match, should forward to external subscribers only, unwrapped)",
			activity: map[string]any{
				"id":    "https://my-mastodon.example.com/activities/1",
				"type":  "Create",
				"actor": "https://my-mastodon.example.com/users/alice",
				"object": map[string]any{
					"id":      "https://my-mastodon.example.com/users/alice/statuses/1",
					"content": "Just talking about weather, not matching filters",
				},
			},
			subscribers: []string{
				"https://my-mastodon.example.com/inbox", // own mastodon (should be excluded)
				"https://external-relay.example.com/inbox",
			},
			expectedDeliveries:   []string{"https://external-relay.example.com/inbox"},
			expectedWrapAnnounce: false,
			expectDrop:           false,
		},
		{
			name: "External post matching filter (Should wrap in Announce and forward to own Mastodon only)",
			activity: map[string]any{
				"id":    "https://external.example.com/activities/2",
				"type":  "Create",
				"actor": "https://external.example.com/users/bob",
				"object": map[string]any{
					"id":      "https://external.example.com/users/bob/statuses/2",
					"content": "Check out this awesome golang library!",
				},
			},
			subscribers: []string{
				"https://my-mastodon.example.com/inbox",
				"https://external-relay.example.com/inbox", // other subscriber (should not receive external posts)
			},
			expectedDeliveries:   []string{"https://my-mastodon.example.com/inbox"},
			expectedWrapAnnounce: true,
			expectDrop:           false,
		},
		{
			name: "External post mismatching filter (Should drop)",
			activity: map[string]any{
				"id":    "https://external.example.com/activities/3",
				"type":  "Create",
				"actor": "https://external.example.com/users/bob",
				"object": map[string]any{
					"id":      "https://external.example.com/users/bob/statuses/3",
					"content": "Talking about weather on external server",
				},
			},
			subscribers: []string{
				"https://my-mastodon.example.com/inbox",
			},
			expectedDeliveries: []string{},
			expectDrop:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mQueue := &testutil.MockQueueClient{}
			mRedis := testutil.NewMockRedis()
			mRedis.PresetSet("relay:subscribers", tt.subscribers...)
			mVerifier := &mockVerifier{err: nil}

			s := &Server{
				cfg:         cfg,
				rdb:         mRedis,
				asynqClient: mQueue,
				verifier:    mVerifier,
			}

			r := chi.NewRouter()
			r.Post("/users/{username}/inbox", s.handleInbox)

			bodyBytes, err := json.Marshal(tt.activity)
			if err != nil {
				t.Fatalf("failed to marshal activity: %v", err)
			}

			req := httptest.NewRequest("POST", "/users/relay/inbox", strings.NewReader(string(bodyBytes)))
			req.Header.Set("Content-Type", "application/activity+json")
			w := httptest.NewRecorder()

			r.ServeHTTP(w, req)

			if w.Code != http.StatusAccepted {
				t.Errorf("expected status 202, got %d", w.Code)
			}

			// Verify delivered tasks
			var deliverTasks []*asynq.Task
			for _, task := range mQueue.EnqueuedTasks {
				if task.Type() == "relay:deliver" {
					deliverTasks = append(deliverTasks, task)
				}
			}

			if tt.expectDrop {
				if len(deliverTasks) > 0 {
					t.Errorf("expected activity to be dropped, but got %d deliveries", len(deliverTasks))
				}
				return
			}

			if len(deliverTasks) != len(tt.expectedDeliveries) {
				t.Fatalf("expected %d deliveries, got %d", len(tt.expectedDeliveries), len(deliverTasks))
			}

			for i, expectedInbox := range tt.expectedDeliveries {
				task := deliverTasks[i]
				var payload worker.DeliverPayload
				if err := json.Unmarshal(task.Payload(), &payload); err != nil {
					t.Fatalf("failed to unmarshal payload: %v", err)
				}

				if payload.InboxURL != expectedInbox {
					t.Errorf("expected delivery inbox %s, got %s", expectedInbox, payload.InboxURL)
				}

				// Check if wrapped in Announce or not
				var payloadActivity map[string]any
				if err := json.Unmarshal(payload.Activity, &payloadActivity); err != nil {
					t.Fatalf("failed to unmarshal payload activity: %v", err)
				}

				activityType, _ := payloadActivity["type"].(string)
				if tt.expectedWrapAnnounce {
					if activityType != "Announce" {
						t.Errorf("expected activity to be wrapped in Announce, got type %s", activityType)
					}
				} else {
					if activityType == "Announce" {
						t.Error("expected activity to not be wrapped in Announce, but it was")
					}
				}
			}
		})
	}
}

func TestHandleInbox_ValidationErrors(t *testing.T) {
	cfg := &config.Config{
		Domain:         "relay.example.com",
		ActorUsername:  "relay",
		DestinationURL: "https://my-mastodon.example.com",
	}

	tests := []struct {
		name           string
		mockVerifyErr  error
		requestBody    string
		expectedStatus int
	}{
		{
			name:           "Signature verification failed (401 Unauthorized)",
			mockVerifyErr:  errors.New("invalid signature"),
			requestBody:    `{"id": "https://external.example.com/activities/1", "type": "Create"}`,
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "Empty request body (400 Bad Request)",
			mockVerifyErr:  nil,
			requestBody:    "",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Invalid JSON body (400 Bad Request)",
			mockVerifyErr:  nil,
			requestBody:    `{"id": "invalid-json",`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Missing activity ID (400 Bad Request)",
			mockVerifyErr:  nil,
			requestBody:    `{"type": "Create", "actor": "https://external.example.com/users/bob"}`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mQueue := &testutil.MockQueueClient{}
			mRedis := testutil.NewMockRedis()
			mVerifier := &mockVerifier{err: tt.mockVerifyErr}

			s := &Server{
				cfg:         cfg,
				rdb:         mRedis,
				asynqClient: mQueue,
				verifier:    mVerifier,
			}

			r := chi.NewRouter()
			r.Post("/users/{username}/inbox", s.handleInbox)

			req := httptest.NewRequest("POST", "/users/relay/inbox", strings.NewReader(tt.requestBody))
			req.Header.Set("Content-Type", "application/activity+json")
			w := httptest.NewRecorder()

			r.ServeHTTP(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}
		})
	}
}

func TestHandleIndex(t *testing.T) {
	cfg := &config.Config{
		Domain:         "relay.example.com",
		ActorUsername:  "relay",
		DestinationURL: "https://my-mastodon.example.com",
		FilterKeywords: []string{"golang", "rust"},
	}

	mQueue := &testutil.MockQueueClient{}
	mRedis := testutil.NewMockRedis()
	mRedis.PresetSet("relay:subscribers", "https://sub1.example.com/inbox", "https://sub2.example.com/inbox")

	s := &Server{
		cfg:         cfg,
		rdb:         mRedis,
		asynqClient: mQueue,
	}

	r := chi.NewRouter()
	r.Get("/", s.handleIndex)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Errorf("expected Content-Type to contain text/plain, got %s", contentType)
	}

	body := w.Body.String()
	expectedSubstrings := []string{
		"ActivityPub Relay Proxy",
		"This service acts as an ActivityPub Relay Proxy for https://my-mastodon.example.com",
		"Relay Actor URI:      https://relay.example.com/users/relay",
		"Active Subscribers:   2",
	}

	for _, sub := range expectedSubstrings {
		if !strings.Contains(body, sub) {
			t.Errorf("expected body to contain %q, but it didn't", sub)
		}
	}
}

func TestHandleWellKnownNodeInfo(t *testing.T) {
	cfg := &config.Config{
		Domain: "relay.example.com",
	}

	s := &Server{
		cfg: cfg,
	}

	r := chi.NewRouter()
	r.Get("/.well-known/nodeinfo", s.handleWellKnownNodeInfo)

	req := httptest.NewRequest("GET", "/.well-known/nodeinfo", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		t.Errorf("expected Content-Type to contain application/json, got %s", contentType)
	}

	var resp NodeInfoWellKnown
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if len(resp.Links) != 1 {
		t.Fatalf("expected 1 link, got %d", len(resp.Links))
	}

	link := resp.Links[0]
	if link.Rel != "http://nodeinfo.diaspora.eu/ns/schema/2.0" {
		t.Errorf("expected rel to be http://nodeinfo.diaspora.eu/ns/schema/2.0, got %s", link.Rel)
	}

	expectedHref := "https://relay.example.com/nodeinfo/2.0"
	if link.Href != expectedHref {
		t.Errorf("expected href to be %s, got %s", expectedHref, link.Href)
	}
}

func TestHandleNodeInfo(t *testing.T) {
	cfg := &config.Config{
		Domain:         "relay.example.com",
		ActorUsername:  "relay",
		DestinationURL: "https://my-mastodon.example.com",
	}

	mQueue := &testutil.MockQueueClient{}
	mRedis := testutil.NewMockRedis()
	mRedis.PresetSet("relay:subscribers", "https://sub1.example.com/inbox")

	s := &Server{
		cfg:         cfg,
		rdb:         mRedis,
		asynqClient: mQueue,
	}

	r := chi.NewRouter()
	r.Get("/nodeinfo/2.0", s.handleNodeInfo)

	req := httptest.NewRequest("GET", "/nodeinfo/2.0", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		t.Errorf("expected Content-Type to contain application/json, got %s", contentType)
	}

	var resp NodeInfoSchema
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if resp.Version != "2.0" {
		t.Errorf("expected version 2.0, got %s", resp.Version)
	}

	if resp.Software.Name != "activitypub-relay-proxy" {
		t.Errorf("expected software name activitypub-relay-proxy, got %s", resp.Software.Name)
	}

	if resp.Metadata.ActorUsername != "relay" {
		t.Errorf("expected metadata actor_username to be relay, got %s", resp.Metadata.ActorUsername)
	}

	if resp.Metadata.SubscribersCount != 1 {
		t.Errorf("expected metadata subscribers_count to be 1, got %d", resp.Metadata.SubscribersCount)
	}
}

