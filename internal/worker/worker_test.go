package worker

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/config"
	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/testutil"
)

func TestHandleDeliver(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	cfg := &config.Config{
		Domain:        "relay.example.com",
		ActorUsername: "relay",
	}

	payload := DeliverPayload{
		InboxURL: "https://external.example.com/inbox",
		Activity: []byte(`{"id":"https://relay.example.com/activities/1","type":"Create"}`),
	}
	payloadBytes, _ := json.Marshal(payload)
	task := asynq.NewTask("relay:deliver", payloadBytes)

	t.Run("HTTP 2xx Success", func(t *testing.T) {
		w := New(cfg, testutil.NewMockRedis(), &testutil.MockQueueClient{}, privKey)
		w.httpClient.Transport = &testutil.MockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				if req.Method != "POST" || req.URL.String() != payload.InboxURL {
					t.Errorf("unexpected request: %s %s", req.Method, req.URL)
				}
				if req.Header.Get("Content-Type") != "application/activity+json" {
					t.Errorf("unexpected content type: %s", req.Header.Get("Content-Type"))
				}
				if req.Header.Get("Signature") == "" {
					t.Error("missing signature header")
				}
				return &http.Response{
					StatusCode: http.StatusAccepted,
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil
			},
		}

		err := w.HandleDeliver(context.Background(), task)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("HTTP 5xx Failure", func(t *testing.T) {
		w := New(cfg, testutil.NewMockRedis(), &testutil.MockQueueClient{}, privKey)
		w.httpClient.Transport = &testutil.MockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader("internal server error")),
				}, nil
			},
		}

		err := w.HandleDeliver(context.Background(), task)
		if err == nil {
			t.Error("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "500") {
			t.Errorf("expected 500 status error, got %v", err)
		}
	})
}

func TestHandleFollow(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	cfg := &config.Config{
		Domain:        "relay.example.com",
		ActorUsername: "relay",
	}

	actorURL := "https://external.example.com/users/bob"
	followActivity := map[string]any{
		"id":    "https://external.example.com/activities/follow-1",
		"type":  "Follow",
		"actor": actorURL,
	}
	payloadBytes, _ := json.Marshal(followActivity)
	task := asynq.NewTask("relay:follow", payloadBytes)

	t.Run("Follow with endpoints.sharedInbox", func(t *testing.T) {
		rdb := testutil.NewMockRedis()
		queue := &testutil.MockQueueClient{}
		w := New(cfg, rdb, queue, privKey)

		profile := map[string]any{
			"id":    actorURL,
			"type":  "Person",
			"inbox": "https://external.example.com/users/bob/inbox",
			"endpoints": map[string]any{
				"sharedInbox": "https://external.example.com/inbox",
			},
		}
		profileBytes, _ := json.Marshal(profile)

		w.httpClient.Transport = &testutil.MockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				if req.URL.String() != actorURL {
					t.Errorf("unexpected fetch URL: %s", req.URL)
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(string(profileBytes))),
				}, nil
			},
		}

		err := w.HandleFollow(context.Background(), task)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !rdb.Sets["relay:subscribers"]["https://external.example.com/inbox"] {
			t.Error("expected inbox https://external.example.com/inbox to be added to subscribers")
		}
		if rdb.KV["relay:actor_to_inbox:"+actorURL] != "https://external.example.com/inbox" {
			t.Errorf("expected actor map to be set to sharedInbox, got %s", rdb.KV["relay:actor_to_inbox:"+actorURL])
		}

		if len(queue.EnqueuedTasks) != 1 {
			t.Fatalf("expected 1 task enqueued, got %d", len(queue.EnqueuedTasks))
		}
		enqueued := queue.EnqueuedTasks[0]
		if enqueued.Type() != "relay:deliver" {
			t.Errorf("expected task type relay:deliver, got %s", enqueued.Type())
		}

		var deliverPayload DeliverPayload
		_ = json.Unmarshal(enqueued.Payload(), &deliverPayload)
		if deliverPayload.InboxURL != "https://external.example.com/inbox" {
			t.Errorf("expected deliver inbox %s, got %s", "https://external.example.com/inbox", deliverPayload.InboxURL)
		}

		var acceptActivity map[string]any
		_ = json.Unmarshal(deliverPayload.Activity, &acceptActivity)
		if acceptActivity["type"] != "Accept" {
			t.Errorf("expected activity type Accept, got %v", acceptActivity["type"])
		}
		if acceptActivity["actor"] != "https://relay.example.com/users/relay" {
			t.Errorf("expected actor %s, got %v", "https://relay.example.com/users/relay", acceptActivity["actor"])
		}
	})

	t.Run("Follow fallback to direct inbox", func(t *testing.T) {
		rdb := testutil.NewMockRedis()
		queue := &testutil.MockQueueClient{}
		w := New(cfg, rdb, queue, privKey)

		profile := map[string]any{
			"id":    actorURL,
			"type":  "Person",
			"inbox": "https://external.example.com/users/bob/inbox",
		}
		profileBytes, _ := json.Marshal(profile)

		w.httpClient.Transport = &testutil.MockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(string(profileBytes))),
				}, nil
			},
		}

		err := w.HandleFollow(context.Background(), task)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !rdb.Sets["relay:subscribers"]["https://external.example.com/users/bob/inbox"] {
			t.Error("expected inbox to be fallback inbox")
		}
	})
}

func TestHandleUnfollow(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	cfg := &config.Config{
		Domain:        "relay.example.com",
		ActorUsername: "relay",
	}

	actorURL := "https://external.example.com/users/bob"
	inboxURL := "https://external.example.com/inbox"
	undoActivity := map[string]any{
		"id":    "https://external.example.com/activities/undo-1",
		"type":  "Undo",
		"actor": actorURL,
		"object": map[string]any{
			"id":     "https://external.example.com/activities/follow-1",
			"type":   "Follow",
			"actor":  actorURL,
			"object": "https://relay.example.com/users/relay",
		},
	}
	payloadBytes, _ := json.Marshal(undoActivity)
	task := asynq.NewTask("relay:unfollow", payloadBytes)

	t.Run("Unfollow Success", func(t *testing.T) {
		rdb := testutil.NewMockRedis()
		rdb.PresetSet("relay:subscribers", inboxURL)
		rdb.PresetKV("relay:actor_to_inbox:"+actorURL, inboxURL)

		w := New(cfg, rdb, &testutil.MockQueueClient{}, privKey)

		err := w.HandleUnfollow(context.Background(), task)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if rdb.Sets["relay:subscribers"][inboxURL] {
			t.Error("expected subscriber to be removed")
		}
		if _, ok := rdb.KV["relay:actor_to_inbox:"+actorURL]; ok {
			t.Error("expected actor map key to be deleted")
		}
	})

	t.Run("Unfollow Actor not found", func(t *testing.T) {
		rdb := testutil.NewMockRedis()
		w := New(cfg, rdb, &testutil.MockQueueClient{}, privKey)

		err := w.HandleUnfollow(context.Background(), task)
		if err != nil {
			t.Errorf("expected no error when actor not found, got %v", err)
		}
	})
}
