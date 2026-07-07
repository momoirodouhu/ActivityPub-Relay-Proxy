package relay

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func TestVerify_DigestMismatch(t *testing.T) {
	v := &SignatureVerifier{}

	req := httptest.NewRequest("POST", "/inbox", strings.NewReader("hello world"))
	req.Header.Set("Digest", "SHA-256=invalidhashvalue")

	err := v.Verify(req)
	if err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Errorf("expected digest mismatch error, got %v", err)
	}
}

func TestVerify_ValidSignature(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pubKeyPem, err := GetPublicKeyPem(privKey)
	if err != nil {
		t.Fatalf("failed to get public key pem: %v", err)
	}

	keyID := "https://relay.example.com/users/relay#main-key"

	body := []byte("hello body content")
	req := httptest.NewRequest("POST", "https://relay.example.com/inbox", bytes.NewReader(body))
	req.Header.Set("Host", "relay.example.com")

	err = SignRequest(req, body, privKey, keyID)
	if err != nil {
		t.Fatalf("failed to sign request: %v", err)
	}

	// Setup verifier with mocked redis returning the public key
	mRedis := testutil.NewMockRedis()
	mRedis.PresetKV("cache:pubkey:"+keyID, pubKeyPem)
	verifier := NewSignatureVerifier(mRedis)

	err = verifier.Verify(req)
	if err != nil {
		t.Errorf("failed to verify request: %v", err)
	}
}

func TestGetPublicKey(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pubKeyPem, err := GetPublicKeyPem(privKey)
	if err != nil {
		t.Fatalf("failed to get public key pem: %v", err)
	}

	keyID := "https://relay.example.com/users/relay#main-key"

	t.Run("cached in redis", func(t *testing.T) {
		mRedis := testutil.NewMockRedis()
		mRedis.PresetKV("cache:pubkey:"+keyID, pubKeyPem)
		verifier := NewSignatureVerifier(mRedis)

		pubKey, err := verifier.GetPublicKey(context.Background(), keyID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		rsaPub, ok := pubKey.(*rsa.PublicKey)
		if !ok {
			t.Fatal("expected rsa.PublicKey")
		}
		if rsaPub.N.Cmp(privKey.PublicKey.N) != 0 {
			t.Error("modulus mismatch")
		}
	})

	t.Run("fetch from network - format 1 (publicKey.publicKeyPem)", func(t *testing.T) {
		mRedis := testutil.NewMockRedis()
		mRedis.GetError = redis.Nil
		verifier := NewSignatureVerifier(mRedis)

		// Mock network response
		payload := map[string]any{
			"publicKey": map[string]any{
				"publicKeyPem": pubKeyPem,
			},
		}
		payloadBytes, _ := json.Marshal(payload)

		verifier.httpClient.Transport = &testutil.MockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(payloadBytes)),
				}, nil
			},
		}

		pubKey, err := verifier.GetPublicKey(context.Background(), keyID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if mRedis.LastSetKey != "cache:pubkey:"+keyID {
			t.Errorf("expected set key %s, got %s", "cache:pubkey:"+keyID, mRedis.LastSetKey)
		}
		if mRedis.LastSetVal != pubKeyPem {
			t.Errorf("expected cached val %s, got %s", pubKeyPem, mRedis.LastSetVal)
		}
		if mRedis.LastSetExp != 24*time.Hour {
			t.Errorf("expected expiration 24h, got %v", mRedis.LastSetExp)
		}

		_ = pubKey.(*rsa.PublicKey)
	})

	t.Run("fetch from network - format 2 (direct publicKeyPem)", func(t *testing.T) {
		mRedis := testutil.NewMockRedis()
		mRedis.GetError = redis.Nil
		verifier := NewSignatureVerifier(mRedis)

		payload := map[string]any{
			"publicKeyPem": pubKeyPem,
		}
		payloadBytes, _ := json.Marshal(payload)

		verifier.httpClient.Transport = &testutil.MockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(payloadBytes)),
				}, nil
			},
		}

		_, err := verifier.GetPublicKey(context.Background(), keyID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
