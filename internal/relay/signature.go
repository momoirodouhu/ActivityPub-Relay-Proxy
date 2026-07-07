package relay

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-fed/httpsig"
	"github.com/redis/go-redis/v9"
)

// SignatureVerifier handles verification of incoming HTTP Signatures and body Digests.
type SignatureVerifier struct {
	redisClient redis.Cmdable
	httpClient  *http.Client
}

// NewSignatureVerifier creates a new SignatureVerifier
func NewSignatureVerifier(rdb redis.Cmdable) *SignatureVerifier {
	return &SignatureVerifier{
		redisClient: rdb,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// Verify checks the HTTP Signature and the Digest header of the incoming request.
func (v *SignatureVerifier) Verify(r *http.Request) error {
	// 1. Verify Digest header if present
	digestHeader := r.Header.Get("Digest")
	if digestHeader != "" {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("failed to read body for digest validation: %w", err)
		}
		// Restore body reader
		r.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))

		parts := strings.SplitN(digestHeader, "=", 2)
		if len(parts) == 2 && strings.ToUpper(parts[0]) == "SHA-256" {
			expectedHash := parts[1]
			h := sha256.New()
			h.Write(bodyBytes)
			actualHash := base64.StdEncoding.EncodeToString(h.Sum(nil))
			if actualHash != expectedHash {
				return errors.New("digest mismatch")
			}
		}
	}

	// 2. Verify HTTP Signature
	verifier, err := httpsig.NewVerifier(r)
	if err != nil {
		return fmt.Errorf("failed to create signature verifier: %w", err)
	}

	keyID := verifier.KeyId()
	if keyID == "" {
		return errors.New("missing keyId in signature header")
	}

	pubKey, err := v.GetPublicKey(r.Context(), keyID)
	if err != nil {
		return fmt.Errorf("failed to get public key for %s: %w", keyID, err)
	}

	return verifier.Verify(pubKey, httpsig.RSA_SHA256)
}

// GetPublicKey retrieves the public key for the given keyID, checking the Redis cache first.
func (v *SignatureVerifier) GetPublicKey(ctx context.Context, keyID string) (crypto.PublicKey, error) {
	cacheKey := "cache:pubkey:" + keyID
	cachedPem, err := v.redisClient.Get(ctx, cacheKey).Result()
	if err == nil && cachedPem != "" {
		return parsePublicKeyPem([]byte(cachedPem))
	}

	// Fetch from remote
	pemBytes, err := v.fetchPublicKeyFromNetwork(ctx, keyID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch public key: %w", err)
	}

	// Cache in Redis for 24 hours
	_ = v.redisClient.Set(ctx, cacheKey, string(pemBytes), 24*time.Hour).Err()

	return parsePublicKeyPem(pemBytes)
}

func (v *SignatureVerifier) fetchPublicKeyFromNetwork(ctx context.Context, keyID string) ([]byte, error) {
	// KeyID can contain a fragment like #main-key.
	// Typically, the URL before fragment is the actor URL.
	// We make a GET request to the keyID directly, as some systems serve the key directly there.
	req, err := http.NewRequestWithContext(ctx, "GET", keyID, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/activity+json, application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"")
	req.Header.Set("User-Agent", "ActivityPub-Relay-Proxy")

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http GET key failed with status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("failed to decode JSON response from %s: %w", keyID, err)
	}

	// Try extracting from Actor publicKey structure
	if pkObj, ok := raw["publicKey"].(map[string]any); ok {
		if pemStr, ok := pkObj["publicKeyPem"].(string); ok {
			return []byte(pemStr), nil
		}
	}

	// Try direct publicKeyPem extraction
	if pemStr, ok := raw["publicKeyPem"].(string); ok {
		return []byte(pemStr), nil
	}

	return nil, fmt.Errorf("no publicKeyPem found in response from %s", keyID)
}

func parsePublicKeyPem(pemBytes []byte) (crypto.PublicKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("failed to decode PEM block for public key")
	}

	pubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PKIX public key: %w", err)
	}

	return pubKey, nil
}

// SignRequest signs the outgoing request using Cavage HTTP Signatures.
func SignRequest(r *http.Request, body []byte, privateKey *rsa.PrivateKey, keyID string) error {
	// Set date
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))

	// Initialize Cavage signer
	prefs := []httpsig.Algorithm{httpsig.RSA_SHA256}
	digestAlgorithm := httpsig.DigestSha256
	headersToSign := []string{httpsig.RequestTarget, "host", "date", "digest"}
	signer, _, err := httpsig.NewSigner(prefs, digestAlgorithm, headersToSign, httpsig.Signature, 0)
	if err != nil {
		return fmt.Errorf("failed to create signer: %w", err)
	}

	return signer.SignRequest(privateKey, keyID, r, body)
}
