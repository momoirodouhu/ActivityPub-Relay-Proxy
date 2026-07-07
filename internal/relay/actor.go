package relay

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

// WebFingerResource represents the JRD format for WebFinger responses
type WebFingerResource struct {
	Subject string          `json:"subject"`
	Links   []WebFingerLink `json:"links"`
}

// WebFingerLink represents a link in the WebFinger response
type WebFingerLink struct {
	Rel  string `json:"rel"`
	Type string `json:"type,omitempty"`
	Href string `json:"href,omitempty"`
}

// Actor represents a simplified ActivityPub Actor JSON schema
type Actor struct {
	Context           any          `json:"@context"`
	ID                string       `json:"id"`
	Type              string       `json:"type"`
	PreferredUsername string       `json:"preferredUsername"`
	Name              string       `json:"name"`
	Inbox             string       `json:"inbox"`
	Outbox            string       `json:"outbox"`
	PublicKey         PublicKeyObj `json:"publicKey"`
}

// PublicKeyObj represents the publicKey object within an Actor
type PublicKeyObj struct {
	ID           string `json:"id"`
	Owner        string `json:"owner"`
	PublicKeyPem string `json:"publicKeyPem"`
}

// NewWebFinger constructs a WebFinger response for the relay actor
func NewWebFinger(domain, username string) WebFingerResource {
	return WebFingerResource{
		Subject: fmt.Sprintf("acct:%s@%s", username, domain),
		Links: []WebFingerLink{
			{
				Rel:  "self",
				Type: "application/activity+json",
				Href: fmt.Sprintf("https://%s/users/%s", domain, username),
			},
		},
	}
}

// NewActor constructs a simplified ActivityPub Actor profile
func NewActor(domain, username, pubKeyPem string) Actor {
	actorID := fmt.Sprintf("https://%s/users/%s", domain, username)
	return Actor{
		Context: []string{
			"https://www.w3.org/ns/activitystreams",
			"https://w3id.org/security/v1",
		},
		ID:                actorID,
		Type:              "Application",
		PreferredUsername: username,
		Name:              "ActivityPub Relay Proxy",
		Inbox:             fmt.Sprintf("%s/inbox", actorID),
		Outbox:            fmt.Sprintf("%s/outbox", actorID),
		PublicKey: PublicKeyObj{
			ID:           fmt.Sprintf("%s#main-key", actorID),
			Owner:        actorID,
			PublicKeyPem: pubKeyPem,
		},
	}
}

// ParsePrivateKey parses an RSA private key from PEM bytes (supports PKCS#1 and PKCS#8)
func ParsePrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("failed to decode PEM block for private key")
	}

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		return key, nil
	}

	// Fallback to PKCS#8
	key8, err8 := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err8 == nil {
		rsaKey, ok := key8.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("decoded private key is not an RSA key")
		}
		return rsaKey, nil
	}

	return nil, fmt.Errorf("failed to parse private key: PKCS1 error: %v, PKCS8 error: %v", err, err8)
}

// GetPublicKeyPem derives the PEM public key from an RSA private key
func GetPublicKeyPem(privKey *rsa.PrivateKey) (string, error) {
	pubKeyBytes, err := x509.MarshalPKIXPublicKey(&privKey.PublicKey)
	if err != nil {
		return "", fmt.Errorf("failed to marshal public key: %w", err)
	}

	block := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubKeyBytes,
	}

	var buf bytes.Buffer
	if err := pem.Encode(&buf, block); err != nil {
		return "", fmt.Errorf("failed to PEM encode public key: %w", err)
	}

	return buf.String(), nil
}
