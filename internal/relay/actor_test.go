package relay

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
)

func TestNewWebFinger(t *testing.T) {
	domain := "relay.example.com"
	username := "relay"
	wf := NewWebFinger(domain, username)

	expectedSubject := "acct:relay@relay.example.com"
	if wf.Subject != expectedSubject {
		t.Errorf("expected subject %s, got %s", expectedSubject, wf.Subject)
	}

	if len(wf.Links) != 1 {
		t.Fatalf("expected 1 link, got %d", len(wf.Links))
	}

	link := wf.Links[0]
	if link.Rel != "self" {
		t.Errorf("expected rel self, got %s", link.Rel)
	}
	if link.Type != "application/activity+json" {
		t.Errorf("expected type application/activity+json, got %s", link.Type)
	}
	expectedHref := "https://relay.example.com/users/relay"
	if link.Href != expectedHref {
		t.Errorf("expected href %s, got %s", expectedHref, link.Href)
	}
}

func TestNewActor(t *testing.T) {
	domain := "relay.example.com"
	username := "relay"
	pubKeyPem := "dummy-pem"
	actor := NewActor(domain, username, pubKeyPem)

	expectedID := "https://relay.example.com/users/relay"
	if actor.ID != expectedID {
		t.Errorf("expected ID %s, got %s", expectedID, actor.ID)
	}
	if actor.Type != "Application" {
		t.Errorf("expected type Application, got %s", actor.Type)
	}
	if actor.PreferredUsername != username {
		t.Errorf("expected preferredUsername %s, got %s", username, actor.PreferredUsername)
	}
	if actor.Inbox != "https://relay.example.com/users/relay/inbox" {
		t.Errorf("expected inbox path, got %s", actor.Inbox)
	}

	if actor.PublicKey.ID != "https://relay.example.com/users/relay#main-key" {
		t.Errorf("expected public key ID, got %s", actor.PublicKey.ID)
	}
	if actor.PublicKey.PublicKeyPem != pubKeyPem {
		t.Errorf("expected public key PEM, got %s", actor.PublicKey.PublicKeyPem)
	}
}

func TestPrivateKeyParsingAndPublicKeyDerivation(t *testing.T) {
	// Generate an RSA private key
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}

	// PEM encode the private key
	privBytes := x509.MarshalPKCS1PrivateKey(privKey)
	privBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privBytes,
	}
	pemBytes := pem.EncodeToMemory(privBlock)

	// Test ParsePrivateKey
	parsedPriv, err := ParsePrivateKey(pemBytes)
	if err != nil {
		t.Fatalf("failed to parse private key: %v", err)
	}

	if parsedPriv.N.Cmp(privKey.N) != 0 {
		t.Error("parsed private key modulus does not match original")
	}

	// Test GetPublicKeyPem
	pubKeyPem, err := GetPublicKeyPem(parsedPriv)
	if err != nil {
		t.Fatalf("failed to derive public key PEM: %v", err)
	}

	// Decode derived public key
	pubBlock, _ := pem.Decode([]byte(pubKeyPem))
	if pubBlock == nil {
		t.Fatal("failed to decode derived public key PEM")
	}
	if pubBlock.Type != "PUBLIC KEY" {
		t.Errorf("expected PEM block type PUBLIC KEY, got %s", pubBlock.Type)
	}

	parsedPub, err := x509.ParsePKIXPublicKey(pubBlock.Bytes)
	if err != nil {
		t.Fatalf("failed to parse PKIX public key: %v", err)
	}

	rsaPub, ok := parsedPub.(*rsa.PublicKey)
	if !ok {
		t.Fatal("derived public key is not an RSA public key")
	}

	if rsaPub.N.Cmp(privKey.PublicKey.N) != 0 {
		t.Error("derived public key modulus does not match original")
	}
}
