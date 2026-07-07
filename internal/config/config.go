package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v10"
	"github.com/joho/godotenv"
)

type Config struct {
	Port             int           `env:"PORT" envDefault:"8080"`
	Domain           string        `env:"DOMAIN,required"`
	RedisURL         string        `env:"REDIS_URL,required"`
	PrivateKeyB64    string        `env:"PRIVATE_KEY,required"`
	PrivateKeyPem    []byte        // Decoded from PrivateKeyB64
	ActorUsername    string        `env:"ACTOR_USERNAME" envDefault:"relay"`
	DestinationURL   string        `env:"DESTINATION_URL,required"`
	FilterKeywords   []string      `env:"FILTER_KEYWORDS" envSeparator:","`
	FilterHashtags   []string      `env:"FILTER_HASHTAGS" envSeparator:","`
	DeduplicationTTL time.Duration `env:"DEDUPLICATION_TTL" envDefault:"24h"`
}

func Load() (*Config, error) {
	// Load .env file if it exists (local development)
	if err := godotenv.Load(); err != nil {
		// Ignore error if file doesn't exist, as env vars might be set in environment
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("error loading .env file: %w", err)
		}
	}

	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("error parsing env variables: %w", err)
	}

	// Decode PrivateKey
	dec, err := base64.StdEncoding.DecodeString(strings.TrimSpace(cfg.PrivateKeyB64))
	if err != nil {
		return nil, fmt.Errorf("failed to decode PRIVATE_KEY from base64: %w", err)
	}
	cfg.PrivateKeyPem = dec

	return &cfg, nil
}
