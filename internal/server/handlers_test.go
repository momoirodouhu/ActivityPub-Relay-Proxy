package server

import (
	"testing"

	"github.com/momoirodouhu/ActivityPub-Relay-Proxy/internal/config"
)

func TestMatchesFilter(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *config.Config
		activity map[string]any
		expected bool
	}{
		{
			name: "No filters defined should pass everything",
			cfg: &config.Config{
				FilterKeywords: []string{},
				FilterHashtags: []string{},
			},
			activity: map[string]any{
				"object": map[string]any{
					"content": "Hello world from Fediverse!",
				},
			},
			expected: true,
		},
		{
			name: "Keyword match in content",
			cfg: &config.Config{
				FilterKeywords: []string{"golang", "rust"},
			},
			activity: map[string]any{
				"object": map[string]any{
					"content": "I am coding in Golang today.",
				},
			},
			expected: true,
		},
		{
			name: "Keyword mismatch in content",
			cfg: &config.Config{
				FilterKeywords: []string{"golang", "rust"},
			},
			activity: map[string]any{
				"object": map[string]any{
					"content": "I am coding in Python today.",
				},
			},
			expected: false,
		},
		{
			name: "Keyword match in summary (Content Warning)",
			cfg: &config.Config{
				FilterKeywords: []string{"spoiler"},
			},
			activity: map[string]any{
				"object": map[string]any{
					"summary": "Movie Spoiler warning",
					"content": "The hero wins.",
				},
			},
			expected: true,
		},
		{
			name: "Hashtag match (case insensitive, strip #)",
			cfg: &config.Config{
				FilterHashtags: []string{"#Mastodon", "ActivityPub"},
			},
			activity: map[string]any{
				"object": map[string]any{
					"content": "Check out this relay!",
					"tag": []any{
						map[string]any{
							"type": "Hashtag",
							"name": "#mastodon",
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Hashtag mismatch",
			cfg: &config.Config{
				FilterHashtags: []string{"golang"},
			},
			activity: map[string]any{
				"object": map[string]any{
					"content": "Check this!",
					"tag": []any{
						map[string]any{
							"type": "Hashtag",
							"name": "#python",
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Announce with nested Note matching keyword",
			cfg: &config.Config{
				FilterKeywords: []string{"golang"},
			},
			activity: map[string]any{
				"type": "Announce",
				"object": map[string]any{
					"type":    "Note",
					"content": "Learning golang is fun!",
				},
			},
			expected: true,
		},
		{
			name: "Announce with nested Note mismatching keyword",
			cfg: &config.Config{
				FilterKeywords: []string{"golang"},
			},
			activity: map[string]any{
				"type": "Announce",
				"object": map[string]any{
					"type":    "Note",
					"content": "Learning ruby is fun!",
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				cfg: tt.cfg,
			}
			got := s.matchesFilter(tt.activity)
			if got != tt.expected {
				t.Errorf("matchesFilter() = %v, expected %v", got, tt.expected)
			}
		})
	}
}
