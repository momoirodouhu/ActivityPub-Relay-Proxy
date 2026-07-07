package testutil

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// MockTransport implements http.RoundTripper for intercepting HTTP client calls.
type MockTransport struct {
	RoundTripFunc func(req *http.Request) (*http.Response, error)
}

// RoundTrip executes the custom mock roundtrip function if set.
func (m *MockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.RoundTripFunc != nil {
		return m.RoundTripFunc(req)
	}
	return nil, errors.New("MockTransport: RoundTripFunc not set")
}

// MockQueueClient is a mock Asynq queue client.
type MockQueueClient struct {
	mu            sync.Mutex
	EnqueuedTasks []*asynq.Task
}

// EnqueueContext adds tasks to the enqueued list for assertion in tests.
func (m *MockQueueClient) EnqueueContext(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.EnqueuedTasks = append(m.EnqueuedTasks, task)
	return nil, nil
}

// Close is a dummy implementation to satisfy the QueueClient interface.
func (m *MockQueueClient) Close() error {
	return nil
}

// MockRedis is an in-memory mock implementation of redis.Cmdable for testing.
type MockRedis struct {
	redis.Cmdable // Embedded to satisfy redis.Cmdable interface

	mu   sync.Mutex
	KV   map[string]string
	Sets map[string]map[string]bool

	// Public fields for custom error injection or assertions
	GetError error
	SetError error

	// Trace fields (useful for assertion compatibility with existing tests)
	LastSetKey string
	LastSetVal string
	LastSetExp time.Duration
}

// NewMockRedis creates an initialized MockRedis.
func NewMockRedis() *MockRedis {
	return &MockRedis{
		KV:   make(map[string]string),
		Sets: make(map[string]map[string]bool),
	}
}

// PresetKV presets key-value data directly in the mock database.
func (m *MockRedis) PresetKV(key, val string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.KV[key] = val
}

// PresetSet presets set data directly in the mock database.
func (m *MockRedis) PresetSet(key string, members ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.Sets[key]; !ok {
		m.Sets[key] = make(map[string]bool)
	}
	for _, member := range members {
		m.Sets[key][member] = true
	}
}

// Get implements redis.Cmdable Get method.
func (m *MockRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewStringCmd(ctx)
	if m.GetError != nil {
		cmd.SetErr(m.GetError)
		return cmd
	}

	val, ok := m.KV[key]
	if !ok {
		cmd.SetErr(redis.Nil)
		return cmd
	}

	cmd.SetVal(val)
	return cmd
}

// Set implements redis.Cmdable Set method.
func (m *MockRedis) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewStatusCmd(ctx)
	if m.SetError != nil {
		cmd.SetErr(m.SetError)
		return cmd
	}

	strVal := ""
	switch v := value.(type) {
	case string:
		strVal = v
	default:
		// Fallback for non-string types
		strVal = redis.NewStatusCmd(ctx).String() // Dummy
	}

	m.KV[key] = strVal
	m.LastSetKey = key
	m.LastSetVal = strVal
	m.LastSetExp = expiration

	cmd.SetVal("OK")
	return cmd
}

// SetNX implements redis.Cmdable SetNX method.
func (m *MockRedis) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewBoolCmd(ctx)
	_, exists := m.KV[key]
	if exists {
		cmd.SetVal(false)
		return cmd
	}

	strVal := ""
	if s, ok := value.(string); ok {
		strVal = s
	}
	m.KV[key] = strVal
	cmd.SetVal(true)
	return cmd
}

// Del implements redis.Cmdable Del method.
func (m *MockRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	deleted := int64(0)
	for _, key := range keys {
		if _, ok := m.KV[key]; ok {
			delete(m.KV, key)
			deleted++
		}
	}

	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(deleted)
	return cmd
}

// SAdd implements redis.Cmdable SAdd method.
func (m *MockRedis) SAdd(ctx context.Context, key string, members ...any) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.Sets[key]; !ok {
		m.Sets[key] = make(map[string]bool)
	}

	added := int64(0)
	for _, member := range members {
		strMember, ok := member.(string)
		if !ok {
			continue
		}
		if !m.Sets[key][strMember] {
			m.Sets[key][strMember] = true
			added++
		}
	}

	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(added)
	return cmd
}

// SRem implements redis.Cmdable SRem method.
func (m *MockRedis) SRem(ctx context.Context, key string, members ...any) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	set, ok := m.Sets[key]
	if !ok {
		cmd := redis.NewIntCmd(ctx)
		cmd.SetVal(0)
		return cmd
	}

	removed := int64(0)
	for _, member := range members {
		strMember, ok := member.(string)
		if !ok {
			continue
		}
		if set[strMember] {
			delete(set, strMember)
			removed++
		}
	}

	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(removed)
	return cmd
}

// SMembers implements redis.Cmdable SMembers method.
func (m *MockRedis) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewStringSliceCmd(ctx)
	set, ok := m.Sets[key]
	if !ok {
		cmd.SetVal([]string{})
		return cmd
	}

	var members []string
	for member := range set {
		members = append(members, member)
	}

	cmd.SetVal(members)
	return cmd
}
