package fakestore

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/you/workdist/coord"
)

// Store is an in-memory implementation of coord.Store for tests.
type Store struct {
	mu      sync.Mutex
	now     time.Time
	nodes   map[string]time.Time
	leases  map[string]leaseEntry
	configs map[string]configEntry
}

type leaseEntry struct {
	owner string
	exp   time.Time
}

type configEntry struct {
	value []byte
	exp   time.Time
}

// New returns a fresh in-memory store.
func New() *Store {
	return &Store{
		now:     time.Now(),
		nodes:   map[string]time.Time{},
		leases:  map[string]leaseEntry{},
		configs: map[string]configEntry{},
	}
}

// Advance moves the internal clock forward (useful for deterministic tests).
func (s *Store) Advance(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = s.now.Add(d)
}

func (s *Store) HeartbeatNode(_ context.Context, nodeID string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[nodeID] = s.now.Add(ttl)
	return nil
}

func (s *Store) ListNodes(_ context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpired()
	nodes := make([]string, 0, len(s.nodes))
	for id := range s.nodes {
		nodes = append(nodes, id)
	}
	return nodes, nil
}

func (s *Store) PruneDeadNodes(ctx context.Context) (int, error) {
	// Alias to eviction; ctx unused but part of interface.
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	before := len(s.nodes)
	s.evictExpired()
	return before - len(s.nodes), nil
}

func (s *Store) AcquireLease(_ context.Context, leaseKey string, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpired()
	if l, ok := s.leases[leaseKey]; ok && l.exp.After(s.now) {
		return false, nil
	}
	s.leases[leaseKey] = leaseEntry{owner: owner, exp: s.now.Add(ttl)}
	return true, nil
}

func (s *Store) RenewLease(_ context.Context, leaseKey string, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpired()
	l, ok := s.leases[leaseKey]
	if !ok || l.owner != owner {
		return false, nil
	}
	l.exp = s.now.Add(ttl)
	s.leases[leaseKey] = l
	return true, nil
}

func (s *Store) ReleaseLease(_ context.Context, leaseKey string, owner string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpired()
	l, ok := s.leases[leaseKey]
	if !ok || l.owner != owner {
		return false, nil
	}
	delete(s.leases, leaseKey)
	return true, nil
}

func (s *Store) GetLeaseOwner(_ context.Context, leaseKey string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpired()
	l, ok := s.leases[leaseKey]
	if !ok {
		return "", false, nil
	}
	return l.owner, true, nil
}

func (s *Store) GetConfig(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpired()
	cfg, ok := s.configs[key]
	if !ok {
		return nil, false, nil
	}
	return cfg.value, true, nil
}

func (s *Store) PutConfig(_ context.Context, key string, value []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs[key] = configEntry{
		value: append([]byte(nil), value...),
		exp:   s.now.Add(ttl),
	}
	return nil
}

func (s *Store) WatchConfig(context.Context, string) (<-chan coord.ConfigEvent, error) {
	return nil, errors.New("watch not supported in fakestore")
}

func (s *Store) evictExpired() {
	for id, exp := range s.nodes {
		if !exp.After(s.now) {
			delete(s.nodes, id)
		}
	}
	for key, l := range s.leases {
		if !l.exp.After(s.now) {
			delete(s.leases, key)
		}
	}
	for key, cfg := range s.configs {
		if !cfg.exp.After(s.now) {
			delete(s.configs, key)
		}
	}
}
