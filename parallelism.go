package workdist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/you/workdist/coord"
)

const configKeyPrefix = "cfg:"

// FallbackFetcher returns parallelism for a work class when not present in cache.
type FallbackFetcher func(ctx context.Context, workClass string) (parallelism int, version string, err error)

// ListFunc lists known work classes for the provider.
type ListFunc func(ctx context.Context) ([]WorkClassConfig, error)

// CachedParallelismProvider caches parallelism values in the coordination store.
type CachedParallelismProvider struct {
	store    coord.Store
	cacheTTL time.Duration
	fallback FallbackFetcher
	listFn   ListFunc
}

// ErrWorkClassNotFound indicates missing configuration.
var ErrWorkClassNotFound = errors.New("work class not found")

// ErrListUnsupported indicates listing work classes is not available.
var ErrListUnsupported = errors.New("list not supported by provider")

// NewCachedParallelismProvider constructs a provider backed by the store.
func NewCachedParallelismProvider(store coord.Store, cacheTTL time.Duration, fallback FallbackFetcher, listFn ListFunc) *CachedParallelismProvider {
	return &CachedParallelismProvider{
		store:    store,
		cacheTTL: cacheTTL,
		fallback: fallback,
		listFn:   listFn,
	}
}

func (p *CachedParallelismProvider) Get(ctx context.Context, workClass string) (int, string, error) {
	key := configKeyPrefix + workClass
	if data, ok, err := p.store.GetConfig(ctx, key); err == nil && ok {
		var stored cachedConfig
		if err := json.Unmarshal(data, &stored); err == nil {
			return stored.Parallelism, stored.Version, nil
		}
	}
	if p.fallback == nil {
		return 0, "", ErrWorkClassNotFound
	}
	parallelism, version, err := p.fallback(ctx, workClass)
	if err != nil {
		return 0, "", err
	}
	cfg := cachedConfig{
		Parallelism: parallelism,
		Version:     version,
		UpdatedAt:   time.Now().UTC(),
	}
	if b, err := json.Marshal(cfg); err == nil {
		_ = p.store.PutConfig(ctx, key, b, p.cacheTTL)
	}
	return parallelism, version, nil
}

func (p *CachedParallelismProvider) List(ctx context.Context) ([]WorkClassConfig, error) {
	if p.listFn == nil {
		return nil, ErrListUnsupported
	}
	return p.listFn(ctx)
}

type cachedConfig struct {
	Parallelism int       `json:"parallelism"`
	Version     string    `json:"version"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// StaticParallelismProvider returns fixed parallelism values for tests or examples.
type StaticParallelismProvider struct {
	configs []WorkClassConfig
	index   map[string]WorkClassConfig
}

// NewStaticParallelismProvider creates a provider from a map of work class to parallelism.
func NewStaticParallelismProvider(m map[string]int) *StaticParallelismProvider {
	configs := make([]WorkClassConfig, 0, len(m))
	index := make(map[string]WorkClassConfig, len(m))
	for wc, p := range m {
		cfg := WorkClassConfig{WorkClass: wc, Parallelism: p, Version: "static"}
		configs = append(configs, cfg)
		index[wc] = cfg
	}
	return &StaticParallelismProvider{configs: configs, index: index}
}

func (s *StaticParallelismProvider) Get(ctx context.Context, workClass string) (int, string, error) {
	_ = ctx
	cfg, ok := s.index[workClass]
	if !ok {
		return 0, "", fmt.Errorf("%w: %s", ErrWorkClassNotFound, workClass)
	}
	return cfg.Parallelism, cfg.Version, nil
}

func (s *StaticParallelismProvider) List(ctx context.Context) ([]WorkClassConfig, error) {
	_ = ctx
	return append([]WorkClassConfig(nil), s.configs...), nil
}
