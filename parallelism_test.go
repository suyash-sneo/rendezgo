package workdist

import (
	"context"
	"testing"
	"time"

	"github.com/you/workdist/internal/fakestore"
)

func TestCachedProviderUsesFallbackOnce(t *testing.T) {
	store := fakestore.New()
	ctx := context.Background()
	calls := 0
	fallback := func(ctx context.Context, workClass string) (int, string, error) {
		calls++
		return 3, "v1", nil
	}
	provider := NewCachedParallelismProvider(store, time.Minute, fallback, nil)

	p, version, err := provider.Get(ctx, "topicA")
	if err != nil {
		t.Fatalf("first get err: %v", err)
	}
	if p != 3 || version != "v1" {
		t.Fatalf("unexpected get result: p=%d version=%s", p, version)
	}
	p, version, err = provider.Get(ctx, "topicA")
	if err != nil {
		t.Fatalf("second get err: %v", err)
	}
	if p != 3 || version != "v1" {
		t.Fatalf("unexpected cached result: p=%d version=%s", p, version)
	}
	if calls != 1 {
		t.Fatalf("fallback called %d times, expected 1", calls)
	}
}

func TestListUnsupported(t *testing.T) {
	provider := NewCachedParallelismProvider(fakestore.New(), time.Minute, nil, nil)
	if _, err := provider.List(context.Background()); err == nil {
		t.Fatalf("expected list unsupported error")
	}
}

func TestStaticParallelismProvider(t *testing.T) {
	provider := NewStaticParallelismProvider(map[string]int{"a": 2})
	p, version, err := provider.Get(context.Background(), "a")
	if err != nil {
		t.Fatalf("get err: %v", err)
	}
	if p != 2 || version != "static" {
		t.Fatalf("unexpected values p=%d version=%s", p, version)
	}
	if _, _, err := provider.Get(context.Background(), "missing"); err == nil {
		t.Fatalf("expected error for missing work class")
	}
}
