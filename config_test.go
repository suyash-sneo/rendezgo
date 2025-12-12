package workdist

import (
	"testing"
	"time"
)

func TestDefaultConfigValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
}

func TestInvalidConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeHeartbeatPeriod = cfg.NodeTTL
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for heartbeat >= ttl")
	}
}

func TestBackoffNext(t *testing.T) {
	b := BackoffConfig{Base: time.Second, Max: 10 * time.Second, Multiplier: 2}
	if got := b.Next(0); got != time.Second {
		t.Fatalf("retry0 expected 1s, got %v", got)
	}
	if got := b.Next(2); got != 4*time.Second {
		t.Fatalf("retry2 expected 4s, got %v", got)
	}
	if got := b.Next(10); got != b.Max {
		t.Fatalf("expected cap at %v, got %v", b.Max, got)
	}
}
