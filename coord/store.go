package coord

import (
	"context"
	"time"
)

// Store defines the coordination backend behavior.
type Store interface {
	// Node registry
	HeartbeatNode(ctx context.Context, nodeID string, ttl time.Duration) error
	ListNodes(ctx context.Context) ([]string, error)
	PruneDeadNodes(ctx context.Context) (int, error)

	// Lease operations
	AcquireLease(ctx context.Context, leaseKey string, owner string, ttl time.Duration) (bool, error)
	RenewLease(ctx context.Context, leaseKey string, owner string, ttl time.Duration) (bool, error)
	ReleaseLease(ctx context.Context, leaseKey string, owner string) (bool, error)
	GetLeaseOwner(ctx context.Context, leaseKey string) (owner string, ok bool, err error)

	// Config cache
	GetConfig(ctx context.Context, key string) (value []byte, ok bool, err error)
	PutConfig(ctx context.Context, key string, value []byte, ttl time.Duration) error
	WatchConfig(ctx context.Context, keyPrefix string) (<-chan ConfigEvent, error)
}

// ConfigEvent represents an update from the store.
type ConfigEvent struct {
	Key   string
	Value []byte
	Err   error
}
