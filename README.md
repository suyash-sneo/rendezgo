# rendez / workdist

Distributed slot ownership for Go services without a central manager. Each node competes for leases in Redis, using rendezvous hashing to decide who _should_ own each logical slot and gentle handoff to avoid churn.

## Status

Early library skeleton with Redis backend, core coordinator, HRW hashing, and unit tests. Interfaces are stable enough for experiments; expect iteration.

## Quickstart

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/you/workdist"
	"github.com/you/workdist/coord/redis"
)

type printRunner struct{}

func (r *printRunner) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			log.Println("processing slot work")
		}
	}
}
func (r *printRunner) Stop(ctx context.Context) error { return nil }

type printFactory struct{}

func (f *printFactory) NewSlotRunner(slot workdist.Slot) (workdist.SlotRunner, error) {
	log.Printf("starting runner for %s", slot.Key())
	return &printRunner{}, nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := redis.New(redis.Options{Addr: "127.0.0.1:6379"})
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	provider := workdist.NewStaticParallelismProvider(map[string]int{
		"kafka.topic.A": 3,
	})
	cfg := workdist.DefaultConfig()
	coord, err := workdist.NewCoordinator(cfg, store, provider, &printFactory{}, workdist.NewDefaultNodeIDProvider(), workdist.NopLogger(), workdist.NopMetrics())
	if err != nil {
		log.Fatalf("coordinator: %v", err)
	}
	if err := coord.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}
```

Run multiple copies of this process against the same Redis. Slots distribute automatically and rebalance when nodes join or leave.

## Concepts

- **Work class**: Named unit (e.g., `kafka.topic.myTopic`) with configured parallelism `P`.
- **Slot**: Logical shard within a work class, indexed `0..P-1`.
- **Ownership**: Lease in Redis at `lease:<slotKey>` with TTL/renewal.
- **Assignment**: Rendezvous hashing picks the desired owner from current live nodes.
- **Stability**: Gentle handoff, cooldowns, rate limits, and backoff prevent thrash.

## Redis backend

- Uses simple keys and Lua scripts for safe renew/release.
- Supports plain Redis or Sentinel via `UniversalClient`.
- Config cache lives under `cfg:<workClass>`.

## Testing

```
go test ./...
go test -race ./...   # optional but recommended locally
```

## Roadmap

- Expand chaos playground with scripted scenarios.
- Add Prometheus metrics helpers.
- Add etcd/Consul store implementations.
- More integration tests with Docker Redis.

## Non-goals

- Exactly-once processing: make your slot work idempotent.
- Perfect partition tolerance: behavior follows best effort with Redis TTLs.
