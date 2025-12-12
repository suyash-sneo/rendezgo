# rendez

Distributed slot ownership for Go services without a central manager. Each node runs the same control loops, competes for leases in Redis, and uses weighted rendezvous hashing (HRW) to decide who _should_ own every `(topic, slot)` pair. No hard steals: healthy leases are left alone and ownership changes only through expiry or voluntary handoff.

## Quickstart

```go
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/suyash-sneo/rendez/pkg/rendez"
)

type printerFactory struct{}

func (printerFactory) NewConsumer(slot rendez.Slot) (rendez.Consumer, error) {
	return &printer{slot: slot}, nil
}

type printer struct{ slot rendez.Slot }

func (p *printer) Run(ctx context.Context) error {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			log.Printf("[slot %s] doing work", p.slot.Key())
		}
	}
}
func (p *printer) Stop(context.Context) error { return nil }

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	cfg := rendez.DefaultConfig()
	cfg.ClusterID = "demo"
	cfg.NodeID = "worker-1"          // optional; defaults to hostname
	cfg.StaticTopics = []rendez.TopicConfig{{Name: "topicA", Partitions: 4}}

	ctrl, err := rendez.NewController(cfg, client, printerFactory{}, rendez.NopLogger(), rendez.NopMetrics())
	if err != nil {
		log.Fatalf("controller: %v", err)
	}
	if err := ctrl.Start(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("controller exited: %v", err)
	}
}
```

Run multiple processes with the same Redis endpoint; slots will converge to the HRW owner set, recover quickly on pod death, and never steal healthy leases.

## Behavior and guarantees

- HRW/weighted HRW for deterministic desired owners across live nodes; weights can be static or provided dynamically.
- Redis keys: `node:<id>` heartbeat with TTL, `nodes:all` membership set, `lease:<topic>:<slot>` with TTL, and `moved:<topic>:<slot>` cooldown markers.
- Acquisition only touches unowned desired slots (`SET ... NX EX`); healthy leases are not preempted.
- Fast recovery when leases expire; new nodes wait for natural release unless gentle handoff is enabled.
- Stabilizers: cooldown per slot, minimum runtime per consumer, Redis backoff gates, per-pod and per-topic caps, optional Kafka oversubscription gate, and rate-limited shedding.
- Lua scripts (compare-and-set renew/release) live in `internal/redis_scripts` and are exercised in tests with miniredis.

## Config highlights (defaults from `DefaultConfig()`)

| Field | Default | Notes |
| --- | --- | --- |
| `HeartbeatTTL` | 120s | TTL for `node:<id>` keys |
| `HeartbeatInterval` | 10s | refresh cadence + membership `SADD` |
| `LeaseTTL` | 90s | slot lease TTL |
| `LeaseRenewInterval` | 30s | renewed via Lua compare-and-expire |
| `ReconcileInterval` | 22s | desired assignment + acquisition/shedding |
| `ReconcileJitter` | 0.15 | spreads thundering herds |
| `SlotMoveCooldown` | 5m | cooldown key `moved:<topic>:<slot>` after moves |
| `MinConsumerRuntime` | 90s | no voluntary shedding before this runtime |
| `SheddingEnabled` | true | gentle handoff of undesired slots |
| `SheddingRelease` | 2 | max voluntary releases per reconcile |
| `MaxConsumersPerPod` | 0 | pod-wide cap (0 = unlimited) |
| `MaxConsumersPerTopicPerPod` | 0 | per-topic cap per pod |
| `RedisBackoff` | 45s | backoff window on Redis instability |
| `ConfigWatchChannel` | `cfg:updates` | optional pubsub push for topic configs |

All knobs are exposed on `Config`; validate with `Validate()` before wiring up a controller.

## Stabilizers in practice

- **Cooldowns:** when a slot is acquired, `moved:<topic>:<slot>` is written with a TTL. HRW-desired nodes skip taking over healthy leases during cooldown; fast recovery still happens when leases expire.
- **Minimum runtime:** consumers are not shed until they have been alive for `MinConsumerRuntime`.
- **Backoff:** Redis errors trigger a backoff window that halts acquisition/shedding but continues renewals. Consumers stop only when a renew fails.
- **Caps:** `MaxConsumersPerPod` and `MaxConsumersPerTopicPerPod` gate acquisition; shedding will not drive below `MinConsumersPerPod`.
- **Shedding:** optional and rate-limited (`SheddingRelease` per interval); uses compare-and-delete Lua to avoid races.

## Chaos playground

An interactive chaos lab that runs real controllers against an in-process Redis by default:

```
go run ./cmd/playground            # simulated Redis/Kafka-free mode
go run ./cmd/playground -mode=real -redis=host:6379
```

Dashboard shows nodes, weights, owned counts, backoff state, slot ownership matrix, churn/min, caps, and convergence %. Commands: `add [n] [weight]`, `remove <id>`, `restart <id>`, `kill <id>`, `weight <id> <w>`, `fail <id> on|off`, `health <id> on|off`, `shedding on|off`, `release <n>`, `scenario <name>`, `load <file>`. Built-in scenarios cover scale-out smoothness, pod death storm, redis instability, weight skew, cross-cluster collisions, and lease flapping. Scenario files are simple YAML/JSON lists of timed commands.

## Architecture

See `docs/architecture.md` for loop breakdowns, key formats, stabilizers, and package layout (`pkg/rendez` for the public API, `internal/redis_scripts`, `cmd/rendez-agent`, `cmd/playground`).

## Testing

```
go test ./...
```
