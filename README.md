# rendezgo

Distributed workload ownership for Go services without a central manager. Each node runs the same control loops, competes for leases in Redis, and uses weighted rendezvous hashing (HRW) to decide who _should_ own every `(workload, unit)` pair. No hard steals: healthy leases are left alone and ownership changes only through expiry or voluntary handoff.

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

	"github.com/suyash-sneo/rendezgo/pkg/rendez"
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
	cfg.NodeID = "worker-1" // optional; defaults to hostname
	cfg.StaticWorkloads = []rendez.WorkloadConfig{{Name: "inbox", Units: 4}}

	ctrl, err := rendez.NewController(cfg, client, printerFactory{}, rendez.NopLogger(), rendez.NopMetrics())
	if err != nil {
		log.Fatalf("controller: %v", err)
	}
	if err := ctrl.Start(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("controller exited: %v", err)
	}
}
```

Run multiple processes with the same Redis endpoint; units will converge to the HRW owner set, recover quickly on node death, and never steal healthy leases.

## Behavior and guarantees

- HRW/weighted HRW for deterministic desired owners across live nodes; weights can be static or provided dynamically.
- Redis keys: `node:<id>` heartbeat with TTL, `nodes:all` membership set, `lease:<workload>:<unit>` with TTL, `moved:<workload>:<unit>` cooldown markers, `cfg:workloads` index, and `cfg:workload:<name>` for dynamic workload configs. PubSub channel remains `cfg:updates`.
- Acquisition only touches unowned desired units (`SET ... NX EX`); healthy leases are not preempted.
- Fast recovery when leases expire; new nodes wait for natural release unless gentle handoff is enabled.
- Stabilizers: cooldown per unit, minimum runtime per consumer, Redis backoff gates, per-node and per-workload caps, and rate-limited shedding.
- Lua scripts (compare-and-set renew/release) live in `internal/redis_scripts` and are exercised in tests with miniredis.

## Config highlights (defaults from `DefaultConfig()`)

| Field | Default | Notes |
| --- | --- | --- |
| `HeartbeatTTL` | 120s | TTL for `node:<id>` keys |
| `HeartbeatInterval` | 10s | refresh cadence + membership `SADD` |
| `LeaseTTL` | 90s | lease TTL |
| `LeaseRenewInterval` | 30s | renewed via Lua compare-and-expire |
| `ReconcileInterval` | 22s | desired assignment + acquisition/shedding |
| `ReconcileJitter` | 0.15 | spreads thundering herds |
| `SlotMoveCooldown` | 5m | cooldown key `moved:<workload>:<unit>` after moves |
| `MinConsumerRuntime` | 90s | no voluntary shedding before this runtime |
| `SheddingEnabled` | true | gentle handoff of undesired units |
| `SheddingRelease` | 2 | max voluntary releases per reconcile |
| `MaxConsumersPerNode` | 0 | node-wide cap (0 = unlimited) |
| `MaxConsumersPerWorkloadPerNode` | 0 | per-workload cap per node |
| `MinConsumersPerNode` | 0 | lower bound during shedding |
| `RedisBackoff` | 45s | backoff window on Redis instability |
| `ConfigWatchChannel` | `cfg:updates` | optional pubsub push for workload configs |
| `WorkloadConfigKeyPrefix` | `cfg:workload:` | dynamic config key prefix |
| `WorkloadIndexKey` | `cfg:workloads` | dynamic workload index set |

All knobs are exposed on `Config`; validate with `Validate()` before wiring up a controller.

## Stabilizers in practice

- **Cooldowns:** when a unit is acquired, `moved:<workload>:<unit>` is written with a TTL. HRW-desired nodes skip taking over healthy leases during cooldown; fast recovery still happens when leases expire.
- **Minimum runtime:** consumers are not shed until they have been alive for `MinConsumerRuntime`.
- **Backoff:** Redis errors trigger a backoff window that halts acquisition/shedding but continues renewals. Consumers stop only when a renew fails.
- **Caps:** `MaxConsumersPerNode` and `MaxConsumersPerWorkloadPerNode` gate acquisition; shedding will not drive below `MinConsumersPerNode`.
- **Shedding:** optional and rate-limited (`SheddingRelease` per interval); uses compare-and-delete Lua to avoid races.

## Chaos playground

An interactive chaos lab that runs real controllers against an in-process Redis by default:

```
go run ./cmd/playground                 # simulated Redis
go run ./cmd/playground -mode=real -redis=host:6379
```

Dashboard shows node weights/state, owned vs desired counts, per-workload breakdowns, churn/min, caps, per-unit ownership/TTL/cooldown, and HRW candidate queues. Commands: `add [n] [weight]`, `remove <id>`, `restart <id>`, `kill <id>`, `weight <id> <w>`, `fail <id> on|off`, `health <id> on|off`, `shedding on|off`, `release <n>`, `focus <workload|none>`, `dashboard on|off`, `predict down <node>`, `explain <workload> <unit>`, `scenario <name>`, `load <file>`. Use `-workloads` (`name:units,...`) and `-dashboard-interval` flags to tune simulations. Scenario files are simple YAML/JSON lists of timed commands.

## Architecture

See `docs/architecture.md` for loop breakdowns, key formats, stabilizers, and package layout (`pkg/rendez` for the public API, `internal/redis_scripts`, `cmd/rendez-agent`, `cmd/playground`).

## Testing

```
go test ./...
```
