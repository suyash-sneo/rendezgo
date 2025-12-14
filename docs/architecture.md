# Architecture

## Overview

`pkg/rendezgo` exposes a single `Controller` that runs inside each node. The controller keeps a heartbeat in Redis, watches workload configs, computes desired ownership with weighted rendezvous hashing, and acquires only unowned desired units. Renewals use Lua compare-and-expire scripts; releases use compare-and-delete. The design favors liveness with bounded churn and no hard stealing.

## Key Redis data

- `node:<nodeID>`: heartbeat value `cluster|node` with TTL.
- `nodes:all`: set of known node IDs (cluster-specific sets are also maintained).
- `lease:<workload>:<unit>`: unit lease storing the owner nodeID with TTL.
- `moved:<workload>:<unit>`: cooldown marker with TTL set on successful acquire/release.
- `cfg:workload:<name>` + `cfg:workloads`: optional workload config cache; updates can be pushed via `cfg:updates` pubsub.

## Loops

- **Heartbeat loop**: refresh `node:<id>` + `SADD nodes:all` at `HeartbeatInterval`.
- **Config cache loop**: subscribe to `ConfigWatchChannel` (optional) and periodically reload workload configs or fall back to static configs.
- **Reconcile loop**: every `ReconcileInterval` (+ jitter) gather live nodes, compute desired units via HRW, enforce caps, acquire only unowned desired units (no stealing), and optionally shed undesired units subject to cooldown, min runtime, health, and rate limits.
- **Renew loop**: run Lua renew for each owned lease; loss stops the consumer immediately.

## Stabilizers

- **Cooldown per unit**: `moved:<workload>:<unit>` blocks voluntary takeover of healthy leases; fast recovery still allowed on expiry.
- **Minimum runtime**: consumers are not shed until `MinConsumerRuntime` elapses.
- **Redis backoff**: Redis timeouts/errors trigger a backoff window where acquisition/shedding are paused but renewals continue.
- **Caps**: `MaxConsumersPerNode`, `MaxConsumersPerWorkloadPerNode`, and `MinConsumersPerNode` guard acquisition and shedding.
- **Gentle handoff**: rate-limited shedding of undesired units via compare-and-delete Lua; never steals.
- **Weights**: HRW weights can be static or provided dynamically to bias ownership by capacity.

## Packages and binaries

- `pkg/rendezgo`: public API (`Config`, `Controller`, `OwnedSlots`, interfaces for Redis client, consumer factory, logger, metrics, weight/health hooks).
- `internal/redis_scripts`: Lua for renew/release with sha helpers.
- `cmd/rendez-agent`: example binary that wires a controller to Redis with a simple consumer factory.
- `cmd/playground`: interactive chaos playground (simulated Redis by default, real mode available) with scenarios, live dashboard, and HRW explanations.

## Failure semantics

- If Redis is unreachable, acquisition/shedding stop and backoff activates; renewals keep running until leases expire.
- Lease loss stops the consumer promptly.
- Node removal is detected via heartbeat TTL expiry; HRW recalculation plus cooldown/shedding handles reassignment without stealing.
