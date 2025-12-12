# Architecture

## Overview

`pkg/rendez` exposes a single `Controller` that runs inside each pod. The controller keeps a heartbeat in Redis, watches topic configs, computes desired ownership with weighted rendezvous hashing, and acquires only unowned desired slots. Renewals use Lua compare-and-expire scripts; releases use compare-and-delete. The design favors liveness with bounded churn and no hard stealing.

## Key Redis data

- `node:<nodeID>`: heartbeat value `cluster|node` with TTL.
- `nodes:all`: set of known node IDs (cluster-specific sets are also maintained).
- `lease:<topic>:<slot>`: slot lease storing the owner nodeID with TTL.
- `moved:<topic>:<slot>`: cooldown marker with TTL set on successful acquire/release.
- `cfg:<topic>` + `cfg:topics`: optional topic config cache; updates can be pushed via `cfg:updates` pubsub.

## Loops

- **Heartbeat loop**: refresh `node:<id>` + `SADD nodes:all` at `HeartbeatInterval`.
- **Config cache loop**: subscribe to `ConfigWatchChannel` (optional) and periodically reload topic configs or fall back to static configs.
- **Reconcile loop**: every `ReconcileInterval` (+ jitter) gather live nodes, compute desired slots via HRW, enforce caps, acquire only unowned desired slots (no stealing), and optionally shed undesired slots subject to cooldown, min runtime, health, and rate limits.
- **Renew loop**: run Lua renew for each owned lease; loss stops the consumer immediately.

## Stabilizers

- **Cooldown per slot**: `moved:<topic>:<slot>` blocks voluntary takeover of healthy leases; fast recovery still allowed on expiry.
- **Minimum runtime**: consumers are not shed until `MinConsumerRuntime` elapses.
- **Redis backoff**: Redis timeouts/errors trigger a backoff window where acquisition/shedding are paused but renewals continue.
- **Caps**: `MaxConsumersPerPod`, `MaxConsumersPerTopicPerPod`, and `MinConsumersPerPod` guard acquisition and shedding.
- **Gentle handoff**: rate-limited shedding of undesired slots via compare-and-delete Lua; never steals.
- **Weights**: HRW weights can be static or provided dynamically to bias ownership by capacity.
- **Kafka gate** (optional): user-provided check to halt acquisition when consumer groups are oversubscribed.

## Packages and binaries

- `pkg/rendez`: public API (`Config`, `Controller`, `OwnedSlots`, interfaces for Redis client, consumer factory, logger, metrics, weight/health/Kafka hooks).
- `internal/redis_scripts`: Lua for renew/release with sha helpers.
- `cmd/rendez-agent`: example binary that wires a controller to Redis with a simple consumer factory.
- `cmd/playground`: interactive chaos playground (simulated Redis by default, real mode available) with scenarios and live dashboard.

## Failure semantics

- If Redis is unreachable, acquisition/shedding stop and backoff activates; renewals keep running until leases expire.
- Lease loss stops the consumer promptly.
- Node removal is detected via heartbeat TTL expiry; HRW recalculation plus cooldown/shedding handles reassignment without stealing.
