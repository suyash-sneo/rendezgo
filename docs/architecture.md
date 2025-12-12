# Architecture

## Overview

Each node runs a coordinator that competes for logical slots using Redis as the coordination store. Desired ownership is determined via rendezvous (highest-random-weight) hashing across the live node set. Ownership is represented by a lease key with TTL and conditional renew/release semantics.

## Key data

- `node:<nodeID>`: heartbeat key with TTL.
- `nodes:all`: set of known node IDs.
- `lease:<slotKey>`: slot ownership lease storing the owner node ID.
- `cfg:<workClass>`: cached parallelism config for a work class.

## Loops

- **Heartbeat**: refresh `node:<nodeID>` and ensure membership.
- **Reconcile**: list nodes, fetch work classes, compute desired slots via HRW, acquire missing leases up to `MaxAcquirePerTick`, optionally release undesired slots using gentle handoff.
- **Renew**: periodically renew held leases; if renew fails, stop the local runner.

## Stability features

- Gentle handoff releases only undesired slots, capped by `ReleasePerMinute`.
- Cooldowns prevent rapid reacquire/release cycles.
- Minimum runtime per slot guards against flapping immediately after a move.
- Per-node and per-class caps bound resource usage.
- Exponential backoff config is exposed for store errors (not fully wired yet).

## Backends

Redis implements the `coord.Store` interface using simple SET/GET and Lua scripts for renew/release. The interface keeps Redis-specific details out of the coordinator to allow future etcd/Consul backends.

## Failure semantics

- If Redis is unavailable, existing runners keep working until renew fails; new leases are not acquired.
- Lease loss triggers runner shutdown.
- Network partitions can lead to brief double-ownership until TTL expiry; design favors liveness with bounded staleness.

## Extensibility

- HRW hashing lives in `hash/` and can be swapped for weighted HRW later.
- Parallelism provider is pluggable; the default cached provider falls back to user logic when the store misses.
- Metrics and logging use lightweight interfaces for easy adapter hooks.
