# Playground

An interactive chaos lab that runs real rendezgo controllers against Redis. It defaults to an in-process Redis simulator but can target a real Redis easily.

## Run (simulated Redis)

From repo root:

```
go run ./playground/cmd
```

Or from this directory:

```
cd playground
go run ./cmd
```

## Run against real Redis

Spin up the included Redis:

```
docker compose -f playground/docker-compose.yml up -d
```

Then point the playground at it:

```
go run ./playground/cmd -mode=real -redis=127.0.0.1:6379
```

## Useful flags

- `-workloads` (`name:units,name2:units2`) to set workloads/units (default `demo:8`).
- `-dashboard-interval` to control dashboard refresh (default 1s).
- `-mode` `simulated|real` and `-redis` address for real mode.

Inside the REPL, type `help` for commands (add/remove nodes, adjust weights, health/fault toggles, focus/predict/explain, scenarios, etc.).
