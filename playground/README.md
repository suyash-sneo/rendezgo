# Playground

Interactive chaos lab for rendezgo controllers with a Bubble Tea TUI. Runs in simulated mode (in-process Redis) by default or against a real Redis.

## Run (simulated)

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

Use the bundled compose file for a local Redis:

```
docker compose -f playground/docker-compose.yml up -d
```

Then start the playground pointing at it:

```
go run ./playground/cmd -mode=real -redis=127.0.0.1:6379
```

## Flags

- `-workloads` (e.g., `demo:8,other:4`)
- `-nodes` initial node count
- `-mode` `simulated|real`
- `-redis` address for real mode
- `-topk` HRW candidate display depth
- `-scenario` built-in scenario name
- `-scenario-file` path to YAML/JSON timed commands

## TUI keys

- `:` enter command input
- `Enter` run command
- `Esc` cancel input
- `?` toggle help
- Arrow/Page keys scroll log; tail follows when at bottom
- Up/Down navigate command history while in command mode
- `q` / `Ctrl+C` quit

## Commands

`add [n] [weight]`, `remove <id>`, `restart <id>`, `kill <id>`, `weight <id> <w>`, `fail <id> on|off`, `health <id> on|off`, `shedding on|off`, `release <n>`, `focus <workload|none>`, `predict down <node>`, `explain <workload> <unit>`, `scenario <name>`, `load <file>`.

Scenario files are simple YAML/JSON lists of timed commands.
