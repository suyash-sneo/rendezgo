# Playground (web)

Glass HUD for rendezgo controllers. The playground spins up real controllers (simulated Redis by default), exposes a web UI over HTTP, and streams events via SSE so nothing disappears between refreshes.

## Run

```bash
go run ./playground/cmd            # simulated (in-process Redis)
go run ./playground/cmd -mode=real -redis=127.0.0.1:6379
```

Flags: `-listen` (default `127.0.0.1:8080`), `-open` (auto-open browser), `-workloads` (`name:units,...`), `-nodes`, `-topk`, `-mode`, `-redis`, `-scenario`, `-scenario-file`.

UI highlights:

- HUD: mode/redis/cluster plus churn, convergence, desired vs owned, misalignment, focus selector.
- Nodes panel: state/backoff/health/fault pills, weight editor, owned vs desired bars, restart/remove/kill.
- Assignments: per-workload grid with owner vs desired, lease TTL bars, cooldown bars, HRW top-K hover, misalignment markers.
- Details: lease/cooldown keys + TTLs, HRW ranking with “Explain” (POST `/api/v1/explain/unit`), predict-down with candidate summary (POST `/api/v1/predict/down`).
- Events: live SSE log (GET `/api/v1/events`), filter/search, export JSON.

## Real Redis

Use the bundled compose file for a local Redis:

```bash
docker compose -f playground/docker-compose.yml up -d
go run ./playground/cmd -mode=real -redis=127.0.0.1:6379
```

## Scenarios

- Built-ins: `scale-out`, `pod-death`, `redis-instability`, `weight-skew`, `cross-cluster`, `lease-flapping` (POST `/api/v1/scenario/run`).
- Upload timed scenario files (YAML/JSON list of `{at: "1s", command: "add 2"}`) via UI or `POST /api/v1/scenario/upload` (multipart field `file`).

## API surface (v1)

- `GET /api/v1/snapshot` → snapshot schema (nowUnixMs, config, metrics, workloads, nodes, units with TTLs, cooldowns, HRW topK).
- `GET /api/v1/events?since=<seq>` → SSE stream (`event: <type>` + Event JSON).
- Node control: `POST /api/v1/nodes/add`, `/remove`, `/restart`, `/kill`, `/weight`, `/health`, `/redisFault`.
- Shedding/focus: `POST /api/v1/shedding/toggle`, `/shedding/release`, `/focus`.
- Scenarios: `POST /api/v1/scenario/run`, `POST /api/v1/scenario/upload`.
- Analysis: `POST /api/v1/predict/down`, `POST /api/v1/explain/unit`.

Error shape: `{ "error": { "code": "bad_request", "message": "..." } }`.
