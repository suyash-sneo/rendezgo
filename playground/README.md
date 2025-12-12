## Chaos playground (local)

Spin up Redis:

```
cd playground
docker-compose up -d
```

Run multiple workers in one process:

```
go run ./cmd/chaos -nodes=3 -work-class=topic.demo -parallelism=6 -v
```

Watch logs to see slot ownership shift as you start/stop the process. Adjust flags to explore different parallelism and node counts.
