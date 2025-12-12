This directory is kept for backward compatibility. The interactive chaos playground now lives at `cmd/playground`:

```
go run ./cmd/playground            # simulated mode with in-process Redis
go run ./cmd/playground -mode=real -redis=127.0.0.1:6379
```

Use `help` inside the REPL for commands and scenarios.
