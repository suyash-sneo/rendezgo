package sim

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/suyash-sneo/rendezgo/pkg/rendezgo"
)

func TestSimulatedMode_DrivesTTLAndRebalancesOnScaleOut(t *testing.T) {
	eng, err := NewEngine(EngineOptions{
		Mode:             "simulated",
		InitialNodes:     2,
		Workloads:        []rendezgo.WorkloadConfig{{Name: "demo", Units: 8}},
		TopK:             0,
		SnapshotInterval: 0,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	eng.template.ReconcileInterval = 60 * time.Millisecond
	eng.template.ReconcileJitter = 0
	eng.template.LeaseTTL = 400 * time.Millisecond
	eng.template.LeaseRenewInterval = 120 * time.Millisecond
	eng.template.HeartbeatInterval = 60 * time.Millisecond
	eng.template.HeartbeatTTL = 500 * time.Millisecond
	eng.template.MinConsumerRuntime = 0
	eng.template.SlotMoveCooldown = 800 * time.Millisecond
	eng.template.ConfigRefreshInterval = 200 * time.Millisecond
	eng.template.AcquireLimit = 200
	eng.template.SheddingRelease = 10

	if err := eng.template.Validate(); err != nil {
		t.Fatalf("template validate: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	redisAddr := eng.Snapshot().RedisAddr
	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = client.Close() })

	waitUntil(t, time.Second, func() bool {
		n, _ := client.Exists(context.Background(), "moved:demo:0").Result()
		return n > 0
	})

	time.Sleep(eng.template.SlotMoveCooldown + 150*time.Millisecond)
	n, err := client.Exists(context.Background(), "moved:demo:0").Result()
	if err != nil {
		t.Fatalf("exists moved marker: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected moved marker to expire in simulated mode, exists=%d", n)
	}

	waitUntil(t, 2*time.Second, func() bool {
		s := eng.Snapshot()
		return len(s.Nodes) == 2 && s.Metrics.MisalignedUnitsTotal == 0 && s.Metrics.OwnedUnitsTotal == s.Metrics.DesiredUnitsTotal
	})

	if err := eng.AddNodes(1, 1.0); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}

	waitUntil(t, 3*time.Second, func() bool {
		s := eng.Snapshot()
		return len(s.Nodes) == 3 && s.Metrics.MisalignedUnitsTotal == 0 && s.Metrics.OwnedUnitsTotal == s.Metrics.DesiredUnitsTotal
	})
}

func waitUntil(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout after %v", timeout)
}
