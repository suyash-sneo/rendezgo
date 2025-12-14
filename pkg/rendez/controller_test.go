package rendez

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/suyash-sneo/rendezgo/internal/redis_scripts"
)

func TestNoStealOnHealthyLease(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	factory := newStubFactory()

	cfgA := fastConfig("nosteal", 1)
	cfgA.NodeID = "a"
	ctrlA, err := NewController(cfgA, client, factory, NopLogger(), NopMetrics())
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	t.Cleanup(cancelA)
	go ctrlA.Start(ctxA)
	waitForOwner(t, client, "lease:nosteal:0", "test:a", 2*time.Second)

	cfgB := cfgA
	cfgB.NodeID = "b"
	ctrlB, err := NewController(cfgB, client, factory, NopLogger(), NopMetrics())
	if err != nil {
		t.Fatalf("new controller b: %v", err)
	}
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(cancelB)
	go ctrlB.Start(ctxB)

	time.Sleep(400 * time.Millisecond)
	got := client.Get(context.Background(), "lease:nosteal:0").Val()
	if got != "test:a" {
		t.Fatalf("expected owner test:a, got %s", got)
	}
}

func TestCooldownFastRecovery(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	factory := newStubFactory()

	slot := pickSlot("a", "b")
	if owner, _ := RendezvousOwner(slot, []NodeWeight{{ID: "test:a", Weight: 0.5}, {ID: "test:b", Weight: 5}}); owner != "test:b" {
		t.Fatalf("expected slot to map to b, got %s", owner)
	}
	cfgA := fastConfig(slot.Workload, 1)
	cfgA.NodeID = "a"
	cfgA.SlotMoveCooldown = 2 * time.Second
	cfgB := cfgA
	cfgB.NodeID = "b"
	cfgB.StaticWeights = map[string]float64{
		"test:a": 0.5,
		"test:b": 5,
	}

	ctrlA, _ := NewController(cfgA, client, factory, NopLogger(), NopMetrics())
	ctxA, cancelA := context.WithCancel(context.Background())
	t.Cleanup(cancelA)
	go ctrlA.Start(ctxA)
	waitForOwner(t, client, leaseKey(slot), "test:a", time.Second)

	ctrlB, _ := NewController(cfgB, client, factory, NopLogger(), NopMetrics())
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(cancelB)
	go ctrlB.Start(ctxB)

	time.Sleep(400 * time.Millisecond)
	if owner := client.Get(context.Background(), leaseKey(slot)).Val(); owner != "test:a" {
		t.Fatalf("lease should remain with a during cooldown, got %s", owner)
	}

	cancelA()
	time.Sleep(cfgA.LeaseTTL + 200*time.Millisecond)
	if owner := client.Get(context.Background(), leaseKey(slot)).Val(); owner != "test:b" {
		t.Fatalf("expected b to recover after expiry, got %s", owner)
	}
}

func TestSheddingRateAndRuntime(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	factory := newStubFactory()

	weights := map[string]float64{
		"test:b": 5,
		"test:a": 0.1,
	}
	workload := workloadForOwner(t, "test:b", 2, weights)

	cfgA := fastConfig(workload, 2)
	cfgA.NodeID = "a"
	cfgA.StaticWeights = weights
	cfgA.SheddingRelease = 1
	cfgA.MinConsumerRuntime = 40 * time.Millisecond
	cfgA.SlotMoveCooldown = 0
	cfgB := cfgA
	cfgB.NodeID = "b"
	cfgB.StaticWeights = weights

	ctrlA, _ := NewController(cfgA, client, factory, NopLogger(), NopMetrics())
	ctxA, cancelA := context.WithCancel(context.Background())
	t.Cleanup(cancelA)
	go ctrlA.Start(ctxA)
	waitForOwner(t, client, leaseKey(Slot{Workload: workload, Unit: 0}), "test:a", time.Second)

	ctrlB, _ := NewController(cfgB, client, factory, NopLogger(), NopMetrics())
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(cancelB)
	go ctrlB.Start(ctxB)

	waitUntil(t, time.Second, func() bool {
		owners := workloadOwners(client, workload, 2)
		count := 0
		for _, v := range owners {
			if v == "test:b" {
				count++
			}
		}
		return count >= 1
	})
	owners := workloadOwners(client, workload, 2)
	countB := 0
	for _, v := range owners {
		if v == "test:b" {
			countB++
		}
	}
	if countB == 0 {
		t.Fatalf("expected at least one slot moved to b after first shed, owners=%v", owners)
	}

	waitUntil(t, 2*time.Second, func() bool {
		owners := workloadOwners(client, workload, 2)
		return owners[fmt.Sprintf("lease:%s:%d", workload, 0)] == "test:b" && owners[fmt.Sprintf("lease:%s:%d", workload, 1)] == "test:b"
	})
}

func TestCapsRespected(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	factory := newStubFactory()

	cfg := fastConfig("caps", 3)
	cfg.NodeID = "solo"
	cfg.MaxConsumersPerNode = 1
	cfg.MaxConsumersPerWorkloadPerNode = 1
	ctrl, _ := NewController(cfg, client, factory, NopLogger(), NopMetrics())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go ctrl.Start(ctx)

	time.Sleep(400 * time.Millisecond)
	owners := workloadOwners(client, "caps", 3)
	count := 0
	for _, v := range owners {
		if v == "test:solo" {
			count++
		}
	}
	if count > 1 {
		t.Fatalf("expected at most one lease due to caps, got %d", count)
	}
}

func TestBackoffSkipsAcquisition(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	factory := newStubFactory()
	cfg := fastConfig("backoff", 1)
	cfg.NodeID = "node"
	cfg.RedisBackoff = time.Minute

	ctrl, _ := NewController(cfg, client, factory, NopLogger(), NopMetrics())
	ctrl.backoffTo.Store(time.Now().Add(time.Minute).UnixNano())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go ctrl.Start(ctx)

	time.Sleep(300 * time.Millisecond)
	owners := workloadOwners(client, "backoff", 1)
	if owners["lease:backoff:0"] != "" {
		t.Fatalf("expected no acquisitions during backoff, got %v", owners)
	}
}

func TestIntegrationConvergence(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	factory := newStubFactory()

	cfg := fastConfig("int", 4)
	cfg.MinConsumerRuntime = 20 * time.Millisecond
	cfg.SheddingRelease = 3
	cfg.SlotMoveCooldown = 0

	cfg1 := cfg
	cfg1.NodeID = "n1"
	ctrl1, _ := NewController(cfg1, client, factory, NopLogger(), NopMetrics())
	ctx1, cancel1 := context.WithCancel(context.Background())
	t.Cleanup(cancel1)
	go ctrl1.Start(ctx1)

	time.Sleep(120 * time.Millisecond)

	cfg2 := cfg
	cfg2.NodeID = "n2"
	ctrl2, _ := NewController(cfg2, client, factory, NopLogger(), NopMetrics())
	ctx2, cancel2 := context.WithCancel(context.Background())
	t.Cleanup(cancel2)
	go ctrl2.Start(ctx2)

	time.Sleep(120 * time.Millisecond)

	cfg3 := cfg
	cfg3.NodeID = "n3"
	ctrl3, _ := NewController(cfg3, client, factory, NopLogger(), NopMetrics())
	ctx3, cancel3 := context.WithCancel(context.Background())
	t.Cleanup(cancel3)
	go ctrl3.Start(ctx3)

	waitUntil(t, 2*time.Second, func() bool {
		owners := workloadOwners(client, "int", 4)
		if len(owners) != 4 {
			return false
		}
		weights := []NodeWeight{{ID: "test:n1", Weight: 1}, {ID: "test:n2", Weight: 1}, {ID: "test:n3", Weight: 1}}
		for i := 0; i < 4; i++ {
			slot := Slot{Workload: "int", Unit: i}
			expected, _ := RendezvousOwner(slot, weights)
			if owners[leaseKey(slot)] != expected {
				return false
			}
		}
		return true
	})
}

func TestMultiWorkloadConvergence(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	factory := newStubFactory()

	cfg := fastConfig("alpha", 1)
	cfg.MinConsumerRuntime = 20 * time.Millisecond
	cfg.SheddingRelease = 3
	cfg.SlotMoveCooldown = 0
	cfg.AcquireLimit = 100
	cfg.StaticWorkloads = []WorkloadConfig{
		{Name: "alpha", Units: 2},
		{Name: "beta", Units: 3},
		{Name: "gamma", Units: 1},
	}

	cfg1 := cfg
	cfg1.NodeID = "n1"
	ctrl1, _ := NewController(cfg1, client, factory, NopLogger(), NopMetrics())
	ctx1, cancel1 := context.WithCancel(context.Background())
	t.Cleanup(cancel1)
	go ctrl1.Start(ctx1)

	time.Sleep(80 * time.Millisecond)

	cfg2 := cfg
	cfg2.NodeID = "n2"
	ctrl2, _ := NewController(cfg2, client, factory, NopLogger(), NopMetrics())
	ctx2, cancel2 := context.WithCancel(context.Background())
	t.Cleanup(cancel2)
	go ctrl2.Start(ctx2)

	time.Sleep(80 * time.Millisecond)

	cfg3 := cfg
	cfg3.NodeID = "n3"
	ctrl3, _ := NewController(cfg3, client, factory, NopLogger(), NopMetrics())
	ctx3, cancel3 := context.WithCancel(context.Background())
	t.Cleanup(cancel3)
	go ctrl3.Start(ctx3)

	weights := []NodeWeight{{ID: "test:n1", Weight: 1}, {ID: "test:n2", Weight: 1}, {ID: "test:n3", Weight: 1}}
	total := 0
	for _, wl := range cfg.StaticWorkloads {
		total += wl.Units
	}
	waitUntil(t, 3*time.Second, func() bool {
		owners := allOwners(client, cfg.StaticWorkloads)
		if len(owners) != total {
			return false
		}
		for _, wl := range cfg.StaticWorkloads {
			for unit := 0; unit < wl.Units; unit++ {
				slot := Slot{Workload: wl.Name, Unit: unit}
				expected, _ := RendezvousOwner(slot, weights)
				if owners[leaseKey(slot)] != expected {
					return false
				}
			}
		}
		return true
	})
}

func TestDeterministicAcquisitionUnderCaps(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	factory := newStubFactory()

	cfg := fastConfig("capdet", 1)
	cfg.NodeID = "solo"
	cfg.StaticWorkloads = []WorkloadConfig{
		{Name: "red", Units: 3},
		{Name: "blue", Units: 3},
		{Name: "green", Units: 2},
	}
	cfg.MaxConsumersPerNode = 2
	cfg.MaxConsumersPerWorkloadPerNode = 1
	cfg.AcquireLimit = 2
	cfg.SlotMoveCooldown = 0
	cfg.MinConsumerRuntime = 0
	cfg.ReconcileInterval = 60 * time.Millisecond
	cfg.SheddingEnabled = false

	ctrl, _ := NewController(cfg, client, factory, NopLogger(), NopMetrics())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go ctrl.Start(ctx)

	weights := []NodeWeight{{ID: "test:solo", Weight: 1}}
	expected := selectExpectedSlots(cfg.StaticWorkloads, weights, "test:solo", cfg.MaxConsumersPerNode, cfg.MaxConsumersPerWorkloadPerNode, cfg.AcquireLimit)

	waitUntil(t, 2*time.Second, func() bool {
		owners := allOwners(client, cfg.StaticWorkloads)
		return len(owners) == len(expected)
	})
	initial := allOwners(client, cfg.StaticWorkloads)
	if !reflect.DeepEqual(initial, expected) {
		t.Fatalf("unexpected leases acquired: got=%v want=%v", initial, expected)
	}

	time.Sleep(3 * cfg.ReconcileInterval)
	after := allOwners(client, cfg.StaticWorkloads)
	if !reflect.DeepEqual(initial, after) {
		t.Fatalf("leases should remain stable under caps: first=%v later=%v", initial, after)
	}
}

func TestScriptsRespectOwnership(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	script := newRedisScript(redis_scripts.NewScript(redis_scripts.Renew))
	ctx := context.Background()
	slot := Slot{Workload: "s", Unit: 0}
	if err := client.Set(ctx, leaseKey(slot), "owner", 50*time.Millisecond).Err(); err != nil {
		t.Fatalf("set err: %v", err)
	}
	if _, err := script.run(ctx, client, []string{leaseKey(slot)}, "owner", int64(500)); err != nil {
		t.Fatalf("run err: %v", err)
	}
	ttl, _ := client.PTTL(ctx, leaseKey(slot)).Result()
	if ttl < 300*time.Millisecond {
		t.Fatalf("expected ttl extended, got %v", ttl)
	}
	if n, err := script.run(ctx, client, []string{leaseKey(slot)}, "other", int64(200)); err != nil || n != 0 {
		t.Fatalf("expected failed renew for other owner, n=%d err=%v", n, err)
	}
}

// Helpers

func fastConfig(workload string, units int) Config {
	cfg := DefaultConfig()
	cfg.ClusterID = "test"
	cfg.HeartbeatTTL = 2 * time.Second
	cfg.HeartbeatInterval = 100 * time.Millisecond
	cfg.LeaseTTL = 1200 * time.Millisecond
	cfg.LeaseRenewInterval = 200 * time.Millisecond
	cfg.ReconcileInterval = 80 * time.Millisecond
	cfg.ReconcileJitter = 0
	cfg.ConfigRefreshInterval = 500 * time.Millisecond
	cfg.SlotMoveCooldown = 500 * time.Millisecond
	cfg.MinConsumerRuntime = 80 * time.Millisecond
	cfg.AcquireLimit = 10
	cfg.StaticWorkloads = []WorkloadConfig{{Name: workload, Units: units}}
	return cfg
}

type stubFactory struct {
	mu      sync.Mutex
	started map[string]int
}

func newStubFactory() *stubFactory {
	return &stubFactory{started: map[string]int{}}
}

func (f *stubFactory) NewConsumer(slot Slot) (Consumer, error) {
	f.mu.Lock()
	f.started[slot.Key()]++
	f.mu.Unlock()
	return &stubConsumer{done: make(chan struct{})}, nil
}

type stubConsumer struct {
	done chan struct{}
}

func (c *stubConsumer) Run(ctx context.Context) error {
	defer close(c.done)
	<-ctx.Done()
	return ctx.Err()
}

func (c *stubConsumer) Stop(ctx context.Context) error {
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
		return nil
	}
}

func waitForOwner(t *testing.T, client *redis.Client, key, expected string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for %s to be %s", key, expected)
		case <-tick.C:
			val, err := client.Get(context.Background(), key).Result()
			if err == nil && val == expected {
				return
			}
		}
	}
}

func workloadOwners(client *redis.Client, workload string, units int) map[string]string {
	owners := make(map[string]string)
	for i := 0; i < units; i++ {
		key := fmt.Sprintf("lease:%s:%d", workload, i)
		val, err := client.Get(context.Background(), key).Result()
		if err == nil {
			owners[key] = val
		}
	}
	return owners
}

func allOwners(client *redis.Client, workloads []WorkloadConfig) map[string]string {
	out := make(map[string]string)
	for _, wl := range workloads {
		for k, v := range workloadOwners(client, wl.Name, wl.Units) {
			out[k] = v
		}
	}
	return out
}

func pickSlot(a, b string) Slot {
	nodes := []NodeWeight{{ID: "test:" + a, Weight: 0.5}, {ID: "test:" + b, Weight: 5}}
	for i := 0; i < 50; i++ {
		slot := Slot{Workload: fmt.Sprintf("work-%d", i), Unit: 0}
		if owner, _ := RendezvousOwner(slot, nodes); owner == "test:"+b {
			return slot
		}
	}
	return Slot{Workload: "fallback", Unit: 0}
}

func workloadForOwner(t *testing.T, owner string, units int, weights map[string]float64) string {
	t.Helper()
	nodes := make([]NodeWeight, 0, len(weights))
	for id, w := range weights {
		nodes = append(nodes, NodeWeight{ID: id, Weight: w})
	}
	for i := 0; i < 500; i++ {
		name := fmt.Sprintf("wl-%d", i)
		ok := true
		for p := 0; p < units; p++ {
			if o, _ := RendezvousOwner(Slot{Workload: name, Unit: p}, nodes); o != owner {
				ok = false
				break
			}
		}
		if ok {
			return name
		}
	}
	t.Fatalf("unable to find workload mapping all units to %s", owner)
	return ""
}

func selectExpectedSlots(workloads []WorkloadConfig, weights []NodeWeight, nodeID string, maxPerNode, maxPerWorkload, acquireLimit int) map[string]string {
	type candidate struct {
		slot  Slot
		score float64
	}
	workloadCaps := make(map[string]int, len(workloads))
	for _, wl := range workloads {
		workloadCaps[wl.Name] = wl.MaxConsumersPerNode
	}
	weightByID := make(map[string]float64, len(weights))
	for _, w := range weights {
		weightByID[w.ID] = w.Weight
	}
	selfWeight := weightByID[nodeID]
	if selfWeight == 0 {
		selfWeight = 1
	}
	names := make([]string, 0, len(workloads))
	for _, wl := range workloads {
		names = append(names, wl.Name)
	}
	sort.Strings(names)

	var candidates []candidate
	for _, name := range names {
		units := 0
		for _, wl := range workloads {
			if wl.Name == name {
				units = wl.Units
				break
			}
		}
		for unit := 0; unit < units; unit++ {
			slot := Slot{Workload: name, Unit: unit}
			owner, ok := RendezvousOwner(slot, weights)
			if !ok || owner != nodeID {
				continue
			}
			candidates = append(candidates, candidate{
				slot:  slot,
				score: weightedScore(slot.Key(), nodeID, selfWeight),
			})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			if candidates[i].slot.Workload == candidates[j].slot.Workload {
				return candidates[i].slot.Unit < candidates[j].slot.Unit
			}
			return candidates[i].slot.Workload < candidates[j].slot.Workload
		}
		return candidates[i].score > candidates[j].score
	})

	results := make(map[string]string)
	perWorkload := make(map[string]int)
	for _, cand := range candidates {
		if maxPerNode > 0 && len(results) >= maxPerNode {
			break
		}
		if acquireLimit > 0 && len(results) >= acquireLimit {
			break
		}
		capForWorkload := maxPerWorkload
		if wlCap := workloadCaps[cand.slot.Workload]; wlCap > 0 && (capForWorkload == 0 || wlCap < capForWorkload) {
			capForWorkload = wlCap
		}
		if capForWorkload > 0 && perWorkload[cand.slot.Workload] >= capForWorkload {
			continue
		}
		results[leaseKey(cand.slot)] = nodeID
		perWorkload[cand.slot.Workload]++
	}
	return results
}

func waitUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}
