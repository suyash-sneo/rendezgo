package rendez

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/suyash-sneo/rendez/internal/redis_scripts"
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
	cfgA := fastConfig(slot.Topic, 1)
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
	topic := topicForOwner("test:b", 2, weights)

	cfgA := fastConfig(topic, 2)
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
	waitForOwner(t, client, leaseKey(Slot{Topic: topic, Index: 0}), "test:a", time.Second)

	ctrlB, _ := NewController(cfgB, client, factory, NopLogger(), NopMetrics())
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(cancelB)
	go ctrlB.Start(ctxB)

	time.Sleep(cfgA.MinConsumerRuntime + cfgA.ReconcileInterval + 40*time.Millisecond)
	owners := slotOwners(client, topic, 2)
	if owners[fmt.Sprintf("lease:%s:%d", topic, 0)] != "test:b" && owners[fmt.Sprintf("lease:%s:%d", topic, 1)] != "test:b" {
		t.Fatalf("expected at least one slot moved to b after first shed, owners=%v", owners)
	}
	if owners[fmt.Sprintf("lease:%s:%d", topic, 0)] == "test:b" && owners[fmt.Sprintf("lease:%s:%d", topic, 1)] == "test:b" {
		t.Fatalf("shedding release rate should not move both slots at once")
	}

	time.Sleep(cfgA.ReconcileInterval + 80*time.Millisecond)
	owners = slotOwners(client, topic, 2)
	if owners[fmt.Sprintf("lease:%s:%d", topic, 0)] != "test:b" || owners[fmt.Sprintf("lease:%s:%d", topic, 1)] != "test:b" {
		t.Fatalf("expected both slots with b after second pass, owners=%v", owners)
	}
}

func TestCapsRespected(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	factory := newStubFactory()

	cfg := fastConfig("caps", 3)
	cfg.NodeID = "solo"
	cfg.MaxConsumersPerPod = 1
	cfg.MaxConsumersPerTopicPerPod = 1
	ctrl, _ := NewController(cfg, client, factory, NopLogger(), NopMetrics())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go ctrl.Start(ctx)

	time.Sleep(400 * time.Millisecond)
	owners := slotOwners(client, "caps", 3)
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
	owners := slotOwners(client, "backoff", 1)
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
		owners := slotOwners(client, "int", 4)
		if len(owners) != 4 {
			return false
		}
		weights := []NodeWeight{{ID: "test:n1", Weight: 1}, {ID: "test:n2", Weight: 1}, {ID: "test:n3", Weight: 1}}
		for i := 0; i < 4; i++ {
			slot := Slot{Topic: "int", Index: i}
			expected, _ := RendezvousOwner(slot, weights)
			if owners[leaseKey(slot)] != expected {
				return false
			}
		}
		return true
	})
}

func TestScriptsRespectOwnership(t *testing.T) {
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	script := newRedisScript(redis_scripts.NewScript(redis_scripts.Renew))
	ctx := context.Background()
	slot := Slot{Topic: "s", Index: 0}
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

func fastConfig(topic string, parts int) Config {
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
	cfg.StaticTopics = []TopicConfig{{Name: topic, Partitions: parts}}
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

func slotOwners(client *redis.Client, topic string, parts int) map[string]string {
	owners := make(map[string]string)
	for i := 0; i < parts; i++ {
		key := fmt.Sprintf("lease:%s:%d", topic, i)
		val, err := client.Get(context.Background(), key).Result()
		if err == nil {
			owners[key] = val
		}
	}
	return owners
}

func pickSlot(a, b string) Slot {
	nodes := []NodeWeight{{ID: "test:" + a, Weight: 0.5}, {ID: "test:" + b, Weight: 5}}
	for i := 0; i < 50; i++ {
		slot := Slot{Topic: fmt.Sprintf("topic-%d", i), Index: 0}
		if owner, _ := RendezvousOwner(slot, nodes); owner == "test:"+b {
			return slot
		}
	}
	return Slot{Topic: "fallback", Index: 0}
}

func topicForOwner(owner string, parts int, weights map[string]float64) string {
	nodes := make([]NodeWeight, 0, len(weights))
	for id, w := range weights {
		nodes = append(nodes, NodeWeight{ID: id, Weight: w})
	}
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("shed-%d", i)
		ok := true
		for p := 0; p < parts; p++ {
			if o, _ := RendezvousOwner(Slot{Topic: name, Index: p}, nodes); o != owner {
				ok = false
				break
			}
		}
		if ok {
			return name
		}
	}
	return fmt.Sprintf("shed-%s", owner)
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
