package workdist

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/you/workdist/coord"
	"github.com/you/workdist/hash"
)

// Coordinator orchestrates slot ownership and runner lifecycle.
type Coordinator struct {
	cfg          Config
	store        coord.Store
	parallelism  ParallelismProvider
	slotFactory  SlotFactory
	nodeIDs      NodeIDProvider
	logger       Logger
	metrics      Metrics
	nodeID       string
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.Mutex
	owned        map[string]*slotHandle
	cooldowns    map[string]time.Time
	releaseStart time.Time
	releases     int
}

type slotHandle struct {
	slot     Slot
	runner   SlotRunner
	cancel   context.CancelFunc
	done     chan struct{}
	started  time.Time
	lastMove time.Time
}

// NewCoordinator constructs a Coordinator with sane defaults.
func NewCoordinator(cfg Config, store coord.Store, provider ParallelismProvider, factory SlotFactory, nodeIDProvider NodeIDProvider, logger Logger, metrics Metrics) (*Coordinator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if store == nil || provider == nil || factory == nil || nodeIDProvider == nil {
		return nil, fmt.Errorf("store, provider, factory, and nodeIDProvider are required")
	}
	if logger == nil {
		logger = NopLogger()
	}
	if metrics == nil {
		metrics = NopMetrics()
	}
	return &Coordinator{
		cfg:          cfg,
		store:        store,
		parallelism:  provider,
		slotFactory:  factory,
		nodeIDs:      nodeIDProvider,
		logger:       logger,
		metrics:      metrics,
		owned:        map[string]*slotHandle{},
		cooldowns:    map[string]time.Time{},
		releaseStart: time.Now(),
	}, nil
}

// Run starts the coordinator loops and blocks until ctx is cancelled.
func (c *Coordinator) Run(ctx context.Context) error {
	nodeID, err := c.nodeIDs.NodeID()
	if err != nil {
		return fmt.Errorf("get node id: %w", err)
	}
	c.nodeID = nodeID
	c.ctx, c.cancel = context.WithCancel(ctx)

	// initial heartbeat so node shows up quickly.
	if err := c.store.HeartbeatNode(c.ctx, c.nodeID, c.cfg.NodeTTL); err != nil {
		return fmt.Errorf("initial heartbeat failed: %w", err)
	}

	c.wg.Add(3)
	go c.heartbeatLoop()
	go c.renewLoop()
	go c.reconcileLoop()

	<-c.ctx.Done()
	c.logger.Info("coordinator stopping", Field{Key: "node", Value: c.nodeID})
	c.stopAll("context cancelled")
	c.wg.Wait()
	return nil
}

// Stop cancels the coordinator context.
func (c *Coordinator) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
}

func (c *Coordinator) heartbeatLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.NodeHeartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.store.HeartbeatNode(c.ctx, c.nodeID, c.cfg.NodeTTL); err != nil {
				c.logger.Warn("node heartbeat failed", Field{Key: "err", Value: err})
			}
		}
	}
}

func (c *Coordinator) renewLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.LeaseRenewPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.renewOwnedLeases()
		}
	}
}

func (c *Coordinator) reconcileLoop() {
	defer c.wg.Done()
	if c.cfg.JoinWarmup > 0 {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(c.cfg.JoinWarmup):
		}
	}
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(jitter(c.cfg.ReconcilePeriod, c.cfg.JitterRatio)):
			c.reconcile()
		}
	}
}

func (c *Coordinator) reconcile() {
	ctx := c.ctx
	nodes, err := c.store.ListNodes(ctx)
	if err != nil {
		c.logger.Warn("list nodes failed", Field{Key: "err", Value: err})
		return
	}
	if !contains(nodes, c.nodeID) {
		nodes = append(nodes, c.nodeID)
	}
	workConfigs, err := c.parallelism.List(ctx)
	if err != nil {
		c.logger.Warn("list work classes failed", Field{Key: "err", Value: err})
		return
	}
	desired := c.computeDesired(nodes, workConfigs)
	c.applyDesired(desired)
}

func (c *Coordinator) computeDesired(nodes []string, configs []WorkClassConfig) map[string]Slot {
	desired := make(map[string]Slot)
	classCounts := make(map[string]int)
	for _, cfg := range configs {
		if cfg.Parallelism <= 0 {
			continue
		}
		for i := 0; i < cfg.Parallelism; i++ {
			slot := Slot{WorkClass: cfg.WorkClass, SlotIndex: i}
			owner, ok := hash.Owner(slot.Key(), nodes)
			if !ok || owner != c.nodeID {
				continue
			}
			if len(desired) >= c.cfg.MaxSlotsPerNode {
				return desired
			}
			if classCounts[cfg.WorkClass] >= c.cfg.MaxSlotsPerWorkClass {
				continue
			}
			classCounts[cfg.WorkClass]++
			desired[slot.Key()] = slot
		}
	}
	return desired
}

func (c *Coordinator) applyDesired(desired map[string]Slot) {
	c.mu.Lock()
	currentOwned := len(c.owned)
	c.mu.Unlock()
	if currentOwned >= c.cfg.MaxSlotsPerNode {
		c.logger.Debug("max slots per node reached", Field{Key: "count", Value: currentOwned})
	}

	acquired := 0
	for key, slot := range desired {
		if acquired >= c.cfg.MaxAcquirePerTick {
			break
		}
		if c.isOwned(key) {
			continue
		}
		if c.cooldownActive(key) {
			continue
		}
		ok, err := c.store.AcquireLease(c.ctx, leaseKey(key), c.nodeID, c.cfg.LeaseTTL)
		if err != nil {
			c.logger.Warn("acquire lease failed", Field{Key: "slot", Value: key}, Field{Key: "err", Value: err})
			continue
		}
		if !ok {
			continue
		}
		if err := c.startSlot(slot); err != nil {
			c.logger.Warn("start slot failed", Field{Key: "slot", Value: key}, Field{Key: "err", Value: err})
			_, _ = c.store.ReleaseLease(c.ctx, leaseKey(key), c.nodeID)
			continue
		}
		acquired++
	}

	if c.cfg.EnableGentleHandoff {
		c.releaseUndesired(desired)
	}
	c.metrics.SetGauge("owned_slots_total", float64(c.countOwned()))
	c.metrics.SetGauge("desired_slots_total", float64(len(desired)))
}

func (c *Coordinator) releaseUndesired(desired map[string]Slot) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if now.Sub(c.releaseStart) >= time.Minute {
		c.releaseStart = now
		c.releases = 0
	}

	for key, handle := range c.owned {
		if _, ok := desired[key]; ok {
			continue
		}
		if c.releases >= c.cfg.ReleasePerMinute {
			return
		}
		if time.Since(handle.started) < c.cfg.MinSlotRuntime {
			continue
		}
		if c.cfg.EnableCooldowns && time.Since(handle.lastMove) < c.cfg.SlotMoveCooldown {
			continue
		}
		c.releases++
		go c.releaseSlot(key, handle, "gentle-handoff")
	}
}

func (c *Coordinator) startSlot(slot Slot) error {
	runner, err := c.slotFactory.NewSlotRunner(slot)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(c.ctx)
	handle := &slotHandle{
		slot:     slot,
		runner:   runner,
		cancel:   cancel,
		done:     make(chan struct{}),
		started:  time.Now(),
		lastMove: time.Now(),
	}

	c.mu.Lock()
	c.owned[slot.Key()] = handle
	c.mu.Unlock()

	c.metrics.IncCounter("runner_starts", 1, Label{Name: "work_class", Value: slot.WorkClass})
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		err := runner.Run(runCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Warn("slot runner exited with error", Field{Key: "slot", Value: slot.Key()}, Field{Key: "err", Value: err})
		}
		close(handle.done)
	}()
	return nil
}

func (c *Coordinator) renewOwnedLeases() {
	c.mu.Lock()
	handles := make(map[string]*slotHandle, len(c.owned))
	for k, h := range c.owned {
		handles[k] = h
	}
	c.mu.Unlock()
	for key, handle := range handles {
		select {
		case <-handle.done:
			c.logger.Warn("slot runner stopped unexpectedly", Field{Key: "slot", Value: key})
			c.releaseSlot(key, handle, "runner-exit")
			continue
		default:
		}
		ok, err := c.store.RenewLease(c.ctx, leaseKey(key), c.nodeID, c.cfg.LeaseTTL)
		if err != nil {
			c.logger.Warn("renew lease failed", Field{Key: "slot", Value: key}, Field{Key: "err", Value: err})
			continue
		}
		if !ok {
			c.logger.Warn("lease lost during renew", Field{Key: "slot", Value: key})
			c.stopSlot(key, handle, false, "lease-lost")
		}
	}
}

func (c *Coordinator) releaseSlot(key string, handle *slotHandle, reason string) {
	if released, err := c.store.ReleaseLease(c.ctx, leaseKey(key), c.nodeID); err != nil {
		c.logger.Warn("release lease failed", Field{Key: "slot", Value: key}, Field{Key: "err", Value: err})
	} else if !released {
		c.logger.Debug("lease release skipped (not owner)", Field{Key: "slot", Value: key})
	}
	c.stopSlot(key, handle, true, reason)
}

func (c *Coordinator) stopSlot(key string, handle *slotHandle, applyCooldown bool, reason string) {
	handle.cancel()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = handle.runner.Stop(stopCtx)
	select {
	case <-handle.done:
	case <-time.After(5 * time.Second):
	}

	c.mu.Lock()
	delete(c.owned, key)
	if applyCooldown {
		c.cooldowns[key] = time.Now()
	}
	c.mu.Unlock()

	c.metrics.IncCounter("runner_stops", 1, Label{Name: "reason", Value: reason})
}

func (c *Coordinator) stopAll(reason string) {
	c.mu.Lock()
	handles := make(map[string]*slotHandle, len(c.owned))
	for k, h := range c.owned {
		handles[k] = h
	}
	c.mu.Unlock()
	for key, handle := range handles {
		c.releaseSlot(key, handle, reason)
	}
}

func (c *Coordinator) isOwned(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.owned[key]
	return ok
}

func (c *Coordinator) countOwned() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.owned)
}

func (c *Coordinator) cooldownActive(key string) bool {
	if !c.cfg.EnableCooldowns {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ts, ok := c.cooldowns[key]; ok {
		return time.Since(ts) < c.cfg.SlotMoveCooldown
	}
	return false
}

func jitter(base time.Duration, ratio float64) time.Duration {
	if ratio <= 0 {
		return base
	}
	delta := int64(float64(base) * ratio)
	if delta == 0 {
		return base
	}
	// add or subtract up to delta.
	offset := rand.Int63n(2*delta+1) - delta
	return time.Duration(int64(base) + offset)
}

func leaseKey(slotKey string) string {
	return "lease:" + slotKey
}

func contains(list []string, id string) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}
	return false
}
