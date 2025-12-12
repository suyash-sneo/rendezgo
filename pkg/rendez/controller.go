package rendez

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/suyash-sneo/rendez/internal/redis_scripts"
)

type ControllerOption func(*Controller)

// WithHealthChecker injects a health gate.
func WithHealthChecker(h HealthChecker) ControllerOption {
	return func(c *Controller) { c.health = h }
}

// WithWeightProvider injects a dynamic weight source.
func WithWeightProvider(w WeightProvider) ControllerOption {
	return func(c *Controller) { c.weights = w }
}

// WithKafkaChecker injects a Kafka oversubscription gate.
func WithKafkaChecker(k KafkaMembershipChecker) ControllerOption {
	return func(c *Controller) { c.kafka = k }
}

// WithNodeIDProvider overrides the default node ID provider.
func WithNodeIDProvider(p NodeIDProvider) ControllerOption {
	return func(c *Controller) { c.nodeIDs = p }
}

// WithNow sets a custom clock (tests).
func WithNow(now func() time.Time) ControllerOption {
	return func(c *Controller) { c.now = now }
}

// Controller owns the loops that maintain leases and consumers.
type Controller struct {
	cfg     Config
	client  RedisClient
	factory ConsumerFactory
	logger  Logger
	metrics Metrics

	health    HealthChecker
	weights   WeightProvider
	kafka     KafkaMembershipChecker
	nodeIDs   NodeIDProvider
	now       func() time.Time
	churnMu   sync.Mutex
	churn     []time.Time
	backoffTo atomic.Int64

	nodeID string

	renewScript   redisScript
	releaseScript redisScript

	mu      sync.RWMutex
	owned   map[string]*consumerHandle
	topics  map[string]TopicConfig
	ctx     context.Context
	cancel  context.CancelFunc
	started time.Time
}

type consumerHandle struct {
	slot      Slot
	consumer  Consumer
	cancel    context.CancelFunc
	done      chan struct{}
	startedAt time.Time
}

// NewController constructs a controller.
func NewController(cfg Config, redisClient RedisClient, consumerFactory ConsumerFactory, logger Logger, metrics Metrics, opts ...ControllerOption) (*Controller, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if redisClient == nil {
		return nil, fmt.Errorf("redis client required")
	}
	if consumerFactory == nil {
		return nil, fmt.Errorf("consumer factory required")
	}
	if logger == nil {
		logger = NopLogger()
	}
	if metrics == nil {
		metrics = NopMetrics()
	}

	c := &Controller{
		cfg:           cfg,
		client:        redisClient,
		factory:       consumerFactory,
		logger:        logger,
		metrics:       metrics,
		nodeIDs:       NewDefaultNodeIDProvider(),
		now:           time.Now,
		renewScript:   newRedisScript(redis_scripts.NewScript(redis_scripts.Renew)),
		releaseScript: newRedisScript(redis_scripts.NewScript(redis_scripts.Release)),
		owned:         map[string]*consumerHandle{},
		topics:        map[string]TopicConfig{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Start runs all loops until ctx is cancelled.
func (c *Controller) Start(ctx context.Context) error {
	nodeID, err := c.resolveNodeID()
	if err != nil {
		return err
	}
	c.nodeID = nodeID
	c.started = c.now()

	ctx, cancel := context.WithCancel(ctx)
	c.ctx = ctx
	c.cancel = cancel

	if err := c.heartbeat(ctx); err != nil {
		c.logger.Warn("initial heartbeat failed", Field{Key: "err", Value: err})
	}

	if err := c.refreshConfigs(ctx); err != nil {
		c.logger.Warn("initial config load failed", Field{Key: "err", Value: err})
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { c.heartbeatLoop(ctx); return nil })
	g.Go(func() error { c.configLoop(ctx); return nil })
	g.Go(func() error { c.reconcileLoop(ctx); return nil })
	g.Go(func() error { c.renewLoop(ctx); return nil })

	err = g.Wait()
	c.shutdown()
	return err
}

// OwnedSlots returns a snapshot of slots owned by this controller.
func (c *Controller) OwnedSlots() map[string][]Slot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string][]Slot, len(c.owned))
	for _, h := range c.owned {
		out[h.slot.Topic] = append(out[h.slot.Topic], h.slot)
	}
	return out
}

// NodeID returns the resolved node id (cluster-prefixed).
func (c *Controller) NodeID() string {
	return c.nodeID
}

// BackoffActive reports whether redis backoff is active.
func (c *Controller) BackoffActive() bool {
	return c.inBackoff()
}

// SetReleaseOnShutdown toggles whether leases are released during shutdown (useful for simulations).
func (c *Controller) SetReleaseOnShutdown(release bool) {
	c.cfg.ReleaseOnShutdown = release
}

func (c *Controller) resolveNodeID() (string, error) {
	if c.cfg.NodeID != "" {
		if strings.HasPrefix(c.cfg.NodeID, c.cfg.ClusterID+":") {
			return c.cfg.NodeID, nil
		}
		return fmt.Sprintf("%s:%s", c.cfg.ClusterID, c.cfg.NodeID), nil
	}
	if c.nodeIDs == nil {
		return "", fmt.Errorf("node id provider nil")
	}
	id, err := c.nodeIDs.NodeID()
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(id, c.cfg.ClusterID+":") {
		return id, nil
	}
	return fmt.Sprintf("%s:%s", c.cfg.ClusterID, id), nil
}

func (c *Controller) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.heartbeat(ctx); err != nil {
				c.logger.Warn("heartbeat failed", Field{Key: "err", Value: err})
				c.markBackoff(err)
			}
		}
	}
}

func (c *Controller) heartbeat(ctx context.Context) error {
	value := fmt.Sprintf("%s|%s", c.cfg.ClusterID, c.nodeID)
	if err := c.client.Set(ctx, nodeKey(c.nodeID), value, c.cfg.HeartbeatTTL).Err(); err != nil {
		return err
	}
	if err := c.client.SAdd(ctx, "nodes:all", c.nodeID).Err(); err != nil {
		return err
	}
	_ = c.client.SAdd(ctx, "nodes:"+c.cfg.ClusterID, c.nodeID).Err()
	return nil
}

func (c *Controller) configLoop(ctx context.Context) {
	updates := make(chan string, 16)
	if c.cfg.ConfigWatchChannel != "" {
		sub := c.client.Subscribe(ctx, c.cfg.ConfigWatchChannel)
		go func() {
			defer close(updates)
			for {
				msg, err := sub.ReceiveMessage(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					c.logger.Warn("config watch receive failed", Field{Key: "err", Value: err})
					return
				}
				updates <- msg.Payload
			}
		}()
	}
	t := time.NewTicker(c.cfg.ConfigRefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.refreshConfigs(ctx); err != nil {
				c.logger.Warn("config refresh failed", Field{Key: "err", Value: err})
			}
		case topic, ok := <-updates:
			if !ok {
				updates = nil
				continue
			}
			if err := c.refreshTopic(ctx, topic); err != nil {
				c.logger.Warn("config refresh topic failed", Field{Key: "topic", Value: topic}, Field{Key: "err", Value: err})
			}
		}
	}
}

func (c *Controller) reconcileLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter(c.cfg.ReconcileInterval, c.cfg.ReconcileJitter)):
			c.reconcile(ctx)
		}
	}
}

func (c *Controller) reconcile(ctx context.Context) {
	if c.inBackoff() {
		c.logger.Debug("redis backoff active, skipping reconcile")
		return
	}
	live, err := c.liveNodes(ctx)
	if err != nil {
		c.logger.Warn("list live nodes failed", Field{Key: "err", Value: err})
		c.markBackoff(err)
		return
	}

	topics := c.topicSnapshot()
	weights := c.weightsSnapshot(live)
	desired := make(map[string]Slot)
	for _, cfg := range topics {
		for i := 0; i < cfg.Partitions; i++ {
			slot := Slot{Topic: cfg.Name, Index: i}
			owner, ok := RendezvousOwner(slot, weights)
			if !ok || owner != c.nodeID {
				continue
			}
			desired[slot.Key()] = slot
		}
	}

	owned := c.ownedSnapshot()
	podCount := len(owned)
	topicCounts := make(map[string]int)
	for _, h := range owned {
		topicCounts[h.slot.Topic]++
	}

	topicCap := func(topic string) int {
		capValue := c.cfg.MaxConsumersPerTopicPerPod
		if tc, ok := topics[topic]; ok && tc.MaxConsumersPerPod > 0 {
			if capValue == 0 || tc.MaxConsumersPerPod < capValue {
				capValue = tc.MaxConsumersPerPod
			}
		}
		return capValue
	}

	acquired := 0
	for key, slot := range desired {
		if _, ok := owned[key]; ok {
			continue
		}
		if c.cfg.AcquireLimit > 0 && acquired >= c.cfg.AcquireLimit {
			break
		}
		if c.cfg.MaxConsumersPerPod > 0 && podCount >= c.cfg.MaxConsumersPerPod {
			break
		}
		capForTopic := topicCap(slot.Topic)
		if capForTopic > 0 && topicCounts[slot.Topic] >= capForTopic {
			continue
		}
		if c.cfg.Kafka.Enabled && c.kafka != nil && c.kafka.OverSubscribed(ctx, slot.Topic, topics[slot.Topic].Partitions) {
			continue
		}
		owner, ok, err := c.getLeaseOwner(ctx, slot)
		if err != nil {
			c.logger.Warn("get lease owner failed", Field{Key: "slot", Value: slot.Key()}, Field{Key: "err", Value: err})
			c.markBackoff(err)
			continue
		}
		if ok {
			if owner == c.nodeID {
				if _, exists := owned[key]; !exists {
					if err := c.startConsumer(slot); err != nil {
						c.logger.Warn("start consumer for held lease failed", Field{Key: "slot", Value: slot.Key()}, Field{Key: "err", Value: err})
					}
				}
				continue
			}
			if c.cooldownActive(ctx, slot) && live[owner] {
				continue
			}
			continue
		}
		ok, err = c.acquire(ctx, slot)
		if err != nil {
			c.logger.Warn("acquire failed", Field{Key: "slot", Value: slot.Key()}, Field{Key: "err", Value: err})
			c.markBackoff(err)
			continue
		}
		if ok {
			acquired++
			podCount++
			topicCounts[slot.Topic]++
		}
	}

	if c.cfg.GentleHandoff && c.cfg.SheddingEnabled {
		c.shed(ctx, desired)
	}
	c.metrics.SetGauge("rendez_owned_slots", float64(len(c.ownedSnapshot())))
}

func (c *Controller) shed(ctx context.Context, desired map[string]Slot) {
	if c.inBackoff() {
		return
	}
	if c.health != nil && !c.health.Healthy(ctx) {
		return
	}
	owned := c.ownedSnapshot()
	topics := c.topicSnapshot()
	allowed := c.cfg.SheddingRelease
	for key, handle := range owned {
		if _, ok := desired[key]; ok {
			continue
		}
		if allowed == 0 {
			return
		}
		if c.cfg.MinConsumersPerPod > 0 && len(owned) <= c.cfg.MinConsumersPerPod {
			return
		}
		if c.cfg.MinConsumerRuntime > 0 && c.now().Sub(handle.startedAt) < c.cfg.MinConsumerRuntime {
			continue
		}
		if c.cooldownActive(ctx, handle.slot) {
			continue
		}
		if c.cfg.Kafka.Enabled && c.kafka != nil {
			if cfg, ok := topics[handle.slot.Topic]; ok && c.kafka.OverSubscribed(ctx, handle.slot.Topic, cfg.Partitions) {
				continue
			}
		}
		allowed--
		c.releaseSlot(ctx, handle.slot, "shedding")
	}
}

func (c *Controller) renewLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.LeaseRenewInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.renewOwned(ctx)
		}
	}
}

func (c *Controller) renewOwned(ctx context.Context) {
	owned := c.ownedSnapshot()
	for _, handle := range owned {
		select {
		case <-handle.done:
			c.releaseSlot(ctx, handle.slot, "consumer-exit")
			continue
		default:
		}
		res, err := c.renewScript.run(ctx, c.client, []string{leaseKey(handle.slot)}, c.nodeID, c.cfg.LeaseTTL.Milliseconds())
		if err != nil {
			c.logger.Warn("renew failed", Field{Key: "slot", Value: handle.slot.Key()}, Field{Key: "err", Value: err})
			c.markBackoff(err)
			continue
		}
		if res == 0 {
			c.logger.Warn("lease lost on renew", Field{Key: "slot", Value: handle.slot.Key()})
			c.stopConsumer(handle.slot, false, "lease-lost")
		}
	}
}

func (c *Controller) acquire(ctx context.Context, slot Slot) (bool, error) {
	ok, err := c.client.SetNX(ctx, leaseKey(slot), c.nodeID, c.cfg.LeaseTTL).Result()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if c.cfg.SlotMoveCooldown > 0 {
		_ = c.client.Set(ctx, movedKey(slot), fmt.Sprintf("%d", c.now().Unix()), c.cfg.SlotMoveCooldown).Err()
	}
	if err := c.startConsumer(slot); err != nil {
		_, _ = c.releaseScript.run(ctx, c.client, []string{leaseKey(slot)}, c.nodeID)
		return false, err
	}
	c.recordChurn()
	return true, nil
}

func (c *Controller) releaseSlot(ctx context.Context, slot Slot, reason string) {
	_, _ = c.releaseScript.run(ctx, c.client, []string{leaseKey(slot)}, c.nodeID)
	c.stopConsumer(slot, true, reason)
}

func (c *Controller) stopConsumer(slot Slot, applyCooldown bool, reason string) {
	key := slot.Key()
	c.mu.Lock()
	handle, ok := c.owned[key]
	if ok {
		delete(c.owned, key)
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	handle.cancel()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = handle.consumer.Stop(stopCtx)
	select {
	case <-handle.done:
	case <-time.After(5 * time.Second):
	}
	if applyCooldown && c.cfg.SlotMoveCooldown > 0 {
		_ = c.client.Set(context.Background(), movedKey(slot), fmt.Sprintf("%d", c.now().Unix()), c.cfg.SlotMoveCooldown).Err()
	}
	c.metrics.IncCounter("rendez_consumer_stop_total", 1, Label{Name: "reason", Value: reason})
}

func (c *Controller) startConsumer(slot Slot) error {
	consumer, err := c.factory.NewConsumer(slot)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(c.ctx)
	handle := &consumerHandle{
		slot:      slot,
		consumer:  consumer,
		cancel:    cancel,
		done:      make(chan struct{}),
		startedAt: c.now(),
	}
	c.mu.Lock()
	c.owned[slot.Key()] = handle
	c.mu.Unlock()

	c.metrics.IncCounter("rendez_consumer_start_total", 1, Label{Name: "topic", Value: slot.Topic})
	go func() {
		defer close(handle.done)
		if err := consumer.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Warn("consumer run error", Field{Key: "slot", Value: slot.Key()}, Field{Key: "err", Value: err})
		}
	}()
	return nil
}

func (c *Controller) ownedSnapshot() map[string]*consumerHandle {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]*consumerHandle, len(c.owned))
	for k, v := range c.owned {
		out[k] = v
	}
	return out
}

func (c *Controller) topicSnapshot() map[string]TopicConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]TopicConfig, len(c.topics))
	for k, v := range c.topics {
		out[k] = v
	}
	return out
}

func (c *Controller) refreshConfigs(ctx context.Context) error {
	if len(c.cfg.StaticTopics) > 0 {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.topics = make(map[string]TopicConfig, len(c.cfg.StaticTopics))
		for _, t := range c.cfg.StaticTopics {
			c.topics[t.Name] = t
		}
		return nil
	}
	names, err := c.client.SMembers(ctx, c.cfg.TopicIndexKey).Result()
	if err != nil {
		return err
	}
	configs := map[string]TopicConfig{}
	for _, name := range names {
		tc, err := c.fetchTopicConfig(ctx, name)
		if err != nil {
			c.logger.Warn("fetch topic config failed", Field{Key: "topic", Value: name}, Field{Key: "err", Value: err})
			continue
		}
		if tc.Name == "" {
			tc.Name = name
		}
		if tc.Partitions > 0 {
			configs[tc.Name] = tc
		}
	}
	c.mu.Lock()
	c.topics = configs
	c.mu.Unlock()
	return nil
}

func (c *Controller) refreshTopic(ctx context.Context, topic string) error {
	if len(c.cfg.StaticTopics) > 0 {
		return nil
	}
	tc, err := c.fetchTopicConfig(ctx, topic)
	if err != nil {
		return err
	}
	if tc.Partitions == 0 {
		return fmt.Errorf("topic %s partitions zero", topic)
	}
	c.mu.Lock()
	c.topics[tc.Name] = tc
	c.mu.Unlock()
	return nil
}

func (c *Controller) fetchTopicConfig(ctx context.Context, topic string) (TopicConfig, error) {
	raw, err := c.client.Get(ctx, c.cfg.ConfigKeyPrefix+topic).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return TopicConfig{}, fmt.Errorf("topic %s missing config", topic)
		}
		return TopicConfig{}, err
	}
	var cfg TopicConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return TopicConfig{}, err
	}
	if cfg.Name == "" {
		cfg.Name = topic
	}
	return cfg, nil
}

func (c *Controller) liveNodes(ctx context.Context) (map[string]bool, error) {
	nodes, err := c.client.SMembers(ctx, "nodes:all").Result()
	if err != nil {
		return nil, err
	}
	live := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		key := nodeKey(n)
		exists, err := c.client.Exists(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		if exists > 0 {
			live[n] = true
		}
	}
	live[c.nodeID] = true
	return live, nil
}

func (c *Controller) weightsSnapshot(live map[string]bool) []NodeWeight {
	out := make([]NodeWeight, 0, len(live))
	for id := range live {
		weight := c.cfg.DefaultWeight
		if w, ok := c.cfg.StaticWeights[id]; ok {
			weight = w
		}
		if c.weights != nil {
			if w, ok := c.weights.Weight(id); ok {
				weight = w
			}
		}
		out = append(out, NodeWeight{ID: id, Weight: weight})
	}
	return out
}

func (c *Controller) getLeaseOwner(ctx context.Context, slot Slot) (string, bool, error) {
	val, err := c.client.Get(ctx, leaseKey(slot)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", false, nil
		}
		return "", false, err
	}
	return val, true, nil
}

func (c *Controller) cooldownActive(ctx context.Context, slot Slot) bool {
	if c.cfg.SlotMoveCooldown <= 0 {
		return false
	}
	ok, err := c.client.Exists(ctx, movedKey(slot)).Result()
	if err != nil {
		c.logger.Warn("cooldown check failed", Field{Key: "slot", Value: slot.Key()}, Field{Key: "err", Value: err})
		return false
	}
	return ok > 0
}

func (c *Controller) inBackoff() bool {
	return c.now().UnixNano() < c.backoffTo.Load()
}

func (c *Controller) markBackoff(err error) {
	if err == nil || c.cfg.RedisBackoff == 0 {
		return
	}
	exp := c.cfg.RedisBackoff
	if c.cfg.RedisBackoffJitter > 0 {
		j := time.Duration(float64(exp) * c.cfg.RedisBackoffJitter)
		exp += time.Duration(rand.Int63n(int64(2*j)+1)) - j
	}
	c.backoffTo.Store(c.now().Add(exp).UnixNano())
}

func (c *Controller) recordChurn() {
	c.churnMu.Lock()
	defer c.churnMu.Unlock()
	now := c.now()
	c.churn = append(c.churn, now)
	cutoff := now.Add(-time.Minute)
	i := 0
	for ; i < len(c.churn); i++ {
		if c.churn[i].After(cutoff) {
			break
		}
	}
	if i > 0 {
		c.churn = c.churn[i:]
	}
	c.metrics.SetGauge("rendez_churn_per_minute", float64(len(c.churn)))
}

func (c *Controller) shutdown() {
	handles := c.ownedSnapshot()
	for _, h := range handles {
		if c.cfg.ReleaseOnShutdown {
			c.releaseSlot(context.Background(), h.slot, "shutdown")
		} else {
			c.stopConsumer(h.slot, false, "shutdown")
		}
	}
}

func nodeKey(nodeID string) string {
	return "node:" + nodeID
}

func leaseKey(slot Slot) string {
	return fmt.Sprintf("lease:%s:%d", slot.Topic, slot.Index)
}

func movedKey(slot Slot) string {
	return fmt.Sprintf("moved:%s:%d", slot.Topic, slot.Index)
}

func jitter(base time.Duration, ratio float64) time.Duration {
	if ratio <= 0 {
		return base
	}
	maxJitter := int64(float64(base) * ratio)
	if maxJitter == 0 {
		return base
	}
	offset := rand.Int63n(2*maxJitter+1) - maxJitter
	return time.Duration(int64(base) + offset)
}
