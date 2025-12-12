package rendez

import (
	"fmt"
	"time"
)

// TopicConfig describes a topic and its slot parallelism.
type TopicConfig struct {
	Name                string `json:"name" yaml:"name"`
	Partitions          int    `json:"partitions" yaml:"partitions"`
	MaxConsumersPerPod  int    `json:"maxConsumersPerPod,omitempty" yaml:"maxConsumersPerPod,omitempty"`
	MaxConsumersPerNode int    `json:"maxConsumersPerNode,omitempty" yaml:"maxConsumersPerNode,omitempty"`
}

// KafkaCheckConfig controls the optional Kafka membership sanity gate.
type KafkaCheckConfig struct {
	Enabled                    bool
	Interval                   time.Duration
	OversubscriptionMultiplier float64
	Threshold                  time.Duration
}

// Config captures all controller tunables.
type Config struct {
	ClusterID string
	NodeID    string

	HeartbeatTTL       time.Duration
	HeartbeatInterval  time.Duration
	LeaseTTL           time.Duration
	LeaseRenewInterval time.Duration
	ReconcileInterval  time.Duration
	ReconcileJitter    float64

	ConfigRefreshInterval time.Duration
	ConfigWatchChannel    string
	ConfigKeyPrefix       string
	TopicIndexKey         string

	GentleHandoff      bool
	SheddingEnabled    bool
	SheddingRelease    int
	MinConsumerRuntime time.Duration
	SlotMoveCooldown   time.Duration

	RedisBackoff       time.Duration
	RedisBackoffJitter float64

	MaxConsumersPerPod         int
	MaxConsumersPerTopicPerPod int
	MinConsumersPerPod         int
	AcquireLimit               int

	HealthGracePeriod time.Duration
	ReleaseOnShutdown bool

	StaticTopics  []TopicConfig
	StaticWeights map[string]float64
	DefaultWeight float64

	Kafka KafkaCheckConfig
}

// DefaultConfig returns conservative defaults aligned with the spec.
func DefaultConfig() Config {
	return Config{
		ClusterID:                  "default",
		HeartbeatTTL:               120 * time.Second,
		HeartbeatInterval:          10 * time.Second,
		LeaseTTL:                   90 * time.Second,
		LeaseRenewInterval:         30 * time.Second,
		ReconcileInterval:          22 * time.Second,
		ReconcileJitter:            0.15,
		ConfigRefreshInterval:      30 * time.Second,
		ConfigWatchChannel:         "cfg:updates",
		ConfigKeyPrefix:            "cfg:",
		TopicIndexKey:              "cfg:topics",
		GentleHandoff:              true,
		SheddingEnabled:            true,
		SheddingRelease:            2,
		MinConsumerRuntime:         90 * time.Second,
		SlotMoveCooldown:           5 * time.Minute,
		RedisBackoff:               45 * time.Second,
		RedisBackoffJitter:         0.25,
		MaxConsumersPerPod:         0, // unlimited
		MaxConsumersPerTopicPerPod: 0,
		MinConsumersPerPod:         0,
		AcquireLimit:               16,
		HealthGracePeriod:          5 * time.Second,
		ReleaseOnShutdown:          true,
		DefaultWeight:              1.0,
		Kafka: KafkaCheckConfig{
			Enabled:                    false,
			Interval:                   1 * time.Minute,
			OversubscriptionMultiplier: 1.5,
			Threshold:                  20 * time.Second,
		},
	}
}

// Validate checks config consistency.
func (c Config) Validate() error {
	if c.ClusterID == "" {
		return fmt.Errorf("ClusterID required")
	}
	if c.HeartbeatTTL <= 0 || c.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat ttl/interval must be >0")
	}
	if c.HeartbeatInterval >= c.HeartbeatTTL {
		return fmt.Errorf("heartbeat interval must be < ttl")
	}
	if c.LeaseTTL <= 0 || c.LeaseRenewInterval <= 0 {
		return fmt.Errorf("lease ttl/renew must be >0")
	}
	if c.LeaseRenewInterval*2 >= c.LeaseTTL {
		return fmt.Errorf("lease renew should be significantly less than ttl")
	}
	if c.ReconcileInterval <= 0 {
		return fmt.Errorf("reconcile interval must be >0")
	}
	if c.ReconcileJitter < 0 || c.ReconcileJitter > 1 {
		return fmt.Errorf("reconcile jitter must be between 0 and 1")
	}
	if c.ConfigRefreshInterval <= 0 {
		return fmt.Errorf("config refresh interval must be >0")
	}
	if c.MinConsumerRuntime < 0 {
		return fmt.Errorf("min consumer runtime cannot be negative")
	}
	if c.SlotMoveCooldown < 0 {
		return fmt.Errorf("slot move cooldown cannot be negative")
	}
	if c.SheddingRelease < 0 {
		return fmt.Errorf("shedding release must be >=0")
	}
	if c.RedisBackoff < 0 {
		return fmt.Errorf("redis backoff must be >=0")
	}
	if c.RedisBackoffJitter < 0 || c.RedisBackoffJitter > 1 {
		return fmt.Errorf("redis backoff jitter must be between 0 and 1")
	}
	if c.AcquireLimit < 0 {
		return fmt.Errorf("acquire limit must be >=0")
	}
	if c.MaxConsumersPerPod < 0 || c.MaxConsumersPerTopicPerPod < 0 {
		return fmt.Errorf("consumer caps must be >=0")
	}
	if c.MinConsumersPerPod < 0 {
		return fmt.Errorf("min consumers per pod must be >=0")
	}
	if c.DefaultWeight <= 0 {
		return fmt.Errorf("default weight must be >0")
	}
	for _, t := range c.StaticTopics {
		if t.Name == "" {
			return fmt.Errorf("static topic missing name")
		}
		if t.Partitions <= 0 {
			return fmt.Errorf("static topic %s partitions must be >0", t.Name)
		}
	}
	return nil
}
