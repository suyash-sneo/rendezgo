package rendez

import (
	"fmt"
	"time"
)

// WorkloadConfig describes a workload and its unit parallelism.
type WorkloadConfig struct {
	Name                string `json:"name" yaml:"name"`
	Units               int    `json:"units" yaml:"units"`
	MaxConsumersPerNode int    `json:"maxConsumersPerNode,omitempty" yaml:"maxConsumersPerNode,omitempty"`
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

	ConfigRefreshInterval   time.Duration
	ConfigWatchChannel      string
	WorkloadConfigKeyPrefix string
	WorkloadIndexKey        string

	GentleHandoff      bool
	SheddingEnabled    bool
	SheddingRelease    int
	MinConsumerRuntime time.Duration
	SlotMoveCooldown   time.Duration

	RedisBackoff       time.Duration
	RedisBackoffJitter float64

	MaxConsumersPerNode            int
	MaxConsumersPerWorkloadPerNode int
	MinConsumersPerNode            int
	AcquireLimit                   int

	HealthGracePeriod time.Duration
	ReleaseOnShutdown bool

	StaticWorkloads []WorkloadConfig
	StaticWeights   map[string]float64
	DefaultWeight   float64
}

// DefaultConfig returns conservative defaults aligned with the spec.
func DefaultConfig() Config {
	return Config{
		ClusterID:                      "default",
		HeartbeatTTL:                   120 * time.Second,
		HeartbeatInterval:              10 * time.Second,
		LeaseTTL:                       90 * time.Second,
		LeaseRenewInterval:             30 * time.Second,
		ReconcileInterval:              22 * time.Second,
		ReconcileJitter:                0.15,
		ConfigRefreshInterval:          30 * time.Second,
		ConfigWatchChannel:             "cfg:updates",
		WorkloadConfigKeyPrefix:        "cfg:workload:",
		WorkloadIndexKey:               "cfg:workloads",
		GentleHandoff:                  true,
		SheddingEnabled:                true,
		SheddingRelease:                2,
		MinConsumerRuntime:             90 * time.Second,
		SlotMoveCooldown:               5 * time.Minute,
		RedisBackoff:                   45 * time.Second,
		RedisBackoffJitter:             0.25,
		MaxConsumersPerNode:            0, // unlimited
		MaxConsumersPerWorkloadPerNode: 0,
		MinConsumersPerNode:            0,
		AcquireLimit:                   16,
		HealthGracePeriod:              5 * time.Second,
		ReleaseOnShutdown:              true,
		DefaultWeight:                  1.0,
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
	if c.MaxConsumersPerNode < 0 || c.MaxConsumersPerWorkloadPerNode < 0 {
		return fmt.Errorf("consumer caps must be >=0")
	}
	if c.MinConsumersPerNode < 0 {
		return fmt.Errorf("min consumers per node must be >=0")
	}
	if c.DefaultWeight <= 0 {
		return fmt.Errorf("default weight must be >0")
	}
	for _, t := range c.StaticWorkloads {
		if t.Name == "" {
			return fmt.Errorf("static workload missing name")
		}
		if t.Units <= 0 {
			return fmt.Errorf("static workload %s units must be >0", t.Name)
		}
	}
	return nil
}
