package workdist

import (
	"fmt"
	"time"
)

// Config controls coordinator timing and stability behavior.
type Config struct {
	// Timing
	NodeTTL             time.Duration
	NodeHeartbeatPeriod time.Duration
	LeaseTTL            time.Duration
	LeaseRenewPeriod    time.Duration
	ReconcilePeriod     time.Duration
	JoinWarmup          time.Duration

	// Stability controls
	MinSlotRuntime       time.Duration
	SlotMoveCooldown     time.Duration
	MaxAcquirePerTick    int
	ReleasePerMinute     int
	MaxSlotsPerNode      int
	MaxSlotsPerWorkClass int

	// Error handling
	StoreErrorBackoff BackoffConfig
	JitterRatio       float64

	// Behavior flags
	EnableGentleHandoff bool
	EnableCooldowns     bool
}

// BackoffConfig describes an exponential backoff policy.
type BackoffConfig struct {
	Base       time.Duration
	Max        time.Duration
	Multiplier float64
}

// Next returns the next backoff duration for the given retry count.
func (b BackoffConfig) Next(retry int) time.Duration {
	if retry <= 0 {
		return b.Base
	}
	d := float64(b.Base)
	for i := 0; i < retry; i++ {
		d *= b.Multiplier
		if d >= float64(b.Max) {
			return b.Max
		}
	}
	return time.Duration(d)
}

// DefaultConfig returns a config tuned for stability.
func DefaultConfig() Config {
	return Config{
		NodeTTL:             120 * time.Second,
		NodeHeartbeatPeriod: 10 * time.Second,
		LeaseTTL:            90 * time.Second,
		LeaseRenewPeriod:    30 * time.Second,
		ReconcilePeriod:     20 * time.Second,
		JoinWarmup:          15 * time.Second,

		MinSlotRuntime:       90 * time.Second,
		SlotMoveCooldown:     4 * time.Minute,
		MaxAcquirePerTick:    5,
		ReleasePerMinute:     2,
		MaxSlotsPerNode:      200,
		MaxSlotsPerWorkClass: 20,

		StoreErrorBackoff: BackoffConfig{
			Base:       500 * time.Millisecond,
			Max:        30 * time.Second,
			Multiplier: 2.0,
		},
		JitterRatio: 0.2,

		EnableGentleHandoff: true,
		EnableCooldowns:     true,
	}
}

// Validate ensures config values are safe.
func (c Config) Validate() error {
	if c.NodeTTL <= 0 {
		return fmt.Errorf("NodeTTL must be >0")
	}
	if c.NodeHeartbeatPeriod <= 0 {
		return fmt.Errorf("NodeHeartbeatPeriod must be >0")
	}
	if c.NodeHeartbeatPeriod >= c.NodeTTL {
		return fmt.Errorf("NodeHeartbeatPeriod must be less than NodeTTL")
	}
	if c.LeaseTTL <= 0 {
		return fmt.Errorf("LeaseTTL must be >0")
	}
	if c.LeaseRenewPeriod <= 0 {
		return fmt.Errorf("LeaseRenewPeriod must be >0")
	}
	if c.LeaseRenewPeriod >= c.LeaseTTL {
		return fmt.Errorf("LeaseRenewPeriod must be less than LeaseTTL")
	}
	if c.ReconcilePeriod <= 0 {
		return fmt.Errorf("ReconcilePeriod must be >0")
	}
	if c.JoinWarmup < 0 {
		return fmt.Errorf("JoinWarmup cannot be negative")
	}
	if c.MinSlotRuntime < 0 {
		return fmt.Errorf("MinSlotRuntime cannot be negative")
	}
	if c.SlotMoveCooldown < 0 {
		return fmt.Errorf("SlotMoveCooldown cannot be negative")
	}
	if c.MaxAcquirePerTick <= 0 {
		return fmt.Errorf("MaxAcquirePerTick must be >0")
	}
	if c.ReleasePerMinute < 0 {
		return fmt.Errorf("ReleasePerMinute cannot be negative")
	}
	if c.MaxSlotsPerNode <= 0 {
		return fmt.Errorf("MaxSlotsPerNode must be >0")
	}
	if c.MaxSlotsPerWorkClass <= 0 {
		return fmt.Errorf("MaxSlotsPerWorkClass must be >0")
	}
	if c.JitterRatio < 0 || c.JitterRatio > 1 {
		return fmt.Errorf("JitterRatio must be between 0 and 1")
	}
	if err := c.StoreErrorBackoff.validate(); err != nil {
		return fmt.Errorf("StoreErrorBackoff invalid: %w", err)
	}
	return nil
}

func (b BackoffConfig) validate() error {
	if b.Base <= 0 {
		return fmt.Errorf("Base must be >0")
	}
	if b.Max <= 0 {
		return fmt.Errorf("Max must be >0")
	}
	if b.Multiplier < 1 {
		return fmt.Errorf("Multiplier must be >=1")
	}
	if b.Base > b.Max {
		return fmt.Errorf("Base must be <= Max")
	}
	return nil
}
