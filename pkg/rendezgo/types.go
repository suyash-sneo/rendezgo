package rendezgo

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// Slot identifies a workload/unit pair.
type Slot struct {
	Workload string
	Unit     int
}

// Key returns the canonical key used for leases and cooldown markers.
func (s Slot) Key() string {
	return s.Workload + "|" + strconv.Itoa(s.Unit)
}

// SlotFromKey parses a slot key back into a Slot.
func SlotFromKey(key string) (Slot, error) {
	parts := strings.Split(key, "|")
	if len(parts) != 2 {
		return Slot{}, fmt.Errorf("invalid slot key %q", key)
	}
	idx, err := strconv.Atoi(parts[1])
	if err != nil {
		return Slot{}, fmt.Errorf("invalid slot index in %q: %w", key, err)
	}
	return Slot{Workload: parts[0], Unit: idx}, nil
}

// Consumer performs work for a slot.
type Consumer interface {
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
}

// ConsumerFactory constructs Consumers for slots.
type ConsumerFactory interface {
	NewConsumer(slot Slot) (Consumer, error)
}

// HealthChecker gates voluntary actions such as shedding.
type HealthChecker interface {
	Healthy(ctx context.Context) bool
}

// WeightProvider supplies dynamic node weights.
type WeightProvider interface {
	Weight(nodeID string) (float64, bool)
}

// Logger is a small structured logger.
type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}

// Field represents a structured log field.
type Field struct {
	Key   string
	Value interface{}
}

// Metrics captures a minimal metrics surface.
type Metrics interface {
	IncCounter(name string, value float64, labels ...Label)
	SetGauge(name string, value float64, labels ...Label)
	ObserveHistogram(name string, value float64, labels ...Label)
}

// Label is a name/value pair for metrics.
type Label struct {
	Name  string
	Value string
}

type nopLogger struct{}

// NopLogger returns a no-op logger.
func NopLogger() Logger { return nopLogger{} }

func (nopLogger) Debug(string, ...Field) {}
func (nopLogger) Info(string, ...Field)  {}
func (nopLogger) Warn(string, ...Field)  {}
func (nopLogger) Error(string, ...Field) {}

type nopMetrics struct{}

// NopMetrics returns a no-op metrics recorder.
func NopMetrics() Metrics { return nopMetrics{} }

func (nopMetrics) IncCounter(string, float64, ...Label)       {}
func (nopMetrics) SetGauge(string, float64, ...Label)         {}
func (nopMetrics) ObserveHistogram(string, float64, ...Label) {}
