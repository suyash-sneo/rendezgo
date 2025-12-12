package workdist

import (
	"context"
	"fmt"
)

// Slot identifies a unit of concurrency within a work class.
type Slot struct {
	WorkClass string
	SlotIndex int
}

// Key returns a stable identifier for the slot used in leases.
func (s Slot) Key() string {
	return fmt.Sprintf("%s|%d", s.WorkClass, s.SlotIndex)
}

// SlotRunner performs work for a slot. Run should block until the context is cancelled or an error occurs.
type SlotRunner interface {
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
}

// SlotFactory constructs SlotRunner instances.
type SlotFactory interface {
	NewSlotRunner(slot Slot) (SlotRunner, error)
}

// SlotRunnerFunc adapts a function to SlotRunner.
type SlotRunnerFunc func(ctx context.Context) error

// Run implements SlotRunner.
func (f SlotRunnerFunc) Run(ctx context.Context) error { return f(ctx) }

// Stop implements SlotRunner.
func (f SlotRunnerFunc) Stop(ctx context.Context) error { return nil }

// ParallelismProvider supplies desired parallelism per work class.
type ParallelismProvider interface {
	Get(ctx context.Context, workClass string) (parallelism int, version string, err error)
	List(ctx context.Context) ([]WorkClassConfig, error)
}

// WorkClassConfig describes a work class and its desired parallelism.
type WorkClassConfig struct {
	WorkClass   string
	Parallelism int
	Version     string
}

// NodeIDProvider returns a stable identifier for the current node.
type NodeIDProvider interface {
	NodeID() (string, error)
}

// Logger is a lightweight structured logger interface.
type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}

// Field holds a structured logging field.
type Field struct {
	Key   string
	Value interface{}
}

// Metrics records counters and gauges.
type Metrics interface {
	IncCounter(name string, value float64, labels ...Label)
	SetGauge(name string, value float64, labels ...Label)
	ObserveHistogram(name string, value float64, labels ...Label)
}

// Label is a simple name/value pair for metrics.
type Label struct {
	Name  string
	Value string
}

type nopLogger struct{}

// NopLogger returns a no-op logger implementation.
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
