package sim

import (
	"sync"
)

// EventType enumerates all event kinds emitted by the engine.
type EventType string

const (
	EventEngineStart   EventType = "engine.start"
	EventEngineStop    EventType = "engine.stop"
	EventNodeAdd       EventType = "node.add"
	EventNodeRemove    EventType = "node.remove"
	EventNodeRestart   EventType = "node.restart"
	EventNodeKill      EventType = "node.kill"
	EventNodeWeight    EventType = "node.weight"
	EventNodeHealth    EventType = "node.health"
	EventNodeRedisFail EventType = "node.redisFault"
	EventShedding      EventType = "shedding.toggle"
	EventSheddingRate  EventType = "shedding.release"
	EventFocus         EventType = "focus.set"
	EventScenarioStart EventType = "scenario.start"
	EventScenarioStep  EventType = "scenario.step"
	EventScenarioDone  EventType = "scenario.done"
	EventScenarioError EventType = "scenario.error"
	EventPredictDown   EventType = "predict.down"
	EventExplainUnit   EventType = "explain.unit"
	EventError         EventType = "error"
)

// Event is stored in the ring buffer and streamed via SSE.
type Event struct {
	Seq      uint64                 `json:"seq"`
	AtUnixMs int64                  `json:"atUnixMs"`
	Type     EventType              `json:"type"`
	Message  string                 `json:"message"`
	Fields   map[string]interface{} `json:"fields,omitempty"`
}

// RingBuffer is a fixed-size buffer used for event history.
type RingBuffer struct {
	mu       sync.Mutex
	capacity int
	events   []Event
	nextSeq  uint64
}

// NewRingBuffer constructs a ring buffer with the given capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 2000
	}
	return &RingBuffer{capacity: capacity, nextSeq: 1}
}

// Append adds an event and returns its assigned sequence.
func (r *RingBuffer) Append(ev Event) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nextSeq == 0 {
		r.nextSeq = 1
	}
	ev.Seq = r.nextSeq
	r.nextSeq++
	if len(r.events) >= r.capacity {
		copy(r.events, r.events[1:])
		r.events[len(r.events)-1] = ev
	} else {
		r.events = append(r.events, ev)
	}
	return ev.Seq
}

// Since returns events with seq > after and the latest delivered seq.
func (r *RingBuffer) Since(after uint64) ([]Event, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	latest := after
	if len(r.events) == 0 {
		return nil, latest
	}
	idx := 0
	for idx < len(r.events) && r.events[idx].Seq <= after {
		idx++
	}
	out := make([]Event, len(r.events)-idx)
	copy(out, r.events[idx:])
	if len(out) > 0 {
		latest = out[len(out)-1].Seq
	}
	return out, latest
}
