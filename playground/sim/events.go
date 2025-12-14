package sim

import (
	"sync"
	"time"
)

type EventType string

const (
	EventCommand    EventType = "command"
	EventInfo       EventType = "info"
	EventError      EventType = "error"
	EventScenario   EventType = "scenario"
	EventPrediction EventType = "prediction"
)

type Event struct {
	Seq     uint64                 `json:"seq"`
	At      time.Time              `json:"at"`
	Type    EventType              `json:"type"`
	Message string                 `json:"message"`
	Fields  map[string]interface{} `json:"fields,omitempty"`
}

type RingBuffer struct {
	mu       sync.Mutex
	capacity int
	events   []Event
	nextSeq  uint64
}

func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 2000
	}
	return &RingBuffer{capacity: capacity, nextSeq: 1}
}

// Append adds an event and returns the assigned sequence number.
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
