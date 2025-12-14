package rendez

import (
	"fmt"
	"testing"
)

func TestRendezvousDeterministic(t *testing.T) {
	nodes := []NodeWeight{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}}
	slot := Slot{Workload: "alpha", Unit: 3}
	first, ok := RendezvousOwner(slot, nodes)
	if !ok {
		t.Fatalf("expected owner")
	}
	for i := 0; i < 5; i++ {
		next, ok := RendezvousOwner(slot, nodes)
		if !ok || next != first {
			t.Fatalf("owner changed at %d: %v %v", i, ok, next)
		}
	}
}

func TestWeightedBias(t *testing.T) {
	nodes := []NodeWeight{{ID: "light", Weight: 1}, {ID: "heavy", Weight: 5}}
	heavy := 0
	total := 200
	for i := 0; i < total; i++ {
		slot := Slot{Workload: "workload", Unit: i}
		owner, ok := RendezvousOwner(slot, nodes)
		if !ok {
			t.Fatalf("missing owner at %d", i)
		}
		if owner == "heavy" {
			heavy++
		}
	}
	if heavy <= total/2 {
		t.Fatalf("heavy node did not dominate: %d of %d", heavy, total)
	}
}

func TestMinimalChurnOnRemoval(t *testing.T) {
	nodes := []NodeWeight{{ID: "n1", Weight: 1}, {ID: "n2", Weight: 1}, {ID: "n3", Weight: 1}}
	keys := make([]Slot, 0, 200)
	for i := 0; i < 200; i++ {
		keys = append(keys, Slot{Workload: fmt.Sprintf("t-%d", i), Unit: i % 3})
	}
	before := map[string]string{}
	for _, s := range keys {
		owner, ok := RendezvousOwner(s, nodes)
		if !ok {
			t.Fatalf("no owner for %v", s)
		}
		before[s.Key()] = owner
	}

	afterNodes := []NodeWeight{{ID: "n1", Weight: 1}, {ID: "n3", Weight: 1}}
	changed := 0
	for _, s := range keys {
		owner, ok := RendezvousOwner(s, afterNodes)
		if !ok {
			t.Fatalf("no owner after for %v", s)
		}
		if owner != before[s.Key()] {
			changed++
		}
	}
	ratio := float64(changed) / float64(len(keys))
	if ratio < 0.15 || ratio > 0.5 {
		t.Fatalf("unexpected movement ratio %.2f", ratio)
	}
}
