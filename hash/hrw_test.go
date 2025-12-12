package hash

import (
	"fmt"
	"testing"
)

func TestOwnerDeterministic(t *testing.T) {
	nodes := []string{"node-a", "node-b", "node-c"}
	key := "workclass:topicA:slot3"

	first, ok := Owner(key, nodes)
	if !ok {
		t.Fatalf("expected owner, got none")
	}
	for i := 0; i < 5; i++ {
		next, ok := Owner(key, nodes)
		if !ok {
			t.Fatalf("iteration %d: expected owner, got none", i)
		}
		if next != first {
			t.Fatalf("owner changed between iterations: %q -> %q", first, next)
		}
	}
}

func TestSortedOwnersOrdering(t *testing.T) {
	nodes := []string{"a", "b", "c", "d"}
	key := "slot-17"

	results := SortedOwners(key, nodes)
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Fatalf("results not sorted descending at %d: %v < %v", i, results[i-1], results[i])
		}
	}
}

func TestMinimalMovementOnRemoval(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	var keys []string
	for i := 0; i < 200; i++ {
		keys = append(keys, fmt.Sprintf("class:%d", i))
	}

	ownersBefore := map[string]string{}
	for _, k := range keys {
		if owner, ok := Owner(k, nodes); ok {
			ownersBefore[k] = owner
		} else {
			t.Fatalf("no owner for %q", k)
		}
	}

	// Remove a node and measure movement; with HRW ~1/len(nodes) keys should move.
	nodesAfter := []string{"n1", "n3"}
	changed := 0
	for _, k := range keys {
		owner, ok := Owner(k, nodesAfter)
		if !ok {
			t.Fatalf("no owner after for %q", k)
		}
		if owner != ownersBefore[k] {
			changed++
		}
	}

	changeRatio := float64(changed) / float64(len(keys))
	if changeRatio < 0.2 || changeRatio > 0.5 {
		t.Fatalf("unexpected movement ratio: %.2f (changed %d of %d)", changeRatio, changed, len(keys))
	}
}
