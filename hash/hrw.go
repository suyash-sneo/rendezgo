package hash

import (
	"sort"

	"github.com/cespare/xxhash/v2"
)

// Owner returns the nodeID with the highest rendezvous score for the given key.
// Returns false if nodes is empty.
func Owner(key string, nodes []string) (string, bool) {
	if len(nodes) == 0 {
		return "", false
	}
	var (
		bestID    string
		bestScore uint64
	)
	for _, n := range nodes {
		score := Score(key, n)
		if bestID == "" || score > bestScore {
			bestID = n
			bestScore = score
		}
	}
	return bestID, true
}

// Score computes a rendezvous hash score for the provided key and nodeID.
func Score(key, nodeID string) uint64 {
	// Concatenate key and nodeID with a separator to avoid accidental overlap.
	hasher := xxhash.New()
	_, _ = hasher.WriteString(key)
	_, _ = hasher.WriteString("::")
	_, _ = hasher.WriteString(nodeID)
	return hasher.Sum64()
}

// SortedOwners returns the nodes sorted by descending rendezvous score.
// Useful for testing and inspection.
func SortedOwners(key string, nodes []string) []Result {
	results := make([]Result, 0, len(nodes))
	for _, n := range nodes {
		results = append(results, Result{
			NodeID: n,
			Score:  Score(key, n),
		})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score == results[j].Score {
			return results[i].NodeID < results[j].NodeID
		}
		return results[i].Score > results[j].Score
	})
	return results
}

// Result captures a rendezvous score for a node.
type Result struct {
	NodeID string
	Score  uint64
}
