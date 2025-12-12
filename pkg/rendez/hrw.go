package rendez

import (
	"math"

	"github.com/cespare/xxhash/v2"
)

// NodeWeight couples node ID with a weight used for weighted HRW.
type NodeWeight struct {
	ID     string
	Weight float64
}

// RendezvousOwner returns the node ID with the highest weighted HRW score.
func RendezvousOwner(slot Slot, nodes []NodeWeight) (string, bool) {
	if len(nodes) == 0 {
		return "", false
	}
	var (
		bestID    string
		bestScore float64
	)
	key := slot.Key()
	for _, n := range nodes {
		weight := n.Weight
		if weight <= 0 {
			continue
		}
		score := weightedScore(key, n.ID, weight)
		if bestID == "" || score > bestScore || (score == bestScore && n.ID < bestID) {
			bestID = n.ID
			bestScore = score
		}
	}
	if bestID == "" {
		return "", false
	}
	return bestID, true
}

func weightedScore(key, nodeID string, weight float64) float64 {
	h := xxhash.New()
	_, _ = h.WriteString(key)
	_, _ = h.WriteString("::")
	_, _ = h.WriteString(nodeID)
	sum := h.Sum64()
	// map to (0,1]; avoid log(0)
	u := float64(sum) / float64(math.MaxUint64)
	if u == 0 {
		u = math.SmallestNonzeroFloat64
	}
	return weight / -math.Log(u)
}
