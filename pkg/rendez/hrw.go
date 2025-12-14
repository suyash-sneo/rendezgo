package rendez

import (
	"math"
	"sort"

	"github.com/cespare/xxhash/v2"
)

// NodeWeight couples node ID with a weight used for weighted HRW.
type NodeWeight struct {
	ID     string
	Weight float64
}

// NodeScore represents a ranked HRW score for a node.
type NodeScore struct {
	ID     string
	Score  float64
	Weight float64
}

// RendezvousOwner returns the node ID with the highest weighted HRW score.
func RendezvousOwner(slot Slot, nodes []NodeWeight) (string, bool) {
	ranking := RendezvousRanking(slot, nodes)
	if len(ranking) == 0 {
		return "", false
	}
	return ranking[0].ID, true
}

// RendezvousRanking returns all candidates sorted by weighted HRW score (descending).
func RendezvousRanking(slot Slot, nodes []NodeWeight) []NodeScore {
	if len(nodes) == 0 {
		return nil
	}
	key := slot.Key()
	out := make([]NodeScore, 0, len(nodes))
	for _, n := range nodes {
		weight := n.Weight
		if weight <= 0 {
			continue
		}
		score := weightedScore(key, n.ID, weight)
		out = append(out, NodeScore{ID: n.ID, Score: score, Weight: weight})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].ID < out[j].ID
		}
		return out[i].Score > out[j].Score
	})
	return out
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
