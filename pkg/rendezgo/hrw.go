package rendezgo

import (
	"container/heap"
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
	if len(nodes) == 0 {
		return "", false
	}
	key := slot.Key()
	var best NodeScore
	hasBest := false
	for _, n := range nodes {
		if n.Weight <= 0 {
			continue
		}
		score := weightedScore(key, n.ID, n.Weight)
		if !hasBest || score > best.Score || (score == best.Score && n.ID < best.ID) {
			best = NodeScore{ID: n.ID, Score: score, Weight: n.Weight}
			hasBest = true
		}
	}
	if !hasBest {
		return "", false
	}
	return best.ID, true
}

// RendezvousTopK returns the top K candidates sorted by weighted HRW score (descending).
func RendezvousTopK(slot Slot, nodes []NodeWeight, k int) []NodeScore {
	if k <= 0 || len(nodes) == 0 {
		return nil
	}
	key := slot.Key()
	h := topKHeap{}
	heap.Init(&h)
	for _, n := range nodes {
		if n.Weight <= 0 {
			continue
		}
		ns := NodeScore{ID: n.ID, Weight: n.Weight, Score: weightedScore(key, n.ID, n.Weight)}
		if h.Len() < k {
			heap.Push(&h, ns)
			continue
		}
		if topKBetter(ns, h[0]) {
			heap.Pop(&h)
			heap.Push(&h, ns)
		}
	}
	if h.Len() == 0 {
		return nil
	}
	out := make([]NodeScore, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(&h).(NodeScore)
	}
	return out
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

type topKHeap []NodeScore

func (h topKHeap) Len() int { return len(h) }
func (h topKHeap) Less(i, j int) bool {
	if h[i].Score == h[j].Score {
		return h[i].ID > h[j].ID
	}
	return h[i].Score < h[j].Score
}
func (h topKHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *topKHeap) Push(x interface{}) {
	*h = append(*h, x.(NodeScore))
}
func (h *topKHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func topKBetter(a, b NodeScore) bool {
	return a.Score > b.Score || (a.Score == b.Score && a.ID < b.ID)
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
