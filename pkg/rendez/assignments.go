package rendez

import "sort"

// DesiredOwners returns deterministic desired lease owners for workloads across nodes.
func DesiredOwners(workloads []WorkloadConfig, nodes []NodeWeight) map[string]string {
	out := make(map[string]string)
	if len(nodes) == 0 {
		return out
	}
	byName := make(map[string]WorkloadConfig, len(workloads))
	names := make([]string, 0, len(workloads))
	for _, wl := range workloads {
		if wl.Name == "" || wl.Units == 0 {
			continue
		}
		byName[wl.Name] = wl
		names = append(names, wl.Name)
	}
	sort.Strings(names)
	for _, name := range names {
		wl := byName[name]
		for unit := 0; unit < wl.Units; unit++ {
			slot := Slot{Workload: name, Unit: unit}
			if owner, ok := RendezvousOwner(slot, nodes); ok {
				out[leaseKey(slot)] = owner
			}
		}
	}
	return out
}
