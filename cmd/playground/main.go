package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/chzyer/readline"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"

	"github.com/suyash-sneo/rendezgo/pkg/rendezgo"
)

func main() {
	var (
		mode              string
		redisAddr         string
		workloadsFlag     string
		initialNodes      int
		scenario          string
		scenarioFile      string
		dashboardInterval time.Duration
	)
	flag.StringVar(&mode, "mode", "simulated", "simulated or real")
	flag.StringVar(&redisAddr, "redis", "127.0.0.1:6379", "redis address for real mode")
	flag.StringVar(&workloadsFlag, "workloads", "demo:8", "workloads as workload:units,...")
	flag.IntVar(&initialNodes, "nodes", 3, "initial node count")
	flag.StringVar(&scenario, "scenario", "", "built-in scenario to run")
	flag.StringVar(&scenarioFile, "scenario-file", "", "path to YAML/JSON scenario file")
	flag.DurationVar(&dashboardInterval, "dashboard-interval", time.Second, "dashboard refresh interval")
	flag.Parse()

	workloads := parseWorkloads(workloadsFlag)
	if len(workloads) == 0 {
		workloads = []rendezgo.WorkloadConfig{{Name: "demo", Units: 8}}
	}
	if dashboardInterval <= 0 {
		dashboardInterval = time.Second
	}

	ctx, cancel := signalContext()
	defer cancel()

	sim, err := newSimulation(ctx, mode, redisAddr, workloads)
	if err != nil {
		fmt.Fprintf(os.Stderr, "playground init: %v\n", err)
		os.Exit(1)
	}
	sim.dashboardInterval = dashboardInterval
	sim.dashboardEnabled.Store(true)
	sim.focus = ""
	sim.topK = 3
	for i := 0; i < initialNodes; i++ {
		sim.addNode(1.0)
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 "> ",
		HistoryLimit:           500,
		DisableAutoSaveHistory: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "readline init: %v\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	if scenario != "" {
		go sim.runScenario(ctx, scenario, rl)
	}
	if scenarioFile != "" {
		go sim.runScenarioFile(ctx, scenarioFile, rl)
	}

	go sim.dashboard(ctx, rl)
	fmt.Fprintln(rl.Stdout(), "Chaos playground ready. Type 'help' for commands.")
	rl.Refresh()
	sim.repl(ctx, rl)
}

type simulation struct {
	template  rendezgo.Config
	workloads []rendezgo.WorkloadConfig

	mu        sync.Mutex
	nodes     map[string]*simNode
	weights   *weightTable
	admin     *redis.Client
	redisAddr string
	server    *miniredis.Miniredis

	owners map[string]string
	churn  []time.Time

	focus             string
	dashboardInterval time.Duration
	dashboardEnabled  atomic.Bool
	topK              int
}

type simNode struct {
	id      string
	weight  float64
	cancel  context.CancelFunc
	ctrl    *rendezgo.Controller
	client  *redis.Client
	hook    *chaosHook
	health  *healthGate
	healthy bool
}

type unitState struct {
	slot        rendezgo.Slot
	owner       string
	desired     string
	leaseTTL    time.Duration
	cooldownTTL time.Duration
	ranking     []rendezgo.NodeScore
}

type workloadCounts struct {
	owned   int
	desired int
}

type nodeSummary struct {
	id       string
	weight   float64
	state    string
	owned    int
	desired  int
	missing  int
	extra    int
	workload map[string]workloadCounts
}

func newSimulation(ctx context.Context, mode, redisAddr string, workloads []rendezgo.WorkloadConfig) (*simulation, error) {
	cfg := rendezgo.DefaultConfig()
	cfg.ClusterID = "sim"
	cfg.ReconcileInterval = 2 * time.Second
	cfg.ReconcileJitter = 0.1
	cfg.LeaseTTL = 10 * time.Second
	cfg.LeaseRenewInterval = 3 * time.Second
	cfg.HeartbeatInterval = 2 * time.Second
	cfg.HeartbeatTTL = 12 * time.Second
	cfg.MinConsumerRuntime = 2 * time.Second
	cfg.SlotMoveCooldown = 8 * time.Second
	cfg.ConfigRefreshInterval = 10 * time.Second
	cfg.StaticWorkloads = workloads
	cfg.AcquireLimit = 200

	sim := &simulation{
		template:  cfg,
		workloads: workloads,
		nodes:     map[string]*simNode{},
		weights:   newWeightTable(),
		owners:    map[string]string{},
	}
	if mode == "real" {
		sim.redisAddr = redisAddr
		sim.admin = redis.NewClient(&redis.Options{Addr: redisAddr})
	} else {
		server, err := miniredis.Run()
		if err != nil {
			return nil, err
		}
		sim.server = server
		sim.redisAddr = server.Addr()
		sim.admin = redis.NewClient(&redis.Options{Addr: server.Addr()})
	}
	go func() {
		<-ctx.Done()
		if sim.server != nil {
			sim.server.Close()
		}
	}()
	return sim, nil
}

func (s *simulation) addNode(weight float64) string {
	return s.spawnNode("", weight)
}

func (s *simulation) spawnNode(customID string, weight float64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := customID
	if id == "" {
		id = fmt.Sprintf("n-%d", len(s.nodes)+1)
	}
	cfg := s.template
	cfg.NodeID = id
	cfg.StaticWeights = nil
	client := redis.NewClient(&redis.Options{Addr: s.redisAddr})
	hook := &chaosHook{}
	client.AddHook(hook)
	health := &healthGate{healthy: true}
	factory := &playConsumerFactory{}
	ctrl, err := rendezgo.NewController(cfg, client, factory, rendezgo.NopLogger(), rendezgo.NopMetrics(), rendezgo.WithWeightProvider(s.weights), rendezgo.WithHealthChecker(health))
	if err != nil {
		fmt.Printf("add node: %v\n", err)
		return ""
	}
	nodeID := cfg.ClusterID + ":" + cfg.NodeID
	s.weights.Set(nodeID, weight)
	ctx, cancel := context.WithCancel(context.Background())
	go ctrl.Start(ctx)
	node := &simNode{
		id:      nodeID,
		weight:  weight,
		cancel:  cancel,
		ctrl:    ctrl,
		client:  client,
		hook:    hook,
		health:  health,
		healthy: true,
	}
	s.nodes[nodeID] = node
	return nodeID
}

func (s *simulation) removeNode(id string, graceful bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.nodes[id]
	if !ok {
		fmt.Printf("node %s not found\n", id)
		return
	}
	node.ctrl.SetReleaseOnShutdown(graceful)
	node.cancel()
	delete(s.nodes, id)
	s.weights.Delete(id)
}

func (s *simulation) adjustWeight(id string, weight float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.nodes[id]
	if !ok {
		fmt.Printf("node %s not found\n", id)
		return
	}
	node.weight = weight
	s.weights.Set(id, weight)
}

func (s *simulation) restartNode(id string) {
	s.mu.Lock()
	node, ok := s.nodes[id]
	weight := 1.0
	if ok {
		weight = node.weight
	}
	s.mu.Unlock()
	if !ok {
		fmt.Printf("node %s not found\n", id)
		return
	}
	s.removeNode(id, true)
	base := id
	if strings.Contains(id, ":") {
		parts := strings.Split(id, ":")
		base = parts[len(parts)-1]
	}
	newID := s.spawnNode(base, weight)
	fmt.Printf("restarted %s -> %s\n", id, newID)
}

func (s *simulation) restartAll() {
	for _, id := range s.nodeIDs() {
		s.restartNode(id)
	}
}

func (s *simulation) setRedisFault(id string, fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.nodes[id]
	if !ok {
		fmt.Printf("node %s not found\n", id)
		return
	}
	node.hook.fail.Store(fail)
}

func (s *simulation) setHealth(id string, healthy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.nodes[id]
	if !ok {
		fmt.Printf("node %s not found\n", id)
		return
	}
	node.health.Set(healthy)
	node.healthy = healthy
}

func (s *simulation) sortedWorkloads() []rendezgo.WorkloadConfig {
	byName := make(map[string]rendezgo.WorkloadConfig, len(s.workloads))
	names := make([]string, 0, len(s.workloads))
	for _, wl := range s.workloads {
		byName[wl.Name] = wl
		names = append(names, wl.Name)
	}
	sort.Strings(names)
	out := make([]rendezgo.WorkloadConfig, 0, len(names))
	for _, name := range names {
		out = append(out, byName[name])
	}
	return out
}

func (s *simulation) liveNodeWeights(exclude string) []rendezgo.NodeWeight {
	s.mu.Lock()
	ids := make([]string, 0, len(s.nodes))
	for id := range s.nodes {
		if id == exclude {
			continue
		}
		ids = append(ids, id)
	}
	s.mu.Unlock()
	sort.Strings(ids)
	snapshot := s.weights.Snapshot()
	out := make([]rendezgo.NodeWeight, 0, len(ids))
	for _, id := range ids {
		w := snapshot[id]
		if w == 0 {
			w = 1
		}
		out = append(out, rendezgo.NodeWeight{ID: id, Weight: w})
	}
	return out
}

func (s *simulation) redisState() (map[string]string, map[string]time.Duration, map[string]time.Duration) {
	owners := map[string]string{}
	leaseTTLs := map[string]time.Duration{}
	cooldownTTLs := map[string]time.Duration{}
	ctx := context.Background()
	pipe := s.admin.Pipeline()
	leaseCmds := map[string]*redis.StringCmd{}
	ttlCmds := map[string]*redis.DurationCmd{}
	coolCmds := map[string]*redis.DurationCmd{}
	for _, wl := range s.sortedWorkloads() {
		for unit := 0; unit < wl.Units; unit++ {
			slot := rendezgo.Slot{Workload: wl.Name, Unit: unit}
			key := leaseKeyLocal(slot)
			leaseCmds[key] = pipe.Get(ctx, key)
			ttlCmds[key] = pipe.PTTL(ctx, key)
			coolCmds[key] = pipe.PTTL(ctx, movedKeyLocal(slot))
		}
	}
	if len(leaseCmds) == 0 {
		return owners, leaseTTLs, cooldownTTLs
	}
	_, _ = pipe.Exec(ctx)
	for key, cmd := range leaseCmds {
		if val, err := cmd.Result(); err == nil {
			owners[key] = val
		}
	}
	for key, cmd := range ttlCmds {
		if ttl, err := cmd.Result(); err == nil && ttl > 0 {
			leaseTTLs[key] = ttl
		}
	}
	for key, cmd := range coolCmds {
		if ttl, err := cmd.Result(); err == nil && ttl > 0 {
			cooldownTTLs[key] = ttl
		}
	}
	return owners, leaseTTLs, cooldownTTLs
}

func (s *simulation) buildUnitStates(weights []rendezgo.NodeWeight) []unitState {
	desired := rendezgo.DesiredOwners(s.workloads, weights)
	owners, leaseTTLs, cooldownTTLs := s.redisState()
	states := make([]unitState, 0)
	for _, wl := range s.sortedWorkloads() {
		for unit := 0; unit < wl.Units; unit++ {
			slot := rendezgo.Slot{Workload: wl.Name, Unit: unit}
			key := leaseKeyLocal(slot)
			states = append(states, unitState{
				slot:        slot,
				owner:       owners[key],
				desired:     desired[key],
				leaseTTL:    leaseTTLs[key],
				cooldownTTL: cooldownTTLs[key],
				ranking:     rendezgo.RendezvousRanking(slot, weights),
			})
		}
	}
	return states
}

func (s *simulation) buildNodeSummaries(states []unitState) []nodeSummary {
	s.mu.Lock()
	nodes := make(map[string]*simNode, len(s.nodes))
	for id, n := range s.nodes {
		nodes[id] = n
	}
	s.mu.Unlock()
	weightSnap := s.weights.Snapshot()
	summary := map[string]*nodeSummary{}
	for id, n := range nodes {
		summary[id] = &nodeSummary{
			id:       id,
			weight:   n.weight,
			state:    nodeState(n),
			workload: map[string]workloadCounts{},
		}
	}
	for _, st := range states {
		if st.desired != "" {
			ns, ok := summary[st.desired]
			if !ok {
				w := weightSnap[st.desired]
				if w == 0 {
					w = 1
				}
				ns = &nodeSummary{
					id:       st.desired,
					weight:   w,
					state:    "unknown",
					workload: map[string]workloadCounts{},
				}
				summary[st.desired] = ns
			}
			ns.desired++
			wc := ns.workload[st.slot.Workload]
			wc.desired++
			ns.workload[st.slot.Workload] = wc
		}
		if st.owner != "" {
			ns, ok := summary[st.owner]
			if !ok {
				w := weightSnap[st.owner]
				if w == 0 {
					w = 1
				}
				ns = &nodeSummary{
					id:       st.owner,
					weight:   w,
					state:    "unknown",
					workload: map[string]workloadCounts{},
				}
				summary[st.owner] = ns
			}
			ns.owned++
			wc := ns.workload[st.slot.Workload]
			wc.owned++
			ns.workload[st.slot.Workload] = wc
		}
	}
	out := make([]nodeSummary, 0, len(summary))
	for _, ns := range summary {
		if ns.desired > ns.owned {
			ns.missing = ns.desired - ns.owned
		}
		if ns.owned > ns.desired {
			ns.extra = ns.owned - ns.desired
		}
		out = append(out, *ns)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

func (s *simulation) dashboard(ctx context.Context, rl *readline.Instance) {
	t := time.NewTicker(s.dashboardInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !s.dashboardEnabled.Load() {
				continue
			}
			s.printDashboard(rl)
		}
	}
}

func (s *simulation) printDashboard(rl *readline.Instance) {
	weights := s.liveNodeWeights("")
	states := s.buildUnitStates(weights)
	nodeSummaries := s.buildNodeSummaries(states)

	current := map[string]string{}
	for _, st := range states {
		if st.owner != "" {
			current[leaseKeyLocal(st.slot)] = st.owner
		}
	}
	churn := s.updateChurn(current)

	w := rl.Stdout()
	fmt.Fprint(w, "\033[H\033[2J")
	focusLabel := s.focus
	if focusLabel == "" {
		focusLabel = "all"
	}
	fmt.Fprintf(w, "redis: %s | nodes: %d | churn/min: %d | focus: %s\n", s.redisAddr, len(nodeSummaries), churn, focusLabel)
	fmt.Fprintf(w, "Caps: max/node=%d max/workload/node=%d min/node=%d acquire=%d\n", s.template.MaxConsumersPerNode, s.template.MaxConsumersPerWorkloadPerNode, s.template.MinConsumersPerNode, s.template.AcquireLimit)
	fmt.Fprintln(w, "Nodes:")
	for _, ns := range nodeSummaries {
		fmt.Fprintf(w, "  %-12s weight=%.2f state=%-9s owned=%d desired=%d missing=%d extra=%d\n", ns.id, ns.weight, ns.state, ns.owned, ns.desired, ns.missing, ns.extra)
		if len(ns.workload) > 0 {
			wlNames := make([]string, 0, len(ns.workload))
			for name := range ns.workload {
				wlNames = append(wlNames, name)
			}
			sort.Strings(wlNames)
			fmt.Fprint(w, "    workloads: ")
			for i, name := range wlNames {
				wc := ns.workload[name]
				if i > 0 {
					fmt.Fprint(w, " | ")
				}
				fmt.Fprintf(w, "%s %d/%d", name, wc.owned, wc.desired)
			}
			fmt.Fprintln(w)
		}
	}

	fmt.Fprintln(w, "\nWorkloads:")
	grouped := make(map[string][]unitState)
	for _, st := range states {
		if s.focus != "" && s.focus != st.slot.Workload {
			continue
		}
		grouped[st.slot.Workload] = append(grouped[st.slot.Workload], st)
	}
	names := make([]string, 0, len(grouped))
	for name := range grouped {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(w, "  %s:\n", name)
		for _, st := range grouped[name] {
			fmt.Fprintf(w, "    [%d] desired=%s owner=%s ttl=%s cooldown=%s\n", st.slot.Unit, shortOwner(st.desired), shortOwner(st.owner), formatTTL(st.leaseTTL), formatTTL(st.cooldownTTL))
			fmt.Fprintf(w, "      candidates: %s\n", formatRanking(st.ranking, s.topK))
		}
	}
	rl.Refresh()
}

func (s *simulation) predictDown(rl *readline.Instance, target string) {
	full := s.liveNodeWeights("")
	remaining := s.liveNodeWeights(target)
	if len(full) == len(remaining) {
		fmt.Fprintf(rl.Stdout(), "node %s not found\n", target)
		return
	}
	currentDesired := rendezgo.DesiredOwners(s.workloads, full)
	predicted := rendezgo.DesiredOwners(s.workloads, remaining)
	moved := make([]rendezgo.Slot, 0)
	for key, newOwner := range predicted {
		if currentDesired[key] != newOwner {
			slot, err := slotFromLeaseKey(key)
			if err == nil {
				moved = append(moved, slot)
			}
		}
	}
	sort.Slice(moved, func(i, j int) bool {
		if moved[i].Workload == moved[j].Workload {
			return moved[i].Unit < moved[j].Unit
		}
		return moved[i].Workload < moved[j].Workload
	})
	fmt.Fprintf(rl.Stdout(), "predicting %s down (%d slots affected):\n", target, len(moved))
	for _, slot := range moved {
		key := leaseKeyLocal(slot)
		ranking := rendezgo.RendezvousRanking(slot, remaining)
		fmt.Fprintf(rl.Stdout(), "  %s[%d]: %s -> %s candidates: %s\n", slot.Workload, slot.Unit, shortOwner(currentDesired[key]), shortOwner(predicted[key]), formatRanking(ranking, s.topK))
	}
}

func (s *simulation) explainUnit(rl *readline.Instance, workload string, unit int) {
	weights := s.liveNodeWeights("")
	slot := rendezgo.Slot{Workload: workload, Unit: unit}
	ranking := rendezgo.RendezvousRanking(slot, weights)
	if len(ranking) == 0 {
		fmt.Fprintln(rl.Stdout(), "no candidates found")
		return
	}
	fmt.Fprintf(rl.Stdout(), "HRW ranking for %s[%d]:\n", workload, unit)
	limit := len(ranking)
	if limit > 10 {
		limit = 10
	}
	for i := 0; i < limit; i++ {
		fmt.Fprintf(rl.Stdout(), "  %d) %s score=%.4f weight=%.2f\n", i+1, ranking[i].ID, ranking[i].Score, ranking[i].Weight)
	}
}

func (s *simulation) updateChurn(current map[string]string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, v := range current {
		if prev, ok := s.owners[k]; !ok || prev != v {
			s.churn = append(s.churn, now)
		}
	}
	cutoff := now.Add(-time.Minute)
	i := 0
	for ; i < len(s.churn); i++ {
		if s.churn[i].After(cutoff) {
			break
		}
	}
	if i > 0 {
		s.churn = s.churn[i:]
	}
	s.owners = current
	return len(s.churn)
}

func (s *simulation) repl(ctx context.Context, rl *readline.Instance) {
	for {
		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt || err == io.EOF {
				return
			}
			fmt.Fprintln(rl.Stdout(), err)
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		switch strings.ToLower(line) {
		case "help":
			fmt.Fprintln(rl.Stdout(), "commands: add [n] [weight], remove <id>, restart <id>, kill <id>, weight <id> <w>, fail <id> on|off, health <id> on|off, shedding on|off, release <n>, focus <workload|none>, dashboard on|off, scenario <name>, load <file>, predict down <node>, explain <workload> <unit>, quit")
			rl.Refresh()
			continue
		case "quit", "exit":
			return
		}
		s.handleCommand(ctx, rl, line)
	}
}

func (s *simulation) handleCommand(ctx context.Context, rl *readline.Instance, line string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return
	}
	switch parts[0] {
	case "dashboard":
		if len(parts) < 2 {
			fmt.Fprintln(rl.Stdout(), "usage: dashboard on|off")
			break
		}
		switch parts[1] {
		case "on", "resume":
			s.dashboardEnabled.Store(true)
			fmt.Fprintln(rl.Stdout(), "dashboard resumed")
		case "off", "pause":
			s.dashboardEnabled.Store(false)
			fmt.Fprintln(rl.Stdout(), "dashboard paused")
		default:
			fmt.Fprintln(rl.Stdout(), "usage: dashboard on|off")
		}
	case "focus":
		if len(parts) < 2 {
			fmt.Fprintln(rl.Stdout(), "usage: focus <workload|none>")
			break
		}
		if parts[1] == "none" {
			s.focus = ""
			fmt.Fprintln(rl.Stdout(), "focus cleared")
		} else {
			s.focus = parts[1]
			fmt.Fprintf(rl.Stdout(), "focus set to %s\n", parts[1])
		}
	case "predict":
		if len(parts) == 3 && parts[1] == "down" {
			s.predictDown(rl, parts[2])
			break
		}
		fmt.Fprintln(rl.Stdout(), "usage: predict down <nodeID>")
	case "explain":
		if len(parts) < 3 {
			fmt.Fprintln(rl.Stdout(), "usage: explain <workload> <unit>")
			break
		}
		unit, err := strconv.Atoi(parts[2])
		if err != nil {
			fmt.Fprintln(rl.Stdout(), "unit must be an integer")
			break
		}
		s.explainUnit(rl, parts[1], unit)
	case "add":
		count := 1
		weight := 1.0
		if len(parts) > 1 {
			if v, err := strconv.Atoi(parts[1]); err == nil {
				count = v
			}
		}
		if len(parts) > 2 {
			if v, err := strconv.ParseFloat(parts[2], 64); err == nil {
				weight = v
			}
		}
		for i := 0; i < count; i++ {
			id := s.addNode(weight)
			fmt.Fprintf(rl.Stdout(), "added node %s\n", id)
		}
	case "remove":
		if len(parts) < 2 {
			fmt.Fprintln(rl.Stdout(), "usage: remove <id>")
			break
		}
		s.removeNode(parts[1], true)
	case "restart":
		if len(parts) < 2 {
			fmt.Fprintln(rl.Stdout(), "usage: restart <id>")
			break
		}
		s.restartNode(parts[1])
	case "kill":
		if len(parts) < 2 {
			fmt.Fprintln(rl.Stdout(), "usage: kill <id>")
			break
		}
		s.removeNode(parts[1], false)
	case "weight":
		if len(parts) < 3 {
			fmt.Fprintln(rl.Stdout(), "usage: weight <id> <weight>")
			break
		}
		w, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			fmt.Fprintln(rl.Stdout(), "invalid weight")
			break
		}
		s.adjustWeight(parts[1], w)
	case "fail":
		if len(parts) < 3 {
			fmt.Fprintln(rl.Stdout(), "usage: fail <id> on|off")
			break
		}
		s.setRedisFault(parts[1], parts[2] == "on")
	case "health":
		if len(parts) < 3 {
			fmt.Fprintln(rl.Stdout(), "usage: health <id> on|off")
			break
		}
		s.setHealth(parts[1], parts[2] == "on")
	case "scenario":
		if len(parts) < 2 {
			fmt.Fprintln(rl.Stdout(), "usage: scenario <name>")
			break
		}
		go s.runScenario(ctx, parts[1], rl)
	case "shedding":
		if len(parts) < 2 {
			fmt.Fprintln(rl.Stdout(), "usage: shedding on|off")
			break
		}
		s.template.SheddingEnabled = parts[1] == "on"
		s.restartAll()
	case "release":
		if len(parts) < 2 {
			fmt.Fprintln(rl.Stdout(), "usage: release <per-interval>")
			break
		}
		if v, err := strconv.Atoi(parts[1]); err == nil {
			s.template.SheddingRelease = v
			s.restartAll()
		}
	case "load":
		if len(parts) < 2 {
			fmt.Fprintln(rl.Stdout(), "usage: load <file>")
			break
		}
		go s.runScenarioFile(ctx, parts[1], rl)
	default:
		fmt.Fprintln(rl.Stdout(), "unknown command")
	}
	rl.Refresh()
}

func (s *simulation) runScenario(ctx context.Context, name string, rl *readline.Instance) {
	output := io.Writer(os.Stdout)
	if rl != nil {
		output = rl.Stdout()
	}
	fmt.Fprintf(output, "running scenario %s\n", name)
	switch name {
	case "scale-out":
		s.addNode(1)
		time.Sleep(1 * time.Second)
		for i := 0; i < 5; i++ {
			s.addNode(1)
		}
		time.Sleep(2 * time.Second)
		for i := 0; i < 10; i++ {
			s.addNode(1)
		}
		s.logSummary(name)
	case "pod-death":
		nodes := s.nodeIDs()
		if len(nodes) == 0 {
			return
		}
		target := len(nodes) / 2
		rand.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
		for i := 0; i < target; i++ {
			s.removeNode(nodes[i], false)
		}
		s.logSummary(name)
	case "redis-instability":
		nodes := s.nodeIDs()
		for i, id := range nodes {
			if i%2 == 0 {
				s.setRedisFault(id, true)
			}
		}
		time.Sleep(3 * time.Second)
		for _, id := range nodes {
			s.setRedisFault(id, false)
		}
		s.logSummary(name)
	case "weight-skew":
		nodes := s.nodeIDs()
		for i, id := range nodes {
			s.adjustWeight(id, float64(i+1))
		}
		s.logSummary(name)
	case "cross-cluster":
		cfg := s.template
		cfg.ClusterID = "other"
		cfg.NodeID = uuid.NewString()
		client := redis.NewClient(&redis.Options{Addr: s.redisAddr})
		hook := &chaosHook{}
		client.AddHook(hook)
		health := &healthGate{healthy: true}
		ctrl, _ := rendezgo.NewController(cfg, client, &playConsumerFactory{}, rendezgo.NopLogger(), rendezgo.NopMetrics(), rendezgo.WithWeightProvider(s.weights), rendezgo.WithHealthChecker(health))
		s.weights.Set(cfg.ClusterID+":"+cfg.NodeID, 1)
		nodeCtx, cancel := context.WithCancel(ctx)
		go ctrl.Start(nodeCtx)
		s.mu.Lock()
		s.nodes[cfg.ClusterID+":"+cfg.NodeID] = &simNode{id: cfg.ClusterID + ":" + cfg.NodeID, weight: 1, cancel: cancel, ctrl: ctrl, client: client, hook: hook, health: health, healthy: true}
		s.mu.Unlock()
		s.logSummary(name)
	case "lease-flapping":
		nodes := s.nodeIDs()
		if len(nodes) == 0 {
			return
		}
		id := nodes[0]
		for i := 0; i < 3; i++ {
			s.setRedisFault(id, true)
			time.Sleep(500 * time.Millisecond)
			s.setRedisFault(id, false)
			time.Sleep(700 * time.Millisecond)
		}
		s.logSummary(name)
	default:
		fmt.Fprintln(output, "unknown scenario")
	}
	if rl != nil {
		rl.Refresh()
	}
}

func (s *simulation) runScenarioFile(ctx context.Context, path string, rl *readline.Instance) {
	output := io.Writer(os.Stdout)
	if rl != nil {
		output = rl.Stdout()
	}
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		fmt.Fprintf(output, "read scenario: %v\n", err)
		return
	}
	var file scenarioFile
	if strings.HasSuffix(path, ".json") {
		if err := json.Unmarshal(data, &file.Events); err != nil {
			fmt.Fprintf(output, "parse scenario: %v\n", err)
			return
		}
	} else {
		if err := yaml.Unmarshal(data, &file.Events); err != nil {
			fmt.Fprintf(output, "parse scenario: %v\n", err)
			return
		}
	}
	start := time.Now()
	for _, evt := range file.Events {
		delay, err := time.ParseDuration(evt.At)
		if err != nil {
			continue
		}
		wait := delay - time.Since(start)
		if wait < 0 {
			wait = 0
		}
		go func(ev scenarioEvent) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
				s.handleCommand(ctx, rl, ev.Command)
			}
		}(evt)
	}
	if rl != nil {
		rl.Refresh()
	}
}

func (s *simulation) nodeIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.nodes))
	for id := range s.nodes {
		ids = append(ids, id)
	}
	return ids
}

func (s *simulation) logSummary(label string) {
	weights := s.liveNodeWeights("")
	owners, _, _ := s.redisState()
	desired := rendezgo.DesiredOwners(s.workloads, weights)
	total := 0
	matches := 0
	dist := map[string]int{}
	for _, wl := range s.workloads {
		for i := 0; i < wl.Units; i++ {
			slot := rendezgo.Slot{Workload: wl.Name, Unit: i}
			key := leaseKeyLocal(slot)
			owner := owners[key]
			if owner != "" {
				dist[owner]++
			}
			if desired[key] == owner {
				matches++
			}
			total++
		}
	}
	convergence := 0.0
	if total > 0 {
		convergence = (float64(matches) / float64(total)) * 100
	}
	fmt.Printf("Scenario %s summary: convergence=%.1f%% churn/min=%d ownership=%v\n", label, convergence, len(s.churn), dist)
}

type playConsumerFactory struct{}

func (playConsumerFactory) NewConsumer(slot rendezgo.Slot) (rendezgo.Consumer, error) {
	return &playConsumer{slot: slot.Key()}, nil
}

type playConsumer struct {
	slot string
}

func (p *playConsumer) Run(ctx context.Context) error {
	t := time.NewTicker(750 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (p *playConsumer) Stop(ctx context.Context) error {
	select {
	case <-ctx.Done():
	case <-time.After(50 * time.Millisecond):
	}
	return nil
}

type weightTable struct {
	mu      sync.RWMutex
	weights map[string]float64
}

func newWeightTable() *weightTable {
	return &weightTable{weights: map[string]float64{}}
}

func (w *weightTable) Weight(id string) (float64, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	val, ok := w.weights[id]
	return val, ok
}

func (w *weightTable) Set(id string, weight float64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.weights[id] = weight
}

func (w *weightTable) Snapshot() map[string]float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	cp := make(map[string]float64, len(w.weights))
	for k, v := range w.weights {
		cp[k] = v
	}
	return cp
}

func (w *weightTable) Delete(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.weights, id)
}

type healthGate struct {
	mu      sync.RWMutex
	healthy bool
}

func (h *healthGate) Healthy(ctx context.Context) bool {
	_ = ctx
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.healthy
}

func (h *healthGate) Set(v bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.healthy = v
}

type chaosHook struct {
	fail atomic.Bool
}

func (h *chaosHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *chaosHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.fail.Load() {
			return fmt.Errorf("chaos redis failure")
		}
		return next(ctx, cmd)
	}
}

func (h *chaosHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.fail.Load() {
			return fmt.Errorf("chaos redis pipeline failure")
		}
		return next(ctx, cmds)
	}
}

type scenarioFile struct {
	Events []scenarioEvent `json:"events" yaml:"events"`
}

type scenarioEvent struct {
	At      string `json:"at" yaml:"at"`
	Command string `json:"command" yaml:"command"`
}

func leaseKeyLocal(slot rendezgo.Slot) string {
	return fmt.Sprintf("lease:%s:%d", slot.Workload, slot.Unit)
}

func movedKeyLocal(slot rendezgo.Slot) string {
	return fmt.Sprintf("moved:%s:%d", slot.Workload, slot.Unit)
}

func slotFromLeaseKey(key string) (rendezgo.Slot, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 || parts[0] != "lease" {
		return rendezgo.Slot{}, fmt.Errorf("invalid lease key %s", key)
	}
	unit, err := strconv.Atoi(parts[2])
	if err != nil {
		return rendezgo.Slot{}, err
	}
	return rendezgo.Slot{Workload: parts[1], Unit: unit}, nil
}

func formatTTL(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	if d >= time.Second {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return d.Round(time.Millisecond).String()
}

func formatRanking(ranking []rendezgo.NodeScore, limit int) string {
	if len(ranking) == 0 {
		return "none"
	}
	if limit <= 0 || limit > len(ranking) {
		limit = len(ranking)
	}
	parts := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		parts = append(parts, fmt.Sprintf("%s(%.3f)", ranking[i].ID, ranking[i].Score))
	}
	return strings.Join(parts, ", ")
}

func nodeState(n *simNode) string {
	if n == nil {
		return "unknown"
	}
	if !n.healthy {
		return "unhealthy"
	}
	if n.ctrl != nil && n.ctrl.BackoffActive() {
		return "backoff"
	}
	return "ok"
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		cancel()
	}()
	return ctx, cancel
}

func parseWorkloads(flagVal string) []rendezgo.WorkloadConfig {
	parts := strings.Split(flagVal, ",")
	var workloads []rendezgo.WorkloadConfig
	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			continue
		}
		kv := strings.SplitN(p, ":", 2)
		if len(kv) != 2 {
			continue
		}
		var units int
		fmt.Sscanf(kv[1], "%d", &units)
		if units <= 0 {
			continue
		}
		workloads = append(workloads, rendezgo.WorkloadConfig{Name: kv[0], Units: units})
	}
	return workloads
}

func shortOwner(owner string) string {
	if owner == "" {
		return "-"
	}
	parts := strings.Split(owner, ":")
	return parts[len(parts)-1]
}
